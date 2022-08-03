// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use sn_interface::{
    messaging::data::{CreateRegister, DataCmd, SignedRegisterCreate},
    types::{
        register::{Policy, User},
        Chunk, Keypair, RegisterCmd,
    },
};
use sn_node::{
    data::{ChunkStorage, RegisterStorage},
    node::cfg::config_handler::Config,
    UsedSpace,
};

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion};
use eyre::{Result, WrapErr};
use rand::{distributions::Alphanumeric, rngs::OsRng, thread_rng, Rng};
use rayon::current_num_threads;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use tempfile::tempdir;
use tokio::runtime::Runtime;

// sample size is _NOT_ the number of times the command is run...
// https://bheisler.github.io/criterion.rs/book/analysis.html#measurement
const SAMPLE_SIZE: usize = 10;

/// Generates a random vector using provided `length`.
fn random_vector(length: usize) -> Vec<u8> {
    use rayon::prelude::*;
    let threads = current_num_threads();

    if threads > length {
        let mut rng = OsRng;
        return ::std::iter::repeat(())
            .map(|()| rng.gen::<u8>())
            .take(length)
            .collect();
    }

    let per_thread = length / threads;
    let remainder = length % threads;

    let mut bytes: Vec<u8> = (0..threads)
        .par_bridge()
        .map(|_| vec![0u8; per_thread])
        .map(|mut bytes| {
            let bytes = bytes.as_mut_slice();
            rand::thread_rng().fill(bytes);
            bytes.to_owned()
        })
        .flatten()
        .collect();

    bytes.extend(vec![0u8; remainder]);

    bytes
}

/// We only testing with 4000 different data inputs.
/// Making the first 4 bytes random shall be enough.
fn grows_vec_to_bytes(seed: &[u8]) -> Bytes {
    let mut bytes = BytesMut::from(seed);
    let mut rng = OsRng;
    bytes[0] = rng.gen::<u8>();
    bytes[1] = rng.gen::<u8>();
    bytes[2] = rng.gen::<u8>();
    bytes[3] = rng.gen::<u8>();

    Bytes::from(bytes)
}

fn main() -> Result<()> {
    let mut criterion = custom_criterion();

    bench_data_storage_writes(&mut criterion)?;
    bench_data_storage_reads(&mut criterion)?;

    Ok(())
}

fn bench_data_storage_writes(c: &mut Criterion) -> Result<()> {
    let mut group = c.benchmark_group("write-sampling");

    let runtime = Runtime::new().unwrap();
    pub const NONSENSE_CHUNK_SIZE: usize = 1024; // data size should not be important for keys() tests

    let size_ranges = [100, 1_000, 4_000];

    for size in size_ranges.iter() {
        let data_set: Vec<_> = (0..*size).map(|_| create_random_register_cmd()).collect();
        group.bench_with_input(
            BenchmarkId::new("register_writes", size),
            &(size, &data_set),
            |b, (size, data_set)| {
                let storage = get_new_register_store()
                    .context("Could not create a temp data store")
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    for i in 0..**size {
                        let _ = storage.clone().write(data_set[i].clone()).await;
                    }
                })
            },
        );
    }

    for size in size_ranges.iter() {
        let seed = random_vector(NONSENSE_CHUNK_SIZE);
        group.bench_with_input(
            BenchmarkId::new("chunk writes", size),
            &(size, &seed),
            |b, (size, seed)| {
                let storage = get_new_chunk_store()
                    .context("Could not create a temp data store")
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    for _ in 0..**size {
                        let random_data = DataCmd::StoreChunk(Chunk::new(grows_vec_to_bytes(seed)));
                        storage
                            .clone()
                            .store(random_data.clone())
                            .await
                            .expect("failed to write chunk {i}");
                    }
                })
            },
        );
    }

    Ok(())
}

fn bench_data_storage_reads(c: &mut Criterion) -> Result<()> {
    let mut group = c.benchmark_group("read-sampling");

    let runtime = Runtime::new().unwrap();
    pub const NONSENSE_CHUNK_SIZE: usize = 1024; // data size should not be important for keys() tests

    let size_ranges = [100, 1_000, 4_000];

    for size in size_ranges.iter() {
        group.bench_with_input(BenchmarkId::new("register_keys", size), size, |b, &size| {
            let storage = get_new_register_store()
                .context("Could not create a temp data store")
                .unwrap();

            for _ in 0..size {
                let random_data = create_random_register_cmd();

                if runtime
                    .block_on(storage.write(random_data))
                    .context("could not store register")
                    .is_err()
                {
                    panic!("Error storing register");
                }
            }

            b.iter(|| async {
                let _ = storage.keys();
            })
        });
    }

    for size in size_ranges.iter() {
        group.bench_with_input(BenchmarkId::new("chunk keys", size), size, |b, &size| {
            let storage = get_new_chunk_store()
                .context("Could not create a temp data store")
                .unwrap();

            for _ in 0..size {
                let file = sn_interface::types::utils::random_bytes(NONSENSE_CHUNK_SIZE);
                let random_data = DataCmd::StoreChunk(Chunk::new(file));
                if runtime
                    .block_on(storage.store(random_data))
                    .context("could not store chunk")
                    .is_err()
                {
                    panic!("Error storing chunk");
                };
            }

            b.iter(|| async {
                let _ = storage.keys();
            })
        });
    }

    Ok(())
}

fn section_auth() -> sn_interface::messaging::SectionAuth {
    use sn_interface::messaging::system::KeyedSig;

    let sk = bls::SecretKey::random();
    let public_key = sk.public_key();
    let data = "hello".to_string();
    let signature = sk.sign(&data);
    let sig = KeyedSig {
        public_key,
        signature,
    };
    sn_interface::messaging::SectionAuth {
        src_name: sn_interface::types::PublicKey::Bls(public_key).into(),
        sig,
    }
}

fn custom_criterion() -> Criterion {
    Criterion::default().sample_size(SAMPLE_SIZE)
}

fn public_policy(owner: User) -> Policy {
    let permissions = BTreeMap::new();
    Policy { owner, permissions }
}

pub fn create_random_register_cmd() -> RegisterCmd {
    let keypair = Keypair::new_ed25519();

    let name = xor_name::rand::random();
    let tag = 15000;
    let owner = User::Key(keypair.public_key());
    let policy = public_policy(owner);

    let op = CreateRegister::Empty {
        name,
        tag,
        size: u16::MAX, // TODO: use argument
        policy,
    };
    let signature = keypair.sign(&bincode::serialize(&op).expect("could not serialize op"));
    RegisterCmd::Create {
        cmd: SignedRegisterCreate {
            op,
            auth: sn_interface::messaging::ServiceAuth {
                public_key: keypair.public_key(),
                signature,
            },
        },
        section_auth: section_auth(), // obtained after presenting a valid payment to the network
    }
}

fn get_new_chunk_store() -> Result<ChunkStorage> {
    let (storage_dir, used_space) = gen_info()?;
    Ok(ChunkStorage::new(&storage_dir, used_space)?)
}

fn get_new_register_store() -> Result<RegisterStorage> {
    let (storage_dir, used_space) = gen_info()?;
    Ok(RegisterStorage::new(&storage_dir, used_space)?)
}

fn gen_info() -> Result<(PathBuf, UsedSpace)> {
    let random_filename: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();
    let root_dir = tempdir().map_err(|e| eyre::eyre!(e.to_string()))?;
    let storage_dir = Path::new(root_dir.path()).join(random_filename);

    let config = Config::default();
    let max_capacity = config.max_capacity();
    let used_space = UsedSpace::new(max_capacity);

    Ok((storage_dir, used_space))
}
