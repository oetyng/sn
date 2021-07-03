// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::path::Path;

use bincode::serialize;
use self_encryption::{DataMap, SelfEncryptor, Storage};
use tempdir::TempDir;
use tokio::time::Instant;
use crate::{client::{Error, client_api::{blob_apis::DataMapLevel, blob_storage::BlobStorageDryRun, file_manager::wal::{FileWAL, FileWALConfig}}, utils::generate_random_vector}, dbs::UsedSpace, types::{Chunk, ChunkAddress, PrivateChunk, PublicChunk}};

#[tokio::test]
#[ignore = "too heavy for CI"]
pub async fn measure_encryption() -> Result<(), Error> {
    let data_sizes = [1, 10, 100, 1000, 10_000, 100_000, 1000_000, 10_000_000, 100_000_000, 1000_000_000];
    for data_size in data_sizes {
        let file = generate_random_vector::<u8>(data_size);
        println!("{} bytes generated", data_size);
        let storage = BlobStorageDryRun::new(None);

        let now = Instant::now();
        let _res = encrypt(file, storage).await?;
        let elapsed = now.elapsed();
        
        println!("Elapsed: {} ms", elapsed.as_millis());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "too heavy for CI"]
pub async fn measure_encryption_to_disk() -> Result<(), Error> {
    let data_sizes = [1, 10, 100, 1000, 10_000, 100_000, 1000_000, 10_000_000, 100_000_000, 1000_000_000];
    let root = TempDir::new("test").unwrap();
    for data_size in data_sizes {
        let file = generate_random_vector::<u8>(data_size);
        println!("{} bytes generated", data_size);
        
        let config = FileWALConfig {
            owner: None,
            used_space: UsedSpace::new(u64::MAX),
        };
        let path = root.path().join(Path::new(&format!("{}", data_size)));
        let storage = FileWAL::new(path.as_path(), config).await?;

        let now = Instant::now();
        let _res = encrypt(file, storage).await?;
        let elapsed = now.elapsed();

        println!("Elapsed: {} ms", elapsed.as_millis());
    }
    Ok(())
}

/// Uses self_encryption to generate an encrypted Blob serialized data map,
/// without connecting and/or writing to the network.
pub async fn encrypt<S>(mut file: Vec<u8>, storage: S) 
-> Result<(DataMap, ChunkAddress), Error> where
S: Storage + Send + Sync + Clone + 'static {
    //let owner = None;
    let mut is_original_data = true;

    let (data_map, head_chunk) = loop {
        let self_encryptor =
            SelfEncryptor::new(storage.clone(), DataMap::None).map_err(Error::SelfEncryption)?;
        self_encryptor
            .write(&file, 0)
            .await
            .map_err(Error::SelfEncryption)?;
        let (data_map, _) = self_encryptor
            .close()
            .await
            .map_err(Error::SelfEncryption)?;

        let chunk_content = if is_original_data {
            is_original_data = false;
            serialize(&DataMapLevel::Root(data_map.clone()))?
        } else {
            serialize(&DataMapLevel::Child(data_map.clone()))?
        };

        // let chunk: Chunk = if let Some(owner) = owner {
        //     PrivateChunk::new(chunk_content, owner).into()
        // } else {
            let chunk: Chunk = PublicChunk::new(chunk_content).into();
        //};

        // If the chunk (data map) is bigger than 1MB we need to break it down
        if chunk.validate_size() {
            break (data_map, chunk);
        } else {
            file = serialize(&chunk)?;
        }
    };

    Ok((data_map, *head_chunk.address()))
}