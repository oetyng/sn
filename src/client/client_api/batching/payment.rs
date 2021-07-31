// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    client::{Error, Result},
    dbs::{KvStore, Value},
    messaging::data::{ChunkWrite, DataCmd, RegisterWrite},
    types::{utils::serialise, Chunk, Token},
};

use dashmap::DashSet;
use futures::future::join_all;
use itertools::Itertools;
use rand::{rngs::OsRng, Rng};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    sync::RwLock,
    task::{self, JoinHandle},
};
use xor_name::XorName;

/// The batching is a way to

/// Batching ops into pools is a means to not include all chunks of the
/// same file into the same quote, since that quote is then sent around the network.
/// So we avoid that information leak by mixing chunks from different files.
/// Since we pay also for other types of ops than chunk uploads, we can speed up the chunk processing
/// by including those ops in the pools as well.
///
/// Say, our pool count is 4, and the limit is 10 (that means we will start payment step
/// when all pools have reached at least 10 ops). That means we will do 4 payments for 10 ops, initiated once we reach 40 ops.
/// So when we store a file of 20 chunks, we will have 5 chunks in each pool. The limit is 10, and is not reached yet.
/// Say we add another file of 15 chunks, we will then have 3 pools with 9 ops each, and 1 pool with 8 ops. We need another 5 ops.
/// Say we then do some operations on a merkle register; after 5 such ops, the pools are now all filled to their limits, and the
/// payment process starts, while we clear the pools for new ops.

/// It is up to client how many
/// entries it wants to batch at a time.
/// It could push a single entry, or thousands of them, in every batch.
pub(crate) struct Batch {
    files: BTreeSet<PathBuf>,
    reg_ops: BTreeSet<RegisterWrite>,
    msg_quotas: BTreeMap<XorName, u64>,
}

#[derive(Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
struct OpId(pub(crate) XorName);

type PaymentJob = DashSet<OpId>;
//type Pools = Arc<RwLock<DashMap<u8, PaymentJob>>>;
type Pools<S: Stash> = PaymentPools<S>;

type Db = Arc<KvStore<XorName, DataCmd>>;

/// A stash of tokens
pub(crate) trait Stash: Clone + Send + Sync {
    /// The total value of the stash.
    fn value(&self) -> Token;
    /// Removes and returns dbcs up to the requested
    /// value, if exists.
    fn take(&self, value: Token) -> Vec<Dbc>;
}

pub(crate) struct Dbc {}

struct PaymentBatching<S: Stash> {
    db: Db,
    pools: Pools<S>,
    pool_limit: usize,
    stash: S,
}

impl<S: Stash> PaymentBatching<S> {
    // pub fn new(pool_count: u8, pool_limit: usize, stash: S) -> Result<Self, Error> {
    //     let mut pools = DashMap::new();
    //     for i in 0..pool_count {
    //         let _ = pools.insert(i, DashSet::new());
    //     }
    //     let pools = Arc::new(RwLock::new(pools));
    //     let db = sled::open(Path::new("")).map_err(Error::)?;
    //     Ok(Self {
    //         db,
    //         pools,
    //         pool_limit,
    //         stash,
    //     })
    // }

    pub(crate) async fn push(&self, batch: Batch) {
        let db = self.db.clone();
        let pools = self.pools.clone();
        let pool_limit = self.pool_limit;
        let _ = task::spawn(push_task(batch, db, pools, pool_limit));
    }
}

async fn push_task<S: Stash>(batch: Batch, db: Db, pools: Pools<S>, pool_limit: usize) {
    let Batch {
        files,
        reg_ops,
        msg_quotas,
    } = batch;

    let db_clone = db.clone();
    let clone_1 = pools.clone();
    let files_task = task::spawn(process_files(files, db_clone, clone_1));

    let clone_2 = pools.clone();
    let reg_task = task::spawn(process_reg_ops(reg_ops, clone_2));

    let clone_3 = pools.clone();
    let msg_task = task::spawn(process_msg_quotas(msg_quotas, clone_3));

    let res = join_all([files_task, reg_task, msg_task])
        .await
        .into_iter()
        .flatten()
        .flatten()
        .collect_vec();

    let _ = join_all(res).await;

    // if all pools have reached the limit...
    if let Ok(Some(previous_version)) = pools.try_move_to_new().await {
        // ... then kick off payment process for the filled pools
        let _ = task::spawn(pay(previous_version, db));
    }
}

async fn process_reg_ops<S: Stash>(
    reg_ops: BTreeSet<RegisterWrite>,
    pools: Pools<S>,
) -> Vec<JoinHandle<()>> {
    unimplemented!()
}

async fn process_msg_quotas<S: Stash>(
    msg_quotas: BTreeMap<XorName, u64>,
    pools: Pools<S>,
) -> Vec<JoinHandle<()>> {
    unimplemented!()
}

async fn process_files<S: Stash>(
    files: BTreeSet<PathBuf>,
    db: Db,
    pools: Pools<S>,
) -> Vec<JoinHandle<Result<()>>> {
    // get chunks via SE (+ store to db), then pool them
    let handles = files
        .into_iter()
        .map(|file| (file, db.clone()))
        .map(|(file, db)| task::spawn_blocking(|| get_chunks(file, db)))
        .collect_vec();

    let mut chunks = join_all(handles)
        .await
        .into_iter()
        .flatten()
        .flatten()
        .map(|c| DataCmd::Chunk(ChunkWrite::New(c)))
        .collect_vec();

    // shuffle chunks before adding to pools
    let mut rng = rand::thread_rng();
    use rand::seq::SliceRandom;
    chunks.shuffle(&mut rng);

    vec![task::spawn(add_to_pools(chunks, db, pools))]
}

fn get_chunks(file: PathBuf, db: Db) -> Vec<Chunk> {
    // let chunks = self_encryptor.encrypt(file);
    let chunks = vec![];

    // return chunks
    chunks
}

async fn add_to_pools<S: Stash>(ops: Vec<DataCmd>, db: Db, pools: Pools<S>) -> Result<(), Error> {
    //db.store_batch(&ops).await.map_err(Error::Database)?;
    let ops = ops.into_iter().map(|c| (*c.key(), c)).collect();
    pools.add(ops).await
}

async fn pay(pool_version: u64, db: Db) {
    // kick off payment process
    // get quote
}

fn send(ops: BTreeSet<OpId>, db: Db) {
    // kick off sending process
}

enum OpType {
    Chunk,
    Register,
}
struct Status {
    op_type: OpType,
}

#[derive(Clone)]
struct PaymentPools<S: Stash> {
    db: Arc<sled::Db>,
    pool_version: Arc<RwLock<u64>>,
    pool_limit: usize,
    pool_count: u8,
    stash: S,
}

impl<S: Stash> PaymentPools<S> {
    pub fn new(
        db: Arc<sled::Db>,
        pool_version: Arc<RwLock<u64>>,
        pool_limit: usize,
        pool_count: u8,
        stash: S,
    ) -> Result<Self, Error> {
        for i in 0..pool_count {
            let tree = db
                .open_tree(format!("payment_pool_{}", i))
                .map_err(Error::Sled)?;
        }

        Ok(Self {
            db,
            pool_version,
            pool_limit,
            pool_count,
            stash,
        })
    }

    fn get_keys(&self) -> Vec<XorName> {
        (0..self.pool_count)
            .into_iter()
            .map(|i| self.db.open_tree(format!("payment_pool_{}", i)))
            .flatten()
            .map(|tree| tree.iter().keys())
            .flatten()
            .flatten()
            .map(|key| convert(key))
            .collect()
    }

    // does not clear pools, but uses new pools
    // only clear after all processing is done
    async fn try_move_to_new(&self) -> Result<Option<u64>, Error> {
        {
            let version = self.pool_version.read().await;
            for i in 0..self.pool_count {
                let tree = self
                    .db
                    .open_tree(format!("payment_pool_{}_v{}", i, version))
                    .map_err(Error::Sled)?;

                if !tree.len() >= self.pool_limit {
                    return Ok(None);
                }
            }
        }

        let mut version = self.pool_version.write().await;
        let previous_version = version.clone();

        match version.checked_add(1) {
            None => return Err(Error::NoResponse), // TODO: Error::PoolVersionOverflow
            Some(next_version) => *version = next_version,
        };

        Ok(Some(previous_version))
    }

    async fn add(&self, ops: BTreeMap<XorName, DataCmd>) -> Result<(), Error> {
        use rayon::iter::*;
        let results: Vec<_> = ops
            .par_iter()
            .map(|(id, job)| {
                let mut rng = OsRng;
                let index = rng.gen_range(0, self.pool_count);
                let key = serialise(&id)?;
                let value = serialise(&job)?;
                Ok::<(u8, (Vec<u8>, Vec<u8>)), Error>((index, (key, value)))
            })
            .flatten()
            .collect();

        let results: BTreeMap<u8, Vec<(Vec<u8>, Vec<u8>)>> = results
            .into_iter()
            .group_by(|(index, _)| *index)
            .into_iter()
            .map(|(key, group)| (key, group.map(|(_, v)| v).collect_vec()))
            .collect();

        // for (id, job) in ops {
        //     let index = rng.gen_range(0, self.pool_count);
        //     if let Some(batch) = pools.get_mut(&index) {
        //         let key = serialise(&id)?;
        //         let value = serialise(&job)?;
        //         let _ = batch.insert(key, value);
        //     }
        // }

        // take exclusive lock on version, to make sure we add to current version of pools
        let version = self.pool_version.write().await;

        for (i, pairs) in results {
            let tree = self
                .db
                .open_tree(format!("payment_pool_{}_v{}", i, version))
                .map_err(Error::Sled)?;
            let mut batch = sled::Batch::default();
            for (key, value) in pairs {
                batch.insert(key, value);
            }
            tree.apply_batch(batch)?;
        }

        Ok(())
    }
}
