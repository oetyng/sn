// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{messaging::data::RegisterWrite, types::Token};

use dashmap::{DashMap, DashSet};
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
type Pools = Arc<RwLock<DashMap<u8, PaymentJob>>>;

type Db = Arc<sled::Db>;

/// A stash of tokens
pub(crate) trait Stash: Clone {
    /// The total value of the stash.
    fn value(&self) -> Token;
    /// Removes and returns dbcs up to the requested
    /// value, if exists.
    fn take(&self, value: Token) -> Vec<Dbc>;
}

pub(crate) struct Dbc {}

struct PaymentBatching<S: Stash> {
    db: Db,
    pools: Pools,
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

async fn push_task(batch: Batch, db: Db, pools: Pools, pool_limit: usize) {
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

    try_clear_pools(db, pools, pool_limit).await;
}

// todo: let pools be persisted
// do not clear pools, but use new pools
// only clear after all processing is done
async fn try_clear_pools(db: Db, pools: Pools, pool_limit: usize) {
    // take exclusive lock on the pools
    let pool = pools.write().await;
    // if all pools have reached the limit...
    if pool.iter().all(|set| set.value().len() >= pool_limit) {
        // ... then kick off payment process, and clear the pools
        let _ = pool
            .iter()
            .map(|set| {
                set.value()
                    .iter()
                    .map(|s| OpId(s.0))
                    .collect::<BTreeSet<_>>()
            })
            .map(|set| (set, db.clone()))
            .map(|(set, db)| task::spawn(pay(set, db)))
            .collect_vec();

        pool.clear();
    }
}

async fn process_reg_ops(reg_ops: BTreeSet<RegisterWrite>, pools: Pools) -> Vec<JoinHandle<()>> {
    unimplemented!()
}

async fn process_msg_quotas(
    msg_quotas: BTreeMap<XorName, u64>,
    pools: Pools,
) -> Vec<JoinHandle<()>> {
    unimplemented!()
}

async fn process_files(files: BTreeSet<PathBuf>, db: Db, pools: Pools) -> Vec<JoinHandle<()>> {
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
        .collect_vec();

    // shuffle chunks before adding to pools
    let mut rng = rand::thread_rng();
    use rand::seq::SliceRandom;
    chunks.shuffle(&mut rng);

    vec![task::spawn(add_to_pools(chunks, pools))]
}

fn get_chunks(file: PathBuf, db: Db) -> Vec<XorName> {
    // let chunks = self_encryptor.encrypt(file);
    // db.store(chunks)
    // return chunk names
    vec![]
}

async fn add_to_pools(ids: Vec<XorName>, pools: Pools) {
    let pool_ref = pools.read().await;
    let pool_count = pool_ref.len() as u8;
    let mut rng = OsRng;
    for id in ids {
        let index = rng.gen_range(0, pool_count);
        if let Some(pool) = pool_ref.get(&index) {
            let _ = pool.value().insert(OpId(id));
        }
    }
}

async fn pay(ops: BTreeSet<OpId>, db: Arc<sled::Db>) {
    // kick off payment process
    // get quote
}

fn send(ops: BTreeSet<OpId>, db: Arc<sled::Db>) {
    // kick off sending process
}
