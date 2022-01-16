// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod chunk_storage;
mod register_storage;

use super::{Command, Core};

use crate::{
    dbs::Result,
    messaging::{
        data::{DataQuery, RegisterStoreExport, StorageLevel},
        system::{NodeCmd, NodeQueryResponse, SystemMsg},
    },
    types::{crdts::User, ReplicatedData, ReplicatedDataAddress as DataAddress},
    UsedSpace,
};

pub(crate) use chunk_storage::ChunkStorage;
pub(crate) use register_storage::RegisterStorage;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::info;
use xor_name::XorName;

/// Operations on data.
#[derive(Clone)]
pub(crate) struct DataStorage {
    chunks: ChunkStorage,
    registers: RegisterStorage,
    used_space: UsedSpace,
    last_recorded_level: Arc<RwLock<StorageLevel>>,
}

impl DataStorage {
    pub(crate) fn new(path: &Path, used_space: UsedSpace) -> Result<Self> {
        Ok(Self {
            chunks: ChunkStorage::new(path, used_space.clone())?,
            registers: RegisterStorage::new(path, used_space.clone())?,
            used_space,
            last_recorded_level: Arc::new(RwLock::new(StorageLevel::zero())),
        })
    }

    /// Store data in the local store
    #[instrument(skip_all)]
    pub(super) async fn store(&self, data: &ReplicatedData) -> Result<Option<StorageLevel>> {
        match data.clone() {
            ReplicatedData::Chunk(chunk) => self.chunks.store(&chunk).await?,
            ReplicatedData::RegisterLog(data) => {
                self.registers
                    .update(RegisterStoreExport(vec![data]))
                    .await?
            }
            ReplicatedData::RegisterWrite(cmd) => self.registers.write(cmd).await?,
        };

        // check if we've filled another apprx. 10%-points of our storage
        // if so, update the recorded level
        let last_recorded_level = { *self.last_recorded_level.read().await };
        if let Ok(next_level) = last_recorded_level.next() {
            // used_space_ratio is a heavy task that's why we don't do it all the time
            let used_space_ratio = self.used_space.ratio();
            let used_space_level = 10.0 * used_space_ratio;
            // every level represents 10 percentage points
            if used_space_level as u8 >= next_level.value() {
                debug!("Next level for storage has been reached");
                *self.last_recorded_level.write().await = next_level;
                return Ok(Some(next_level));
            }
        }

        Ok(None)
    }

    // Query the local store and return NodeQueryResponse
    pub(crate) async fn query(&self, query: &DataQuery, requester: User) -> NodeQueryResponse {
        match query {
            DataQuery::GetChunk(addr) => self.chunks.get(addr).await,
            DataQuery::Register(read) => self.registers.read(read, requester).await,
        }
    }

    /// --- System calls ---

    /// Stores data that Elders sent to it for replication.
    /// Chunk should already have network authority
    /// TODO: define what authority is needed here...
    pub(crate) async fn store_for_replication(
        &self,
        data: &ReplicatedData,
    ) -> Result<Option<StorageLevel>> {
        debug!("Trying to store for replication: {:?}", data.name());
        self.store(data).await
    }

    // Read data from local store
    pub(crate) async fn get_for_replication(
        &self,
        address: &DataAddress,
    ) -> Result<ReplicatedData> {
        match address {
            DataAddress::Chunk(addr) => {
                self.chunks.get_chunk(addr).await.map(ReplicatedData::Chunk)
            }
            DataAddress::Register(addr) => self
                .registers
                .get_register_replica(addr)
                .await
                .map(ReplicatedData::RegisterLog),
        }
    }

    pub(crate) async fn remove(&self, address: &DataAddress) -> Result<()> {
        match address {
            DataAddress::Chunk(addr) => self.chunks.remove_chunk(addr).await,
            DataAddress::Register(addr) => self.registers.remove_register(addr).await,
        }
    }

    async fn keys(&self) -> Result<Vec<DataAddress>> {
        let chunk_keys = self.chunks.keys()?.into_iter().map(DataAddress::Chunk);
        let reg_keys = self
            .registers
            .keys()
            .await?
            .into_iter()
            .map(DataAddress::Register);
        Ok(reg_keys.chain(chunk_keys).collect())
    }
}

impl Core {
    #[allow(clippy::mutable_key_type)]
    pub(crate) async fn reorganize_data(
        &self,
        our_name: XorName,
        new_adults: BTreeSet<XorName>,
        lost_adults: BTreeSet<XorName>,
        remaining: BTreeSet<XorName>,
    ) -> Result<Vec<Command>> {
        let data = self.data_storage.clone();
        let keys = data.keys().await?;
        let mut data_for_replication = BTreeMap::new();
        for addr in keys.iter() {
            if let Some((data, holders)) = self
                .republish_and_cache(addr, &our_name, &new_adults, &lost_adults, &remaining)
                .await
            {
                let _prev = data_for_replication.insert(data.name(), (data, holders));
            }
        }

        let mut commands = vec![];
        let section_pk = self.network_knowledge.section_key().await;
        for (_, (data, targets)) in data_for_replication {
            for name in targets {
                commands.push(Command::PrepareNodeMsgToSend {
                    msg: SystemMsg::NodeCmd(NodeCmd::ReplicateData(data.clone())),
                    dst: crate::messaging::DstLocation::Node { name, section_pk },
                })
            }
        }

        Ok(commands)
    }

    // on adults
    async fn republish_and_cache(
        &self,
        address: &DataAddress,
        our_name: &XorName,
        new_adults: &BTreeSet<XorName>,
        lost_adults: &BTreeSet<XorName>,
        remaining: &BTreeSet<XorName>,
    ) -> Option<(ReplicatedData, BTreeSet<XorName>)> {
        let storage = self.data_storage.clone();

        let old_adult_list = remaining.union(lost_adults).copied().collect();
        let new_adult_list = remaining.union(new_adults).copied().collect();
        let new_holders = self.compute_holders(address, &new_adult_list);
        let old_holders = self.compute_holders(address, &old_adult_list);

        let we_are_not_holder_anymore = !new_holders.contains(our_name);
        let new_adult_is_holder = !new_holders.is_disjoint(new_adults);
        let lost_old_holder = !old_holders.is_disjoint(lost_adults);

        if we_are_not_holder_anymore || new_adult_is_holder || lost_old_holder {
            info!("Republishing data at {:?}", address);
            trace!("We are not a holder anymore? {}, New Adult is Holder? {}, Lost Adult was holder? {}", we_are_not_holder_anymore, new_adult_is_holder, lost_old_holder);
            let data = storage.get_for_replication(address).await.ok()?;
            if we_are_not_holder_anymore {
                if let Err(err) = storage.remove(address).await {
                    warn!("Error deleting data during republish: {:?}", err);
                }
            }
            // TODO: Push to LRU cache
            Some((data, new_holders))
        } else {
            None
        }
    }
}
