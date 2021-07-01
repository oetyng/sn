// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod data_store;

use super::node_ops::{MsgType, OutgoingMsg};
use crate::node::{
    node_ops::{NodeDuties, NodeDuty},
    Result,
};
use crate::types::{Chunk, ChunkAddress, PublicKey};
use crate::{
    messaging::{
        client::{ChunkRead, ChunkWrite, Error as ErrorMessage},
        node::{NodeDataQueryResponse, NodeMsg, NodeQueryResponse},
        Aggregation, DstLocation, MessageId,
    },
    node::Error,
    types::DataAddress,
};
use data_store::{Data, DataId, DataStore, Subdir};
use std::{
    fmt::{self, Display, Formatter},
    path::Path,
};
use tracing::info;

pub use data_store::UsedSpace;

/// At 50% full, the node will report that it's reaching full capacity.
pub const MAX_STORAGE_USAGE_RATIO: f64 = 0.5;

impl Data for Chunk {
    type Id = ChunkAddress;
    fn id(&self) -> &Self::Id {
        match self {
            Chunk::Public(ref chunk) => chunk.address(),
            Chunk::Private(ref chunk) => chunk.address(),
        }
    }
}

impl DataId for ChunkAddress {
    fn to_data_address(&self) -> DataAddress {
        DataAddress::Chunk(*self)
    }
}

impl Subdir for DataStore<Chunk> {
    fn subdir() -> &'static Path {
        Path::new("chunks")
    }
}

/// Operations on data chunks.
pub(crate) struct ChunkStore {
    store: DataStore<Chunk>,
}

impl ChunkStore {
    pub async fn new(path: &Path, max_capacity: u64) -> Result<Self> {
        Ok(Self {
            store: DataStore::<Chunk>::new(path, max_capacity).await?,
        })
    }

    pub async fn keys(&self) -> Result<Vec<ChunkAddress>> {
        self.store.keys().await
    }

    pub async fn remove_chunk(&mut self, address: &ChunkAddress) -> Result<()> {
        self.store.delete(address).await
    }

    pub async fn get_chunk(&self, address: &ChunkAddress) -> Result<Chunk> {
        self.store.get(address).await
    }

    pub async fn read(&self, read: &ChunkRead, msg_id: MessageId) -> NodeDuty {
        let ChunkRead::Get(address) = read;
        let result = self
            .get_chunk(address)
            .await
            .map_err(|_| ErrorMessage::DataNotFound(DataAddress::Chunk(*address)));

        NodeDuty::Send(OutgoingMsg {
            msg: MsgType::Node(NodeMsg::NodeQueryResponse {
                response: NodeQueryResponse::Data(NodeDataQueryResponse::GetChunk(result)),
                id: MessageId::in_response_to(&msg_id),
                correlation_id: msg_id,
            }),
            section_source: false, // sent as single node
            dst: DstLocation::Section(*address.name()),
            aggregation: Aggregation::None,
        })
    }

    pub async fn write(
        &mut self,
        write: &ChunkWrite,
        msg_id: MessageId,
        requester: PublicKey,
    ) -> Result<NodeDuty> {
        match &write {
            ChunkWrite::New(data) => self.try_store(&data).await,
            ChunkWrite::DeletePrivate(head_address) => {
                if !self.store.has(&head_address).await {
                    info!(
                        "{}: Immutable chunk doesn't exist: {:?}",
                        self, head_address
                    );
                    return Ok(NodeDuty::NoOp);
                }

                match self.store.get(&head_address).await {
                    Ok(Chunk::Private(data)) => {
                        if data.owner() == &requester {
                            self.store
                                .delete(&head_address)
                                .await
                                .map_err(|_error| ErrorMessage::FailedToDelete)
                        } else {
                            Err(ErrorMessage::InvalidOwners(requester))
                        }
                    }
                    Ok(_) => {
                        error!(
                            "{}: Invalid DeletePrivate(Chunk::Public) encountered: {:?}",
                            self, msg_id
                        );
                        Err(ErrorMessage::InvalidOperation(format!(
                            "{}: Invalid DeletePrivate(Chunk::Public) encountered: {:?}",
                            self, msg_id
                        )))
                    }
                    _ => Err(ErrorMessage::NoSuchKey),
                }?;

                Ok(NodeDuty::NoOp)
            }
        }
    }

    async fn try_store(&mut self, data: &Chunk) -> Result<NodeDuty> {
        if self.store.has(data.address()).await {
            info!(
                "{}: Immutable chunk already exists, not storing: {:?}",
                self,
                data.address()
            );
            return Err(Error::DataExists);
        }
        self.store.put(&data).await?;

        Ok(NodeDuty::NoOp)
    }

    pub async fn check_storage(&self) -> Result<NodeDuties> {
        info!("Checking used storage");
        if self.store.used_space_ratio().await > MAX_STORAGE_USAGE_RATIO {
            Ok(NodeDuties::from(NodeDuty::ReachingMaxCapacity))
        } else {
            Ok(vec![])
        }
    }

    /// Stores a chunk that Elders sent to it for replication.
    pub async fn store_for_replication(&mut self, chunk: Chunk) -> Result<NodeDuty> {
        if self.store.has(chunk.address()).await {
            info!(
                "{}: Immutable chunk already exists, not storing: {:?}",
                self,
                chunk.address()
            );
            return Ok(NodeDuty::NoOp);
        }

        self.store.put(&chunk).await?;

        Ok(NodeDuty::NoOp)
    }
}

impl Display for ChunkStore {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "ChunkStore")
    }
}
