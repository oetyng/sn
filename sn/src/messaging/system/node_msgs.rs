// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    data::{DataQuery, MetadataExchange, OperationId, QueryResponse, Result, StorageLevel},
    EndUser, MsgId, ServiceAuth,
};
use crate::types::{
    register::{Entry, EntryHash, Permissions, Policy, Register, User},
    Chunk, PublicKey, ReplicatedData, ReplicatedDataAddress,
};

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use xor_name::XorName;

/// cmd message sent among nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeCmd {
    /// Notify Elders on nearing max capacity
    RecordStorageLevel {
        /// Node Id
        node_id: PublicKey,
        /// Section to which the message needs to be sent to. (NB: this is the section of the node id).
        section: XorName,
        /// The storage level reported by the node.
        level: StorageLevel,
    },
    /// Tells an Adult to store a replica of the data
    ReplicateData(Vec<ReplicatedData>),
    /// Tells an Adult to get and replicate given data from the sender
    ReplicateDataAt(Vec<ReplicatedDataAddress>),
    /// Sent to all promoted nodes (also sibling if any) after
    /// a completed transition to a new constellation.
    ReceiveMetadata {
        /// Metadata
        metadata: MetadataExchange,
    },
}

/// Event message sent among nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeEvent {
    /// Sent by a full Adult, and tells the Elders to store a chunk at some other Adult in the section
    CouldNotStoreData {
        /// Node Id
        node_id: PublicKey,
        /// The data that the Adult couldn't store
        data: ReplicatedData,
        /// Whether store failed due to full
        full: bool,
    },
    /// Inform Adults of a possible deviant node
    DeviantsDetected(BTreeSet<XorName>),
}

/// Query originating at a node
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum NodeQuery {
    /// Service msgs are related to the services provided
    /// by the network. Data stored by clients is handled by Adults.
    DataService {
        /// The query
        query: DataQuery,
        /// Client signature
        auth: ServiceAuth,
        /// The user that has initiated this query
        origin: EndUser,
        /// The correlation id that recorded in Elders for this query
        correlation_id: MsgId,
    },
    /// System msgs are related to maintaining the network system.
    System(NodeSystemQuery),
}

/// System msgs are related to maintaining the network system.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeSystemQuery {
    /// Get data that we need to replicate
    GetData(Vec<ReplicatedDataAddress>),
}

/// Responses to queries from Elders to Adults.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeQueryResponse {
    //
    // ===== Chunk =====
    //
    /// Response to [`ChunkRead::Get`].
    GetChunk(Result<Chunk>),
    //
    // ===== Register Data =====
    //
    /// Response to [`RegisterQuery::Get`].
    GetRegister((Result<Register>, OperationId)),
    /// Response to [`RegisterQuery::GetOwner`].
    GetRegisterOwner((Result<User>, OperationId)),
    /// Response to [`RegisterQuery::GetEntry`].
    GetRegisterEntry((Result<Entry>, OperationId)),
    /// Response to [`RegisterQuery::GetPolicy`].
    GetRegisterPolicy((Result<Policy>, OperationId)),
    /// Response to [`RegisterQuery::Read`].
    ReadRegister((Result<BTreeSet<(EntryHash, Entry)>>, OperationId)),
    /// Response to [`RegisterQuery::GetUserPermissions`].
    GetRegisterUserPermissions((Result<Permissions>, OperationId)),
    //
    // ===== Other =====
    //
    /// Failed to create id generation
    FailedToCreateOperationId,
}

impl NodeQueryResponse {
    pub(crate) fn convert(self) -> QueryResponse {
        use NodeQueryResponse::*;
        match self {
            GetChunk(res) => QueryResponse::GetChunk(res),
            GetRegister(res) => QueryResponse::GetRegister(res),
            GetRegisterEntry(res) => QueryResponse::GetRegisterEntry(res),
            GetRegisterOwner(res) => QueryResponse::GetRegisterOwner(res),
            ReadRegister(res) => QueryResponse::ReadRegister(res),
            GetRegisterPolicy(res) => QueryResponse::GetRegisterPolicy(res),
            GetRegisterUserPermissions(res) => QueryResponse::GetRegisterUserPermissions(res),
            FailedToCreateOperationId => QueryResponse::FailedToCreateOperationId,
        }
    }

    pub(crate) fn operation_id(&self) -> Result<OperationId> {
        self.clone().convert().operation_id()
    }
}
