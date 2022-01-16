// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    data::{DataCmd, DataExchange, DataQuery, OperationId, QueryResponse, Result, StorageLevel},
    EndUser, ServiceAuth,
};
use crate::types::{
    crdts::{Entry, EntryHash, Permissions, Policy, Register, User},
    Chunk, PublicKey, ReplicatedData,
};

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use xor_name::XorName;

/// Command message sent among nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeCmd {
    /// Metadata is handled by Elders
    Metadata {
        /// The contained command
        cmd: DataCmd,
        /// Requester pk and signature
        auth: ServiceAuth,
        /// Message source
        origin: EndUser,
    },
    /// Data is stored by Adults
    StoreData {
        /// The data
        data: ReplicatedData,
        /// Message source
        origin: EndUser,
    },
    /// Notify Elders on nearing max capacity
    RecordStorageLevel {
        /// Node Id
        node_id: PublicKey,
        /// Section to which the message needs to be sent to. (NB: this is the section of the node id).
        section: XorName,
        /// The storage level reported by the node.
        level: StorageLevel,
    },
    /// Replicate a given chunk at an Adult (sent from elders on receipt of RepublishData)
    ReplicateData(ReplicatedData),
    /// Tells the Elders to re-publish a chunk in the data section
    RepublishData(ReplicatedData),
    /// Sent to all promoted nodes (also sibling if any) after
    /// a completed transition to a new constellation.
    ReceiveMetadata {
        /// Metadata
        metadata: DataExchange,
    },
}

/// Query originating at a node
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum NodeQuery {
    /// Metadata is handled by Elders
    Metadata {
        /// The actual query message
        query: DataQuery,
        /// Client signature
        auth: ServiceAuth,
        /// The user that has initiated this query
        origin: EndUser,
    },
    /// Data is handled by Adults
    Data {
        /// The query
        query: DataQuery,
        /// Client signature
        auth: ServiceAuth,
        /// The user that has initiated this query
        origin: EndUser,
    },
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
}
