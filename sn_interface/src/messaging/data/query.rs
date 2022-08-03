// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    chunk_operation_id, register::RegisterQuery, spentbook::SpentbookQuery, Error, OperationId,
    QueryResponse, Result,
};
use crate::types::{ChunkAddress, ReplicatedDataAddress, SpentbookAddress};
use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// A query for requesting (meta)data at a particular adult.
#[derive(Hash, Eq, PartialEq, PartialOrd, Clone, Serialize, Deserialize, Debug)]
pub struct TargetedDataQuery {
    /// The actual query, e.g. retrieving a Chunk or Register
    pub query: DataQuery,
    /// nth closest adult (XOR distance) to query for data
    pub target_adult_index: usize,
}

impl TargetedDataQuery {
    /// Returns the xorname of the data destination.
    pub fn dst_name(&self) -> XorName {
        self.query.dst_name()
    }
}

/// Data queries - retrieving data and inspecting their structure.
///
/// See the [`types`] module documentation for more details of the types supported by the Safe
/// Network, and their semantics.
///
/// [`types`]: crate::types
#[allow(clippy::large_enum_variant)]
#[derive(Hash, Eq, PartialEq, PartialOrd, Clone, Serialize, Deserialize, Debug)]
pub enum DataQuery {
    #[cfg(feature = "chunks")]
    /// Retrieve a [`Chunk`] at the given address.
    ///
    /// This should eventually lead to a [`GetChunk`] response.
    ///
    /// [`Chunk`]:  crate::types::Chunk
    /// [`GetChunk`]: QueryResponse::GetChunk
    GetChunk(ChunkAddress),
    #[cfg(feature = "registers")]
    /// [`Register`] read operation.
    ///
    /// [`Register`]: crate::types::register::Register
    Register(RegisterQuery),
    #[cfg(feature = "spentbook")]
    /// Spentbook read operation.
    Spentbook(SpentbookQuery),
}

impl DataQuery {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// Request variant.
    pub fn error(&self, error: Error) -> Result<QueryResponse> {
        use DataQuery::*;
        match self {
            #[cfg(feature = "chunks")]
            GetChunk(_) => Ok(QueryResponse::GetChunk(Err(error))),
            #[cfg(feature = "registers")]
            Register(q) => q.error(error),
            #[cfg(feature = "spentbook")]
            Spentbook(q) => q.error(error),
        }
    }

    /// Returns the xorname of the data destination for `request`.
    pub fn dst_name(&self) -> XorName {
        use DataQuery::*;
        match self {
            #[cfg(feature = "chunks")]
            GetChunk(address) => *address.name(),
            #[cfg(feature = "registers")]
            Register(q) => q.dst_name(),
            #[cfg(feature = "spentbook")]
            Spentbook(q) => q.dst_name(),
        }
    }

    /// Returns the address of the data
    pub fn address(&self) -> ReplicatedDataAddress {
        match self {
            #[cfg(feature = "chunks")]
            DataQuery::GetChunk(address) => ReplicatedDataAddress::Chunk(*address),
            #[cfg(feature = "registers")]
            DataQuery::Register(read) => ReplicatedDataAddress::Register(read.dst_address()),
            #[cfg(feature = "spentbook")]
            DataQuery::Spentbook(read) => {
                ReplicatedDataAddress::Spentbook(SpentbookAddress::new(*read.dst_address().name()))
            }
        }
    }

    /// Retrieves the operation identifier for this response, use in tracking node liveness
    /// and responses at clients.
    /// Must be the same as the query response
    /// Right now returning result to fail for anything non-chunk, as that's all we're tracking from other nodes here just now.
    pub fn operation_id(&self) -> Result<OperationId> {
        match self {
            #[cfg(feature = "chunks")]
            DataQuery::GetChunk(address) => chunk_operation_id(address),
            #[cfg(feature = "registers")]
            DataQuery::Register(read) => read.operation_id(),
            #[cfg(feature = "spentbook")]
            DataQuery::Spentbook(read) => read.operation_id(),
        }
    }
}
