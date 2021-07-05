// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{data::DataQuery, payment::CostInquiry};
use xor_name::XorName;

use serde::{Deserialize, Serialize};

/// Data queries - retrieving data and inspecting their structure.
///
/// See the [`types`] module documentation for more details of the types supported by the Safe
/// Network, and their semantics.
///
/// [`types`]: crate::types
#[allow(clippy::large_enum_variant)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub enum Query {
    /// [`Data`] read operation.
    ///
    /// [`Data`]: crate::types
    Data(DataQuery),
    /// [`Payment`] read operation.
    ///
    /// [`Payment`]: crate::types::register::Register
    Payment(CostInquiry),
}

impl Query {
    // /// Creates a Response containing an error, with the Response variant corresponding to the
    // /// Request variant.
    // pub fn error(&self, error: Error) -> QueryResponse {
    //     use DataQuery::*;
    //     match self {
    //         Blob(q) => q.error(error),
    //         Register(q) => q.error(error),
    //     }
    // }

    /// Returns the address of the destination for `request`.
    pub fn dst_address(&self) -> XorName {
        use Query::*;
        match self {
            Data(q) => q.dst_address(),
            Payment(q) => q.dst_address(),
        }
    }
}
