// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::payment::ChargedOps;
use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// Command messages for data or transfer operations
#[allow(clippy::large_enum_variant)]
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Cmd {
    /// Commands for manipulating data
    Debitable(ChargedOps),
}

impl Cmd {
    /// Returns the address of the destination for `cuest`.
    pub fn dst_address(&self) -> XorName {
        use Cmd::*;
        match self {
            Debitable(_cmd) => XorName::random(), // cmd.dst_address(),
        }
    }
}
