// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::data::{ChunkWrite, CmdError, DataCmd, Error, RegisterWrite};
use super::payment::{PaymentReceipt, RegisterPayment};
use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// Commands.
///
/// See the [`types`] module documentation for more details of the types supported by the Safe
/// Network, and their semantics.
///
/// [`types`]: crate::types
#[allow(clippy::large_enum_variant)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub enum Cmd {
    /// [`Data`] write operation (to be deprecated).
    ///
    /// [`Data`]: crate::types
    Data(DataCmd),
    /// [`BatchedWrites`] operation (with Dbcs).
    ///
    /// [`BatchedWrites`]: crate::messaging::cmd
    BatchData(BatchedWrites),
    /// [`Payment`] registration.
    ///
    /// [`Payment`]: crate::messaging::payment::RegisterPayment
    Payment(RegisterPayment),
}

impl Cmd {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// command variant.
    pub fn error(&self, error: Error) -> CmdError {
        match self {
            Self::Data(c) => c.error(error),
            Self::BatchData(c) => unimplemented!(),
            Self::Payment(c) => c.error(error),
        }
    }

    /// Returns the address of the destination for command.
    pub fn dst_address(&self) -> XorName {
        match self {
            Self::Data(c) => c.dst_address(),
            Self::BatchData(c) => unimplemented!(),
            Self::Payment(c) => c.dst_address(),
        }
    }
}

/// Data commands - creating, updating, or removing data.
///
/// The provided data must match the name and bytes specified
/// in the quote.
/// Also the quote must be signed by a known section key (this is at DbcSection).
/// It is then guaranteed to be accepted (at DataSection), if payment provided
/// matches the quote, and the dbcs are valid.
///
/// [`types`]: crate::types
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct BatchedWrites {
    /// [`Chunk`] write operation.
    ///
    /// [`Chunk`]: crate::types::Chunk
    pub uploads: Vec<ChunkWrite>,
    /// [`Register`] write operation.
    ///
    /// [`Register`]: crate::types::register::Register
    pub edits: Vec<RegisterWrite>,
    /// [`Payment`] receipt.
    ///
    /// [`Payment`]: crate::messaging::payment::PaymentReceipt
    pub payment: PaymentReceipt,
}

impl BatchedWrites {
    /// Returns the address of the destination for command.
    pub fn dst_address(&self) -> XorName {
        unimplemented!()
    }
}
