// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// https://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

mod map_crdt;
mod multimap;
mod policy;
mod reg_crdt;
mod register;

pub use multimap::Multimap;
pub use policy::{
    Permissions, Policy, PrivatePermissions, PrivatePolicy, PublicPermissions, PublicPolicy, User,
};
pub use register::Register;

use super::{Error, Result, Signature};
use crate::types::RegisterAddress as Address;

use crdts::merkle_reg::Node;
use map_crdt::MultimapCrdt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    hash::Hash,
};

///
pub type MultimapKey = Vec<u8>;
///
pub type MultimapValue = Vec<u8>;
///
pub type MultimapKeyValue = (MultimapKey, MultimapValue);
///
pub type MultimapKeyValues = BTreeSet<(EntryHash, MultimapKeyValue)>;

/// Pseudo-arbitrary maximum size of a register entry.
/// (there are considerations taken)
pub const MAX_REG_ENTRY_SIZE: usize = 1024;

/// Register mutation operation to apply to Register.
pub type RegisterOp<T> = CrdtOperation<T>;

/// An action on Register data type.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum Action {
    /// Read from the data.
    Read,
    /// Write to the data.
    Write,
}

/// An entry in a Register (note that the vec<u8> is size limited: MAX_REG_ENTRY_SIZE)
pub type Entry = Vec<u8>;

/// Hash of the register entry. Logging as the same format of XorName.
#[derive(Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntryHash(pub crdts::merkle_reg::Hash);

impl Debug for EntryHash {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        write!(formatter, "{}", self)
    }
}

impl Display for EntryHash {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        write!(
            formatter,
            "{:02x}{:02x}{:02x}..",
            self.0[0], self.0[1], self.0[2]
        )
    }
}

/// CRDT Data operation applicable to other Register replica.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrdtOperation<T> {
    /// Address of a Register object on the network.
    pub address: Address,
    /// The data operation to apply.
    pub crdt_op: Node<T>,
    /// The PublicKey of the entity that generated the operation
    pub source: User,
    /// The signature of source on the crdt_top, required to apply the op
    pub signature: Option<Signature>,
}
