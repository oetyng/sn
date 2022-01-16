// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// https://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use super::{
    super::{
        RegisterAddress as Address, {Error, Result},
    },
    CrdtOperation, Entry, EntryHash, User,
};
use crdts::{merkle_reg::MerkleReg, CmRDT};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fmt::{self, Debug, Display, Formatter},
    hash::Hash,
};

/// Register data type as a CRDT without Access Control
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
pub(super) struct RegisterCrdt {
    /// Address on the network of this piece of data
    address: Address,
    /// CRDT to store the actual data, i.e. the items of the Register.
    data: MerkleReg<Entry>,
}

impl Display for RegisterCrdt {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "(")?;
        for (i, entry) in self.data.read().values().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "<{:?}>", entry,)?;
        }
        write!(f, ")")
    }
}

impl RegisterCrdt {
    /// Constructs a new 'RegisterCrdt'.
    pub(super) fn new(address: Address) -> Self {
        Self {
            address,
            data: MerkleReg::new(),
        }
    }

    /// Returns the address.
    pub(super) fn address(&self) -> &Address {
        &self.address
    }

    /// Returns total number of items in the register.
    pub(super) fn size(&self) -> u64 {
        (self.data.num_nodes() + self.data.num_orphans()) as u64
    }

    /// Write a new entry to the RegisterCrdt, returning the hash
    /// of the entry and the CRDT operation without a signature
    pub(super) fn write(
        &mut self,
        entry: Entry,
        children: BTreeSet<EntryHash>,
        source: User,
    ) -> Result<(EntryHash, CrdtOperation<Entry>)> {
        let address = *self.address();

        let children_array: BTreeSet<[u8; 32]> = children.iter().map(|itr| itr.0).collect();
        let crdt_op = self.data.write(entry, children_array);
        self.data.apply(crdt_op.clone());
        let hash = crdt_op.hash();

        // We return the operation as it may need to be broadcasted to other replicas
        let op = CrdtOperation {
            address,
            crdt_op,
            source,
            signature: None,
        };

        Ok((EntryHash(hash), op))
    }

    /// Apply a remote data CRDT operation to this replica of the RegisterCrdt.
    pub(super) fn apply_op(&mut self, op: CrdtOperation<Entry>) -> Result<()> {
        // Let's first check the op is validly signed.
        // Note: Perms and valid sig for the op are checked at the upper Register layer.

        // Check the targetting address is correct
        if self.address != op.address {
            return Err(Error::CrdtWrongAddress(op.address));
        }

        // Apply the CRDT operation to the Register
        self.data.apply(op.crdt_op);

        Ok(())
    }

    /// Get the entry corresponding to the provided `hash` if it exists.
    pub(super) fn get(&self, hash: EntryHash) -> Option<&Entry> {
        self.data.node(hash.0).map(|node| &node.value)
    }

    /// Read current entries (multiple entries occur on concurrent writes).
    pub(super) fn read(&self) -> BTreeSet<(EntryHash, Entry)> {
        self.data
            .read()
            .hashes_and_nodes()
            .map(|(hash, node)| (EntryHash(hash), node.value.clone()))
            .collect()
    }
}
