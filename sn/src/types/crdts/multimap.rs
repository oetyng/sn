// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// https://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use super::{
    Action, EntryHash, Error, MultimapCrdt, MultimapKeyValue, MultimapKeyValues, Permissions,
    Policy, Result, User,
};
use crate::{types::RegisterAddress as Address, types::Scope};

use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, hash::Hash};
use xor_name::XorName;

/// Object storing the Multimap
#[derive(Clone, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub struct Multimap {
    crdt: MultimapCrdt,
    policy: Policy,
    cap: u16,
}

impl Multimap {
    /// Create a Multimap
    pub fn new(name: XorName, tag: u64, policy: Policy, cap: u16) -> Self {
        let address = if matches!(policy, Policy::Public(_)) {
            Address::Public { name, tag }
        } else {
            Address::Private { name, tag }
        };
        Self {
            crdt: MultimapCrdt::new(address),
            policy,
            cap,
        }
    }

    /// Return `true` if public.
    pub fn is_public(&self) -> bool {
        self.address().is_public()
    }

    /// Return `true` if private.
    pub fn is_private(&self) -> bool {
        self.address().is_private()
    }

    /// Return the address.
    pub fn address(&self) -> &Address {
        self.crdt.address()
    }

    /// Return the scope.
    pub fn scope(&self) -> Scope {
        self.address().scope()
    }

    /// Return the name.
    pub fn name(&self) -> &XorName {
        self.address().name()
    }

    /// Return the tag.
    pub fn tag(&self) -> u64 {
        self.address().tag()
    }

    /// Return the owner of the data.
    pub fn owner(&self) -> User {
        *self.policy.owner()
    }

    /// Return the max number of items that can be held in the register.
    pub fn cap(&self) -> u16 {
        self.cap
    }

    /// Return the number of items held in the register
    pub fn size(&self) -> u64 {
        self.crdt.size()
    }

    /// Return true if the register is empty.
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Return user permissions, if applicable.
    pub fn permissions(&self, user: User) -> Result<Permissions> {
        self.policy.permissions(user).ok_or(Error::NoSuchEntry)
    }

    /// Return the policy.
    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Increment the size cap of the register, returning the previous value.
    pub fn increment_cap(&mut self, add: u16) {
        self.cap += add;
    }

    /// Insert a key-value pair into a Multimap on the network
    pub fn insert(
        &mut self,
        entry: MultimapKeyValue,
        replace: BTreeSet<EntryHash>,
        requester: User,
    ) -> Result<EntryHash> {
        //debug!("Inserting '{:?}' into Multimap at {}", entry, multimap_url);
        self.crdt.insert(entry, replace, requester)
    }

    /// Remove entries from a Multimap on the network
    /// This tombstones the removed entries, effectively hiding them if they where the latest
    /// Note that they are still stored on the network as history is kept,
    /// and you can still access them with their EntryHash
    pub fn remove(&mut self, to_remove: BTreeSet<EntryHash>, requester: User) -> Result<EntryHash> {
        //debug!("Removing from Multimap at {}: {:?}", url, to_remove);
        self.crdt.remove(to_remove, requester)
    }

    /// Return the value of a Multimap on the network corresponding to the key provided
    pub fn get_by_key(&self, key: &[u8]) -> Result<MultimapKeyValues> {
        self.crdt.get_by_key(key)
    }

    /// Return the value of a Multimap on the network corresponding to the hash provided
    pub fn get_by_hash(&self, hash: EntryHash) -> Result<MultimapKeyValue> {
        self.crdt.get_by_hash(hash)
    }

    /// Return the values of a Multimap
    pub fn get_values(&self) -> Result<MultimapKeyValues> {
        self.crdt.get_values()
    }

    /// To check permissions for given `action`
    /// for the given requester's public key.
    ///
    /// Returns:
    /// `Ok(())` if the permissions are valid,
    /// `Err::AccessDenied` if the action is not allowed.
    pub fn check_permissions(&self, action: Action, requester: User) -> Result<()> {
        self.policy.is_action_allowed(requester, action)
    }
}
