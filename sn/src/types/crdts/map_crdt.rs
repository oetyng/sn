// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use super::{reg_crdt::RegisterCrdt, Entry, EntryHash, MultimapKeyValue, MultimapKeyValues, User};
use crate::types::{Error, RegisterAddress as Address, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MULTIMAP_REMOVED_MARK: &[u8] = b"";

///
#[derive(Clone, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub(super) struct MultimapCrdt {
    register: RegisterCrdt,
}

impl MultimapCrdt {
    /// Create a Multimap
    pub(super) fn new(address: Address) -> Self {
        Self {
            register: RegisterCrdt::new(address),
        }
    }

    /// Returns the address.
    pub(super) fn address(&self) -> &Address {
        self.register.address()
    }

    /// Returns total number of items in the register.
    pub(super) fn size(&self) -> u64 {
        self.register.size()
    }

    /// Insert a key-value pair into a Multimap on the network
    pub(super) fn insert(
        &mut self,
        entry: MultimapKeyValue,
        replace: BTreeSet<EntryHash>,
        requester: User,
    ) -> Result<EntryHash> {
        //debug!("Inserting '{:?}' into Multimap at {}", entry, multimap_url);
        let serialised_entry = rmp_serde::to_vec_named(&entry).map_err(|err| {
            Error::Serialisation(format!(
                "Couldn't serialise the Multimap entry '{:?}': {:?}",
                entry, err
            ))
        })?;

        let entry: Entry = serialised_entry.to_vec();

        let (entry_hash, op) = self.register.write(entry, replace, requester)?;

        self.register.apply_op(op)?;

        Ok(entry_hash)
    }

    /// Remove entries from a Multimap on the network
    /// This tombstones the removed entries, effectively hiding them if they where the latest
    /// Note that they are still stored on the network as history is kept,
    /// and you can still access them with their EntryHash
    pub(super) fn remove(
        &mut self,
        to_remove: BTreeSet<EntryHash>,
        requester: User,
    ) -> Result<EntryHash> {
        //debug!("Removing from Multimap at {}: {:?}", url, to_remove);

        let (entry_hash, op) =
            self.register
                .write(MULTIMAP_REMOVED_MARK.to_vec(), to_remove, requester)?;

        self.register.apply_op(op)?;

        Ok(entry_hash)
    }

    /// Return the value of a Multimap on the network corresponding to the key provided
    pub(super) fn get_by_key(&self, key: &[u8]) -> Result<MultimapKeyValues> {
        let entries = self.get_values()?;
        Ok(entries
            .into_iter()
            .filter(|(_, (entry_key, _))| entry_key == key)
            .collect())
    }

    /// Return the value of a Multimap on the network corresponding to the hash provided
    pub(super) fn get_by_hash(&self, hash: EntryHash) -> Result<MultimapKeyValue> {
        let entry = self.register.get(hash).ok_or(Error::NoSuchEntry)?;

        // We parse the entry in the Register as a 'MultimapKeyValue'
        if entry == MULTIMAP_REMOVED_MARK {
            Err(Error::NoSuchEntry)
        } else {
            let key_val = Self::decode_multimap_entry(entry)?;
            Ok(key_val)
        }
    }

    // Crate's helper to return the value of a Multimap on
    // the network without resolving the SafeUrl,
    // filtering by hash if a version is provided
    pub(super) fn get_values(&self) -> Result<MultimapKeyValues> {
        let entries = self.register.read();

        // We parse each entry in the Register as a 'MultimapKeyValue'
        let mut multimap_key_vals = MultimapKeyValues::new();
        for (hash, entry) in entries.iter() {
            if entry == MULTIMAP_REMOVED_MARK {
                // this is a tombstone entry created to delete some old entries
                continue;
            }
            let key_val = Self::decode_multimap_entry(entry)?;
            let _ = multimap_key_vals.insert((*hash, key_val));
        }
        Ok(multimap_key_vals)
    }

    fn decode_multimap_entry(entry: &[u8]) -> Result<MultimapKeyValue> {
        rmp_serde::from_slice(entry).map_err(|err| {
            Error::FailedToParse(format!("Couldn't parse Multimap entry: {:?}", err))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use xor_name::XorName;

    #[tokio::test]
    async fn test_new() -> Result<()> {
        let name = XorName::random();
        let tag = 25_000;

        let map = MultimapCrdt::new(Address::Public { name, tag });

        let key = b"".to_vec();
        let received_data = map.get_by_key(&key)?;

        assert_eq!(received_data, Default::default());

        Ok(())
    }

    #[tokio::test]
    async fn test_multimap_insert() -> Result<()> {
        let key = b"key".to_vec();
        let val = b"value".to_vec();
        let key_val = (key.clone(), val);

        let val2 = b"value2".to_vec();
        let key_val2 = (key.clone(), val2);

        let name = XorName::random();
        let tag = 25_000;
        let requester = User::Anyone;

        let mut map = MultimapCrdt::new(Address::Public { name, tag });

        let _ = map.get_by_key(&key)?;

        let hash = map.insert(key_val.clone(), BTreeSet::new(), requester)?;

        let received_data = map.get_by_key(&key)?;

        assert_eq!(received_data, vec![(hash, key_val)].into_iter().collect());

        // Let's now test an insert which replace the previous value for a key
        let hashes_to_replace = vec![hash].into_iter().collect();
        let hash2 = map.insert(key_val2.clone(), hashes_to_replace, requester)?;

        let received_data = map.get_by_key(&key)?;

        assert_eq!(received_data, vec![(hash2, key_val2)].into_iter().collect());

        Ok(())
    }

    #[tokio::test]
    async fn test_multimap_get_by_hash() -> Result<()> {
        let key = b"key".to_vec();
        let val = b"value".to_vec();
        let key_val = (key, val);
        let key2 = b"key2".to_vec();
        let val2 = b"value2".to_vec();
        let key_val2 = (key2, val2);

        let name = XorName::random();
        let tag = 25_000;
        let requester = User::Anyone;

        let mut map = MultimapCrdt::new(Address::Public { name, tag });

        let hash = map.insert(key_val.clone(), BTreeSet::new(), requester)?;
        let hash2 = map.insert(key_val2.clone(), BTreeSet::new(), requester)?;

        let received_data = map.get_by_hash(hash)?;

        assert_eq!(received_data, key_val);

        let received_data = map.get_by_hash(hash2)?;

        assert_eq!(received_data, key_val2);

        Ok(())
    }

    #[tokio::test]
    async fn test_multimap_remove() -> Result<()> {
        let key = b"key".to_vec();
        let val = b"value".to_vec();
        let key_val = (key.clone(), val.clone());

        let name = XorName::random();
        let tag = 25_000;
        let requester = User::Anyone;

        let mut map = MultimapCrdt::new(Address::Public { name, tag });

        let hash = map.insert(key_val, BTreeSet::new(), requester)?;

        let received_data = map.get_by_key(&key)?;

        assert_eq!(received_data.len(), 1);
        let (read_hash, read_key_val) = received_data.into_iter().next().unwrap();
        assert_eq!((read_hash, read_key_val), (hash, (key.clone(), val)));

        let hashes_to_remove = vec![hash].into_iter().collect();
        let removed_mark_hash = map.remove(hashes_to_remove, requester)?;
        assert_ne!(removed_mark_hash, hash);

        assert_eq!(map.get_by_key(&key)?, Default::default());

        Ok(())
    }
}
