// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{Error, Result, ToDbKey};
use crate::types::utils::{deserialise, serialise};
use serde::{de::DeserializeOwned, Serialize};
use sled::{Db, Tree};
use std::{fmt::Debug, marker::PhantomData};
use xor_name::XorName;

/// Disk storage for events and similar.
#[derive(Clone, Debug)]
pub(crate) struct EventStore<TEvent: Debug + Serialize + DeserializeOwned> {
    tree: Tree,
    db_name: String,
    _phantom: PhantomData<TEvent>,
}

impl<'a, TEvent: Debug + Serialize + DeserializeOwned> EventStore<TEvent>
where
    TEvent: 'a,
{
    pub(crate) fn new(id: XorName, db: Db) -> Result<Self> {
        let db_name = id.to_db_key()?;
        let tree = db.open_tree(&db_name)?;
        Ok(Self {
            tree,
            db_name,
            _phantom: PhantomData::default(),
        })
    }

    /// Get all events stored in db
    pub(crate) fn get_all(&self) -> Result<Vec<TEvent>> {
        let iter = self.tree.iter();

        let mut events = vec![];
        for (_, res) in iter.enumerate() {
            let (key, val) = res?;
            let db_key = String::from_utf8(key.to_vec())
                .map_err(|_| Error::CouldNotParseDbKey(key.to_vec()))?;

            let value: TEvent = deserialise(&val)?;
            events.push((db_key, value))
        }

        events.sort_by(|(key_a, _), (key_b, _)| key_a.partial_cmp(key_b).unwrap());

        let events: Vec<TEvent> = events.into_iter().map(|(_, val)| val).collect();

        Ok(events)
    }

    /// append a new entry
    pub(crate) fn append(&mut self, event: TEvent) -> Result<()> {
        let key = &self.tree.len().to_string();
        if self.tree.get(key)?.is_some() {
            return Err(Error::InvalidOperation(format!(
                "Key exists: {}. Event: {:?}",
                key, event
            )));
        }

        let event = serialise(&event)?;
        let _ = self.tree.insert(key, event).map_err(Error::Sled)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::EventStore;
    use crate::node::{Error, Result};
    use crate::types::Token;
    use std::path::Path;
    use tempfile::tempdir;

    #[tokio::test]
    async fn history() -> Result<()> {
        let id = xor_name::XorName::random();
        let tmp_dir = tempdir()?;
        let db_dir = tmp_dir.into_path().join(Path::new(&"Token".to_string()));
        let db = sled::open(db_dir).map_err(|error| {
            trace!("Sled Error: {:?}", error);
            Error::Sled(error)
        })?;
        let mut store = EventStore::<Token>::new(id, db)?;

        store.append(Token::from_nano(10))?;

        let events = store.get_all()?;
        assert_eq!(events.len(), 1);

        match events.get(0) {
            Some(token) => assert_eq!(token.as_nano(), 10),
            None => unreachable!(),
        }

        Ok(())
    }
}
