// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    client::Error,
    dbs::{DataStore, UsedSpace},
    types::{Chunk, ChunkAddress, PrivateChunk, PublicChunk, PublicKey},
};
use async_trait::async_trait;
use self_encryption::{SelfEncryptionError, Storage};
use std::path::Path;
use tracing::trace;
use xor_name::{XorName, XOR_NAME_LEN};

/// Network storage is the concrete type which self_encryption crate will use
/// to put or get data from the network.
#[derive(Clone)]
pub struct FileWAL {
    db: DataStore<Chunk>,
    owner: Option<PublicKey>,
}

pub struct FileWALConfig {
    pub owner: Option<PublicKey>,
    pub used_space: UsedSpace,
}

impl FileWAL {
    /// Create a new LocalBlobStorage instance.
    pub async fn new(root: &Path, config: FileWALConfig) -> Result<Self, Error> {
        let suffix = if config.owner.is_none() { "public" } else { "private" };
        let sub_dirs = Path::new("wal").join(suffix);

        let db = DataStore::new(root, sub_dirs.as_path(), config.used_space)
            .await
            .map_err(Error::from)?;

        Ok(Self {
            db,
            owner: config.owner,
        })
    }
}

#[async_trait]
impl Storage for FileWAL {
    async fn get(&mut self, name: &[u8]) -> Result<Vec<u8>, SelfEncryptionError> {
        if name.len() != XOR_NAME_LEN {
            return Err(SelfEncryptionError::Generic(
                "Requested `name` is incorrect size.".to_owned(),
            ));
        }

        let name = {
            let mut temp = [0_u8; XOR_NAME_LEN];
            temp.clone_from_slice(name);
            XorName(temp)
        };

        let address = if self.owner.is_none() {
            ChunkAddress::Public(name)
        } else {
            ChunkAddress::Private(name)
        };

        trace!("Self encrypt invoked GetChunk({:?})", &address);

        match self.db.get(&address).await {
            Ok(data) => Ok(data.value().clone()),
            Err(error) => Err(SelfEncryptionError::Generic(format!("{:?}", error))),
        }
    }

    async fn delete(&mut self, name: &[u8]) -> Result<(), SelfEncryptionError> {
        if name.len() != XOR_NAME_LEN {
            return Err(SelfEncryptionError::Generic(
                "Requested `name` is incorrect size.".to_owned(),
            ));
        }

        let name = {
            let mut temp = [0_u8; XOR_NAME_LEN];
            temp.clone_from_slice(name);
            XorName(temp)
        };

        let address = if self.owner.is_none() {
            return Err(SelfEncryptionError::Generic(
                "Cannot delete on a public storage".to_owned(),
            ));
        } else {
            ChunkAddress::Private(name)
        };
        trace!("Self encrypt invoked DeleteBlob({:?})", &address);

        match self.db.delete(&address).await {
            Ok(_) => Ok(()),
            Err(error) => Err(SelfEncryptionError::Generic(format!("{:?}", error))),
        }
    }

    async fn put(&mut self, _: Vec<u8>, data: Vec<u8>) -> Result<(), SelfEncryptionError> {
        let chunk: Chunk = if let Some(owner) = self.owner {
            PrivateChunk::new(data, owner).into()
        } else {
            PublicChunk::new(data).into()
        };
        trace!("Self encrypt invoked StoreChunk({:?})", &chunk);
        self.db
            .put(&chunk)
            .await
            .map_err(|err| SelfEncryptionError::Generic(format!("{:?}", err)))
    }

    async fn generate_address(&self, data: &[u8]) -> Result<Vec<u8>, SelfEncryptionError> {
        let chunk: Chunk = if let Some(owner) = self.owner {
            PrivateChunk::new(data.to_vec(), owner).into()
        } else {
            PublicChunk::new(data.to_vec()).into()
        };
        Ok(chunk.name().0.to_vec())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     #[ignore = "too heavy for CI"]
//     pub async fn basic() -> Result<()> {}
// }
