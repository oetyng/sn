// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(test)]
mod tests;
mod wal;

use crate::{client::Error, types::{Chunk, ChunkAddress, PrivateChunk, PublicChunk, PublicKey}};
use bincode::serialize;
use self_encryption::{DataMap, SelfEncryptor};
use wal::FileWAL;

use super::blob_apis::DataMapLevel;

///
pub struct FileManager {
    wal: FileWAL,
}

impl FileManager {
    pub async fn encrypt(&self, mut file: Vec<u8>, owner: Option<PublicKey>) -> Result<(DataMap, ChunkAddress), Error> {
        let mut is_original_data = true;

        let (data_map, head_chunk) = loop {
            let encryptor = SelfEncryptor::new(self.wal.clone(), DataMap::None).map_err(Error::from)?;
                
            encryptor
                .write(&file, 0)
                .await
                .map_err(Error::SelfEncryption)?;
            let (data_map, _) = encryptor
                .close()
                .await
                .map_err(Error::SelfEncryption)?;
    
            let chunk_content = if is_original_data {
                is_original_data = false;
                serialize(&DataMapLevel::Root(data_map.clone()))?
            } else {
                serialize(&DataMapLevel::Child(data_map.clone()))?
            };
    
            let chunk: Chunk = if let Some(owner) = owner {
                PrivateChunk::new(chunk_content, owner).into()
            } else {
                PublicChunk::new(chunk_content).into()
            };
    
            // If the chunk (data map) is bigger than 1MB we need to break it down
            if chunk.validate_size() {
                break (data_map, chunk);
            } else {
                file = serialize(&chunk)?;
            }
        };
    
        Ok((data_map, *head_chunk.address()))
    }
}
