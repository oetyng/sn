// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{Error, Result};
use hex_fmt::HexFmt;
use serde::{Deserialize, Serialize};
use std::fmt;
use tiny_keccak::{Hasher, Sha3};
use xor_name::XorName;

/// Constant byte length of `MessageId`.
pub const MESSAGE_ID_LEN: usize = 32;

/// Unique ID for messages.
///
/// This is used for deduplication: Since the network sends messages redundantly along different
/// routes, the same message will usually arrive more than once at any given node. A message with
/// an ID that is already in the cache will be ignored.
#[derive(
    Ord, PartialOrd, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash, custom_debug::Debug,
)]
pub struct MessageId(#[debug(with = "Self::fmt_bytes")] [u8; MESSAGE_ID_LEN]);

impl MessageId {
    /// Generates a new `MessageId` with random content.
    pub fn new() -> Self {
        // Here we use XorName just as helper to generate a random id
        Self(XorName::random().0)
    }

    /// Generates a new `MessageId` containing provided bytes.
    pub fn with(id: [u8; MESSAGE_ID_LEN]) -> Self {
        Self(id)
    }

    /// Generate a message id deterministically derived from a piece of data.
    pub fn from_content<T: Serialize>(content: &T) -> Result<Self> {
        // Here we use XorName just as a helper to generate the id
        let msg_id = Self(
            XorName::from_content(&[
                &bincode::serialize(content).map_err(|e| Error::Serialisation(e.to_string()))?
            ])
            .0,
        );

        Ok(msg_id)
    }

    /// Generates a new `MessageId` based on provided id.
    pub fn in_response_to(src: &Self) -> Self {
        let mut hasher = Sha3::v256();
        let mut output = [0; MESSAGE_ID_LEN];
        hasher.update(src.as_ref());
        hasher.finalize(&mut output);

        Self(output)
    }

    /// Generates a new `MessageId` deterministically derived from provided sources.
    pub fn combine(sources: &[[u8; MESSAGE_ID_LEN]]) -> Self {
        let mut hash_bytes = Vec::new();
        for src in sources.iter() {
            hash_bytes.extend_from_slice(src);
        }

        let mut hasher = Sha3::v256();
        let mut output = [0; MESSAGE_ID_LEN];
        hasher.update(&hash_bytes);
        hasher.finalize(&mut output);

        Self(output)
    }

    fn fmt_bytes(bytes: &[u8; MESSAGE_ID_LEN], f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:0.10}", HexFmt(bytes))
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<[u8; MESSAGE_ID_LEN]> for MessageId {
    fn as_ref(&self) -> &[u8; MESSAGE_ID_LEN] {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}..",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}
