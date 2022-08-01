// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{AuthKind, Dst, Error, MsgId, Result};
use bincode::{
    config::{BigEndian, FixintEncoding, WithOtherEndian, WithOtherIntEncoding},
    Options,
};
use bytes::{BufMut, Bytes, BytesMut};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[cfg(feature = "traceroute")]
use crate::messaging::Entity;

// Current version of the messaging protocol.
// At this point this implementation supports only this version.
const MESSAGING_PROTO_VERSION: u16 = 1u16;

// Header to be serialisied at the front of the wire message.
// This header contains the information needed to deserialize the payload.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct WireMsgHeader {
    // We serialise a header size field, but we don't know it up front until we deserialise it.
    // header_size: u16,
    version: u16,
    pub msg_envelope: MsgEnvelope,
}

// The message envelope contains the ID of the message, the authority
// signing this message (if any), as well as destination information
// This is all part of the message header, and it gets deserialized
// when the `WireMsgHeader` is deserialized, allowing the caller to read
// all this information before deciding to deserialise the actual message payload.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct MsgEnvelope {
    pub msg_id: MsgId,
    pub auth: AuthKind,
    pub dst: Dst,
    #[cfg(feature = "traceroute")]
    pub traceroute: Vec<Entity>,
}

// The first two fields in the header. This is not part of the public interface.
#[derive(Debug, Deserialize, Serialize)]
struct HeaderMeta {
    header_len: u16,
    version: u16,
}

impl HeaderMeta {
    const SIZE: usize = size_of::<Self>();

    fn header_len(&self) -> usize {
        self.header_len.into()
    }
}

lazy_static! {
    // Options used for all bincode encoding.
    static ref BINCODE_OPTIONS: WithOtherIntEncoding<
        WithOtherEndian<bincode::DefaultOptions, BigEndian>,
        FixintEncoding,
    > = {
        bincode::DefaultOptions::new()
                    // This choice is arbitrary, and maintained for backwards compatibility.
                    .with_big_endian()
                    // We want known sizes in our wire format.
                    .with_fixint_encoding()
    };
}

impl WireMsgHeader {
    // Instantiate a WireMsgHeader as per current supported version.
    pub fn new(
        msg_id: MsgId,
        auth: AuthKind,
        dst: Dst,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Self {
        Self {
            //header_size: Self::max_size(),
            version: MESSAGING_PROTO_VERSION,
            msg_envelope: MsgEnvelope {
                msg_id,
                auth,
                dst,
                #[cfg(feature = "traceroute")]
                traceroute,
            },
        }
    }

    // Parses the provided bytes to deserialize a WireMsgHeader,
    // returning the created WireMsgHeader, as well as the remaining bytes which
    // correspond to the message payload. The caller shall then take care of
    // deserializing the payload using the information provided in the `WireMsgHeader`.
    pub fn from(mut bytes: Bytes) -> Result<(Self, Bytes)> {
        let bytes_len = bytes.len();

        // Parse the leading metadata
        let meta: HeaderMeta = BINCODE_OPTIONS
            .allow_trailing_bytes()
            .deserialize(&bytes)
            .map_err(|err| Error::FailedToParse(format!("invalid message header: {}", err)))?;

        // We check that we have at least the claimed number of header bytes.
        if meta.header_len() > bytes_len {
            return Err(Error::FailedToParse(format!(
                "not enough bytes received ({}) to deserialize wire message header",
                bytes_len
            )));
        }

        // Make sure we support this version
        if meta.version != MESSAGING_PROTO_VERSION {
            return Err(Error::UnsupportedVersion(meta.version));
        }

        // ...finally, we read the message envelope bytes
        let msg_envelope_bytes = &bytes[HeaderMeta::SIZE..meta.header_len()];
        let msg_envelope: MsgEnvelope =
            rmp_serde::from_slice(msg_envelope_bytes).map_err(|err| {
                Error::FailedToParse(format!(
                    "source authority couldn't be deserialized from the header: {}",
                    err
                ))
            })?;

        let header = Self {
            //header_size,
            version: meta.version,
            msg_envelope,
        };

        // Get a slice for the payload bytes, i.e. the bytes after the header bytes
        let payload_bytes = bytes.split_off(meta.header_len());

        Ok((header, payload_bytes))
    }

    /// Write header metadata and msg envelope info into a provided buffer
    pub fn write(&self, buffer: BytesMut) -> Result<(BytesMut, u16)> {
        // first serialise the msg envelope so we can figure out the total header size
        let msg_envelope_vec = rmp_serde::to_vec_named(&self.msg_envelope).map_err(|err| {
            Error::Serialisation(format!(
                "could not serialize message envelope with Msgpack: {}",
                err
            ))
        })?;

        let meta = HeaderMeta {
            // real header size based on the length of serialised msg envelope
            header_len: (HeaderMeta::SIZE + msg_envelope_vec.len()) as u16,
            version: self.version,
        };

        let mut buffer_writer = buffer.writer();
        // Write the leading metadata
        BINCODE_OPTIONS
            .serialize_into(&mut buffer_writer, &meta)
            .map_err(|err| {
                Error::Serialisation(format!(
                    "header metadata couldn't be serialized into the header: {}",
                    err
                ))
            })?;

        let mut buffer = buffer_writer.into_inner();
        // ...now write the message envelope
        buffer.extend_from_slice(&msg_envelope_vec);

        Ok((buffer, meta.header_len))
    }

    // Message Pack uses type tags, but also variable length encoding, so we expect that serialized
    // `MsgEnvelope`s size will typically be ≤ their in-memory size. This should only be relied on
    // as a 'ballpark' estimate.
    pub fn max_size() -> u16 {
        (HeaderMeta::SIZE + size_of::<MsgEnvelope>()) as u16
    }
}
