// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! The Safe Network messaging interface.
//!
//! This modules defines the messages that can be handled by the Safe Network. In particular:
//!
//! - This module contains types that are common across the messaging API.
//! - The [`serialisation`] module defines the wire format and message (de)serialization API.
//! - The [`data`] module defines the data messages that clients and nodes send, and their
//!   possible responses.
//! - The [`node`] module defines the messages that nodes can exchange on the network.
//! - The [`section_info`] module defines the queries and responses for section information – these
//!   may be sent by both clients and nodes.

/// Data messages that clients and nodes can send.
pub mod data;
/// Messages that nodes can exchange on the network.
pub mod node;
/// Queries and responses for section information.
pub mod section_info;
/// The wire format and message (de)serialization API.
pub mod serialisation;
/// BLS Signature aggregator
pub mod signature_aggregator;

// Message authority - keys and signatures.
mod authority;
// Error types definitions
mod errors;
// Source and destination structs for messages
mod location;
// Message ID definition
mod msg_id;
// Types of messages and corresponding source authorities
mod msg_kind;
// SectionAuthorityProvider
mod sap;

pub use self::{
    authority::{
        AuthorityProof, BlsShareAuth, NodeAuth, SectionAuth, ServiceAuth, VerifyAuthority,
    },
    errors::{Error, Result},
    location::{DstLocation, EndUser, SocketId, SrcLocation},
    msg_id::{MessageId, MESSAGE_ID_LEN},
    msg_kind::MsgKind,
    sap::SectionAuthorityProvider,
    serialisation::{MessageType, NodeMsgAuthority, WireMsg},
};
