// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Prefix;
use crate::messaging::data::Error as ErrorMessage;
pub use crate::routing::core::AggregatorError;
pub use crate::routing::dkg::proposal::ProposalError;
use crate::types::{convert_dt_error_to_error_message, DataAddress, PublicKey};
use qp2p::Error as Qp2pError;
use secured_linked_list::error::Error as SecuredLinkedListError;
use std::io;
use std::net::SocketAddr;
use thiserror::Error;
use xor_name::XorName;

/// The type returned by the sn_routing message handling methods.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Internal error.
#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error("Failed signature check.")]
    FailedSignature,
    #[error("Cannot route.")]
    CannotRoute,
    #[error("Empty recipient list")]
    EmptyRecipientList,
    #[error("The config is invalid: {err}")]
    InvalidConfig {
        #[source]
        err: Qp2pError,
    },
    #[error("Cannot connect to the endpoint: {err}")]
    CannotConnectEndpoint {
        #[source]
        err: Qp2pError,
    },
    #[error("Address not reachable: {err}")]
    AddressNotReachable {
        #[source]
        err: Qp2pError,
    },
    #[error("The node is not in a state to handle the action.")]
    InvalidState,
    #[error("Invalid source location.")]
    InvalidSrcLocation,
    #[error("Invalid destination location.")]
    InvalidDstLocation,
    #[error("Content of a received message is inconsistent.")]
    InvalidMessage,
    #[error("A signature share is invalid.")]
    InvalidSignatureShare,
    #[error("The section key share is missing.")]
    MissingSectionKeyShare,
    #[error("Failed to send a message to {0}, {1}")]
    FailedSend(SocketAddr, XorName),
    #[error("Connection closed locally")]
    ConnectionClosed,
    #[error("Invalid section chain: {0}")]
    InvalidSectionChain(#[from] SecuredLinkedListError),
    #[error("Messaging protocol error: {0}")]
    Messaging(#[from] crate::messaging::Error),
    #[error("proposal error: {0}")]
    ProposalError(#[from] ProposalError),
    #[error("invalid payload")]
    InvalidPayload,
    #[error("Routing is set to not allow taking any new node")]
    TryJoinLater,
    #[error("No matching Section")]
    NoMatchingSection,
    #[error("No matching Elder")]
    NoMatchingElder,
    #[error("Node cannot join the network since it is not externally reachable: {0}")]
    NodeNotReachable(SocketAddr),
    /// Database error.
    #[error("Database error:: {0}")]
    Database(#[from] crate::dbs::Error),

    /// Not enough in the section to perform Chunk operation
    #[error("Not enough Adults available in Section({0:?}) to perform operation")]
    NoAdults(Prefix),

    /// Not Section PublicKey.
    #[error("Not section public key returned from routing")]
    NoSectionPublicKey,
    /// Not Section PublicKeySet.
    #[error("Not section public key set returned from routing")]
    NoSectionPublicKeySet,
    /// Not Section PublicKey.
    #[error("Not section public key returned from routing for xorname {0}")]
    NoSectionPublicKeyKnown(XorName),
    /// Key, Value pair not found.
    #[error("No such data: {0:?}")]
    NoSuchData(DataAddress),
    /// Creating temp directory failed.
    #[error("Could not create temp store: {0}")]
    TempDirCreationFailed(String),
    /// Chunk already exists for this node
    #[error("Data already exists at this node")]
    DataExists,
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// JSON serialisation error.
    #[error("JSON serialisation error:: {0}")]
    JsonSerialisation(#[from] serde_json::Error),
    /// Bincode error.
    #[error("Bincode error:: {0}")]
    Bincode(#[from] bincode::Error),
    /// Network service message error.
    #[error("Network service message error:: {0}")]
    ServiceMsg(#[from] crate::messaging::data::Error),
    /// Network service error.
    #[error("Service error:: {0:?}")]
    ServiceError(crate::messaging::data::ServiceError),
    /// Network data error.
    #[error("Network data error:: {0}")]
    NetworkData(#[from] crate::types::Error),
    // /// Message is invalid.
    // #[error("Message with id: '{0:?}' is invalid. {1}")]
    // InvalidMessageReceived(MessageId, String),
    /// Data owner provided is invalid.
    #[error("Provided PublicKey is not a valid owner. Provided PublicKey: {0}")]
    InvalidOwner(PublicKey),
    /// Operation is invalid, eg signing validation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    /// No mapping to sn_messages::Error could be found. Either we need a new error there, or we need to handle or convert this error before sending it as a message
    #[error("No mapping to sn_messages error is set up for this NodeError {0}")]
    NoErrorMapping(String),
    /// Logic error.
    #[error("Logic error: {0}")]
    Logic(String),
    /// Timeout when trying to join the network
    #[error("Timeout when trying to join the network")]
    JoinTimeout,
    /// Configuration error.
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Configuration error.
    #[error("Invalid node authority received for a QueryResponse message")]
    InvalidQueryResponseAuthority,
}

pub(crate) fn convert_to_error_message(error: Error) -> ErrorMessage {
    match error {
        Error::InvalidOperation(msg) => ErrorMessage::InvalidOperation(msg),
        // Error::InvalidMessage(_, msg) => ErrorMessage::InvalidOperation(msg),
        Error::InvalidOwner(key) => ErrorMessage::InvalidOwner(key),
        Error::NoSuchData(address) => ErrorMessage::DataNotFound(address),
        Error::TempDirCreationFailed(_) => ErrorMessage::FailedToWriteFile,
        Error::DataExists => ErrorMessage::DataExists,
        Error::NetworkData(error) => convert_dt_error_to_error_message(error),
        other => {
            ErrorMessage::InvalidOperation(format!("Failed to perform operation: {:?}", other))
        }
    }
}
