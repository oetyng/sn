// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{client::Error as ErrorMessage, MessageId, MessageType};
use crate::routing::Prefix;
use crate::types::{DataAddress, Error as DtError, PublicKey};
use std::io;
use thiserror::Error;
use xor_name::XorName;

/// Specialisation of `std::Result` for Node.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
#[non_exhaustive]
/// Node error variants.
pub enum Error {
    /// Attempted to perform an operation meant only for Adults when we are not one.
    #[error("Attempted Adult operation when not an Adult")]
    NotAnAdult,
    /// Attempted to perform an operation meant only for Elders when we are not one.
    #[error("Attempted Elder operation when not an Elder")]
    NotAnElder,
    /// Not enough in the section to perform Chunk operation
    #[error("Not enough Adults available in Section({0:?}) to perform operation")]
    NoAdults(Prefix),
    /// Not enough space in `DataStore` to perform `put`.
    #[error("Not enough space")]
    NotEnoughSpace,
    /// Not enough storage available on the network.
    #[error("Not enough storage available on the network")]
    NetworkFull,
    /// No source message provided for ProcessingError
    #[error("No source message for ProcessingError")]
    NoSourceMessageForProcessingError,
    /// Unexpected Process msg. A ProcessingError was expected here...
    #[error("Unexpected Process msg. A ProcessingError was expected here...")]
    UnexpectedProcessMsg,
    /// Node does not manage any section funds.
    #[error("Node does not currently manage any section funds")]
    NoSectionFunds,
    /// Node does not manage any metadata, so is likely not a fully prepared elder yet.
    #[error("Node does not currently manage any section metadata")]
    NoSectionMetaData,
    /// Node does not manage any immutable chunks.
    #[error("Node does not currently manage any immutable chunks")]
    NoImmutableChunks,
    /// Node is currently churning so cannot perform the request.
    #[error("Cannot complete request due to churning of funds")]
    NodeChurningFunds,
    /// Node is currently churning, but failed to sign a message.
    #[error("Error signing message during churn")]
    ChurnSignError,
    /// Genesis node not in genesis stage.
    #[error("Not in genesis stage")]
    NotInGenesis,
    /// Target xorname could not be determined from DstLocation
    #[error("No destination name found")]
    NoDestinationName,
    /// Failed to activate a node, due to it being active already
    #[error("Cannot activate node: Node is already active")]
    NodeAlreadyActive,
    /// Not Section PublicKey.
    #[error("Not section public key returned from routing")]
    NoSectionPublicKey,
    /// Unknown as a Section PublicKey.
    #[error("PublicKey provided was not identified as a section {0}")]
    UnknownSectionKey(PublicKey),
    /// Nodes cannot send direct messages
    #[error("Node cannot send direct messages. This functionality will be deprecated in routing.")]
    CannotDirectMessage,
    /// Node cannot be updated, message cannot be resent
    #[error("Process error could not be handled. We cannot update the erroring node.")]
    CannotUpdateProcessErrorNode,
    /// Not a Section PublicKeyShare.
    #[error("PublicKey provided for signing as elder is not a BLS PublicKeyShare")]
    ProvidedPkIsNotBlsShare,
    /// Not a Section PublicKey.
    #[error("PublicKey provided for signing as elder is not a BLS")]
    ProvidedPkIsNotBls,
    /// Not Section PublicKeySet.
    #[error("Not section public key set returned from routing")]
    NoSectionPublicKeySet,
    /// Not Section PublicKey.
    #[error("Not section public key returned from routing for xorname {0}")]
    NoSectionPublicKeyKnown(XorName),
    /// Unable to parse reward proposal.
    #[error("Cannot parse reward proposal at this stage")]
    InvalidRewardStage,
    /// Node not found for rewarding
    #[error("Node not found for rewards")]
    NodeNotFoundForReward,
    /// Key, Value pair not found.
    #[error("No such data: {0:?}")]
    NoSuchData(DataAddress),
    /// Unable to process fund churn message.
    #[error("Cannot process fund churn message")]
    NotChurningFunds,
    /// Creating temp directory failed.
    #[error("Could not create temp store: {0}")]
    TempDirCreationFailed(String),
    /// Chunk Store Id could not be found
    #[error("Could not fetch StoreId")]
    NoStoreId,
    /// Threshold crypto combine signatures error
    #[error("Could not combine signatures")]
    CouldNotCombineSignatures,
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
    /// Network message error.
    #[error("Client message error:: {0}")]
    ClientMsg(#[from] crate::messaging::client::Error),
    /// Network processing error message.
    #[error("Procesing error:: {0:?}")]
    ProcessingError(crate::messaging::client::ProcessingError),
    /// Network message error.
    #[error("Network message error:: {0}")]
    Message(#[from] crate::messaging::Error),
    /// PickleDb error.
    #[error("PickleDb error:: {0}")]
    PickleDb(#[from] pickledb::error::Error),
    /// NetworkData error.
    #[error("Network data error:: {0}")]
    NetworkData(#[from] crate::types::Error),
    /// Routing error.
    #[error("Routing error:: {0}")]
    Routing(#[from] crate::routing::Error),
    /// Message is invalid.
    #[error("Message with id: '{0:?}' is invalid. {1}")]
    InvalidMessage(MessageId, String),
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
    /// Failed to send message to connection.
    #[error("Failed to send message to connection: {{0.0}}")]
    UnableToSend(MessageType),
}

pub(crate) fn convert_to_error_message(error: Error) -> ErrorMessage {
    match error {
        Error::InvalidOperation(msg) => ErrorMessage::InvalidOperation(msg),
        Error::InvalidMessage(_, msg) => ErrorMessage::InvalidOperation(msg),
        Error::InvalidOwner(key) => ErrorMessage::InvalidOwners(key),
        Error::NoSuchData(address) => ErrorMessage::DataNotFound(address),
        Error::NotEnoughSpace => ErrorMessage::NotEnoughSpace,
        Error::TempDirCreationFailed(_) => ErrorMessage::FailedToWriteFile,
        Error::DataExists => ErrorMessage::DataExists,
        Error::NetworkData(error) => convert_dt_error_to_error_message(error),
        other => {
            ErrorMessage::InvalidOperation(format!("Failed to perform operation: {:?}", other))
        }
    }
}
pub(crate) fn convert_dt_error_to_error_message(error: DtError) -> ErrorMessage {
    match error {
        DtError::InvalidOperation => {
            ErrorMessage::InvalidOperation("DtError::InvalidOperation".to_string())
        }
        DtError::NoSuchEntry => ErrorMessage::NoSuchEntry,
        DtError::AccessDenied(pk) => ErrorMessage::AccessDenied(pk),
        other => ErrorMessage::InvalidOperation(format!("DtError: {:?}", other)),
    }
}
