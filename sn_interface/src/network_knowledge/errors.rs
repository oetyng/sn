// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use secured_linked_list::error::Error as SecuredLinkedListError;
use thiserror::Error;

/// The type returned by the `sn_routing` message handling methods.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Internal error.
#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error("Section authority provider cannot be trusted: {0}")]
    UntrustedSectionAuthProvider(String),
    #[error("Proof chain cannot be trusted: {0}")]
    UntrustedProofChain(String),
    #[error("Invalid genesis key of provided prefix map: {}", hex::encode(_0.to_bytes()))]
    InvalidGenesisKey(bls::PublicKey),
    #[error("The node is not in a state to handle the action.")]
    InvalidState,
    #[error("A signature share is invalid.")]
    InvalidSignatureShare,
    #[error("A node has invalid relocation details")]
    InvalidRelocationDetails,
    #[error("The secret key share is missing for public key {0:?}")]
    MissingSecretKeyShare(bls::PublicKey),
    #[error("Invalid section chain: {0}")]
    InvalidSectionChain(#[from] SecuredLinkedListError),
    #[error("Invalid payload")]
    InvalidPayload,
    #[error("The section is currently set to not allow taking any new node")]
    TryJoinLater,
    #[error("No matching Section")]
    NoMatchingSection,
    #[error("No matching Relation between peers")]
    NoMatchingRelation,
    #[error(
        "A JoinResponse was reeived after we've already joined the network. It has been ignored."
    )]
    ExistingMemberConflict,
    #[error("The node in question is not a member of the section")]
    NotAMember,
    #[error("Request does not match the section prefix")]
    WrongSection,
    #[error("Consensus({0:?})")]
    Consensus(#[from] sn_consensus::Error),
    #[error("An archived node attempted to rejoin the section")]
    ArchivedNodeRejoined,
}
