// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{agreement::SectionAuth, KeyedSig, NodeState};
use crate::messaging::SectionAuthorityProvider;
use bls::PublicKey as BlsPublicKey;
use ed25519_dalek::Signature;
use secured_linked_list::SecuredLinkedList;
use serde::{Deserialize, Serialize};
use sn_consensus::Decision;
use std::{collections::VecDeque, net::SocketAddr};

/// Response to a request to join a section
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum JoinRequest {
    Initiate {
        /// The public key of the section to join.
        section_key: BlsPublicKey,
    },
    SubmitResourceProof {
        /// The public key of the section to join.
        section_key: BlsPublicKey,
        /// The challenge solution
        proof: Box<ResourceProof>,
    },
}

/// Joining peer's proof of resolvement of given resource proofing challenge.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, custom_debug::Debug)]
pub struct ResourceProof {
    #[allow(missing_docs)]
    pub solution: u64,
    #[allow(missing_docs)]
    #[debug(skip)]
    pub data: VecDeque<u8>,
    #[allow(missing_docs)]
    #[debug(skip)]
    pub nonce: [u8; 32],
    #[allow(missing_docs)]
    #[debug(with = "crate::types::Signature::fmt_ed25519")]
    pub nonce_signature: Signature,
}

/// Response to a request to join a section
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum JoinResponse {
    /// Challenge sent from existing elder nodes to the joining peer for resource proofing.
    ResourceChallenge {
        #[allow(missing_docs)]
        data_size: usize,
        /// how hard the challenge should be to solve
        difficulty: u8,
        #[allow(missing_docs)]
        nonce: [u8; 32],
        #[allow(missing_docs)]
        nonce_signature: Signature,
    },
    /// Up to date section information for a joining peer to retry its join request with
    Retry {
        /// Current `SectionAuthorityProvider` of the section.
        section_auth: SectionAuthorityProvider,
        /// Section signature over the `SectionAuthorityProvider`.
        section_signed: KeyedSig,
        /// Section chain truncated from the section key found in the join request.
        proof_chain: SecuredLinkedList,
        /// The age of the node as expected by the section.
        expected_age: u8,
    },
    /// Response redirecting a joining peer to join a different section,
    /// containing addresses of nodes that are closer (than the recipient) to the
    /// requested name. The `JoinRequest` should be re-sent to these addresses.
    Redirect(SectionAuthorityProvider),
    /// Message sent to joining peer containing the necessary
    /// info to become a member of the section.
    Approved {
        /// Network genesis key (needed to validate) section_chain
        genesis_key: BlsPublicKey,
        /// SectionAuthorityProvider Signed by (current section)
        section_auth: SectionAuth<SectionAuthorityProvider>,
        /// Full verifiable section chain
        section_chain: SecuredLinkedList,
        /// Current node's state
        decision: Decision<NodeState>,
    },
    /// Join was rejected
    Rejected(JoinRejectionReason),
}

/// Reason of a join request being rejected
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JoinRejectionReason {
    /// No new peers are currently accepted for joining
    JoinsDisallowed,
    /// The requesting node is not externally reachable
    NodeNotReachable(SocketAddr),
}
