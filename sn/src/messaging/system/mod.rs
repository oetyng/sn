// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod agreement;
mod join;
mod join_as_relocated;
mod node_msgs;
mod node_state;
mod signed;

pub use agreement::{DkgFailureSig, DkgFailureSigSet, DkgSessionId, Proposal, SectionAuth};
pub use join::{JoinRejectionReason, JoinRequest, JoinResponse, ResourceProofResponse};
pub use join_as_relocated::{JoinAsRelocatedRequest, JoinAsRelocatedResponse};
pub use node_msgs::{NodeCmd, NodeEvent, NodeQuery, NodeQueryResponse};
pub use node_state::{MembershipState, NodeState, RelocateDetails};
pub use signed::{KeyedSig, SigShare};

/// List of peers of a section
pub type SectionPeers = BTreeSet<SectionAuth<NodeState>>;

use crate::messaging::{EndUser, MsgId, SectionAuthorityProvider};
use bls_dkg::key_gen::message::Message as DkgMessage;
use bytes::Bytes;
use secured_linked_list::SecuredLinkedList;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};
use xor_name::{Prefix, XorName};

use super::authority::SectionAuth as SectionAuthProof;
use super::AuthorityProof;

#[derive(Clone, PartialEq, Serialize, Deserialize, custom_debug::Debug)]
#[allow(clippy::large_enum_variant)]
/// Message sent over the among nodes
pub enum SystemMsg {
    /// Message sent to a peer when a message with outdated section
    /// information was received, attaching the bounced message so
    /// the peer can resend it with up to date destination information.
    AntiEntropyRetry {
        /// Current `SectionAuthorityProvider` of the sender's section.
        section_auth: SectionAuthorityProvider,
        /// Sender's section signature over the `SectionAuthorityProvider`.
        section_signed: KeyedSig,
        /// Sender's section chain truncated from the dst section key found in the `bounced_msg`.
        proof_chain: SecuredLinkedList,
        /// Message bounced due to outdated destination section information.
        #[debug(skip)]
        bounced_msg: Bytes,
    },
    /// Message sent to a peer when a message needs to be sent to a different
    /// and/or closest section, attaching the bounced message so the peer can
    /// resend it to the correct section with up to date destination information.
    AntiEntropyRedirect {
        /// Current `SectionAuthorityProvider` of a closest section.
        section_auth: SectionAuthorityProvider,
        /// Section signature over the `SectionAuthorityProvider` of the closest
        /// section the bounced message shall be resent to.
        section_signed: KeyedSig,
        /// Section chain (from genesis key) for the closest section.
        section_chain: SecuredLinkedList,
        /// Message bounced that shall be resent by the peer.
        #[debug(skip)]
        bounced_msg: Bytes,
    },
    /// Message to update a section when they bounced a message as untrusted back at us.
    /// That section must be behind our current knowledge.
    AntiEntropyUpdate {
        /// Current `SectionAuthorityProvider` of our section.
        section_auth: SectionAuthorityProvider,
        /// Section signature over the `SectionAuthorityProvider` of our
        /// section the bounced message shall be resent to.
        section_signed: KeyedSig,
        /// Our section chain truncated from the triggering msg's dst section_key (or genesis key for full proof)
        proof_chain: SecuredLinkedList,
        /// Section members
        members: SectionPeers,
    },
    /// Probes the network by sending a message to a random dst triggering an AE flow.
    AntiEntropyProbe(XorName),
    /// Sent when a msg-consuming node wants to update a msg-producing node on the number of msgs per s it wants to receive.
    /// It tells the node to adjust msg sending rate according to the provided value in this msg.
    BackPressure(f64),
    /// Send from a section to the node to be immediately relocated.
    Relocate(SectionAuth<NodeState>),
    /// Sent from a bootstrapping peer to the section requesting to join as a new member
    JoinRequest(Box<JoinRequest>),
    /// Response to a `JoinRequest`
    JoinResponse(Box<JoinResponse>),
    /// Sent from a peer to the section requesting to join as relocated from another section
    JoinAsRelocatedRequest(Box<JoinAsRelocatedRequest>),
    /// Response to a `JoinAsRelocatedRequest`
    JoinAsRelocatedResponse(Box<JoinAsRelocatedResponse>),
    /// Sent to the new elder candidates to start the DKG process.
    DkgStart {
        /// The identifier of the DKG session to start.
        session_id: DkgSessionId,
        /// The section prefix. It matches all the members' names.
        prefix: Prefix,
        /// The section's complete set of elders as a map from their name to their socket address.
        elders: BTreeMap<XorName, SocketAddr>,
    },
    /// Message sent when a DKG session has not started
    DkgSessionUnknown {
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// DKG message that came in
        message: DkgMessage,
    },
    /// DKG session info along with section authority
    DkgSessionInfo {
        /// The identifier of the DKG session to start.
        session_id: DkgSessionId,
        /// The section prefix. It matches all the members' names.
        prefix: Prefix,
        /// The section's complete set of elders as a map from their name to their socket address.
        elders: BTreeMap<XorName, SocketAddr>,
        /// Section authority for the DKG start message
        section_auth: AuthorityProof<SectionAuthProof>,
        /// Messages processed in the session so far
        message_cache: Vec<DkgMessage>,
        /// The original DKG message
        message: DkgMessage,
    },
    /// Message exchanged for DKG process.
    DkgMessage {
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// The DKG message.
        message: DkgMessage,
    },
    /// Message signalling that the node is not ready for the
    /// DKG message yet
    DkgNotReady {
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// The sent DKG message.
        message: DkgMessage,
    },
    /// Message containing a history of received DKG messages so other nodes can catch-up
    DkgRetry {
        /// History of messages received at the sender's end
        message_history: Vec<DkgMessage>,
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// The originally sent DKG message.
        message: DkgMessage,
    },
    /// Broadcast to the other DKG participants when a DKG failure is observed.
    DkgFailureObservation {
        /// The DKG key
        session_id: DkgSessionId,
        /// Signature over the failure
        sig: DkgFailureSig,
        /// Nodes that failed to participate
        failed_participants: BTreeSet<XorName>,
    },
    /// Sent to the current elders by the DKG participants when at least majority of them observe
    /// a DKG failure.
    DkgFailureAgreement(DkgFailureSigSet),
    /// Message containing a single `Proposal` to be aggregated in the proposal aggregator.
    Propose {
        /// The content of the proposal
        proposal: Proposal,
        // TODO: try to remove this in favor of the msg header MsgKind sig share we already have
        /// BLS signature share
        sig_share: SigShare,
    },
    /// Message that notifies a section to test
    /// the connectivity to a node
    StartConnectivityTest(XorName),
    /// Cmds are orders to perform some operation, only sent internally in the network.
    NodeCmd(NodeCmd),
    /// Queries is a read-only operation.
    NodeQuery(NodeQuery),
    /// Events are facts about something that happened on a node.
    NodeEvent(NodeEvent),
    /// The response to a query, containing the query result.
    NodeQueryResponse {
        /// QueryResponse.
        response: NodeQueryResponse,
        /// ID of causing query.
        correlation_id: MsgId,
        /// TEMP: Add user here as part of return flow. Remove this as we have chunk routing etc
        user: EndUser,
    },
    /// The returned error, from any msg handling on recipient node.
    NodeMsgError {
        /// The error.
        // TODO: return node::Error instead
        error: crate::messaging::data::Error,
        /// ID of causing cmd.
        correlation_id: MsgId,
    },
}

// highest priority, since we must sort out membership first of all
pub(crate) const DKG_MSG_PRIORITY: i32 = 10;
// very high prio, since we must have correct contact details to the network
pub(crate) const ANTIENTROPY_MSG_PRIORITY: i32 = 8;
// high prio as recipient can't do anything until they've joined. Needs to be lower than DKG (or else no split)
pub(crate) const JOIN_RESPONSE_PRIORITY: i32 = 6;
// our joining to the network
pub(crate) const JOIN_RELOCATE_MSG_PRIORITY: i32 = 4;
// reporting dysfunction is somewhat critical, so not super low
pub(crate) const DYSFUNCTION_MSG_PRIORITY: i32 = 2;
// reporting backpressure isn't time critical, so fairly low
pub(crate) const BACKPRESSURE_MSG_PRIORITY: i32 = 0;
// not maintaining network structure, so can wait
pub(crate) const NODE_DATA_MSG_PRIORITY: i32 = -6;

impl SystemMsg {
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        match self {
            // DKG messages
            SystemMsg::DkgStart { .. }
            | SystemMsg::DkgSessionUnknown { .. }
            | SystemMsg::DkgSessionInfo { .. }
            | SystemMsg::DkgNotReady { .. }
            | SystemMsg::DkgRetry { .. }
            | SystemMsg::DkgMessage { .. }
            | SystemMsg::DkgFailureObservation { .. }
            | SystemMsg::DkgFailureAgreement(_) => DKG_MSG_PRIORITY,

            // Inter-node comms for AE updates
            SystemMsg::AntiEntropyRetry { .. }
            | SystemMsg::AntiEntropyRedirect { .. }
            | SystemMsg::AntiEntropyUpdate { .. }
            | SystemMsg::AntiEntropyProbe(_) => ANTIENTROPY_MSG_PRIORITY,

            // Join responses
            SystemMsg::JoinResponse(_) | SystemMsg::JoinAsRelocatedResponse(_) => {
                JOIN_RESPONSE_PRIORITY
            }

            // Inter-node comms for joining, relocating etc.
            SystemMsg::Relocate(_)
            | SystemMsg::JoinRequest(_)
            | SystemMsg::JoinAsRelocatedRequest(_)
            | SystemMsg::Propose { .. }
            | SystemMsg::StartConnectivityTest(_) => JOIN_RELOCATE_MSG_PRIORITY,

            // Inter-node comms for dysfunction detection
            SystemMsg::NodeEvent(NodeEvent::SuspiciousNodesDetected(_)) => DYSFUNCTION_MSG_PRIORITY,

            // Inter-node comms for backpressure
            SystemMsg::BackPressure(_) => BACKPRESSURE_MSG_PRIORITY,

            // Inter-node comms related to processing client requests
            SystemMsg::NodeCmd(_)
            | SystemMsg::NodeEvent(NodeEvent::CouldNotStoreData { .. })
            | SystemMsg::NodeQuery(_)
            | SystemMsg::NodeQueryResponse { .. }
            | SystemMsg::NodeMsgError { .. } => NODE_DATA_MSG_PRIORITY,
        }
    }
}
