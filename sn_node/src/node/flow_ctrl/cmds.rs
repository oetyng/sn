// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    messaging::{OutgoingMsg, Peers},
    Proposal, XorName,
};

use bytes::Bytes;
use custom_debug::Debug;
use sn_consensus::Decision;
use sn_dysfunction::IssueType;
use sn_interface::{
    messaging::{
        data::{OperationId, ServiceMsg},
        system::{DkgFailureSigSet, KeyedSig, NodeState, SectionAuth, SystemMsg},
        AuthorityProof, MsgId, NodeMsgAuthority, ServiceAuth, WireMsg,
    },
    network_knowledge::{SectionAuthorityProvider, SectionKeyShare},
    types::Peer,
};
use std::{
    collections::BTreeSet,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[cfg(feature = "traceroute")]
use sn_interface::messaging::Entity;

/// Internal cmds for a node.
///
/// Cmds are used to connect different modules, allowing
/// for a better modularization of the code base.
/// Modelling a call like this also allows for throttling
/// and prioritization, which is not something e.g. tokio tasks allow.
/// In other words, it enables enhanced flow control.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub(crate) enum Cmd {
    /// Start chain reaction based on something that happened.
    HandleEvent(crate::node::Event),
    /// Validate `wire_msg` from `sender`.
    /// Holding the WireMsg that has been received from the network,
    ValidateMsg {
        origin: Peer,
        wire_msg: WireMsg,
        #[debug(skip)]
        // original bytes to avoid reserializing for entropy checks
        original_bytes: Bytes,
    },
    /// Log a Node's Punishment, this pulls dysfunction and write locks out of some functions
    TrackNodeIssueInDysfunction { name: XorName, issue: IssueType },
    /// Adds peer to set of recipients of an already pending query,
    /// or adds a pending query if it didn't already exist.
    AddToPendingQueries {
        operation_id: OperationId,
        origin: Peer,
    },
    HandleValidSystemMsg {
        msg_id: MsgId,
        msg: SystemMsg,
        origin: Peer,
        msg_authority: NodeMsgAuthority,
        #[debug(skip)]
        wire_msg_payload: Bytes,
        #[cfg(feature = "traceroute")]
        traceroute: Vec<Entity>,
    },
    HandleValidServiceMsg {
        msg_id: MsgId,
        msg: ServiceMsg,
        origin: Peer,
        /// Requester's authority over this message
        auth: AuthorityProof<ServiceAuth>,
        #[cfg(feature = "traceroute")]
        traceroute: Vec<Entity>,
    },
    /// Cleanup node's PeerLinks, removing any unsused, unconnected peers
    CleanupPeerLinks,
    /// Handle a timeout previously scheduled with `ScheduleDkgTimeout`.
    HandleDkgTimeout(u64),
    /// Handle peer that's been detected as lost.
    HandlePeerFailedSend(Peer),
    /// Handle agreement on a proposal.
    HandleAgreement { proposal: Proposal, sig: KeyedSig },
    /// Handle a membership decision.
    HandleMembershipDecision(Decision<NodeState>),
    /// Handle agree on elders. This blocks node message processing until complete.
    HandleNewEldersAgreement {
        new_elders: SectionAuth<SectionAuthorityProvider>,
        sig: KeyedSig,
    },
    /// Handle the outcome of a DKG session where we are one of the participants (that is, one of
    /// the proposed new elders).
    HandleDkgOutcome {
        section_auth: SectionAuthorityProvider,
        outcome: SectionKeyShare,
    },
    /// Handle a DKG failure that was observed by a majority of the DKG participants.
    HandleDkgFailure(DkgFailureSigSet),
    /// Performs serialisation and signing and sends the msg.
    SendMsg {
        msg: OutgoingMsg,
        msg_id: MsgId,
        recipients: Peers,
        #[cfg(feature = "traceroute")]
        traceroute: Vec<Entity>,
    },
    /// Schedule a timeout after the given duration. When the timeout expires, a `HandleDkgTimeout`
    /// cmd is raised. The token is used to identify the timeout.
    ScheduleDkgTimeout { duration: Duration, token: u64 },
    /// Proposes peers as offline
    ProposeOffline(BTreeSet<XorName>),
    /// Send a signal to all Elders to
    /// test the connectivity to a specific node
    TellEldersToStartConnectivityTest(XorName),
    /// Test Connectivity
    TestConnectivity(XorName),
    /// Comm cmds
    #[allow(unused)]
    Comm(crate::comm::Cmd),
    /// Data cmds
    Data(crate::data::Cmd),
}

impl Cmd {
    pub(crate) fn send_msg(msg: OutgoingMsg, recipients: Peers) -> Self {
        Self::send_traced_msg(
            msg,
            recipients,
            #[cfg(feature = "traceroute")]
            vec![],
        )
    }

    pub(crate) fn send_traced_msg(
        msg: OutgoingMsg,
        recipients: Peers,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Self {
        Cmd::SendMsg {
            msg,
            msg_id: MsgId::new(),
            recipients,
            #[cfg(feature = "traceroute")]
            traceroute,
        }
    }

    /// The priority of the cmd
    pub(crate) fn priority(&self) -> i32 {
        use Cmd::*;
        match self {
            SendMsg { msg, .. } => match msg {
                OutgoingMsg::System(_) => 20,
                _ => 19,
            },

            HandleAgreement { .. } => 10,
            HandleNewEldersAgreement { .. } => 10,
            HandleDkgOutcome { .. } => 10,
            HandleDkgFailure(_) => 10,
            HandleDkgTimeout(_) => 10,

            HandlePeerFailedSend(_) => 9,
            TrackNodeIssueInDysfunction { .. } => 9,
            ProposeOffline(_) => 9,
            HandleMembershipDecision(_) => 9,
            CleanupPeerLinks => 9,

            ScheduleDkgTimeout { .. } => 8,
            TellEldersToStartConnectivityTest(_) => 8,
            TestConnectivity(_) => 8,

            Comm(_) => 7,
            HandleEvent(_) => 5,
            Data(_) => 0,

            AddToPendingQueries { .. } => 6,

            // See [`MsgType`] for the priority constants and the range of possible values.
            HandleValidSystemMsg { msg, .. } => msg.priority(),
            HandleValidServiceMsg { msg, .. } => msg.priority(),

            ValidateMsg { .. } => -9, // before it's validated, we cannot give it high prio, as it would be a spam vector
        }
    }
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::CleanupPeerLinks => {
                write!(f, "CleanupPeerLinks")
            }
            Cmd::HandleEvent(_) => write!(f, "HandleEvent"),
            Cmd::HandleDkgTimeout(_) => write!(f, "HandleDkgTimeout"),
            Cmd::ScheduleDkgTimeout { .. } => write!(f, "ScheduleDkgTimeout"),
            #[cfg(not(feature = "test-utils"))]
            Cmd::ValidateMsg { wire_msg, .. } => {
                write!(f, "ValidateMsg {:?}", wire_msg.msg_id())
            }
            #[cfg(feature = "test-utils")]
            Cmd::ValidateMsg { wire_msg, .. } => {
                write!(
                    f,
                    "ValidateMsg {:?} {:?}",
                    wire_msg.msg_id(),
                    wire_msg.payload_debug
                )
            }
            Cmd::HandleValidSystemMsg { msg_id, msg, .. } => {
                write!(f, "HandleValidSystemMsg {:?}: {:?}", msg_id, msg)
            }
            Cmd::HandleValidServiceMsg { msg_id, msg, .. } => {
                write!(f, "HandleValidServiceMsg {:?}: {:?}", msg_id, msg)
            }
            Cmd::HandlePeerFailedSend(peer) => write!(f, "HandlePeerFailedSend({:?})", peer.name()),
            Cmd::HandleAgreement { .. } => write!(f, "HandleAgreement"),
            Cmd::HandleNewEldersAgreement { .. } => write!(f, "HandleNewEldersAgreement"),
            Cmd::HandleMembershipDecision(_) => write!(f, "HandleMembershipDecision"),
            Cmd::HandleDkgOutcome { .. } => write!(f, "HandleDkgOutcome"),
            Cmd::HandleDkgFailure(_) => write!(f, "HandleDkgFailure"),
            Cmd::SendMsg { .. } => write!(f, "SendMsg"),
            Cmd::TrackNodeIssueInDysfunction { name, issue } => {
                write!(f, "TrackNodeIssueInDysfunction {:?}, {:?}", name, issue)
            }
            Cmd::ProposeOffline(_) => write!(f, "ProposeOffline"),
            Cmd::AddToPendingQueries { .. } => write!(f, "AddToPendingQueries"),
            Cmd::TellEldersToStartConnectivityTest(_) => {
                write!(f, "TellEldersToStartConnectivityTest")
            }
            Cmd::TestConnectivity(_) => write!(f, "TestConnectivity"),
            Cmd::Comm(cmd) => write!(f, "Comm({:?})", cmd),
            Cmd::Data(cmd) => write!(f, "Data({:?})", cmd),
        }
    }
}

/// Generate unique timer token.
pub(crate) fn next_timer_token() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}
