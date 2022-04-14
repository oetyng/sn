// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    system::{DkgFailureSigSet, KeyedSig, NodeState, SectionAuth, SystemMsg},
    DstLocation, WireMsg,
};
use crate::node::{
    core::Proposal,
    network_knowledge::{SectionAuthorityProvider, SectionKeyShare},
    XorName,
};
use crate::types::Peer;

use bytes::Bytes;
use custom_debug::Debug;
use std::{
    collections::BTreeSet,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

#[derive(Debug, Clone)]
pub struct CmdJob {
    id: u64, // Consider use of subcmd id e.g. parent "963111461", child "963111461.0"
    cmd: Cmd,
    priority: i32,
    time: SystemTime,
}

impl CmdJob {
    pub(crate) fn new(id: u64, cmd: Cmd, time: SystemTime) -> Self {
        let priority = cmd.priority();
        Self {
            id,
            cmd,
            priority,
            time,
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn cmd(&self) -> &Cmd {
        &self.cmd
    }

    pub(crate) fn priority(&self) -> i32 {
        self.priority
    }

    pub(crate) fn time(&self) -> SystemTime {
        self.time
    }
}

/// Internal cmds for a node.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub(crate) enum Cmd {
    /// Cleanup node's PeerLinks, removing any unsused, unconnected peers
    CleanupPeerLinks,
    /// Handle `message` from `sender`.
    /// Holding the WireMsg that has been received from the network,
    HandleMsg {
        sender: Peer,
        wire_msg: WireMsg,
        #[debug(skip)]
        // original bytes to avoid reserializing for entropy checks
        original_bytes: Option<Bytes>,
    },
    /// Handle a timeout previously scheduled with `ScheduleDkgTimeout`.
    HandleDkgTimeout(u64),
    /// Handle peer that's been detected as lost.
    HandlePeerLost(Peer),
    /// Handle agreement on a proposal.
    HandleAgreement { proposal: Proposal, sig: KeyedSig },
    /// Handle a new Node joining agreement.
    HandleNewNodeOnline(SectionAuth<NodeState>),
    /// Handle agree on elders. This blocks node message processing until complete.
    HandleNewEldersAgreement { proposal: Proposal, sig: KeyedSig },
    /// Handle the outcome of a DKG session where we are one of the participants (that is, one of
    /// the proposed new elders).
    HandleDkgOutcome {
        section_auth: SectionAuthorityProvider,
        outcome: SectionKeyShare,
    },
    /// Handle a DKG failure that was observed by a majority of the DKG participants.
    HandleDkgFailure(DkgFailureSigSet),
    /// Send a message to the given `recipients`.
    SendMsg {
        recipients: Vec<Peer>,
        wire_msg: WireMsg,
    },
    /// Send the batch of given messages in a throttled/controlled fashion to the given `recipients`.
    ThrottledSendBatchMsgs {
        throttle_duration: Duration,
        recipients: Vec<Peer>,
        wire_msgs: Vec<WireMsg>,
    },
    /// Performs serialisation and signing for sending of NodeMsg.
    /// This cmd only send this to other nodes
    SignOutgoingSystemMsg { msg: SystemMsg, dst: DstLocation },
    /// Send a message to `delivery_group_size` peers out of the given `recipients`.
    SendMsgDeliveryGroup {
        recipients: Vec<Peer>,
        delivery_group_size: usize,
        wire_msg: WireMsg,
    },
    /// Schedule a timeout after the given duration. When the timeout expires, a `HandleDkgTimeout`
    /// cmd is raised. The token is used to identify the timeout.
    ScheduleDkgTimeout { duration: Duration, token: u64 },
    /// Test peer's connectivity
    SendAcceptedOnlineShare {
        peer: Peer,
        // Previous name if relocated.
        previous_name: Option<XorName>,
    },
    /// Proposes peers as offline
    ProposeOffline(BTreeSet<XorName>),
    /// Send a signal to all Elders to
    /// test the connectivity to a specific node
    StartConnectivityTest(XorName),
    /// Test Connectivity
    TestConnectivity(XorName),
}

impl Cmd {
    /// The priority of the cmd
    pub(crate) fn priority(&self) -> i32 {
        use Cmd::*;
        let prio = match self {
            HandleAgreement { .. } => 10,
            HandleNewEldersAgreement { .. } => 10,
            HandleDkgOutcome { .. } => 10,
            HandleDkgFailure(_) => 10,
            SendAcceptedOnlineShare { .. } => 10,
            HandlePeerLost(_) => 10,
            ProposeOffline(_) => 10,

            HandleDkgTimeout(_) => 9,
            HandleNewNodeOnline(_) => 9,

            ScheduleDkgTimeout { .. } => 8,
            StartConnectivityTest(_) => 8,
            TestConnectivity(_) => 8,

            HandleMsg { wire_msg, .. } => wire_msg.priority(),
            SendMsg { wire_msg, .. } => wire_msg.priority(),
            SignOutgoingSystemMsg { msg, .. } => msg.priority(),
            SendMsgDeliveryGroup { wire_msg, .. } => wire_msg.priority(),
            ThrottledSendBatchMsgs { wire_msgs, .. } => {
                if wire_msgs.is_empty() {
                    -11
                } else {
                    wire_msgs.iter().map(|msg| msg.priority()).sum::<i32>() / wire_msgs.len() as i32
                }
            }

            CleanupPeerLinks => -10,
        };

        prio
    }
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::CleanupPeerLinks => {
                write!(f, "CleanupPeerLinks")
            }
            Cmd::HandleDkgTimeout(_) => write!(f, "HandleDkgTimeout"),
            Cmd::ScheduleDkgTimeout { .. } => write!(f, "ScheduleDkgTimeout"),
            #[cfg(not(feature = "test-utils"))]
            Cmd::HandleMsg { wire_msg, .. } => {
                write!(f, "HandleMsg {:?}", wire_msg.msg_id())
            }
            #[cfg(feature = "test-utils")]
            Cmd::HandleMsg { wire_msg, .. } => {
                write!(
                    f,
                    "HandleMsg {:?} {:?}",
                    wire_msg.msg_id(),
                    wire_msg.payload_debug
                )
            }
            Cmd::HandlePeerLost(peer) => write!(f, "HandlePeerLost({:?})", peer.name()),
            Cmd::HandleAgreement { .. } => write!(f, "HandleAgreement"),
            Cmd::HandleNewEldersAgreement { .. } => write!(f, "HandleNewEldersAgreement"),
            Cmd::HandleNewNodeOnline(_) => write!(f, "HandleNewNodeOnline"),
            Cmd::HandleDkgOutcome { .. } => write!(f, "HandleDkgOutcome"),
            Cmd::HandleDkgFailure(_) => write!(f, "HandleDkgFailure"),
            #[cfg(not(feature = "test-utils"))]
            Cmd::SendMsg { wire_msg, .. } => {
                write!(f, "SendMsg {:?}", wire_msg.msg_id())
            }
            #[cfg(feature = "test-utils")]
            Cmd::SendMsg { wire_msg, .. } => {
                write!(
                    f,
                    "SendMsg {:?} {:?}",
                    wire_msg.msg_id(),
                    wire_msg.payload_debug
                )
            }
            Cmd::SignOutgoingSystemMsg { .. } => write!(f, "SignOutgoingSystemMsg"),
            Cmd::ThrottledSendBatchMsgs { .. } => write!(f, "ThrottledSendBatchMsgs"),
            #[cfg(not(feature = "test-utils"))]
            Cmd::SendMsgDeliveryGroup { wire_msg, .. } => {
                write!(f, "SendMsgDeliveryGroup {:?}", wire_msg.msg_id())
            }
            #[cfg(feature = "test-utils")]
            Cmd::SendMsgDeliveryGroup { wire_msg, .. } => {
                write!(
                    f,
                    "SendMsg {:?} {:?}",
                    wire_msg.msg_id(),
                    wire_msg.payload_debug
                )
            }
            Cmd::SendAcceptedOnlineShare { .. } => write!(f, "SendAcceptedOnlineShare"),
            Cmd::ProposeOffline(_) => write!(f, "ProposeOffline"),
            Cmd::StartConnectivityTest(_) => write!(f, "StartConnectivityTest"),
            Cmd::TestConnectivity(_) => write!(f, "TestConnectivity"),
        }
    }
}

/// Generate unique timer token.
pub(crate) fn next_timer_token() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}
