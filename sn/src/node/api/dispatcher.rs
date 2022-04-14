// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::elder_count;
use crate::messaging::{system::SystemMsg, MsgKind, WireMsg};
use crate::node::{
    api::Cmd,
    core::{DeliveryStatus, Node, Proposal},
    messages::WireMsgUtils,
    Error, Result,
};
use crate::types::Peer;

use itertools::Itertools;
use std::{sync::Arc, time::Duration};
use tokio::time::MissedTickBehavior;
use tokio::{sync::watch, time};

// Cmd Dispatcher.
#[derive(Clone)]
pub(crate) struct Dispatcher {
    node: Arc<Node>,
    dkg_timeout: Arc<DkgTimeout>,
}

impl Dispatcher {
    pub(super) fn new(node: Arc<Node>) -> Self {
        let (cancel_timer_tx, cancel_timer_rx) = watch::channel(false);
        let dkg_timeout = Arc::new(DkgTimeout {
            cancel_timer_tx,
            cancel_timer_rx,
        });

        Self { node, dkg_timeout }
    }

    /// Handles a single cmd.
    pub(crate) async fn process_cmd(&self, cmd: Cmd) -> Result<Vec<Cmd>> {
        match cmd {
            Cmd::CleanupPeerLinks => {
                let linked_peers = self.node.comm.linked_peers().await;

                if linked_peers.len() < elder_count() {
                    return Ok(vec![]);
                }

                self.node.comm.remove_expired().await;

                let sections = self.node.network_knowledge().prefix_map().all();
                let network_peers = sections
                    .iter()
                    .flat_map(|info| info.elders_vec())
                    .collect_vec();

                for peer in linked_peers.clone() {
                    if !network_peers.contains(&peer) {
                        // not among known peers in the network
                        if !self.node.pending_data_queries_contains_client(&peer).await
                            && !self.node.comm.is_connected(&peer).await
                        {
                            trace!("{peer:?} not waiting on queries and not in the network, so lets unlink them");
                            self.node.comm.unlink_peer(&peer).await;
                        }
                    }
                }

                Ok(vec![])
            }
            Cmd::SignOutgoingSystemMsg { msg, dst } => {
                let src_section_pk = self.node.network_knowledge().section_key().await;
                let wire_msg =
                    WireMsg::single_src(&*self.node.info.read().await, dst, msg, src_section_pk)?;

                let mut cmds = vec![];
                cmds.extend(self.node.send_msg_to_nodes(wire_msg).await?);

                Ok(cmds)
            }
            Cmd::HandleMsg {
                sender,
                wire_msg,
                original_bytes,
            } => self.node.handle_msg(sender, wire_msg, original_bytes).await,
            Cmd::HandleDkgTimeout(token) => self.node.handle_dkg_timeout(token).await,
            Cmd::HandleAgreement { proposal, sig } => {
                self.node.handle_general_agreements(proposal, sig).await
            }
            Cmd::HandleNewNodeOnline(auth) => {
                self.node
                    .handle_online_agreement(auth.value.into_state(), auth.sig)
                    .await
            }
            Cmd::HandleNewEldersAgreement { proposal, sig } => match proposal {
                Proposal::NewElders(section_auth) => {
                    self.node
                        .handle_new_elders_agreement(section_auth, sig)
                        .await
                }
                _ => {
                    error!("Other agreement messages should be handled in `HandleAgreement`, which is non-blocking ");
                    Ok(vec![])
                }
            },
            Cmd::HandlePeerLost(peer) => self.node.handle_peer_lost(&peer.addr()).await,
            Cmd::HandleDkgOutcome {
                section_auth,
                outcome,
            } => self.node.handle_dkg_outcome(section_auth, outcome).await,
            Cmd::HandleDkgFailure(signeds) => self
                .node
                .handle_dkg_failure(signeds)
                .await
                .map(|cmd| vec![cmd]),
            Cmd::SendMsg {
                recipients,
                wire_msg,
            } => self.send_msg(&recipients, recipients.len(), wire_msg).await,
            Cmd::ThrottledSendBatchMsgs {
                throttle_duration,
                recipients,
                mut wire_msgs,
            } => {
                self.send_throttled_batch_msgs(recipients, &mut wire_msgs, throttle_duration)
                    .await
            }
            Cmd::SendMsgDeliveryGroup {
                recipients,
                delivery_group_size,
                wire_msg,
            } => {
                self.send_msg(&recipients, delivery_group_size, wire_msg)
                    .await
            }
            Cmd::ScheduleDkgTimeout { duration, token } => Ok(self
                .handle_scheduled_dkg_timeout(duration, token)
                .await
                .into_iter()
                .collect()),
            Cmd::SendAcceptedOnlineShare {
                peer,
                previous_name,
            } => {
                self.node
                    .send_accepted_online_share(peer, previous_name)
                    .await
            }
            Cmd::ProposeOffline(names) => self.node.cast_offline_proposals(&names).await,
            Cmd::StartConnectivityTest(name) => Ok(vec![
                self.node
                    .send_msg_to_our_elders(SystemMsg::StartConnectivityTest(name))
                    .await?,
            ]),
            Cmd::TestConnectivity(name) => {
                if let Some(member_info) = self
                    .node
                    .network_knowledge()
                    .get_section_member(&name)
                    .await
                {
                    if self
                        .node
                        .comm
                        .is_reachable(&member_info.addr())
                        .await
                        .is_err()
                    {
                        self.node.log_comm_issue(member_info.name()).await?
                    }
                }
                Ok(vec![])
            }
        }
    }

    async fn send_msg(
        &self,
        recipients: &[Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<Vec<Cmd>> {
        let cmds = match wire_msg.msg_kind() {
            MsgKind::NodeAuthMsg(_) | MsgKind::NodeBlsShareAuthMsg(_) => {
                self.deliver_msgs(recipients, delivery_group_size, wire_msg)
                    .await?
            }
            MsgKind::ServiceMsg(_) => {
                // we should never be sending such a msg to more than one recipient
                // need refactors further up to solve in a nicer way
                if recipients.len() > 1 {
                    warn!("Unexpected number of client recipients {:?} for msg {:?}. Only sending to first.",
                    recipients.len(), wire_msg);
                }
                if let Some(recipient) = recipients.get(0) {
                    if let Err(err) = self
                        .node
                        .comm
                        .send_to_client(recipient, wire_msg.clone())
                        .await
                    {
                        error!(
                            "Failed sending message {:?} to client {:?} with error {:?}",
                            wire_msg, recipient, err
                        );
                    }
                }

                vec![]
            }
        };

        Ok(cmds)
    }

    async fn send_throttled_batch_msgs(
        &self,
        recipients: Vec<Peer>,
        messages: &mut Vec<WireMsg>,
        throttle_duration: Duration,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        let mut interval = tokio::time::interval(throttle_duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            let _instant = interval.tick().await;
            if let Some(message) = messages.pop() {
                cmds.extend(
                    self.send_msg(&recipients, recipients.len(), message)
                        .await?,
                )
            } else {
                info!("Finished sending a batch of messages");
                break;
            }
        }

        Ok(cmds)
    }

    async fn deliver_msgs(
        &self,
        recipients: &[Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<Vec<Cmd>> {
        let status = self
            .node
            .comm
            .send(recipients, delivery_group_size, wire_msg)
            .await?;

        match status {
            DeliveryStatus::MinDeliveryGroupSizeReached(failed_recipients)
            | DeliveryStatus::MinDeliveryGroupSizeFailed(failed_recipients) => {
                Ok(failed_recipients
                    .into_iter()
                    .map(Cmd::HandlePeerLost)
                    .collect())
            }
            _ => Ok(vec![]),
        }
        .map_err(|e: Error| e)
    }

    async fn handle_scheduled_dkg_timeout(&self, duration: Duration, token: u64) -> Option<Cmd> {
        let mut cancel_rx = self.dkg_timeout.cancel_timer_rx.clone();

        if *cancel_rx.borrow() {
            // Timers are already cancelled, do nothing.
            return None;
        }

        tokio::select! {
            _ = time::sleep(duration) => Some(Cmd::HandleDkgTimeout(token)),
            _ = cancel_rx.changed() => None,
        }
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        // Cancel all scheduled timers including any future ones.
        let _res = self.dkg_timeout.cancel_timer_tx.send(true);
    }
}

struct DkgTimeout {
    cancel_timer_tx: watch::Sender<bool>,
    cancel_timer_rx: watch::Receiver<bool>,
}
