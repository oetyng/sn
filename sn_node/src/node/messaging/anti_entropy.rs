// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::core::NodeContext;
use crate::node::{flow_ctrl::cmds::Cmd, messaging::Peers, Error, MyNode, Result};
use bls::PublicKey as BlsPublicKey;
use itertools::Itertools;
use qp2p::{SendStream, UsrMsgBytes};
use sn_dysfunction::IssueType;
use sn_interface::messaging::data::StorageThreshold;
use sn_interface::{
    messaging::{
        data::ClientDataResponse,
        system::{AntiEntropyKind, NodeEvent, NodeMsg, SectionSigned},
        Dst, MsgId, MsgType, WireMsg,
    },
    network_knowledge::{NodeState, SectionTreeUpdate},
    types::{log_markers::LogMarker, Peer, PublicKey},
};
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use xor_name::XorName;

// Returned by `check_for_entropy` private helper to indicate the
// type of AE response that needs to be sent back to either to the client/Node
enum AeResponseKind {
    Retry(SectionTreeUpdate),
    Redirect(SectionTreeUpdate),
}

impl MyNode {
    /// Check if the origin needs to be updated on network structure/members.
    /// Returns an ae cmd if we need to halt msg validation and update the origin instead.
    #[instrument(skip_all)]
    pub(crate) async fn check_ae_on_node_msg(
        context: &NodeContext,
        origin: &Peer,
        msg: &NodeMsg,
        wire_msg: &WireMsg,
        dst: &Dst,
        send_stream: Option<Arc<Mutex<SendStream>>>,
    ) -> Result<Vec<Cmd>> {
        // Adult nodes don't need to carry out entropy checking,
        // however the message shall always be handled.
        if !context.is_elder {
            return Ok(vec![]);
        }
        // For the case of receiving a join request not matching our prefix,
        // we just let the join request handler to deal with it later on.
        // We also skip AE check on Anti-Entropy messages
        //
        // TODO: consider changing the join and "join as relocated" flows to
        // make use of AntiEntropy retry/redirect responses.
        let msg_id = wire_msg.msg_id();
        match msg {
            NodeMsg::AntiEntropy { .. }
            | NodeMsg::JoinRequest(_)
            | NodeMsg::JoinAsRelocatedRequest(_) => {
                trace!("Entropy check skipped for {msg_id:?}, handling message directly");
                Ok(vec![])
            }
            _ => {
                debug!("Checking {msg_id:?} for entropy");
                if let Some(ae_cmd) =
                    MyNode::check_node_msg_for_entropy(wire_msg, context, dst, origin, send_stream)?
                {
                    // we want to log issues with any node repeatedly out of sync here...
                    let cmds = vec![
                        Cmd::TrackNodeIssueInDysfunction {
                            name: origin.name(),
                            issue: sn_dysfunction::IssueType::Knowledge,
                        },
                        ae_cmd,
                    ];

                    return Ok(cmds);
                }

                trace!("Entropy check passed. Handling verified msg {msg_id:?}");

                Ok(vec![])
            }
        }
    }

    /// Check if the client needs to be updated on network structure.
    /// Returns `true` if an AE msg was sent to the client.
    #[instrument(skip_all)]
    pub(crate) async fn is_ae_sent_to_client(
        context: &NodeContext,
        origin: &Peer,
        wire_msg: &WireMsg,
        dst: &Dst,
        send_stream: Arc<Mutex<SendStream>>,
    ) -> Result<bool> {
        let msg_id = wire_msg.msg_id();
        if !context.is_elder {
            trace!("Redirecting from Adult to our section Elders upon {msg_id:?}");
            let section_tree_update = MyNode::generate_ae_section_tree_update(context, None);
            MyNode::send_ae_response_to_client(
                context,
                origin,
                send_stream,
                wire_msg.serialize()?,
                section_tree_update,
            )
            .await?;
            return Ok(true);
        }

        match MyNode::check_for_entropy(context, msg_id, dst)? {
            None => Ok(false),
            Some(
                AeResponseKind::Redirect(section_tree_update)
                | AeResponseKind::Retry(section_tree_update),
            ) => {
                trace!(
                    "{} {msg_id:?} entropy found. Client {origin:?} should be updated",
                    LogMarker::AeSendRetryAsOutdated
                );

                let bounced_msg = wire_msg.serialize()?;
                MyNode::send_ae_response_to_client(
                    context,
                    origin,
                    send_stream,
                    bounced_msg,
                    section_tree_update,
                )
                .await?;

                Ok(true)
            }
        }
    }

    /// Send `AntiEntropy` update message to all nodes in our own section.
    pub(crate) fn send_ae_update_to_our_section(&self) -> Result<Option<Cmd>> {
        let our_name = self.info().name();
        let context = &self.context();
        let recipients: BTreeSet<_> = self
            .network_knowledge
            .section_members()
            .into_iter()
            .filter(|info| info.name() != our_name)
            .map(|info| *info.peer())
            .collect();

        if recipients.is_empty() {
            warn!("No peers of our section found in our network knowledge to send AE-Update");
            return Ok(None);
        }

        let leaf = self.section_chain().last_key()?;
        // The previous PK which is likely what adults know
        match self.section_chain().get_parent_key(&leaf) {
            Ok(prev_pk) => {
                let prev_pk = prev_pk.unwrap_or(*self.section_chain().genesis_key());
                Ok(Some(MyNode::send_ae_update_to_nodes(
                    context, recipients, prev_pk,
                )))
            }
            Err(_) => {
                error!("SectionsDAG fields went out of sync");
                Ok(None)
            }
        }
    }

    /// Send `AntiEntropy` update message to the specified nodes.
    pub(crate) fn send_ae_update_to_nodes(
        context: &NodeContext,
        recipients: BTreeSet<Peer>,
        section_pk: BlsPublicKey,
    ) -> Cmd {
        let members = context.network_knowledge.section_signed_members();

        let ae_msg = NodeMsg::AntiEntropy {
            section_tree_update: MyNode::generate_ae_section_tree_update(context, Some(section_pk)),
            kind: AntiEntropyKind::Update { members },
        };

        MyNode::send_system_msg(ae_msg, Peers::Multiple(recipients), context.clone())
    }

    /// Send `StorageThresholdReached` event to the specified nodes
    pub(crate) fn report_us_as_full(&self, recipients: BTreeSet<Peer>) -> Cmd {
        let context = self.context();
        let node_id = PublicKey::from(context.keypair.public);
        let our_name = XorName::from(node_id);

        // we report to the recipients that our storage threshold has been reached
        let msg = NodeMsg::NodeEvent(NodeEvent::StorageThresholdReached {
            section: our_name,
            node_id,
            level: StorageThreshold::new(),
        });

        MyNode::send_system_msg(msg, Peers::Multiple(recipients), context)
    }

    #[instrument(skip_all)]
    /// Send AntiEntropy update message to the nodes in our sibling section.
    pub(crate) fn send_updates_to_sibling_section(
        &self,
        prev_context: &NodeContext,
    ) -> Result<Vec<Cmd>> {
        debug!("{}", LogMarker::AeSendUpdateToSiblings);
        let sibling_prefix = prev_context.network_knowledge.prefix().sibling();
        if let Some(sibling_sap) = prev_context
            .network_knowledge
            .section_tree()
            .get_signed(&sibling_prefix)
        {
            let promoted_sibling_elders: BTreeSet<_> = sibling_sap
                .elders()
                .filter(|peer| !prev_context.network_knowledge.elders().contains(peer))
                .cloned()
                .collect();

            if promoted_sibling_elders.is_empty() {
                debug!("No promoted siblings found in our network knowledge to send AE-Update");
                return Ok(vec![]);
            }

            // Using previous_key as dst_section_key as newly promoted
            // sibling Elders shall still in the state of pre-split.
            let previous_section_key = prev_context.network_knowledge.section_key();

            // Send AE update to sibling section's new Elders
            Ok(vec![MyNode::send_ae_update_to_nodes(
                prev_context,
                promoted_sibling_elders,
                previous_section_key,
            )])
        } else {
            error!("Failed to get sibling SAP during split.");
            Ok(vec![])
        }
    }

    // Private helper to generate a SectionTreeUpdate to update
    // a peer abot our SAP, with proof_chain and members list.
    fn generate_ae_section_tree_update(
        context: &NodeContext,
        dst_section_key: Option<BlsPublicKey>,
    ) -> SectionTreeUpdate {
        let signed_sap = context.network_knowledge.signed_sap();

        let proof_chain = dst_section_key
            .and_then(|key| {
                context
                    .network_knowledge
                    .get_proof_chain_to_current_section(&key)
                    .ok()
            })
            .unwrap_or_else(|| context.network_knowledge.section_chain());

        SectionTreeUpdate::new(signed_sap, proof_chain)
    }

    /// returns (we_have_a_share_of_this_key, we_should_become_an_elder)
    fn does_section_tree_update_make_us_an_elder_with_key(
        context: &NodeContext,
        section_tree_update: &SectionTreeUpdate,
    ) -> (bool, bool) {
        let our_name = context.name;
        let sap = section_tree_update.signed_sap.clone();

        let we_have_a_share_of_this_key = context
            .section_keys_provider
            .key_share(&sap.section_key())
            .is_ok();

        // check we should be _becoming_ an elder
        let we_should_become_an_elder = sap.contains_elder(&our_name);
        (we_have_a_share_of_this_key, we_should_become_an_elder)
    }

    #[instrument(skip_all)]
    /// Test a context to see if we would update Network Knowledge
    /// returns
    ///   Ok(true) if the update had new valid information
    ///   Ok(false) if the update was valid but did not contain new information
    ///   Err(_) if the update was invalid
    pub(crate) fn would_we_update_network_knowledge(
        context: &NodeContext,
        section_tree_update: SectionTreeUpdate,
        members: Option<BTreeSet<SectionSigned<NodeState>>>,
    ) -> Result<bool> {
        let mut mutable_context = context.clone();
        let (we_have_a_share_of_this_key, we_should_become_an_elder) =
            MyNode::does_section_tree_update_make_us_an_elder_with_key(
                context,
                &section_tree_update,
            );

        trace!("we_have_a_share_of_this_key: {we_have_a_share_of_this_key}, we_should_become_an_elder: {we_should_become_an_elder}");

        if we_should_become_an_elder && !we_have_a_share_of_this_key {
            warn!("We should be an elder, but we're missing the keyshare!, ignoring update to wait until we have our keyshare");
            return Ok(false);
        };

        Ok(mutable_context
            .network_knowledge
            .update_knowledge_if_valid(section_tree_update, members, &context.name)?)
    }

    // Update's Network Knowledge
    // returns
    //   Ok(true) if the update had new valid information
    //   Ok(false) if the update was valid but did not contain new information
    //   Err(_) if the update was invalid
    pub(crate) fn update_network_knowledge(
        &mut self,
        context: &NodeContext,
        section_tree_update: SectionTreeUpdate,
        members: Option<BTreeSet<SectionSigned<NodeState>>>,
    ) -> Result<bool> {
        let (we_have_a_share_of_this_key, we_should_become_an_elder) =
            MyNode::does_section_tree_update_make_us_an_elder_with_key(
                context,
                &section_tree_update,
            );

        trace!("we_have_a_share_of_this_key: {we_have_a_share_of_this_key}, we_should_become_an_elder: {we_should_become_an_elder}");

        // This prevent us from updating our NetworkKnowledge based on an AE message where
        // we don't have the key share for the new SAP, making this node unable to sign section
        // messages and possibly being kicked out of the group of Elders.
        if we_should_become_an_elder && !we_have_a_share_of_this_key {
            warn!("We should be an elder, but we're missing the keyshare!, ignoring update to wait until we have our keyshare");
            return Ok(false);
        };

        Ok(self.network_knowledge.update_knowledge_if_valid(
            section_tree_update,
            members,
            &context.name,
        )?)
    }

    #[instrument(skip_all)]
    pub(crate) async fn handle_anti_entropy_msg(
        node: Arc<RwLock<MyNode>>,
        starting_context: NodeContext,
        section_tree_update: SectionTreeUpdate,
        kind: AntiEntropyKind,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        debug!("[NODE READ]: handling AE read gottt...");
        let sap = section_tree_update.signed_sap.value.clone();

        let members = if let AntiEntropyKind::Update { members } = &kind {
            Some(members.clone())
        } else {
            None
        };

        let mut cmds = vec![];

        // block off the write lock
        let updated = {
            let should_update = MyNode::would_we_update_network_knowledge(
                &starting_context,
                section_tree_update.clone(),
                members.clone(),
            )?;
            if should_update {
                let mut write_locked_node = node.write().await;
                debug!("[NODE WRITE]: handling AE write gottt...");
                let updated = write_locked_node.update_network_knowledge(
                    &starting_context,
                    section_tree_update,
                    members,
                )?;
                debug!("net knowledge udpated");
                // always run this, only changes will trigger events
                cmds.extend(
                    write_locked_node
                        .update_on_elder_change(&starting_context)
                        .await?,
                );
                debug!("updated for elder change");
                updated
            } else {
                false
            }
        };

        debug!("[NODE READ] Latest context read");
        let latest_context = node.read().await.context();
        debug!("[NODE READ] Latest context got.");
        // Only trigger reorganize data when there is a membership change happens.
        if updated && !latest_context.is_elder {
            // only done if adult, since as an elder we dont want to get any more
            // data for our name (elders will eventually be caching data in general)
            cmds.push(MyNode::ask_for_any_new_data(&latest_context).await);
        }

        if updated {
            MyNode::write_section_tree(&latest_context);
            let prefix = sap.prefix();
            info!("SectionTree written to disk with update for prefix {prefix:?}");

            // check if we've been kicked out of the section
            if starting_context
                .network_knowledge
                .members()
                .iter()
                .map(|m| m.name())
                .contains(&latest_context.name)
                && !latest_context
                    .network_knowledge
                    .members()
                    .iter()
                    .map(|m| m.name())
                    .contains(&latest_context.name)
            {
                error!("We've been removed from the section");

                return Err(Error::RemovedFromSection);
            }
        } else {
            debug!("No update to network knowledge");
        }

        // Check if we need to resend any messsages and who should we send it to.
        let (bounced_msg, response_peer) = match kind {
            AntiEntropyKind::Update { .. } => {
                // log the msg as received. Elders track this for other elders in dysfunction
                node.read()
                    .await
                    .untrack_node_issue(sender.name(), IssueType::AeProbeMsg);
                debug!("[NODE READ]: ae update lock received");
                return Ok(cmds);
            } // Nope, bail early
            AntiEntropyKind::Retry { bounced_msg } => {
                trace!("{}", LogMarker::AeResendAfterRetry);
                (bounced_msg, sender)
            }
            AntiEntropyKind::Redirect { bounced_msg } => {
                // We choose the Elder closest to the dst section key,
                // just to pick one of them in an arbitrary but deterministic fashion.
                let target_name = XorName::from(PublicKey::Bls(sap.section_key()));

                let chosen_dst_elder = if let Some(dst) = sap
                    .elders()
                    .max_by(|lhs, rhs| target_name.cmp_distance(&lhs.name(), &rhs.name()))
                {
                    *dst
                } else {
                    error!("Failed to find closest Elder to resend msg upon AE-Redirect response.");
                    return Ok(cmds);
                };

                trace!("{}", LogMarker::AeResendAfterRedirect);

                (bounced_msg, chosen_dst_elder)
            }
        };

        let (msg_to_resend, msg_id, dst) = match WireMsg::deserialize(bounced_msg)? {
            MsgType::Node {
                msg, msg_id, dst, ..
            } => (msg, msg_id, dst),
            _ => {
                warn!("Non System MsgType received in AE response. We do not handle any other type in AE msgs yet.");
                return Ok(cmds);
            }
        };

        // If the new SAP's section key is the same as the section key set when the
        // bounced message was originally sent, we just drop it.
        if dst.section_key == sap.section_key() {
            error!("Dropping bounced msg ({sender:?}) received in AE-Retry from {msg_id:?} as suggested new dst section key is the same as previously sent: {:?}", sap.section_key());
            return Ok(cmds);
        }

        trace!("Resend Original {msg_id:?} to {response_peer:?} with {msg_to_resend:?}");
        trace!("{}", LogMarker::AeResendAfterRedirect);

        cmds.push(MyNode::send_system_msg(
            msg_to_resend,
            Peers::Single(response_peer),
            latest_context,
        ));
        Ok(cmds)
    }

    // If entropy is found, determine the msg to send in order to
    // bring the sender's knowledge about us up to date.
    fn check_node_msg_for_entropy(
        wire_msg: &WireMsg,
        context: &NodeContext,
        dst: &Dst,
        sender: &Peer,
        send_stream: Option<Arc<Mutex<SendStream>>>,
    ) -> Result<Option<Cmd>> {
        let msg_id = wire_msg.msg_id();

        let ae_msg = match MyNode::check_for_entropy(context, msg_id, dst)? {
            None => return Ok(None),
            Some(AeResponseKind::Redirect(section_tree_update)) => {
                // Redirect to the closest section
                trace!(
                    "{} {msg_id:?} entropy found. {sender:?} should be updated",
                    LogMarker::AeSendRedirect
                );
                let bounced_msg = wire_msg.serialize()?;
                NodeMsg::AntiEntropy {
                    section_tree_update,
                    kind: AntiEntropyKind::Redirect { bounced_msg },
                }
            }
            Some(AeResponseKind::Retry(section_tree_update)) => {
                let bounced_msg = wire_msg.serialize()?;
                let ae_msg = NodeMsg::AntiEntropy {
                    section_tree_update,
                    kind: AntiEntropyKind::Retry { bounced_msg },
                };
                trace!(
                    "CMD of Sending AE message to {:?} with {:?}",
                    sender,
                    ae_msg
                );
                ae_msg
            }
        };

        if send_stream.is_some() {
            debug!("sending repsonse over send_stream");
            Ok(Some(Cmd::send_msg_via_response_stream(
                ae_msg,
                Peers::Single(*sender),
                send_stream,
                context.clone(),
            )))
        } else {
            debug!("sending repsonse over fresh conn");
            Ok(Some(Cmd::send_msg(
                ae_msg,
                Peers::Single(*sender),
                context.clone(),
            )))
        }
    }

    // If entropy is found, determine the `SectionTreeUpdate` and kind of AE response
    // to send in order to bring the sender's knowledge about us up to date.
    fn check_for_entropy(
        context: &NodeContext,
        msg_id: MsgId,
        dst: &Dst,
    ) -> Result<Option<AeResponseKind>> {
        // Check if the message has reached the correct section,
        // if not, we'll need to respond with AE
        let our_prefix = context.network_knowledge.prefix();
        // Let's try to find a section closer to the destination, if it's not for us.
        if !our_prefix.matches(&dst.name) {
            debug!(
                "AE: {msg_id:?} prefix not matching. We are: {:?}, they sent to: {:?}",
                our_prefix, dst.name
            );
            let closest_sap = context.network_knowledge.closest_signed_sap(&dst.name);
            return match closest_sap {
                Some((signed_sap, proof_chain)) => {
                    info!(
                        "{msg_id:?} Found a better matching prefix {:?}: {signed_sap:?}",
                        signed_sap.prefix()
                    );
                    let section_tree_update =
                        SectionTreeUpdate::new(signed_sap.clone(), proof_chain);
                    Ok(Some(AeResponseKind::Redirect(section_tree_update)))
                }
                None => {
                    // TODO: instead of just dropping the message, don't we actually need
                    // to get up to date info from other Elders in our section as it may be
                    // a section key we are not aware of yet?
                    // ...and once we acquired new key/s we attempt AE check again?
                    warn!(
                        "Anti-Entropy: cannot reply with redirect msg for dst_name {:?} and \
                        key {:?} to a closest section. Our SectionTree is empty.",
                        dst.name, dst.section_key
                    );
                    Err(Error::NoMatchingSection)
                }
            };
        }

        let our_section_key = context.network_knowledge.section_key();
        trace!(
            "Performing AE checks on {msg_id:?}, provided pk was: {:?} ours is: {:?}",
            dst.section_key,
            our_section_key
        );

        if dst.section_key == our_section_key {
            // Destination section key matches our current section key
            return Ok(None);
        }

        let section_tree_update =
            MyNode::generate_ae_section_tree_update(context, Some(dst.section_key));
        Ok(Some(AeResponseKind::Retry(section_tree_update)))
    }

    // Generate an AE response msg for the given message and send it to the client
    async fn send_ae_response_to_client(
        context: &NodeContext,
        sender: &Peer,
        client_response_stream: Arc<Mutex<SendStream>>,
        bounced_msg: UsrMsgBytes,
        section_tree_update: SectionTreeUpdate,
    ) -> Result<()> {
        trace!(
            "{} in send_ae_response_to_client {sender:?} ",
            LogMarker::AeSendRetryAsOutdated
        );

        let ae_msg = ClientDataResponse::AntiEntropy {
            section_tree_update,
            bounced_msg,
        };
        let (kind, payload) = MyNode::serialize_client_msg_response(context.name, ae_msg)?;
        let msg_id = MsgId::new();

        MyNode::send_msg_on_stream(
            context.network_knowledge.section_key(),
            payload,
            kind,
            client_response_stream,
            Some(*sender),
            msg_id,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{flow_ctrl::tests::network_builder::TestNetworkBuilder, MIN_ADULT_AGE};
    use sn_interface::{
        elder_count,
        messaging::{Dst, MsgId, MsgKind},
        network_knowledge::MyNodeInfo,
        test_utils::{gen_addr, prefix},
        types::keys::ed25519,
    };

    use assert_matches::assert_matches;
    use bls::SecretKey;
    use eyre::Result;
    use xor_name::Prefix;

    #[tokio::test]
    async fn ae_everything_up_to_date() -> Result<()> {
        // create an env with 3 churns in prefix0. And a single churn in prefix1
        let our_prefix = prefix("0");
        let other_prefix = prefix("1");
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(other_prefix, elder_count(), 0, None, None)
            .build();
        // get node from the latest section of our_prefix
        let node = env.get_nodes(our_prefix, 1, 0, None).remove(0);

        let dst_section_key = node.network_knowledge().section_key();
        let msg = create_msg(&our_prefix, dst_section_key)?;
        let sender = node.info().peer();
        let dst = Dst {
            name: our_prefix.substituted_in(xor_name::rand::random()),
            section_key: dst_section_key,
        };

        let context = node.context();

        let cmd = MyNode::check_node_msg_for_entropy(&msg, &context, &dst, &sender, None)?;

        assert!(cmd.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn ae_redirect_to_other_section() -> Result<()> {
        // create an env with 3 churns in prefix0. And a single churn in prefix1
        let our_prefix = prefix("0");
        let other_prefix = prefix("1");
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(other_prefix, elder_count(), 0, None, None)
            .build();
        let other_section = env.get_network_knowledge(other_prefix, None);
        let other_sap = other_section.signed_sap();

        // get node from the latest section of our_prefix
        let mut node = env.get_nodes(our_prefix, 1, 0, None).remove(0);

        let other_sk = bls::SecretKey::random();
        let other_pk = other_sk.public_key();

        let wire_msg = create_msg(&other_prefix, other_pk)?;
        let sender = node.info().peer();

        // since it's not aware of the other prefix, it will redirect to self
        let dst = Dst {
            section_key: other_pk,
            name: other_sap.prefix().name(),
        };
        let context = node.context();

        let cmd = MyNode::check_node_msg_for_entropy(&wire_msg, &context, &dst, &sender, None);

        let msg = assert_matches!(cmd, Ok(Some(Cmd::SendMsg { msg, .. })) => {
            msg
        });

        assert_matches!(msg, NodeMsg::AntiEntropy { section_tree_update, kind: AntiEntropyKind::Redirect {..}, .. } => {
            assert_eq!(section_tree_update.signed_sap, node.network_knowledge().signed_sap());
        });

        // now let's insert the other SAP to make it aware of the other prefix
        let section_tree_update =
            SectionTreeUpdate::new(other_sap.clone(), other_section.section_chain());
        assert!(node.update_network_knowledge(&context, section_tree_update, None,)?);

        let new_context = node.context();
        // and it now shall give us an AE redirect msg
        // with the SAP we inserted for other prefix
        let cmd = MyNode::check_node_msg_for_entropy(&wire_msg, &new_context, &dst, &sender, None);

        let msg = assert_matches!(cmd, Ok(Some(Cmd::SendMsg { msg, .. })) => {
            msg
        });

        assert_matches!(msg, NodeMsg::AntiEntropy { section_tree_update, kind: AntiEntropyKind::Redirect {..}, .. } => {
            assert_eq!(section_tree_update.signed_sap, other_sap);
        });
        Ok(())
    }

    #[tokio::test]
    async fn ae_outdated_dst_key_of_our_section() -> Result<()> {
        // create an env with 3 churns in prefix0. And a single churn in prefix1
        let our_prefix = prefix("0");
        let other_prefix = prefix("1");
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(other_prefix, elder_count(), 0, None, None)
            .build();
        // get node from the latest section of our_prefix
        let node = env.get_nodes(our_prefix, 1, 0, None).remove(0);

        let context = node.context();
        let msg = create_msg(&our_prefix, node.network_knowledge().section_key())?;
        let sender = node.info().peer();
        let dst = Dst {
            section_key: *node.network_knowledge().genesis_key(),
            name: our_prefix.substituted_in(xor_name::rand::random()),
        };

        let cmd = MyNode::check_node_msg_for_entropy(&msg, &context, &dst, &sender, None)?;

        let msg = assert_matches!(cmd, Some(Cmd::SendMsg { msg, .. }) => {
            msg
        });

        assert_matches!(&msg, NodeMsg::AntiEntropy { section_tree_update, kind: AntiEntropyKind::Retry{..}, .. } => {
            assert_eq!(section_tree_update.signed_sap, node.network_knowledge().signed_sap());
            assert_eq!(section_tree_update.proof_chain, node.section_chain());
        });
        Ok(())
    }

    #[tokio::test]
    async fn ae_wrong_dst_key_of_our_section_returns_retry() -> Result<()> {
        // create an env with 3 churns in prefix0. And a single churn in prefix1
        let our_prefix = prefix("0");
        let other_prefix = prefix("1");
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(other_prefix, elder_count(), 0, None, None)
            .build();
        // get node from the latest section of our_prefix
        let node = env.get_nodes(our_prefix, 1, 0, None).remove(0);

        let msg = create_msg(&our_prefix, node.network_knowledge().section_key())?;
        let sender = node.info().peer();

        let bogus_network_gen = bls::SecretKey::random();
        let dst = Dst {
            section_key: bogus_network_gen.public_key(),
            name: our_prefix.substituted_in(xor_name::rand::random()),
        };
        let context = node.context();

        let cmd = MyNode::check_node_msg_for_entropy(&msg, &context, &dst, &sender, None)?;

        let msg = assert_matches!(cmd, Some(Cmd::SendMsg { msg, .. }) => {
            msg
        });

        assert_matches!(&msg, NodeMsg::AntiEntropy { section_tree_update, kind: AntiEntropyKind::Retry {..}, .. } => {
            assert_eq!(section_tree_update.signed_sap, node.network_knowledge().signed_sap());
            assert_eq!(section_tree_update.proof_chain, node.section_chain());
        });
        Ok(())
    }

    fn create_msg(src_section_prefix: &Prefix, src_section_pk: BlsPublicKey) -> Result<WireMsg> {
        let sender = MyNodeInfo::new(
            ed25519::gen_keypair(&src_section_prefix.range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        // just some message we can construct easily
        let payload_msg = NodeMsg::AntiEntropyProbe(src_section_pk);
        let payload = WireMsg::serialize_msg_payload(&payload_msg)?;

        let dst = Dst {
            name: xor_name::rand::random(),
            section_key: SecretKey::random().public_key(),
        };
        let msg_id = MsgId::new();
        Ok(WireMsg::new_msg(
            msg_id,
            payload,
            MsgKind::Node(sender.name()),
            dst,
        ))
    }
}
