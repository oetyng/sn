// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::{core::NodeContext, flow_ctrl::cmds::Cmd, messaging::Peers, MyNode, Result},
    storage::Error as StorageError,
};
use qp2p::SendStream;
use sn_dysfunction::IssueType;
use sn_interface::{
    messaging::{
        data::{CmdResponse, StorageThreshold},
        system::{JoinResponse, NodeDataCmd, NodeDataQuery, NodeDataResponse, NodeEvent, NodeMsg},
        MsgId,
    },
    network_knowledge::NetworkKnowledge,
    types::{log_markers::LogMarker, Keypair, Peer, PublicKey, ReplicatedData},
};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use xor_name::{Prefix, XorName};

impl MyNode {
    /// Send a (`NodeMsg`) message to all Elders in our section
    pub(crate) fn send_msg_to_our_elders(context: &NodeContext, msg: NodeMsg) -> Cmd {
        let sap = context.network_knowledge.section_auth();
        let recipients = sap.elders_set();
        MyNode::send_system_msg(msg, Peers::Multiple(recipients), context.clone())
    }

    /// Send a (`NodeMsg`) message to a node
    /// Context is consumed and passed into the SendMsg command
    pub(crate) fn send_system_msg(msg: NodeMsg, recipients: Peers, context: NodeContext) -> Cmd {
        trace!("{}: {:?}", LogMarker::SendToNodes, msg);
        Cmd::send_msg(msg, recipients, context)
    }

    pub(crate) async fn store_data_as_adult_and_respond(
        context: &NodeContext,
        data: ReplicatedData,
        response_stream: Option<Arc<Mutex<SendStream>>>,
        target: Peer,
        original_msg_id: MsgId,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        let section_pk = PublicKey::Bls(context.network_knowledge.section_key());
        let node_keypair = Keypair::Ed25519(context.keypair.clone());
        let data_addr = data.address();
        let our_node_name = context.name;

        trace!("About to store data from {original_msg_id:?}: {data_addr:?}");

        // We are an adult here, so just store away!
        // This may return a NotEnoughSpace error... but we should have reported storage increase
        // well before this
        let response = match context
            .data_storage
            .store(&data, section_pk, node_keypair.clone())
            .await
        {
            Ok(level_report) => {
                trace!("Data has been stored: {data_addr:?}");
                info!("Storage level report: {:?}", level_report);
                cmds.extend(MyNode::record_storage_level_if_any(context, level_report)?);
                CmdResponse::ok(data)?
            }
            Err(StorageError::NotEnoughSpace) => {
                // storage full
                error!("Not enough space to store data {data_addr:?}");
                let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                    node_id: PublicKey::from(context.keypair.public),
                    data: data.clone(),
                    full: true,
                });

                cmds.push(MyNode::send_msg_to_our_elders(context, msg));
                CmdResponse::err(data, StorageError::NotEnoughSpace.into())?
            }
            Err(error) => {
                // the rest seem to be non-problematic errors.. (?)
                // this could be an "we already have it" error... so we should continue with that...
                error!("Problem storing data {data_addr:?}, but it was ignored: {error}");
                CmdResponse::ok(data)?
            }
        };

        if let Some(stream) = response_stream {
            let msg = NodeDataResponse::CmdResponse {
                response,
                correlation_id: original_msg_id,
            };
            let (kind, payload) = MyNode::serialize_node_msg_response(our_node_name, msg)?;

            MyNode::send_msg_on_stream(
                context.network_knowledge.section_key(),
                payload,
                kind,
                stream,
                Some(target),
                original_msg_id,
            )
            .await?;
        } else {
            error!("Cannot respond over stream, none exists after storing! {data_addr:?}");
        }

        Ok(cmds)
    }

    // Handler for data messages which have successfully
    // passed all signature checks and msg verifications
    pub(crate) async fn handle_valid_node_msg(
        node: Arc<RwLock<MyNode>>,
        context: NodeContext,
        msg_id: MsgId,
        msg: NodeMsg,
        sender: Peer,
        send_stream: Option<Arc<Mutex<SendStream>>>,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?}: {msg_id:?}", LogMarker::NodeMsgToBeHandled);

        match msg {
            NodeMsg::Relocate(node_state) => {
                let mut node = node.write().await;
                debug!("[NODE WRITE]: Relocated write gottt...");

                trace!("Handling msg: Relocate from {}: {:?}", sender, msg_id);
                Ok(node.handle_relocate(node_state)?.into_iter().collect())
            }
            NodeMsg::JoinAsRelocatedResponse(join_response) => {
                let mut node = node.write().await;
                debug!("[NODE WRITE]: joinasreloac write gottt...");
                trace!("Handling msg: JoinAsRelocatedResponse from {}", sender);
                if let Some(ref mut joining_as_relocated) = node.relocate_state {
                    if let Some(cmd) =
                        joining_as_relocated.handle_join_response(*join_response, sender.addr())?
                    {
                        return Ok(vec![cmd]);
                    }
                } else {
                    error!(
                        "No relocation in progress upon receiving {:?}",
                        join_response
                    );
                }

                Ok(vec![])
            }
            NodeMsg::AntiEntropy {
                section_tree_update,
                kind,
            } => {
                trace!("Handling msg: AE from {sender}: {msg_id:?}");
                // as we've data storage reqs inside here for reorganisation, we have async calls to
                // the fs
                MyNode::handle_anti_entropy_msg(node, context, section_tree_update, kind, sender)
                    .await
            }
            // Respond to a probe msg
            // We always respond to probe msgs if we're an elder as health checks use this to see if a node is alive
            // and repsonsive, as well as being a method of keeping nodes up to date.
            NodeMsg::AntiEntropyProbe(section_key) => {
                debug!("Aeprobe in");

                let mut cmds = vec![];
                if !context.is_elder {
                    info!("Dropping AEProbe since we are not an elder");
                    // early return here as we do not get health checks as adults,
                    // normal AE rules should have applied
                    return Ok(cmds);
                }

                trace!("Received Probe message from {}: {:?}", sender, msg_id);
                let recipients = BTreeSet::from_iter([sender]);
                cmds.push(MyNode::send_ae_update_to_nodes(
                    &context,
                    recipients,
                    section_key,
                ));
                Ok(cmds)
            }
            // The AcceptedOnlineShare for relocation will be received here.
            NodeMsg::JoinResponse(join_response) => {
                let mut node = node.write().await;

                match join_response {
                    JoinResponse::Approved { .. } => {
                        info!(
                            "Relocation: Aggregating received ApprovalShare from {:?}",
                            sender
                        );
                        info!("Relocation: Successfully aggregated ApprovalShares for joining the network");
                        if let Some(ref mut joining_as_relocated) = node.relocate_state {
                            let new_node = joining_as_relocated.node.clone();
                            let new_name = new_node.name();
                            let previous_name = context.name;
                            let new_keypair = new_node.keypair;

                            info!(
                                "Relocation: switching from {:?} to {:?} with keypair {:?}",
                                previous_name, new_name, new_keypair
                            );

                            let section_tree = node.network_knowledge.section_tree().clone();
                            let section_tree_update = node
                                .network_knowledge
                                .section_tree()
                                .generate_section_tree_update(&Prefix::default())?; // TODO: remove this dummy update
                            let new_network_knowledge =
                                NetworkKnowledge::new(section_tree, section_tree_update)?;

                            // TODO: confirm whether carry out the switch immediately here
                            //       or still using the cmd pattern.
                            //       As the sending of the JoinRequest as notification
                            //       may require the `node` to be switched to new already.
                            node.relocate(new_keypair, new_network_knowledge)?;

                            trace!("{}", LogMarker::RelocateEnd);
                        } else {
                            warn!("Relocation:  node.relocate_state is not in Progress");
                        }

                        Ok(vec![])
                    }
                    _ => {
                        debug!("Relocation: Ignoring unexpected join response message: {join_response:?}");
                        Ok(vec![])
                    }
                }
            }
            NodeMsg::HandoverVotes(votes) => {
                let mut node = node.write().await;
                debug!("[NODE WRITE]: handover votes write gottt...");
                node.handle_handover_msg(sender, votes)
            }
            NodeMsg::HandoverAE(gen) => {
                debug!("[NODE READ]: handover ae attempts");
                let node = node.read().await;
                debug!("[NODE READ]: handover ae got");

                Ok(node
                    .handle_handover_anti_entropy(sender, gen)
                    .into_iter()
                    .collect())
            }
            NodeMsg::JoinRequest(join_request) => {
                trace!("Handling msg {:?}: JoinRequest from {}", msg_id, sender);

                MyNode::handle_join_request(node, &context, sender, join_request)
                    .await
                    .map(|c| c.into_iter().collect())
            }
            NodeMsg::JoinAsRelocatedRequest(join_request) => {
                trace!("Handling msg: JoinAsRelocatedRequest from {}", sender);

                if !context.is_elder
                    && join_request.section_key == context.network_knowledge.section_key()
                {
                    return Ok(vec![]);
                }

                Ok(
                    MyNode::handle_join_as_relocated_request(node, &context, sender, *join_request)
                        .await
                        .into_iter()
                        .collect(),
                )
            }
            NodeMsg::MembershipVotes(votes) => {
                let mut node = node.write().await;
                debug!("[NODE WRITE]: MembershipVotes write gottt...");
                let mut cmds = vec![];
                cmds.extend(node.handle_membership_votes(sender, votes)?);
                Ok(cmds)
            }
            NodeMsg::MembershipAE(gen) => {
                let (node_context, membership_context) = {
                    debug!("[NODE READ]: membership ae read ");
                    let membership = node.read().await.membership.clone();
                    debug!("[NODE READ]: membership ae read got");
                    (context, membership)
                };

                Ok(MyNode::handle_membership_anti_entropy(
                    membership_context,
                    node_context,
                    sender,
                    gen,
                )
                .into_iter()
                .collect())
            }
            NodeMsg::Propose {
                proposal,
                sig_share,
                optional_sig_share,
            } => {
                let mut node = node.write().await;
                debug!("[NODE WRITE]: PROPOSE write gottt...");
                if node.is_not_elder() {
                    trace!("Adult handling a Propose msg from {}: {:?}", sender, msg_id);
                }

                trace!(
                    "Handling proposal msg: {proposal:?} from {}: {:?}",
                    sender,
                    msg_id
                );

                node.handle_proposal(msg_id, proposal, sig_share, optional_sig_share, sender)
            }
            NodeMsg::DkgStart(session_id, elder_sig) => {
                trace!(
                    "Handling msg: DkgStart s{} {:?}: {} elders from {}",
                    session_id.sh(),
                    session_id.prefix,
                    session_id.elders.len(),
                    sender
                );

                let mut node = node.write().await;
                debug!("[NODE WRITE]: DKGstart write gottt...");
                node.untrack_node_issue(sender.name(), IssueType::Dkg);
                node.handle_dkg_start(session_id, elder_sig)
            }
            NodeMsg::DkgEphemeralPubKey {
                session_id,
                section_auth,
                pub_key,
                sig,
            } => {
                trace!(
                    "{} s{} from {}",
                    LogMarker::DkgHandleEphemeralPubKey,
                    session_id.sh(),
                    sender
                );
                let mut node = node.write().await;
                debug!("[NODE WRITE]: DKG Ephemeral write gottt...");
                node.handle_dkg_ephemeral_pubkey(&session_id, section_auth, pub_key, sig, sender)
            }
            NodeMsg::DkgVotes {
                session_id,
                pub_keys,
                votes,
            } => {
                trace!(
                    "{} s{} from {}: {:?}",
                    LogMarker::DkgVotesHandling,
                    session_id.sh(),
                    sender,
                    votes
                );
                let mut node = node.write().await;
                debug!("[NODE WRITE]: DKG Votes write gottt...");

                node.untrack_node_issue(sender.name(), IssueType::Dkg);

                node.handle_dkg_votes(&session_id, pub_keys, votes, sender)
            }
            NodeMsg::DkgAE(session_id) => {
                debug!("[NODE READ]: dkg ae read ");

                let node = node.read().await;
                debug!("[NODE READ]: dkg ae read got");
                trace!("Handling msg: DkgAE s{} from {}", session_id.sh(), sender);
                node.handle_dkg_anti_entropy(session_id, sender)
            }
            NodeMsg::NodeEvent(NodeEvent::StorageThresholdReached { node_id, level, .. }) => {
                let mut node = node.write().await;
                debug!("[NODE WRITE]: StorageThresholdReached write gottt...");
                let changed = node.set_adult_full(&node_id, level);
                if changed {
                    // ..then we accept a new node in place of the full node
                    node.joins_allowed = true;
                    if node.are_majority_of_adults_full() {
                        // ..then we accept new nodes until we split
                        node.joins_allowed_until_split = true;
                    }
                }
                Ok(vec![])
            }
            NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                node_id,
                data,
                full,
            }) => {
                info!(
                    "Processing CouldNotStoreData event with MsgId: {:?}",
                    msg_id
                );

                if !context.is_elder {
                    error!("Received unexpected message while Adult");
                    return Ok(vec![]);
                }

                if full {
                    let mut node = node.write().await;
                    debug!("[NODE WRITE]: CouldNotStore write gottt...");
                    let changed = node.set_adult_full(&node_id, StorageThreshold::new());
                    if changed {
                        // ..then we accept a new node in place of the full node
                        node.joins_allowed = true;
                        if node.are_majority_of_adults_full() {
                            // ..then we accept new nodes until we split
                            node.joins_allowed_until_split = true;
                        }
                    }
                }

                let targets = MyNode::target_data_holders(&context, data.name());

                // TODO: handle responses where replication failed...
                let _results =
                    MyNode::replicate_data_to_adults(&context, data, msg_id, targets).await?;

                Ok(vec![])
            }
            NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateOneData(data)) => {
                debug!("Replicate one data");

                if context.is_elder {
                    error!("Received unexpected message while Elder");
                    return Ok(vec![]);
                }
                debug!(
                    "Attempting to store data locally as adult: {:?}",
                    data.address()
                );

                // store data and respond w/ack on the response stream
                MyNode::store_data_as_adult_and_respond(&context, data, send_stream, sender, msg_id)
                    .await
            }
            NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateData(data_collection)) => {
                info!("ReplicateData collection MsgId: {:?}", msg_id);

                if context.is_elder {
                    error!("Received unexpected message while Elder");
                    return Ok(vec![]);
                }

                let mut cmds = vec![];

                let section_pk = PublicKey::Bls(context.network_knowledge.section_key());
                let node_keypair = Keypair::Ed25519(context.keypair.clone());

                for data in data_collection {
                    // grab the write lock each time in the loop to not hold it over large data sets
                    let store_result = context
                        .data_storage
                        .store(&data, section_pk, node_keypair.clone())
                        .await;

                    // We are an adult here, so just store away!
                    // This may return a DatabaseFull error... but we should have reported storage increase
                    // well before this
                    match store_result {
                        Ok(level_report) => {
                            info!("Storage level report: {:?}", level_report);
                            cmds.extend(MyNode::record_storage_level_if_any(
                                &context,
                                level_report,
                            )?);

                            info!("End of message flow.");
                        }
                        Err(StorageError::NotEnoughSpace) => {
                            // storage full
                            error!("Not enough space to store more data");

                            let node_id = PublicKey::from(context.keypair.public);
                            let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                                node_id,
                                data,
                                full: true,
                            });

                            cmds.push(MyNode::send_msg_to_our_elders(&context, msg))
                        }
                        Err(error) => {
                            // the rest seem to be non-problematic errors.. (?)
                            error!("Problem storing data, but it was ignored: {error}");
                        }
                    }
                }

                Ok(cmds)
            }
            NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(known_data_addresses)) => {
                info!(
                    "{:?} MsgId: {:?}",
                    LogMarker::RequestForAnyMissingData,
                    msg_id
                );

                Ok(
                    MyNode::get_missing_data_for_node(&context, sender, known_data_addresses)
                        .await
                        .into_iter()
                        .collect(),
                )
            }
            NodeMsg::NodeDataQuery(NodeDataQuery {
                query,
                auth,
                operation_id,
            }) => {
                // A request from EndUser - via elders - for locally stored data
                debug!(
                    "Handle NodeQuery with msg_id {:?}, operation_id {}",
                    msg_id, operation_id
                );

                MyNode::handle_data_query_at_adult(
                    &context,
                    operation_id,
                    &query,
                    auth,
                    sender,
                    msg_id,
                    send_stream,
                )
                .await?;
                Ok(vec![])
            }
        }
    }

    /// Sets Cmd to locally record the storage level and send msgs to Elders
    /// Advising the same
    fn record_storage_level_if_any(
        context: &NodeContext,
        level: Option<StorageThreshold>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        if let Some(level) = level {
            info!("Storage has now reached {} % used.", level.value());

            // Run a SetStorageThresholdReached Cmd to actually update the DataStorage instance
            cmds.push(Cmd::SetStorageThresholdReached(level));
            let node_id = PublicKey::from(context.keypair.public);
            let node_xorname = XorName::from(node_id);

            // we ask the section to record the new level reached
            let msg = NodeMsg::NodeEvent(NodeEvent::StorageThresholdReached {
                section: node_xorname,
                node_id,
                level,
            });

            let dst = Peers::Multiple(context.network_knowledge.elders());

            cmds.push(Cmd::send_msg(msg, dst, context.clone()));
        }

        Ok(cmds)
    }
}
