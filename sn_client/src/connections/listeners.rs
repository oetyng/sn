// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Session;

use crate::{
    connections::{
        messaging::{send_msg, NUM_OF_ELDERS_SUBSET_FOR_QUERIES},
        PendingCmdAcks,
    },
    Error, Result,
};

use sn_interface::{
    at_least_one_correct_elder,
    messaging::{
        data::{CmdError, ServiceMsg},
        system::{KeyedSig, SectionAuth, SystemMsg},
        AuthKind, AuthorityProof, Dst, MsgId, MsgType, ServiceAuth, WireMsg,
    },
    network_knowledge::{
        utils::compare_and_write_prefix_map_to_disk, NetworkKnowledge, SectionAuthorityProvider,
    },
    types::{log_markers::LogMarker, Peer},
};

use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use itertools::Itertools;
use qp2p::{Close, ConnectionError, ConnectionIncoming as IncomingMsgs, SendError};
use rand::{rngs::OsRng, seq::SliceRandom};
use secured_linked_list::SecuredLinkedList;
use std::net::SocketAddr;
use tracing::Instrument;

impl Session {
    // Listen for incoming msgs on a connection
    #[instrument(skip_all, level = "debug")]
    pub(crate) fn spawn_msg_listener_thread(
        session: Session,
        peer: Peer,
        conn: qp2p::Connection,
        mut incoming_msgs: IncomingMsgs,
    ) {
        let mut first = true;
        let addr = peer.addr();
        let connection_id = conn.id();

        debug!("Listening for incoming msgs from {:?}", peer);

        let _handle = tokio::spawn(async move {
            loop {
                match Self::listen_for_incoming_msg(addr, &mut incoming_msgs).await {
                    Ok(Some(msg)) => {
                        if first {
                            first = false;
                            session.peer_links.add_incoming(&peer, conn.clone()).await;
                        }

                        if let Err(err) = Self::handle_msg(msg, peer, session.clone()).await {
                            error!("Error while handling incoming msg: {:?}. Listening for next msg...", err);
                        }
                    },
                    Ok(None) => {
                        // once the msg loop breaks, we know this specific connection is closed
                        break;
                    }
                    Err( Error::QuicP2pSend(SendError::ConnectionLost(
                        ConnectionError::Closed(Close::Application { reason, .. }),
                    ))) => {
                        warn!(
                            "Connection was closed by the node: {:?}",
                            String::from_utf8(reason.to_vec())
                        );

                        break;

                    },
                    Err(Error::QuicP2p(qp2p_err)) => {
                          // TODO: Can we recover here?
                          info!("Error from Qp2p received, closing listener loop. {:?}", qp2p_err);
                          break;
                    },
                    Err(error) => {
                        error!("Error while processing incoming msg: {:?}. Listening for next msg...", error);
                    }
                }
            }

            // once the msg loop breaks, we know the connection is closed
            trace!("{} to {} (id: {})", LogMarker::ConnectionClosed, addr, connection_id);

        }.instrument(info_span!("Listening for incoming msgs from {}", ?addr))).in_current_span();
    }

    #[instrument(skip_all, level = "debug")]
    pub(crate) async fn listen_for_incoming_msg(
        src: SocketAddr,
        incoming_msgs: &mut IncomingMsgs,
    ) -> Result<Option<MsgType>, Error> {
        if let Some(msg) = incoming_msgs.next().await? {
            trace!("Incoming msg from {:?}", src);
            let wire_msg = WireMsg::from(msg)?;
            let msg_type = wire_msg.into_msg()?;

            #[cfg(feature = "traceroute")]
            {
                info!(
                    "Message {} with the Traceroute received at client: {:?}",
                    msg_type,
                    wire_msg.show_trace()
                )
            }

            Ok(Some(msg_type))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip_all, level = "debug")]
    pub(crate) async fn handle_msg(
        msg: MsgType,
        src_peer: Peer,
        session: Session,
    ) -> Result<(), Error> {
        match msg.clone() {
            MsgType::Service { msg_id, msg, .. } => {
                Self::handle_client_msg(session, msg_id, msg, src_peer)
            }
            MsgType::System {
                msg:
                    SystemMsg::AntiEntropyRedirect {
                        section_auth,
                        section_signed,
                        section_chain,
                        bounced_msg,
                    },
                msg_authority,
                ..
            } => {
                let sys_msg = SystemMsg::AntiEntropyRedirect {
                    section_auth: section_auth.clone(),
                    section_signed: section_signed.clone(),
                    section_chain: section_chain.clone(),
                    bounced_msg: bounced_msg.clone(),
                };
                // check that the message can be trusted based upon our network knowledge

                // Let's now verify the section key in the msg authority is trusted
                // based on our current knowledge of the network and sections chains.
                let known_keys: Vec<BlsPublicKey> = session
                    .all_sections_chains
                    .read()
                    .await
                    .keys()
                    .cloned()
                    .collect();

                if !NetworkKnowledge::verify_node_msg_can_be_trusted(
                    msg_authority.clone(),
                    sys_msg,
                    &known_keys,
                ) {
                    warn!(
                        "Untrusted message has been dropped, from {:?}: {:?} ",
                        src_peer, msg
                    );
                    return Err(Error::UntrustedMessage);
                }

                // Okay, we can carry on
                debug!("AE-Redirect msg received");
                let result = Self::handle_ae_msg(
                    session,
                    section_auth.into_state(),
                    section_signed,
                    section_chain,
                    bounced_msg,
                    src_peer,
                )
                .await;
                if result.is_err() {
                    error!(
                        "Failed to handle AE-Redirect msg from {:?}, {result:?}",
                        src_peer.addr()
                    );
                }
                result
            }
            MsgType::System {
                msg:
                    SystemMsg::AntiEntropyRetry {
                        section_auth,
                        section_signed,
                        bounced_msg,
                        proof_chain,
                    },
                ..
            } => {
                debug!("AE-Retry msg received");
                let result = Self::handle_ae_msg(
                    session,
                    section_auth.into_state(),
                    section_signed,
                    proof_chain,
                    bounced_msg,
                    src_peer,
                )
                .await;
                if result.is_err() {
                    error!("Failed to handle AE-Retry msg from {:?}", src_peer.addr());
                }
                result
            }
            msg_type => {
                warn!("Unexpected msg type received: {:?}", msg_type);
                Ok(())
            }
        }
    }

    #[instrument(skip(cmds), level = "debug")]
    fn send_cmd_response(
        cmds: PendingCmdAcks,
        correlation_id: MsgId,
        src: SocketAddr,
        error: Option<CmdError>,
    ) {
        if let Some(sender) = cmds.get(&correlation_id) {
            trace!(
                "Sending cmd response from {:?} for cmd w/{:?} via channel.",
                src,
                correlation_id
            );
            let result = sender.try_send((src, error));
            if result.is_err() {
                trace!("Error sending cmd response on a channel for cmd_id {:?}: {:?}. (It has likely been removed)", correlation_id, result)
            }
        } else {
            // Likely the channel is removed when received majority of Acks
            trace!("No channel found for cmd Ack of {:?}", correlation_id);
        }
    }

    // Handle msgs intended for client consumption (re: queries + cmds)
    #[instrument(skip(session), level = "debug")]
    fn handle_client_msg(
        session: Session,
        msg_id: MsgId,
        msg: ServiceMsg,
        src_peer: Peer,
    ) -> Result<(), Error> {
        debug!(
            "ServiceMsg with id {:?} received from {:?}",
            msg_id,
            src_peer.addr()
        );
        let queries = session.pending_queries.clone();
        let cmds = session.pending_cmds;

        let _handle = tokio::spawn(async move {
            match msg {
                ServiceMsg::QueryResponse {
                    response,
                    correlation_id,
                } => {
                    trace!(
                        "ServiceMsg with id {:?} is QueryResponse regarding {:?} with response {:?}",
                        msg_id,
                        correlation_id,
                        response,
                    );
                    // Note that this doesn't remove the sender from here since multiple
                    // responses corresponding to the same msg ID might arrive.
                    // Once we are satisfied with the response this is channel is discarded in
                    // ConnectionManager::send_query

                    if let Ok(op_id) = response.operation_id() {
                        if let Some(entry) = queries.get(&op_id) {
                            let all_senders = entry.value();
                            // Only valid response shall get broadcast to all
                            for (ori_msg_id, sender) in all_senders {
                                let res = if response.is_success() || ori_msg_id == &correlation_id
                                {
                                    sender.try_send(response.clone())
                                } else {
                                    continue;
                                };
                                if res.is_err() {
                                    trace!("Error relaying query response internally on a channel for {:?} op_id {:?}: {:?}. (It has likely been removed)", msg_id, op_id, res)
                                }
                            }
                        } else {
                            // TODO: The trace is only needed when we have an identified case of not finding a channel, but expecting one.
                            // When expecting one, we can log "No channel found for operation", (and then probably at warn or error level).
                            // But when we have received enough responses, we aren't really expecting a channel there, so there is no reason to log anything.
                            // Right now, if we have already received enough responses for a query,
                            // we drop the channels and drop any further responses for that query.
                            // but we should not drop it immediately, but clean it up after a while
                            // and then not log that "no channel was found" when we already had enough responses.
                            //trace!("No channel found for operation {}", op_id);
                        }
                    } else {
                        warn!("Ignoring query response without operation id");
                    }
                }
                ServiceMsg::CmdError {
                    error,
                    correlation_id,
                    ..
                } => {
                    warn!("CmdError was received for {correlation_id:?}: {:?}", error);
                    Self::send_cmd_response(cmds, correlation_id, src_peer.addr(), Some(error));
                }
                ServiceMsg::CmdAck { correlation_id } => {
                    debug!(
                        "CmdAck was received for Message{:?} w/ID: {:?} from {:?}",
                        msg_id,
                        correlation_id,
                        src_peer.addr()
                    );
                    Self::send_cmd_response(cmds, correlation_id, src_peer.addr(), None);
                }
                _ => {
                    warn!("Ignoring unexpected msg type received: {:?}", msg);
                }
            };
        });

        Ok(())
    }

    // Handle Anti-Entropy Redirect or Retry msgs
    #[instrument(skip_all, level = "debug")]
    async fn handle_ae_msg(
        session: Session,
        target_sap: SectionAuthorityProvider,
        section_signed: KeyedSig,
        provided_section_chain: SecuredLinkedList,
        bounced_msg: Bytes,
        src_peer: Peer,
    ) -> Result<(), Error> {
        debug!(
            "Received Anti-Entropy from {}, with SAP: {:?}",
            src_peer.addr(),
            target_sap
        );

        // Try to update our network knowledge first
        Self::update_network_knowledge(
            &session,
            target_sap.clone(),
            section_signed,
            provided_section_chain,
            src_peer,
        )
        .await;

        if let Some((msg_id, elders, service_msg, dst, auth)) =
            Self::new_target_elders(bounced_msg.clone(), &target_sap).await?
        {
            let ae_msg_src_name = src_peer.name();
            // here we send this to only one elder for each AE message we get in. We _should_ have one per elder we sent to.
            // deterministically send to most elder based upon sender
            let target_elder = elders
                .iter()
                .sorted_by(|lhs, rhs| ae_msg_src_name.cmp_distance(&lhs.name(), &rhs.name()))
                .cloned()
                .collect_vec()
                .pop();

            // there should always be one
            if let Some(elder) = target_elder {
                let payload = WireMsg::serialize_msg_payload(&service_msg)?;
                let wire_msg =
                    WireMsg::new_msg(msg_id, payload, AuthKind::Service(auth.into_inner()), dst);

                debug!("Resending original message on AE-Redirect with updated details. Expecting an AE-Retry next");

                send_msg(session, vec![elder], wire_msg, msg_id).await?;
            } else {
                error!("No elder determined for resending AE message");
            }
        }

        Ok(())
    }

    /// Update our network knowledge making sure proof chain validates the
    /// new SAP based on currently known remote section SAP or genesis key.
    async fn update_network_knowledge(
        session: &Session,
        sap: SectionAuthorityProvider,
        section_signed: KeyedSig,
        proof_chain: SecuredLinkedList,
        sender: Peer,
    ) {
        // Update our network PrefixMap based upon passed in knowledge
        match session.network.verify_with_chain_and_update(
            SectionAuth {
                value: sap.clone(),
                sig: section_signed,
            },
            &proof_chain,
            &*session.all_sections_chains.read().await,
        ) {
            Ok(true) => {
                debug!(
                    "Anti-Entropy: updated remote section SAP updated for {:?}",
                    sap.prefix()
                );
                // Update the PrefixMap on disk
                if let Err(e) = compare_and_write_prefix_map_to_disk(&session.network).await {
                    error!(
                        "Error writing freshly updated PrefixMap to client dir: {:?}",
                        e
                    );
                }
            }
            Ok(false) => {
                debug!(
                    "Anti-Entropy: discarded SAP for {:?} since it's the same as the one in our records: {:?}",
                    sap.prefix(), sap
                );
            }
            Err(err) => {
                warn!(
                    "Anti-Entropy: failed to update remote section SAP w/ err: {:?}",
                    err
                );
                warn!(
                    "Anti-Entropy: bounced msg dropped. Failed section auth was {:?} sent by: {:?}",
                    sap.section_key(),
                    sender
                );
                return;
            }
        }

        // Since the proof chain is valid (we've verified that in above step when
        // updating our PrefixMap), let's now update our knowledge of all sections chains
        match session.all_sections_chains.write().await.join(proof_chain.clone()) {
            Ok(()) => debug!(
                "Anti-Entropy: updated our knowledge of network sections chains with proof chain {:?}",
                proof_chain
            ),
            Err(e) =>
            error!(
                "Error updating our knowledge of all sections chains: {:?}",
                e
            )
        }
    }

    /// Checks AE cache to see if we should be forwarding this msg (and to whom)
    /// or if it has already been dealt with
    #[instrument(skip_all, level = "debug")]
    async fn new_target_elders(
        bounced_msg: Bytes,
        received_auth: &SectionAuthorityProvider,
    ) -> Result<
        Option<(
            MsgId,
            Vec<Peer>,
            ServiceMsg,
            Dst,
            AuthorityProof<ServiceAuth>,
        )>,
        Error,
    > {
        let (msg_id, service_msg, dst, auth) = match WireMsg::deserialize(bounced_msg.clone())? {
            MsgType::Service {
                msg_id,
                msg,
                auth,
                dst,
            } => (msg_id, msg, dst, auth),
            other => {
                warn!(
                    "Unexpected non-serviceMsg returned in AE-Redirect response: {:?}",
                    other
                );
                return Ok(None);
            }
        };

        trace!(
            "Bounced msg ({:?}) received in an AE response: {:?}",
            msg_id,
            service_msg
        );

        let (target_count, dst_address_of_bounced_msg) = match service_msg.clone() {
            ServiceMsg::Cmd(cmd) => (at_least_one_correct_elder(), cmd.dst_name()),
            ServiceMsg::Query(query) => {
                (NUM_OF_ELDERS_SUBSET_FOR_QUERIES, query.variant.dst_name())
            }
            _ => {
                warn!(
                    "Invalid bounced msg {:?} received in AE response: {:?}. Msg is of invalid type",
                    msg_id,
                    service_msg
                );
                // Early return with random name as we will discard the msg at the caller func
                return Ok(None);
            }
        };

        let target_public_key;

        // We normally have received auth when we're in AE-Redirect
        let mut target_elders: Vec<_> = {
            target_public_key = received_auth.section_key();

            received_auth
                .elders_vec()
                .into_iter()
                .sorted_by(|lhs, rhs| {
                    dst_address_of_bounced_msg.cmp_distance(&lhs.name(), &rhs.name())
                })
                .take(target_count)
                .collect()
        };
        // shuffle so elders sent to is random for better availability
        target_elders.shuffle(&mut OsRng);

        // Let's rebuild the msg with the updated destination details
        let dst = Dst {
            name: dst.name,
            section_key: target_public_key,
        };

        if !target_elders.is_empty() {
            debug!(
                "Final target elders for resending {:?} : {:?} msg are {:?}",
                msg_id, service_msg, target_elders
            );
        }

        Ok(Some((msg_id, target_elders, service_msg, dst, auth)))
    }
}
