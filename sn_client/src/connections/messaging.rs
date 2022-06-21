// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{QueryResult, Session};

use crate::{connections::CmdResponse, Error, Result};

use sn_interface::{
    messaging::{
        data::{CmdError, DataQuery, QueryResponse},
        AuthKind, DstLocation, MsgId, ServiceAuth, WireMsg,
    },
    network_knowledge::{prefix_map::NetworkPrefixMap, supermajority},
    types::{Peer, PeerLinks, SendToOneError},
};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::join_all;
use qp2p::{Close, Config as QuicP2pConfig, ConnectionError, Endpoint, SendError};
use rand::{rngs::OsRng, seq::SliceRandom};
use secured_linked_list::SecuredLinkedList;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc::{channel, Sender},
    sync::RwLock,
    task::JoinHandle,
};
use tracing::{debug, error, trace, warn};
use xor_name::XorName;

// Number of Elders subset to send queries to
pub(crate) const NUM_OF_ELDERS_SUBSET_FOR_QUERIES: usize = 3;

// Number of bootstrap nodes to attempt to contact per batch (if provided by the node_config)
pub(crate) const NODES_TO_CONTACT_PER_STARTUP_BATCH: usize = 3;

// Duration of wait for the node to have chance to pickup network knowledge at the beginning
const INITIAL_WAIT: u64 = 1;

// Number of retries for sending a message due to a connection issue.
const CLIENT_SEND_RETRIES: usize = 1;

impl Session {
    /// Acquire a session by bootstrapping to a section, maintaining connections to several nodes.
    #[instrument(skip(err_sender), level = "debug")]
    pub(crate) fn new(
        genesis_key: bls::PublicKey,
        qp2p_config: QuicP2pConfig,
        err_sender: Sender<CmdError>,
        local_addr: SocketAddr,
        cmd_ack_wait: Duration,
        prefix_map: NetworkPrefixMap,
    ) -> Result<Session> {
        let endpoint = Endpoint::new_client(local_addr, qp2p_config)?;
        let peer_links = PeerLinks::new(endpoint.clone());

        let session = Session {
            pending_queries: Arc::new(DashMap::default()),
            incoming_err_sender: Arc::new(err_sender),
            pending_cmds: Arc::new(DashMap::default()),
            endpoint,
            network: Arc::new(prefix_map),
            genesis_key,
            initial_connection_check_msg_id: Arc::new(RwLock::new(None)),
            cmd_ack_wait,
            peer_links,
            all_sections_chains: Arc::new(RwLock::new(SecuredLinkedList::new(genesis_key))),
        };

        Ok(session)
    }

    #[instrument(skip(self, auth, payload), level = "debug", name = "session send cmd")]
    pub(crate) async fn send_cmd(
        &self,
        dst_address: XorName,
        auth: ServiceAuth,
        payload: Bytes,
    ) -> Result<()> {
        let endpoint = self.endpoint.clone();
        // TODO: Consider other approach: Keep a session per section!

        let (section_pk, elders) = self.get_cmd_elders(dst_address).await?;

        let msg_id = MsgId::new();

        let elders_len = elders.len();

        debug!(
            "Sending cmd w/id {msg_id:?}, from {}, to {elders_len} Elders w/ dst: {dst_address:?}",
            endpoint.public_addr(),
        );

        let dst_location = DstLocation::Section {
            name: dst_address,
            section_pk,
        };

        let auth_kind = AuthKind::Service(auth);
        let wire_msg = WireMsg::new_msg(msg_id, payload, auth_kind, dst_location)?;

        // The insertion of channel will be executed AFTER the completion of the `send_message`.
        let (sender, mut receiver) = channel::<CmdResponse>(elders_len);
        let _ = self.pending_cmds.insert(msg_id, sender);
        trace!("Inserted channel for cmd {:?}", msg_id);

        send_msg(self.clone(), elders, wire_msg, msg_id).await?;

        let expected_acks = elders_len * 2 / 3 + 1;

        // We are not wait for the receive of majority of cmd Acks.
        // This could be further strict to wait for ALL the Acks get received.
        // The period is expected to have AE completed, hence no extra wait is required.
        let mut received_ack = 0;
        let mut received_err = 0;
        let mut attempts = 0;
        let interval = Duration::from_millis(1000);
        let expected_cmd_ack_wait_attempts =
            std::cmp::max(10, self.cmd_ack_wait.as_millis() / interval.as_millis());
        loop {
            match receiver.try_recv() {
                Ok((src, None)) => {
                    received_ack += 1;
                    trace!("received CmdAck of {msg_id:?} from {src:?}, so far {received_ack} / {expected_acks}");

                    if received_ack >= expected_acks {
                        let _ = self.pending_cmds.remove(&msg_id);
                        break;
                    }
                }
                Ok((src, Some(error))) => {
                    received_err += 1;
                    error!(
                        "received error response {:?} of cmd {:?} from {:?}, so far {} acks vs. {} errors",
                        error, msg_id, src, received_ack, received_err
                    );
                    if received_err >= expected_acks {
                        error!("Received majority of error response for cmd {:?}", msg_id);
                        let _ = self.pending_cmds.remove(&msg_id);
                        return Err(Error::from((error, msg_id)));
                    }
                }
                Err(_err) => {
                    // this is not an error..the channel is just empty atm
                }
            }
            attempts += 1;
            if attempts >= expected_cmd_ack_wait_attempts {
                warn!(
                    "Terminated with insufficient CmdAcks for {:?}, {} / {} acks received",
                    msg_id, received_ack, expected_acks
                );
                break;
            }
            trace!(
                "current ack waiting loop count {}/{}",
                attempts,
                expected_cmd_ack_wait_attempts
            );
            tokio::time::sleep(interval).await;
        }

        trace!("Wait for any cmd response/reaction (AE msgs eg), is over)");
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    /// Send a `ServiceMsg` to the network awaiting for the response.
    pub(crate) async fn send_query(
        &self,
        query: DataQuery,
        auth: ServiceAuth,
        payload: Bytes,
    ) -> Result<QueryResult> {
        let endpoint = self.endpoint.clone();

        let chunk_addr = if let DataQuery::GetChunk(address) = query {
            Some(address)
        } else {
            None
        };

        let dst = query.dst_name();

        let (section_pk, elders) = self.get_query_elders(dst).await?;
        let elders_len = elders.len();
        let msg_id = MsgId::new();

        debug!(
            "Sending query message {:?}, msg_id: {:?}, from {}, to the {} Elders closest to data name: {:?}",
            query,
            msg_id,
            endpoint.public_addr(),
            elders_len,
            elders
        );

        let (sender, mut receiver) = channel::<QueryResponse>(7);

        if let Ok(op_id) = query.operation_id() {
            // Insert the response sender
            trace!("Inserting channel for op_id {:?}", (msg_id, op_id));
            if let Some(mut entry) = self.pending_queries.get_mut(&op_id) {
                let senders_vec = entry.value_mut();
                senders_vec.push((msg_id, sender))
            } else {
                let _nonexistant_entry = self.pending_queries.insert(op_id, vec![(msg_id, sender)]);
            }

            trace!("Inserted channel for {:?}", op_id);
        } else {
            warn!("No op_id found for query");
        }

        let dst_location = DstLocation::Section {
            name: dst,
            section_pk,
        };
        let auth_kind = AuthKind::Service(auth);
        let wire_msg = WireMsg::new_msg(msg_id, payload, auth_kind, dst_location)?;

        send_msg_in_bg(self.clone(), elders, wire_msg, msg_id)?;

        // TODO:
        // We are now simply accepting the very first valid response we receive,
        // but we may want to revisit this to compare multiple responses and validate them,
        // similar to what we used to do up to the following commit:
        // https://github.com/maidsafe/sn_client/blob/9091a4f1f20565f25d3a8b00571cc80751918928/src/connection_manager.rs#L328
        //
        // For Chunk responses we already validate its hash matches the xorname requested from,
        // so we don't need more than one valid response to prevent from accepting invalid responses
        // from byzantine nodes, however for mutable data (non-Chunk responses) we will
        // have to review the approach.
        let mut discarded_responses: usize = 0;

        let response = loop {
            let mut error_response = None;
            match (receiver.recv().await, chunk_addr) {
                (Some(QueryResponse::GetChunk(Ok(chunk))), Some(chunk_addr)) => {
                    // We are dealing with Chunk query responses, thus we validate its hash
                    // matches its xorname, if so, we don't need to await for more responses
                    debug!("Chunk QueryResponse received is: {:#?}", chunk);

                    if chunk_addr.name() == chunk.name() {
                        trace!("Valid Chunk received for {:?}", msg_id);
                        break Some(QueryResponse::GetChunk(Ok(chunk)));
                    } else {
                        // the Chunk content doesn't match its XorName,
                        // this is suspicious and it could be a byzantine node
                        warn!("We received an invalid Chunk response from one of the nodes");
                        discarded_responses += 1;
                    }
                }
                // Erring on the side of positivity. \
                // Saving error, but not returning until we have more responses in
                // (note, this will overwrite prior errors, so we'll just return whichever was last received)
                (response @ Some(QueryResponse::GetChunk(Err(_))), Some(_))
                | (response @ Some(QueryResponse::GetRegister((Err(_), _))), None)
                | (response @ Some(QueryResponse::GetRegisterPolicy((Err(_), _))), None)
                | (response @ Some(QueryResponse::GetRegisterOwner((Err(_), _))), None)
                | (response @ Some(QueryResponse::GetRegisterUserPermissions((Err(_), _))), None) =>
                {
                    debug!("QueryResponse error received (but may be overridden by a non-error response from another elder): {:#?}", &response);
                    error_response = response;
                    discarded_responses += 1;
                }
                (Some(response), _) => {
                    debug!("QueryResponse received is: {:#?}", response);
                    break Some(response);
                }
                (None, _) => {
                    debug!("QueryResponse channel closed.");
                    break None;
                }
            }
            if discarded_responses == elders_len {
                break error_response;
            }
        };

        debug!(
            "Response obtained for query w/id {:?}: {:?}",
            msg_id, response
        );

        if let Some(query) = &response {
            if let Ok(query_op_id) = query.operation_id() {
                // Remove the response sender
                trace!("Removing channel for {:?}", (msg_id, &query_op_id));
                // let _old_channel =
                if let Some(mut entry) = self.pending_queries.get_mut(&query_op_id) {
                    let listeners_for_op = entry.value_mut();
                    if let Some(index) = listeners_for_op
                        .iter()
                        .position(|(id, _sender)| *id == msg_id)
                    {
                        let _old_listener = listeners_for_op.swap_remove(index);
                    }
                } else {
                    warn!("No listeners found for our op_id: {:?}", query_op_id)
                }
            }
        }

        match response {
            Some(response) => {
                let operation_id = response
                    .operation_id()
                    .map_err(|_| Error::UnknownOperationId)?;
                Ok(QueryResult {
                    response,
                    operation_id,
                })
            }
            None => Err(Error::NoResponse),
        }
    }

    #[instrument(skip_all, level = "debug")]
    pub(crate) async fn make_contact_with_nodes(
        &self,
        nodes: Vec<Peer>,
        dst_address: XorName,
        auth: ServiceAuth,
        payload: Bytes,
    ) -> Result<(), Error> {
        let endpoint = self.endpoint.clone();
        // Get DataSection elders details.
        // TODO: we should be able to handle using an pre-existing prefixmap. This is here for when that's in place.
        let (elders_or_adults, section_pk) =
            if let Some(sap) = self.network.closest_or_opposite(&dst_address, None) {
                let mut nodes: Vec<_> = sap.elders_vec();

                nodes.shuffle(&mut OsRng);

                (nodes, sap.section_key())
            } else {
                // Send message to our bootstrap peer with network's genesis PK.
                (nodes, self.genesis_key)
            };

        let msg_id = MsgId::new();

        debug!(
            "Making initial contact with nodes. Our PublicAddr: {:?}. Using {:?} to {} nodes: {:?}",
            endpoint.public_addr(),
            msg_id,
            elders_or_adults.len(),
            elders_or_adults
        );

        // TODO: Don't use genesis key if we have a full section
        let dst_location = DstLocation::Section {
            name: dst_address,
            section_pk,
        };
        let auth_kind = AuthKind::Service(auth);
        let wire_msg = WireMsg::new_msg(msg_id, payload, auth_kind, dst_location)?;

        // When the client bootstrap using the nodes read from the config, the list is sorted
        // by socket addresses. To improve the efficiency, the `elders_or_adults` shall be sorted
        // by `age`, so that `elders` can be contacted first.
        // Unfortunately, the bootstrap nodes were created using random names as the stored
        // prefix_map file doesn't contains the `name` info associated with the socket address,
        // which invalidates the sorting on age.

        let initial_contacts = elders_or_adults
            .clone()
            .into_iter()
            .take(NODES_TO_CONTACT_PER_STARTUP_BATCH)
            .collect();

        send_msg_in_bg(self.clone(), initial_contacts, wire_msg.clone(), msg_id)?;

        *self.initial_connection_check_msg_id.write().await = Some(msg_id);

        let mut knowledge_checks = 0;
        let mut outgoing_msg_rounds = 1;
        let mut last_start_pos = 0;
        let mut tried_every_contact = false;

        let mut backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(1500),
            max_interval: Duration::from_secs(5),
            max_elapsed_time: Some(Duration::from_secs(60)),
            ..Default::default()
        };

        // this seems needed for custom settings to take effect
        backoff.reset();

        // wait here to give a chance for AE responses to come in and be parsed
        tokio::time::sleep(Duration::from_secs(INITIAL_WAIT)).await;

        info!("Client startup... awaiting some network knowledge");

        let mut known_sap = self.network.closest_or_opposite(&dst_address, None);

        // wait until we have sufficient network knowledge
        while known_sap.is_none() {
            if tried_every_contact {
                return Err(Error::NetworkContact);
            }

            let stats = self.network.known_sections_count();
            debug!("Client still has not received a complete section's AE-Retry message... Current sections known: {:?}", stats);
            knowledge_checks += 1;

            // only after a couple of waits do we try contacting more nodes...
            // This just gives the initial contacts more time.
            if knowledge_checks > 2 {
                let mut start_pos = outgoing_msg_rounds * NODES_TO_CONTACT_PER_STARTUP_BATCH;
                outgoing_msg_rounds += 1;

                // if we'd run over known contacts, then we just go to the end
                if start_pos > elders_or_adults.len() {
                    start_pos = last_start_pos;
                }

                last_start_pos = start_pos;

                let next_batch_end = start_pos + NODES_TO_CONTACT_PER_STARTUP_BATCH;

                // if we'd run over known contacts, then we just go to the end
                let next_contacts = if next_batch_end > elders_or_adults.len() {
                    // but incase we _still_ dont know anything after this
                    let next = elders_or_adults[start_pos..].to_vec();
                    // mark as tried all
                    tried_every_contact = true;

                    next
                } else {
                    elders_or_adults[start_pos..start_pos + NODES_TO_CONTACT_PER_STARTUP_BATCH]
                        .to_vec()
                };

                trace!("Sending out another batch of initial contact msgs to new nodes");
                send_msg_in_bg(self.clone(), next_contacts, wire_msg.clone(), msg_id)?;

                let next_wait = backoff.next_backoff();
                trace!(
                    "Awaiting a duration of {:?} before trying new nodes",
                    next_wait
                );

                // wait here to give a chance for AE responses to come in and be parsed
                if let Some(wait) = next_wait {
                    tokio::time::sleep(wait).await;
                }

                known_sap = self.network.closest_or_opposite(&dst_address, None);

                debug!("Known sap: {known_sap:?}");
            }
        }

        let stats = self.network.known_sections_count();
        debug!("Client has received updated network knowledge. Current sections known: {:?}. Sap for our startup-query: {:?}", stats, known_sap);

        Ok(())
    }

    async fn get_query_elders(&self, dst: XorName) -> Result<(bls::PublicKey, Vec<Peer>)> {
        // Get DataSection elders details. Resort to own section if DataSection is not available.
        let sap = self.network.closest_or_opposite(&dst, None);
        let (section_pk, mut elders) = if let Some(sap) = &sap {
            (sap.section_key(), sap.elders_vec())
        } else {
            return Err(Error::NoNetworkKnowledge);
        };

        elders.shuffle(&mut OsRng);

        // We select the NUM_OF_ELDERS_SUBSET_FOR_QUERIES closest Elders we are querying
        let elders: Vec<_> = elders
            .into_iter()
            .take(NUM_OF_ELDERS_SUBSET_FOR_QUERIES)
            .collect();

        let elders_len = elders.len();
        if elders_len < NUM_OF_ELDERS_SUBSET_FOR_QUERIES && elders_len > 1 {
            return Err(Error::InsufficientElderConnections {
                connections: elders_len,
                required: NUM_OF_ELDERS_SUBSET_FOR_QUERIES,
            });
        }

        Ok((section_pk, elders))
    }

    async fn get_cmd_elders(&self, dst_address: XorName) -> Result<(bls::PublicKey, Vec<Peer>)> {
        let a_close_sap = self.network.closest_or_opposite(&dst_address, None);

        // Get DataSection elders details.
        if let Some(sap) = a_close_sap {
            let sap_elders = sap.elders_vec();
            let section_pk = sap.section_key();
            trace!("SAP elders found {:?}", sap_elders);

            // Supermajority of elders is expected.
            let targets_count = supermajority(sap_elders.len());

            // any SAP that does not hold elders_count() is indicative of a broken network (after genesis)
            if sap_elders.len() < targets_count {
                error!("Insufficient knowledge to send to address {:?}, elders for this section: {sap_elders:?} ({targets_count} needed), section PK is: {section_pk:?}", dst_address);
                return Err(Error::InsufficientElderKnowledge {
                    connections: sap_elders.len(),
                    required: targets_count,
                    section_pk,
                });
            }

            Ok((section_pk, sap_elders))
        } else {
            Err(Error::NoNetworkKnowledge)
        }
    }
}

#[instrument(skip_all, level = "trace")]
/// Pushes a send_msg call into a background thread. Errors will be logged
pub(super) fn send_msg_in_bg(
    session: Session,
    nodes: Vec<Peer>,
    wire_msg: WireMsg,
    msg_id: MsgId,
) -> Result<()> {
    trace!("Sending client message in bg thread so as not to block");

    let _handle = tokio::spawn(async move {
        let send_res = send_msg(session, nodes, wire_msg, msg_id).await;

        if send_res.is_err() {
            error!("Error sending msg in the bg: {:?}", send_res);
        }
    });

    Ok(())
}
#[instrument(skip_all, level = "trace")]
pub(super) async fn send_msg(
    session: Session,
    nodes: Vec<Peer>,
    wire_msg: WireMsg,
    msg_id: MsgId,
) -> Result<()> {
    let priority = wire_msg.priority();
    let msg_bytes = wire_msg.serialize()?;

    let mut last_error = None;
    drop(wire_msg);

    // Send message to all Elders concurrently
    let mut tasks = Vec::default();

    let successes = Arc::new(RwLock::new(0));

    for peer in nodes.clone() {
        let session = session.clone();
        let msg_bytes_clone = msg_bytes.clone();
        let peer_name = peer.name();

        let task_handle: JoinHandle<(XorName, Result<()>)> = tokio::spawn(async move {
            let link = session.peer_links.get_or_create(&peer).await;

            let listen = |conn, incoming_msgs| {
                Session::spawn_msg_listener_thread(session.clone(), peer, conn, incoming_msgs);
            };

            let mut retries = 0;

            let send_and_retry = || async {
                match link
                    .send_with(msg_bytes_clone.clone(), priority, None, listen)
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(error) => match error {
                        SendToOneError::Connection(err) => Err(Error::QuicP2pConnection(err)),
                        SendToOneError::Send(err) => Err(Error::QuicP2pSend(err)),
                    },
                }
            };
            let mut result = send_and_retry().await;

            while result.is_err() && retries < CLIENT_SEND_RETRIES {
                warn!(
                    "Attempting to send msg again {msg_id:?}, attempt #{:?}",
                    retries.clone()
                );
                retries += 1;
                result = send_and_retry().await;
            }

            (peer_name, result)
        });

        tasks.push(task_handle);
    }

    // Let's await for all messages to be sent
    let results = join_all(tasks).await;

    for r in results {
        match r {
            Ok((peer_name, send_result)) => match send_result {
                Err(Error::QuicP2pSend(SendError::ConnectionLost(ConnectionError::Closed(
                    Close::Application { reason, error_code },
                )))) => {
                    warn!(
                        "Connection was closed by node {}, reason: {:?}",
                        peer_name,
                        String::from_utf8(reason.to_vec())
                    );
                    last_error = Some(Error::QuicP2pSend(SendError::ConnectionLost(
                        ConnectionError::Closed(Close::Application { reason, error_code }),
                    )));
                }
                Err(Error::QuicP2pSend(SendError::ConnectionLost(error))) => {
                    warn!("Connection to {} was lost: {:?}", peer_name, error);
                    last_error = Some(Error::QuicP2pSend(SendError::ConnectionLost(error)));
                }
                Err(error) => {
                    warn!(
                        "Issue during {:?} send to {}: {:?}",
                        msg_id, peer_name, error
                    );
                    last_error = Some(error);
                }
                Ok(_) => *successes.write().await += 1,
            },
            Err(join_error) => {
                warn!("Tokio join error as we send: {:?}", join_error)
            }
        }
    }

    let failures = nodes.len() - *successes.read().await;

    if failures > 0 {
        trace!(
            "Sending the message ({:?}) from {} to {}/{} of the nodes failed: {:?}",
            msg_id,
            session.endpoint.public_addr(),
            failures,
            nodes.len(),
            nodes,
        );
    }

    let successful_sends = *successes.read().await;
    if failures > successful_sends {
        warn!("More errors when sending a message than successes");
        if let Some(error) = last_error {
            warn!("The relevant error is: {error}");
            return Err(error);
        }
    }

    Ok(())
}

#[instrument(skip_all, level = "trace")]
pub(crate) async fn create_safe_dir() -> Result<PathBuf, Error> {
    let mut root_dir = dirs_next::home_dir().ok_or(Error::CouldNotReadHomeDir)?;
    root_dir.push(".safe");

    // Create `.safe/client` dir if not present
    tokio::fs::create_dir_all(root_dir.clone())
        .await
        .map_err(|_| Error::CouldNotCreateSafeDir)?;

    Ok(root_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sn_interface::network_knowledge::test_utils::{
        gen_section_authority_provider, section_signed,
    };

    use eyre::{eyre, Result};
    use std::net::Ipv4Addr;
    use xor_name::Prefix;

    fn prefix(s: &str) -> Result<Prefix> {
        s.parse()
            .map_err(|err| eyre!("failed to parse Prefix '{}': {}", s, err))
    }

    fn new_network_prefix_map() -> (NetworkPrefixMap, bls::SecretKey, bls::PublicKey) {
        let genesis_sk = bls::SecretKey::random();
        let genesis_pk = genesis_sk.public_key();

        let map = NetworkPrefixMap::new(genesis_pk);

        (map, genesis_sk, genesis_pk)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cmd_sent_to_all_elders() -> Result<()> {
        let elders_len = 5;
        let (err_sender, _err_receiver) = channel::<CmdError>(10);

        let prefix = prefix("0")?;
        let (section_auth, _, secret_key_set) = gen_section_authority_provider(prefix, elders_len);
        let sap0 = section_signed(secret_key_set.secret_key(), section_auth)?;
        let (prefix_map, _genesis_sk, genesis_key) = new_network_prefix_map();
        assert!(prefix_map.insert_without_chain(sap0));

        let session = Session::new(
            genesis_key,
            QuicP2pConfig::default(),
            err_sender,
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            Duration::from_secs(10),
            prefix_map,
        )?;

        let mut rng = rand::thread_rng();
        let result = session.get_cmd_elders(XorName::random(&mut rng)).await?;
        assert_eq!(result.0, secret_key_set.public_keys().public_key());
        assert_eq!(result.1.len(), elders_len);

        Ok(())
    }
}
