// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{QueryResult, Session};
use crate::client::Error;
use crate::messaging::{
    data::{ChunkRead, DataCmd, DataQuery, QueryResponse},
    section_info::SectionInfoMsg,
    MessageId, ServiceAuth, WireMsg,
};
use crate::messaging::{DstLocation, MsgKind};
use crate::types::{Chunk, PrivateChunk, PublicChunk, PublicKey};
use bytes::Bytes;
use futures::{future::join_all, stream::FuturesUnordered};
use itertools::Itertools;
use std::{collections::BTreeSet, net::SocketAddr, time::Duration};
use tokio::{sync::mpsc::channel, task::JoinHandle, time::timeout};
use tracing::{debug, error, info, trace, warn};
use xor_name::XorName;

// Number of attemps when retrying to send a message to a node
const NUMBER_OF_RETRIES: usize = 3;
// Number of Elders subset to send queries to
const NUM_OF_ELDERS_SUBSET_FOR_QUERIES: usize = 3;

impl Session {
    /// Bootstrap to the network maintaining connections to several nodes.
    pub(crate) async fn bootstrap(&mut self, client_pk: PublicKey) -> Result<(), Error> {
        trace!(
            "Trying to bootstrap to the network with public_key: {:?}",
            client_pk
        );

        let (endpoint, _, mut incoming_messages, mut disconnections, mut bootstrapped_peer) =
            self.qp2p.bootstrap().await?;

        self.endpoint = Some(endpoint.clone());
        let mut bootstrap_nodes = endpoint
            .clone()
            .bootstrap_nodes()
            .to_vec()
            .into_iter()
            .collect::<BTreeSet<_>>();

        let cloned_endpoint = endpoint.clone();
        let _ = tokio::spawn(async move {
            while let Some(disconnected_peer) = disconnections.next().await {
                // we assume elders should have high connectivity.
                // any problem there and they'd be voted off and we'd get an updated section
                // so just keep trying to reconnect
                warn!(
                    "Disconnected from elder {:?}. Attempting to reconnect",
                    disconnected_peer
                );
                match cloned_endpoint.connect_to(&disconnected_peer).await {
                    Ok(_) => info!("Reconnected to {:?}", disconnected_peer),
                    Err(error) => {
                        warn!(
                            "Could not reconnect to {:?}, error: {:?}",
                            disconnected_peer, error
                        );
                    }
                };
            }
        });

        self.send_get_section_query(client_pk, &bootstrapped_peer)
            .await?;

        // Bootstrap and send a handshake request to the bootstrapped peer
        let mut we_have_keyset = false;
        while !we_have_keyset {
            // This means that the peer we bootstrapped to
            // has responded with a SectionInfo Message
            if let Ok(Ok(true)) = timeout(
                Duration::from_secs(30),
                self.process_incoming_message(&mut incoming_messages, client_pk),
            )
            .await
            {
                we_have_keyset = self.section_key_set.read().await.is_some();
            } else {
                // Remove the unresponsive peer we boostrapped to and bootstrap again
                let _ = bootstrap_nodes.remove(&bootstrapped_peer);
                bootstrapped_peer = self
                    .qp2p
                    .rebootstrap(
                        &endpoint,
                        &bootstrap_nodes.iter().cloned().collect::<Vec<_>>(),
                    )
                    .await?;
            }
        }

        self.spawn_message_listener_thread(incoming_messages, client_pk)
            .await;

        debug!(
            "Successfully obtained the list of Elders to send all messages to: {:?}",
            self.connected_elders.read().await.keys()
        );

        Ok(())
    }

    /// Send a `ServiceMsg` to the network without awaiting for a response.
    pub(crate) async fn send_cmd(
        &self,
        cmd: DataCmd,
        auth: ServiceAuth,
        payload: Bytes,
    ) -> Result<(), Error> {
        let endpoint = self.endpoint()?.clone();

        let elders = self
            .connected_elders
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<SocketAddr>>();

        let msg_id = MessageId::new();
        debug!(
            "Sending command w/id {:?}, from {}, to {} Elders",
            msg_id,
            endpoint.socket_addr(),
            elders.len()
        );

        trace!(
            "Sending (from {}) command message {:?} w/ id: {:?}",
            endpoint.socket_addr(),
            cmd,
            msg_id
        );

        let section_pk = self
            .section_key()
            .await?
            .bls()
            .ok_or(Error::NoBlsSectionKey)?;

        let dst_section_name = cmd.dst_address();
        let dst_location = DstLocation::Section {
            name: dst_section_name,
            section_pk,
        };
        let msg_kind = MsgKind::ServiceMsg(auth);
        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst_location)?;

        let msg_bytes = wire_msg.serialize()?;

        // Send message to all Elders concurrently
        let mut tasks = Vec::default();

        // clone elders as we want to update them in this process
        for socket in elders {
            let msg_bytes_clone = msg_bytes.clone();
            let endpoint = endpoint.clone();
            let task_handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                trace!("About to send cmd message {:?} to {:?}", msg_id, &socket);
                endpoint.connect_to(&socket).await?;
                endpoint.send_message(msg_bytes_clone, &socket).await?;

                trace!("Sent cmd with MsgId {:?}to {:?}", msg_id, &socket);
                Ok(())
            });
            tasks.push(task_handle);
        }

        // Let's await for all messages to be sent
        let results = join_all(tasks).await;

        let mut failures = 0;
        results.iter().for_each(|res| {
            if res.is_err() {
                failures += 1;
            }
        });

        if failures > 0 {
            error!("Sending the message to {} Elders failed", failures);
        }

        Ok(())
    }

    /// Send a `ServiceMsg` to the network awaiting for the response.
    pub(crate) async fn send_query(
        &self,
        query: DataQuery,
        auth: ServiceAuth,
        payload: Bytes,
    ) -> Result<QueryResult, Error> {
        let endpoint = self.endpoint()?.clone();
        let pending_queries = self.pending_queries.clone();

        let chunk_addr = if let DataQuery::Blob(ChunkRead::Get(address)) = query {
            Some(address)
        } else {
            None
        };

        let section_pk = self
            .section_key()
            .await?
            .bls()
            .ok_or(Error::NoBlsSectionKey)?;

        let data_name = query.dst_address();
        let dst_location = DstLocation::Section {
            name: data_name,
            section_pk,
        };
        let msg_id = MessageId::new();
        let msg_kind = MsgKind::ServiceMsg(auth);
        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst_location)?;

        let msg_bytes = wire_msg.serialize()?;

        // We select the NUM_OF_ELDERS_SUBSET_FOR_QUERIES closest
        // connected Elders to the data we are querying
        let elders: Vec<SocketAddr> = self
            .connected_elders
            .read()
            .await
            .clone()
            .into_iter()
            .sorted_by(|(_, lhs_name), (_, rhs_name)| data_name.cmp_distance(lhs_name, rhs_name))
            .take(NUM_OF_ELDERS_SUBSET_FOR_QUERIES)
            .map(|(addr, _)| addr)
            .collect();

        let elders_len = elders.len();
        if elders_len < NUM_OF_ELDERS_SUBSET_FOR_QUERIES {
            error!(
                "Not enough Elder connections: {}, minimum required: {}",
                elders_len, NUM_OF_ELDERS_SUBSET_FOR_QUERIES
            );
            return Err(Error::InsufficientElderConnections(elders_len));
        }

        debug!(
            "Sending query message {:?}, msg_id: {}, from {}, to the {} Elders closest to data name: {:?}",
            query,
            msg_id,
            endpoint.socket_addr(),
            elders_len,
            elders
        );

        // We send the same message to all Elders concurrently
        let tasks = FuturesUnordered::new();
        let (sender, mut receiver) = channel::<QueryResponse>(7);

        let pending_queries_for_thread = pending_queries.clone();
        let _ = tokio::spawn(async move {
            // Remove the response sender
            trace!("Inserting channel for {:?}", msg_id);
            let _ = pending_queries_for_thread
                .write()
                .await
                .insert(msg_id, sender);
        });

        let responses_discarded = std::sync::Arc::new(tokio::sync::Mutex::new(0_usize));

        // Set up response listeners
        for socket in elders {
            let endpoint = endpoint.clone();
            let msg_bytes = msg_bytes.clone();
            let counter_clone = responses_discarded.clone();
            let task_handle = tokio::spawn(async move {
                endpoint.connect_to(&socket).await?;

                // Retry queries that failed due to connection issues only
                let mut result = Err(Error::ElderQuery);
                for attempt in 0..NUMBER_OF_RETRIES + 1 {
                    let msg_bytes_clone = msg_bytes.clone();

                    if let Err(err) = endpoint.send_message(msg_bytes_clone, &socket).await {
                        error!(
                            "Try #{:?} @ {:?}, failed sending query message: {:?}",
                            attempt + 1,
                            socket,
                            err
                        );
                        result = Err(Error::SendingQuery);
                        if attempt <= NUMBER_OF_RETRIES {
                            let millis = 2_u64.pow(attempt as u32 - 1) * 100;
                            tokio::time::sleep(std::time::Duration::from_millis(millis)).await
                        }
                    } else {
                        trace!("ServiceMsg with id: {:?}, sent to {}", &msg_id, &socket);
                        result = Ok(());
                        break;
                    }
                }

                match &result {
                    Err(err) => {
                        error!("Error sending Query to elder: {:?} ", err);
                        let mut a = counter_clone.lock().await;
                        *a += 1;
                    }
                    Ok(()) => (),
                }

                result
            });

            tasks.push(task_handle);
        }

        // TODO:
        // We are now simply accepting the very first valid response we receive,
        // but we may want to revisit this to compare multiple responses and validate them,
        // similar to what we used to do up to the following commit:
        // https://github.com/maidsafe/sn_client/blob/9091a4f1f20565f25d3a8b00571cc80751918928/src/connection_manager.rs#L328
        //
        // For Chunk responses we already validate its hash matches the xorname requested from,
        // so we don't need more than one valid response to prevent from accepting invaid responses
        // from byzantine nodes, however for mutable data (non-Chunk esponses) we will
        // have to review the approach.
        let mut responses_discarded: usize = 0;

        // Send all queries concurrently
        let results = join_all(tasks).await;

        for result in results {
            if let Err(err) = result {
                error!("Error spawning task to send query: {:?} ", err);
                responses_discarded += 1;
            }
        }

        let response = loop {
            let mut error_response = None;
            match (receiver.recv().await, chunk_addr) {
                (Some(QueryResponse::GetChunk(Ok(blob))), Some(chunk_addr)) => {
                    // We are dealing with Chunk query responses, thus we validate its hash
                    // matches its xorname, if so, we don't need to await for more responses
                    debug!("Chunk QueryResponse received is: {:#?}", blob);

                    let xorname = match &blob {
                        Chunk::Private(priv_chunk) => {
                            *PrivateChunk::new(priv_chunk.value().clone(), *priv_chunk.owner())
                                .name()
                        }
                        Chunk::Public(pub_chunk) => {
                            *PublicChunk::new(pub_chunk.value().clone()).name()
                        }
                    };

                    if *chunk_addr.name() == xorname {
                        trace!("Valid Chunk received for {}", msg_id);
                        break Some(QueryResponse::GetChunk(Ok(blob)));
                    } else {
                        // the Chunk content doesn't match its Xorname,
                        // this is suspicious and it could be a byzantine node
                        warn!("We received an invalid Chunk response from one of the nodes");
                        responses_discarded += 1;
                    }
                }
                // Erring on the side of positivity. \
                // Saving error, but not returning until we have more responses in
                // (note, this will overwrite prior errors, so we'll just return whicever was last received)
                (response @ Some(QueryResponse::GetChunk(Err(_))), Some(_))
                | (response @ Some(QueryResponse::GetRegister(Err(_))), None)
                | (response @ Some(QueryResponse::GetRegisterPolicy(Err(_))), None)
                | (response @ Some(QueryResponse::GetRegisterOwner(Err(_))), None)
                | (response @ Some(QueryResponse::GetRegisterUserPermissions(Err(_))), None) => {
                    debug!("QueryResponse error received (but may be overridden by a non-error reponse from another elder): {:#?}", &response);
                    error_response = response;
                    responses_discarded += 1;
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
            if responses_discarded == elders_len {
                break error_response;
            }
        };

        debug!(
            "Response obtained for query w/id {:?}: {:?}",
            msg_id, response
        );

        let _ = tokio::spawn(async move {
            // Remove the response sender
            trace!("Removing channel for {:?}", msg_id);
            let _ = pending_queries.clone().write().await.remove(&msg_id);
        });

        response
            .map(|response| QueryResult { response, msg_id })
            .ok_or(Error::NoResponse)
    }

    // Get section info from the peer we have bootstrapped with.
    pub(crate) async fn send_get_section_query(
        &self,
        client_pk: PublicKey,
        bootstrapped_peer: &SocketAddr,
    ) -> Result<(), Error> {
        if self.is_connecting_to_new_elders {
            // This should ideally be unreachable code. Leaving it while this is a WIP
            error!("Already attempting elder connections, not sending section query until that is complete.");
            return Ok(());
        }

        trace!(
            "Querying for section info from bootstrapped node: {:?}",
            bootstrapped_peer
        );

        let dst_section_name = XorName::from(client_pk);

        // FIXME: we don't know our section PK. We must supply a pk for now we do a random one...
        let random_section_pk = bls::SecretKey::random().public_key();

        let msg_id = MessageId::new();
        let query = SectionInfoMsg::GetSectionQuery(client_pk);
        let payload = WireMsg::serialize_msg_payload(&query)?;
        let msg_kind = MsgKind::SectionInfoMsg;
        let dst_location = DstLocation::Section {
            name: dst_section_name,
            section_pk: random_section_pk,
        };
        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst_location)?;
        let msg_bytes = wire_msg.serialize()?;

        debug!(
            "Sending query message {:?}, msg_id {}, from {}, to bootstrapped node {} ",
            query,
            msg_id,
            self.endpoint()?.socket_addr(),
            bootstrapped_peer
        );

        self.endpoint()?
            .send_message(msg_bytes, bootstrapped_peer)
            .await?;

        Ok(())
    }

    pub(crate) async fn disconnect_from_peers(&self, peers: Vec<SocketAddr>) -> Result<(), Error> {
        for elder in peers {
            self.endpoint()?.disconnect_from(&elder).await?;
        }

        Ok(())
    }

    // Connect to a set of Elders nodes which will be
    // the receipients of our messages on the network.
    pub(crate) async fn connect_to_elders(&mut self) -> Result<(), Error> {
        // TODO: remove this function completely

        self.is_connecting_to_new_elders = true;

        if self.known_elders_count().await == 0 {
            // this is not necessarily an error in case we didn't get elder info back yet
            warn!("Not attempted to connect, insufficient elders yet known");
        }

        let new_elders = self.all_known_elders.read().await.clone();
        let peers_len = new_elders.len();

        trace!("We now know our {} Elders.", peers_len);
        {
            let mut session_elders = self.connected_elders.write().await;
            *session_elders = new_elders;
        }

        self.is_connecting_to_new_elders = false;

        Ok(())
    }
}
