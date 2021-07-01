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
    client::{ChunkRead, ClientMsg, ClientSig, Cmd, DataQuery, ProcessMsg, Query, QueryResponse},
    section_info::SectionInfoMsg,
    MessageId,
};
use crate::types::{Chunk, PrivateChunk, PublicChunk, PublicKey};
use futures::{future::join_all, stream::FuturesUnordered, StreamExt};
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
    pub async fn bootstrap(&mut self, client_pk: PublicKey) -> Result<(), Error> {
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

    /// Send a `ClientMsg` to the network without awaiting for a response.
    pub async fn send_cmd(
        &self,
        cmd: Cmd,
        client_sig: ClientSig,
        send_to_specific_elder: Option<SocketAddr>,
    ) -> Result<(), Error> {
        let msg_id = MessageId::new();
        let endpoint = self.endpoint()?.clone();

        let elders = if let Some(socket) = send_to_specific_elder {
            vec![socket]
        } else {
            self.connected_elders
                .read()
                .await
                .keys()
                .cloned()
                .collect::<Vec<SocketAddr>>()
        };

        debug!(
            "Sending command w/id {:?}, to {} Elders",
            msg_id,
            elders.len()
        );

        let src_addr = endpoint.socket_addr();
        trace!(
            "Sending (from {}) command message {:?} w/ id: {:?}",
            src_addr,
            cmd,
            msg_id
        );

        let section_pk = self
            .section_key()
            .await?
            .bls()
            .ok_or(Error::NoBlsSectionKey)?;
        let dst_section_name = cmd.dst_address();

        let msg = ClientMsg::Process(ProcessMsg::Cmd {
            id: msg_id,
            cmd,
            client_sig,
        });

        let msg_bytes = msg.serialize(dst_section_name, section_pk)?;

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

    /// Send a Query `ClientMsg` to the network awaiting for the response.
    pub(crate) async fn send_query(
        &self,
        query: Query,
        client_sig: ClientSig,
    ) -> Result<QueryResult, Error> {
        let data_name = query.dst_address();

        let endpoint = self.endpoint()?.clone();
        let pending_queries = self.pending_queries.clone();

        let chunk_addr = if let Query::Data(DataQuery::Blob(ChunkRead::Get(address))) = query {
            Some(address)
        } else {
            None
        };

        let section_pk = self
            .section_key()
            .await?
            .bls()
            .ok_or(Error::NoBlsSectionKey)?;
        let dst_section_name = XorName::from(client_sig.public_key);

        let msg_id = MessageId::new();
        let msg = ClientMsg::Process(ProcessMsg::Query {
            id: msg_id,
            query,
            client_sig,
        });

        let msg_bytes = msg.serialize(dst_section_name, section_pk)?;

        // We select the NUM_OF_ELDERS_SUBSET_FOR_QUERIES closest
        // connected Elders to the data we are querying
        let elders: Vec<SocketAddr> = self
            .connected_elders
            .read()
            .await
            .clone()
            .into_iter()
            .sorted_by(|(_, lhs_name), (_, rhs_name)| data_name.cmp_distance(&lhs_name, &rhs_name))
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

        info!(
            "Sending query message {:?}, to the {} Elders closest to data name: {:?}",
            msg, elders_len, elders
        );

        // We send the same message to all Elders concurrently
        let mut tasks = FuturesUnordered::new();
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

        // Set up response listeners
        for socket in elders {
            let endpoint = endpoint.clone();
            let msg_bytes = msg_bytes.clone();
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
                    } else {
                        trace!("ClientMsg with id: {:?}, sent to {}", &msg_id, &socket);
                        result = Ok(());
                        break;
                    }
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

        // Send all queries first
        while let Some(result) = tasks.next().await {
            match result {
                Err(err) => {
                    error!("Error spawning task to send query: {:?} ", err);
                    responses_discarded += 1;
                }
                Ok(Err(err)) => {
                    error!("Error sending Query to elder: {:?} ", err);
                    responses_discarded += 1;
                }
                _ => (),
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
                | (response @ Some(QueryResponse::GetMap(Err(_))), None)
                | (response @ Some(QueryResponse::GetRegister(Err(_))), None)
                | (response @ Some(QueryResponse::GetSequence(Err(_))), None)
                | (response @ Some(QueryResponse::GetMapShell(Err(_))), None)
                | (response @ Some(QueryResponse::GetMapValue(Err(_))), None)
                | (response @ Some(QueryResponse::GetMapVersion(Err(_))), None)
                | (response @ Some(QueryResponse::GetRegisterPolicy(Err(_))), None)
                | (response @ Some(QueryResponse::GetRegisterOwner(Err(_))), None)
                | (response @ Some(QueryResponse::GetRegisterUserPermissions(Err(_))), None)
                | (response @ Some(QueryResponse::GetSequenceLastEntry(Err(_))), None)
                | (response @ Some(QueryResponse::GetSequencePrivatePolicy(Err(_))), None)
                | (response @ Some(QueryResponse::GetSequencePublicPolicy(Err(_))), None)
                | (response @ Some(QueryResponse::GetSequenceRange(Err(_))), None) => {
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

        let msg = SectionInfoMsg::GetSectionQuery(client_pk)
            .serialize(dst_section_name, random_section_pk)?;

        self.endpoint()?
            .send_message(msg, bootstrapped_peer)
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
