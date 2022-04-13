// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod back_pressure;
mod link;
mod listener;
mod peer_session;

use self::back_pressure::BackPressure;
use self::link::Link;
use self::listener::{ListenerEvent, MsgListener};
use self::peer_session::{PeerSession, SendWatcher};

use crate::messaging::{MsgId, WireMsg};
use crate::node::{
    core::{comm::peer_session::SendStatus, Measurements},
    error::{Error, Result},
};
use crate::types::Peer;

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use qp2p::{Endpoint, IncomingConnections};
use std::time::Duration;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    sync::{mpsc, RwLock},
    task,
};

// Communication component of the node to interact with other nodes.
#[derive(Clone)]
pub(crate) struct Comm {
    our_endpoint: Endpoint,
    msg_listener: MsgListener,
    back_pressure: BackPressure,
    sessions: Arc<RwLock<BTreeMap<Peer, PeerSession>>>,
}

impl Comm {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn first_node(
        local_addr: SocketAddr,
        config: qp2p::Config,
        monitoring: Measurements,
        receive_msg: mpsc::Sender<MsgEvent>,
    ) -> Result<Self> {
        // Doesn't bootstrap, just creates an endpoint to listen to
        // the incoming messages from other nodes.
        let (our_endpoint, incoming_connections, _) =
            Endpoint::new_peer(local_addr, Default::default(), config).await?;

        let (comm, _) = setup_comms(our_endpoint, incoming_connections, monitoring, receive_msg);

        Ok(comm)
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn bootstrap(
        local_addr: SocketAddr,
        bootstrap_nodes: &[SocketAddr],
        config: qp2p::Config,
        monitoring: Measurements,
        receive_msg: mpsc::Sender<MsgEvent>,
    ) -> Result<(Self, SocketAddr)> {
        // Bootstrap to the network returning the connection to a node.
        let (our_endpoint, incoming_connections, bootstrap_node) =
            Endpoint::new_peer(local_addr, bootstrap_nodes, config).await?;

        let (comm, msg_listener) =
            setup_comms(our_endpoint, incoming_connections, monitoring, receive_msg);

        let (connection, incoming_msgs) = bootstrap_node.ok_or(Error::BootstrapFailed)?;
        let remote_address = connection.remote_address();

        msg_listener.listen(connection, incoming_msgs);

        Ok((comm, remote_address))
    }

    pub(crate) fn our_connection_info(&self) -> SocketAddr {
        self.our_endpoint.public_addr()
    }

    pub(crate) async fn remove_expired(&self) {
        let sessions = self.sessions.read().await;
        for (_, session) in sessions.iter() {
            session.remove_expired().await;
        }
    }

    pub(crate) async fn is_connected(&self, id: &Peer) -> bool {
        let sessions = self.sessions.read().await;
        if let Some(c) = sessions.get(id) {
            // peer id exists, check if connected
            return c.is_connected().await;
        }

        false
    }

    /// Fake function used as replacement for testing only.
    #[cfg(test)]
    pub(crate) async fn is_reachable(&self, _peer: &SocketAddr) -> Result<(), Error> {
        Ok(())
    }

    /// Tests whether the peer is reachable.
    #[cfg(not(test))]
    pub(crate) async fn is_reachable(&self, peer: &SocketAddr) -> Result<(), Error> {
        let qp2p_config = qp2p::Config {
            forward_port: false,
            ..Default::default()
        };

        let connectivity_endpoint =
            Endpoint::new_client((self.our_endpoint.local_addr().ip(), 0), qp2p_config)?;

        let result = connectivity_endpoint
            .is_reachable(peer)
            .await
            .map_err(|err| {
                info!("Peer {} is NOT externally reachable: {:?}", peer, err);
                err.into()
            })
            .map(|()| {
                info!("Peer {} is externally reachable.", peer);
            });
        connectivity_endpoint.close();
        result
    }

    pub(crate) async fn linked_peers(&self) -> BTreeSet<Peer> {
        let sessions = self.sessions.read().await;
        sessions.keys().into_iter().cloned().collect()
    }

    /// Returns our caller-specific tolerated msgs per s, if the value has changed significantly.
    pub(crate) async fn tolerated_msgs_per_s(&self, caller: &Peer) -> Option<f64> {
        self.back_pressure.tolerated_msgs_per_s(caller).await
    }

    /// Regulates comms with the specified peer
    /// according to the tolerated msgs per s provided by it.
    pub(crate) async fn regulate(&self, peer: &Peer, msgs_per_s: f64) {
        let session = self.get_or_create(peer).await;
        session.update_send_rate(msgs_per_s).await;
    }

    /// Disposes of the link and all underlying
    /// resources, and drops all queued msgs.
    /// TODO: Also use this when new membership is in place, call whenever we drop a member.
    pub(crate) async fn unlink_peer(&self, peer: &Peer) {
        let mut sessions = self.sessions.write().await;
        let session = match sessions.remove(peer) {
            Some(session) => session,
            None => return, // none here, all good
        };
        session.disconnect().await;
    }

    /// Sends a message to a client. Reuses an existing or creates a connection if none.
    pub(crate) async fn send_to_client(
        &self,
        recipient: &Peer,
        mut wire_msg: WireMsg,
    ) -> Result<(), Error> {
        trace!(
            "Sending msg on existing connection to client {:?}",
            recipient
        );

        let name = recipient.name();
        let addr = recipient.addr();

        wire_msg.set_dst_xorname(name);

        let bytes = wire_msg.serialize()?;
        // TODO: rework priority so this we dont need to deserialise payload to determine priority.
        let priority = wire_msg.priority();

        let (_, result) = self
            .send_to_one(*recipient, wire_msg.msg_id(), priority, bytes)
            .await;

        match result {
            Err(error) => {
                // there is only one type of error returned: [`Error::InvalidState`]
                // which should not happen (be reachable) if we only access PeerSession from Comm
                // The error means we accessed a peer that we disconnected from.
                // So, this would potentially be a bug!
                warn!(
                    "Accessed a disconnected peer: {}. This is potentially a bug!",
                    recipient
                );
                error!(
                    "Sending message (msg_id: {:?}) to {:?} (name {:?}) failed as we have disconnected from the peer. (Error is: {})",
                    wire_msg.msg_id(),
                    addr,
                    name,
                    error,
                );
                Err(Error::FailedSend(*recipient))
            }
            Ok(mut watcher) => {
                // here we can monitor the sending
                // and we now watch the status of the send
                loop {
                    match watcher.await_change().await {
                        SendStatus::Sent => {
                            return Ok(()); // all good
                        }
                        SendStatus::Enqueued => {
                            // this block should be unreachable, as Enqueued is the initial state
                            // but let's handle it anyway..
                            continue; // moves on to awaiting a new change
                        }
                        SendStatus::PeerLinkDropped => {
                            // The connection was closed by us which means
                            // we have dropped this peer for some reason
                            error!(
                                "Sending message (msg_id: {:?}) to {:?} (name {:?}) failed, as we have dropped the link to it.",
                                wire_msg.msg_id(),
                                addr,
                                name,
                            );
                            return Err(Error::PeerLinkDropped(*recipient));
                        }
                        SendStatus::MaxRetriesReached(retries) => {
                            // this would perhaps be a place to start taking action against this peer
                            // (if not, then this clause can be collapsed with the one below)
                            error!(
                                "Gave up on sending message (msg_id: {:?}) to {:?} (name {:?}), after retrying {} times",
                                wire_msg.msg_id(),
                                addr,
                                name,
                                retries,
                            );
                            return Err(Error::FailedSend(*recipient));
                        }
                        SendStatus::WatcherDropped => {
                            // the send job is dropped for some reason,
                            // that happens when the peer session dropped
                            // or the msg was sent, meaning the send didn't actually fail,
                            error!(
                                "Sending message (msg_id: {:?}) to {:?} (name {:?}) possibly failed, as monitoring of the send job was aborted",
                                wire_msg.msg_id(),
                                addr,
                                name,
                            );
                            return Err(Error::FailedSend(*recipient));
                        }
                        SendStatus::TransientError(error) => {
                            // An individual connection can for example have been lost when we tried to send. This
                            // could indicate the connection timed out whilst it was held, or some other
                            // transient connection issue. We don't treat this as a failed recipient, but we sleep a little longer here.
                            // Retries are managed by the peer session, where it will open a new connection.
                            debug!(
                                "Transient error when sending to peer {}: {}",
                                recipient, error
                            );
                            continue; // moves on to awaiting a new change
                        }
                    }
                }
            }
        }
    }

    /// Sends a message to multiple recipients. Attempts to send to `delivery_group_size`
    /// recipients out of the `recipients` list. If a send fails, attempts to send to the next peer
    /// until `delivery_group_size` successful sends complete or there are no more recipients to
    /// try.
    ///
    /// Returns an `Error::EmptyRecipientList` if the recipient list is empty. Else it returns a
    /// `DeliveryStatus::MinDeliveryGroupSizeReached` or `DeliveryStatus::MinDeliveryGroupSizeFailed` depending
    /// on if the minimum delivery group size is met or not. The failed recipients are sent along
    /// with the status. It returns a `DeliveryStatus::AllRecipients` if message is sent to all the recipients.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn send(
        &self,
        recipients: &[Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<DeliveryStatus> {
        // todo: this type of task needs a send job, that we can come back to

        let msg_id = wire_msg.msg_id();
        trace!(
            "Sending message (msg_id: {:?}) to {} of {:?}",
            msg_id,
            delivery_group_size,
            recipients
        );

        if recipients.len() < delivery_group_size {
            warn!(
                "Less than delivery_group_size valid recipients - delivery_group_size: {}, recipients: {:?}",
                delivery_group_size,
                recipients,
            );
        }

        let delivery_group_size = delivery_group_size.min(recipients.len());

        if recipients.is_empty() {
            return Err(Error::EmptyRecipientList);
        }

        let msg_bytes = wire_msg.serialize().map_err(Error::Messaging)?;
        let priority = wire_msg.clone().priority();

        // Run all the sends concurrently (using `FuturesUnordered`). If any of them fails, pick
        // the next recipient and try to send to them. Proceed until the needed number of sends
        // succeeds or if there are no more recipients to pick.
        let mut tasks: FuturesUnordered<_> = recipients[0..delivery_group_size]
            .iter()
            .map(|recipient| self.send_to_one(*recipient, msg_id, priority, msg_bytes.clone()))
            .collect();

        let mut next = delivery_group_size;
        let mut successes = 0;
        let mut failed_recipients = vec![];

        let mut try_next = |error, recipient, tasks: &mut FuturesUnordered<_>| {
            warn!("during sending, received error {:?}", error);
            failed_recipients.push(recipient);

            if next < recipients.len() {
                tasks.push(self.send_to_one(recipients[next], msg_id, priority, msg_bytes.clone()));
                next += 1;
            }
        };

        while let Some((recipient, result)) = tasks.next().await {
            match result {
                Err(error) => {
                    // there is only one type of error returned: [`Error::InvalidState`]
                    // which should not happen (be reachable) if we only access PeerSession from Comm
                    // The error means we accessed a peer that we disconnected from.
                    // So, this would potentially be a bug!
                    //
                    // (let's log that bug here, but continue running anyway, as it isn't that critical)
                    warn!(
                        "Accessed a disconnected peer: {}. This is potentially a bug!",
                        recipient
                    );
                    try_next(error, recipient, &mut tasks);
                }
                Ok(mut watcher) => {
                    // we now watch the status of the send for this particular recipient..
                    loop {
                        match watcher.await_change().await {
                            SendStatus::Sent => {
                                successes += 1;
                                break; // we now move to checking next recipient send task..
                            }
                            SendStatus::Enqueued => {
                                // this block should be unreachable, as Enqueued is the initial state
                                // but let's handle it anyway..
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                continue; // await change on the same recipient again
                            }
                            SendStatus::PeerLinkDropped => {
                                // The connection was closed by us which means
                                // we have dropped this peer for some reason, thus try next
                                try_next(Error::PeerLinkDropped(recipient), recipient, &mut tasks);
                                break; // we now move to checking next recipient send task..
                            }
                            SendStatus::MaxRetriesReached(_) => {
                                // this would perhaps be a place to start taking action against this peer
                                // (if not, then this clause can be collapsed with the one below)
                                try_next(Error::FailedSend(recipient), recipient, &mut tasks);
                                break; // we now move to checking next recipient send task..
                            }
                            SendStatus::WatcherDropped => {
                                // the send job is dropped for some reason,
                                // that happens when the peer session dropped
                                // or the msg was sent, meaning the send didn't actually fail,
                                // so we would be sending an extra msg here in case of such a glitch
                                try_next(Error::FailedSend(recipient), recipient, &mut tasks);
                                break; // we now move to checking next recipient send task..
                            }
                            SendStatus::TransientError(error) => {
                                // An individual connection can for example have been lost when we tried to send. This
                                // could indicate the connection timed out whilst it was held, or some other
                                // transient connection issue. We don't treat this as a failed recipient, but we sleep a little longer here.
                                // Retries are managed by the peer session, where it will open a new connection.
                                debug!(
                                    "Transient error when sending to peer {}: {}",
                                    recipient, error
                                );
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                continue; // await change on the same recipient again
                            }
                        }
                    }
                }
            }
        }

        trace!(
            "Finished sending message {:?} to {}/{} recipients (failed: {:?})",
            wire_msg,
            successes,
            delivery_group_size,
            failed_recipients
        );

        if successes == delivery_group_size {
            if failed_recipients.is_empty() {
                Ok(DeliveryStatus::AllRecipients)
            } else {
                Ok(DeliveryStatus::MinDeliveryGroupSizeReached(
                    failed_recipients,
                ))
            }
        } else {
            // todo: is this really a success case..?
            Ok(DeliveryStatus::MinDeliveryGroupSizeFailed(
                failed_recipients,
            ))
        }
    }

    async fn get(&self, id: &Peer) -> Option<PeerSession> {
        let sessions = self.sessions.read().await;
        sessions.get(id).cloned()
    }

    async fn get_or_create(&self, peer: &Peer) -> PeerSession {
        if let Some(session) = self.get(peer).await {
            return session;
        }

        // if peer is not in list, the entire list needs to be locked
        // i.e. first comms to any node, will impact all sending at that instant..
        // however, first comms should be a minor part of total time spent using link,
        // so that is ok
        let mut sessions = self.sessions.write().await;
        match sessions.get(peer).cloned() {
            // someone else inserted in the meanwhile, so use that
            Some(session) => session,
            // still not in list, go ahead and create + insert
            None => {
                let link = Link::new(*peer, self.our_endpoint.clone(), self.msg_listener.clone());
                let session = PeerSession::new(link);
                let _ = sessions.insert(*peer, session.clone());
                session
            }
        }
    }

    /// Any number of incoming qp2p:Connections can be added.
    /// We will eventually converge to the same one in our comms with the peer.
    async fn add_incoming(&self, peer: &Peer, conn: qp2p::Connection) {
        {
            let session = self.sessions.read().await;
            if let Some(c) = session.get(peer) {
                // peer exists, add to it
                c.add(conn).await;
                return;
            }
            // else still not in list, go ahead and insert
        }

        let mut sessions = self.sessions.write().await;
        match sessions.get(peer) {
            // someone else inserted in the meanwhile, add to it
            Some(c) => c.add(conn).await,
            // still not in list, go ahead and insert
            None => {
                let link = Link::new_with(
                    *peer,
                    self.our_endpoint.clone(),
                    self.msg_listener.clone(),
                    conn,
                )
                .await;
                let session = PeerSession::new(link);
                let _ = sessions.insert(*peer, session);
            }
        }
    }

    // Helper to send a message to a single recipient.
    async fn send_to_one(
        &self,
        recipient: Peer,
        msg_id: MsgId,
        msg_priority: i32,
        msg_bytes: Bytes,
    ) -> (Peer, Result<SendWatcher>) {
        trace!(
            "Sending message ({} bytes, msg_id: {:?}) to {:?}",
            msg_bytes.len(),
            msg_id,
            recipient,
        );

        let peer = self.get_or_create(&recipient).await;
        let result = peer.send(msg_id, msg_priority, msg_bytes).await;

        (recipient, result)
    }
}

#[tracing::instrument(skip_all)]
fn setup_comms(
    our_endpoint: Endpoint,
    incoming_connections: IncomingConnections,
    monitoring: Measurements,
    receive_msg: mpsc::Sender<MsgEvent>,
) -> (Comm, MsgListener) {
    let (comm, msg_listener) = setup(our_endpoint, monitoring, receive_msg);

    listen(msg_listener.clone(), incoming_connections);

    (comm, msg_listener)
}

#[tracing::instrument(skip_all)]
fn setup(
    our_endpoint: Endpoint,
    monitoring: Measurements,
    receive_msg: mpsc::Sender<MsgEvent>,
) -> (Comm, MsgListener) {
    let back_pressure = BackPressure::new(monitoring);
    let (add_connection, conn_receiver) = mpsc::channel(100);
    let (count_msg, msg_counter) = mpsc::channel(1000);

    let msg_listener = MsgListener::new(add_connection, receive_msg, count_msg);

    let comm = Comm {
        our_endpoint,
        msg_listener: msg_listener.clone(),
        back_pressure: back_pressure.clone(),
        sessions: Arc::new(RwLock::new(BTreeMap::new())),
    };

    let _ = task::spawn(async move { count_msgs(back_pressure, msg_counter).await });
    let _ = task::spawn(receive_conns(comm.clone(), conn_receiver));

    (comm, msg_listener)
}

#[tracing::instrument(skip_all)]
async fn count_msgs(back_pressure: BackPressure, mut msg_counter: mpsc::Receiver<()>) {
    debug!("Entered msg counting listener loop.");
    while let Some(()) = msg_counter.recv().await {
        back_pressure.count_msg().await;
    }
    debug!("Exited msg counting listener loop..!");
}

#[tracing::instrument(skip_all)]
async fn receive_conns(comm: Comm, mut conn_receiver: mpsc::Receiver<ListenerEvent>) {
    while let Some(ListenerEvent::Connected { peer, connection }) = conn_receiver.recv().await {
        comm.add_incoming(&peer, connection).await;
    }
}

#[tracing::instrument(skip_all)]
fn listen(msg_listener: MsgListener, mut incoming_connections: IncomingConnections) {
    let _ = task::spawn(async move {
        while let Some((connection, incoming_msgs)) = incoming_connections.next().await {
            trace!(
                "incoming_connection from {:?} with connection_id {:?}",
                connection.remote_address(),
                connection.id()
            );

            msg_listener.listen(connection, incoming_msgs);
        }
    });
}

impl Drop for Comm {
    fn drop(&mut self) {
        // Close all existing connections and stop accepting new ones.
        // FIXME: this may be broken – `Comm` is clone, so this will break any clones?
        self.our_endpoint.close();
    }
}

#[derive(Debug)]
pub(crate) enum MsgEvent {
    Received {
        sender: Peer,
        wire_msg: WireMsg,
        original_bytes: Bytes,
    },
}

/// Returns the status of the send operation.
#[derive(Debug, Clone)]
pub(crate) enum DeliveryStatus {
    AllRecipients,
    MinDeliveryGroupSizeReached(Vec<Peer>),
    MinDeliveryGroupSizeFailed(Vec<Peer>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::data::{DataQuery, ServiceMsg};
    use crate::messaging::{DstLocation, MsgId, MsgKind, ServiceAuth};
    use crate::types::{ChunkAddress, Keypair, Peer};
    use assert_matches::assert_matches;
    use eyre::Result;
    use futures::future;
    use qp2p::Config;
    use rand::rngs::OsRng;
    use std::{net::Ipv4Addr, time::Duration};
    use tokio::{net::UdpSocket, sync::mpsc, time};

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm =
            Comm::first_node(local_addr(), Config::default(), Measurements::new(), tx).await?;

        let (peer0, mut rx0) = new_peer().await?;
        let (peer1, mut rx1) = new_peer().await?;

        let original_message = new_test_msg()?;

        let status = comm
            .send(&[peer0, peer1], 2, original_message.clone())
            .await?;

        assert_matches!(status, DeliveryStatus::AllRecipients);

        if let Some(bytes) = rx0.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message.clone());
        }

        if let Some(bytes) = rx1.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send_to_subset() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm =
            Comm::first_node(local_addr(), Config::default(), Measurements::new(), tx).await?;

        let (peer0, mut rx0) = new_peer().await?;
        let (peer1, mut rx1) = new_peer().await?;

        let original_message = new_test_msg()?;
        let status = comm
            .send(&[peer0, peer1], 1, original_message.clone())
            .await?;

        assert_matches!(status, DeliveryStatus::AllRecipients);

        if let Some(bytes) = rx0.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message);
        }

        assert!(time::timeout(TIMEOUT, rx1.recv())
            .await
            .unwrap_or_default()
            .is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::first_node(
            local_addr(),
            Config {
                // This makes this test faster.
                idle_timeout: Some(Duration::from_millis(1)),
                ..Config::default()
            },
            Measurements::new(),
            tx,
        )
        .await?;
        let invalid_peer = get_invalid_peer().await?;
        let invalid_addr = invalid_peer.addr();

        let status = comm.send(&[invalid_peer], 1, new_test_msg()?).await?;

        assert_matches!(
            &status,
            &DeliveryStatus::MinDeliveryGroupSizeFailed(_) => vec![invalid_addr]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send_after_failed_attempts() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::first_node(
            local_addr(),
            Config {
                idle_timeout: Some(Duration::from_millis(1)),
                ..Config::default()
            },
            Measurements::new(),
            tx,
        )
        .await?;
        let (peer, mut rx) = new_peer().await?;
        let invalid_peer = get_invalid_peer().await?;

        let message = new_test_msg()?;
        let status = comm.send(&[invalid_peer, peer], 1, message.clone()).await?;
        assert_matches!(status, DeliveryStatus::MinDeliveryGroupSizeReached(failed_recipients) => {
            assert_eq!(&failed_recipients, &[invalid_peer])
        });

        if let Some(bytes) = rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, message);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partially_successful_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::first_node(
            local_addr(),
            Config {
                idle_timeout: Some(Duration::from_millis(1)),
                ..Config::default()
            },
            Measurements::new(),
            tx,
        )
        .await?;
        let (peer, mut rx) = new_peer().await?;
        let invalid_peer = get_invalid_peer().await?;

        let message = new_test_msg()?;
        let status = comm.send(&[invalid_peer, peer], 2, message.clone()).await?;

        assert_matches!(
            status,
            DeliveryStatus::MinDeliveryGroupSizeFailed(_) => vec![invalid_peer]
        );

        if let Some(bytes) = rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, message);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_after_reconnect() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let send_comm =
            Comm::first_node(local_addr(), Config::default(), Measurements::new(), tx).await?;

        let (recv_endpoint, mut incoming_connections, _) =
            Endpoint::new_peer(local_addr(), &[], Config::default()).await?;
        let recv_addr = recv_endpoint.public_addr();
        let name = xor_name::rand::random();

        let msg0 = new_test_msg()?;
        let status = send_comm
            .send(&[Peer::new(name, recv_addr)], 1, msg0.clone())
            .await?;
        assert_matches!(status, DeliveryStatus::AllRecipients);

        let mut msg0_received = false;

        // Receive one message and disconnect from the peer
        {
            if let Some((_, mut incoming_msgs)) = incoming_connections.next().await {
                if let Some(msg) = time::timeout(TIMEOUT, incoming_msgs.next()).await?? {
                    assert_eq!(WireMsg::from(msg)?, msg0);
                    msg0_received = true;
                }

                // connection dropped here
            }
            assert!(msg0_received);
        }

        let msg1 = new_test_msg()?;
        let status = send_comm
            .send(&[Peer::new(name, recv_addr)], 1, msg1.clone())
            .await?;
        assert_matches!(status, DeliveryStatus::AllRecipients);

        let mut msg1_received = false;

        if let Some((_, mut incoming_msgs)) = incoming_connections.next().await {
            if let Some(msg) = time::timeout(TIMEOUT, incoming_msgs.next()).await?? {
                assert_eq!(WireMsg::from(msg)?, msg1);
                msg1_received = true;
            }
        }

        assert!(msg1_received);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn incoming_connection_lost() -> Result<()> {
        let (tx, mut rx0) = mpsc::channel(1);
        let comm0 =
            Comm::first_node(local_addr(), Config::default(), Measurements::new(), tx).await?;
        let addr0 = comm0.our_connection_info();

        let (tx, _rx) = mpsc::channel(1);
        let comm1 =
            Comm::first_node(local_addr(), Config::default(), Measurements::new(), tx).await?;

        // Send a message to establish the connection
        let status = comm1
            .send(
                &[Peer::new(xor_name::rand::random(), addr0)],
                1,
                new_test_msg()?,
            )
            .await?;
        assert_matches!(status, DeliveryStatus::AllRecipients);

        assert_matches!(rx0.recv().await, Some(MsgEvent::Received { .. }));
        // Drop `comm1` to cause connection lost.
        drop(comm1);

        assert_matches!(time::timeout(TIMEOUT, rx0.recv()).await, Err(_));

        Ok(())
    }

    fn new_test_msg() -> Result<WireMsg> {
        let dst_location = DstLocation::Node {
            name: xor_name::rand::random(),
            section_pk: bls::SecretKey::random().public_key(),
        };

        let mut rng = OsRng;
        let src_keypair = Keypair::new_ed25519(&mut rng);

        let msg = ServiceMsg::Query(DataQuery::GetChunk(ChunkAddress(xor_name::rand::random())));
        let payload = WireMsg::serialize_msg_payload(&msg)?;
        let auth = ServiceAuth {
            public_key: src_keypair.public_key(),
            signature: src_keypair.sign(&payload),
        };

        let wire_msg = WireMsg::new_msg(
            MsgId::new(),
            payload,
            MsgKind::ServiceMsg(auth),
            msg.priority(),
            dst_location,
        )?;

        Ok(wire_msg)
    }

    async fn new_peer() -> Result<(Peer, mpsc::Receiver<Bytes>)> {
        let (endpoint, mut incoming_connections, _) =
            Endpoint::new_peer(local_addr(), &[], Config::default()).await?;
        let addr = endpoint.public_addr();

        let (tx, rx) = mpsc::channel(1);

        let _handle = tokio::spawn(async move {
            while let Some((_, mut incoming_messages)) = incoming_connections.next().await {
                while let Ok(Some(msg)) = incoming_messages.next().await {
                    let _ = tx.send(msg).await;
                }
            }
        });

        Ok((Peer::new(xor_name::rand::random(), addr), rx))
    }

    async fn get_invalid_peer() -> Result<Peer> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = socket.local_addr()?;

        // Keep the socket alive to keep the address bound, but don't read/write to it so any
        // attempt to connect to it will fail.
        let _handle = tokio::spawn(async move {
            debug!("get invalid peer");
            future::pending::<()>().await;
            let _ = socket;
        });

        Ok(Peer::new(xor_name::rand::random(), addr))
    }

    fn local_addr() -> SocketAddr {
        (Ipv4Addr::LOCALHOST, 0).into()
    }
}
