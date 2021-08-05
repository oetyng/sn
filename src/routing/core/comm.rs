// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::WireMsg;
use crate::routing::error::{Error, Result};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use qp2p::{Endpoint, QuicP2p};
use std::net::SocketAddr;
use tokio::{sync::mpsc, task};
use xor_name::XorName;

// Communication component of the node to interact with other nodes.
#[derive(Clone)]
pub(crate) struct Comm {
    quic_p2p: QuicP2p,
    endpoint: Endpoint,
}

impl Drop for Comm {
    fn drop(&mut self) {
        // Close all existing connections and stop accepting new ones.
        self.endpoint.close();
    }
}

impl Comm {
    pub(crate) async fn new(
        transport_config: qp2p::Config,
        event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> Result<Self> {
        let quic_p2p = QuicP2p::with_config(Some(transport_config), &[], true)
            .map_err(|err| Error::InvalidConfig { err })?;

        // Don't bootstrap, just create an endpoint to listen to
        // the incoming messages from other nodes.
        // This also returns the a channel where we can listen for
        // disconnection events.
        let (endpoint, _incoming_connections, incoming_messages, disconnections) = quic_p2p
            .new_endpoint()
            .await
            .map_err(|err| Error::CannotConnectEndpoint { err })?;

        let _ = task::spawn(handle_incoming_messages(
            incoming_messages,
            event_tx.clone(),
        ));

        let _ = task::spawn(handle_disconnection_events(
            disconnections,
            event_tx.clone(),
        ));

        Ok(Self { quic_p2p, endpoint })
    }

    pub(crate) async fn bootstrap(
        transport_config: qp2p::Config,
        event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> Result<(Self, SocketAddr)> {
        let quic_p2p = QuicP2p::with_config(Some(transport_config), &[], true)
            .map_err(|err| Error::InvalidConfig { err })?;

        // Bootstrap to the network returning the connection to a node.
        // We can use the returned channels to listen for incoming messages and disconnection events
        let (endpoint, _incoming_connections, incoming_messages, disconnections, bootstrap_addr) =
            quic_p2p
                .bootstrap()
                .await
                .map_err(|err| Error::CannotConnectEndpoint { err })?;

        let _ = task::spawn(handle_incoming_messages(
            incoming_messages,
            event_tx.clone(),
        ));

        let _ = task::spawn(handle_disconnection_events(
            disconnections,
            event_tx.clone(),
        ));

        Ok((Self { quic_p2p, endpoint }, bootstrap_addr))
    }

    pub(crate) fn our_connection_info(&self) -> SocketAddr {
        self.endpoint.socket_addr()
    }

    /// Sends a message on an existing connection. If no such connection exists, returns an error.
    pub(crate) async fn send_on_existing_connection(
        &self,
        recipients: &[(XorName, SocketAddr)],
        mut wire_msg: WireMsg,
    ) -> Result<(), Error> {
        for (name, addr) in recipients {
            wire_msg.set_dst_xorname(*name);
            let bytes = wire_msg.serialize()?;

            self.endpoint
                .try_send_message(bytes, addr)
                .await
                .map_err(|err| {
                    error!(
                        "Sending message (msg_id: {}) to {:?} (name {:?}) failed with {}",
                        wire_msg.msg_id(),
                        addr,
                        name,
                        err
                    );
                    Error::FailedSend(*addr, *name)
                })?;
        }

        Ok(())
    }

    /// Tests whether the peer is reachable.
    pub(crate) async fn is_reachable(&self, peer: &SocketAddr) -> Result<(), Error> {
        let qp2p_config = qp2p::Config {
            local_ip: Some(self.endpoint.local_addr().ip()),
            local_port: Some(0),
            forward_port: false,
            ..Default::default()
        };

        let qp2p = QuicP2p::with_config(Some(qp2p_config), &[], false)
            .map_err(|err| Error::InvalidConfig { err })?;
        let (connectivity_endpoint, _, _, _) = qp2p
            .new_endpoint()
            .await
            .map_err(|err| Error::CannotConnectEndpoint { err })?;

        let result = connectivity_endpoint
            .is_reachable(peer)
            .await
            .map_err(|err| {
                info!("Peer {} is NOT externally reachable: {}", peer, err);
                Error::AddressNotReachable { err }
            })
            .map(|()| {
                info!("Peer {} is externally reachable.", peer);
            });
        connectivity_endpoint.close();
        result
    }

    /// Sends a message to multiple recipients. Attempts to send to `delivery_group_size`
    /// recipients out of the `recipients` list. If a send fails, attempts to send to the next peer
    /// until `delivery_group_size`  successful sends complete or there are no more recipients to
    /// try.
    ///
    /// Returns an `Error::ConnectionClosed` if the connection is closed locally. Else it returns a
    /// `SendStatus::MinDeliveryGroupSizeReached` or `SendStatus::MinDeliveryGroupSizeFailed` depending
    /// on if the minimum delivery group size is met or not. The failed recipients are sent along
    /// with the status. It returns a `SendStatus::AllRecipients` if message is sent to all the recipients.
    pub(crate) async fn send(
        &self,
        recipients: &[(XorName, SocketAddr)],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<SendStatus> {
        trace!(
            "Sending message to {} of {:?}",
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

        // Run all the sends concurrently (using `FuturesUnordered`). If any of them fails, pick
        // the next recipient and try to send to them. Proceed until the needed number of sends
        // succeeds or if there are no more recipients to pick.
        let send = |recipient: (XorName, SocketAddr), msg_bytes: Bytes| async move {
            trace!(
                "Sending message ({} bytes) to {} of delivery group size {}",
                msg_bytes.len(),
                recipient.1,
                delivery_group_size,
            );

            let result = self
                .endpoint
                .send_message(msg_bytes, &recipient.1)
                .await
                .map_err(|err| match err {
                    qp2p::Error::Connection(qp2p::ConnectionError::LocallyClosed) => {
                        Error::ConnectionClosed
                    }
                    _ => {
                        trace!("during sending, received error {:?}", err);
                        Error::AddressNotReachable { err }
                    }
                });

            (result, recipient.1)
        };

        let mut tasks: FuturesUnordered<_> = recipients[0..delivery_group_size]
            .iter()
            .map(|(name, recipient)| send((*name, *recipient), msg_bytes.clone()))
            .collect();

        let mut next = delivery_group_size;
        let mut successes = 0;
        let mut failed_recipients = vec![];

        while let Some((result, addr)) = tasks.next().await {
            match result {
                Ok(()) => successes += 1,
                Err(Error::ConnectionClosed) => {
                    // The connection was closed by us which means
                    // we are terminating so let's cut this short.
                    return Err(Error::ConnectionClosed);
                }
                Err(_) => {
                    failed_recipients.push(addr);

                    if next < recipients.len() {
                        tasks.push(send(recipients[next], msg_bytes.clone()));
                        next += 1;
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
                Ok(SendStatus::AllRecipients)
            } else {
                Ok(SendStatus::MinDeliveryGroupSizeReached(failed_recipients))
            }
        } else {
            Ok(SendStatus::MinDeliveryGroupSizeFailed(failed_recipients))
        }
    }
}

#[derive(Debug)]
pub(crate) enum ConnectionEvent {
    Received((SocketAddr, Bytes)),
    Disconnected(SocketAddr),
}

async fn handle_disconnection_events(
    mut disconnections: qp2p::DisconnectionEvents,
    event_tx: mpsc::Sender<ConnectionEvent>,
) {
    while let Some(peer_addr) = disconnections.next().await {
        let _ = event_tx
            .send(ConnectionEvent::Disconnected(peer_addr))
            .await;
    }
}

async fn handle_incoming_messages(
    mut incoming_msgs: qp2p::IncomingMessages,
    event_tx: mpsc::Sender<ConnectionEvent>,
) {
    while let Some((src, msg)) = incoming_msgs.next().await {
        let _ = event_tx.send(ConnectionEvent::Received((src, msg))).await;
    }
}

/// Returns the status of the send operation.
#[derive(Debug, Clone)]
pub(crate) enum SendStatus {
    AllRecipients,
    MinDeliveryGroupSizeReached(Vec<SocketAddr>),
    MinDeliveryGroupSizeFailed(Vec<SocketAddr>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::{section_info::SectionInfoMsg, DstLocation, WireMsg};
    use crate::types::PublicKey;
    use assert_matches::assert_matches;
    use eyre::Result;
    use futures::future;
    use qp2p::Config;
    use std::{net::Ipv4Addr, time::Duration};
    use tokio::{net::UdpSocket, sync::mpsc, time};

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(transport_config(), tx).await?;

        let mut peer0 = Peer::new().await?;
        let mut peer1 = Peer::new().await?;

        let original_message = new_section_info_message()?;

        let status = comm
            .send(
                &[(peer0._name, peer0.addr), (peer1._name, peer1.addr)],
                2,
                original_message.clone(),
            )
            .await?;

        assert_matches!(status, SendStatus::AllRecipients);

        if let Some(bytes) = peer0.rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message.clone());
        }

        if let Some(bytes) = peer1.rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send_to_subset() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(transport_config(), tx).await?;

        let mut peer0 = Peer::new().await?;
        let mut peer1 = Peer::new().await?;

        let original_message = new_section_info_message()?;
        let status = comm
            .send(
                &[(peer0._name, peer0.addr), (peer1._name, peer1.addr)],
                1,
                original_message.clone(),
            )
            .await?;

        assert_matches!(status, SendStatus::AllRecipients);

        if let Some(bytes) = peer0.rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message);
        }

        assert!(time::timeout(TIMEOUT, peer1.rx.recv())
            .await
            .unwrap_or_default()
            .is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(
            Config {
                // This makes this test faster.
                idle_timeout_msec: Some(1),
                ..transport_config()
            },
            tx,
        )
        .await?;
        let invalid_addr = get_invalid_addr().await?;

        let status = comm
            .send(
                &[(XorName::random(), invalid_addr)],
                1,
                new_section_info_message()?,
            )
            .await?;

        assert_matches!(
            &status,
            &SendStatus::MinDeliveryGroupSizeFailed(_) => vec![invalid_addr]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send_after_failed_attempts() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(
            Config {
                idle_timeout_msec: Some(1),
                ..transport_config()
            },
            tx,
        )
        .await?;
        let mut peer = Peer::new().await?;
        let invalid_addr = get_invalid_addr().await?;
        let name = XorName::random();

        let message = new_section_info_message()?;
        let _ = comm
            .send(
                &[(name, invalid_addr), (peer._name, peer.addr)],
                1,
                message.clone(),
            )
            .await?;

        if let Some(bytes) = peer.rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, message);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partially_successful_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(
            Config {
                idle_timeout_msec: Some(1),
                ..transport_config()
            },
            tx,
        )
        .await?;
        let mut peer = Peer::new().await?;
        let invalid_addr = get_invalid_addr().await?;
        let name = XorName::random();

        let message = new_section_info_message()?;
        let status = comm
            .send(
                &[(name, invalid_addr), (peer._name, peer.addr)],
                2,
                message.clone(),
            )
            .await?;

        assert_matches!(
            status,
            SendStatus::MinDeliveryGroupSizeFailed(_) => vec![invalid_addr]
        );

        if let Some(bytes) = peer.rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, message);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_after_reconnect() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let send_comm = Comm::new(transport_config(), tx).await?;

        let recv_transport = QuicP2p::with_config(Some(transport_config()), &[], false)?;
        let (recv_endpoint, _, mut incoming_msgs, _) = recv_transport.new_endpoint().await?;
        let recv_addr = recv_endpoint.socket_addr();
        let name = XorName::random();

        // Send the first message.
        let key0 = bls::SecretKey::random().public_key();
        let query = SectionInfoMsg::GetSectionQuery(PublicKey::Bls(key0));
        let dst_location = DstLocation::Node {
            name,
            section_pk: key0,
        };
        let msg0 = WireMsg::new_section_info_msg(&query, dst_location)?;
        let _ = send_comm
            .send(&[(name, recv_addr)], 1, msg0.clone())
            .await?;

        let mut msg0_received = false;

        // Receive one message and disconnect from the peer
        {
            if let Some((src, msg)) = time::timeout(TIMEOUT, incoming_msgs.next()).await? {
                assert_eq!(WireMsg::from(msg)?, msg0);
                msg0_received = true;
                recv_endpoint.disconnect_from(&src).await?;
            }
            assert!(msg0_received);
        }

        // Send the second message.
        let key1 = bls::SecretKey::random().public_key();
        let query = SectionInfoMsg::GetSectionQuery(PublicKey::Bls(key1));
        let dst_location = DstLocation::Node {
            name,
            section_pk: key1,
        };
        let msg1 = WireMsg::new_section_info_msg(&query, dst_location)?;
        let _ = send_comm
            .send(&[(name, recv_addr)], 1, msg1.clone())
            .await?;

        let mut msg1_received = false;

        if let Some((_src, msg)) = time::timeout(TIMEOUT, incoming_msgs.next()).await? {
            assert_eq!(WireMsg::from(msg)?, msg1);
            msg1_received = true;
        }

        assert!(msg1_received);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn incoming_connection_lost() -> Result<()> {
        let (tx, mut rx0) = mpsc::channel(1);
        let comm0 = Comm::new(transport_config(), tx).await?;
        let addr0 = comm0.our_connection_info();

        let (tx, _rx) = mpsc::channel(1);
        let comm1 = Comm::new(transport_config(), tx).await?;
        let addr1 = comm1.our_connection_info();

        // Send a message to establish the connection
        let _ = comm1
            .send(
                &[(XorName::random(), addr0)],
                1,
                new_section_info_message()?,
            )
            .await?;

        assert_matches!(rx0.recv().await, Some(ConnectionEvent::Received(_)));
        // Drop `comm1` to cause connection lost.
        drop(comm1);

        assert_matches!(
            time::timeout(TIMEOUT, rx0.recv()).await?,
            Some(ConnectionEvent::Disconnected(addr)) => assert_eq!(addr, addr1)
        );

        Ok(())
    }

    fn transport_config() -> Config {
        Config {
            local_ip: Some(Ipv4Addr::LOCALHOST.into()),
            ..Default::default()
        }
    }

    fn new_section_info_message() -> Result<WireMsg> {
        let random_bls_pk = bls::SecretKey::random().public_key();
        let query = SectionInfoMsg::GetSectionQuery(PublicKey::Bls(random_bls_pk));
        let dst_location = DstLocation::Node {
            name: XorName::random(),
            section_pk: bls::SecretKey::random().public_key(),
        };

        let wire_msg = WireMsg::new_section_info_msg(&query, dst_location)?;
        Ok(wire_msg)
    }

    struct Peer {
        addr: SocketAddr,
        _incoming_connections: qp2p::IncomingConnections,
        _disconnections: qp2p::DisconnectionEvents,
        _name: XorName,
        rx: mpsc::Receiver<Bytes>,
    }

    impl Peer {
        async fn new() -> Result<Self> {
            let transport = QuicP2p::with_config(Some(transport_config()), &[], false)?;

            let (endpoint, incoming_connections, mut incoming_messages, disconnections) =
                transport.new_endpoint().await?;
            let addr = endpoint.socket_addr();

            let (tx, rx) = mpsc::channel(1);

            let _ = tokio::spawn(async move {
                while let Some((_src, msg)) = incoming_messages.next().await {
                    let _ = tx.send(msg).await;
                }
            });

            Ok(Self {
                addr,
                rx,
                _incoming_connections: incoming_connections,
                _disconnections: disconnections,
                _name: XorName::random(),
            })
        }
    }

    async fn get_invalid_addr() -> Result<SocketAddr> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = socket.local_addr()?;

        // Keep the socket alive to keep the address bound, but don't read/write to it so any
        // attempt to connect to it will fail.
        let _ = tokio::spawn(async move {
            future::pending::<()>().await;
            let _ = socket;
        });

        Ok(addr)
    }
}
