// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(test)]
pub(crate) mod tests;

pub(crate) mod command;

pub(super) mod config;
mod dispatcher;
pub(super) mod event;
pub(super) mod event_stream;

use self::{
    command::Command,
    config::Config,
    dispatcher::Dispatcher,
    event::{Elders, Event, NodeElderChange},
    event_stream::EventStream,
};
use crate::messaging::{
    node::{NodeMsg, Peer},
    DstLocation, SectionAuthorityProvider, WireMsg,
};
use crate::routing::{
    core::{join_network, ChunkStore, Comm, ConnectionEvent, Core, RegisterStorage},
    ed25519,
    error::Result,
    messages::WireMsgUtils,
    network::NetworkUtils,
    node::Node,
    peer::PeerUtils,
    section::SectionUtils,
    Error, SectionAuthorityProviderUtils, MIN_ADULT_AGE,
};
use crate::{dbs::UsedSpace, messaging::data::ChunkDataExchange};
use ed25519_dalek::{PublicKey, Signature, Signer, KEYPAIR_LENGTH};

use crate::types::PublicKey as DtPublicKey;
use itertools::Itertools;
use secured_linked_list::SecuredLinkedList;
use std::path::PathBuf;
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};
use tokio::{sync::mpsc, task};
use xor_name::{Prefix, XorName};

/// Interface for sending and receiving messages to and from other nodes, in the role of a full
/// routing node.
///
/// A node is a part of the network that can route messages and be a member of a section or group
/// location. Its methods can be used to send requests and responses as either an individual
/// `Node` or as a part of a section or group location. Their `src` argument indicates that
/// role, and can be `crate::messaging::SrcLocation::Node` or `crate::messaging::SrcLocation::Section`.
#[allow(missing_debug_implementations)]
pub struct Routing {
    dispatcher: Arc<Dispatcher>,
}

static EVENT_CHANNEL_SIZE: usize = 20;

impl Routing {
    ////////////////////////////////////////////////////////////////////////////
    // Public API
    ////////////////////////////////////////////////////////////////////////////

    /// Creates new node using the given config and bootstraps it to the network.
    ///
    /// NOTE: It's not guaranteed this function ever returns. This can happen due to messages being
    /// lost in transit during bootstrapping, or other reasons. It's the responsibility of the
    /// caller to handle this case, for example by using a timeout.
    pub async fn new(
        config: Config,
        used_space: UsedSpace,
        root_storage_dir: PathBuf,
    ) -> Result<(Self, EventStream)> {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_SIZE);
        let (connection_event_tx, mut connection_event_rx) = mpsc::channel(1);

        let core = if config.first {
            // Genesis node having a fix age of 255.
            let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), 255);
            let node_name = ed25519::name(&keypair.public);

            info!(
                "{} Starting a new network as the genesis node (PID: {}).",
                node_name,
                std::process::id()
            );

            let comm = Comm::new(config.transport_config, connection_event_tx).await?;
            let node = Node::new(keypair, comm.our_connection_info());
            let core = Core::first_node(comm, node, event_tx, used_space, root_storage_dir)?;

            let section = core.section();

            let elders = Elders {
                prefix: *section.prefix(),
                key: *section.chain().last_key(),
                remaining: BTreeSet::new(),
                added: section.authority_provider().names(),
                removed: BTreeSet::new(),
            };

            core.send_event(Event::EldersChanged {
                elders,
                self_status_change: NodeElderChange::Promoted,
            })
            .await;
            info!("{} Genesis node started!", node_name);

            core
        } else {
            let keypair = config.keypair.unwrap_or_else(|| {
                ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE)
            });
            let node_name = ed25519::name(&keypair.public);
            info!("{} Bootstrapping a new node.", node_name);

            let (comm, bootstrap_addr) =
                Comm::bootstrap(config.transport_config, connection_event_tx).await?;
            info!(
                "{} Joining as a new node (PID: {}) from {} to {}",
                node_name,
                std::process::id(),
                comm.our_connection_info(),
                bootstrap_addr
            );
            let joining_node = Node::new(keypair, comm.our_connection_info());
            let (node, section) = join_network(
                joining_node,
                &comm,
                &mut connection_event_rx,
                bootstrap_addr,
            )
            .await?;
            let core = Core::new(
                comm,
                node,
                section,
                None,
                event_tx,
                used_space,
                root_storage_dir.to_path_buf(),
            )?;
            info!("{} Joined the network!", core.node().name());

            core
        };

        let dispatcher = Arc::new(Dispatcher::new(core));
        let event_stream = EventStream::new(event_rx);

        // Start listening to incoming connections.
        let _ = task::spawn(handle_connection_events(
            dispatcher.clone(),
            connection_event_rx,
        ));

        let routing = Self { dispatcher };

        Ok((routing, event_stream))
    }

    pub(crate) async fn get_register_storage(&self) -> RegisterStorage {
        self.dispatcher.get_register_storage().await
    }

    pub(crate) async fn get_chunk_storage(&self) -> ChunkStore {
        self.dispatcher.get_chunk_storage().await
    }
    pub(crate) async fn get_chunk_data_of(&self, prefix: &Prefix) -> ChunkDataExchange {
        self.dispatcher.get_chunk_data_of(prefix).await
    }
    pub(crate) async fn increase_full_node_count(&self, node_id: &DtPublicKey) {
        self.dispatcher.increase_full_node_count(node_id).await
    }
    pub(crate) async fn retain_members_only(&self, members: BTreeSet<XorName>) -> Result<()> {
        self.dispatcher.retain_members_only(members).await
    }

    pub(crate) async fn update_chunks(&self, chunks: ChunkDataExchange) {
        self.dispatcher
            .core
            .read()
            .await
            .update_chunks(chunks)
            .await
    }

    /// Sets the JoinsAllowed flag.
    pub async fn set_joins_allowed(&self, joins_allowed: bool) -> Result<()> {
        let command = Command::SetJoinsAllowed(joins_allowed);
        self.dispatcher.clone().handle_commands(command).await
    }

    /// Starts a proposal that a node has gone offline.
    /// This can be done only by an Elder.
    pub async fn propose_offline(&self, name: XorName) -> Result<()> {
        if !self.is_elder().await {
            return Err(Error::InvalidState);
        }
        let command = Command::ProposeOffline(name);
        self.dispatcher.clone().handle_commands(command).await
    }

    /// Signals the Elders of our section to test connectivity to a node.
    pub async fn start_connectivity_test(&self, name: XorName) -> Result<()> {
        let command = Command::StartConnectivityTest(name);
        self.dispatcher.clone().handle_commands(command).await
    }

    /// Returns the current age of this node.
    pub async fn age(&self) -> u8 {
        self.dispatcher.core.read().await.node().age()
    }

    /// Returns the ed25519 public key of this node.
    pub async fn public_key(&self) -> PublicKey {
        self.dispatcher.core.read().await.node().keypair.public
    }

    /// Returns the ed25519 keypair of this node, as bytes.
    pub async fn keypair_as_bytes(&self) -> [u8; KEYPAIR_LENGTH] {
        self.dispatcher.core.read().await.node().keypair.to_bytes()
    }

    /// Signs `data` with the ed25519 key of this node.
    pub async fn sign_as_node(&self, data: &[u8]) -> Signature {
        self.dispatcher.core.read().await.node().keypair.sign(data)
    }

    /// Signs `data` with the BLS secret key share of this node, if it has any. Returns
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn sign_as_elder(
        &self,
        data: &[u8],
        public_key: &bls::PublicKey,
    ) -> Result<(usize, bls::SignatureShare)> {
        self.dispatcher
            .core
            .read()
            .await
            .sign_with_section_key_share(data, public_key)
    }

    /// Verifies `signature` on `data` with the ed25519 public key of this node.
    pub async fn verify(&self, data: &[u8], signature: &Signature) -> bool {
        self.dispatcher
            .core
            .read()
            .await
            .node()
            .keypair
            .verify(data, signature)
            .is_ok()
    }

    /// The name of this node.
    pub async fn name(&self) -> XorName {
        self.dispatcher.core.read().await.node().name()
    }

    /// Returns connection info of this node.
    pub async fn our_connection_info(&self) -> SocketAddr {
        self.dispatcher.core.read().await.our_connection_info()
    }

    /// Returns the Section Signed Chain
    pub async fn section_chain(&self) -> SecuredLinkedList {
        self.dispatcher.core.read().await.section_chain().clone()
    }

    /// Prefix of our section
    pub async fn our_prefix(&self) -> Prefix {
        *self.dispatcher.core.read().await.section().prefix()
    }

    /// Finds out if the given XorName matches our prefix.
    pub async fn matches_our_prefix(&self, name: &XorName) -> bool {
        self.our_prefix().await.matches(name)
    }

    /// Returns whether the node is Elder.
    pub async fn is_elder(&self) -> bool {
        self.dispatcher.core.read().await.is_elder()
    }

    /// Returns the information of all the current section elders.
    pub async fn our_elders(&self) -> Vec<Peer> {
        self.dispatcher
            .core
            .read()
            .await
            .section()
            .authority_provider()
            .peers()
            .collect()
    }

    /// Returns the elders of our section sorted by their distance to `name` (closest first).
    pub async fn our_elders_sorted_by_distance_to(&self, name: &XorName) -> Vec<Peer> {
        self.our_elders()
            .await
            .into_iter()
            .sorted_by(|lhs, rhs| name.cmp_distance(lhs.name(), rhs.name()))
            .collect()
    }

    /// Returns the information of all the current section adults.
    pub async fn our_adults(&self) -> Vec<Peer> {
        self.dispatcher
            .core
            .read()
            .await
            .section()
            .adults()
            .copied()
            .collect()
    }

    /// Returns the adults of our section sorted by their distance to `name` (closest first).
    /// If we are not elder or if there are no adults in the section, returns empty vec.
    pub async fn our_adults_sorted_by_distance_to(&self, name: &XorName) -> Vec<Peer> {
        self.our_adults()
            .await
            .into_iter()
            .sorted_by(|lhs, rhs| name.cmp_distance(lhs.name(), rhs.name()))
            .collect()
    }

    /// Returns our section's authority provider.
    pub async fn our_section_auth(&self) -> SectionAuthorityProvider {
        self.dispatcher
            .core
            .read()
            .await
            .section()
            .authority_provider()
            .clone()
    }

    /// Returns the info about other sections in the network known to us.
    pub async fn other_sections(&self) -> Vec<SectionAuthorityProvider> {
        self.dispatcher
            .core
            .read()
            .await
            .network()
            .all()
            .cloned()
            .collect()
    }

    /// Returns the info about the section matching the name.
    pub async fn matching_section(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        let core = self.dispatcher.core.read().await;
        core.matching_section(name)
    }

    /// Builds a WireMsg signed by this Node
    pub async fn sign_single_src_msg(
        &self,
        node_msg: NodeMsg,
        dst: DstLocation,
    ) -> Result<WireMsg> {
        let src_section_pk = *self.section_chain().await.last_key();
        WireMsg::single_src(
            self.dispatcher.core.read().await.node(),
            dst,
            node_msg,
            src_section_pk,
        )
    }

    /// Builds a WireMsg signed for accumulateion at destination
    pub async fn sign_msg_for_dst_accumulation(
        &self,
        node_msg: NodeMsg,
        dst: DstLocation,
    ) -> Result<WireMsg> {
        let src = self.name().await;
        let src_section_pk = *self.section_chain().await.last_key();

        WireMsg::for_dst_accumulation(
            self.dispatcher
                .core
                .read()
                .await
                .key_share()
                .map_err(|err| err)?,
            src,
            dst,
            node_msg,
            src_section_pk,
        )
    }

    /// Send a message.
    /// Messages sent here, either section to section or node to node.
    pub async fn send_message(&self, wire_msg: WireMsg) -> Result<()> {
        self.dispatcher
            .clone()
            .handle_commands(Command::ParseAndSendWireMsg(wire_msg))
            .await
    }

    /// Returns the current BLS public key set if this node has one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn public_key_set(&self) -> Result<bls::PublicKeySet> {
        self.dispatcher.core.read().await.public_key_set()
    }

    /// Returns our index in the current BLS group if this node is a member of one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn our_index(&self) -> Result<usize> {
        self.dispatcher.core.read().await.our_index()
    }
}

// Listen for incoming connection events and handle them.
async fn handle_connection_events(
    dispatcher: Arc<Dispatcher>,
    mut incoming_conns: mpsc::Receiver<ConnectionEvent>,
) {
    while let Some(event) = incoming_conns.recv().await {
        match event {
            ConnectionEvent::Disconnected(addr) => {
                trace!("Lost connection to {:?}", addr);
                let _ = dispatcher
                    .clone()
                    .handle_commands(Command::HandleConnectionLost(addr))
                    .await;
            }
            ConnectionEvent::Received((sender, bytes)) => {
                trace!(
                    "New message ({} bytes) received from: {}",
                    bytes.len(),
                    sender
                );
                let wire_msg = match WireMsg::from(bytes) {
                    Ok(wire_msg) => wire_msg,
                    Err(error) => {
                        error!("Failed to deserialize message header: {}", error);
                        continue;
                    }
                };

                let span = {
                    let mut core = dispatcher.core.write().await;

                    if !core.add_to_filter(&wire_msg).await {
                        trace!(
                            "not handling message {:?} from {}, already handled",
                            wire_msg.msg_id(),
                            sender,
                        );
                        continue;
                    }

                    trace_span!("handle_message", name = %core.node().name(), %sender)
                };
                let _span_guard = span.enter();

                let command = Command::HandleMessage { sender, wire_msg };
                let _ = task::spawn(dispatcher.clone().handle_commands(command));
            }
        }
    }

    error!("Fatal error, the stream for incoming connections has been unexpectedly closed. No new connections or messages can be received from the network from here on.");
}
