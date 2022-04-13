// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(test)]
pub(crate) mod tests;

pub(crate) mod cmds;

pub(super) mod event;
pub(super) mod event_stream;
pub(super) mod flowcontrol;

use self::{
    cmds::Cmd,
    event::{DeprecatedEvent, Elders, Event, NodeElderChange},
    event_stream::{DeprecatedEventStream, EventStream},
    flowcontrol::FlowControl,
};

use crate::messaging::{system::SystemMsg, DstLocation, WireMsg};
use crate::node::{
    cfg::keypair_storage::{get_reward_pk, store_network_keypair, store_new_reward_keypair},
    core::{join_network, Comm, Node},
    ed25519,
    error::{Error, Result},
    //logging::{log_ctx::LogCtx, run_system_logger},
    messages::WireMsgUtils,
    network_knowledge::SectionAuthorityProvider,
    Config,
    NodeInfo,
    Peer,
    MIN_ADULT_AGE,
};
use crate::types::{log_markers::LogMarker, PublicKey as TypesPublicKey};
use crate::UsedSpace;

use ed25519_dalek::PublicKey;
use itertools::Itertools;
use rand::rngs::OsRng;
use secured_linked_list::SecuredLinkedList;
use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use xor_name::{Prefix, XorName};

use super::core::Measurements;

/// Interface for sending and receiving messages to and from other nodes, in the role of a full
/// routing node.
///
/// A node is a part of the network that can route messages and be a member of a section or group
/// location. Its methods can be used to send requests and responses as either an individual
/// `Node` or as a part of a section or group location. Their `src` argument indicates that
/// role, and can be `crate::messaging::SrcLocation::Node` or `crate::messaging::SrcLocation::Section`.
#[allow(missing_debug_implementations)]
pub struct NodeApi {
    node: Arc<Node>,
    flow_ctrl: FlowControl,
}

static DEPRECATED_EVENT_CHANNEL_SIZE: usize = 20;

impl NodeApi {
    ////////////////////////////////////////////////////////////////////////////
    // Public API
    ////////////////////////////////////////////////////////////////////////////

    /// Initialize a new node.
    pub async fn new(
        config: &Config,
        joining_timeout: Duration,
    ) -> Result<(Self, EventStream, DeprecatedEventStream)> {
        let root_dir_buf = config.root_dir()?;
        let root_dir = root_dir_buf.as_path();
        tokio::fs::create_dir_all(root_dir).await?;

        let _reward_key = match get_reward_pk(root_dir).await? {
            Some(public_key) => TypesPublicKey::Ed25519(public_key),
            None => {
                let mut rng = OsRng;
                let keypair = ed25519_dalek::Keypair::generate(&mut rng);
                store_new_reward_keypair(root_dir, &keypair).await?;
                TypesPublicKey::Ed25519(keypair.public)
            }
        };

        let used_space = UsedSpace::new(config.max_capacity());

        let (api, network_events, deprecated_network_events) = tokio::time::timeout(
            joining_timeout,
            Self::start_node(config, used_space, root_dir),
        )
        .await
        .map_err(|_| Error::JoinTimeout)??;

        // Network keypair may have to be changed due to naming criteria or network requirements.
        let keypair_as_bytes = api.node.info.read().await.keypair.to_bytes();
        store_network_keypair(root_dir, keypair_as_bytes).await?;

        let our_pid = std::process::id();
        let node_prefix = api.our_prefix().await;
        let node_name = api.name().await;
        let node_age = api.age().await;
        let our_conn_info = api.our_connection_info().await;
        let our_conn_info_json = serde_json::to_string(&our_conn_info)
            .unwrap_or_else(|_| "Failed to serialize connection info".into());
        println!(
            "Node PID: {:?}, prefix: {:?}, name: {:?}, age: {}, connection info:\n{}",
            our_pid, node_prefix, node_name, node_age, our_conn_info_json,
        );
        info!(
            "Node PID: {:?}, prefix: {:?}, name: {:?}, age: {}, connection info: {}",
            our_pid, node_prefix, node_name, node_age, our_conn_info_json,
        );

        //run_system_logger(LogCtx::new(api.dispatcher.clone()), config.resource_logs).await;

        Ok((api, network_events, deprecated_network_events))
    }

    // Private helper to create a new node using the given config and bootstraps it to the network.
    //
    // NOTE: It's not guaranteed this function ever returns. This can happen due to messages being
    // lost in transit during bootstrapping, or other reasons. It's the responsibility of the
    // caller to handle this case, for example by using a timeout.
    async fn start_node(
        config: &Config,
        used_space: UsedSpace,
        root_storage_dir: &Path,
    ) -> Result<(Self, EventStream, DeprecatedEventStream)> {
        let (deprecated_event_tx, deprecated_event_rx) =
            mpsc::channel(DEPRECATED_EVENT_CHANNEL_SIZE);
        let (connection_event_tx, mut connection_event_rx) = mpsc::channel(1);

        let local_addr = config
            .local_addr
            .unwrap_or_else(|| SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)));

        let monitoring = Measurements::new();

        let node = if config.is_first() {
            // Genesis node having a fix age of 255.
            let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), 255);
            let node_name = ed25519::name(&keypair.public);

            info!(
                "{} Starting a new network as the genesis node (PID: {}).",
                node_name,
                std::process::id()
            );

            let comm = Comm::first_node(
                local_addr,
                config.network_config().clone(),
                monitoring.clone(),
                connection_event_tx,
            )
            .await?;
            let info = NodeInfo::new(keypair, comm.our_connection_info());

            let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
            let node = Node::first_node(
                comm,
                info,
                deprecated_event_tx,
                used_space.clone(),
                root_storage_dir.to_path_buf(),
                genesis_sk_set,
            )
            .await?;

            let network_knowledge = node.network_knowledge();

            let elders = Elders {
                prefix: network_knowledge.prefix().await,
                key: network_knowledge.section_key().await,
                remaining: BTreeSet::new(),
                added: network_knowledge.authority_provider().await.names(),
                removed: BTreeSet::new(),
            };

            info!("{}", LogMarker::PromotedToElder);
            node.send_deprecated_event(DeprecatedEvent::EldersChanged {
                elders,
                self_status_change: NodeElderChange::Promoted,
            })
            .await;

            let genesis_key = network_knowledge.genesis_key();
            info!(
                "{} Genesis node started!. Genesis key {:?}, hex: {}",
                node_name,
                genesis_key,
                hex::encode(genesis_key.to_bytes())
            );

            node
        } else {
            let genesis_key_str = config.genesis_key.as_ref().ok_or_else(|| {
                Error::Configuration("Network's genesis key was not provided.".to_string())
            })?;
            let genesis_key = TypesPublicKey::bls_from_hex(genesis_key_str)?
                .bls()
                .ok_or_else(|| {
                    Error::Configuration(
                        "Unexpectedly failed to obtain genesis key from configuration.".to_string(),
                    )
                })?;

            let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE);
            let node_name = ed25519::name(&keypair.public);
            info!("{} Bootstrapping a new node.", node_name);

            let (comm, bootstrap_addr) = Comm::bootstrap(
                local_addr,
                config
                    .hard_coded_contacts
                    .iter()
                    .copied()
                    .collect_vec()
                    .as_slice(),
                config.network_config().clone(),
                monitoring.clone(),
                connection_event_tx,
            )
            .await?;
            info!(
                "{} Joining as a new node (PID: {}) our socket: {}, bootstrapper was: {}, network's genesis key: {:?}",
                node_name,
                std::process::id(),
                comm.our_connection_info(),
                bootstrap_addr,
                genesis_key
            );

            let joining_node = NodeInfo::new(keypair, comm.our_connection_info());
            let (info, network_knowledge) = join_network(
                joining_node,
                &comm,
                &mut connection_event_rx,
                bootstrap_addr,
                genesis_key,
            )
            .await?;

            let node = Node::new(
                comm,
                info,
                network_knowledge,
                None,
                deprecated_event_tx,
                used_space.clone(),
                root_storage_dir.to_path_buf(),
            )
            .await?;
            info!("{} Joined the network!", node.info.read().await.name());
            info!("Our AGE: {}", node.info.read().await.age());

            node
        };

        let node = Arc::new(node);
        let (flow_ctrl, event_stream) =
            FlowControl::new(node.clone(), monitoring, connection_event_rx);
        let api = Self { node, flow_ctrl };

        Ok((
            api,
            event_stream,
            DeprecatedEventStream::new(deprecated_event_rx),
        ))
    }

    /// Returns the current age of this node.
    pub async fn age(&self) -> u8 {
        self.node.info.read().await.age()
    }

    /// Returns the ed25519 public key of this node.
    pub async fn public_key(&self) -> PublicKey {
        self.node.info.read().await.keypair.public
    }

    /// The name of this node.
    pub async fn name(&self) -> XorName {
        self.node.info.read().await.name()
    }

    /// Returns connection info of this node.
    pub async fn our_connection_info(&self) -> SocketAddr {
        self.node.our_connection_info()
    }

    /// Returns the Section Signed Chain
    pub async fn section_chain(&self) -> SecuredLinkedList {
        self.node.section_chain().await
    }

    /// Returns the Section Chain's genesis key
    pub async fn genesis_key(&self) -> bls::PublicKey {
        *self.node.network_knowledge().genesis_key()
    }

    /// Prefix of our section
    pub async fn our_prefix(&self) -> Prefix {
        self.node.network_knowledge().prefix().await
    }

    /// Returns whether the node is Elder.
    pub async fn is_elder(&self) -> bool {
        self.node.is_elder().await
    }

    /// Returns the information of all the current section elders.
    pub async fn our_elders(&self) -> Vec<Peer> {
        self.node.network_knowledge().elders().await
    }

    /// Returns the information of all the current section adults.
    pub async fn our_adults(&self) -> Vec<Peer> {
        self.node.network_knowledge().adults().await
    }

    /// Returns the info about the section matching the name.
    pub async fn matching_section(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        self.node.matching_section(name).await
    }

    /// Builds a WireMsg signed by this Node
    pub async fn sign_single_src_msg(
        &self,
        node_msg: SystemMsg,
        dst: DstLocation,
    ) -> Result<WireMsg> {
        let src_section_pk = *self.section_chain().await.last_key();
        WireMsg::single_src(
            &self.node.info.read().await.clone(),
            dst,
            node_msg,
            src_section_pk,
        )
    }

    /// Send a message.
    /// Messages sent here, either section to section or node to node.
    pub async fn send_msg_to_nodes(&self, wire_msg: WireMsg) -> Result<()> {
        trace!(
            "{:?} {:?}",
            LogMarker::DispatchSendMsgCmd,
            wire_msg.msg_id()
        );

        if let Some(cmd) = self.node.send_msg_to_nodes(wire_msg).await? {
            self.flow_ctrl.process(cmd).await?;
        }

        Ok(())
    }

    /// Returns the current BLS public key set if this node has one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn public_key_set(&self) -> Result<bls::PublicKeySet> {
        self.node.public_key_set().await
    }
}
