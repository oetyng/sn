// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::dbs::UsedSpace;
use crate::messaging::{data::ChunkDataExchange, node::NodeMsg, DstLocation, WireMsg};
use crate::node::{state_db::store_network_keypair, Config as NodeConfig, Error, Result};
use crate::routing::{
    ChunkStore, Config as RoutingConfig, Error as RoutingError, EventStream, PeerUtils,
    RegisterStorage, Routing as RoutingNode, SectionAuthorityProviderUtils,
};
use crate::types::PublicKey;
use bls::{PublicKey as BlsPublicKey, PublicKeySet};
use ed25519_dalek::PublicKey as Ed25519PublicKey;
use secured_linked_list::SecuredLinkedList;
use std::{collections::BTreeSet, net::SocketAddr, path::Path, sync::Arc};
use xor_name::{Prefix, XorName};

///
#[derive(Clone)]
pub(crate) struct Network {
    routing: Arc<RoutingNode>,
}

#[allow(missing_docs)]
impl Network {
    pub(crate) async fn new(
        root_dir: &Path,
        config: &NodeConfig,
        used_space: UsedSpace,
    ) -> Result<(Self, EventStream)> {
        let routing_config = RoutingConfig {
            first: config.is_first(),
            transport_config: config.network_config().clone(),
            keypair: None,
        };
        let (routing, event_stream) =
            RoutingNode::new(routing_config, used_space, root_dir.to_path_buf()).await?;

        // Network keypair may have to be changed due to naming criteria or network requirements.
        store_network_keypair(root_dir, routing.keypair_as_bytes().await).await?;

        Ok((
            Self {
                routing: Arc::new(routing),
            },
            event_stream,
        ))
    }

    pub(crate) async fn get_register_storage(&self) -> RegisterStorage {
        self.routing.get_register_storage().await
    }

    pub(crate) async fn get_chunk_storage(&self) -> ChunkStore {
        self.routing.get_chunk_storage().await
    }

    pub(crate) async fn get_chunk_data_of(&self, prefix: &Prefix) -> ChunkDataExchange {
        self.routing.get_chunk_data_of(prefix).await
    }
    pub(crate) async fn increase_full_node_count(&self, node_id: &PublicKey) {
        self.routing.increase_full_node_count(node_id).await
    }
    pub(crate) async fn retain_members_only(&self, members: BTreeSet<XorName>) -> Result<()> {
        self.routing
            .retain_members_only(members)
            .await
            .map_err(Error::from)
    }

    pub(crate) async fn age(&self) -> u8 {
        self.routing.age().await
    }

    pub(crate) async fn public_key(&self) -> Ed25519PublicKey {
        self.routing.public_key().await
    }

    pub(crate) async fn update_chunks(&self, chunks: ChunkDataExchange) {
        self.routing.update_chunks(chunks).await
    }

    /// Returns public key of our section public key set.
    pub(crate) async fn section_public_key(&self) -> Result<PublicKey> {
        Ok(PublicKey::Bls(
            self.routing
                .public_key_set()
                .await
                .map_err(|_| Error::NoSectionPublicKey)?
                .public_key(),
        ))
    }

    /// Returns our section's public key.
    pub(crate) async fn our_section_public_key(&self) -> BlsPublicKey {
        self.routing
            .our_section_auth()
            .await
            .public_key_set
            .public_key()
    }

    pub(crate) async fn our_public_key_set(&self) -> Result<PublicKeySet> {
        let pk_set = self.routing.public_key_set().await?;
        Ok(pk_set)
    }

    pub(crate) async fn get_section_pk_by_name(&self, name: &XorName) -> Result<PublicKey> {
        self.routing
            .matching_section(name)
            .await
            .map(|provider| PublicKey::from(provider.section_key()))
            .map_err(From::from)
    }

    pub(crate) async fn our_name(&self) -> XorName {
        self.routing.name().await
    }

    pub(crate) async fn our_connection_info(&self) -> SocketAddr {
        self.routing.our_connection_info().await
    }

    pub(crate) async fn our_prefix(&self) -> Prefix {
        self.routing.our_prefix().await
    }

    pub(crate) async fn section_chain(&self) -> SecuredLinkedList {
        self.routing.section_chain().await
    }

    pub(crate) async fn send_message(&self, wire_msg: WireMsg) -> Result<(), RoutingError> {
        self.routing.send_message(wire_msg).await
    }

    pub(crate) async fn set_joins_allowed(&mut self, joins_allowed: bool) -> Result<()> {
        self.routing.set_joins_allowed(joins_allowed).await?;
        Ok(())
    }

    // Returns whether the node is Elder.
    pub(crate) async fn is_elder(&self) -> bool {
        self.routing.is_elder().await
    }

    pub(crate) async fn our_elder_names(&self) -> BTreeSet<XorName> {
        self.routing
            .our_elders()
            .await
            .iter()
            .map(|p2p_node| *p2p_node.name())
            .collect::<BTreeSet<_>>()
    }

    pub(crate) async fn our_adults(&self) -> BTreeSet<XorName> {
        self.routing
            .our_adults()
            .await
            .into_iter()
            .map(|p2p_node| *p2p_node.name())
            .collect::<BTreeSet<_>>()
    }

    pub(crate) async fn sign_msg_for_dst_accumulation(
        &self,
        node_msg: NodeMsg,
        dst: DstLocation,
    ) -> Result<WireMsg> {
        let wire_msg = self
            .routing
            .sign_msg_for_dst_accumulation(node_msg, dst)
            .await?;
        Ok(wire_msg)
    }

    pub(crate) async fn sign_single_src_msg(
        &self,
        node_msg: NodeMsg,
        dst: DstLocation,
    ) -> Result<WireMsg> {
        let wire_msg = self.routing.sign_single_src_msg(node_msg, dst).await?;
        Ok(wire_msg)
    }
}
