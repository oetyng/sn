// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod api;
mod back_pressure;
mod bootstrap;
mod capacity;
mod chunk_records;
mod chunk_store;
mod comm;
mod connectivity;
mod delivery_group;
mod liveness_tracking;
mod messaging;
mod msg_count;
mod msg_handling;
mod proposal;
mod register_storage;
mod split_barrier;

pub(crate) use back_pressure::BackPressure;
pub(crate) use bootstrap::{join_network, JoiningAsRelocated};
pub(crate) use capacity::MIN_LEVEL_WHEN_FULL;
pub(crate) use chunk_store::{ChunkStore, Error as ChunkStoreError};
pub(crate) use comm::{Comm, ConnectionEvent, SendStatus};
pub(crate) use proposal::Proposal;
pub(crate) use register_storage::RegisterStorage;

use self::split_barrier::SplitBarrier;

use super::{
    super::error::Result,
    api::command::Command,
    dkg::DkgVoter,
    network_knowledge::{NetworkKnowledge, SectionKeyShare, SectionKeysProvider},
    node::Node,
    relocation::RelocateState,
    Elders, Event, NodeElderChange,
};

use crate::messaging::{
    signature_aggregator::SignatureAggregator,
    system::{DkgSessionId, SystemMsg},
    AuthorityProof, SectionAuth,
};

use crate::peer::Peer;
use crate::types::{log_markers::LogMarker, utils::write_data_to_disk, Cache};

use backoff::ExponentialBackoff;
use capacity::Capacity;
use itertools::Itertools;
use liveness_tracking::Liveness;
use resource_proof::ResourceProof;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, RwLock, Semaphore};
use uluru::LRUCache;
use xor_name::{Prefix, XorName};

pub(super) const RESOURCE_PROOF_DATA_SIZE: usize = 128;
pub(super) const RESOURCE_PROOF_DIFFICULTY: u8 = 10;

const BACKOFF_CACHE_LIMIT: usize = 100;
pub(crate) const CONCURRENT_JOINS: usize = 7;

// How long to hold on to correlated `Peer`s for chunk queries. Since chunk queries are forwarded
// from elders (with whom the client is connected) to adults (who hold the data), the elder handling
// the query cannot reply immediately. For now, they stash a reference to the client `Peer` in
// `Core::pending_chunk_queries`, which is a cache with duration-based expiry.
// TODO: The value chosen here is longer than the default client timeout (see
// `crate::client::SN_CLIENT_QUERY_TIMEOUT`), but the timeout is configurable. Ideally this would be
// based on liveness properties (e.g. the timeout should be dynamic based on the responsiveness of
// the section).
const CHUNK_QUERY_TIMEOUT: Duration = Duration::from_secs(60 * 5 /* 5 mins */);

// store up to 100 in use backoffs
pub(crate) type AeBackoffCache =
    Arc<RwLock<LRUCache<(Peer, ExponentialBackoff), BACKOFF_CACHE_LIMIT>>>;

#[derive(Clone)]
pub(crate) struct DkgSessionInfo {
    pub(crate) prefix: Prefix,
    pub(crate) elders: BTreeMap<XorName, SocketAddr>,
    pub(crate) authority: AuthorityProof<SectionAuth>,
}

// State + logic of a routing node.
pub(crate) struct Core {
    pub(crate) comm: Comm,
    pub(crate) node: Arc<RwLock<Node>>,
    network_knowledge: NetworkKnowledge,
    pub(crate) section_keys_provider: SectionKeysProvider,
    message_aggregator: SignatureAggregator,
    proposal_aggregator: SignatureAggregator,
    split_barrier: Arc<RwLock<SplitBarrier>>,
    dkg_sessions: Arc<RwLock<HashMap<DkgSessionId, DkgSessionInfo>>>,
    // Voter for Dkg
    dkg_voter: DkgVoter,
    relocate_state: Arc<RwLock<Option<RelocateState>>>,
    pub(super) event_tx: mpsc::Sender<Event>,
    joins_allowed: Arc<RwLock<bool>>,
    current_joins_semaphore: Arc<Semaphore>,
    is_genesis_node: bool,
    resource_proof: ResourceProof,
    pub(super) register_storage: RegisterStorage,
    pub(super) chunk_storage: ChunkStore,
    root_storage_dir: PathBuf,
    capacity: Capacity,
    liveness: Liveness,
    pending_chunk_queries: Arc<Cache<XorName, Peer>>,
    ae_backoff_cache: AeBackoffCache,
}

impl Core {
    // Creates `Core` for a regular node.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        comm: Comm,
        mut node: Node,
        network_knowledge: NetworkKnowledge,
        section_key_share: Option<SectionKeyShare>,
        event_tx: mpsc::Sender<Event>,
        reg_store_size: u64,
        chunk_store_size: u64,
        root_storage_dir: PathBuf,
        is_genesis_node: bool,
    ) -> Result<Self> {
        let section_keys_provider = SectionKeysProvider::new(section_key_share).await;

        // make sure the Node has the correct local addr as Comm
        node.addr = comm.our_connection_info();

        let register_storage = RegisterStorage::new(&root_storage_dir, reg_store_size)?;
        let chunk_storage = ChunkStore::new(&root_storage_dir, chunk_store_size)?;

        let capacity = Capacity::new(BTreeMap::new());
        let adult_liveness = Liveness::new();

        Ok(Self {
            comm,
            node: Arc::new(RwLock::new(node)),
            network_knowledge,
            section_keys_provider,
            dkg_sessions: Arc::new(RwLock::new(HashMap::default())),
            proposal_aggregator: SignatureAggregator::default(),
            split_barrier: Arc::new(RwLock::new(SplitBarrier::new())),
            message_aggregator: SignatureAggregator::default(),
            dkg_voter: DkgVoter::default(),
            relocate_state: Arc::new(RwLock::new(None)),
            event_tx,
            joins_allowed: Arc::new(RwLock::new(true)),
            current_joins_semaphore: Arc::new(Semaphore::new(CONCURRENT_JOINS)),
            is_genesis_node,
            resource_proof: ResourceProof::new(RESOURCE_PROOF_DATA_SIZE, RESOURCE_PROOF_DIFFICULTY),
            register_storage,
            chunk_storage,
            capacity,
            liveness: adult_liveness,
            root_storage_dir,
            pending_chunk_queries: Arc::new(Cache::with_expiry_duration(CHUNK_QUERY_TIMEOUT)),
            ae_backoff_cache: AeBackoffCache::default(),
        })
    }

    ////////////////////////////////////////////////////////////////////////////
    // Miscellaneous
    ////////////////////////////////////////////////////////////////////////////

    pub(crate) async fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            is_elder: self.is_elder().await,
            section_key: self.network_knowledge.section_key().await,
            prefix: self.network_knowledge.prefix().await,
            elders: self.network_knowledge().authority_provider().await.names(),
        }
    }

    pub(crate) async fn generate_probe_message(&self) -> Result<Command> {
        // Generate a random address not belonging to our Prefix
        let mut dst = XorName::random();

        // We don't probe ourselves
        while self.network_knowledge.prefix().await.matches(&dst) {
            dst = XorName::random();
        }

        let matching_section = self.network_knowledge.section_by_name(&dst)?;

        let message = SystemMsg::AntiEntropyProbe(dst);
        let section_key = matching_section.section_key();
        let dst_name = matching_section.prefix().name();
        let recipients = matching_section.elders_vec();

        info!(
            "ProbeMessage target {:?} w/key {:?}",
            matching_section.prefix(),
            section_key
        );

        self.send_direct_message_to_nodes_in_section(recipients, message, dst_name, section_key)
            .await
    }

    pub(crate) async fn write_prefix_map(&self) {
        info!("Writing our latest PrefixMap to disk");
        // TODO: Make this serialization human readable

        let prefix_map = self.network_knowledge.prefix_map().clone();
        let storage_dir = self.root_storage_dir.join("prefix_map");
        let is_genesis_node = self.is_genesis_node;

        let _ = tokio::spawn(async move {
            // Write to the node's root dir
            if let Err(e) = write_data_to_disk(&prefix_map, &storage_dir).await {
                error!("Error writing PrefixMap to root dir: {:?}", e);
            }

            // If we are genesis Node, write to `~/.safe` dir
            if is_genesis_node {
                if let Some(mut safe_dir) = dirs_next::home_dir() {
                    safe_dir.push(".safe");
                    safe_dir.push("prefix_map");
                    if let Err(e) = write_data_to_disk(&prefix_map, &safe_dir).await {
                        error!("Error writing PrefixMap to `~/.safe` dir: {:?}", e);
                    }
                } else {
                    error!("Could not write PrefixMap in SAFE dir: Home directory not found");
                }
            }
        });
    }

    /// Generate commands and fire events based upon any node state changes.
    pub(crate) async fn update_self_for_new_node_state_and_fire_events(
        &self,
        old: StateSnapshot,
    ) -> Result<Vec<Command>> {
        let mut commands = vec![];
        let new = self.state_snapshot().await;

        if new.section_key != old.section_key {
            if new.is_elder {
                info!(
                    "Section updated: prefix: ({:b}), key: {:?}, elders: {}",
                    new.prefix,
                    new.section_key,
                    self.network_knowledge
                        .authority_provider()
                        .await
                        .elders()
                        .format(", ")
                );

                if !self.section_keys_provider.is_empty().await {
                    commands.extend(self.promote_and_demote_elders().await?);

                    // Whenever there is an elders change, casting a round of joins_allowed
                    // proposals to sync.
                    commands.extend(
                        self.propose(Proposal::JoinsAllowed(*self.joins_allowed.read().await))
                            .await?,
                    );
                }

                self.print_network_stats().await;
            }

            if new.is_elder || old.is_elder {
                commands.extend(self.send_ae_update_to_our_section().await);
            }

            let current: BTreeSet<_> = self.network_knowledge.authority_provider().await.names();
            let added = current.difference(&old.elders).copied().collect();
            let removed = old.elders.difference(&current).copied().collect();
            let remaining = old.elders.intersection(&current).copied().collect();

            let elders = Elders {
                prefix: new.prefix,
                key: new.section_key,
                remaining,
                added,
                removed,
            };

            let self_status_change = if !old.is_elder && new.is_elder {
                trace!("{}: {:?}", LogMarker::PromotedToElder, new.prefix);
                NodeElderChange::Promoted
            } else if old.is_elder && !new.is_elder {
                trace!("{}", LogMarker::DemotedFromElder);
                info!("Demoted");
                self.section_keys_provider.wipe().await;
                NodeElderChange::Demoted
            } else {
                NodeElderChange::None
            };

            // During the split, sibling's SAP could be unknown to us yet.
            // Hence, fire the SectionSplit event whenever detect a prefix change.
            // We also need to update other nodes w/ our known data.
            let event = if (new.prefix != old.prefix) && new.is_elder {
                info!("{}: {:?}", LogMarker::SplitSuccess, new.prefix);

                if old.is_elder {
                    info!("{}: {:?}", LogMarker::StillElderAfterSplit, new.prefix);
                }

                commands.extend(self.send_updates_to_sibling_section(&old).await?);
                self.retain_members_only(
                    self.network_knowledge
                        .adults()
                        .await
                        .iter()
                        .map(|peer| peer.name())
                        .collect(),
                )
                .await?;

                Event::SectionSplit {
                    elders,
                    self_status_change,
                }
            } else {
                commands.extend(
                    self.send_data_updates_to(
                        self.network_knowledge.prefix().await,
                        self.network_knowledge
                            .authority_provider()
                            .await
                            .elders_vec(),
                        old.section_key,
                    )
                    .await?,
                );

                Event::EldersChanged {
                    elders,
                    self_status_change,
                }
            };

            commands.extend(
                self.send_data_updates_to(
                    self.network_knowledge.prefix().await,
                    self.network_knowledge
                        .authority_provider()
                        .await
                        .elders()
                        .filter(|peer| !old.elders.contains(&peer.name()))
                        .cloned()
                        .collect(),
                    old.section_key,
                )
                .await?,
            );

            self.send_event(event).await
        }

        if !new.is_elder {
            commands.extend(self.return_relocate_promise().await);
        }

        Ok(commands)
    }

    pub(crate) async fn section_key_by_name(&self, name: &XorName) -> bls::PublicKey {
        if self.network_knowledge.prefix().await.matches(name) {
            self.network_knowledge.section_key().await
        } else if let Ok(sap) = self.network_knowledge.section_by_name(name) {
            sap.section_key()
        } else if self
            .network_knowledge
            .prefix()
            .await
            .sibling()
            .matches(name)
        {
            // For sibling with unknown key, use the previous key in our chain under the assumption
            // that it's the last key before the split and therefore the last key of theirs we know.
            // In case this assumption is not correct (because we already progressed more than one
            // key since the split) then this key would be unknown to them and they would send
            // us back their whole section chain. However, this situation should be rare.
            *self.network_knowledge.section_chain().await.prev_key()
        } else {
            *self.network_knowledge.genesis_key()
        }
    }

    pub(crate) async fn print_network_stats(&self) {
        self.network_knowledge
            .prefix_map()
            .network_stats(&self.network_knowledge.authority_provider().await)
            .print();
        self.comm.print_stats();
    }

    pub(super) async fn log_network_stats(&self) {
        let adults = self.network_knowledge.adults().await.len();
        let elders = self
            .network_knowledge
            .authority_provider()
            .await
            .elder_count();
        let prefix = self.network_knowledge.prefix().await;
        debug!("{:?}: {:?} Elders, {:?} Adults.", prefix, elders, adults);
    }
}

pub(crate) struct StateSnapshot {
    is_elder: bool,
    section_key: bls::PublicKey,
    prefix: Prefix,
    elders: BTreeSet<XorName>,
}
