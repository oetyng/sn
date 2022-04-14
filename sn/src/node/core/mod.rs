// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod api;
mod bootstrap;
mod comm;
mod connectivity;
mod data;
mod delivery_group;
mod messaging;
mod monitoring;
mod proposal;
mod relocation;
mod split_barrier;

pub(crate) use bootstrap::{join_network, JoiningAsRelocated};
pub(crate) use comm::{Comm, DeliveryStatus, MsgEvent};
pub(crate) use data::MIN_LEVEL_WHEN_FULL;
pub(crate) use monitoring::Measurements;
pub(crate) use proposal::Proposal;
#[cfg(test)]
pub(crate) use relocation::{check as relocation_check, ChurnId};

use self::{data::DataStorage, split_barrier::SplitBarrier};

use super::{
    api::cmds::Cmd,
    dkg::DkgVoter,
    network_knowledge::{NetworkKnowledge, SectionKeyShare, SectionKeysProvider},
    DeprecatedEvent, Elders, NodeElderChange, NodeInfo,
};

use crate::messaging::{
    data::OperationId,
    signature_aggregator::SignatureAggregator,
    system::{DkgSessionId, NodeEvent, SystemMsg},
    AuthorityProof, DstLocation, SectionAuth,
};
use crate::node::error::{Error, Result};
use crate::types::{
    log_markers::LogMarker, utils::compare_and_write_prefix_map_to_disk, Cache, Peer,
};
use crate::{elder_count, UsedSpace};

use backoff::ExponentialBackoff;
use dashmap::DashSet;
use data::Capacity;
use itertools::Itertools;
use resource_proof::ResourceProof;
use sn_dysfunction::{DysfunctionDetection, DysfunctionSeverity};
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

// How long to hold on to correlated `Peer`s for data queries. Since data queries are forwarded
// from elders (with whom the client is connected) to adults (who hold the data), the elder handling
// the query cannot reply immediately. For now, they stash a reference to the client `Peer` in
// `Core::pending_data_queries`, which is a cache with duration-based expiry.
// TODO: The value chosen here is shorter than the default client timeout (see
// `crate::client::SN_CLIENT_QUERY_TIMEOUT`), but the timeout is configurable. Ideally this would be
// based on liveness properties (e.g. the timeout should be dynamic based on the responsiveness of
// the section).
const DATA_QUERY_TIMEOUT: Duration = Duration::from_secs(15);

/// How long to keep a cache of a given suspect node. Use to check if it's a newly suspicopus node
/// and relevant flows should be triggered. (So a throttle on supect flows pehaps)
const SUSPECT_NODE_RETENTION_DURATION: Duration = Duration::from_secs(60 * 25 /* 25 mins */);

// This prevents pending query limit unbound growth
pub(crate) const DATA_QUERY_LIMIT: usize = 100;
// per query we can have this many peers, so the total peers waiting can be QUERY_LIMIT * MAX_WAITING_PEERS_PER_QUERY
pub(crate) const MAX_WAITING_PEERS_PER_QUERY: usize = 100;

// Store up to 100 in use backoffs
pub(crate) type AeBackoffCache =
    Arc<RwLock<LRUCache<(Peer, ExponentialBackoff), BACKOFF_CACHE_LIMIT>>>;

#[derive(Clone)]
pub(crate) struct DkgSessionInfo {
    pub(crate) prefix: Prefix,
    pub(crate) elders: BTreeMap<XorName, SocketAddr>,
    pub(crate) authority: AuthorityProof<SectionAuth>,
}

// Core state + logic of a node.
pub(crate) struct Node {
    pub(super) deprecated_event_tx: mpsc::Sender<DeprecatedEvent>,
    pub(crate) info: Arc<RwLock<NodeInfo>>,

    pub(crate) comm: Comm,

    pub(super) data_storage: DataStorage, // Adult only before cache

    resource_proof: ResourceProof,
    // Network resources
    pub(crate) section_keys_provider: SectionKeysProvider,
    network_knowledge: NetworkKnowledge,
    // Signature aggregators
    message_aggregator: SignatureAggregator,
    proposal_aggregator: SignatureAggregator,
    // DKG/Split/Churn modules
    split_barrier: Arc<RwLock<SplitBarrier>>,
    dkg_sessions: Arc<RwLock<HashMap<DkgSessionId, DkgSessionInfo>>>,
    dkg_voter: DkgVoter,
    relocate_state: Arc<RwLock<Option<Box<JoiningAsRelocated>>>>,
    // ======================== Elder only ========================
    joins_allowed: Arc<RwLock<bool>>,
    current_joins_semaphore: Arc<Semaphore>,
    // Trackers
    capacity: Capacity,
    dysfunction_tracking: DysfunctionDetection,
    pending_data_queries: Arc<Cache<OperationId, Arc<DashSet<Peer>>>>,
    /// Timed cache of suspect nodes and their score
    known_suspect_nodes: Arc<Cache<XorName, usize>>,
    // Caches
    ae_backoff_cache: AeBackoffCache,
}

impl Node {
    // Creates `Core` for a regular node.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        comm: Comm,
        mut info: NodeInfo,
        network_knowledge: NetworkKnowledge,
        section_key_share: Option<SectionKeyShare>,
        deprecated_event_tx: mpsc::Sender<DeprecatedEvent>,
        used_space: UsedSpace,
        root_storage_dir: PathBuf,
    ) -> Result<Self> {
        let section_keys_provider = SectionKeysProvider::new(section_key_share).await;

        // make sure the Node has the correct local addr as Comm
        info.addr = comm.our_connection_info();

        let data_storage = DataStorage::new(&root_storage_dir, used_space.clone())?;

        info!("Creating DysfunctionDetection checks");
        let node_dysfunction_detector = DysfunctionDetection::new(
            network_knowledge
                .adults()
                .await
                .iter()
                .map(|peer| peer.name())
                .collect::<Vec<XorName>>(),
            elder_count(),
        );
        info!(
            "DysfunctionDetection check: {:?}",
            node_dysfunction_detector
        );

        let node = Self {
            comm,
            info: Arc::new(RwLock::new(info)),
            network_knowledge,
            section_keys_provider,
            dkg_sessions: Arc::new(RwLock::new(HashMap::default())),
            proposal_aggregator: SignatureAggregator::default(),
            split_barrier: Arc::new(RwLock::new(SplitBarrier::new())),
            message_aggregator: SignatureAggregator::default(),
            dkg_voter: DkgVoter::default(),
            relocate_state: Arc::new(RwLock::new(None)),
            deprecated_event_tx,
            joins_allowed: Arc::new(RwLock::new(true)),
            current_joins_semaphore: Arc::new(Semaphore::new(CONCURRENT_JOINS)),
            resource_proof: ResourceProof::new(RESOURCE_PROOF_DATA_SIZE, RESOURCE_PROOF_DIFFICULTY),
            data_storage,
            capacity: Capacity::default(),
            dysfunction_tracking: node_dysfunction_detector,
            pending_data_queries: Arc::new(Cache::with_expiry_duration(DATA_QUERY_TIMEOUT)),
            known_suspect_nodes: Arc::new(Cache::with_expiry_duration(
                SUSPECT_NODE_RETENTION_DURATION,
            )),
            ae_backoff_cache: AeBackoffCache::default(),
        };

        node.write_prefix_map().await;

        Ok(node)
    }

    ////////////////////////////////////////////////////////////////////////////
    // Miscellaneous
    ////////////////////////////////////////////////////////////////////////////

    pub(crate) async fn generate_probe_msg(&self) -> Result<Cmd> {
        // Generate a random address not belonging to our Prefix
        let mut dst = xor_name::rand::random();

        // We don't probe ourselves
        while self.network_knowledge.prefix().await.matches(&dst) {
            dst = xor_name::rand::random();
        }

        let matching_section = self.network_knowledge.section_by_name(&dst)?;

        let message = SystemMsg::AntiEntropyProbe(dst);
        let section_key = matching_section.section_key();
        let dst_name = matching_section.prefix().name();
        let recipients = matching_section.elders_vec();

        info!(
            "ProbeMsg target {:?} w/key {:?}",
            matching_section.prefix(),
            section_key
        );

        self.send_direct_msg_to_nodes(recipients, message, dst_name, section_key)
            .await
    }

    /// returns names that are relatively dysfunctional
    pub(crate) async fn get_dysfunctional_node_names(&self) -> Result<BTreeSet<XorName>> {
        self.dysfunction_tracking
            .get_nodes_beyond_severity(DysfunctionSeverity::Dysfunctional)
            .await
            .map_err(Error::from)
    }

    /// returns names that are relatively dysfunctional
    async fn get_suspicious_node_names(&self) -> Result<BTreeSet<XorName>> {
        self.dysfunction_tracking
            .get_nodes_beyond_severity(DysfunctionSeverity::Suspicious)
            .await
            .map_err(Error::from)
    }

    /// form Cmds about any newly suspicious nodes
    pub(crate) async fn notify_about_newly_suspect_nodes(&self) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        let all_suspect_nodes = self.get_suspicious_node_names().await?;

        let known_suspect_nodes = self.known_suspect_nodes.get_items().await;

        let newly_suspect_nodes: BTreeSet<_> = all_suspect_nodes
            .iter()
            .filter(|node| !known_suspect_nodes.contains_key(node))
            .copied()
            .collect();

        if !newly_suspect_nodes.is_empty() {
            warn!("New nodes have crossed preemptive replication threshold:  {newly_suspect_nodes:?} . Triggering preemptive data replication");

            for sus in &all_suspect_nodes {
                // 0 is set here as we actually don't need or want the score.
                let _prev = self.known_suspect_nodes.set(*sus, 0, None).await;
            }

            let our_adults = self.network_knowledge.adults().await;
            let valid_adults = our_adults
                .iter()
                .filter(|peer| !newly_suspect_nodes.contains(&peer.name()))
                .cloned()
                .collect::<Vec<Peer>>();

            debug!(
                "{:?}: {newly_suspect_nodes:?}",
                LogMarker::SendSuspiciousNodesDetected
            );

            for adult in valid_adults {
                cmds.push(Cmd::SignOutgoingSystemMsg {
                    msg: SystemMsg::NodeEvent(NodeEvent::SuspiciousNodesDetected(
                        newly_suspect_nodes.clone(),
                    )),
                    dst: DstLocation::Node {
                        name: adult.name(),
                        section_pk: *self.section_chain().await.last_key(),
                    },
                });
            }
        }

        Ok(cmds)
    }

    /// Log a communication problem
    pub(crate) async fn log_comm_issue(&self, name: XorName) -> Result<()> {
        self.dysfunction_tracking
            .track_comm_issue(name)
            .await
            .map_err(Error::from)
    }

    pub(super) async fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            is_elder: self.is_elder().await,
            section_key: self.network_knowledge.section_key().await,
            prefix: self.network_knowledge.prefix().await,
            elders: self.network_knowledge().authority_provider().await.names(),
        }
    }

    /// Generate cmds and fire events based upon any node state changes.
    pub(super) async fn update_self_for_new_node_state(
        &self,
        old: StateSnapshot,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        let new = self.state_snapshot().await;

        if new.section_key != old.section_key {
            if new.is_elder {
                let sap = self.network_knowledge.authority_provider().await;
                info!(
                    "Section updated: prefix: ({:b}), key: {:?}, elders: {}",
                    new.prefix,
                    new.section_key,
                    sap.elders().format(", ")
                );

                if !self.section_keys_provider.is_empty().await {
                    cmds.extend(self.promote_and_demote_elders().await?);

                    // Whenever there is an elders change, casting a round of joins_allowed
                    // proposals to sync.
                    cmds.extend(
                        self.propose(Proposal::JoinsAllowed(*self.joins_allowed.read().await))
                            .await?,
                    );
                }

                self.print_network_stats().await;
            }

            if new.is_elder || old.is_elder {
                cmds.extend(self.send_ae_update_to_our_section().await);
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
                info!("{}: {:?}", LogMarker::PromotedToElder, new.prefix);
                NodeElderChange::Promoted
            } else if old.is_elder && !new.is_elder {
                info!("{}", LogMarker::DemotedFromElder);
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

                cmds.extend(self.send_updates_to_sibling_section(&old).await?);
                self.liveness_retain_only(
                    self.network_knowledge
                        .adults()
                        .await
                        .iter()
                        .map(|peer| peer.name())
                        .collect(),
                )
                .await?;

                DeprecatedEvent::SectionSplit {
                    elders,
                    self_status_change,
                }
            } else {
                cmds.extend(
                    self.send_metadata_updates_to_nodes(
                        self.network_knowledge
                            .authority_provider()
                            .await
                            .elders_vec(),
                        &self.network_knowledge.prefix().await,
                        new.section_key,
                    )
                    .await?,
                );

                DeprecatedEvent::EldersChanged {
                    elders,
                    self_status_change,
                }
            };

            cmds.extend(
                self.send_metadata_updates_to_nodes(
                    self.network_knowledge
                        .authority_provider()
                        .await
                        .elders()
                        .filter(|peer| !old.elders.contains(&peer.name()))
                        .cloned()
                        .collect(),
                    &self.network_knowledge.prefix().await,
                    new.section_key,
                )
                .await?,
            );

            self.send_deprecated_event(event).await
        }

        Ok(cmds)
    }

    pub(super) async fn section_key_by_name(&self, name: &XorName) -> bls::PublicKey {
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

    pub(super) async fn print_network_stats(&self) {
        self.network_knowledge
            .prefix_map()
            .network_stats(&self.network_knowledge.authority_provider().await)
            .print();
    }

    pub(super) async fn log_section_stats(&self) {
        let adults = self.network_knowledge.adults().await.len();
        let elders = self
            .network_knowledge
            .authority_provider()
            .await
            .elder_count();
        let prefix = self.network_knowledge.prefix().await;
        debug!("{:?}: {:?} Elders, {:?} Adults.", prefix, elders, adults);
    }

    async fn write_prefix_map(&self) {
        info!("Writing our latest PrefixMap to disk");
        // TODO: Make this serialization human readable

        let prefix_map = self.network_knowledge.prefix_map().clone();

        let _ = tokio::spawn(async move {
            // Compare and write Prefix to `~/.safe/prefix_maps` dir
            if let Err(e) = compare_and_write_prefix_map_to_disk(&prefix_map).await {
                error!("Error writing PrefixMap to `~/.safe` dir: {:?}", e);
            }
        });
    }
}

pub(crate) struct StateSnapshot {
    is_elder: bool,
    section_key: bls::PublicKey,
    prefix: Prefix,
    pub(crate) elders: BTreeSet<XorName>,
}
