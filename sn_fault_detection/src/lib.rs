// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Node fault detection for the SAFE Network.
//! All fault detection should route through this for proper weighting in
//! relation to other possible sources of faults.

// For quick_error
#![recursion_limit = "256"]
#![doc(
    html_logo_url = "https://github.com/maidsafe/QA/raw/master/Images/maidsafe_logo.png",
    html_favicon_url = "https://maidsafe.net/img/favicon.ico",
    test(attr(deny(warnings)))
)]
// Forbid some very bad patterns. Forbid is stronger than `deny`, preventing us from suppressing the
// lint with `#[allow(...)]` et-all.
#![forbid(
    arithmetic_overflow,
    mutable_transmutes,
    no_mangle_const_items,
    unknown_crate_types,
    unsafe_code
)]
// Turn on some additional warnings to encourage good style.
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    clippy::unicode_not_nfc,
    clippy::unwrap_used
)]

#[macro_use]
extern crate tracing;

mod detection;

pub use detection::IssueType;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};
use xor_name::XorName;

/// Some reproducible xorname derived from the operation. This is a permanent reference needed for logging all faults.
type NodeIdentifier = XorName;

pub(crate) type TimedTracker = BTreeMap<NodeIdentifier, VecDeque<Instant>>;

#[derive(Clone, Debug)]
/// Faulty nodes tracking. Allows various potential issues to be tracked and weighted,
/// with unresposive or suspect nodes being noted on request, against which action can then be taken.
pub struct FaultDetection {
    /// The communication issues logged against a node, along with a timestamp.
    pub communication_issues: TimedTracker,
    /// The dkg issues logged against a node, along with a timestamp to expire after some time.
    pub dkg_issues: TimedTracker,
    /// The probe issues logged against a node and as yet unfulfilled, along with a timestamp to expire after some time.
    pub probe_issues: TimedTracker,
    /// The knowledge issues logged against a node, along with a timestamp.
    pub knowledge_issues: TimedTracker,
    /// The unfulfilled pending request operation issues logged against a node, along with an
    /// operation ID.
    pub unfulfilled_ops: TimedTracker,
    nodes: Vec<XorName>,
}

impl FaultDetection {
    /// Set up a new tracker.
    pub fn new(nodes: Vec<NodeIdentifier>) -> Self {
        Self {
            communication_issues: BTreeMap::new(),
            dkg_issues: BTreeMap::new(),
            probe_issues: BTreeMap::new(),
            knowledge_issues: BTreeMap::new(),
            unfulfilled_ops: BTreeMap::new(),
            nodes,
        }
    }

    /// Adds an issue to the fault tracker.
    ///
    /// The `op_id` only applies when adding an operational issue.
    pub fn track_issue(&mut self, node_id: NodeIdentifier, issue_type: IssueType) {
        debug!("Adding a new issue to {node_id:?} the fault tracker: {issue_type:?}");
        match issue_type {
            IssueType::Dkg => {
                let queue = self.dkg_issues.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
            IssueType::AeProbeMsg => {
                let queue = self.probe_issues.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
            IssueType::Communication => {
                let queue = self.communication_issues.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
            IssueType::Knowledge => {
                let queue = self.knowledge_issues.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
            IssueType::RequestOperation => {
                let queue = self.unfulfilled_ops.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
        }
    }

    /// Removes a DKG session from the node liveness records.
    pub fn dkg_ack_fulfilled(&mut self, node_id: &NodeIdentifier) {
        trace!("Attempting to remove logged dkg session for {:?}", node_id,);

        if let Some(v) = self.dkg_issues.get_mut(node_id) {
            // only remove the first instance from the vec
            let prev = v.pop_front();

            if prev.is_some() {
                trace!("Pending dkg session removed for node: {:?}", node_id,);
            } else {
                trace!("No Pending dkg session found for node: {:?}", node_id);
            }
        }
    }

    /// Removes a Knowledge issue from the node liveness records.
    pub fn knowledge_updated(&mut self, node_id: &NodeIdentifier) {
        trace!(
            "Attempting to remove logged knowledge issue for {:?}",
            node_id,
        );

        if let Some(v) = self.knowledge_issues.get_mut(node_id) {
            // only remove the first instance from the vec
            let prev = v.pop_front();

            if prev.is_some() {
                trace!("Pending knowledge issue removed for node: {:?}", node_id,);
            } else {
                trace!("No Pending knowledge issue found for node: {:?}", node_id);
            }
        }
    }

    /// Removes a probe tracker from the node liveness records.
    pub fn ae_update_msg_received(&mut self, node_id: &NodeIdentifier) {
        trace!(
            "Attempting to remove pending AEProbe response for {:?}",
            node_id,
        );

        if let Some(v) = self.probe_issues.get_mut(node_id) {
            // only remove the first instance from the vec
            let prev = v.pop_front();

            if prev.is_some() {
                trace!("Pending probe msg removed for node: {:?}", node_id,);
            } else {
                trace!("No Pending probe session found for node: {:?}", node_id);
            }
        }
    }

    /// List all current tracked nodes
    pub fn current_nodes(&self) -> &Vec<XorName> {
        &self.nodes
    }

    /// Add a new node to the tracker and recompute closest nodes.
    pub fn add_new_node(&mut self, node: XorName) {
        info!("Adding new node:{node} to FaultDetection tracker");
        self.nodes.push(node);
    }

    /// Removes tracked nodes not present in `current_members`.
    ///
    /// Tracked issues related to nodes that were removed will also be removed.
    pub fn retain_members_only(&mut self, current_members: BTreeSet<XorName>) {
        let nodes = self.current_nodes();
        let nodes_being_removed = nodes
            .iter()
            .filter(|x| !current_members.contains(x))
            .copied()
            .collect::<Vec<XorName>>();

        self.nodes.retain(|x| current_members.contains(x));

        for node in &nodes_being_removed {
            let _ = self.communication_issues.remove(node);
            let _ = self.knowledge_issues.remove(node);
            let _ = self.dkg_issues.remove(node);
            let _ = self.probe_issues.remove(node);
            let _ = self.unfulfilled_ops.remove(node);
        }
    }
}

/// Calculates the avg value in a data set
/// https://rust-lang-nursery.github.io/rust-cookbook/science/mathematics/statistics.html
pub(crate) fn get_mean_of(data: &[f32]) -> Option<f32> {
    let sum: f32 = data.iter().sum();
    let count = data.len();
    if count > 0 {
        Some(sum / count as f32)
    } else {
        None
    }
}

/// Calculates the standard deviation of a data set
fn std_deviation(data: &[f32]) -> Option<f32> {
    match (get_mean_of(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data
                .iter()
                .map(|value| {
                    let diff = data_mean - *value;

                    diff * diff
                })
                .sum::<f32>()
                / count as f32;

            Some(variance.sqrt())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{FaultDetection, IssueType};

    use eyre::Error;
    use std::{collections::BTreeSet, sync::Once};
    use xor_name::{rand::random as random_xorname, XorName};

    type Result<T, E = Error> = std::result::Result<T, E>;

    static INIT: Once = Once::new();

    /// Initialise logger for tests, this is run only once, even if called multiple times.
    pub(crate) fn init_test_logger() {
        INIT.call_once(|| {
            tracing_subscriber::fmt::fmt()
                // NOTE: uncomment this line for pretty printed log output.
                .with_thread_names(true)
                .with_ansi(false)
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_target(false)
                // .event_format(LogFormatter::default())
                .try_init()
                .unwrap_or_else(|_| println!("Error initializing logger"));
        });
    }

    #[tokio::test]
    async fn retain_members_should_remove_other_nodes() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut fault_detection = FaultDetection::new(nodes.clone());
        let nodes_to_retain = nodes[5..10].iter().cloned().collect::<BTreeSet<XorName>>();

        fault_detection.retain_members_only(nodes_to_retain.clone());

        let current_nodes = fault_detection.current_nodes();
        assert_eq!(current_nodes.len(), 5);
        for member in current_nodes {
            assert!(nodes_to_retain.contains(member));
        }

        Ok(())
    }

    #[tokio::test]
    async fn retain_members_should_remove_issues_relating_to_nodes_not_retained() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut fault_detection = FaultDetection::new(nodes.clone());

        // Track some issues for nodes that are going to be removed.
        for node in nodes.iter().take(3) {
            fault_detection.track_issue(*node, IssueType::Communication);
            fault_detection.track_issue(*node, IssueType::Knowledge);
            fault_detection.track_issue(*node, IssueType::RequestOperation);
        }

        // Track some issues for nodes that will be retained.
        fault_detection.track_issue(nodes[5], IssueType::Communication);
        fault_detection.track_issue(nodes[6], IssueType::Knowledge);
        fault_detection.track_issue(nodes[7], IssueType::RequestOperation);

        let nodes_to_retain = nodes[5..10].iter().cloned().collect::<BTreeSet<XorName>>();

        fault_detection.retain_members_only(nodes_to_retain);

        assert_eq!(fault_detection.communication_issues.len(), 1);
        assert_eq!(fault_detection.knowledge_issues.len(), 1);
        assert_eq!(fault_detection.unfulfilled_ops.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn track_issue_should_add_a_comm_issue() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut fault_detection = FaultDetection::new(nodes.clone());

        fault_detection.track_issue(nodes[0], IssueType::Communication);

        assert_eq!(fault_detection.communication_issues.len(), 1);
        assert_eq!(fault_detection.knowledge_issues.len(), 0);
        assert_eq!(fault_detection.unfulfilled_ops.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn track_issue_should_add_a_knowledge_issue() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut fault_detection = FaultDetection::new(nodes.clone());

        fault_detection.track_issue(nodes[0], IssueType::Knowledge);

        assert_eq!(fault_detection.knowledge_issues.len(), 1);
        assert_eq!(fault_detection.communication_issues.len(), 0);
        assert_eq!(fault_detection.unfulfilled_ops.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn track_issue_should_add_a_pending_op_issue() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut fault_detection = FaultDetection::new(nodes.clone());

        fault_detection.track_issue(nodes[0], IssueType::RequestOperation);

        assert_eq!(fault_detection.unfulfilled_ops.len(), 1);
        assert_eq!(fault_detection.knowledge_issues.len(), 0);
        assert_eq!(fault_detection.communication_issues.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn add_new_node_should_track_new_node() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut fault_detection = FaultDetection::new(nodes);

        let new_adult = random_xorname();
        fault_detection.add_new_node(new_adult);

        let current_nodes = fault_detection.current_nodes();

        assert_eq!(current_nodes.len(), 11);
        Ok(())
    }
}
