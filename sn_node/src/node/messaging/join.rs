// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::comm::Comm;
use crate::node::{flow_ctrl::cmds::Cmd, Node, Result};
use sn_interface::{
    elder_count,
    messaging::system::{
        JoinAsRelocatedRequest, JoinAsRelocatedResponse, JoinRejectionReason, JoinRequest,
        JoinResponse, MembershipState, NodeState, SystemMsg,
    },
    network_knowledge::{SectionAuthUtils, FIRST_SECTION_MAX_AGE, MIN_ADULT_AGE},
    types::{log_markers::LogMarker, Peer},
};

use std::vec;

const FIRST_SECTION_MIN_ELDER_AGE: u8 = 82;

// Message handling
impl Node {
    pub(crate) async fn handle_join_request(
        &mut self,
        peer: Peer,
        join_request: JoinRequest,
        comm: &Comm,
    ) -> Result<Vec<Cmd>> {
        debug!("Received {:?} from {}", join_request, peer);

        // Require resource signed if joining as a new node.
        if let Some(response) = join_request.resource_proof_response {
            if !self.validate_resource_proof_response(&peer.name(), response) {
                debug!("Ignoring JoinRequest from {peer} - invalid resource signed response");
                return Ok(vec![]);
            }

            let node_state = NodeState::joined(peer.name(), peer.addr(), None);
            return self.propose_membership_change(node_state);
        }

        let our_section_key = self.network_knowledge.section_key();
        let section_key_matches = join_request.section_key == our_section_key;

        // Ignore `JoinRequest` if we are not elder, unless the join request
        // is outdated in which case we'll reply with `JoinResponse::Retry`
        // with the up-to-date info.
        if self.is_not_elder() && section_key_matches {
            // Note: We don't bounce this message because the current bounce-resend
            // mechanism wouldn't preserve the original SocketAddr which is needed for
            // properly handling this message.
            // This is OK because in the worst case the join request just timeouts and the
            // joining node sends it again.
            return Ok(vec![]);
        }

        let our_prefix = self.network_knowledge.prefix();
        if !our_prefix.matches(&peer.name()) {
            debug!("Redirecting JoinRequest from {peer} - name doesn't match our prefix {our_prefix:?}.");

            let retry_sap = self.matching_section(&peer.name())?;

            let node_msg =
                SystemMsg::JoinResponse(Box::new(JoinResponse::Redirect(retry_sap.to_msg())));

            trace!("Sending {:?} to {}", node_msg, peer);
            trace!("{}", LogMarker::SendJoinRedirected);
            return Ok(vec![self.send_direct_msg(vec![peer], node_msg)?]);
        }

        if !self.joins_allowed {
            debug!(
                "Rejecting JoinRequest from {} - joins currently not allowed.",
                peer,
            );
            let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Rejected(
                JoinRejectionReason::JoinsDisallowed,
            )));

            trace!("{}", LogMarker::SendJoinsDisallowed);

            trace!("Sending {:?} to {}", node_msg, peer);
            return Ok(vec![self.send_direct_msg(vec![peer], node_msg)?]);
        }

        let (is_age_invalid, expected_age) = self.verify_joining_node_age(&peer);

        trace!(
            "our_prefix {:?} expected_age {:?} is_age_invalid {:?}",
            our_prefix,
            expected_age,
            is_age_invalid
        );

        if !section_key_matches {
            trace!("{}", LogMarker::SendJoinRetryNotCorrectKey);
            trace!(
                "JoinRequest from {} doesn't have our latest section_key {:?}, presented {:?}.",
                peer,
                our_section_key,
                join_request.section_key
            );
        }

        if is_age_invalid {
            trace!("{}", LogMarker::SendJoinRetryAgeIssue);
            trace!(
                "JoinRequest from {} (with age {}) doesn't have the expected age: {}",
                peer,
                peer.age(),
                expected_age,
            );
        }

        if !section_key_matches || is_age_invalid {
            let proof_chain = self.network_knowledge.section_chain();
            let signed_sap = self.network_knowledge.section_signed_authority_provider();

            let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Retry {
                section_auth: signed_sap.value.to_msg(),
                section_signed: signed_sap.sig,
                proof_chain,
                expected_age,
            }));

            trace!("Sending {:?} to {}", node_msg, peer);
            return Ok(vec![self.send_direct_msg(vec![peer], node_msg)?]);
        }

        // Do reachability check only for the initial join request
        let cmd = if comm.is_reachable(&peer.addr()).await.is_err() {
            let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Rejected(
                JoinRejectionReason::NodeNotReachable(peer.addr()),
            )));

            trace!("{}", LogMarker::SendJoinRejected);

            trace!("Sending {:?} to {}", node_msg, peer);
            self.send_direct_msg(vec![peer], node_msg)?
        } else {
            // It's reachable, let's then send the proof challenge
            self.send_resource_proof_challenge(peer)?
        };

        Ok(vec![cmd])
    }

    pub(crate) fn verify_joining_node_age(&self, peer: &Peer) -> (bool, u8) {
        // During the first section, nodes shall use ranged age to avoid too many nodes getting
        // relocated at the same time. After the first section splits, nodes shall only
        // start with an age of MIN_ADULT_AGE
        let current_section_size = self.network_knowledge.section_size();
        let our_prefix = self.network_knowledge.prefix();

        // Prefix will be empty for first section
        if our_prefix.is_empty() {
            let elders = self.network_knowledge.elders();
            // Forces the joining node to be younger than the youngest elder in genesis section
            // avoiding unnecessary churn.

            // Check if `elder_count()` Elders are already present
            if elders.len() == elder_count() {
                // Check if the joining node is younger than the youngest elder and older than
                // MIN_ADULT_AGE in the first section, to avoid unnecessary churn during genesis.
                let expected_age = FIRST_SECTION_MIN_ELDER_AGE - current_section_size as u8 * 2;
                let is_age_invalid = peer.age() <= MIN_ADULT_AGE || peer.age() > expected_age;
                (is_age_invalid, expected_age)
            } else {
                // Since enough elders haven't joined the first section calculate a value
                // within the range [FIRST_SECTION_MIN_ELDER_AGE, FIRST_SECTION_MAX_AGE].
                let expected_age = FIRST_SECTION_MAX_AGE - current_section_size as u8 * 2;
                // TODO: avoid looping by ensure can only update to lower non-FIRST_SECTION_MIN_ELDER_AGE age
                let is_age_invalid = peer.age() != expected_age;
                (is_age_invalid, expected_age)
            }
        } else {
            // Age should be MIN_ADULT_AGE for joining nodes after genesis section.
            let is_age_invalid = peer.age() != MIN_ADULT_AGE;
            (is_age_invalid, MIN_ADULT_AGE)
        }
    }

    pub(crate) async fn handle_join_as_relocated_request(
        &mut self,
        peer: Peer,
        join_request: JoinAsRelocatedRequest,
        comm: &Comm,
    ) -> Result<Vec<Cmd>> {
        debug!("Received JoinAsRelocatedRequest {join_request:?} from {peer}",);

        let our_prefix = self.network_knowledge.prefix();
        if !our_prefix.matches(&peer.name())
            || join_request.section_key != self.network_knowledge.section_key()
        {
            debug!(
                "JoinAsRelocatedRequest from {peer} - name doesn't match our prefix {our_prefix:?}."
            );

            let node_msg =
                SystemMsg::JoinAsRelocatedResponse(Box::new(JoinAsRelocatedResponse::Retry(
                    self.network_knowledge.authority_provider().to_msg(),
                )));

            trace!("{} b", LogMarker::SendJoinAsRelocatedResponse);

            trace!("Sending {node_msg:?} to {peer}");
            return Ok(vec![self.send_direct_msg(vec![peer], node_msg)?]);
        }

        let relocate_details = if let MembershipState::Relocated(ref details) =
            join_request.relocate_proof.value.state
        {
            // Check for signatures and trust of the relocate_proof
            if !join_request.relocate_proof.self_verify() {
                debug!("Ignoring JoinAsRelocatedRequest from {peer} - invalid sig.");
                return Ok(vec![]);
            }
            let known_keys = self.network_knowledge.known_keys();
            if !known_keys.contains(&join_request.relocate_proof.sig.public_key) {
                debug!("Ignoring JoinAsRelocatedRequest from {peer} - untrusted src.");
                return Ok(vec![]);
            }

            details
        } else {
            debug!(
                "Ignoring JoinAsRelocatedRequest from {peer} with invalid relocate proof state: {:?}",
                join_request.relocate_proof.value.state
            );
            return Ok(vec![]);
        };

        if !relocate_details.verify_identity(&peer.name(), &join_request.signature_over_new_name) {
            debug!("Ignoring JoinAsRelocatedRequest from {peer} - invalid node name signature.");
            return Ok(vec![]);
        }

        // Finally do reachability check
        if comm.is_reachable(&peer.addr()).await.is_err() {
            let node_msg = SystemMsg::JoinAsRelocatedResponse(Box::new(
                JoinAsRelocatedResponse::NodeNotReachable(peer.addr()),
            ));
            trace!("{}", LogMarker::SendJoinAsRelocatedResponse);

            trace!("Sending {:?} to {}", node_msg, peer);
            return Ok(vec![self.send_direct_msg(vec![peer], node_msg)?]);
        };

        self.propose_membership_change(join_request.relocate_proof.value)
    }
}
