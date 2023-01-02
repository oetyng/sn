// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{flow_ctrl::cmds::Cmd, messaging::Peers, MyNode, Result, SectionStateVote};
use sn_interface::{
    messaging::{
        system::{NodeMsg, SectionSigShare},
        MsgId,
    },
    types::Peer,
};

impl MyNode {
    /// Send section state proposal to all our elders
    pub(crate) fn propose_section_state(&mut self, proposal: SectionStateVote) -> Result<Vec<Cmd>> {
        let elders = self.network_knowledge.section_auth().elders_vec();
        self.send_section_state_proposal(elders, proposal)
    }

    /// Send section state proposal to `recipients`
    pub(crate) fn send_section_state_proposal(
        &mut self,
        recipients: Vec<Peer>,
        proposal: SectionStateVote,
    ) -> Result<Vec<Cmd>> {
        trace!("Sending section state proposal: {proposal:?} to {recipients:?}");

        // sign the proposal
        let serialized_proposal = bincode::serialize(&proposal).map_err(|err| {
            error!(
                "Failed to serialize section state proposal {:?}: {:?}",
                proposal, err
            );
            err
        })?;
        let sig_share = self
            .sign_with_section_key_share(serialized_proposal)
            .map_err(|err| {
                error!(
                    "Failed to sign section state proposal {:?}: {:?}",
                    proposal, err
                );
                err
            })?;

        // broadcast the proposal to the recipients
        let mut cmds = vec![];
        let (other_peers, myself) = self.split_peers_and_self(recipients);
        let peers = Peers::Multiple(other_peers);
        let msg = NodeMsg::ProposeSectionState {
            proposal: proposal.clone(),
            sig_share: sig_share.clone(),
        };
        cmds.push(MyNode::send_system_msg(msg, peers, self.context()));

        // handle ourselves if we are in the recipients
        if let Some(me) = myself {
            cmds.extend(self.handle_section_state_proposal(
                MsgId::new(),
                proposal,
                sig_share,
                me,
            )?)
        }

        Ok(cmds)
    }

    pub(crate) fn handle_section_state_proposal(
        &mut self,
        msg_id: MsgId,
        proposal: SectionStateVote,
        sig_share: SectionSigShare,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        // proposals from other sections shall be ignored
        let our_prefix = self.network_knowledge.prefix();
        if !our_prefix.matches(&sender.name()) {
            trace!(
                "Ignore section state proposal {msg_id:?} with prefix mismatch from {sender}: {proposal:?}"
            );
            return Ok(vec![]);
        }

        // let's now verify the section key in the msg authority is trusted
        // based on our current knowledge of the network and sections chains
        let sig_share_pk = &sig_share.public_key_set.public_key();
        if !self.network_knowledge.has_chain_key(sig_share_pk) {
            warn!(
                "Ignore section state proposal {msg_id:?} with untrusted sig share from {sender}: {proposal:?}"
            );
            return Ok(vec![]);
        }

        // try aggregate
        let serialized_proposal = bincode::serialize(&proposal).map_err(|err| {
            error!("Failed to serialise section state proposal {msg_id:?} from {sender}: {proposal:?}: {err:?}");
            err
        })?;
        match self
            .section_proposal_aggregator
            .try_aggregate(&serialized_proposal, sig_share)
        {
            Ok(Some(sig)) => Ok(vec![Cmd::HandleSectionDecisionAgreement { proposal, sig }]),
            Ok(None) => {
                trace!("Section state proposal {msg_id:?} acknowledged, waiting for more...");
                Ok(vec![])
            }
            Err(err) => {
                error!(
                    "Failed to aggregate section state proposal {msg_id:?} from {sender}: {err:?}"
                );
                Ok(vec![])
            }
        }
    }
}
