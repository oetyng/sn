// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::NodeContext,
    flow_ctrl::cmds::Cmd,
    handover::{Error as HandoverError, Handover},
    membership::{elder_candidates, try_split_dkg},
    messaging::Peers,
    Error, MyNode, NodeMsg, Peer, Result,
};
use sn_consensus::{Generation, SignedVote, VoteResponse};
use sn_interface::{
    messaging::{system::SectionSigned, MsgId, SectionSigShare},
    network_knowledge::{NodeState, SapCandidate, SectionAuthorityProvider},
    types::log_markers::LogMarker,
};
use std::collections::{BTreeMap, BTreeSet};
use tracing::warn;
use xor_name::{Prefix, XorName};

impl MyNode {
    /// Handle a Handover consensus trigger request by a DKG member
    pub(crate) fn handle_handover_request(
        &mut self,
        msg_id: MsgId,
        sap: SectionAuthorityProvider,
        sig_share: SectionSigShare,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        trace!("Handling post DKG handover request {msg_id:?} from {sender:?}: {sap:?}");

        // check sender
        if !sap.contains_elder(&sender.name()) {
            trace!("Ignoring invalid handover request {msg_id:?}: not DKG participant");
            return Ok(vec![]);
        }

        // try aggregate
        let serialised_sap = bincode::serialize(&sap).map_err(|err| {
            error!("Failed to serialise handover request {msg_id:?} from {sender}: {err:?}");
            err
        })?;
        match self
            .handover_request_aggregator
            .try_aggregate(&serialised_sap, sig_share)
        {
            Ok(Some(sig)) => {
                trace!("Handover request {msg_id:?} successfully aggregated");
                self.handle_request_handover_agreement(sap, sig)
            }
            Ok(None) => {
                trace!("Handover request {msg_id:?} acknowledged, waiting for more...");
                Ok(vec![])
            }
            Err(err) => {
                error!("Failed to aggregate handover request {msg_id:?} from {sender}: {err:?}");
                Ok(vec![])
            }
        }
    }

    /// Make a handover consensus proposal vote for a sap candidate
    pub(crate) fn propose_handover_consensus(
        &mut self,
        sap_candidates: SapCandidate,
    ) -> Result<Vec<Cmd>> {
        let context = &self.context();
        let mut cmds = vec![];
        match &self.handover_voting {
            Some(handover_voting_state) => {
                let mut vs = handover_voting_state.clone();
                let vote = vs.propose(sap_candidates.clone())?;
                self.handover_voting = Some(vs.clone());

                debug!("{}: {:?}", LogMarker::HandoverConsensusTrigger, &vote);
                cmds.push(MyNode::broadcast_handover_vote_msg(context, vote));

                // For handover 2 elders sap, only the handover vote from genesis is required.
                // Which make the vote state reached consensus when initialized.
                if vs.consensus_value().is_some() {
                    debug!(
                        "{}: {:?}",
                        LogMarker::HandoverConsensusTermination,
                        sap_candidates
                    );
                    match self.broadcast_handover_completed(sap_candidates) {
                        Ok(c) => cmds.extend(c),
                        Err(err) => error!("Error broadcasting handover complete: {err:?}"),
                    }
                }
            }
            None => {
                warn!("Failed to make handover consensus proposal because node is not an Elder");
            }
        }
        Ok(cmds)
    }

    /// Broadcast handover Vote message to Elders
    pub(crate) fn broadcast_handover_vote_msg(
        context: &NodeContext,
        signed_vote: SignedVote<SapCandidate>,
    ) -> Cmd {
        // Deliver each SignedVote to all current Elders
        trace!("Broadcasting Vote msg: {:?}", signed_vote);

        MyNode::send_msg_to_our_elders(context, NodeMsg::HandoverVotes(vec![signed_vote]))
    }

    /// Broadcast the decision of the terminated handover consensus by proposing the HandoverCompleted SAP(s)
    /// for signature by the current elders
    #[instrument(skip(self), level = "trace")]
    pub(crate) fn broadcast_handover_completed(
        &mut self,
        candidate: SapCandidate,
    ) -> Result<Vec<Cmd>> {
        let proposal_recipients = candidate.elders();
        let (others, myself) = self.split_peers_and_self(proposal_recipients);
        let peers = Peers::Multiple(others);

        // sends a promotion message to all of the to-be-Elders with our sig_share over their new pub key
        // it is aggregated by them to obtain a section signed section pub key (proof of inheritance)
        let mut cmds = vec![];
        match candidate {
            SapCandidate::ElderHandover(sap) => {
                let serialized_sap = bincode::serialize(&sap.sig.public_key)?;
                let sig_share = self.sign_with_section_key_share(serialized_sap)?;
                let msg = NodeMsg::SectionHandoverPromotion {
                    sap: sap.clone(),
                    sig_share: sig_share.clone(),
                };
                cmds.push(MyNode::send_system_msg(msg, peers, self.context()));

                // handle our own if we are elder
                if let Some(elder) = myself {
                    match self.handle_handover_promotion(MsgId::new(), sap, sig_share, elder) {
                        Ok(c) => cmds.extend(c),
                        Err(e) => error!("Failed to handle our own handover promotion: {e:?}"),
                    };
                }
            }
            SapCandidate::SectionSplit(sap0, sap1) => {
                let serialized_sap0_pk = bincode::serialize(&sap0.sig.public_key)?;
                let sig_share0 = self.sign_with_section_key_share(serialized_sap0_pk)?;
                let serialized_sap1_pk = bincode::serialize(&sap1.sig.public_key)?;
                let sig_share1 = self.sign_with_section_key_share(serialized_sap1_pk)?;
                let msg = NodeMsg::SectionSplitPromotion {
                    sap0: sap0.clone(),
                    sig_share0: sig_share0.clone(),
                    sap1: sap1.clone(),
                    sig_share1: sig_share1.clone(),
                };
                cmds.push(MyNode::send_system_msg(msg, peers, self.context()));

                // handle our own if we are elder
                if let Some(elder) = myself {
                    match self.handle_section_split_promotion(
                        MsgId::new(),
                        sap0,
                        sig_share0,
                        sap1,
                        sig_share1,
                        elder,
                    ) {
                        Ok(c) => cmds.extend(c),
                        Err(e) => error!("Failed to handle our own split promotion: {e:?}"),
                    };
                }
            }
        };
        Ok(cmds)
    }

    /// helper to handle a handover vote
    #[instrument(skip(context), level = "trace")]
    fn handle_vote(
        context: &NodeContext,
        handover_state: &mut Handover,
        signed_vote: SignedVote<SapCandidate>,
        peer: Peer,
    ) -> Result<Vec<Cmd>> {
        match handover_state.handle_signed_vote(signed_vote.clone()) {
            Ok(VoteResponse::Broadcast(signed_vote)) => {
                trace!(
                    ">>> Handover Vote msg successfully handled, broadcasting our vote: {:?}",
                    signed_vote
                );
                Ok(vec![MyNode::broadcast_handover_vote_msg(
                    context,
                    signed_vote,
                )])
            }
            Ok(VoteResponse::WaitingForMoreVotes) => {
                trace!(
                    ">>> Handover Vote msg successfully handled, awaiting for more votes: {:?}",
                    signed_vote
                );
                Ok(vec![])
            }
            Err(HandoverError::RequestAntiEntropy) => {
                trace!("Handover - We are behind the voter, requesting AE");
                Err(Error::RequestHandoverAntiEntropy(
                    handover_state.generation(),
                ))
            }
            Err(err) => {
                error!(">>> Failed to handle handover Vote msg: {:?}", err);
                Ok(vec![])
            }
        }
    }

    /// Verifies the SAP signature and checks that the signature's public key matches the
    /// signature of the SAP, because SAP candidates are signed by the candidate section key
    fn check_sap_sig(&self, sap: &SectionSigned<SectionAuthorityProvider>) -> Result<()> {
        let sap_bytes = bincode::serialize(&sap.value)?;
        if !sap.sig.verify(&sap_bytes) {
            return Err(Error::InvalidSignature);
        }
        if sap.value.section_key() != sap.sig.public_key {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "the auth around the SAP does not match the SAP's public key: {:?} != {:?}",
                sap.sig.public_key,
                sap.value.section_key(),
            )));
        }
        Ok(())
    }

    fn get_members_at_gen(&self, gen: Generation) -> Result<BTreeMap<XorName, NodeState>> {
        if let Some(m) = self.membership.as_ref() {
            Ok(m.section_members(gen)?)
        } else {
            error!("Missing membership instance when checking handover SAP candidates");
            Err(Error::MissingMembershipInstance)
        }
    }

    fn get_sap_for_prefix(&self, prefix: Prefix) -> Result<SectionAuthorityProvider> {
        self.network_knowledge
            .section_tree()
            .get(&prefix)
            .ok_or(Error::FailedToGetSAPforPrefix(prefix))
    }

    fn check_elder_handover_candidates(&self, sap: &SectionAuthorityProvider) -> Result<()> {
        // in regular handover the previous SAP's prefix is the same
        let previous_gen_sap = self.get_sap_for_prefix(sap.prefix())?;
        let members = self.get_members_at_gen(sap.membership_gen())?;
        let received_candidates: BTreeSet<&Peer> = sap.elders().collect();

        let expected_peers: BTreeSet<Peer> =
            elder_candidates(members.values().cloned(), &previous_gen_sap)
                .iter()
                .map(|node| *node.peer())
                .collect();
        let expected_candidates: BTreeSet<&Peer> = expected_peers.iter().collect();
        if received_candidates != expected_candidates {
            debug!("InvalidElderCandidates: received SAP at gen {} with candidates {:#?}, expected candidates {:#?}", sap.membership_gen(), received_candidates, expected_candidates);
            return Err(Error::InvalidElderCandidates);
        }
        Ok(())
    }

    fn check_section_split_candidates(
        &self,
        sap1: &SectionAuthorityProvider,
        sap2: &SectionAuthorityProvider,
    ) -> Result<()> {
        // in split handover, the previous SAP's prefix is prefix.popped()
        // we use gen/prefix from sap1, both SAPs in a split have the same generation
        // and the same ancestor prefix
        let prev_prefix = sap1.prefix().popped();
        let previous_gen_sap = self.get_sap_for_prefix(prev_prefix)?;
        let members = self.get_members_at_gen(sap1.membership_gen())?;
        let dummy_chain_len = 0;
        let dummy_gen = 0;

        let received_candidates1: BTreeSet<&Peer> = sap1.elders().collect();
        let received_candidates2: BTreeSet<&Peer> = sap2.elders().collect();

        if let Some((dkg1, dkg2)) =
            try_split_dkg(&members, &previous_gen_sap, dummy_chain_len, dummy_gen)
        {
            let expected_peers1: BTreeSet<Peer> =
                dkg1.elders.iter().map(|(n, a)| Peer::new(*n, *a)).collect();
            let expected_peers2: BTreeSet<Peer> =
                dkg2.elders.iter().map(|(n, a)| Peer::new(*n, *a)).collect();
            let expected_candidates1: BTreeSet<&Peer> = expected_peers1.iter().collect();
            let expected_candidates2: BTreeSet<&Peer> = expected_peers2.iter().collect();

            // the order of these SAPs is not absolute, so we try both comparisons
            if (received_candidates1 != expected_candidates1
                || received_candidates2 != expected_candidates2)
                && (received_candidates2 != expected_candidates1
                    || received_candidates1 != expected_candidates2)
            {
                debug!("InvalidElderCandidates: received SAP1 at gen {} with candidates {:#?}, expected candidates {:#?}", sap1.membership_gen(), received_candidates1, expected_candidates1);
                debug!("InvalidElderCandidates: received SAP2 at gen {} with candidates {:#?}, expected candidates {:#?}", sap2.membership_gen(), received_candidates2, expected_candidates2);
                return Err(Error::InvalidElderCandidates);
            }
            Ok(())
        } else {
            Err(Error::InvalidSplitCandidates)
        }
    }

    fn check_sap_candidate_prefix(&self, sap_candidate: &SapCandidate) -> Result<()> {
        let section_prefix = self.network_knowledge.prefix();
        match sap_candidate {
            SapCandidate::ElderHandover(single_sap) => {
                // single handover, must be same prefix
                if single_sap.prefix() == section_prefix {
                    Ok(())
                } else {
                    Err(Error::InvalidSectionPrefixForCandidate)
                }
            }
            SapCandidate::SectionSplit(sap1, sap2) => {
                // section split, must be 2 distinct children prefixes
                let our_p = &section_prefix;
                let p1 = sap1.prefix();
                let p2 = sap2.prefix();
                if p1.is_extension_of(our_p)
                    && p2.is_extension_of(our_p)
                    && p1.bit_count() == our_p.bit_count() + 1
                    && p2.bit_count() == our_p.bit_count() + 1
                    && p1 != p2
                {
                    Ok(())
                } else {
                    Err(Error::InvalidSectionPrefixForSplitCandidates)
                }
            }
        }
    }

    /// Checks if the elder candidates in the SAP match the oldest elders in the corresponding
    /// membership generation this SAP was proposed at
    /// Also checks the SAP signature
    fn check_sap_candidate(&self, sap_candidate: &SapCandidate) -> Result<()> {
        self.check_sap_candidate_prefix(sap_candidate)?;
        match sap_candidate {
            SapCandidate::ElderHandover(authed_sap) => {
                self.check_sap_sig(authed_sap)?;
                self.check_elder_handover_candidates(&authed_sap.value)
            }
            SapCandidate::SectionSplit(authed_sap1, authed_sap2) => {
                self.check_sap_sig(authed_sap1)?;
                self.check_sap_sig(authed_sap2)?;
                self.check_section_split_candidates(&authed_sap1.value, &authed_sap2.value)
            }
        }
    }

    fn check_signed_vote_saps(&self, signed_vote: &SignedVote<SapCandidate>) -> Result<()> {
        let sap_candidates = signed_vote.proposals();
        for sap_can in sap_candidates {
            let _ = self.check_sap_candidate(&sap_can);
        }
        Ok(())
    }

    fn handle_handover_vote(
        &mut self,
        peer: Peer,
        signed_vote: SignedVote<SapCandidate>,
    ) -> Result<Vec<Cmd>> {
        self.check_signed_vote_saps(&signed_vote)?;
        let context = &self.context();
        match &self.handover_voting {
            Some(handover_state) => {
                let had_consensus_value = handover_state.consensus_value().is_some();
                let mut state = handover_state.clone();
                let mut cmds = MyNode::handle_vote(context, &mut state, signed_vote, peer)?;

                // check for unsuccessful termination
                state.handle_empty_set_decision();

                // check for successful termination
                if let Some(candidates_sap) = state.consensus_value() {
                    // The Termination & Decision Broadcasting shall only undertaken
                    // on the first time the consensus reached.
                    if !had_consensus_value {
                        debug!(
                            "{}: {:?}",
                            LogMarker::HandoverConsensusTermination,
                            candidates_sap
                        );

                        match self.broadcast_handover_completed(candidates_sap) {
                            Ok(c) => cmds.extend(c),
                            Err(err) => error!("Failed to broadcast handover complete: {err:?}"),
                        }
                    }
                }
                self.handover_voting = Some(state);
                Ok(cmds)
            }
            None => {
                trace!("Non-elder node unexpectedly received handover Vote msg, ignoring...");
                Ok(vec![])
            }
        }
    }

    pub(crate) fn handle_handover_msg(
        &mut self,
        peer: Peer,
        signed_votes: Vec<SignedVote<SapCandidate>>,
    ) -> Result<Vec<Cmd>> {
        debug!("{}", LogMarker::HandoverMsgBeingHandled);

        let mut cmds = vec![];

        for vote in signed_votes {
            match self.handle_handover_vote(peer, vote) {
                Ok(vec) => {
                    cmds.extend(vec);
                }
                Err(Error::RequestHandoverAntiEntropy(gen)) => {
                    // We hit an error while processing this vote, perhaps we are missing information.
                    // We'll send a handover AE request to see if they can help us catch up.
                    debug!("{:?}", LogMarker::HandoverSendingAeUpdateRequest);
                    cmds.push(MyNode::send_system_msg(
                        NodeMsg::HandoverAE(gen),
                        Peers::Single(peer),
                        self.context(),
                    ));
                    // return the vec w/ the AE cmd there so as not to loop and generate AE for
                    // any subsequent commands
                    return Ok(cmds);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(cmds)
    }

    pub(crate) fn handle_handover_anti_entropy(&self, peer: Peer, gen: Generation) -> Option<Cmd> {
        debug!(
            "{:?} handover anti-entropy request for gen {:?} from {}",
            LogMarker::HandoverAeRequestReceived,
            gen,
            peer,
        );

        if let Some(handover) = self.handover_voting.as_ref() {
            match handover.anti_entropy(gen) {
                Ok(catchup_votes) => Some(MyNode::send_system_msg(
                    NodeMsg::HandoverVotes(catchup_votes),
                    Peers::Single(peer),
                    self.context(),
                )),
                Err(e) => {
                    error!("Handover - Error while processing anti-entropy {:?}", e);
                    None
                }
            }
        } else {
            error!("Unexpected attempt to handle handover anti-entropy when we don't have a handover instance (handover is for elders only)");
            None
        }
    }
}
