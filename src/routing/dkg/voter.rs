// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    node::{DkgFailureSig, DkgFailureSigSet, DkgKey, ElderCandidates},
    SectionAuthorityProvider,
};
use crate::routing::{
    dkg::session::{Backlog, Session},
    ed25519,
    error::Result,
    node::Node,
    routing_api::command::Command,
    section::{ElderCandidatesUtils, SectionKeyShare},
    supermajority, SectionAuthorityProviderUtils,
};
use bls::PublicKey as BlsPublicKey;
use bls_dkg::key_gen::{message::Message as DkgMessage, KeyGen};
use std::collections::{BTreeSet, HashMap};
use xor_name::XorName;

/// DKG voter carries out the work of participating and/or observing a DKG.
///
/// # Usage
///
/// 1. First the current elders propose the new elder candidates in the form of
///    `SectionAuthorityProvider`structure.
/// 2. They send an accumulating message `DkgStart` containing this proposed
///    `SectionAuthorityProvider` to the new elders candidates (DKG participants).
/// 3. When the `DkgStart` message accumulates, the participants call `start`.
/// 4. The participants keep exchanging the DKG messages and calling `process_message`.
/// 5. On DKG completion, the participants send `DkgResult` vote to the current elders (observers)
/// 6. When the observers accumulate the votes, they can proceed with voting for the section update.
///
/// Note: in case of heavy churn, it can happen that more than one DKG session completes
/// successfully. Some kind of disambiguation strategy needs to be employed in that case, but that
/// is currently not a responsibility of this module.
pub(crate) struct DkgVoter {
    sessions: HashMap<DkgKey, Session>,

    // Due to the asyncronous nature of the network we might sometimes receive a DKG message before
    // we created the corresponding session. To avoid losing those messages, we store them in this
    // backlog and replay them once we create the session.
    backlog: Backlog,
}

impl Default for DkgVoter {
    fn default() -> Self {
        Self {
            sessions: HashMap::default(),
            backlog: Backlog::new(),
        }
    }
}

impl DkgVoter {
    // Starts a new DKG session.
    pub(crate) fn start(
        &mut self,
        node: &Node,
        dkg_key: DkgKey,
        elder_candidates: ElderCandidates,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        if self.sessions.contains_key(&dkg_key) {
            trace!("DKG for {:?} already in progress", elder_candidates);
            return Ok(vec![]);
        }

        let name = ed25519::name(&node.keypair.public);
        let participant_index = if let Some(index) = elder_candidates.position(&name) {
            index
        } else {
            error!(
                "DKG for {:?} failed to start: {} is not a participant",
                elder_candidates, name
            );
            return Ok(vec![]);
        };

        // Special case: only one participant.
        if elder_candidates.elders.len() == 1 {
            let secret_key_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
            let section_auth = SectionAuthorityProvider::from_elder_candidates(
                elder_candidates,
                secret_key_set.public_keys(),
            );
            return Ok(vec![Command::HandleDkgOutcome {
                section_auth,
                outcome: SectionKeyShare {
                    public_key_set: secret_key_set.public_keys(),
                    index: participant_index,
                    secret_key_share: secret_key_set.secret_key_share(0),
                },
            }]);
        }

        let threshold = supermajority(elder_candidates.elders.len()) - 1;
        let participants = elder_candidates.elders.keys().copied().collect();

        match KeyGen::initialize(name, threshold, participants) {
            Ok((key_gen, message)) => {
                trace!("DKG for {:?} starting", elder_candidates);

                let mut session = Session {
                    key_gen,
                    elder_candidates,
                    participant_index,
                    timer_token: 0,
                    failures: DkgFailureSigSet::default(),
                    complete: false,
                };

                let mut commands = vec![];
                commands.extend(session.broadcast(node, &dkg_key, message, section_pk)?);

                for message in self.backlog.take(&dkg_key).into_iter() {
                    commands.extend(session.process_message(node, &dkg_key, message, section_pk)?);
                }

                let _ = self.sessions.insert(dkg_key, session);

                // Remove uneeded old sessions.
                self.sessions.retain(|existing_dkg_key, _| {
                    existing_dkg_key.generation >= dkg_key.generation
                });
                self.backlog.prune(&dkg_key);

                Ok(commands)
            }
            Err(error) => {
                // TODO: return a separate error here.
                error!("DKG for {:?} failed to start: {}", elder_candidates, error);
                Ok(vec![])
            }
        }
    }

    // Make key generator progress with timed phase.
    pub(crate) fn handle_timeout(
        &mut self,
        node: &Node,
        timer_token: u64,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        if let Some((dkg_key, session)) = self
            .sessions
            .iter_mut()
            .find(|(_, session)| session.timer_token() == timer_token)
        {
            session.handle_timeout(node, dkg_key, section_pk)
        } else {
            Ok(vec![])
        }
    }

    // Handle a received DkgMessage.
    pub(crate) fn process_message(
        &mut self,
        node: &Node,
        dkg_key: &DkgKey,
        message: DkgMessage,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        if let Some(session) = self.sessions.get_mut(dkg_key) {
            session.process_message(node, dkg_key, message, section_pk)
        } else {
            self.backlog.push(*dkg_key, message);
            Ok(vec![])
        }
    }

    pub(crate) fn process_failure(
        &mut self,
        dkg_key: &DkgKey,
        failed_participants: &BTreeSet<XorName>,
        signed: DkgFailureSig,
    ) -> Option<Command> {
        self.sessions
            .get_mut(dkg_key)?
            .process_failure(dkg_key, failed_participants, signed)
    }
}
