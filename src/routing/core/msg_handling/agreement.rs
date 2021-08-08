// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::cmp;

use crate::messaging::{
    node::{KeyedSig, MembershipState, NodeMsg, NodeState, Proposal, SectionAuth},
    SectionAuthorityProvider,
};
use crate::routing::{
    dkg::SectionAuthUtils,
    error::Result,
    network::NetworkLogic,
    peer::PeerUtils,
    routing_api::command::Command,
    section::{ElderCandidatesUtils, SectionLogic, SectionPeersLogic},
    Event, SectionAuthorityProviderUtils, MIN_AGE,
};

use super::Core;

// Agreement
impl Core {
    pub(crate) async fn handle_agreement(
        &self,
        proposal: Proposal,
        sig: KeyedSig,
    ) -> Result<Vec<Command>> {
        debug!("handle agreement on {:?}", proposal);
        match proposal {
            Proposal::Online { node_state, .. } => {
                self.handle_online_agreement(node_state, sig).await
            }
            Proposal::Offline(node_state) => self.handle_offline_agreement(node_state, sig).await,
            Proposal::SectionInfo(section_auth) => {
                self.handle_section_info_agreement(section_auth, sig).await
            }
            Proposal::OurElders(section_auth) => {
                self.handle_our_elders_agreement(section_auth, sig).await
            }
            Proposal::JoinsAllowed(joins_allowed) => {
                self.joins_allowed.set(joins_allowed.1).await;
                Ok(vec![])
            }
        }
    }

    async fn handle_online_agreement(
        &self,
        new_info: NodeState,
        sig: KeyedSig,
    ) -> Result<Vec<Command>> {
        let mut commands = vec![];
        if let Some(old_info) = self
            .section
            .members()
            .get_section_signed(new_info.peer.name())
            .await
        {
            // This node is rejoin with same name.

            if old_info.value.state != MembershipState::Left {
                debug!(
                    "Ignoring Online node {} - {:?} not Left.",
                    new_info.peer.name(),
                    old_info.value.state,
                );

                return Ok(commands);
            }

            let new_age = cmp::max(MIN_AGE, old_info.value.peer.age() / 2);

            if new_age > MIN_AGE {
                // TODO: consider handling the relocation inside the bootstrap phase, to avoid
                // having to send this `NodeApproval`.
                commands.push(self.send_node_approval(old_info.as_ref().clone()).await?);
                commands.extend(
                    self.relocate_rejoining_peer(&new_info.peer, new_age)
                        .await?,
                );

                return Ok(commands);
            }
        }

        let new_info = SectionAuth {
            value: new_info,
            sig,
        };

        if !self.section.update_member(new_info.clone()).await {
            info!("ignore Online: {:?}", new_info.value.peer);
            return Ok(vec![]);
        }

        info!("handle Online: {:?}", new_info.value.peer);

        self.send_event(Event::MemberJoined {
            name: *new_info.value.peer.name(),
            previous_name: new_info.value.previous_name,
            age: new_info.value.peer.age(),
        })
        .await;

        commands.extend(
            self.relocate_peers(new_info.value.peer.name(), &new_info.sig.signature)
                .await?,
        );

        let result = self.promote_and_demote_elders().await?;
        if result.is_empty() {
            commands.extend(self.send_sync_to_adults().await?);
        }

        commands.extend(result);
        commands.push(self.send_node_approval(new_info).await?);

        self.print_network_stats().await;

        Ok(commands)
    }

    async fn handle_offline_agreement(
        &self,
        node_state: NodeState,
        sig: KeyedSig,
    ) -> Result<Vec<Command>> {
        let mut commands = vec![];
        let peer = node_state.peer;
        let age = peer.age();
        let signature = sig.signature.clone();

        if !self
            .section
            .update_member(SectionAuth {
                value: node_state,
                sig,
            })
            .await
        {
            info!("ignore Offline: {:?}", peer);
            return Ok(commands);
        }

        info!("handle Offline: {:?}", peer);

        commands.extend(self.relocate_peers(peer.name(), &signature).await?);

        let result = self.promote_and_demote_elders().await?;
        if result.is_empty() {
            commands.extend(self.send_sync_to_adults().await?);
        }

        commands.extend(result);

        self.send_event(Event::MemberLeft {
            name: *peer.name(),
            age,
        })
        .await;

        Ok(commands)
    }

    async fn handle_section_info_agreement(
        &self,
        section_auth: SectionAuthorityProvider,
        sig: KeyedSig,
    ) -> Result<Vec<Command>> {
        let mut commands = vec![];
        let equal_or_extension = section_auth.prefix() == self.section.prefix().await
            || section_auth
                .prefix()
                .is_extension_of(&self.section.prefix().await);
        let signed_section_auth = SectionAuth::new(section_auth, sig.clone());

        if equal_or_extension {
            // Our section of sub-section

            let infos = self
                .section
                .promote_and_demote_elders(&self.node.name())
                .await;
            if !infos.contains(&signed_section_auth.value.elder_candidates()) {
                // SectionInfo out of date, ignore.
                return Ok(commands);
            }

            // Send a `Sync` message to all the to-be-promoted members so they have the full
            // section and network data.
            let sync_recipients: Vec<_> = infos
                .iter()
                .flat_map(|info| info.peers())
                .filter(|peer| self.section.is_elder(peer.name()))
                .map(|peer| (*peer.name(), *peer.addr()))
                .collect();
            if !sync_recipients.is_empty() {
                let node_msg = NodeMsg::Sync {
                    section: self.section.clone().await,
                    network: self.network.get().await.as_ref().clone().await,
                };
                let cmd = self
                    .send_direct_message_to_nodes(sync_recipients, node_msg, sig.public_key)
                    .await?;

                commands.push(cmd);
            }

            // Send the `OurElder` proposal to all of the to-be-elders so it's aggregated by them.
            let our_elders_recipients: Vec<_> =
                infos.iter().flat_map(|info| info.peers()).collect();
            commands.extend(
                self.send_proposal(
                    &our_elders_recipients,
                    Proposal::OurElders(signed_section_auth),
                )
                .await?,
            );
        } else {
            // Other section
            // FIXME: we shouln't be receiving or updating a SAP for
            // a remote section here, that would be done with a AE msg response ???.
            let section_chain = self.section.chain_clone().await;
            let _ = self.network.get().await.update_remote_section_sap(
                signed_section_auth,
                &section_chain,
                &section_chain,
            );
        }

        Ok(commands)
    }

    async fn handle_our_elders_agreement(
        &self,
        signed_section_auth: SectionAuth<SectionAuthorityProvider>,
        key_sig: KeyedSig,
    ) -> Result<Vec<Command>> {
        let updates = self.split_barrier.write().await.process(
            &self.section.prefix().await,
            signed_section_auth,
            key_sig,
        );
        if updates.is_empty() {
            return Ok(vec![]);
        }

        let snapshot = self.state_snapshot().await;

        for (section_auth, key_sig) in updates {
            if section_auth.value.prefix.matches(&self.node.name()) {
                let _ = self.section.update_elders(section_auth, key_sig).await;
            } else {
                // FIXME: we shouln't be receiving or updating a SAP for
                // a remote section here, that should be done with a AE msg response.
                let section_chain = self.section.chain_clone().await;
                let _ = self.network.get().await.update_remote_section_sap(
                    section_auth,
                    &section_chain,
                    &section_chain,
                );
            }
        }

        self.update_state(snapshot).await
    }
}
