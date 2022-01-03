// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    system::{KeyedSig, SectionAuth, SystemMsg},
    MessageId, MessageType, SrcLocation, WireMsg,
};
use crate::node::{
    error::{Error, Result},
    routing::{
        api::command::Command,
        core::Core,
        messages::WireMsgUtils,
        network_knowledge::{SectionAuthorityProvider, SectionPeers},
    },
};
use crate::peer::Peer;
use crate::types::{log_markers::LogMarker, PublicKey};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use itertools::Itertools;
use secured_linked_list::SecuredLinkedList;
use std::time::Duration;
use xor_name::XorName;

impl Core {
    #[instrument(skip_all)]
    pub(crate) async fn handle_anti_entropy_update_msg(
        &self,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        proof_chain: SecuredLinkedList,
        members: Option<SectionPeers>,
    ) -> Result<Vec<Command>> {
        let snapshot = self.state_snapshot().await;

        let our_name = self.node.read().await.name();
        let signed_sap = SectionAuth {
            value: section_auth.clone(),
            sig: section_signed.clone(),
        };

        let _updated = self
            .network_knowledge
            .update_knowledge_if_valid(
                signed_sap.clone(),
                &proof_chain,
                members,
                &our_name,
                &self.section_keys_provider,
            )
            .await?;

        let mut commands = vec![];

        commands.extend(self.fire_node_event_for_any_new_adults().await?);

        // always run this, only changes will trigger events
        commands.extend(
            self.update_self_for_new_node_state_and_fire_events(snapshot)
                .await?,
        );

        Ok(commands)
    }

    pub(crate) async fn handle_anti_entropy_retry_msg(
        &self,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        proof_chain: SecuredLinkedList,
        bounced_msg: Bytes,
        sender: Peer,
    ) -> Result<Vec<Command>> {
        let dst_section_key = section_auth.section_key();
        let snapshot = self.state_snapshot().await;

        let to_resend = self
            .update_network_knowledge(
                section_auth,
                section_signed,
                proof_chain,
                bounced_msg,
                sender.clone(),
            )
            .await?;

        match to_resend {
            None => Ok(vec![]),
            Some((msg_to_resend, _)) => {
                // TODO: we may need to check if 'bounced_msg' dest section pk is different
                // from the received new SAP key, to prevent from endlessly resending a msg
                // if a sybil/corrupt peer keeps sending us the same AE msg.
                trace!(
                    "{} resending {:?}",
                    LogMarker::AeResendAfterRetry,
                    msg_to_resend
                );

                self.create_or_wait_for_backoff(&sender).await;

                let mut result = Vec::new();
                if let Ok(cmds) = self
                    .update_self_for_new_node_state_and_fire_events(snapshot)
                    .await
                {
                    result.extend(cmds);
                }

                result.push(
                    self.send_direct_message(sender, msg_to_resend, dst_section_key)
                        .await?,
                );

                Ok(result)
            }
        }
    }

    pub(crate) async fn handle_anti_entropy_redirect_msg(
        &self,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        section_chain: SecuredLinkedList,
        bounced_msg: Bytes,
        sender: Peer,
    ) -> Result<Vec<Command>> {
        let dst_section_key = section_auth.section_key();

        // We choose the Elder closest to the dest section key,
        // just to pick one of them in an arbitrary but deterministic fashion.
        let target_name = XorName::from(PublicKey::Bls(dst_section_key));
        let chosen_dst_elder = section_auth
            .elders()
            .sorted_by(|lhs, rhs| target_name.cmp_distance(&lhs.name(), &rhs.name()))
            .peekable()
            .peek()
            .map(|elder| (*elder).clone());

        let to_resend = self
            .update_network_knowledge(
                section_auth,
                section_signed,
                section_chain,
                bounced_msg,
                sender.clone(),
            )
            .await?;

        match to_resend {
            None => Ok(vec![]),
            Some((msg_to_redirect, msg_id)) => match chosen_dst_elder {
                None => {
                    error!(
                            "Failed to find closest Elder to resend msg ({:?}) upon AE-Redirect response.",
                            msg_id
                        );
                    Ok(vec![])
                }
                Some(elder) if elder.addr() == sender.addr() => {
                    error!(
                            "Failed to find an alternative Elder to resend msg ({:?}) upon AE-Redirect response.",
                            msg_id
                        );
                    Ok(vec![])
                }
                Some(elder) => {
                    trace!("{}", LogMarker::AeResendAfterAeRedirect);

                    self.create_or_wait_for_backoff(&elder).await;

                    let cmd = self
                        .send_direct_message(elder, msg_to_redirect, dst_section_key)
                        .await?;

                    Ok(vec![cmd])
                }
            },
        }
    }

    // Try to update network knowledge and return the 'SystemMsg' that needs to be resent.
    async fn update_network_knowledge(
        &self,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        proof_chain: SecuredLinkedList,
        bounced_msg: Bytes,
        sender: Peer,
    ) -> Result<Option<(SystemMsg, MessageId)>> {
        let (bounced_msg, msg_id, dst_location) = match WireMsg::deserialize(bounced_msg)? {
            MessageType::System {
                msg,
                msg_id,
                dst_location,
                ..
            } => (msg, msg_id, dst_location),
            _ => {
                warn!("Non System MessageType received in AE response. We do not handle any other type in AE msgs yet.");
                return Ok(None);
            }
        };

        info!("Anti-Entropy: message received from peer: {}", sender);

        let prefix = section_auth.prefix();
        let dst_section_key = section_auth.section_key();
        let signed_sap = SectionAuth {
            value: section_auth.clone(),
            sig: section_signed.clone(),
        };
        let our_name = self.node.read().await.name();

        // Update our network knowledge.
        if self
            .network_knowledge
            .update_knowledge_if_valid(
                signed_sap.clone(),
                &proof_chain,
                None,
                &our_name,
                &self.section_keys_provider,
            )
            .await?
        {
            self.write_prefix_map().await;
            info!(
                "PrefixMap written to disk with update for prefix {:?}",
                prefix
            );
        }

        // If the new SAP's section key is the same as the section key set when the
        // bounced message was originally sent, we just drop it.
        if dst_location.section_pk() == Some(dst_section_key) {
            error!("Dropping bounced msg ({:?}) received in AE-Retry from {} as suggested new dst section key is the same as previously sent: {:?}", msg_id, sender,dst_section_key);
            Ok(None)
        } else {
            Ok(Some((bounced_msg, msg_id)))
        }
    }

    /// Checks AE-BackoffCache for backoff, or creates a new instance
    /// waits for any required backoff duration
    async fn create_or_wait_for_backoff(&self, peer: &Peer) {
        let mut ae_backoff_guard = self.ae_backoff_cache.write().await;
        let our_backoff = ae_backoff_guard
            .find(|(node, _)| node == peer)
            .map(|(_, backoff)| backoff);

        if let Some(backoff) = our_backoff {
            let next_backoff = backoff.next_backoff();
            let sleep_time = if let Some(mut next_wait) = next_backoff {
                // The default setup start with around 400ms
                // then increases to around 50s after 25 calls.
                next_wait /= 100;
                if next_wait > Duration::from_secs(1) {
                    backoff.reset();
                }
                Some(next_wait)
            } else {
                // TODO: we've done all backoffs and are _still_ getting messages?
                // we should probably penalise the node here.
                None
            };

            drop(ae_backoff_guard);

            if let Some(sleep_time) = sleep_time {
                tokio::time::sleep(sleep_time).await;
            }
        } else {
            let _res = ae_backoff_guard.insert((peer.clone(), ExponentialBackoff::default()));
        }
    }

    // If entropy is found, determine the msg to send in order to
    // bring the sender's knowledge about us up to date.
    pub(crate) async fn check_for_entropy(
        &self,
        original_bytes: Bytes,
        src_location: &SrcLocation,
        dst_section_key: &BlsPublicKey,
        dst_name: XorName,
        sender: &Peer,
    ) -> Result<Option<Command>> {
        // Check if the message has reached the correct section,
        // if not, we'll need to respond with AE
        trace!("Checking for entropy");

        let our_prefix = self.network_knowledge.prefix().await;

        // Let's try to find a section closer to the destination, if it's not for us.
        if !self.network_knowledge.prefix().await.matches(&dst_name) {
            debug!(
                "AE: prefix not matching. We are: {:?}, they sent to: {:?}",
                our_prefix, dst_name
            );
            match self
                .network_knowledge
                .get_closest_or_opposite_signed_sap(&dst_name)
                .await
            {
                Some((signed_sap, section_chain)) => {
                    info!("Found a better matching prefix {:?}", signed_sap.prefix());
                    let bounced_msg = original_bytes;
                    // Redirect to the closest section
                    let ae_msg = SystemMsg::AntiEntropyRedirect {
                        section_auth: signed_sap.value.to_msg(),
                        section_signed: signed_sap.sig,
                        section_chain,
                        bounced_msg,
                    };
                    let wire_msg = WireMsg::single_src(
                        &self.node.read().await.clone(),
                        src_location.to_dst(),
                        ae_msg,
                        self.network_knowledge.section_key().await,
                    )?;
                    trace!("{}", LogMarker::AeSendRedirect);

                    return Ok(Some(Command::SendMessage {
                        recipients: vec![sender.clone()],
                        wire_msg,
                    }));
                }
                None => {
                    warn!("Our PrefixMap is empty");
                    // TODO: instead of just dropping the message, don't we actually need
                    // to get up to date info from other Elders in our section as it may be
                    // a section key we are not aware of yet?
                    // ...and once we acquired new key/s we attempt AE check again?
                    warn!(
                        "Anti-Entropy: cannot reply with redirect msg for dst_name {:?} and key {:?} to a closest section.",
                        dst_name, dst_section_key
                    );

                    return Err(Error::NoMatchingSection);
                }
            }
        }

        trace!(
            "Performing AE checks, provided pk was: {:?} ours is: {:?}",
            dst_section_key,
            self.network_knowledge.section_key().await
        );

        if dst_section_key == &self.network_knowledge.section_key().await {
            trace!("Provided Section PK matching our latest. All AE checks passed!");
            // Destination section key matches our current section key
            return Ok(None);
        }

        let ae_msg = match self
            .network_knowledge
            .get_proof_chain_to_current(dst_section_key)
            .await
        {
            Ok(proof_chain) => {
                info!("Anti-Entropy: sender's ({}) knowledge of our SAP is outdated, bounce msg for AE-Retry with up to date SAP info.", sender);

                let signed_sap = self
                    .network_knowledge
                    .section_signed_authority_provider()
                    .await;

                trace!(
                    "Sending AE-Retry with: proofchain last key: {:?} and  section key: {:?}",
                    proof_chain.last_key(),
                    &signed_sap.value.section_key()
                );
                trace!("{}", LogMarker::AeSendRetryAsOutdated);

                SystemMsg::AntiEntropyRetry {
                    section_auth: signed_sap.value.to_msg(),
                    section_signed: signed_sap.sig,
                    proof_chain,
                    bounced_msg: original_bytes,
                }
            }
            Err(_) => {
                trace!(
                    "Anti-Entropy: cannot find dst_section_key {:?} sent by {} in our chain",
                    dst_section_key,
                    sender
                );

                let proof_chain = self.network_knowledge.section_chain().await;

                let signed_sap = self
                    .network_knowledge
                    .section_signed_authority_provider()
                    .await;

                trace!("{}", LogMarker::AeSendRetryDstPkFail);

                SystemMsg::AntiEntropyRetry {
                    section_auth: signed_sap.value.to_msg(),
                    section_signed: signed_sap.sig,
                    proof_chain,
                    bounced_msg: original_bytes,
                }
            }
        };

        let wire_msg = WireMsg::single_src(
            &self.node.read().await.clone(),
            src_location.to_dst(),
            ae_msg,
            self.network_knowledge.section_key().await,
        )?;

        Ok(Some(Command::SendMessage {
            recipients: vec![sender.clone()],
            wire_msg,
        }))
    }

    // Generate an AE redirect command for the given message
    pub(crate) async fn ae_redirect_to_our_elders(
        &self,
        sender: Peer,
        src_location: &SrcLocation,
        original_wire_msg: &WireMsg,
    ) -> Result<Command> {
        let signed_sap = self
            .network_knowledge
            .section_signed_authority_provider()
            .await;

        let ae_msg = SystemMsg::AntiEntropyRedirect {
            section_auth: signed_sap.value.to_msg(),
            section_signed: signed_sap.sig,
            section_chain: self.network_knowledge.section_chain().await,
            bounced_msg: original_wire_msg.serialize()?,
        };

        let wire_msg = WireMsg::single_src(
            &self.node.read().await.clone(),
            src_location.to_dst(),
            ae_msg,
            self.network_knowledge.section_key().await,
        )?;

        trace!("{} in ae_redirect", LogMarker::AeSendRedirect);

        Ok(Command::SendMessage {
            recipients: vec![sender],
            wire_msg,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elder_count;
    use crate::messaging::{DstLocation, MessageId, MessageType, MsgKind, NodeAuth};
    use crate::node::routing::{
        api::tests::create_comm,
        create_test_max_capacity_and_root_storage,
        dkg::test_utils::section_signed,
        ed25519,
        network_knowledge::{
            test_utils::{gen_addr, gen_section_authority_provider},
            SectionKeysProvider,
        },
        node::Node,
        SectionKeyShare, XorName, MIN_ADULT_AGE,
    };

    use assert_matches::assert_matches;
    use bls::SecretKey;
    use eyre::{Context, Result};
    use rand::Rng;
    use secured_linked_list::SecuredLinkedList;
    use tokio::sync::mpsc;
    use xor_name::Prefix;

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_everything_up_to_date() -> Result<()> {
        let mut rng = rand::thread_rng();
        let env = Env::new().await?;
        let our_prefix = env.core.network_knowledge().prefix().await;
        let (msg, src_location) = env.create_message(
            &our_prefix,
            env.core.network_knowledge().section_key().await,
        )?;
        let sender = env.core.node.read().await.peer();
        let dst_name = our_prefix.substituted_in(rng.gen());
        let dst_section_key = env.core.network_knowledge().section_key().await;

        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                &dst_section_key,
                dst_name,
                &sender,
            )
            .await?;

        assert!(command.is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_redirect_to_other_section() -> Result<()> {
        let env = Env::new().await?;

        let other_sk = bls::SecretKey::random();
        let other_pk = other_sk.public_key();

        let (msg, src_location) = env.create_message(&env.other_sap.prefix(), other_pk)?;
        let sender = env.core.node.read().await.peer();

        // since it's not aware of the other prefix, it shall redirect us to genesis section/SAP
        let dst_section_key = other_pk;
        let dst_name = env.other_sap.prefix().name();
        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                &dst_section_key,
                dst_name,
                &sender,
            )
            .await;

        let msg_type = assert_matches!(command, Ok(Some(Command::SendMessage { wire_msg, .. })) => {
            wire_msg
                .into_message()
                .context("failed to deserialised anti-entropy message")?
        });

        assert_matches!(msg_type, MessageType::System{ msg, .. } => {
            assert_matches!(msg, SystemMsg::AntiEntropyRedirect { section_auth, .. } => {
                assert_eq!(section_auth, env.genesis_sap.to_msg());
            });
        });

        // now let's insert the other SAP to make it aware of the other prefix
        assert!(
            env.core
                .network_knowledge()
                .update_knowledge_if_valid(
                    env.other_sap.clone(),
                    &env.proof_chain,
                    None,
                    &env.core.node.read().await.name(),
                    &env.core.section_keys_provider
                )
                .await?
        );

        // and it now shall give us an AE redirect msg
        // with the SAP we inserted for other prefix
        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                &dst_section_key,
                dst_name,
                &sender,
            )
            .await;

        let msg_type = assert_matches!(command, Ok(Some(Command::SendMessage { wire_msg, .. })) => {
            wire_msg
                .into_message()
                .context("failed to deserialised anti-entropy message")?
        });

        assert_matches!(msg_type, MessageType::System{ msg, .. } => {
            assert_matches!(msg, SystemMsg::AntiEntropyRedirect { section_auth, .. } => {
                assert_eq!(section_auth, env.other_sap.value.to_msg());
            });
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_outdated_dst_key_of_our_section() -> Result<()> {
        let mut rng = rand::thread_rng();
        let env = Env::new().await?;
        let our_prefix = env.core.network_knowledge().prefix().await;

        let (msg, src_location) = env.create_message(
            &our_prefix,
            env.core.network_knowledge().section_key().await,
        )?;
        let sender = env.core.node.read().await.peer();
        let dst_name = our_prefix.substituted_in(rng.gen());
        let dst_section_key = env.core.network_knowledge().genesis_key();

        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                dst_section_key,
                dst_name,
                &sender,
            )
            .await?;

        let msg_type = assert_matches!(command, Some(Command::SendMessage { wire_msg, .. }) => {
            wire_msg
                .into_message()
                .context("failed to deserialised anti-entropy message")?
        });

        assert_matches!(msg_type, MessageType::System{ msg, .. } => {
            assert_matches!(msg, SystemMsg::AntiEntropyRetry { ref section_auth, ref proof_chain, .. } => {
                assert_eq!(section_auth, &env.core.network_knowledge().authority_provider().await.to_msg());
                assert_eq!(proof_chain, &env.core.section_chain().await);
            });
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_wrong_dst_key_of_our_section_returns_retry() -> Result<()> {
        let mut rng = rand::thread_rng();
        let env = Env::new().await?;
        let our_prefix = env.core.network_knowledge().prefix().await;

        let (msg, src_location) = env.create_message(
            &our_prefix,
            env.core.network_knowledge().section_key().await,
        )?;
        let sender = env.core.node.read().await.peer();
        let dst_name = our_prefix.substituted_in(rng.gen());

        let bogus_env = Env::new().await?;
        let dst_section_key = bogus_env.core.network_knowledge().genesis_key();

        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                dst_section_key,
                dst_name,
                &sender,
            )
            .await?;

        let msg_type = assert_matches!(command, Some(Command::SendMessage { wire_msg, .. }) => {
            wire_msg
                .into_message()
                .context("failed to deserialised anti-entropy message")?
        });

        assert_matches!(msg_type, MessageType::System{ msg, .. } => {
            assert_matches!(msg, SystemMsg::AntiEntropyRetry { ref section_auth, ref proof_chain, .. } => {
                assert_eq!(*section_auth, env.core.network_knowledge().authority_provider().await.to_msg());
                assert_eq!(*proof_chain, env.core.section_chain().await);
            });
        });

        Ok(())
    }

    struct Env {
        core: Core,
        other_sap: SectionAuth<SectionAuthorityProvider>,
        proof_chain: SecuredLinkedList,
        genesis_sap: SectionAuthorityProvider,
    }

    impl Env {
        async fn new() -> Result<Self> {
            let prefix0 = Prefix::default().pushed(false);
            let prefix1 = Prefix::default().pushed(true);

            // generate a SAP for prefix0
            let (section_auth, mut nodes, secret_key_set) =
                gen_section_authority_provider(prefix0, elder_count());
            let node = nodes.remove(0);
            let sap_sk = secret_key_set.secret_key();
            let signed_sap = section_signed(sap_sk, section_auth)?;

            let (chain, genesis_sk_set) = create_chain(sap_sk, signed_sap.section_key())
                .context("failed to create section chain")?;
            let genesis_pk = genesis_sk_set.public_keys().public_key();
            assert_eq!(genesis_pk, *chain.root_key());

            let (reg_store_size, chunk_store_size, root_storage_dir) =
                create_test_max_capacity_and_root_storage()?;
            let mut core = Core::first_node(
                create_comm().await?,
                node.clone(),
                mpsc::channel(1).0,
                reg_store_size,
                chunk_store_size,
                root_storage_dir,
                genesis_sk_set.clone(),
            )
            .await?;

            let genesis_sap = core.network_knowledge().authority_provider().await;
            let section_key_share = SectionKeyShare {
                public_key_set: secret_key_set.public_keys(),
                index: 0,
                secret_key_share: secret_key_set.secret_key_share(0),
            };

            core.section_keys_provider = SectionKeysProvider::new(Some(section_key_share)).await;

            // get our Core to now be in prefix(0)
            let _ = core
                .network_knowledge()
                .update_knowledge_if_valid(
                    signed_sap.clone(),
                    &chain,
                    None,
                    &node.name(),
                    &core.section_keys_provider,
                )
                .await;

            // generate other SAP for prefix1
            let (other_sap, _, secret_key_set) =
                gen_section_authority_provider(prefix1, elder_count());
            let other_sap_sk = secret_key_set.secret_key();
            let other_sap = section_signed(other_sap_sk, other_sap)?;
            // generate a proof chain for this other SAP
            let mut proof_chain = SecuredLinkedList::new(genesis_pk);
            let signature = bincode::serialize(&other_sap_sk.public_key())
                .map(|bytes| genesis_sk_set.secret_key().sign(&bytes))?;
            proof_chain.insert(&genesis_pk, other_sap_sk.public_key(), signature)?;

            Ok(Self {
                core,
                other_sap,
                proof_chain,
                genesis_sap,
            })
        }

        fn create_message(
            &self,
            src_section_prefix: &Prefix,
            src_section_pk: BlsPublicKey,
        ) -> Result<(WireMsg, SrcLocation)> {
            let sender = Node::new(
                ed25519::gen_keypair(&src_section_prefix.range_inclusive(), MIN_ADULT_AGE),
                gen_addr(),
            );

            let sender_name = sender.name();
            let src_node_keypair = sender.keypair;

            let payload_msg = SystemMsg::StartConnectivityTest(XorName::random());
            let payload = WireMsg::serialize_msg_payload(&payload_msg)?;

            let dst_name = XorName::random();
            let dst_section_key = SecretKey::random().public_key();
            let dst_location = DstLocation::Node {
                name: dst_name,
                section_pk: dst_section_key,
            };

            let msg_id = MessageId::new();

            let node_auth = NodeAuth::authorize(src_section_pk, &src_node_keypair, &payload);

            let msg_kind = MsgKind::NodeAuthMsg(node_auth.into_inner());

            let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst_location)?;

            let src_location = SrcLocation::Node {
                name: sender_name,
                section_pk: src_section_pk,
            };

            Ok((wire_msg, src_location))
        }
    }

    // Creates a section chain with three blocks
    fn create_chain(
        sap_sk: &SecretKey,
        last_key: BlsPublicKey,
    ) -> Result<(SecuredLinkedList, bls::SecretKeySet)> {
        // create chain with random genesis key
        let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
        let genesis_pk = genesis_sk_set.public_keys().public_key();
        let mut chain = SecuredLinkedList::new(genesis_pk);

        // insert second key which is the PK derived from SAP's SK
        let sap_pk = sap_sk.public_key();
        let sig = genesis_sk_set
            .secret_key()
            .sign(&bincode::serialize(&sap_pk)?);
        chain.insert(&genesis_pk, sap_pk, sig)?;

        // insert third key which is provided `last_key`
        let last_sig = sap_sk.sign(&bincode::serialize(&last_key)?);
        chain.insert(&sap_pk, last_key, last_sig)?;

        Ok((chain, genesis_sk_set))
    }
}
