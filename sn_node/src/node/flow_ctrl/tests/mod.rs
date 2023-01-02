// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#![allow(dead_code)]
pub(crate) mod cmd_utils;
pub(crate) mod dbc_utils;
pub(crate) mod network_builder;

use crate::{
    comm::MsgFromPeer,
    node::{
        flow_ctrl::{
            dispatcher::Dispatcher,
            tests::network_builder::{TestNetwork, TestNetworkBuilder},
        },
        messaging::Peers,
        relocation_check, ChurnId, Cmd, Error, SectionStateVote,
    },
};
use cmd_utils::{handle_online_cmd, ProcessAndInspectCmds};
use sn_consensus::Decision;
use sn_dbc::Hash;
use sn_interface::{
    dbcs::gen_genesis_dbc,
    elder_count, init_logger,
    messaging::{
        data::{ClientMsg, DataCmd, SpentbookCmd},
        system::{self, AntiEntropyKind, JoinAsRelocatedRequest, NodeDataCmd, NodeMsg},
        Dst, MsgType, WireMsg,
    },
    network_knowledge::{
        recommended_section_size, supermajority, Error as NetworkKnowledgeError, MembershipState,
        MyNodeInfo, NodeState, RelocateDetails, SectionKeysProvider, SectionTreeUpdate,
        SectionsDAG, MIN_ADULT_AGE,
    },
    test_utils::*,
    types::{keys::ed25519, PublicKey, ReplicatedData},
};

use assert_matches::assert_matches;
use eyre::{bail, eyre, Result};
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use std::{
    collections::{BTreeSet, HashSet},
    iter,
    sync::Arc,
};
use tokio::sync::RwLock;
use xor_name::{Prefix, XorName};

#[tokio::test]
async fn membership_churn_starts_on_join_request_from_relocated_node() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("receive_join_request_from_relocated_node").entered();

    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), 0, None, None)
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);
    let sk_set = env.get_secret_key_set(prefix, None);
    let section_key = sk_set.public_keys().public_key();
    let node = dispatcher.node();
    let node_info = node.read().await.info();
    let relocated_node_old_name = node_info.name();
    let relocated_node_old_keypair = node_info.keypair.clone();

    let relocated_node = MyNodeInfo::new(
        ed25519::gen_keypair(&prefix.range_inclusive(), MIN_ADULT_AGE + 1),
        gen_addr(),
    );

    let relocate_details = RelocateDetails {
        previous_name: relocated_node_old_name,
        dst: relocated_node_old_name,
        dst_section_key: section_key,
        age: relocated_node.age(),
    };

    let node_state = NodeState::relocated(
        relocated_node.peer(),
        Some(relocated_node_old_name),
        relocate_details,
    );
    let relocate_proof = TestKeys::get_section_signed(&sk_set.secret_key(), node_state);

    let signature_over_new_name =
        ed25519::sign(&relocated_node.name().0, &relocated_node_old_keypair);

    let wire_msg = WireMsg::single_src_node_join(
        &relocated_node,
        Dst {
            name: XorName::from(PublicKey::Bls(section_key)),
            section_key,
        },
        NodeMsg::JoinAsRelocatedRequest(Box::new(JoinAsRelocatedRequest {
            section_key,
            relocate_proof,
            signature_over_new_name,
        })),
    )?;

    ProcessAndInspectCmds::new(
        Cmd::HandleMsg {
            origin: relocated_node.peer(),
            wire_msg,
            send_stream: None,
        },
        &dispatcher,
    )
    .process_all()
    .await?;

    assert!(node
        .read()
        .await
        .membership
        .as_ref()
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .is_churn_in_progress());

    Ok(())
}

#[tokio::test]
async fn handle_agreement_on_online() -> Result<()> {
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), 0, None, None)
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);
    let sk_set = env.get_secret_key_set(prefix, None);
    let new_peer = gen_peer(MIN_ADULT_AGE);
    let status = handle_online_cmd(&new_peer, &sk_set, &dispatcher).await?;
    assert!(status.node_approval_sent);

    assert!(dispatcher
        .node()
        .read()
        .await
        .network_knowledge()
        .is_adult(&new_peer.name()));

    Ok(())
}

#[tokio::test]
async fn handle_agreement_on_online_of_elder_candidate() -> Result<()> {
    init_logger();
    // Creates nodes where everybody has age 6 except one has 5.
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(
            prefix,
            elder_count(),
            0,
            Some(&[MIN_ADULT_AGE, MIN_ADULT_AGE + 1]),
            None,
        )
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);
    let node_name = dispatcher.node().read().await.name();
    let section = env.get_network_knowledge(prefix, None);
    let sk_set = env.get_secret_key_set(prefix, None);

    let mut expected_new_elders = section
        .elders()
        .into_iter()
        .filter(|peer| peer.age() == MIN_ADULT_AGE + 1)
        .collect::<BTreeSet<_>>();

    // Handle agreement on Online of a peer that is older than the youngest
    // current elder - that means this peer is going to be promoted.
    let new_peer = gen_peer(MIN_ADULT_AGE + 1);
    let node_state = NodeState::joined(new_peer, Some(xor_name::rand::random()));

    let membership_decision = section_decision(&sk_set, node_state.clone());

    // Force this node to join
    dispatcher
        .node()
        .write()
        .await
        .membership
        .as_mut()
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .force_bootstrap(node_state);

    let mut cmds = ProcessAndInspectCmds::new(
        Cmd::HandleMembershipDecision(membership_decision),
        &dispatcher,
    );

    // Verify we sent a `DkgStart` message with the expected participants.
    let mut dkg_start_sent = false;
    let _changed = expected_new_elders.insert(new_peer);

    while let Some(cmd) = cmds.next().await? {
        let (msg, recipients) = match cmd {
            Cmd::SendMsg {
                recipients, msg, ..
            } => (msg, recipients),
            _ => continue,
        };

        let actual_elder_candidates = match msg {
            NodeMsg::DkgStart(session, _) => session.elders.clone(),
            _ => continue,
        };

        itertools::assert_equal(
            actual_elder_candidates,
            expected_new_elders.iter().map(|p| (p.name(), p.addr())),
        );

        let expected_dkg_start_recipients: BTreeSet<_> = expected_new_elders
            .iter()
            .filter(|peer| peer.name() != node_name)
            .cloned()
            .collect();

        assert_matches!(recipients, Peers::Multiple(peers) => {
            assert_eq!(peers, &expected_dkg_start_recipients);
        });

        dkg_start_sent = true;
    }

    assert!(dkg_start_sent);
    Ok(())
}

#[tokio::test]
async fn handle_join_request_of_rejoined_node() -> Result<()> {
    init_logger();
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), 0, None, None)
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);

    // Make a left peer.
    let peer = gen_peer_in_prefix(MIN_ADULT_AGE, prefix);
    dispatcher
        .node()
        .write()
        .await
        .membership
        .as_mut()
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .force_bootstrap(NodeState::left(peer, None));

    // Simulate the same peer rejoining
    let node_state = NodeState::joined(peer, None);
    let join_cmd = dispatcher
        .node()
        .write()
        .await
        .propose_membership_change(node_state.clone());

    // A rejoining node will always be rejected
    assert!(join_cmd.is_none()); // no cmd signals this membership proposal was dropped.
    assert!(!dispatcher
        .node()
        .read()
        .await
        .membership
        .as_ref()
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .is_churn_in_progress());
    Ok(())
}

#[tokio::test]
async fn handle_agreement_on_offline_of_non_elder() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("handle_agreement_on_offline_of_non_elder").entered();
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), 1, None, None)
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);
    let sk_set = env.get_secret_key_set(prefix, None);

    // get the node state of the non_elder node
    let node_state = env.get_nodes(prefix, 0, 1, None).remove(0).info().peer();
    let node_state = NodeState::left(node_state, None);

    let proposal = SectionStateVote::NodeIsOffline(node_state.clone());
    let sig = TestKeys::get_section_sig_bytes(&sk_set.secret_key(), &get_single_sig(&proposal));

    ProcessAndInspectCmds::new(
        Cmd::HandleSectionDecisionAgreement { proposal, sig },
        &dispatcher,
    )
    .process_all()
    .await?;

    assert!(!dispatcher
        .node()
        .read()
        .await
        .network_knowledge()
        .section_members()
        .contains(&node_state));
    Ok(())
}

#[tokio::test]
async fn handle_agreement_on_offline_of_elder() -> Result<()> {
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), 0, None, None)
        .build();
    let mut elders = env.get_dispatchers(prefix, 2, 0, None);
    let dispatcher = elders.remove(0);
    let sk_set = env.get_secret_key_set(prefix, None);

    let remove_elder = elders.remove(0).node().read().await.info().peer();
    let remove_elder = NodeState::left(remove_elder, None);

    // Handle agreement on the Offline proposal
    let proposal = SectionStateVote::NodeIsOffline(remove_elder.clone());
    let sig = TestKeys::get_section_sig_bytes(&sk_set.secret_key(), &get_single_sig(&proposal));

    ProcessAndInspectCmds::new(
        Cmd::HandleSectionDecisionAgreement { proposal, sig },
        &dispatcher,
    )
    .process_all()
    .await?;

    // Verify we initiated a membership churn
    assert!(dispatcher
        .node()
        .read()
        .await
        .membership
        .as_ref()
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .is_churn_in_progress());
    Ok(())
}

#[tokio::test]
async fn ae_msg_from_the_future_is_handled() -> Result<()> {
    init_logger();
    let _span = info_span!("ae_msg_from_the_future_is_handled").entered();

    let prefix = Prefix::default();
    let (elders0, ..) = TestNetwork::gen_node_infos(&prefix, elder_count(), 0, Some(&[6]));
    let new_elder = TestNetwork::gen_info(MIN_ADULT_AGE, Some(prefix));
    let elders1 = elders0
        .clone()
        .into_iter()
        .take(elder_count() - 1)
        .chain(vec![(new_elder.0, new_elder.1)])
        .collect::<Vec<_>>();

    // SAP0 is succeeded by SAP1 with a change in elder list
    let env = TestNetworkBuilder::new(thread_rng())
        .sap_with_members(prefix, elders0.clone(), elders0)
        .sap_with_members(prefix, elders1.clone(), elders1)
        .build();
    let sk_set0 = env.get_secret_key_set(prefix, Some(0));
    let sap1 = env.get_sap(prefix, Some(1));
    let sk_set1 = env.get_secret_key_set(prefix, Some(1));
    let pk_0 = sk_set0.public_keys().public_key();

    // Our node does not know about SAP1
    let mut node = env.get_nodes(prefix, 1, 0, Some(0)).remove(0);

    let new_section_elders: BTreeSet<_> = sap1.elders_set();
    let section_tree_update = TestSectionTree::get_section_tree_update(
        &sap1,
        &node.section_chain(),
        &sk_set0.secret_key(),
    );

    // Create the `Sync` message containing the new `Section`.
    let sender = gen_info(MIN_ADULT_AGE, None);
    let wire_msg = WireMsg::single_src_node(
        &sender,
        Dst {
            name: XorName::from(PublicKey::Bls(pk_0)),
            section_key: pk_0,
        },
        NodeMsg::AntiEntropy {
            section_tree_update,
            kind: AntiEntropyKind::Update {
                members: BTreeSet::default(),
            },
        },
    )?;

    // Simulate DKG round finished succesfully by adding
    // the new section key share to our cache
    node.section_keys_provider
        .insert(TestKeys::get_section_key_share(&sk_set1, 0));

    let (dispatcher, _) = Dispatcher::new(Arc::new(RwLock::new(node)));

    ProcessAndInspectCmds::new(
        Cmd::HandleMsg {
            origin: sender.peer(),
            wire_msg,
            send_stream: None,
        },
        &dispatcher,
    )
    .process_all()
    .await?;

    // Verify our `Section` got updated.
    assert_lists(
        dispatcher.node().read().await.network_knowledge().elders(),
        new_section_elders,
    );
    Ok(())
}

/// Checking when we send AE info to a section from untrusted section, we do not handle it and
/// error out.
#[tokio::test]
async fn untrusted_ae_msg_errors() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("untrusted_ae_msg_errors").entered();

    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), 0, None, None)
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);
    let section = env.get_network_knowledge(prefix, None);
    let sk_set = env.get_secret_key_set(prefix, None);
    let pk = sk_set.secret_key().public_key();

    // a valid AE msg but with a non-verifiable SAP...
    let signed_sap = section.signed_sap();
    let bogus_section_pk = bls::SecretKey::random().public_key();
    let bogus_section_tree_update =
        SectionTreeUpdate::new(signed_sap.clone(), SectionsDAG::new(bogus_section_pk));

    let node_msg = NodeMsg::AntiEntropy {
        section_tree_update: bogus_section_tree_update,
        kind: AntiEntropyKind::Update {
            members: BTreeSet::default(),
        },
    };

    let sender = gen_info(MIN_ADULT_AGE, None);
    let wire_msg = WireMsg::single_src_node(
        &sender,
        Dst {
            name: XorName::from(PublicKey::Bls(bogus_section_pk)),
            section_key: bogus_section_pk,
        },
        node_msg.clone(),
        // we use the nonsense here
    )?;

    assert!(matches!(
        ProcessAndInspectCmds::new(
            Cmd::HandleMsg {
                origin: sender.peer(),
                wire_msg,
                send_stream: None,
            },
            &dispatcher,
        )
        .process_all()
        .await,
        Err(Error::NetworkKnowledge(
            NetworkKnowledgeError::UntrustedProofChain(_)
        ))
    ));

    assert_eq!(
        dispatcher
            .node()
            .read()
            .await
            .network_knowledge()
            .genesis_key(),
        &pk
    );
    assert_eq!(
        dispatcher
            .node()
            .read()
            .await
            .network_knowledge()
            .section_tree()
            .all()
            .collect::<Vec<_>>(),
        vec![&signed_sap.value]
    );
    Ok(())
}

#[tokio::test]
async fn relocation_of_non_elder() -> Result<()> {
    relocation(RelocatedPeerRole::NonElder).await
}

/// Create a `SectionStateVote::Online` whose agreement handling triggers relocation of a node with the
/// given age.
///
/// NOTE: recommended to call this with low `age` (4 or 5), otherwise it might take very long time
/// to complete because it needs to generate a signature with the number of trailing zeroes equal
/// to (or greater that) `age`.
pub(crate) fn create_relocation_trigger(
    sk_set: &bls::SecretKeySet,
    age: u8,
) -> Decision<NodeState> {
    loop {
        let node_state = NodeState::joined(gen_peer(MIN_ADULT_AGE), Some(xor_name::rand::random()));
        let decision = section_decision(sk_set, node_state.clone());

        let sig: bls::Signature = decision.proposals[&node_state].clone();
        let churn_id = ChurnId(sig.to_bytes());

        if relocation_check(age, &churn_id) && !relocation_check(age + 1, &churn_id) {
            return decision;
        }
    }
}

fn threshold() -> usize {
    supermajority(elder_count()) - 1
}

enum RelocatedPeerRole {
    NonElder,
    Elder,
}

async fn relocation(relocated_peer_role: RelocatedPeerRole) -> Result<()> {
    let prefix: Prefix = prefix("0");
    let section_size = match relocated_peer_role {
        RelocatedPeerRole::Elder => elder_count(),
        RelocatedPeerRole::NonElder => recommended_section_size(),
    };
    let adults = section_size - elder_count();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), adults, None, None)
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);
    let mut section = env.get_network_knowledge(prefix, None);
    let sk_set = env.get_secret_key_set(prefix, None);

    let relocated_peer = match relocated_peer_role {
        // our node is elder idx 0, so remove the second elder
        RelocatedPeerRole::Elder => env.get_peers(prefix, 2, 0, None).remove(1),
        RelocatedPeerRole::NonElder => {
            let non_elder_peer = gen_peer(MIN_ADULT_AGE - 1);
            let node_state = NodeState::joined(non_elder_peer, None);
            let node_state = TestKeys::get_section_signed(&sk_set.secret_key(), node_state);
            assert!(section.update_member(node_state));
            // update our node with the new network_knowledge
            dispatcher.node().write().await.network_knowledge = section.clone();
            non_elder_peer
        }
    };

    let membership_decision = create_relocation_trigger(&sk_set, relocated_peer.age());
    let mut cmds = ProcessAndInspectCmds::new(
        Cmd::HandleMembershipDecision(membership_decision),
        &dispatcher,
    );

    let mut offline_relocate_sent = false;

    while let Some(cmd) = cmds.next().await? {
        let msg = match cmd {
            Cmd::SendMsg { msg, .. } => msg,
            _ => continue,
        };

        if let NodeMsg::ProposeSectionState {
            proposal: system::SectionStateVote::NodeIsOffline(node_state),
            ..
        } = msg
        {
            assert_eq!(node_state.name(), relocated_peer.name());
            if let MembershipState::Relocated(relocate_details) = node_state.state() {
                assert_eq!(relocate_details.age, relocated_peer.age() + 1);
                offline_relocate_sent = true;
            }
        }
    }

    assert!(offline_relocate_sent);
    Ok(())
}

#[tokio::test]
async fn msg_to_self() -> Result<()> {
    let prefix = Prefix::default();
    let mut env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, 1, 0, None, None)
        .build();

    let node = env.get_nodes(prefix, 1, 0, None).remove(0);
    let mut comm_rx = env.take_comm_rx(node.info().public_key());
    let context = node.context();
    let info = node.info();
    let (dispatcher, _) = Dispatcher::new(Arc::new(RwLock::new(node)));

    let node_msg = NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateDataBatch(vec![]));

    // don't use the cmd collection fn, as it skips Cmd::SendMsg
    let cmds = dispatcher
        .process_cmd(Cmd::send_msg(
            node_msg.clone(),
            Peers::Single(info.peer()),
            context,
        ))
        .await?;

    assert!(cmds.is_empty());

    let msg_type = assert_matches!(comm_rx.recv().await, Some(MsgFromPeer { sender, wire_msg, .. }) => {
        assert_eq!(sender.addr(), info.addr);
        assert_matches!(wire_msg.into_msg(), Ok(msg_type) => msg_type)
    });

    assert_matches!(msg_type, MsgType::Node { msg, .. } => {
        assert_eq!(
            msg,
            node_msg
        );
    });
    Ok(())
}

#[tokio::test]
async fn handle_elders_update() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("handle_elders_update").entered();

    let prefix = Prefix::default();
    // Start with section that has `elder_count()` elders with age 6, 1 non-elder with age 5 and one
    // to-be-elder with age 7
    let (elders0, ..) = TestNetwork::gen_node_infos(&prefix, elder_count(), 1, Some(&[6]));
    let mut elders1 = elders0.clone();
    let promoted_peer = {
        let (promoted_node, promoted_comm, _) = TestNetwork::gen_info(MIN_ADULT_AGE + 2, None);
        (promoted_node, promoted_comm)
    };
    // members list remain the same for the two SAPs
    let members = elders1
        .clone()
        .into_iter()
        .chain(vec![promoted_peer.clone()]);

    let demoted_peer = elders1.remove(elders1.len() - 1);
    elders1.push(promoted_peer.clone());

    let env = TestNetworkBuilder::new(StdRng::seed_from_u64(123))
        .sap_with_members(prefix, elders0, members.clone())
        .sap_with_members(prefix, elders1, members)
        .build();
    let section0 = env.get_network_knowledge(prefix, Some(0));
    let sk_set0 = env.get_secret_key_set(prefix, Some(0));
    let sap1 = env.get_sap(prefix, Some(1));
    let sk_set1 = env.get_secret_key_set(prefix, Some(1));

    // node from sap0 will process `HandleNewEldersAgreement` to update its knowledge about sap1
    let mut node = env.get_nodes(prefix, 1, 0, Some(0)).remove(0);
    let info = node.info();
    // Simulate DKG round finished successfully by adding
    // the new section key share to our cache
    node.section_keys_provider
        .insert(TestKeys::get_section_key_share(&sk_set1, 0));
    let (dispatcher, _) = Dispatcher::new(Arc::new(RwLock::new(node)));

    // Create `HandleNewEldersAgreement` cmd. This will demote one of the
    // current elders and promote the oldest peer.
    let elders_1: BTreeSet<_> = sap1.elders_set();
    let bytes = bincode::serialize(&sap1).expect("Failed to serialize");
    let sig = TestKeys::get_section_sig_bytes(&sk_set0.secret_key(), &bytes);

    let mut cmds = ProcessAndInspectCmds::new(
        Cmd::HandleNewEldersAgreement {
            new_elders: sap1,
            sig,
        },
        &dispatcher,
    );

    let mut update_actual_recipients = HashSet::new();
    while let Some(cmd) = cmds.next().await? {
        let (msg, recipients) = match cmd {
            Cmd::SendMsg {
                msg,
                recipients: Peers::Multiple(recipients),
                ..
            } => (msg, recipients),
            _ => continue,
        };

        let section_tree_update = match msg {
            NodeMsg::AntiEntropy {
                section_tree_update,
                kind: AntiEntropyKind::Update { .. },
                ..
            } => section_tree_update.clone(),
            _ => continue,
        };

        assert_eq!(
            section_tree_update.proof_chain.last_key()?,
            sk_set1.public_keys().public_key()
        );
        // Merging the section contained in the message with the original section succeeds.
        assert!(section0
            .clone()
            .update_knowledge_if_valid(section_tree_update, None, &info.name())
            .is_ok());

        update_actual_recipients.extend(recipients);
    }

    let update_expected_recipients: HashSet<_> = env
        .get_peers(prefix, elder_count(), 1, Some(0))
        .into_iter()
        .filter(|peer| *peer != info.peer())
        .chain(iter::once(promoted_peer.0.peer()))
        .chain(iter::once(demoted_peer.0.peer()))
        .collect();

    assert_eq!(update_actual_recipients, update_expected_recipients);

    assert_lists(
        dispatcher.node().read().await.network_knowledge().elders(),
        elders_1,
    );

    Ok(())
}

/// Test that demoted node still sends `Sync` messages on split.
#[tokio::test]
async fn handle_demote_during_split() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("handle_demote_during_split").entered();
    let prefix0 = prefix("0");
    let prefix1 = prefix("1");

    // `peers_a` + `info` are pre-split elders.
    // `peers_a` + `peer_c` are prefix-0 post-split elders.
    let (mut peers_a, ..) =
        TestNetwork::gen_node_infos(&prefix0, elder_count(), 0, Some(&[MIN_ADULT_AGE]));

    let info = peers_a
        .pop()
        .unwrap_or_else(|| panic!("No nodes generated!"));
    let node_name = info.0.name();

    // `peers_b` are prefix-1 post-split elders.
    let (peers_b, ..) =
        TestNetwork::gen_node_infos(&prefix1, elder_count(), 0, Some(&[MIN_ADULT_AGE]));
    // `peer_c` is a prefix-0 post-split elder.
    let peer_c = {
        let (peer_c, comm, _) = TestNetwork::gen_info(MIN_ADULT_AGE, Some(prefix0));
        (peer_c, comm)
    };
    // all members
    let members = peers_a
        .iter()
        .chain(peers_b.iter())
        .cloned()
        .chain([info.clone(), peer_c.clone()]);

    let env = TestNetworkBuilder::new(thread_rng())
        // pre-split section
        .sap_with_members(
            Prefix::default(),
            peers_a.iter().cloned().chain(iter::once(info.clone())),
            members.clone(),
        )
        // post-split prefix-0
        .sap_with_members(
            prefix0,
            peers_a.iter().cloned().chain(iter::once(peer_c.clone())),
            members.clone(),
        )
        // post-split prefix-1
        .sap_with_members(prefix1, peers_b.clone(), members)
        .build();

    let sk_set_gen = env.get_secret_key_set(Prefix::default(), None);
    let sap0 = env.get_sap(prefix0, None).value;
    let sk_set0 = env.get_secret_key_set(prefix0, None);
    let sap1 = env.get_sap(prefix1, None).value;
    let sk_set1 = env.get_secret_key_set(prefix1, None);

    // get the `info` node from pre-split section
    let mut node = env.get_node_by_key(Prefix::default(), info.0.public_key(), None);

    // Simulate DKG round finished successfully by adding the new section
    // key share to our cache (according to which split section we'll belong to).
    if prefix0.matches(&node_name) {
        node.section_keys_provider
            .insert(TestKeys::get_section_key_share(&sk_set0, 0));
    } else {
        node.section_keys_provider
            .insert(TestKeys::get_section_key_share(&sk_set1, 0));
    }

    let (dispatcher, _) = Dispatcher::new(Arc::new(RwLock::new(node)));

    let cmd = {
        // Sign the saps.
        let sap0 = TestKeys::get_section_signed(&sk_set0.secret_key(), sap0);
        let sap1 = TestKeys::get_section_signed(&sk_set1.secret_key(), sap1);

        let bytes0 = bincode::serialize(&sap0.sig.public_key).expect("Failed to serialize");
        let bytes1 = bincode::serialize(&sap1.sig.public_key).expect("Failed to serialize");

        Cmd::HandleNewSectionsAgreement {
            sap1: sap0,
            sig1: TestKeys::get_section_sig_bytes(&sk_set_gen.secret_key(), &bytes0),
            sap2: sap1,
            sig2: TestKeys::get_section_sig_bytes(&sk_set_gen.secret_key(), &bytes1),
        }
    };
    let mut cmds = ProcessAndInspectCmds::new(cmd, &dispatcher);

    let mut update_recipients = BTreeSet::new();
    while let Some(cmd) = cmds.next().await? {
        let (msg, recipients) = match cmd {
            Cmd::SendMsg {
                msg, recipients, ..
            } => (msg, recipients.clone()),
            _ => continue,
        };

        if let NodeMsg::AntiEntropy {
            kind: AntiEntropyKind::Update { .. },
            ..
        } = msg
        {
            update_recipients.extend(recipients.into_iter().map(|r| r.name()))
        }
    }

    // our node's whole section
    assert_eq!(update_recipients.len(), elder_count());
    Ok(())
}

#[tokio::test]
#[ignore = "This needs to be refactored away from Cmd handling, as we need/use a client response stream therein"]
async fn spentbook_spend_client_message_should_replicate_to_adults_and_send_ack() -> Result<()> {
    init_logger();
    let prefix = Prefix::default();
    let replication_count = 5;
    std::env::set_var("SN_DATA_COPY_COUNT", replication_count.to_string());

    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), 6, None, Some(0))
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);
    let peer = dispatcher.node().read().await.info().peer();
    let sk_set = env.get_secret_key_set(prefix, None);

    let (key_image, tx, spent_proofs, spent_transactions) =
        dbc_utils::get_genesis_dbc_spend_info(&sk_set)?;

    let mut cmds = ProcessAndInspectCmds::new_with_client_msg(
        ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
            key_image,
            tx: tx.clone(),
            spent_proofs,
            spent_transactions,
            network_knowledge: None,
        })),
        peer,
        &dispatcher,
    )?;

    let replicate_cmd = cmds.next().await?.expect("Recplicate cmd not found");
    let recipients = replicate_cmd.recipients()?;
    assert_eq!(recipients.len(), replication_count);

    let replicated_data = replicate_cmd.get_replicated_data()?;
    assert_matches!(replicated_data, ReplicatedData::SpentbookWrite(_));

    let spent_proof_share = dbc_utils::get_spent_proof_share_from_replicated_data(replicated_data)?;
    assert_eq!(key_image.to_hex(), spent_proof_share.key_image().to_hex());
    assert_eq!(Hash::from(tx.hash()), spent_proof_share.transaction_hash());
    assert_eq!(
        sk_set.public_keys().public_key().to_hex(),
        spent_proof_share.spentbook_pks().public_key().to_hex()
    );

    // let client_msg = cmds[1].clone().get_client_msg()?;
    // assert_matches!(client_msg, ClientMsg::CmdResponse { response, .. } => response.is_success());
    Ok(())
}

#[tokio::test]
#[ignore = "This needs to be refactored away from Cmd handling, as we need/use a client response stream therein"]
async fn spentbook_spend_transaction_with_no_inputs_should_return_spentbook_error() -> Result<()> {
    init_logger();
    let prefix = prefix("1");
    let replication_count = 5;
    std::env::set_var("SN_DATA_COPY_COUNT", replication_count.to_string());

    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix, elder_count(), 6, None, Some(0))
        .build();
    let dispatcher = env.get_dispatchers(prefix, 1, 0, None).remove(0);
    let peer = dispatcher.node().read().await.info().peer();
    let section = env.get_network_knowledge(prefix, None);
    let sk_set = env.get_secret_key_set(prefix, None);

    // These conditions will produce a failure on `tx.verify` in the message handler.
    let sap = section.section_auth();
    let keys_provider = dispatcher.node().read().await.section_keys_provider.clone();
    let genesis_dbc = gen_genesis_dbc(&sk_set, &sk_set.secret_key())?;
    let new_dbc = reissue_dbc(
        &genesis_dbc,
        10,
        &bls::SecretKey::random(),
        &sap,
        &keys_provider,
    )?;
    let new_dbc2_sk = bls::SecretKey::random();
    let new_dbc2 = dbc_utils::reissue_invalid_dbc_with_no_inputs(&new_dbc, 5, &new_dbc2_sk)?;

    let mut _cmds = ProcessAndInspectCmds::new_with_client_msg(
        ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
            key_image: new_dbc2_sk.public_key(),
            tx: new_dbc2.transaction,
            spent_proofs: new_dbc.spent_proofs.clone(),
            spent_transactions: new_dbc.spent_transactions,
            network_knowledge: None,
        })),
        peer,
        &dispatcher,
    )?;

    // while let Some(cmd) = cmds.next().await? {
    //     match cmd {
    //         Cmd::SendMsg {
    //             msg:
    //                 OutgoingMsg::Client(ClientMsg::CmdResponse {
    //                     response: CmdResponse::SpendKey(result),
    //                     ..
    //                 }),
    //             ..
    //         } => {
    //             if let Some(error) = result.err() {
    //                 assert_eq!(
    //                     error.to_string(),
    //                     MessagingDataError::from(Error::SpentbookError(
    //                         "The DBC transaction must have at least one input".to_string()
    //                     ))
    //                     .to_string(),
    //                     "A different error was expected for this case: {:?}",
    //                     error
    //                 );
    //                 return Ok(());
    //             } else {
    //                 bail!("We expected an error to be returned");
    //             }
    //         }
    //         _ => continue,
    //     }
    // }

    bail!("We expected an error to be returned");
}

/// This could potentially be the start of a case for the updated proof chain and SAP being sent
/// with the spend request, but I don't know exactly what the conditions are for getting the
/// network knowledge to update correctly.
#[tokio::test]
#[ignore = "Needs to be refactored to take into account that ClientMsgs require a stream (or to avoid this)"]
async fn spentbook_spend_with_updated_network_knowledge_should_update_the_node() -> Result<()> {
    init_logger();
    let replication_count = 5;
    let prefix1 = prefix("1");
    std::env::set_var("SN_DATA_COPY_COUNT", replication_count.to_string());

    let env = TestNetworkBuilder::new(thread_rng())
        .sap(prefix("0"), elder_count(), 6, None, Some(0))
        .sap(prefix1, elder_count(), 6, None, Some(0))
        .build();
    let dispatcher = env.get_dispatchers(Prefix::default(), 1, 0, None).remove(0);
    let info = dispatcher.node().read().await.info();
    let genesis_sk_set = env.get_secret_key_set(Prefix::default(), None);
    let other_section_key_share = env.get_section_key_share(prefix1, info.public_key(), None);
    let other_section = env.get_network_knowledge(prefix1, None);
    let other_section_key = env.get_secret_key_set(prefix1, None);

    // At this point, only the genesis key should be in the proof chain on this node.
    let tree = dispatcher
        .node()
        .read()
        .await
        .network_knowledge()
        .section_tree()
        .clone();
    let proof_chain = tree.get_sections_dag().clone();
    assert_eq!(proof_chain.keys().into_iter().count(), 1);

    // The key share also needs to be added to the section keys provider, which is stored
    // on the node.
    dispatcher
        .node()
        .write()
        .await
        .section_keys_provider
        .insert(other_section_key_share.clone());

    // Reissue a couple of DBC from genesis. They will be reissued using the section keys
    // provider and SAP from the other section, hence the spent proofs will be signed with
    // the unknown section key.
    // The owners of the DBCs here don't really matter, so we just use random keys.
    let skp = SectionKeysProvider::new(Some(other_section_key_share.clone()));
    let sap = other_section.signed_sap();
    let genesis_dbc = gen_genesis_dbc(&genesis_sk_set, &genesis_sk_set.secret_key())?;
    let new_dbc = reissue_dbc(&genesis_dbc, 10, &bls::SecretKey::random(), &sap, &skp)?;
    let new_dbc2 = reissue_dbc(&new_dbc, 5, &bls::SecretKey::random(), &sap, &skp)?;
    let new_dbc2_spent_proof = new_dbc2
        .spent_proofs
        .iter()
        .next()
        .ok_or_else(|| eyre!("This DBC should have been reissued with a spent proof"))?;
    assert_eq!(
        new_dbc2_spent_proof.spentbook_pub_key,
        other_section_key.secret_key().public_key()
    );

    // Finally, spend new_dbc2 as part of the input for another reissue.
    // It needs to be associated with a valid transaction, which is why the util function
    // is used. Again, the owner of the output DBCs don't really matter, so a random key is
    // used.
    let proof_chain = other_section.section_chain();
    let (key_image, tx) = get_input_dbc_spend_info(&new_dbc2, 2, &bls::SecretKey::random())?;

    ProcessAndInspectCmds::new_with_client_msg(
        ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
            key_image,
            tx,
            spent_proofs: new_dbc2.spent_proofs,
            spent_transactions: new_dbc2.spent_transactions,
            network_knowledge: Some((proof_chain, sap)),
        })),
        info.peer(),
        &dispatcher,
    )?
    .process_all()
    .await?;

    // // The commands returned here should include the new command to update the network
    // // knowledge and also the other two commands to replicate the spent proof shares and
    // // the ack command, but we've already validated the other two as part of another test.
    // assert_eq!(cmds.len(), 3);
    // let update_cmd = cmds[0].clone();
    // assert_matches!(update_cmd, Cmd::UpdateNetworkAndHandleValidClientMsg { .. });

    // Now the proof chain should have the other section key.
    let tree = dispatcher
        .node()
        .read()
        .await
        .network_knowledge()
        .section_tree()
        .clone();
    let proof_chain = tree.get_sections_dag().clone();
    assert_eq!(proof_chain.keys().into_iter().count(), 2);
    let mut proof_chain_iter = proof_chain.keys();
    let genesis_key = genesis_sk_set.public_keys().public_key();
    assert_eq!(
        genesis_key,
        proof_chain_iter
            .next()
            .ok_or_else(|| eyre!("The proof chain should include the genesis key"))?
    );
    assert_eq!(
        other_section_key.secret_key().public_key(),
        proof_chain_iter
            .next()
            .ok_or_else(|| eyre!("The proof chain should include the other section key"))?
    );

    Ok(())
}

fn get_single_sig(proposal: &SectionStateVote) -> Vec<u8> {
    bincode::serialize(proposal).expect("Failed to serialize")
}
