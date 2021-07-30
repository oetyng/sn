// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#![allow(dead_code, unused_imports)]

use super::{Comm, Command, Core, Dispatcher};
use crate::dbs::UsedSpace;
use crate::messaging::{
    node::{
        JoinAsRelocatedRequest, JoinRequest, JoinResponse, KeyedSig, MembershipState, Network,
        NodeMsg, NodeState, Peer, Proposal, RelocateDetails, RelocatePayload,
        ResourceProofResponse, Section, SectionAuth,
    },
    section_info::{GetSectionResponse, SectionInfoMsg},
    AuthorityProof, DstLocation, MessageId, MessageType, MsgKind, NodeAuth,
    SectionAuth as MsgKindSectionAuth, SectionAuthorityProvider, WireMsg,
};
use crate::node::RegisterStorage;

use crate::routing::{
    core::{ConnectionEvent, RESOURCE_PROOF_DATA_SIZE, RESOURCE_PROOF_DIFFICULTY},
    create_test_used_space_and_root_storage,
    dkg::{
        test_utils::{prove, section_signed},
        ProposalUtils,
    },
    ed25519,
    messages::{NodeMsgAuthorityUtils, WireMsgUtils},
    network::NetworkUtils,
    node::Node,
    peer::PeerUtils,
    relocation::{self, RelocatePayloadUtils},
    section::{
        test_utils::*, ElderCandidatesUtils, NodeStateUtils, SectionKeyShare, SectionPeersUtils,
        SectionUtils,
    },
    supermajority, Event, SectionAuthorityProviderUtils, ELDER_SIZE, FIRST_SECTION_MIN_AGE,
    MIN_ADULT_AGE, MIN_AGE,
};
use crate::types::{Keypair, PublicKey};
use anyhow::{anyhow, Context, Result};
use assert_matches::assert_matches;
use ed25519_dalek::Signer;
use rand::rngs::OsRng;
use rand::{distributions::Alphanumeric, Rng};
use resource_proof::ResourceProof;
use secured_linked_list::SecuredLinkedList;
use std::path::Path;
use std::{
    collections::{BTreeSet, HashSet},
    iter,
    net::Ipv4Addr,
    ops::Deref,
};
use tempfile::tempdir;
use tokio::{
    sync::mpsc,
    time::{timeout, Duration},
};
use xor_name::{Prefix, XorName};

static TEST_EVENT_CHANNEL_SIZE: usize = 20;

#[tokio::test(flavor = "multi_thread")]
async fn receive_matching_get_section_request_as_elder() -> Result<()> {
    let node = create_node(MIN_ADULT_AGE, None);
    let node_name = node.name();
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::first_node(
        create_comm().await?,
        node,
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let node_section_pk = *core.section().chain().last_key();
    let section_auth = core.section().authority_provider().clone();
    let dispatcher = Dispatcher::new(core);

    let new_node_comm = create_comm().await?;
    let new_node = Node::new(
        ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
        new_node_comm.our_connection_info(),
    );

    let query = SectionInfoMsg::GetSectionQuery(PublicKey::from(new_node.keypair.public));
    let dst_location = DstLocation::Node {
        name: node_name,
        section_pk: node_section_pk,
    };
    let wire_msg = WireMsg::new_section_info_msg(&query, dst_location)?;

    let mut commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: new_node.addr,
            wire_msg,
        })
        .await?
        .into_iter();

    let (recipients, received_wire_msg) = assert_matches!(
        commands.next(),
        Some(Command::SendMessage {
            recipients,
            wire_msg, ..
        }) => (recipients, wire_msg)
    );

    assert_eq!(recipients, [(new_node.name(), new_node.addr)]);

    assert_matches!(
        received_wire_msg.into_message(),
        Ok(MessageType::SectionInfo {
            msg: SectionInfoMsg::GetSectionResponse(GetSectionResponse::Success(received_section_auth)),
            ..
        }) => assert_eq!(received_section_auth, section_auth)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_mismatching_get_section_request_as_adult() -> Result<()> {
    let good_prefix = Prefix::default().pushed(false);

    let sk_set = SecretKeySet::random();
    let (section_auth, _, _) = gen_section_authority_provider(good_prefix, ELDER_SIZE);
    let (section, _) = create_section(&sk_set, &section_auth)?;

    let node = create_node(MIN_ADULT_AGE, Some(good_prefix));
    let node_name = node.name();
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section,
        None,
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let node_section_pk = *core.section().chain().last_key();
    let dispatcher = Dispatcher::new(core);

    let mut rng = rand::thread_rng();
    let mut keypair = Keypair::new_ed25519(&mut rng);
    let mut random_pk = keypair.public_key();
    let mut new_node_name = XorName::from(random_pk);

    while new_node_name.bit(0) {
        keypair = Keypair::new_ed25519(&mut rng);
        random_pk = keypair.public_key();
        new_node_name = XorName::from(random_pk);
    }

    let new_node_comm = create_comm().await?;
    let new_node_addr = new_node_comm.our_connection_info();

    let query = SectionInfoMsg::GetSectionQuery(random_pk);
    let dst_location = DstLocation::Node {
        name: node_name,
        section_pk: node_section_pk,
    };
    let wire_msg = WireMsg::new_section_info_msg(&query, dst_location)?;

    let mut commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: new_node_addr,
            wire_msg,
        })
        .await?
        .into_iter();

    let (recipients, received_wire_msg) = assert_matches!(
        commands.next(),
        Some(Command::SendMessage {
            recipients,
            wire_msg, ..
        }) => (recipients, wire_msg)
    );

    assert_eq!(recipients, [(new_node_name, new_node_addr)]);
    assert_matches!(
        received_wire_msg.into_message(),
        Ok(MessageType::SectionInfo{msg: SectionInfoMsg::GetSectionResponse(GetSectionResponse::Redirect(received_section_auth)),..}) => {
            assert_eq!(received_section_auth, section_auth)
        }
    );

    Ok(())
}

// TODO: add test `receive_mismatching_get_section_request_as_elder` - should respond with
// `Redirect` response containing addresses of nodes in a section that is closer to the joining
// name.

#[tokio::test(flavor = "multi_thread")]
async fn receive_join_request_without_resource_proof_response() -> Result<()> {
    let node = create_node(FIRST_SECTION_MIN_AGE, None);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::first_node(
        create_comm().await?,
        node,
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let new_node_comm = create_comm().await?;
    let new_node = Node::new(
        ed25519::gen_keypair(&Prefix::default().range_inclusive(), FIRST_SECTION_MIN_AGE),
        new_node_comm.our_connection_info(),
    );
    let section_key = *dispatcher.core.read().await.section().chain().last_key();

    let wire_msg = WireMsg::single_src(
        &new_node,
        DstLocation::DirectAndUnrouted(section_key),
        NodeMsg::JoinRequest(Box::new(JoinRequest {
            section_key,
            resource_proof_response: None,
        })),
        section_key,
    )?;
    let mut commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: new_node.addr,
            wire_msg,
        })
        .await?
        .into_iter();

    let response_wire_msg = assert_matches!(
        commands.next(),
        Some(Command::SendMessage {
            wire_msg,
            ..
        }) => wire_msg
    );

    assert_matches!(
        response_wire_msg.into_message(),
        Ok(MessageType::Node {
            msg: NodeMsg::JoinResponse(response),
            ..
        }) => assert_matches!(*response, JoinResponse::ResourceChallenge { .. })
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_join_request_with_resource_proof_response() -> Result<()> {
    let node = create_node(FIRST_SECTION_MIN_AGE, None);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::first_node(
        create_comm().await?,
        node,
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let new_node = Node::new(
        ed25519::gen_keypair(&Prefix::default().range_inclusive(), FIRST_SECTION_MIN_AGE),
        gen_addr(),
    );
    let section_key = *dispatcher.core.read().await.section().chain().last_key();

    let nonce: [u8; 32] = rand::random();
    let serialized = bincode::serialize(&(new_node.name(), nonce))?;
    let nonce_signature = ed25519::sign(&serialized, &dispatcher.core.read().await.node().keypair);

    let rp = ResourceProof::new(RESOURCE_PROOF_DATA_SIZE, RESOURCE_PROOF_DIFFICULTY);
    let data = rp.create_proof_data(&nonce);
    let mut prover = rp.create_prover(data.clone());
    let solution = prover.solve();

    let wire_msg = WireMsg::single_src(
        &new_node,
        DstLocation::DirectAndUnrouted(section_key),
        NodeMsg::JoinRequest(Box::new(JoinRequest {
            section_key,
            resource_proof_response: Some(ResourceProofResponse {
                solution,
                data,
                nonce,
                nonce_signature,
            }),
        })),
        section_key,
    )?;

    let commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: new_node.addr,
            wire_msg,
        })
        .await?
        .into_iter();

    let mut test_connectivity = false;
    for command in commands {
        if let Command::ProposeOnline {
            peer,
            previous_name,
            dst_key,
        } = command
        {
            assert_eq!(*peer.name(), new_node.name());
            assert_eq!(*peer.addr(), new_node.addr);
            assert_eq!(peer.age(), FIRST_SECTION_MIN_AGE);
            assert_eq!(previous_name, None);
            assert_eq!(dst_key, None);

            test_connectivity = true;
        }
    }

    assert!(test_connectivity);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_join_request_from_relocated_node() -> Result<()> {
    let (section_auth, mut nodes) = create_section_auth();

    let sk_set = SecretKeySet::random();
    let pk_set = sk_set.public_keys();
    let section_key = pk_set.public_key();

    let (section, section_key_share) = create_section(&sk_set, &section_auth)?;
    let node = nodes.remove(0);
    let node_name = node.name();
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section,
        Some(section_key_share),
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let relocated_node_old_keypair =
        ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE);
    let relocated_node_old_name = ed25519::name(&relocated_node_old_keypair.public);
    let relocated_node = Node::new(
        ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_AGE + 2),
        gen_addr(),
    );

    let relocate_details = RelocateDetails {
        pub_id: relocated_node_old_name,
        dst: node_name,
        dst_key: section_key,
        age: relocated_node.age(),
    };

    let relocate_node_msg = NodeMsg::Relocate(relocate_details);
    let payload = WireMsg::serialize_msg_payload(&relocate_node_msg)?;
    let signature = sk_set.secret_key().sign(&payload);
    let section_signed = MsgKindSectionAuth {
        section_pk: section_key,
        src_name: node_name,
        sig: KeyedSig {
            public_key: section_key,
            signature,
        },
    };
    let section_auth = AuthorityProof::verify(section_signed.clone(), &payload).unwrap();

    let relocate_payload = RelocatePayload::new(
        relocate_node_msg,
        section_auth,
        &relocated_node.name(),
        &relocated_node_old_keypair,
    );

    let wire_msg = WireMsg::single_src(
        &relocated_node,
        DstLocation::DirectAndUnrouted(section_key),
        NodeMsg::JoinAsRelocatedRequest(Box::new(JoinAsRelocatedRequest {
            section_key,
            relocate_payload: Some(relocate_payload),
        })),
        section_key,
    )?;

    let commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: relocated_node.addr,
            wire_msg,
        })
        .await?;

    let mut propose_cmd_returned = false;

    for command in commands {
        if let Command::ProposeOnline {
            peer,
            previous_name,
            dst_key,
        } = command
        {
            assert_eq!(peer, relocated_node.peer());
            assert_eq!(previous_name, Some(relocated_node_old_name));
            assert_eq!(dst_key, Some(section_key));

            propose_cmd_returned = true;
        }
    }

    assert!(propose_cmd_returned);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_proposals() -> Result<()> {
    let (section_auth, nodes) = create_section_auth();
    let sk_set = SecretKeySet::random();
    let pk_set = sk_set.public_keys();
    let (section, section_key_share) = create_section(&sk_set, &section_auth)?;
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        nodes[0].clone(),
        section.clone(),
        Some(section_key_share),
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let new_peer = create_peer(MIN_AGE);
    let node_state = NodeState::joined(new_peer, None);
    let proposal = Proposal::Online {
        node_state,
        dst_key: None,
    };

    for (index, node) in nodes.iter().enumerate().take(THRESHOLD) {
        let sig_share = proposal.prove(pk_set.clone(), index, &sk_set.secret_key_share(index))?;
        let wire_msg = WireMsg::single_src(
            node,
            DstLocation::DirectAndUnrouted(*section.chain().last_key()),
            NodeMsg::Propose {
                content: proposal.clone(),
                sig_share,
            },
            section_auth.section_key(),
        )?;

        let commands = dispatcher
            .handle_command(Command::HandleMessage {
                sender: node.addr,
                wire_msg,
            })
            .await?;
        assert!(commands.is_empty());
    }

    let sig_share = proposal.prove(
        pk_set.clone(),
        THRESHOLD,
        &sk_set.secret_key_share(THRESHOLD),
    )?;
    let wire_msg = WireMsg::single_src(
        &nodes[THRESHOLD],
        DstLocation::DirectAndUnrouted(*section.chain().last_key()),
        NodeMsg::Propose {
            content: proposal.clone(),
            sig_share,
        },
        section_auth.section_key(),
    )?;
    let mut commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: nodes[THRESHOLD].addr,
            wire_msg,
        })
        .await?
        .into_iter();

    assert_matches!(
        commands.next(),
        Some(Command::HandleAgreement { proposal: agreement, .. }) => {
            assert_eq!(agreement, proposal);
        }
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_agreement_on_online() -> Result<()> {
    let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);

    let prefix = Prefix::default();

    let (section_auth, mut nodes, _) = gen_section_authority_provider(prefix, ELDER_SIZE);
    let sk_set = SecretKeySet::random();
    let (section, section_key_share) = create_section(&sk_set, &section_auth)?;
    let node = nodes.remove(0);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section,
        Some(section_key_share),
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let new_peer = create_peer(MIN_AGE);

    let status = handle_online_command(&new_peer, &sk_set, &dispatcher, &section_auth).await?;
    assert!(status.node_approval_sent);

    assert_matches!(event_rx.recv().await, Some(Event::MemberJoined { name, age, .. }) => {
        assert_eq!(name, *new_peer.name());
        assert_eq!(age, MIN_AGE);
    });

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_agreement_on_online_of_elder_candidate() -> Result<()> {
    let sk_set = SecretKeySet::random();
    let chain = SecuredLinkedList::new(sk_set.secret_key().public_key());

    // Creates nodes where everybody has age 6 except one has 5.
    let mut nodes: Vec<_> = gen_sorted_nodes(&Prefix::default(), ELDER_SIZE, true);

    let section_auth = SectionAuthorityProvider::new(
        nodes.iter().map(Node::peer),
        Prefix::default(),
        sk_set.public_keys(),
    );
    let section_signed_section_auth = section_signed(sk_set.secret_key(), section_auth.clone())?;

    let mut section = Section::new(*chain.root_key(), chain, section_signed_section_auth)?;
    let mut expected_new_elders = BTreeSet::new();

    for peer in section_auth.peers() {
        let mut peer = peer;
        peer.set_reachable(true);
        let node_state = NodeState::joined(peer, None);
        let sig = prove(sk_set.secret_key(), &node_state)?;
        let _ = section.update_member(SectionAuth {
            value: node_state,
            sig,
        });
        if peer.age() == MIN_AGE + 2 {
            let _ = expected_new_elders.insert(peer);
        }
    }

    let node = nodes.remove(0);
    let node_name = node.name();
    let section_key_share = create_section_key_share(&sk_set, 0);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section,
        Some(section_key_share),
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    // Handle agreement on Online of a peer that is older than the youngest
    // current elder - that means this peer is going to be promoted.
    let new_peer = create_peer(MIN_AGE + 2);
    let node_state = NodeState::joined(new_peer, Some(XorName::random()));
    let proposal = Proposal::Online {
        node_state,
        dst_key: Some(sk_set.secret_key().public_key()),
    };
    let sig = prove(sk_set.secret_key(), &proposal.as_signable())?;

    let commands = dispatcher
        .handle_command(Command::HandleAgreement { proposal, sig })
        .await?;

    // Verify we sent a `DkgStart` message with the expected participants.
    let mut dkg_start_sent = false;
    let _ = expected_new_elders.insert(new_peer);

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        let actual_elder_candidates = match wire_msg.into_message() {
            Ok(MessageType::Node {
                msg: NodeMsg::DkgStart {
                    elder_candidates, ..
                },
                ..
            }) => elder_candidates,
            _ => continue,
        };
        itertools::assert_equal(actual_elder_candidates.peers(), expected_new_elders.clone());

        let expected_dkg_start_recipients: Vec<_> = expected_new_elders
            .iter()
            .filter(|peer| *peer.name() != node_name)
            .map(|peer| (*peer.name(), *peer.addr()))
            .collect();
        assert_eq!(recipients, expected_dkg_start_recipients);

        dkg_start_sent = true;
    }

    assert!(dkg_start_sent);

    Ok(())
}

// Handles a concensused Online proposal.
async fn handle_online_command(
    peer: &Peer,
    sk_set: &SecretKeySet,
    dispatcher: &Dispatcher,
    section_auth: &SectionAuthorityProvider,
) -> Result<HandleOnlineStatus> {
    let node_state = NodeState::joined(*peer, None);
    let proposal = Proposal::Online {
        node_state,
        dst_key: None,
    };
    let sig = prove(sk_set.secret_key(), &proposal.as_signable())?;

    let commands = dispatcher
        .handle_command(Command::HandleAgreement { proposal, sig })
        .await?;

    let mut status = HandleOnlineStatus {
        node_approval_sent: false,
        relocate_details: None,
    };

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        match wire_msg.into_message() {
            Ok(MessageType::Node {
                msg: NodeMsg::JoinResponse(response),
                ..
            }) => {
                if let JoinResponse::Approval {
                    section_auth: section_signed_section_auth,
                    ..
                } = *response
                {
                    assert_eq!(section_signed_section_auth.value, *section_auth);
                    assert_eq!(recipients, [(*peer.name(), *peer.addr())]);
                    status.node_approval_sent = true;
                }
            }
            Ok(MessageType::Node {
                msg: NodeMsg::Relocate(details),
                ..
            }) => {
                if details.pub_id != *peer.name() {
                    continue;
                }

                assert_eq!(recipients, [(*peer.name(), *peer.addr())]);

                status.relocate_details = Some(details.clone());
            }
            _ => continue,
        }
    }

    Ok(status)
}

struct HandleOnlineStatus {
    node_approval_sent: bool,
    relocate_details: Option<RelocateDetails>,
}

enum NetworkPhase {
    Startup,
    Regular,
}

async fn handle_agreement_on_online_of_rejoined_node(phase: NetworkPhase, age: u8) -> Result<()> {
    let prefix = match phase {
        NetworkPhase::Startup => Prefix::default(),
        NetworkPhase::Regular => "0".parse().unwrap(),
    };
    let (section_auth, mut nodes, _) = gen_section_authority_provider(prefix, ELDER_SIZE);
    let sk_set = SecretKeySet::random();
    let (mut section, section_key_share) = create_section(&sk_set, &section_auth)?;

    // Make a left peer.
    let peer = create_peer(age);
    let node_state = NodeState {
        peer,
        state: MembershipState::Left,
        previous_name: None,
    };
    let node_state = section_signed(sk_set.secret_key(), node_state)?;
    let _ = section.update_member(node_state);

    // Make a Node
    let (event_tx, _event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let node = nodes.remove(0);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let state = Core::new(
        create_comm().await?,
        node,
        section,
        Some(section_key_share),
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(state);

    // Simulate peer with the same name is rejoin and verify resulted behaviours.
    let status = handle_online_command(&peer, &sk_set, &dispatcher, &section_auth).await?;

    // A rejoin node with low age will be rejected.
    if age / 2 <= MIN_AGE {
        assert!(!status.node_approval_sent);
        assert!(status.relocate_details.is_none());
        return Ok(());
    }

    assert!(status.node_approval_sent);
    assert_matches!(status.relocate_details, Some(details) => {
        assert_eq!(details.dst, *peer.name());
        assert_eq!(details.age, (age / 2).max(MIN_AGE));
    });

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_agreement_on_online_of_rejoined_node_with_high_age_in_startup() -> Result<()> {
    handle_agreement_on_online_of_rejoined_node(NetworkPhase::Startup, 16).await
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_agreement_on_online_of_rejoined_node_with_high_age_after_startup() -> Result<()> {
    handle_agreement_on_online_of_rejoined_node(NetworkPhase::Regular, 16).await
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_agreement_on_online_of_rejoined_node_with_low_age_in_startup() -> Result<()> {
    handle_agreement_on_online_of_rejoined_node(NetworkPhase::Startup, 8).await
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_agreement_on_online_of_rejoined_node_with_low_age_after_startup() -> Result<()> {
    handle_agreement_on_online_of_rejoined_node(NetworkPhase::Regular, 8).await
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_agreement_on_offline_of_non_elder() -> Result<()> {
    let (section_auth, mut nodes) = create_section_auth();
    let sk_set = SecretKeySet::random();

    let (mut section, section_key_share) = create_section(&sk_set, &section_auth)?;

    let existing_peer = create_peer(MIN_AGE);
    let node_state = NodeState::joined(existing_peer, None);
    let node_state = section_signed(sk_set.secret_key(), node_state)?;
    let _ = section.update_member(node_state);

    let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let node = nodes.remove(0);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section,
        Some(section_key_share),
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let node_state = NodeState {
        peer: existing_peer,
        state: MembershipState::Left,
        previous_name: None,
    };
    let proposal = Proposal::Offline(node_state);
    let sig = prove(sk_set.secret_key(), &proposal.as_signable())?;

    let _ = dispatcher
        .handle_command(Command::HandleAgreement { proposal, sig })
        .await?;

    assert_matches!(event_rx.recv().await, Some(Event::MemberLeft { name, age, }) => {
        assert_eq!(name, *existing_peer.name());
        assert_eq!(age, MIN_AGE);
    });

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_agreement_on_offline_of_elder() -> Result<()> {
    let (section_auth, mut nodes) = create_section_auth();
    let sk_set = SecretKeySet::random();

    let (mut section, section_key_share) = create_section(&sk_set, &section_auth)?;

    let existing_peer = create_peer(MIN_AGE);
    let node_state = NodeState::joined(existing_peer, None);
    let node_state = section_signed(sk_set.secret_key(), node_state)?;
    let _ = section.update_member(node_state);

    // Pick the elder to remove.
    let remove_peer = section_auth.peers().last().expect("section_auth is empty");

    let remove_node_state = section
        .members()
        .get(remove_peer.name())
        .expect("member not found")
        .leave()?;

    // Create our node
    let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let node = nodes.remove(0);
    let node_name = node.name();
    let core = Core::new(
        create_comm().await?,
        node,
        section,
        Some(section_key_share),
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    // Handle agreement on the Offline proposal
    let proposal = Proposal::Offline(remove_node_state);
    let sig = prove(sk_set.secret_key(), &proposal.as_signable())?;

    let commands = dispatcher
        .handle_command(Command::HandleAgreement { proposal, sig })
        .await?;

    // Verify we sent a `DkgStart` message with the expected participants.
    let mut dkg_start_sent = false;

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        let actual_elder_candidates = match wire_msg.into_message() {
            Ok(MessageType::Node {
                msg: NodeMsg::DkgStart {
                    elder_candidates, ..
                },
                ..
            }) => elder_candidates,
            _ => continue,
        };

        let expected_new_elders: BTreeSet<_> = section_auth
            .peers()
            .filter(|peer| *peer != remove_peer)
            .chain(iter::once(existing_peer))
            .collect();
        itertools::assert_equal(actual_elder_candidates.peers(), expected_new_elders.clone());

        let expected_dkg_start_recipients: Vec<_> = expected_new_elders
            .iter()
            .filter(|peer| *peer.name() != node_name)
            .map(|peer| (*peer.name(), *peer.addr()))
            .collect();
        assert_eq!(recipients, expected_dkg_start_recipients);

        dkg_start_sent = true;
    }

    assert!(dkg_start_sent);

    assert_matches!(event_rx.recv().await, Some(Event::MemberLeft { name, .. }) => {
        assert_eq!(name, *remove_peer.name());
    });

    // The removed peer is still our elder because we haven't yet processed the section update.
    assert!(dispatcher
        .core
        .read()
        .await
        .section()
        .authority_provider()
        .contains_elder(remove_peer.name()));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_untrusted_message_from_peer() -> Result<()> {
    handle_untrusted_message(UntrustedMessageSource::Peer).await
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_untrusted_accumulated_message() -> Result<()> {
    handle_untrusted_message(UntrustedMessageSource::Accumulation).await
}

enum UntrustedMessageSource {
    Peer,
    Accumulation,
}

async fn handle_untrusted_message(source: UntrustedMessageSource) -> Result<()> {
    let sk0 = bls::SecretKey::random();
    let pk0 = sk0.public_key();
    let chain = SecuredLinkedList::new(pk0);

    let (section_auth, _) = create_section_auth();
    let sender = *section_auth
        .addresses()
        .get(0)
        .ok_or_else(|| anyhow!("section_auth is empty"))?;

    let section_signed_section_auth = section_signed(&sk0, section_auth.clone())?;
    let mut section = Section::new(pk0, chain, section_signed_section_auth)?;

    let node = create_node(MIN_ADULT_AGE, None);
    let node_name = node.name();
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section.clone(),
        None,
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    // Create a any message signed by a key not known to the node.
    let sk1 = bls::SecretKey::random();
    let pk1 = sk1.public_key();
    let unknown_chain = SecuredLinkedList::new(pk1);
    // we corrupt the section chain so it's unknown
    section.chain = unknown_chain;
    let original_node_msg = NodeMsg::Sync {
        section,
        network: Network::new(),
    };
    let payload = WireMsg::serialize_msg_payload(&original_node_msg)?;
    let expected_recipients = vec![sender];
    let msg_kind = match source {
        UntrustedMessageSource::Peer => {
            let mut rng = OsRng {};
            let keypair = ed25519_dalek::Keypair::generate(&mut rng);
            MsgKind::NodeAuthMsg(NodeAuth {
                section_pk: pk1,
                public_key: keypair.public,
                signature: keypair.sign(&payload),
            })
        }
        UntrustedMessageSource::Accumulation => MsgKind::SectionAuthMsg(MsgKindSectionAuth {
            section_pk: pk1,
            src_name: Prefix::default().name(),
            sig: KeyedSig {
                public_key: pk1,
                signature: sk1.sign(&payload),
            },
        }),
    };

    let dst_location = DstLocation::Section {
        name: node_name,
        section_pk: pk0,
    };
    let wire_msg = WireMsg::new_msg(MessageId::new(), payload, msg_kind, dst_location)?;

    let commands = dispatcher
        .handle_command(Command::HandleMessage { sender, wire_msg })
        .await?;

    let mut bounce_sent = false;

    for command in commands {
        let (recipients, wire_msg) = if let Command::SendMessage {
            recipients,
            wire_msg,
            ..
        } = command
        {
            (recipients, wire_msg)
        } else {
            continue;
        };

        if let Ok(MessageType::Node {
            msg:
                NodeMsg::BouncedUntrustedMessage {
                    msg,
                    dst_section_pk,
                },
            ..
        }) = wire_msg.into_message()
        {
            assert_eq!(
                recipients
                    .into_iter()
                    .map(|recp| recp.1)
                    .collect::<Vec<_>>(),
                expected_recipients
            );
            assert_eq!(*msg, original_node_msg);
            assert_eq!(dst_section_pk, pk0);

            bounce_sent = true;
        }
    }

    assert!(bounce_sent);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_bounced_untrusted_message() -> Result<()> {
    let (section_auth, mut nodes, sk_set0) =
        gen_section_authority_provider(Prefix::default(), ELDER_SIZE);

    // Create section chain with two keys.
    let pk0 = sk_set0.public_keys().public_key();
    let sk1_set = SecretKeySet::random();
    let pk1 = sk1_set.secret_key().public_key();
    let pk1_signature = sk_set0.key.sign(&bincode::serialize(&pk1)?);

    let mut chain = SecuredLinkedList::new(pk0);
    let _ = chain.insert(&pk0, pk1, pk1_signature);

    let section_signed_section_auth = section_signed(sk1_set.secret_key(), section_auth)?;
    let section = Section::new(pk0, chain.clone(), section_signed_section_auth)?;
    let section_key_share = create_section_key_share(&sk1_set, 0);

    let node = nodes.remove(0);

    // Create the original message whose bounce we want to test.
    let other_node = Node::new(
        ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
        gen_addr(),
    );

    let original_node_msg = NodeMsg::Sync {
        section: section.clone(),
        network: Network::new(),
    };

    // Create our node.
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section.clone(),
        Some(section_key_share),
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    // Create the bounced message, indicating the last key the peer knows is `pk0`
    let bounced_wire_msg = WireMsg::single_src(
        &other_node,
        DstLocation::DirectAndUnrouted(pk0),
        NodeMsg::BouncedUntrustedMessage {
            msg: Box::new(original_node_msg.clone()),
            dst_section_pk: pk0,
        },
        pk0,
    )?;

    let commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: other_node.addr,
            wire_msg: bounced_wire_msg,
        })
        .await?;

    let mut message_resent = false;

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        match wire_msg.into_message() {
            Ok(MessageType::Node {
                msg, dst_location, ..
            }) => {
                assert_eq!(recipients, [(other_node.name(), other_node.addr)]);
                assert_eq!(msg, original_node_msg);
                assert_eq!(dst_location.section_pk(), Some(pk0));

                message_resent = true;
            }
            _ => continue,
        }
    }

    assert!(message_resent);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_sync() -> Result<()> {
    // Create first `Section` with a chain of length 2
    let sk0 = bls::SecretKey::random();
    let pk0 = sk0.public_key();
    let sk1_set = SecretKeySet::random();
    let pk1 = sk1_set.secret_key().public_key();
    let pk1_signature = sk0.sign(bincode::serialize(&pk1)?);

    let mut chain = SecuredLinkedList::new(pk0);
    assert_eq!(chain.insert(&pk0, pk1, pk1_signature), Ok(()));

    let (old_section_auth, mut nodes) = create_section_auth();
    let section_signed_old_section_auth =
        section_signed(sk1_set.secret_key(), old_section_auth.clone())?;
    let old_section = Section::new(pk0, chain.clone(), section_signed_old_section_auth)?;

    // Create our node
    let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let section_key_share = create_section_key_share(&sk1_set, 0);
    let node = nodes.remove(0);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        old_section,
        Some(section_key_share),
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    // Create new `Section` as a successor to the previous one.
    let sk2_set = SecretKeySet::random();
    let sk2 = sk2_set.secret_key();
    let pk2 = sk2.public_key();
    let pk2_signature = sk1_set.secret_key().sign(bincode::serialize(&pk2)?);
    chain.insert(&pk1, pk2, pk2_signature)?;

    let old_node = nodes.remove(0);

    // Create the new `SectionAuthorityProvider` by replacing the last peer with a new one.
    let new_peer = create_peer(MIN_AGE);
    let new_section_auth = SectionAuthorityProvider::new(
        old_section_auth
            .peers()
            .take(old_section_auth.elder_count() - 1)
            .chain(iter::once(new_peer)),
        old_section_auth.prefix,
        sk2_set.public_keys(),
    );
    let new_section_elders: BTreeSet<_> = new_section_auth.names();
    let section_signed_new_section_auth = section_signed(sk2, new_section_auth)?;
    let new_section = Section::new(pk0, chain, section_signed_new_section_auth)?;

    // Create the `Sync` message containing the new `Section`.
    let wire_msg = WireMsg::single_src(
        &old_node,
        DstLocation::DirectAndUnrouted(*new_section.chain().last_key()),
        NodeMsg::Sync {
            section: new_section.clone(),
            network: Network::new(),
        },
        *new_section.chain().last_key(),
    )?;

    // Handle the wire_msg.
    let _ = dispatcher
        .handle_command(Command::HandleMessage {
            sender: old_node.addr,
            wire_msg,
        })
        .await?;

    // Verify our `Section` got updated.
    assert_matches!(
        event_rx.recv().await,
        Some(Event::EldersChanged { elders, .. }) => {
            assert_eq!(elders.key, pk2);
            assert!(elders.added.iter().all(|a| new_section_elders.contains(a)));
            assert!(elders.remaining.iter().all(|a| new_section_elders.contains(a)));
            assert!(elders.removed.iter().all(|r| !new_section_elders.contains(r)));
        }
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_untrusted_sync() -> Result<()> {
    let sk0 = bls::SecretKey::random();
    let pk0 = sk0.public_key();

    let sk1 = bls::SecretKey::random();
    let pk1 = sk1.public_key();
    let sig1 = sk0.sign(&bincode::serialize(&pk1)?);

    let sk2 = bls::SecretKey::random();
    let pk2 = sk2.public_key();
    let sig2 = sk1.sign(&bincode::serialize(&pk2)?);

    let mut chain = SecuredLinkedList::new(pk0);
    chain.insert(&pk0, pk1, sig1)?;
    chain.insert(&pk1, pk2, sig2)?;

    let (old_section_auth, _) = create_section_auth();
    let section_signed_old_section_auth = section_signed(&sk0, old_section_auth.clone())?;
    let old_section = Section::new(
        pk0,
        SecuredLinkedList::new(pk0),
        section_signed_old_section_auth,
    )?;

    let (new_section_auth, _) = create_section_auth();
    let section_signed_new_section_auth = section_signed(&sk2, new_section_auth.clone())?;
    let new_section = Section::new(pk0, chain.truncate(2), section_signed_new_section_auth)?;

    let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let node = create_node(MIN_ADULT_AGE, None);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        old_section,
        None,
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let sender = create_node(MIN_ADULT_AGE, None);
    let original_node_msg = NodeMsg::Sync {
        section: new_section.clone(),
        network: Network::new(),
    };
    let wire_msg = WireMsg::single_src(
        &sender,
        DstLocation::DirectAndUnrouted(*new_section.chain().last_key()),
        original_node_msg.clone(),
        *new_section.chain().last_key(),
    )?;

    let commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: sender.addr,
            wire_msg,
        })
        .await?;

    let mut bounce_sent = false;

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        match wire_msg.into_message() {
            Ok(MessageType::Node {
                msg: NodeMsg::BouncedUntrustedMessage { msg, .. },
                ..
            }) => {
                assert_eq!(*msg, original_node_msg);
                assert_eq!(recipients, [(sender.name(), sender.addr)]);
                bounce_sent = true;
            }
            _ => continue,
        }
    }

    assert!(bounce_sent);
    assert!(timeout(Duration::from_secs(5), event_rx.recv())
        .await
        .is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_bounced_untrusted_sync() -> Result<()> {
    let sk0 = bls::SecretKey::random();
    let pk0 = sk0.public_key();

    let sk1 = bls::SecretKey::random();
    let pk1 = sk1.public_key();
    let sig1 = sk0.sign(&bincode::serialize(&pk1)?);

    let sk2_set = SecretKeySet::random();
    let sk2 = sk2_set.secret_key();
    let pk2 = sk2.public_key();
    let sig2 = sk1.sign(&bincode::serialize(&pk2)?);

    let mut chain = SecuredLinkedList::new(pk0);
    chain.insert(&pk0, pk1, sig1)?;
    chain.insert(&pk1, pk2, sig2)?;

    let (section_auth, mut nodes) = create_section_auth();
    let section_signed_section_auth = section_signed(sk2, section_auth.clone())?;
    let section_full = Section::new(pk0, chain, section_signed_section_auth)?;

    let (event_tx, _) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let node = nodes.remove(0);
    let section_key_share = create_section_key_share(&sk2_set, 0);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node.clone(),
        section_full.clone(),
        Some(section_key_share),
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let original_node_msg = NodeMsg::Sync {
        section: section_full.clone(),
        network: Network::new(),
    };

    let sender = create_node(MIN_ADULT_AGE, None);
    let bounced_node_msg = NodeMsg::BouncedUntrustedMessage {
        msg: Box::new(original_node_msg),
        dst_section_pk: pk0,
    };
    let bounced_wire_msg = WireMsg::single_src(
        &sender,
        DstLocation::Node {
            name: node.name(),
            section_pk: *section_full.chain().last_key(),
        },
        bounced_node_msg,
        bls::SecretKey::random().public_key(),
    )?;

    let commands = dispatcher
        .handle_command(Command::HandleMessage {
            sender: sender.addr,
            wire_msg: bounced_wire_msg,
        })
        .await?;

    let mut message_resent = false;

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        match wire_msg.into_message() {
            Ok(MessageType::Node {
                msg: NodeMsg::Sync { section, .. },
                ..
            }) => {
                assert_eq!(recipients, [(sender.name(), sender.addr)]);
                assert!(section.chain().has_key(&pk0));
                message_resent = true;
            }
            _ => continue,
        }
    }

    assert!(message_resent);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn relocation_of_non_elder() -> Result<()> {
    relocation(RelocatedPeerRole::NonElder).await
}

const THRESHOLD: usize = supermajority(ELDER_SIZE) - 1;

#[allow(dead_code)]
enum RelocatedPeerRole {
    NonElder,
    Elder,
}

async fn relocation(relocated_peer_role: RelocatedPeerRole) -> Result<()> {
    let sk_set = SecretKeySet::random();

    let prefix: Prefix = "0".parse().unwrap();
    let (section_auth, mut nodes, _) = gen_section_authority_provider(prefix, ELDER_SIZE);
    let (mut section, section_key_share) = create_section(&sk_set, &section_auth)?;

    let non_elder_peer = create_peer(MIN_AGE);
    let node_state = NodeState::joined(non_elder_peer, None);
    let node_state = section_signed(sk_set.secret_key(), node_state)?;
    assert!(section.update_member(node_state));
    let node = nodes.remove(0);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section,
        Some(section_key_share),
        mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let relocated_peer = match relocated_peer_role {
        RelocatedPeerRole::Elder => section_auth.peers().nth(1).expect("too few elders"),
        RelocatedPeerRole::NonElder => non_elder_peer,
    };

    let (proposal, sig) = create_relocation_trigger(sk_set.secret_key(), relocated_peer.age())?;
    let commands = dispatcher
        .handle_command(Command::HandleAgreement { proposal, sig })
        .await?;

    let mut relocate_sent = false;

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        if recipients
            .into_iter()
            .map(|recp| recp.1)
            .collect::<Vec<_>>()
            != [*relocated_peer.addr()]
        {
            continue;
        }
        match relocated_peer_role {
            RelocatedPeerRole::NonElder => {
                if let Ok(MessageType::Node {
                    msg: NodeMsg::Relocate(details),
                    ..
                }) = wire_msg.into_message()
                {
                    assert_eq!(details.pub_id, *relocated_peer.name());
                    assert_eq!(details.age, relocated_peer.age() + 1);
                } else {
                    continue;
                }
            }
            RelocatedPeerRole::Elder => {
                if let Ok(MessageType::Node {
                    msg: NodeMsg::RelocatePromise(promise),
                    ..
                }) = wire_msg.into_message()
                {
                    assert_eq!(promise.name, *relocated_peer.name());
                } else {
                    continue;
                }
            }
        }

        relocate_sent = true;
    }

    assert!(relocate_sent);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_message_to_self() -> Result<()> {
    message_to_self(MessageDst::Node).await
}

#[tokio::test(flavor = "multi_thread")]
async fn section_message_to_self() -> Result<()> {
    message_to_self(MessageDst::Section).await
}

enum MessageDst {
    Node,
    Section,
}

async fn message_to_self(dst: MessageDst) -> Result<()> {
    let node = create_node(MIN_ADULT_AGE, None);
    let (event_tx, _) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let (comm_tx, mut comm_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let comm = Comm::new(
        qp2p::Config {
            local_ip: Some(Ipv4Addr::LOCALHOST.into()),
            ..Default::default()
        },
        comm_tx,
    )
    .await?;
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;

    let core = Core::first_node(comm, node, event_tx, used_space, root_storage_dir)?;
    let node = core.node().clone();
    let section_pk = *core.section_chain().last_key();
    let dispatcher = Dispatcher::new(core);

    let dst_location = match dst {
        MessageDst::Node => DstLocation::Node {
            name: node.name(),
            section_pk,
        },
        MessageDst::Section => DstLocation::Section {
            name: node.name(),
            section_pk,
        },
    };

    let node_msg = NodeMsg::NodeMsgError {
        error: crate::messaging::data::Error::FailedToWriteFile,
        correlation_id: MessageId::new(),
    };
    let wire_msg = WireMsg::single_src(&node, dst_location, node_msg.clone(), section_pk)?;

    let commands = dispatcher
        .handle_command(Command::SendMessage {
            recipients: vec![(node.name(), node.addr)],
            wire_msg,
        })
        .await?;

    assert!(commands.is_empty());

    let msg_type = assert_matches!(comm_rx.recv().await, Some(ConnectionEvent::Received((sender, bytes))) => {
        assert_eq!(sender, node.addr);
        assert_matches!(WireMsg::deserialize(bytes), Ok(msg_type) => msg_type)
    });

    assert_matches!(msg_type, MessageType::Node { msg, dst_location: dst, .. } => {
        assert_eq!(dst, dst_location);
        assert_eq!(
            msg,
            node_msg
        );
    });

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_elders_update() -> Result<()> {
    // Start with section that has `ELDER_SIZE` elders with age 6, 1 non-elder with age 5 and one
    // to-be-elder with age 7:
    let node = create_node(MIN_AGE + 2, None);
    let mut other_elder_peers: Vec<_> = iter::repeat_with(|| create_peer(MIN_AGE + 2))
        .take(ELDER_SIZE - 1)
        .collect();
    let adult_peer = create_peer(MIN_ADULT_AGE);
    let promoted_peer = create_peer(MIN_AGE + 3);

    let sk_set0 = SecretKeySet::random();
    let pk0 = sk_set0.secret_key().public_key();

    let section_auth0 = SectionAuthorityProvider::new(
        iter::once(node.peer()).chain(other_elder_peers.clone()),
        Prefix::default(),
        sk_set0.public_keys(),
    );

    let (mut section0, section_key_share) = create_section(&sk_set0, &section_auth0)?;

    for peer in &[adult_peer, promoted_peer] {
        let node_state = NodeState::joined(*peer, None);
        let node_state = section_signed(sk_set0.secret_key(), node_state)?;
        assert!(section0.update_member(node_state));
    }

    let demoted_peer = other_elder_peers.remove(0);

    let sk_set1 = SecretKeySet::random();
    let pk1 = sk_set1.secret_key().public_key();
    // Create `HandleAgreement` command for an `OurElders` proposal. This will demote one of the
    // current elders and promote the oldest peer.
    let section_auth1 = SectionAuthorityProvider::new(
        iter::once(node.peer())
            .chain(other_elder_peers.clone())
            .chain(iter::once(promoted_peer)),
        Prefix::default(),
        sk_set1.public_keys(),
    );
    let elder_names1: BTreeSet<_> = section_auth1.names();

    let section_signed_section_auth1 = section_signed(sk_set1.secret_key(), section_auth1)?;
    let proposal = Proposal::OurElders(section_signed_section_auth1);
    let signature = sk_set0
        .secret_key()
        .sign(&bincode::serialize(&proposal.as_signable())?);
    let sig = KeyedSig {
        signature,
        public_key: pk0,
    };

    let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section0.clone(),
        Some(section_key_share),
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let commands = dispatcher
        .handle_command(Command::HandleAgreement { proposal, sig })
        .await?;

    let mut sync_actual_recipients = HashSet::new();

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        let (section, msg_authority) = match wire_msg.into_message() {
            Ok(MessageType::Node {
                msg: NodeMsg::Sync { section, .. },
                msg_authority,
                ..
            }) => (section, msg_authority),
            _ => continue,
        };

        assert_eq!(section.chain().last_key(), &pk1);

        // The message is trusted even by peers who don't yet know the new section key.
        assert!(msg_authority.verify_src_section_key(&[pk0]));

        // Merging the section contained in the message with the original section succeeds.
        assert_matches!(section0.clone().merge(section.clone()), Ok(()));

        sync_actual_recipients.extend(recipients);
    }

    let sync_expected_recipients: HashSet<_> = other_elder_peers
        .into_iter()
        .map(|peer| (*peer.name(), *peer.addr()))
        .chain(iter::once((*promoted_peer.name(), *promoted_peer.addr())))
        .chain(iter::once((*demoted_peer.name(), *demoted_peer.addr())))
        .chain(iter::once((*adult_peer.name(), *adult_peer.addr())))
        .collect();

    assert_eq!(sync_actual_recipients, sync_expected_recipients);

    assert_matches!(
        event_rx.recv().await,
        Some(Event::EldersChanged { elders, .. }) => {
            assert_eq!(elders.key, pk1);
            assert_eq!(elder_names1, elders.added.union(&elders.remaining).copied().collect());
            assert!(elders.removed.iter().all(|r| !elder_names1.contains(r)));
        }
    );

    Ok(())
}

// Test that demoted node still sends `Sync` messages on split.
#[tokio::test(flavor = "multi_thread")]
async fn handle_demote_during_split() -> Result<()> {
    let node = create_node(MIN_ADULT_AGE, None);
    let node_name = node.name();

    let prefix0 = Prefix::default().pushed(false);
    let prefix1 = Prefix::default().pushed(true);

    // These peers together with `node` are pre-split elders.
    // These peers together with `peer_c` are prefix-0 post-split elders.
    let peers_a: Vec<_> = iter::repeat_with(|| create_peer_in_prefix(&prefix0, MIN_ADULT_AGE))
        .take(ELDER_SIZE - 1)
        .collect();
    // These peers are prefix-1 post-split elders.
    let peers_b: Vec<_> = iter::repeat_with(|| create_peer_in_prefix(&prefix1, MIN_ADULT_AGE))
        .take(ELDER_SIZE)
        .collect();
    // This peer is a prefix-0 post-split elder.
    let peer_c = create_peer_in_prefix(&prefix0, MIN_ADULT_AGE);

    // Create the pre-split section
    let sk_set_v0 = SecretKeySet::random();
    let section_auth_v0 = SectionAuthorityProvider::new(
        iter::once(node.peer()).chain(peers_a.iter().copied()),
        Prefix::default(),
        sk_set_v0.public_keys(),
    );

    let (mut section, section_key_share) = create_section(&sk_set_v0, &section_auth_v0)?;

    for peer in peers_b.iter().chain(iter::once(&peer_c)) {
        let node_state = NodeState::joined(*peer, None);
        let node_state = section_signed(sk_set_v0.secret_key(), node_state)?;
        assert!(section.update_member(node_state));
    }

    let (event_tx, _) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
    let core = Core::new(
        create_comm().await?,
        node,
        section,
        Some(section_key_share),
        event_tx,
        used_space,
        root_storage_dir,
    )?;
    let dispatcher = Dispatcher::new(core);

    let sk_set_v1_p0 = SecretKeySet::random();
    let sk_set_v1_p1 = SecretKeySet::random();

    // Create agreement on `OurElder` for both sub-sections
    let create_our_elders_command = |sk, section_auth| -> Result<_> {
        let section_signed_section_auth = section_signed(sk, section_auth)?;
        let proposal = Proposal::OurElders(section_signed_section_auth);
        let signature = sk_set_v0
            .secret_key()
            .sign(&bincode::serialize(&proposal.as_signable())?);
        let sig = KeyedSig {
            signature,
            public_key: sk_set_v0.secret_key().public_key(),
        };

        Ok(Command::HandleAgreement { proposal, sig })
    };

    // Handle agreement on `OurElders` for prefix-0.
    let section_auth = SectionAuthorityProvider::new(
        peers_a.iter().copied().chain(iter::once(peer_c)),
        prefix0,
        sk_set_v1_p0.public_keys(),
    );
    let command = create_our_elders_command(sk_set_v1_p0.secret_key(), section_auth)?;
    let commands = dispatcher.handle_command(command).await?;
    assert_matches!(&commands[..], &[]);

    // Handle agreement on `OurElders` for prefix-1.
    let section_auth =
        SectionAuthorityProvider::new(peers_b.iter().copied(), prefix1, sk_set_v1_p1.public_keys());
    let command = create_our_elders_command(sk_set_v1_p1.secret_key(), section_auth)?;
    let commands = dispatcher.handle_command(command).await?;

    let mut sync_recipients = HashSet::new();

    for command in commands {
        let (recipients, wire_msg) = match command {
            Command::SendMessage {
                recipients,
                wire_msg,
                ..
            } => (recipients, wire_msg),
            _ => continue,
        };

        if matches!(
            wire_msg.into_message(),
            Ok(MessageType::Node {
                msg: NodeMsg::Sync { .. },
                ..
            })
        ) {
            sync_recipients.extend(recipients);
        }
    }

    let expected_sync_recipients = if prefix0.matches(&node_name) {
        peers_a
            .iter()
            .map(|peer| (*peer.name(), *peer.addr()))
            .chain(iter::once((*peer_c.name(), *peer_c.addr())))
            .collect()
    } else {
        peers_b
            .iter()
            .map(|peer| (*peer.name(), *peer.addr()))
            .collect()
    };

    assert_eq!(sync_recipients, expected_sync_recipients);

    Ok(())
}

fn create_peer(age: u8) -> Peer {
    let name = ed25519::gen_name_with_age(age);
    let mut peer = Peer::new(name, gen_addr());
    peer.set_reachable(true);
    peer
}

fn create_peer_in_prefix(prefix: &Prefix, age: u8) -> Peer {
    let name = ed25519::gen_name_with_age(age);
    let mut peer = Peer::new(prefix.substituted_in(name), gen_addr());
    peer.set_reachable(true);
    peer
}

fn create_node(age: u8, prefix: Option<Prefix>) -> Node {
    Node::new(
        ed25519::gen_keypair(&prefix.unwrap_or_default().range_inclusive(), age),
        gen_addr(),
    )
}

async fn create_comm() -> Result<Comm> {
    let (tx, _rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    Ok(Comm::new(
        qp2p::Config {
            local_ip: Some(Ipv4Addr::LOCALHOST.into()),
            ..Default::default()
        },
        tx,
    )
    .await?)
}

// Generate random SectionAuthorityProvider and the corresponding Nodes.
fn create_section_auth() -> (SectionAuthorityProvider, Vec<Node>) {
    let (section_auth, elders, _) = gen_section_authority_provider(Prefix::default(), ELDER_SIZE);
    (section_auth, elders)
}

fn create_section_key_share(sk_set: &bls::SecretKeySet, index: usize) -> SectionKeyShare {
    SectionKeyShare {
        public_key_set: sk_set.public_keys(),
        index,
        secret_key_share: sk_set.secret_key_share(index),
    }
}

fn create_section(
    sk_set: &SecretKeySet,
    section_auth: &SectionAuthorityProvider,
) -> Result<(Section, SectionKeyShare)> {
    let section_chain = SecuredLinkedList::new(sk_set.secret_key().public_key());
    let section_signed_section_auth = section_signed(sk_set.secret_key(), section_auth.clone())?;

    let mut section = Section::new(
        *section_chain.root_key(),
        section_chain,
        section_signed_section_auth,
    )?;

    for peer in section_auth.peers() {
        let mut peer = peer;
        peer.set_reachable(true);
        let node_state = NodeState::joined(peer, None);
        let node_state = section_signed(sk_set.secret_key(), node_state)?;
        let _ = section.update_member(node_state);
    }

    let section_key_share = create_section_key_share(sk_set, 0);

    Ok((section, section_key_share))
}

// Create a `Proposal::Online` whose agreement handling triggers relocation of a node with the
// given age.
// NOTE: recommended to call this with low `age` (4 or 5), otherwise it might take very long time
// to complete because it needs to generate a signature with the number of trailing zeroes equal to
// (or greater that) `age`.
fn create_relocation_trigger(sk: &bls::SecretKey, age: u8) -> Result<(Proposal, KeyedSig)> {
    loop {
        let proposal = Proposal::Online {
            node_state: NodeState::joined(create_peer(MIN_ADULT_AGE), Some(rand::random())),
            dst_key: None,
        };

        let signature = sk.sign(&bincode::serialize(&proposal.as_signable())?);

        if relocation::check(age, &signature) && !relocation::check(age + 1, &signature) {
            let sig = KeyedSig {
                public_key: sk.public_key(),
                signature,
            };

            return Ok((proposal, sig));
        }
    }
}

// Wrapper for `bls::SecretKeySet` that also allows to retrieve the corresponding `bls::SecretKey`.
// Note: `bls::SecretKeySet` does have a `secret_key` method, but it's test-only and not available
// for the consumers of the crate.
pub(crate) struct SecretKeySet {
    set: bls::SecretKeySet,
    key: bls::SecretKey,
}

impl SecretKeySet {
    pub(crate) fn random() -> Self {
        let poly = bls::poly::Poly::random(THRESHOLD, &mut rand::thread_rng());
        let key = bls::SecretKey::from_mut(&mut poly.evaluate(0));
        let set = bls::SecretKeySet::from(poly);

        Self { set, key }
    }

    pub(crate) fn secret_key(&self) -> &bls::SecretKey {
        &self.key
    }
}

impl Deref for SecretKeySet {
    type Target = bls::SecretKeySet;

    fn deref(&self) -> &Self::Target {
        &self.set
    }
}
