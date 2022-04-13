// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#![allow(dead_code, unused_imports)]

use super::{Cmd, Comm};

use crate::dbs::UsedSpace;
use crate::messaging::{
    system::{
        JoinAsRelocatedRequest, JoinRequest, JoinResponse, KeyedSig, MembershipState,
        NodeState as NodeStateMsg, RelocateDetails, ResourceProofResponse, SectionAuth, SystemMsg,
    },
    AuthorityProof, DstLocation, MsgId, MsgKind, MsgType, NodeAuth,
    SectionAuth as MsgKindSectionAuth, WireMsg,
};
use crate::node::{
    core::{
        relocation_check, ChurnId, Measurements, MsgEvent, Node, Proposal,
        RESOURCE_PROOF_DATA_SIZE, RESOURCE_PROOF_DIFFICULTY,
    },
    create_test_max_capacity_and_root_storage,
    dkg::test_utils::{prove, section_signed},
    ed25519,
    messages::{NodeMsgAuthorityUtils, WireMsgUtils},
    network_knowledge::{
        test_utils::*, NetworkKnowledge, NodeState, SectionAuthorityProvider, SectionKeyShare,
    },
    recommended_section_size, supermajority, Error, Event, NodeInfo, Result as RoutingResult,
    FIRST_SECTION_MAX_AGE, FIRST_SECTION_MIN_AGE, MIN_ADULT_AGE,
};
use crate::types::{Keypair, Peer, PublicKey};
use crate::{elder_count, init_test_logger};

use assert_matches::assert_matches;
use bls_dkg::message::Message;
use ed25519_dalek::Signer;
use eyre::{bail, eyre, Context, Result};
use itertools::Itertools;
use rand::{distributions::Alphanumeric, rngs::OsRng, Rng};
use resource_proof::ResourceProof;
use secured_linked_list::SecuredLinkedList;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    iter,
    net::Ipv4Addr,
    ops::Deref,
    path::Path,
};
use tempfile::tempdir;
use tokio::{
    sync::mpsc,
    time::{timeout, Duration},
};
use xor_name::{Prefix, XorName};

static TEST_EVENT_CHANNEL_SIZE: usize = 20;

// #[tokio::test(flavor = "multi_thread")]
// async fn receive_join_request_without_resource_proof_response() -> Result<()> {
//     let prefix1 = Prefix::default().pushed(true);
//     let (section_auth, mut nodes, sk_set) = gen_section_authority_provider(prefix1, elder_count());

//     let pk_set = sk_set.public_keys();
//     let section_key = pk_set.public_key();

//     let (section, section_key_share) = create_section(&sk_set, &section_auth).await?;
//     let node = nodes.remove(0);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         section,
//         Some(section_key_share),
//         mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     let new_node_comm = create_comm().await?;
//     let new_node = NodeInfo::new(
//         ed25519::gen_keypair(&prefix1.range_inclusive(), MIN_ADULT_AGE),
//         new_node_comm.our_connection_info(),
//     );

//     let wire_msg = WireMsg::single_src(
//         &new_node,
//         DstLocation::Section {
//             name: XorName::from(PublicKey::Bls(section_key)),
//             section_pk: section_key,
//         },
//         SystemMsg::JoinRequest(Box::new(JoinRequest {
//             section_key,
//             resource_proof_response: None,
//             aggregated: None,
//         })),
//         section_key,
//     )?;

//     let mut cmds = dispatcher
//         .process_cmd(
//             Cmd::HandleMsg {
//                 sender: new_node.peer(),
//                 wire_msg,
//                 original_bytes: None,
//             },
//             "cmd-id",
//         )
//         .await?
//         .into_iter();

//     let mut next_cmd = cmds.next();

//     if !matches!(next_cmd, Some(Cmd::SendMsg { .. })) {
//         next_cmd = cmds.next();
//     }

//     let response_wire_msg = assert_matches!(
//         // we want to check the cmd _after_ that
//         next_cmd,
//         Some(Cmd::SendMsg {
//             wire_msg,
//             ..
//         }) => wire_msg
//     );

//     assert_matches!(
//         response_wire_msg.into_msg(),
//         Ok(MsgType::System {
//             msg: SystemMsg::JoinResponse(response),
//             ..
//         }) => assert_matches!(*response, JoinResponse::ResourceChallenge { .. })
//     );

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn receive_join_request_with_resource_proof_response() -> Result<()> {
//     let prefix1 = Prefix::default().pushed(true);
//     let (section_auth, mut nodes, sk_set) = gen_section_authority_provider(prefix1, elder_count());

//     let pk_set = sk_set.public_keys();
//     let section_key = pk_set.public_key();

//     let (section, section_key_share) = create_section(&sk_set, &section_auth).await?;
//     let node = nodes.remove(0);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         section,
//         Some(section_key_share),
//         mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     let new_node = NodeInfo::new(
//         ed25519::gen_keypair(&prefix1.range_inclusive(), MIN_ADULT_AGE),
//         gen_addr(),
//     );

//     let nonce: [u8; 32] = rand::random();
//     let serialized = bincode::serialize(&(new_node.name(), nonce))?;
//     let nonce_signature = ed25519::sign(&serialized, &dispatcher.node.info.read().await.keypair);

//     let rp = ResourceProof::new(RESOURCE_PROOF_DATA_SIZE, RESOURCE_PROOF_DIFFICULTY);
//     let data = rp.create_proof_data(&nonce);
//     let mut prover = rp.create_prover(data.clone());
//     let solution = prover.solve();

//     let node_state = NodeState::joined(new_node.peer(), None);
//     let auth = section_signed(sk_set.secret_key(), node_state.to_msg())?;

//     let wire_msg = WireMsg::single_src(
//         &new_node,
//         DstLocation::Section {
//             name: XorName::from(PublicKey::Bls(section_key)),
//             section_pk: section_key,
//         },
//         SystemMsg::JoinRequest(Box::new(JoinRequest {
//             section_key,
//             resource_proof_response: Some(ResourceProofResponse {
//                 solution,
//                 data,
//                 nonce,
//                 nonce_signature,
//             }),
//             aggregated: Some(auth.clone()),
//         })),
//         section_key,
//     )?;

//     let cmds = dispatcher
//         .process_cmd(
//             Cmd::HandleMsg {
//                 sender: new_node.peer(),
//                 wire_msg,
//                 original_bytes: None,
//             },
//             "cmd-id",
//         )
//         .await?;

//     let mut test_connectivity = false;
//     for cmd in cmds {
//         if let Cmd::HandleNewNodeOnline(response) = cmd {
//             assert_eq!(response.value.name, new_node.name());
//             assert_eq!(response.value.addr, new_node.addr);
//             assert_eq!(response.value.age(), MIN_ADULT_AGE);

//             test_connectivity = true;
//         }
//     }

//     assert!(test_connectivity);

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn receive_join_request_from_relocated_node() -> Result<()> {
//     init_test_logger();
//     let _span = tracing::info_span!("receive_join_request_from_relocated_node").entered();

//     let (section_auth, mut nodes, sk_set) = create_section_auth();

//     let pk_set = sk_set.public_keys();
//     let section_key = pk_set.public_key();

//     let (section, section_key_share) = create_section(&sk_set, &section_auth).await?;
//     let node = nodes.remove(0);
//     let relocated_node_old_name = node.name();
//     let relocated_node_old_keypair = node.keypair.clone();
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         section,
//         Some(section_key_share),
//         mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     let relocated_node = NodeInfo::new(
//         ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE + 1),
//         gen_addr(),
//     );

//     let relocate_details = RelocateDetails {
//         previous_name: relocated_node_old_name,
//         dst: relocated_node_old_name,
//         dst_section_key: section_key,
//         age: relocated_node.age(),
//     };

//     let node_state = NodeState::relocated(
//         relocated_node.peer(),
//         Some(relocated_node_old_name),
//         relocate_details,
//     );
//     let relocate_proof = section_signed(sk_set.secret_key(), node_state.to_msg())?;

//     let signature_over_new_name =
//         ed25519::sign(&relocated_node.name().0, &relocated_node_old_keypair);

//     let wire_msg = WireMsg::single_src(
//         &relocated_node,
//         DstLocation::Section {
//             name: XorName::from(PublicKey::Bls(section_key)),
//             section_pk: section_key,
//         },
//         SystemMsg::JoinAsRelocatedRequest(Box::new(JoinAsRelocatedRequest {
//             section_key,
//             relocate_proof,
//             signature_over_new_name,
//         })),
//         section_key,
//     )?;

//     let mut propose_cmd_returned = false;

//     let inner_cmds = dispatcher
//         .process_cmd(
//             Cmd::HandleMsg {
//                 sender: relocated_node.peer(),
//                 wire_msg,
//                 original_bytes: None,
//             },
//             "cmd-id",
//         )
//         .await?;

//     for cmd in inner_cmds {
//         // third pass should now be handled and return propose
//         if let Cmd::SendAcceptedOnlineShare {
//             peer,
//             previous_name,
//         } = cmd
//         {
//             assert_eq!(peer, relocated_node.peer());
//             assert_eq!(previous_name, Some(relocated_node_old_name));

//             propose_cmd_returned = true;
//         }
//     }

//     assert!(propose_cmd_returned);

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_agreement_on_online() -> Result<()> {
//     let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);

//     let prefix = Prefix::default();

//     let (section_auth, mut nodes, sk_set) = gen_section_authority_provider(prefix, elder_count());
//     let (section, section_key_share) = create_section(&sk_set, &section_auth).await?;
//     let node = nodes.remove(0);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         section,
//         Some(section_key_share),
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     let new_peer = create_peer(MIN_ADULT_AGE);

//     let status = handle_online_cmd(&new_peer, &sk_set, &dispatcher, &section_auth).await?;
//     assert!(status.node_approval_sent);

//     assert_matches!(event_rx.recv().await, Some(Event::MemberJoined { name, age, .. }) => {
//         assert_eq!(name, new_peer.name());
//         assert_eq!(age, MIN_ADULT_AGE);
//     });

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_agreement_on_online_of_elder_candidate() -> Result<()> {
//     let sk_set = SecretKeySet::random();
//     let chain = SecuredLinkedList::new(sk_set.secret_key().public_key());

//     // Creates nodes where everybody has age 6 except one has 5.
//     let mut nodes: Vec<_> = gen_sorted_nodes(&Prefix::default(), elder_count(), true);

//     let section_auth = SectionAuthorityProvider::new(
//         nodes.iter().map(NodeInfo::peer),
//         Prefix::default(),
//         sk_set.public_keys(),
//     );
//     let signed_sap = section_signed(sk_set.secret_key(), section_auth.clone())?;

//     let section = NetworkKnowledge::new(*chain.root_key(), chain, signed_sap, None)?;
//     let mut expected_new_elders = BTreeSet::new();

//     for peer in section_auth.elders() {
//         let node_state = NodeState::joined(*peer, None);
//         let sig = prove(sk_set.secret_key(), &node_state)?;
//         let _updated = section
//             .update_member(SectionAuth {
//                 value: node_state,
//                 sig,
//             })
//             .await;
//         if peer.age() == MIN_ADULT_AGE + 1 {
//             let _changed = expected_new_elders.insert(peer);
//         }
//     }

//     let node = nodes.remove(0);
//     let node_name = node.name();
//     let section_key_share = create_section_key_share(&sk_set, 0);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         section,
//         Some(section_key_share),
//         mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     // Handle agreement on Online of a peer that is older than the youngest
//     // current elder - that means this peer is going to be promoted.
//     let new_peer = create_peer(MIN_ADULT_AGE + 1);
//     let node_state = NodeState::joined(new_peer, Some(xor_name::rand::random()));

//     let auth = section_signed(sk_set.secret_key(), node_state.to_msg())?;

//     let cmds = dispatcher
//         .process_cmd(Cmd::HandleNewNodeOnline(auth), "cmd-id")
//         .await?;

//     // Verify we sent a `DkgStart` message with the expected participants.
//     let mut dkg_start_sent = false;
//     let _changed = expected_new_elders.insert(&new_peer);

//     for cmd in cmds {
//         let (recipients, wire_msg) = match cmd {
//             Cmd::SendMsg {
//                 recipients,
//                 wire_msg,
//                 ..
//             } => (recipients, wire_msg),
//             _ => continue,
//         };

//         let actual_elder_candidates = match wire_msg.into_msg() {
//             Ok(MsgType::System {
//                 msg: SystemMsg::DkgStart { elders, .. },
//                 ..
//             }) => elders.into_iter().map(|(name, addr)| Peer::new(name, addr)),
//             _ => continue,
//         };
//         itertools::assert_equal(actual_elder_candidates, expected_new_elders.clone());

//         let expected_dkg_start_recipients: Vec<_> = expected_new_elders
//             .iter()
//             .filter(|peer| peer.name() != node_name)
//             .cloned()
//             .collect();
//         assert_eq!(recipients, expected_dkg_start_recipients);

//         dkg_start_sent = true;
//     }

//     assert!(dkg_start_sent);

//     Ok(())
// }

// // Handles a consensus-ed Online proposal.
// async fn handle_online_cmd(
//     peer: &Peer,
//     sk_set: &SecretKeySet,
//     dispatcher: &Dispatcher,
//     section_auth: &SectionAuthorityProvider,
// ) -> Result<HandleOnlineStatus> {
//     let node_state = NodeState::joined(*peer, None);
//     let auth = section_signed(sk_set.secret_key(), node_state.to_msg())?;

//     let cmds = dispatcher
//         .process_cmd(Cmd::HandleNewNodeOnline(auth), "cmd-id")
//         .await?;

//     let mut status = HandleOnlineStatus {
//         node_approval_sent: false,
//         relocate_details: None,
//     };

//     for cmd in cmds {
//         let (recipients, wire_msg) = match cmd {
//             Cmd::SendMsg {
//                 recipients,
//                 wire_msg,
//                 ..
//             } => (recipients, wire_msg),
//             _ => continue,
//         };

//         match wire_msg.into_msg() {
//             Ok(MsgType::System {
//                 msg: SystemMsg::JoinResponse(response),
//                 ..
//             }) => {
//                 if let JoinResponse::Approval {
//                     section_auth: signed_sap,
//                     ..
//                 } = *response
//                 {
//                     assert_eq!(signed_sap.value, section_auth.clone().to_msg());
//                     assert_eq!(recipients, [peer]);
//                     status.node_approval_sent = true;
//                 }
//             }
//             Ok(MsgType::System {
//                 msg:
//                     SystemMsg::Propose {
//                         proposal: crate::messaging::system::Proposal::Offline(node_state),
//                         ..
//                     },
//                 ..
//             }) => {
//                 if let MembershipState::Relocated(details) = node_state.state {
//                     if details.previous_name != peer.name() {
//                         continue;
//                     }
//                     status.relocate_details = Some(*details.clone());
//                 }
//             }
//             _ => continue,
//         }
//     }

//     Ok(status)
// }

// struct HandleOnlineStatus {
//     node_approval_sent: bool,
//     relocate_details: Option<RelocateDetails>,
// }

// enum NetworkPhase {
//     Startup,
//     Regular,
// }

// async fn handle_agreement_on_online_of_rejoined_node(phase: NetworkPhase, age: u8) -> Result<()> {
//     let prefix = match phase {
//         NetworkPhase::Startup => Prefix::default(),
//         NetworkPhase::Regular => "0".parse().unwrap(),
//     };
//     let (section_auth, mut node_infos, sk_set) =
//         gen_section_authority_provider(prefix, elder_count());
//     let (section, section_key_share) = create_section(&sk_set, &section_auth).await?;

//     // Make a left peer.
//     let peer = create_peer(age);
//     let node_state = NodeState::left(peer, None);
//     let node_state = section_signed(sk_set.secret_key(), node_state)?;
//     let _updated = section.update_member(node_state).await;

//     // Make a Node
//     let (event_tx, _) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let info = node_infos.remove(0);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         info,
//         section,
//         Some(section_key_share),
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     // Simulate peer with the same name is rejoin and verify resulted behaviours.
//     let status = handle_online_cmd(&peer, &sk_set, &dispatcher, &section_auth).await?;

//     // A rejoin node with low age will be rejected.
//     if age / 2 < MIN_ADULT_AGE {
//         assert!(!status.node_approval_sent);
//         assert!(status.relocate_details.is_none());
//         return Ok(());
//     }

//     assert!(status.node_approval_sent);
//     assert_matches!(status.relocate_details, Some(details) => {
//         assert_eq!(details.dst, peer.name());
//         assert_eq!(details.age, (age / 2).max(MIN_ADULT_AGE));
//     });

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_agreement_on_online_of_rejoined_node_with_high_age_in_startup() -> Result<()> {
//     handle_agreement_on_online_of_rejoined_node(NetworkPhase::Startup, 16).await
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_agreement_on_online_of_rejoined_node_with_high_age_after_startup() -> Result<()> {
//     handle_agreement_on_online_of_rejoined_node(NetworkPhase::Regular, 16).await
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_agreement_on_online_of_rejoined_node_with_low_age_in_startup() -> Result<()> {
//     handle_agreement_on_online_of_rejoined_node(NetworkPhase::Startup, 8).await
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_agreement_on_online_of_rejoined_node_with_low_age_after_startup() -> Result<()> {
//     handle_agreement_on_online_of_rejoined_node(NetworkPhase::Regular, 8).await
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_agreement_on_offline_of_non_elder() -> Result<()> {
//     init_test_logger();
//     let _span = tracing::info_span!("handle_agreement_on_offline_of_non_elder").entered();

//     let (section_auth, mut nodes, sk_set) = create_section_auth();

//     let (section, section_key_share) = create_section(&sk_set, &section_auth).await?;

//     let existing_peer = create_peer(MIN_ADULT_AGE);

//     let node_state = NodeState::joined(existing_peer, None);
//     let node_state = section_signed(sk_set.secret_key(), node_state)?;
//     let _updated = section.update_member(node_state).await;

//     let (event_tx, _event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let node = nodes.remove(0);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         section,
//         Some(section_key_share),
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     let node_state = NodeState::left(existing_peer, None);
//     let proposal = Proposal::Offline(node_state.clone());
//     let sig = keyed_signed(sk_set.secret_key(), &proposal.as_signable_bytes()?);

//     let _cmds = dispatcher
//         .process_cmd(Cmd::HandleAgreement { proposal, sig }, "cmd-id")
//         .await?;

//     assert!(!dispatcher
//         .node
//         .network_knowledge()
//         .section_members()
//         .await
//         .contains(&node_state));

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_agreement_on_offline_of_elder() -> Result<()> {
//     let (section_auth, mut nodes, sk_set) = create_section_auth();

//     let (section, section_key_share) = create_section(&sk_set, &section_auth).await?;

//     let existing_peer = create_peer(MIN_ADULT_AGE);
//     let node_state = NodeState::joined(existing_peer, None);
//     let node_state = section_signed(sk_set.secret_key(), node_state)?;
//     let _updated = section.update_member(node_state).await;

//     // Pick the elder to remove.
//     let auth_peers = section_auth.elders();
//     let remove_peer = auth_peers.last().expect("section_auth is empty");

//     let remove_node_state = section
//         .get_section_member(&remove_peer.name())
//         .await
//         .expect("member not found")
//         .leave()?;

//     // Create our node
//     let (event_tx, _event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = nodes.remove(0);
//     let node_name = node.name();
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         section,
//         Some(section_key_share),
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     // Handle agreement on the Offline proposal
//     let proposal = Proposal::Offline(remove_node_state.clone());
//     let sig = keyed_signed(sk_set.secret_key(), &proposal.as_signable_bytes()?);

//     let cmds = dispatcher
//         .process_cmd(Cmd::HandleAgreement { proposal, sig }, "cmd-id")
//         .await?;

//     // Verify we sent a `DkgStart` message with the expected participants.
//     let mut dkg_start_sent = false;

//     for cmd in cmds {
//         let (recipients, wire_msg) = match cmd {
//             Cmd::SendMsg {
//                 recipients,
//                 wire_msg,
//                 ..
//             } => (recipients, wire_msg),
//             _ => continue,
//         };

//         let actual_elder_candidates = match wire_msg.into_msg() {
//             Ok(MsgType::System {
//                 msg: SystemMsg::DkgStart { elders, .. },
//                 ..
//             }) => elders.into_iter().map(|(name, addr)| Peer::new(name, addr)),
//             _ => continue,
//         };

//         let expected_new_elders: BTreeSet<_> = section_auth
//             .elders()
//             .filter(|peer| peer != &remove_peer)
//             .chain(iter::once(&existing_peer))
//             .cloned()
//             .collect();
//         itertools::assert_equal(actual_elder_candidates, expected_new_elders.clone());

//         let expected_dkg_start_recipients: Vec<_> = expected_new_elders
//             .into_iter()
//             .filter(|peer| peer.name() != node_name)
//             .collect();

//         assert_eq!(recipients, expected_dkg_start_recipients);

//         dkg_start_sent = true;
//     }

//     assert!(dkg_start_sent);

//     assert!(!dispatcher
//         .node
//         .network_knowledge()
//         .section_members()
//         .await
//         .contains(&remove_node_state));

//     // The removed peer is still our elder because we haven't yet processed the section update.
//     assert!(dispatcher
//         .node
//         .network_knowledge()
//         .authority_provider()
//         .await
//         .contains_elder(&remove_peer.name()));

//     Ok(())
// }

// #[derive(PartialEq)]
// enum UntrustedMessageSource {
//     Peer,
//     Accumulation,
// }

// #[tokio::test(flavor = "multi_thread")]
// // Checking when we get AE info that is ahead of us we should handle it.
// async fn ae_msg_from_the_future_is_handled() -> Result<()> {
//     init_test_logger();
//     let _span = info_span!("ae_msg_from_the_future_is_handled").entered();

//     // Create first `Section` with a chain of length 2
//     let sk0 = bls::SecretKey::random();
//     let pk0 = sk0.public_key();

//     let (old_sap, mut nodes, sk_set1) = create_section_auth();
//     let pk1 = sk_set1.secret_key().public_key();
//     let pk1_signature = sk0.sign(bincode::serialize(&pk1)?);

//     let mut chain = SecuredLinkedList::new(pk0);
//     assert_eq!(chain.insert(&pk0, pk1, pk1_signature), Ok(()));

//     let signed_old_sap = section_signed(sk_set1.secret_key(), old_sap.clone())?;
//     let network_knowledge = NetworkKnowledge::new(pk0, chain.clone(), signed_old_sap, None)?;

//     // Create our node
//     let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let section_key_share = create_section_key_share(&sk_set1, 0);
//     let node = nodes.remove(0);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         network_knowledge,
//         Some(section_key_share),
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;

//     // Create new `Section` as a successor to the previous one.
//     let sk_set2 = SecretKeySet::random();
//     let sk2 = sk_set2.secret_key();
//     let pk2 = sk2.public_key();
//     let pk2_signature = sk_set1.secret_key().sign(bincode::serialize(&pk2)?);
//     chain.insert(&pk1, pk2, pk2_signature)?;

//     let old_node = nodes.remove(0);
//     let src_section_pk = pk2;

//     // Create the new `SectionAuthorityProvider` by replacing the last peer with a new one.
//     let new_peer = create_peer(MIN_ADULT_AGE);
//     let new_elders = old_sap
//         .elders()
//         .take(old_sap.elder_count() - 1)
//         .cloned()
//         .chain(vec![new_peer]);

//     let new_sap =
//         SectionAuthorityProvider::new(new_elders, old_sap.prefix(), sk_set2.public_keys());
//     let new_section_elders: BTreeSet<_> = new_sap.names();
//     let signed_new_sap = section_signed(sk2, new_sap.clone())?;

//     // Create the `Sync` message containing the new `Section`.
//     let wire_msg = WireMsg::single_src(
//         &old_node,
//         DstLocation::Node {
//             name: XorName::from(PublicKey::Bls(pk1)),
//             section_pk: pk1,
//         },
//         SystemMsg::AntiEntropyUpdate {
//             section_auth: new_sap.to_msg(),
//             members: BTreeSet::default(),
//             section_signed: signed_new_sap.sig,
//             proof_chain: chain,
//         },
//         src_section_pk,
//     )?;

//     // Simulate DKG round finished succesfully by adding
//     // the new section key share to our cache
//     node.section_keys_provider
//         .insert(create_section_key_share(&sk_set2, 0))
//         .await;

//     let dispatcher = Dispatcher::new(node);

//     let _cmds = dispatcher
//         .process_cmd(
//             Cmd::HandleMsg {
//                 sender: old_node.peer(),
//                 wire_msg,
//                 original_bytes: None,
//             },
//             "cmd-id",
//         )
//         .await?;

//     // Verify our `Section` got updated.
//     assert_matches!(
//         event_rx.recv().await,
//         Some(Event::EldersChanged { elders, .. }) => {
//             assert_eq!(elders.key, pk2);
//             assert!(elders.added.iter().all(|a| new_section_elders.contains(a)));
//             assert!(elders.remaining.iter().all(|a| new_section_elders.contains(a)));
//             assert!(elders.removed.iter().all(|r| !new_section_elders.contains(r)));
//         }
//     );

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// // Checking when we send AE info to a section from untrusted section, we do not handle it and error out
// async fn untrusted_ae_msg_errors() -> Result<()> {
//     init_test_logger();
//     let _span = tracing::info_span!("untrusted_ae_msg_errors").entered();

//     let (our_section_auth, _, sk_set0) = create_section_auth();
//     let sk0 = sk_set0.secret_key();
//     let pk0 = sk0.public_key();

//     let section_signed_our_section_auth = section_signed(sk0, our_section_auth.clone())?;
//     let our_section = NetworkKnowledge::new(
//         pk0,
//         SecuredLinkedList::new(pk0),
//         section_signed_our_section_auth.clone(),
//         None,
//     )?;

//     // a valid AE msg but with a non-verifiable SAP...
//     let bogus_section_pk = bls::SecretKey::random().public_key();
//     let node_msg = SystemMsg::AntiEntropyUpdate {
//         section_auth: section_signed_our_section_auth.value.clone().to_msg(),
//         section_signed: section_signed_our_section_auth.sig,
//         proof_chain: SecuredLinkedList::new(bogus_section_pk),
//         members: BTreeSet::default(),
//     };

//     let (event_tx, _) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let info = gen_info(MIN_ADULT_AGE, None);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         info,
//         our_section.clone(),
//         None,
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;

//     let dispatcher = Dispatcher::new(node);

//     let sender = gen_info(MIN_ADULT_AGE, None);
//     let wire_msg = WireMsg::single_src(
//         &sender,
//         DstLocation::Section {
//             name: XorName::from(PublicKey::Bls(bogus_section_pk)),
//             section_pk: bogus_section_pk,
//         },
//         node_msg.clone(),
//         // we use the nonsense here
//         bogus_section_pk,
//     )?;

//     let _cmds = dispatcher
//         .process_cmd(
//             Cmd::HandleMsg {
//                 sender: sender.peer(),
//                 wire_msg,
//                 original_bytes: None,
//             },
//             "cmd-id",
//         )
//         .await?;

//     assert_eq!(dispatcher.node.network_knowledge().genesis_key(), &pk0);
//     assert_eq!(
//         dispatcher.node.network_knowledge().prefix_map().all(),
//         vec![section_signed_our_section_auth.value]
//     );

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn relocation_of_non_elder() -> Result<()> {
//     relocation(RelocatedPeerRole::NonElder).await
// }

fn threshold() -> usize {
    supermajority(elder_count()) - 1
}

// #[allow(dead_code)]
// enum RelocatedPeerRole {
//     NonElder,
//     Elder,
// }

// async fn relocation(relocated_peer_role: RelocatedPeerRole) -> Result<()> {
//     let prefix: Prefix = "0".parse().unwrap();
//     let section_size = match relocated_peer_role {
//         RelocatedPeerRole::Elder => elder_count(),
//         RelocatedPeerRole::NonElder => recommended_section_size(),
//     };
//     let (section_auth, mut nodes, sk_set) = gen_section_authority_provider(prefix, elder_count());
//     let (section, section_key_share) = create_section(&sk_set, &section_auth).await?;

//     let mut adults = section_size - elder_count();
//     while adults > 0 {
//         adults -= 1;
//         let non_elder_peer = create_peer(MIN_ADULT_AGE);
//         let node_state = NodeState::joined(non_elder_peer, None);
//         let node_state = section_signed(sk_set.secret_key(), node_state)?;
//         assert!(section.update_member(node_state).await);
//     }

//     let non_elder_peer = create_peer(MIN_ADULT_AGE - 1);
//     let node_state = NodeState::joined(non_elder_peer, None);
//     let node_state = section_signed(sk_set.secret_key(), node_state)?;
//     assert!(section.update_member(node_state).await);
//     let node = nodes.remove(0);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         node,
//         section,
//         Some(section_key_share),
//         mpsc::channel(TEST_EVENT_CHANNEL_SIZE).0,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;
//     let dispatcher = Dispatcher::new(node);

//     let relocated_peer = match relocated_peer_role {
//         RelocatedPeerRole::Elder => *section_auth.elders().nth(1).expect("too few elders"),
//         RelocatedPeerRole::NonElder => non_elder_peer,
//     };

//     let auth = create_relocation_trigger(sk_set.secret_key(), relocated_peer.age())?;
//     let cmds = dispatcher
//         .process_cmd(Cmd::HandleNewNodeOnline(auth), "cmd-id")
//         .await?;

//     let mut offline_relocate_sent = false;

//     for cmd in cmds {
//         let wire_msg = match cmd {
//             Cmd::SendMsg { wire_msg, .. } => wire_msg,
//             _ => continue,
//         };

//         if let Ok(MsgType::System {
//             msg:
//                 SystemMsg::Propose {
//                     proposal: crate::messaging::system::Proposal::Offline(node_state),
//                     ..
//                 },
//             ..
//         }) = wire_msg.into_msg()
//         {
//             assert_eq!(node_state.name, relocated_peer.name());
//             if let MembershipState::Relocated(relocate_details) = node_state.state {
//                 assert_eq!(relocate_details.age, relocated_peer.age() + 1);
//                 offline_relocate_sent = true;
//             }
//         }
//     }

//     assert!(offline_relocate_sent);

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn node_msg_to_self() -> Result<()> {
//     message_to_self(MessageDst::Node).await
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn section_msg_to_self() -> Result<()> {
//     message_to_self(MessageDst::Section).await
// }

// enum MessageDst {
//     Node,
//     Section,
// }

// async fn message_to_self(dst: MessageDst) -> Result<()> {
//     let info = gen_info(MIN_ADULT_AGE, None);
//     let (event_tx, _) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let (comm_tx, mut comm_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let comm =
//         Comm::first_node((Ipv4Addr::LOCALHOST, 0).into(), Default::default(), comm_tx).await?;
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;

//     let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
//     let node = Node::first_node(
//         comm,
//         info,
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//         genesis_sk_set,
//     )
//     .await?;
//     let info = node.info.read().await.clone();
//     let section_pk = node.network_knowledge().section_key().await;
//     let dispatcher = Dispatcher::new(node);

//     let dst_location = match dst {
//         MessageDst::Node => DstLocation::Node {
//             name: info.name(),
//             section_pk,
//         },
//         MessageDst::Section => DstLocation::Section {
//             name: info.name(),
//             section_pk,
//         },
//     };

//     let node_msg = SystemMsg::NodeMsgError {
//         error: crate::messaging::data::Error::FailedToWriteFile,
//         correlation_id: MsgId::new(),
//     };
//     let wire_msg = WireMsg::single_src(&info, dst_location, node_msg.clone(), section_pk)?;

//     let cmds = dispatcher
//         .process_cmd(
//             Cmd::SendMsg {
//                 recipients: vec![info.peer()],
//                 wire_msg,
//             },
//             "cmd-id",
//         )
//         .await?;

//     assert!(cmds.is_empty());

//     let msg_type = assert_matches!(comm_rx.recv().await, Some(MsgEvent::Received { sender, wire_msg, .. }) => {
//         assert_eq!(sender.addr(), info.addr);
//         assert_matches!(wire_msg.into_msg(), Ok(msg_type) => msg_type)
//     });

//     assert_matches!(msg_type, MsgType::System { msg, dst_location: dst, .. } => {
//         assert_eq!(dst, dst_location);
//         assert_eq!(
//             msg,
//             node_msg
//         );
//     });

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn handle_elders_update() -> Result<()> {
//     init_test_logger();
//     let _span = tracing::info_span!("handle_elders_update").entered();
//     // Start with section that has `elder_count()` elders with age 6, 1 non-elder with age 5 and one
//     // to-be-elder with age 7:
//     let info = gen_info(MIN_ADULT_AGE + 1, None);
//     let mut other_elder_peers: Vec<_> = iter::repeat_with(|| create_peer(MIN_ADULT_AGE + 1))
//         .take(elder_count() - 1)
//         .collect();
//     let adult_peer = create_peer(MIN_ADULT_AGE);
//     let promoted_peer = create_peer(MIN_ADULT_AGE + 2);

//     let sk_set0 = SecretKeySet::random();
//     let pk0 = sk_set0.secret_key().public_key();

//     let sap0 = SectionAuthorityProvider::new(
//         iter::once(info.peer()).chain(other_elder_peers.clone()),
//         Prefix::default(),
//         sk_set0.public_keys(),
//     );

//     let (section0, section_key_share) = create_section(&sk_set0, &sap0).await?;

//     for peer in [&adult_peer, &promoted_peer] {
//         let node_state = NodeState::joined(*peer, None);
//         let node_state = section_signed(sk_set0.secret_key(), node_state)?;
//         assert!(section0.update_member(node_state).await);
//     }

//     let demoted_peer = other_elder_peers.remove(0);

//     let sk_set1 = SecretKeySet::random();

//     let pk1 = sk_set1.secret_key().public_key();
//     // Create `HandleAgreement` cmd for an `NewElders` proposal. This will demote one of the
//     // current elders and promote the oldest peer.
//     let sap1 = SectionAuthorityProvider::new(
//         iter::once(info.peer())
//             .chain(other_elder_peers.clone())
//             .chain(iter::once(promoted_peer)),
//         Prefix::default(),
//         sk_set1.public_keys(),
//     );
//     let elder_names1: BTreeSet<_> = sap1.names();

//     let signed_sap1 = section_signed(sk_set1.secret_key(), sap1)?;
//     let proposal = Proposal::NewElders(signed_sap1);
//     let signature = sk_set0.secret_key().sign(&proposal.as_signable_bytes()?);
//     let sig = KeyedSig {
//         signature,
//         public_key: pk0,
//     };

//     let (event_tx, mut event_rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         info,
//         section0.clone(),
//         Some(section_key_share),
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;

//     // Simulate DKG round finished succesfully by adding
//     // the new section key share to our cache
//     node.section_keys_provider
//         .insert(create_section_key_share(&sk_set1, 0))
//         .await;

//     let dispatcher = Dispatcher::new(node);

//     let cmds = dispatcher
//         .process_cmd(Cmd::HandleNewEldersAgreement { proposal, sig }, "cmd-id")
//         .await?;

//     let mut update_actual_recipients = HashSet::new();

//     for cmd in cmds {
//         let (recipients, wire_msg) = match cmd {
//             Cmd::SendMsg {
//                 recipients,
//                 wire_msg,
//                 ..
//             } => (recipients, wire_msg),
//             _ => continue,
//         };

//         let (proof_chain, msg_authority) = match wire_msg.into_msg() {
//             Ok(MsgType::System {
//                 msg: SystemMsg::AntiEntropyUpdate { proof_chain, .. },
//                 msg_authority,
//                 ..
//             }) => (proof_chain, msg_authority),
//             _ => continue,
//         };

//         assert_eq!(proof_chain.last_key(), &pk1);

//         // The message is trusted even by peers who don't yet know the new section key.
//         assert!(msg_authority.verify_src_section_key_is_known(&[pk0]));

//         // Merging the section contained in the message with the original section succeeds.
//         // TODO: how to do this here?
//         // assert_matches!(section0.clone().merge(proof_chain.clone()), Ok(()));

//         update_actual_recipients.extend(recipients);
//     }

//     let update_expected_recipients: HashSet<_> = other_elder_peers
//         .into_iter()
//         .chain(iter::once(promoted_peer))
//         .chain(iter::once(demoted_peer))
//         .chain(iter::once(adult_peer))
//         .collect();

//     assert_eq!(update_actual_recipients, update_expected_recipients);

//     assert_matches!(
//         event_rx.recv().await,
//         Some(Event::EldersChanged { elders, .. }) => {
//             assert_eq!(elders.key, pk1);
//             assert_eq!(elder_names1, elders.added.union(&elders.remaining).copied().collect());
//             assert!(elders.removed.iter().all(|r| !elder_names1.contains(r)));
//         }
//     );

//     Ok(())
// }

// // Test that demoted node still sends `Sync` messages on split.
// #[tokio::test(flavor = "multi_thread")]
// async fn handle_demote_during_split() -> Result<()> {
//     init_test_logger();
//     let _span = tracing::info_span!("handle_demote_during_split").entered();

//     let info = gen_info(MIN_ADULT_AGE, None);
//     let node_name = info.name();
//     let prefix0 = Prefix::default().pushed(false);
//     let prefix1 = Prefix::default().pushed(true);
//     // These peers together with `node` are pre-split elders.
//     // These peers together with `peer_c` are prefix-0 post-split elders.
//     let peers_a: Vec<_> = iter::repeat_with(|| create_peer_in_prefix(&prefix0, MIN_ADULT_AGE))
//         .take(elder_count() - 1)
//         .collect();
//     // These peers are prefix-1 post-split elders.
//     let peers_b: Vec<_> = iter::repeat_with(|| create_peer_in_prefix(&prefix1, MIN_ADULT_AGE))
//         .take(elder_count())
//         .collect();
//     // This peer is a prefix-0 post-split elder.
//     let peer_c = create_peer_in_prefix(&prefix0, MIN_ADULT_AGE);

//     // Create the pre-split section
//     let sk_set_v0 = SecretKeySet::random();
//     let section_auth_v0 = SectionAuthorityProvider::new(
//         iter::once(info.peer()).chain(peers_a.iter().cloned()),
//         Prefix::default(),
//         sk_set_v0.public_keys(),
//     );
//     let (section, section_key_share) = create_section(&sk_set_v0, &section_auth_v0).await?;

//     for peer in peers_b.iter().chain(iter::once(&peer_c)).cloned() {
//         let node_state = NodeState::joined(peer, None);
//         let node_state = section_signed(sk_set_v0.secret_key(), node_state)?;
//         assert!(section.update_member(node_state).await);
//     }

//     let (event_tx, _) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
//     let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
//     let node = Node::new(
//         create_comm().await?,
//         info,
//         section,
//         Some(section_key_share),
//         event_tx,
//         UsedSpace::new(max_capacity),
//         root_storage_dir,
//     )
//     .await?;

//     let sk_set_v1_p0 = SecretKeySet::random();
//     let sk_set_v1_p1 = SecretKeySet::random();

//     // Simulate DKG round finished succesfully by adding the new section
//     // key share to our cache (according to which split section we'll belong to).
//     if prefix0.matches(&node_name) {
//         node.section_keys_provider
//             .insert(create_section_key_share(&sk_set_v1_p0, 0))
//             .await;
//     } else {
//         node.section_keys_provider
//             .insert(create_section_key_share(&sk_set_v1_p1, 0))
//             .await;
//     }

//     let dispatcher = Dispatcher::new(node);

//     // Create agreement on `OurElder` for both sub-sections
//     let create_our_elders_cmd = |signed_sap| -> Result<_> {
//         let proposal = Proposal::NewElders(signed_sap);
//         let signature = sk_set_v0.secret_key().sign(&proposal.as_signable_bytes()?);
//         let sig = KeyedSig {
//             signature,
//             public_key: sk_set_v0.public_keys().public_key(),
//         };

//         Ok(Cmd::HandleNewEldersAgreement { proposal, sig })
//     };

//     // Handle agreement on `NewElders` for prefix-0.
//     let section_auth = SectionAuthorityProvider::new(
//         peers_a.iter().cloned().chain(iter::once(peer_c)),
//         prefix0,
//         sk_set_v1_p0.public_keys(),
//     );

//     let signed_sap = section_signed(sk_set_v1_p0.secret_key(), section_auth)?;
//     let cmd = create_our_elders_cmd(signed_sap)?;
//     let cmds = dispatcher.process_cmd(cmd, "cmd-id-1").await?;

//     assert_matches!(&cmds[..], &[]);

//     // Handle agreement on `NewElders` for prefix-1.
//     let section_auth =
//         SectionAuthorityProvider::new(peers_b.iter().cloned(), prefix1, sk_set_v1_p1.public_keys());

//     let signed_sap = section_signed(sk_set_v1_p1.secret_key(), section_auth)?;
//     let cmd = create_our_elders_cmd(signed_sap)?;

//     let cmds = dispatcher.process_cmd(cmd, "cmd-id-2").await?;

//     let mut update_recipients = BTreeMap::new();

//     for cmd in cmds {
//         let (recipients, wire_msg) = match cmd {
//             Cmd::SendMsg {
//                 recipients,
//                 wire_msg,
//                 ..
//             } => (recipients, wire_msg),
//             _ => continue,
//         };

//         if matches!(
//             wire_msg.into_msg(),
//             Ok(MsgType::System {
//                 msg: SystemMsg::AntiEntropyUpdate { .. },
//                 ..
//             })
//         ) {
//             for recipient in recipients {
//                 let _old = update_recipients.insert(recipient.name(), recipient.addr());
//             }
//         }
//     }

//     // our node's whole section
//     assert_eq!(update_recipients.len(), elder_count());

//     Ok(())
// }

// fn create_peer(age: u8) -> Peer {
//     let name = ed25519::gen_name_with_age(age);
//     Peer::new(name, gen_addr())
// }

// fn create_peer_in_prefix(prefix: &Prefix, age: u8) -> Peer {
//     let name = ed25519::gen_name_with_age(age);
//     Peer::new(prefix.substituted_in(name), gen_addr())
// }

// fn gen_info(age: u8, prefix: Option<Prefix>) -> NodeInfo {
//     NodeInfo::new(
//         ed25519::gen_keypair(&prefix.unwrap_or_default().range_inclusive(), age),
//         gen_addr(),
//     )
// }

pub(crate) async fn create_comm() -> Result<Comm> {
    let (tx, _rx) = mpsc::channel(TEST_EVENT_CHANNEL_SIZE);
    Ok(Comm::first_node(
        (Ipv4Addr::LOCALHOST, 0).into(),
        Default::default(),
        Measurements::new(),
        tx,
    )
    .await?)
}

// // Generate random SectionAuthorityProvider and the corresponding Nodes.
// fn create_section_auth() -> (SectionAuthorityProvider, Vec<NodeInfo>, SecretKeySet) {
//     let (section_auth, elders, secret_key_set) =
//         gen_section_authority_provider(Prefix::default(), elder_count());
//     (section_auth, elders, secret_key_set)
// }

// fn create_section_key_share(sk_set: &bls::SecretKeySet, index: usize) -> SectionKeyShare {
//     SectionKeyShare {
//         public_key_set: sk_set.public_keys(),
//         index,
//         secret_key_share: sk_set.secret_key_share(index),
//     }
// }

// async fn create_section(
//     sk_set: &SecretKeySet,
//     section_auth: &SectionAuthorityProvider,
// ) -> Result<(NetworkKnowledge, SectionKeyShare)> {
//     let section_chain = SecuredLinkedList::new(sk_set.public_keys().public_key());
//     let signed_sap = section_signed(sk_set.secret_key(), section_auth.clone())?;

//     let section =
//         NetworkKnowledge::new(*section_chain.root_key(), section_chain, signed_sap, None)?;

//     for peer in section_auth.elders() {
//         let node_state = NodeState::joined(*peer, None);
//         let node_state = section_signed(sk_set.secret_key(), node_state)?;
//         let _updated = section.update_member(node_state).await;
//     }

//     let section_key_share = create_section_key_share(sk_set, 0);

//     Ok((section, section_key_share))
// }

// // Create a `Proposal::Online` whose agreement handling triggers relocation of a node with the
// // given age.
// // NOTE: recommended to call this with low `age` (4 or 5), otherwise it might take very long time
// // to complete because it needs to generate a signature with the number of trailing zeroes equal to
// // (or greater that) `age`.
// fn create_relocation_trigger(sk: &bls::SecretKey, age: u8) -> Result<SectionAuth<NodeStateMsg>> {
//     loop {
//         let node_state =
//             NodeState::joined(create_peer(MIN_ADULT_AGE), Some(xor_name::rand::random()));
//         let auth = section_signed(sk, node_state.to_msg())?;

//         let churn_id = ChurnId(auth.sig.signature.to_bytes().to_vec());
//         if relocation_check(age, &churn_id) && !relocation_check(age + 1, &churn_id) {
//             return Ok(auth);
//         }
//     }
// }

// Wrapper for `bls::SecretKeySet` that also allows to retrieve the corresponding `bls::SecretKey`.
// Note: `bls::SecretKeySet` does have a `secret_key` method, but it's test-only and not available
// for the consumers of the crate.
pub(crate) struct SecretKeySet {
    set: bls::SecretKeySet,
    key: bls::SecretKey,
}

impl SecretKeySet {
    pub(crate) fn random() -> Self {
        let poly = bls::poly::Poly::random(threshold(), &mut rand::thread_rng());
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

// Create signature for the given bytes using the given secret key.
fn keyed_signed(secret_key: &bls::SecretKey, bytes: &[u8]) -> KeyedSig {
    KeyedSig {
        public_key: secret_key.public_key(),
        signature: secret_key.sign(bytes),
    }
}
