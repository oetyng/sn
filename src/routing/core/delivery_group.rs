// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::routing::{
    error::{Error, Result},
    network::{Network, NetworkLogic},
    peer::PeerUtils,
    section::{SectionLogic, SectionPeersLogic},
    supermajority, SectionAuthorityProviderUtils, ELDER_SIZE,
};
use crate::{
    messaging::{node::Peer, DstLocation},
    routing::section::Section,
};
use itertools::Itertools;
use std::{cmp, iter};
use xor_name::XorName;

/// Returns a set of nodes and their section PublicKey to which a message for the given
/// `DstLocation` could be sent onwards, sorted by priority, along with the number of targets the
/// message should be sent to. If the total number of targets returned is larger than this number,
/// the spare targets can be used if the message can't be delivered to some of the initial ones.
///
/// * If the destination is a `DstLocation::Section` OR `DstLocation::EndUser`:
///     - if our section is the closest on the network (i.e. our section's prefix is a prefix of
///       the dst), returns all other members of our section; otherwise
///     - returns the `N/3` closest members to the target
///
/// * If the destination is an individual node:
///     - if our name *is* the dst, returns an empty set; otherwise
///     - if the destination name is an entry in the routing table, returns it; otherwise
///     - returns the `N/3` closest members of the RT to the target
pub(crate) async fn delivery_targets(
    dst: &DstLocation,
    our_name: &XorName,
    section: &Section,
    network: &Network,
) -> Result<(Vec<Peer>, usize)> {
    // Adult now having the knowledge of other adults within the own section.
    // Functions of `section_candidates` and `candidates` only take section elder into account.

    match dst {
        DstLocation::Section { name, .. } => {
            section_candidates(name, our_name, section, network).await
        }
        DstLocation::EndUser(user) => {
            section_candidates(&user.xorname, our_name, section, network).await
        }
        DstLocation::Node { name, .. } => {
            if name == our_name {
                return Ok((Vec::new(), 0));
            }
            if let Some(node) = get_peer(name, section, network).await {
                return Ok((vec![node], 1));
            }

            if !section.is_elder(our_name) {
                // We are not Elder - return all the elders of our section,
                // so the message can be properly relayed through them.
                let targets: Vec<_> = section.authority_provider().await.peers().collect();
                let dg_size = targets.len();
                Ok((targets, dg_size))
            } else {
                candidates(name, our_name, section, network).await
            }
        }
        DstLocation::DirectAndUnrouted(_) => Err(Error::CannotRoute),
    }
}

async fn section_candidates(
    target_name: &XorName,
    our_name: &XorName,
    section: &Section,
    network: &Network,
) -> Result<(Vec<Peer>, usize)> {
    let prefix = section.prefix().await;
    let sap = section.authority_provider().await;
    // Find closest section to `target_name` out of the ones we know (including our own)
    //let info = iter::once(sap);
    let info = {
        let all_network = network.all().await;
        if let Some(info) = all_network
            .into_iter()
            .chain(iter::once(sap.clone()))
            .min_by(|lhs, rhs| lhs.prefix.cmp_distance(&rhs.prefix, target_name))
        {
            info
        } else {
            sap
        }
    };

    if info.prefix == prefix {
        // Exclude our name since we don't need to send to ourself
        let chosen_section: Vec<_> = info
            .peers()
            .filter(|node| node.name() != our_name)
            .collect();
        let dg_size = chosen_section.len();
        return Ok((chosen_section, dg_size));
    }

    candidates(target_name, our_name, section, network).await
}

// Obtain the delivery group candidates for this target
async fn candidates(
    target_name: &XorName,
    our_name: &XorName,
    section: &Section,
    network: &Network,
) -> Result<(Vec<Peer>, usize)> {
    // All sections we know (including our own), sorted by distance to `target_name`.
    let section_prefix = section.prefix().await;
    let info = iter::once(section.authority_provider().await);
    let network_all = network.all().await.into_iter();
    let sections = info
        .chain(network_all)
        .sorted_by(|lhs, rhs| lhs.prefix.cmp_distance(&rhs.prefix, target_name))
        .map(|info| (info.prefix, info.elder_count(), info.peers().collect_vec()));

    // gives at least 1 honest target among recipients.
    let min_dg_size = 1 + ELDER_SIZE - supermajority(ELDER_SIZE);
    let mut dg_size = min_dg_size;
    let mut candidates = Vec::new();

    for (idx, (prefix, len, connected)) in sections.enumerate() {
        candidates.extend(connected);
        if prefix.matches(target_name) {
            // If we are last hop before final dst, send to all candidates.
            dg_size = len;
        } else {
            // If we don't have enough contacts send to as many as possible
            // up to dg_size of Elders
            dg_size = cmp::min(len, dg_size);
        }
        if len < min_dg_size {
            warn!(
                "Delivery group only {:?} when it should be {:?}",
                len, min_dg_size
            )
        }

        if prefix == section_prefix {
            // Send to all connected targets so they can forward the message
            candidates.retain(|node| node.name() != our_name);
            dg_size = candidates.len();
            break;
        }
        if idx == 0 && candidates.len() >= dg_size {
            // can deliver to enough of the closest section
            break;
        }
    }
    candidates.sort_by(|lhs, rhs| target_name.cmp_distance(lhs.name(), rhs.name()));

    if dg_size > 0 && candidates.len() >= dg_size {
        Ok((candidates, dg_size))
    } else {
        Err(Error::CannotRoute)
    }
}

// Returns a `Peer` for a known node.
async fn get_peer(name: &XorName, section: &Section, network: &Network) -> Option<Peer> {
    match section.members().get(name).await.map(|info| info.peer) {
        Some(peer) => Some(peer),
        None => network.get_elder(name).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::{node::NodeState, SectionAuthorityProvider};
    use crate::routing::{
        dkg::test_utils::section_signed,
        ed25519,
        section::{
            test_utils::{gen_addr, gen_section_authority_provider},
            NodeStateUtils,
        },
        SectionAuthorityProviderUtils, MIN_ADULT_AGE,
    };
    use eyre::{ContextCompat, Result};
    use rand::seq::IteratorRandom;
    use secured_linked_list::SecuredLinkedList;
    use xor_name::Prefix;

    #[tokio::test]
    async fn delivery_targets_elder_to_our_elder() -> Result<()> {
        let (our_name, section, network, _) = setup_elder().await?;

        let dst_name = *section
            .authority_provider()
            .await
            .names()
            .iter()
            .filter(|&&name| name != our_name)
            .choose(&mut rand::thread_rng())
            .context("too few elders")?;

        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send only to the dst node.
        assert_eq!(dg_size, 1);
        assert_eq!(recipients[0].name(), &dst_name);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_to_our_adult() -> Result<()> {
        let (our_name, section, network, sk) = setup_elder().await?;

        let name = ed25519::gen_name_with_age(MIN_ADULT_AGE);
        let dst_name = section.prefix().await.substituted_in(name);
        let peer = Peer::new(dst_name, gen_addr());
        let node_state = NodeState::joined(peer, None);
        let node_state = section_signed(&sk, node_state)?;
        assert!(section.update_member(node_state).await);

        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send only to the dst node.
        assert_eq!(dg_size, 1);
        assert_eq!(recipients[0].name(), &dst_name);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_to_our_section() -> Result<()> {
        let (our_name, section, network, _) = setup_elder().await?;

        let dst_name = section.prefix().await.substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        let auth = section.authority_provider().await;
        // Send to all our elders except us.
        let expected_recipients = auth.peers().filter(|peer| peer.name() != &our_name);
        assert_eq!(dg_size, expected_recipients.count());

        let expected_recipients = auth.peers().filter(|peer| peer.name() != &our_name);
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_to_known_remote_peer() -> Result<()> {
        let (our_name, section, network, _) = setup_elder().await?;

        let section_auth1 = network
            .get(&Prefix::default().pushed(true))
            .await
            .context("unknown section")?;

        let dst_name = choose_elder_name(&section_auth1)?;
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send only to the dst node.
        assert_eq!(dg_size, 1);
        assert_eq!(recipients[0].name(), &dst_name);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_to_final_hop_unknown_remote_peer() -> Result<()> {
        let (our_name, section, network, _) = setup_elder().await?;

        let section_auth1 = network
            .get(&Prefix::default().pushed(true))
            .await
            .context("unknown section")?;

        let dst_name = section_auth1.prefix.substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to all elders in the dst section
        let expected_recipients = section_auth1
            .peers()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(lhs.name(), rhs.name()));
        assert_eq!(dg_size, section_auth1.elder_count());
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "Need to setup network so that we do not locate final dst, as to trigger correct outcome."]
    async fn delivery_targets_elder_to_intermediary_hop_unknown_remote_peer() -> Result<()> {
        let (our_name, section, network, _) = setup_elder().await?;

        let elders_info1 = network
            .get(&Prefix::default().pushed(true))
            .await
            .context("unknown section")?;

        let dst_name = elders_info1
            .prefix
            .pushed(false)
            .substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to all elders in the dst section
        let expected_recipients = elders_info1
            .peers()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(lhs.name(), rhs.name()));
        let min_dg_size =
            1 + elders_info1.elder_count() - supermajority(elders_info1.elder_count());
        assert_eq!(dg_size, min_dg_size);
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_final_hop_to_remote_section() -> Result<()> {
        let (our_name, section, network, _) = setup_elder().await?;

        let section_auth1 = network
            .get(&Prefix::default().pushed(true))
            .await
            .context("unknown section")?;

        let dst_name = section_auth1.prefix.substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to all elders in the final dst section
        let expected_recipients = section_auth1
            .peers()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(lhs.name(), rhs.name()));
        assert_eq!(dg_size, section_auth1.elder_count());
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "Need to setup network so that we do not locate final dst, as to trigger correct outcome."]
    async fn delivery_targets_elder_intermediary_hop_to_remote_section() -> Result<()> {
        let (our_name, section, network, _) = setup_elder().await?;

        let elders_info1 = network
            .get(&Prefix::default().pushed(true))
            .await
            .context("unknown section")?;

        let dst_name = elders_info1
            .prefix
            .pushed(false)
            .substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to a subset of elders in the intermediary dst section
        let min_dg_size =
            1 + elders_info1.elder_count() - supermajority(elders_info1.elder_count());
        let expected_recipients = elders_info1
            .peers()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(lhs.name(), rhs.name()))
            .take(min_dg_size);

        assert_eq!(dg_size, min_dg_size);
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_our_elder() -> Result<()> {
        let (our_name, section, network) = setup_adult().await?;

        let dst_name = choose_elder_name(&section.authority_provider().await)?;
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to all elders
        assert_eq!(dg_size, section.authority_provider().await.elder_count());
        itertools::assert_equal(recipients, section.authority_provider().await.peers());

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_our_adult() -> Result<()> {
        let (our_name, section, network) = setup_adult().await?;

        let dst_name = section.prefix().await.substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to all elders
        assert_eq!(dg_size, section.authority_provider().await.elder_count());
        itertools::assert_equal(recipients, section.authority_provider().await.peers());

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_our_section() -> Result<()> {
        let (our_name, section, network) = setup_adult().await?;

        let dst_name = section.prefix().await.substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to all elders
        assert_eq!(dg_size, section.authority_provider().await.elder_count());
        itertools::assert_equal(recipients, section.authority_provider().await.peers());

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_remote_peer() -> Result<()> {
        let (our_name, section, network) = setup_adult().await?;

        let dst_name = Prefix::default()
            .pushed(true)
            .substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to all elders
        assert_eq!(dg_size, section.authority_provider().await.elder_count());
        itertools::assert_equal(recipients, section.authority_provider().await.peers());

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_remote_section() -> Result<()> {
        let (our_name, section, network) = setup_adult().await?;

        let dst_name = Prefix::default()
            .pushed(true)
            .substituted_in(rand::random());
        let section_pk = section.authority_provider().await.section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &section, &network).await?;

        // Send to all elders
        assert_eq!(dg_size, section.authority_provider().await.elder_count());
        itertools::assert_equal(recipients, section.authority_provider().await.peers());

        Ok(())
    }

    async fn setup_elder() -> Result<(XorName, Section, Network, bls::SecretKey)> {
        let prefix0 = Prefix::default().pushed(false);
        let prefix1 = Prefix::default().pushed(true);

        let genesis_sk = bls::SecretKey::random();
        let genesis_pk = genesis_sk.public_key();

        let sk = bls::SecretKey::random();
        let pk = sk.public_key();

        let (section_auth0, _, _) = gen_section_authority_provider(prefix0, ELDER_SIZE);
        let elders0: Vec<_> = section_auth0.peers().collect();
        let section_auth0 = section_signed(&sk, section_auth0)?;

        let mut chain = SecuredLinkedList::new(genesis_pk);
        let sig = bincode::serialize(&pk).map(|bytes| genesis_sk.sign(&bytes))?;
        chain.insert(&genesis_pk, pk, sig)?;
        let section = Section::new(pk, chain, section_auth0)?;

        for peer in elders0 {
            let node_state = NodeState::joined(peer, None);
            let node_state = section_signed(&sk, node_state)?;
            assert!(section.update_member(node_state).await);
        }

        let network = Network::new();

        let (section_auth1, _, _) = gen_section_authority_provider(prefix1, ELDER_SIZE);
        let section_auth1 = section_signed(&sk, section_auth1)?;
        let mut proof_chain = SecuredLinkedList::new(genesis_pk);
        let pk1 = section_auth1.value.public_key_set.public_key();
        let sig = bincode::serialize(&pk1).map(|bytes| genesis_sk.sign(&bytes))?;
        proof_chain.insert(&genesis_pk, pk1, sig)?;

        assert!(
            network
                .update_remote_section_sap(
                    section_auth1,
                    &proof_chain,
                    &section.chain_clone().await
                )
                .await
        );

        let our_name = choose_elder_name(&section.authority_provider().await)?;

        Ok((our_name, section, network, sk))
    }

    async fn setup_adult() -> Result<(XorName, Section, Network)> {
        let prefix0 = Prefix::default().pushed(false);

        let sk = bls::SecretKey::random();
        let pk = sk.public_key();
        let chain = SecuredLinkedList::new(pk);

        let (section_auth, _, _) = gen_section_authority_provider(prefix0, ELDER_SIZE);
        let section_auth = section_signed(&sk, section_auth)?;
        let section = Section::new(pk, chain, section_auth)?;

        let network = Network::new();
        let our_name = section.prefix().await.substituted_in(rand::random());

        Ok((our_name, section, network))
    }

    fn choose_elder_name(section_auth: &SectionAuthorityProvider) -> Result<XorName> {
        section_auth
            .elders()
            .keys()
            .choose(&mut rand::thread_rng())
            .copied()
            .context("no elders")
    }
}
