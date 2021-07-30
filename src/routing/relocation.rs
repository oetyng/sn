// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Relocation related types and utilities.

use crate::messaging::{
    node::{
        Network, NodeMsg, NodeState, Peer, RelocateDetails, RelocatePayload, RelocatePromise,
        Section,
    },
    AuthorityProof, SectionAuth,
};
use crate::routing::{
    core::JoiningAsRelocated,
    ed25519::{self, Keypair, Verifier},
    error::Error,
    network::NetworkUtils,
    peer::PeerUtils,
    section::{SectionPeersUtils, SectionUtils},
};
use xor_name::XorName;

/// Find all nodes to relocate after a churn event and create the relocate actions for them.
pub(crate) fn actions(
    section: &Section,
    network: &Network,
    churn_name: &XorName,
    churn_signature: &bls::Signature,
) -> Vec<(NodeState, RelocateAction)> {
    // Find the peers that pass the relocation check and take only the oldest ones to avoid
    // relocating too many nodes at the same time.
    let candidates: Vec<_> = section
        .members()
        .joined()
        .filter(|info| check(info.peer.age(), churn_signature))
        .collect();

    let max_age = if let Some(age) = candidates.iter().map(|info| (*info).peer.age()).max() {
        age
    } else {
        return vec![];
    };

    candidates
        .into_iter()
        .filter(|info| info.peer.age() == max_age)
        .map(|info| {
            (
                *info,
                RelocateAction::new(section, network, &info.peer, churn_name),
            )
        })
        .collect()
}

/// Details of a relocation: which node to relocate, where to relocate it to and what age it should
/// get once relocated.
pub(super) trait RelocateDetailsUtils {
    fn new(section: &Section, network: &Network, peer: &Peer, dst: XorName) -> Self;

    fn with_age(
        section: &Section,
        network: &Network,
        peer: &Peer,
        dst: XorName,
        age: u8,
    ) -> RelocateDetails;
}

impl RelocateDetailsUtils for RelocateDetails {
    fn new(section: &Section, network: &Network, peer: &Peer, dst: XorName) -> Self {
        Self::with_age(section, network, peer, dst, peer.age().saturating_add(1))
    }

    fn with_age(
        section: &Section,
        network: &Network,
        peer: &Peer,
        dst: XorName,
        age: u8,
    ) -> RelocateDetails {
        let dst_key = network
            .key_by_name(&dst)
            .unwrap_or_else(|_| *section.chain().root_key());

        RelocateDetails {
            pub_id: *peer.name(),
            dst,
            dst_key,
            age,
        }
    }
}

pub(super) trait RelocatePayloadUtils {
    fn new(
        details: NodeMsg,
        section_auth: AuthorityProof<SectionAuth>,
        new_name: &XorName,
        old_keypair: &Keypair,
    ) -> Self;

    fn verify_identity(&self, new_name: &XorName) -> bool;

    fn relocate_details(&self) -> Result<&RelocateDetails, Error>;
}

impl RelocatePayloadUtils for RelocatePayload {
    fn new(
        details: NodeMsg,
        section_auth: AuthorityProof<SectionAuth>,
        new_name: &XorName,
        old_keypair: &Keypair,
    ) -> Self {
        let signature_of_new_name_with_old_key = ed25519::sign(&new_name.0, old_keypair);

        Self {
            details,
            section_signed: section_auth.into_inner(),
            signature_of_new_name_with_old_key,
        }
    }

    fn verify_identity(&self, new_name: &XorName) -> bool {
        let details = if let Ok(details) = self.relocate_details() {
            details
        } else {
            return false;
        };

        let pub_key = if let Ok(pub_key) = ed25519::pub_key(&details.pub_id) {
            pub_key
        } else {
            return false;
        };

        pub_key
            .verify(&new_name.0, &self.signature_of_new_name_with_old_key)
            .is_ok()
    }

    fn relocate_details(&self) -> Result<&RelocateDetails, Error> {
        if let NodeMsg::Relocate(relocate_details) = &self.details {
            Ok(relocate_details)
        } else {
            error!("RelocateDetails does not contain a NodeMsg::Relocate");
            Err(Error::InvalidMessage)
        }
    }
}

pub(crate) enum RelocateState {
    // Node is undergoing delayed relocation. This happens when the node is selected for relocation
    // while being an elder. It must keep fulfilling its duties as elder until its demoted, then it
    // can send the bytes (which are serialized `RelocatePromise` message) back to the elders who
    // will exchange it for an actual `Relocate` message.
    Delayed(NodeMsg),
    // Relocation in progress.
    InProgress(Box<JoiningAsRelocated>),
}

/// Action to relocate a node.
#[derive(Debug)]
pub(crate) enum RelocateAction {
    /// Relocate the node instantly.
    Instant(RelocateDetails),
    /// Relocate the node after they are no longer our elder.
    Delayed(RelocatePromise),
}

impl RelocateAction {
    pub(crate) fn new(
        section: &Section,
        network: &Network,
        peer: &Peer,
        churn_name: &XorName,
    ) -> Self {
        let dst = dst(peer.name(), churn_name);

        if section.is_elder(peer.name()) {
            RelocateAction::Delayed(RelocatePromise {
                name: *peer.name(),
                dst,
            })
        } else {
            RelocateAction::Instant(RelocateDetails::new(section, network, peer, dst))
        }
    }

    pub(crate) fn dst(&self) -> &XorName {
        match self {
            Self::Instant(details) => &details.dst,
            Self::Delayed(promise) => &promise.dst,
        }
    }

    #[cfg(test)]
    pub(crate) fn name(&self) -> &XorName {
        match self {
            Self::Instant(details) => &details.pub_id,
            Self::Delayed(promise) => &promise.name,
        }
    }
}

// Relocation check - returns whether a member with the given age is a candidate for relocation on
// a churn event with the given signature.
pub(crate) fn check(age: u8, churn_signature: &bls::Signature) -> bool {
    // Evaluate the formula: `signature % 2^age == 0` Which is the same as checking the signature
    // has at least `age` trailing zero bits.
    trailing_zeros(&churn_signature.to_bytes()[..]) >= age as u32
}

// Compute the destination for the node with `relocating_name` to be relocated to. `churn_name` is
// the name of the joined/left node that triggered the relocation.
fn dst(relocating_name: &XorName, churn_name: &XorName) -> XorName {
    XorName::from_content(&[&relocating_name.0, &churn_name.0])
}

// Returns the number of trailing zero bits of the byte slice.
fn trailing_zeros(bytes: &[u8]) -> u32 {
    let mut output = 0;

    for &byte in bytes.iter().rev() {
        if byte == 0 {
            output += 8;
        } else {
            output += byte.trailing_zeros();
            break;
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::SectionAuthorityProvider;
    use crate::routing::{
        dkg::test_utils::section_signed, peer::test_utils::arbitrary_unique_peers,
        routing_api::tests::SecretKeySet, section::NodeStateUtils, SectionAuthorityProviderUtils,
        ELDER_SIZE, MIN_AGE,
    };
    use anyhow::Result;
    use assert_matches::assert_matches;
    use itertools::Itertools;
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    use secured_linked_list::SecuredLinkedList;
    use xor_name::Prefix;

    #[test]
    fn byte_slice_trailing_zeros() {
        assert_eq!(trailing_zeros(&[0]), 8);
        assert_eq!(trailing_zeros(&[1]), 0);
        assert_eq!(trailing_zeros(&[2]), 1);
        assert_eq!(trailing_zeros(&[4]), 2);
        assert_eq!(trailing_zeros(&[8]), 3);
        assert_eq!(trailing_zeros(&[0, 0]), 16);
        assert_eq!(trailing_zeros(&[1, 0]), 8);
        assert_eq!(trailing_zeros(&[2, 0]), 9);
    }

    const MAX_AGE: u8 = MIN_AGE + 4;

    proptest! {
        #[test]
        fn proptest_actions(
            peers in arbitrary_unique_peers(2..ELDER_SIZE + 1, MIN_AGE..MAX_AGE),
            signature_trailing_zeros in 0..MAX_AGE,
            seed in any::<u64>().no_shrink())
        {
            proptest_actions_impl(peers, signature_trailing_zeros, seed).unwrap()
        }
    }

    fn proptest_actions_impl(
        peers: Vec<Peer>,
        signature_trailing_zeros: u8,
        seed: u64,
    ) -> Result<()> {
        let mut rng = SmallRng::seed_from_u64(seed);

        let sk_set = SecretKeySet::random();
        let sk = sk_set.secret_key();
        let pk = sk.public_key();

        // Create `Section` with `peers` as its members and set the `ELDER_SIZE` oldest peers as
        // the elders.
        let section_auth = SectionAuthorityProvider::new(
            peers
                .iter()
                .sorted_by_key(|peer| peer.age())
                .rev()
                .take(ELDER_SIZE)
                .copied(),
            Prefix::default(),
            sk_set.public_keys(),
        );
        let section_auth = section_signed(sk, section_auth)?;

        let mut section = Section::new(pk, SecuredLinkedList::new(pk), section_auth)?;

        for peer in &peers {
            let info = NodeState::joined(*peer, None);
            let info = section_signed(sk, info)?;

            assert!(section.update_member(info));
        }

        let network = Network::new();

        // Simulate a churn event whose signature has the given number of trailing zeros.
        let churn_name = rng.gen();
        let churn_signature = signature_with_trailing_zeros(signature_trailing_zeros as u32);

        let actions = actions(&section, &network, &churn_name, &churn_signature);
        let actions: Vec<_> = actions
            .into_iter()
            .map(|(_, action)| action)
            .sorted_by_key(|action| *action.name())
            .collect();

        // Only the oldest matching peers should be relocated.
        let expected_relocated_age = peers
            .iter()
            .map(Peer::age)
            .filter(|age| *age <= signature_trailing_zeros)
            .max();

        let expected_relocated_peers: Vec<_> = peers
            .iter()
            .filter(|peer| Some(peer.age()) == expected_relocated_age)
            .sorted_by_key(|peer| *peer.name())
            .collect();

        assert_eq!(expected_relocated_peers.len(), actions.len());

        // Verify the relocate action is correct depending on whether the peer is elder or not.
        // NOTE: `zip` works here, because both collections are sorted by name.
        for (peer, action) in expected_relocated_peers.into_iter().zip(actions) {
            assert_eq!(peer.name(), action.name());

            if section.is_elder(peer.name()) {
                assert_matches!(action, RelocateAction::Delayed(_));
            } else {
                assert_matches!(action, RelocateAction::Instant(_));
            }
        }

        Ok(())
    }

    // Fetch a `bls::Signature` with the given number of trailing zeros. The signature is generated
    // from an unspecified random data using an unspecified random `SecretKey`. That is OK because
    // the relocation algorithm doesn't care about whether the signature is valid. It only
    // cares about its number of trailing zeros.
    fn signature_with_trailing_zeros(trailing_zeros_count: u32) -> bls::Signature {
        use std::{cell::RefCell, collections::HashMap};

        // Cache the signatures to avoid expensive re-computation.
        thread_local! {
            static CACHE: RefCell<HashMap<u32, bls::Signature>> = RefCell::new(HashMap::new());
        }

        CACHE.with(|cache| {
            cache
                .borrow_mut()
                .entry(trailing_zeros_count)
                .or_insert_with(|| gen_signature_with_trailing_zeros(trailing_zeros_count))
                .clone()
        })
    }

    fn gen_signature_with_trailing_zeros(trailing_zeros_count: u32) -> bls::Signature {
        let mut rng = SmallRng::seed_from_u64(0);
        let sk: bls::SecretKey = rng.gen();

        loop {
            let data: u64 = rng.gen();
            let signature = sk.sign(&data.to_be_bytes());

            if trailing_zeros(&signature.to_bytes()) == trailing_zeros_count {
                return signature;
            }
        }
    }
}
