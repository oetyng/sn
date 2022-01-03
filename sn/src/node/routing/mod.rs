// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Peer implementation for a resilient decentralised network infrastructure.
//!
//! This is the "engine room" of a hybrid p2p network, where the p2p nodes are built on
//! top of this library. The features this library gives us is:
//!
//!  * Sybil resistant p2p nodes
//!  * Sharded network with up to approx 200 p2p nodes per shard
//!  * All data encrypted at network level with TLS 1.3
//!  * Network level `quic` compatibility, satisfying industry standards and further
//!    obfuscating the p2p network data.
//!  * Upgrade capable nodes.
//!  * All network messages signed via ED25519 and/or BLS
//!  * Section consensus via an ABFT algorithm (PARSEC)

mod api;
mod core;
mod dkg;
mod ed25519;
mod messages;
mod network_knowledge;
mod node;
mod relocation;

pub use self::{
    api::{
        config::Config,
        event::{Elders, Event, MessageReceived, NodeElderChange},
        event_stream::EventStream,
        Routing,
    },
    dkg::SectionAuthUtils,
    network_knowledge::node_state::{
        FIRST_SECTION_MAX_AGE, FIRST_SECTION_MIN_AGE, MIN_ADULT_AGE, MIN_AGE,
    },
};
pub use qp2p::{Config as NetworkConfig, SendStream};
pub use xor_name::{Prefix, XorName, XOR_NAME_LEN}; // TODO remove pub on API update

#[cfg(any(test, feature = "test-utils"))]
pub use test_utils::*;

#[cfg(test)]
pub(crate) use dkg::test_utils::section_signed;
#[cfg(test)]
pub(crate) use network_knowledge::test_utils::gen_section_authority_provider;

pub(crate) use self::{
    core::{ChunkStoreError, MIN_LEVEL_WHEN_FULL},
    network_knowledge::{section_keys::SectionKeyShare, SectionAuthorityProvider},
};

/// Recommended section size.
/// The section will keep adding nodes when requested by the upper layers, until it can split.
/// A split happens if both post-split sections would have at least this number of nodes.
pub(crate) fn recommended_section_size() -> usize {
    2 * crate::elder_count()
}

/// SuperMajority of a given group (i.e. > 2/3)
#[inline]
pub(crate) const fn supermajority(group_size: usize) -> usize {
    1 + group_size * 2 / 3
}

use crate::peer::Peer;

#[cfg(any(test, feature = "test-utils"))]
mod test_utils {
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    const TEST_MAX_CAPACITY: u64 = 1024 * 1024;

    /// Create a register store for routing examples
    pub fn create_test_max_capacity_and_root_storage() -> eyre::Result<(u64, u64, PathBuf)> {
        let random_filename: String = thread_rng().sample_iter(&Alphanumeric).take(15).collect();

        let root_dir = tempdir().map_err(|e| eyre::eyre!(e.to_string()))?;
        let storage_dir = Path::new(root_dir.path()).join(random_filename);

        Ok((TEST_MAX_CAPACITY, TEST_MAX_CAPACITY, storage_dir))
    }
}

#[cfg(test)]
mod tests {
    use super::supermajority;
    use proptest::prelude::*;

    #[test]
    fn supermajority_of_small_group() {
        assert_eq!(supermajority(0), 1);
        assert_eq!(supermajority(1), 1);
        assert_eq!(supermajority(2), 2);
        assert_eq!(supermajority(3), 3);
        assert_eq!(supermajority(4), 3);
        assert_eq!(supermajority(5), 4);
        assert_eq!(supermajority(6), 5);
        assert_eq!(supermajority(7), 5);
        assert_eq!(supermajority(8), 6);
        assert_eq!(supermajority(9), 7);
    }

    proptest! {
        #[test]
        fn proptest_supermajority(a in 0usize..10000) {
            let n = 3 * a;
            assert_eq!(supermajority(n),     2 * a + 1);
            assert_eq!(supermajority(n + 1), 2 * a + 1);
            assert_eq!(supermajority(n + 2), 2 * a + 2);
        }
    }
}
