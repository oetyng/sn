// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::mem;

use crate::messaging::{node::SectionAuth, SectionAuthorityProvider};
use crate::routing::dkg::KeyedSig;
use xor_name::Prefix;

type Entry = (SectionAuth<SectionAuthorityProvider>, KeyedSig);

// Helper structure to make sure we process a split by updating info about both our section and the
// sibling section at the same time.
pub(crate) struct SplitBarrier(Vec<Entry>);

impl SplitBarrier {
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }

    // Pass an aggreed-on proposal for `OurElders` through this function. If there is no split, it
    // returns it unchanged. If there is a split and we've seen the aggreement for only one
    // subsection so far, it caches it and returns nothing. Otherwise it returns both proposals.
    //
    // Note: in case of a fork, it can return more than two proposals. In that case one of the
    // proposals will be for one subsection and all the others for the other subsection.
    pub(crate) fn process(
        &mut self,
        our_prefix: &Prefix,
        section_auth: SectionAuth<SectionAuthorityProvider>,
        keyed_sig: KeyedSig,
    ) -> Vec<Entry> {
        if !section_auth.value.prefix.is_extension_of(our_prefix) {
            // Not a split, no need to cache.
            return vec![(section_auth, keyed_sig)];
        }

        // Split detected. Find all cached siblings.
        let (mut give, keep) =
            mem::take(&mut self.0)
                .into_iter()
                .partition(|(cached_section_auth, _)| {
                    cached_section_auth.value.prefix == section_auth.value.prefix.sibling()
                });
        self.0 = keep;

        if give.is_empty() {
            // No sibling found. Cache this update until we see the sibling update.
            self.0.push((section_auth, keyed_sig));
            vec![]
        } else {
            // Sibling found. We can proceed with the update.
            give.push((section_auth, keyed_sig));
            give
        }
    }
}
