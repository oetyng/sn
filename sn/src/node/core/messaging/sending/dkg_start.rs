// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    system::{DkgSessionId, SystemMsg},
    DstLocation, WireMsg,
};
use crate::node::{
    api::cmds::Cmd, core::Node, dkg::DkgSessionIdUtils, messages::WireMsgUtils,
    network_knowledge::ElderCandidates, Result,
};
use crate::types::Peer;

use xor_name::XorName;

impl Node {
    /// Send a `DkgStart` message to the provided set of candidates
    pub(crate) async fn send_dkg_start(
        &self,
        elder_candidates: ElderCandidates,
    ) -> Result<Vec<Cmd>> {
        let src_prefix = elder_candidates.prefix();
        let generation = self.network_knowledge.chain_len().await;
        let session_id = DkgSessionId::new(&elder_candidates, generation);

        // Send DKG start to all candidates
        let recipients: Vec<_> = elder_candidates.elders().cloned().collect();

        trace!(
            "Send DkgStart for {:?} with {:?} to {:?}",
            elder_candidates,
            session_id,
            recipients
        );

        let node_msg = SystemMsg::DkgStart {
            session_id,
            prefix: elder_candidates.prefix(),
            elders: elder_candidates
                .elders()
                .map(|peer| (peer.name(), peer.addr()))
                .collect(),
        };
        let section_pk = self.network_knowledge.section_key().await;
        self.send_msg_for_dst_accumulation(
            src_prefix.name(),
            DstLocation::Section {
                name: src_prefix.name(),
                section_pk,
            },
            node_msg,
            recipients,
        )
        .await
    }

    async fn send_msg_for_dst_accumulation(
        &self,
        src: XorName,
        dst: DstLocation,
        node_msg: SystemMsg,
        recipients: Vec<Peer>,
    ) -> Result<Vec<Cmd>> {
        let section_key = self.network_knowledge.section_key().await;

        let key_share = self
            .section_keys_provider
            .key_share(&section_key)
            .await
            .map_err(|err| {
                trace!(
                    "Can't create message {:?} for accumulation at dst {:?}: {:?}",
                    node_msg,
                    dst,
                    err
                );
                err
            })?;

        #[cfg(feature = "test-utils")]
        let node_msg_clone = node_msg.clone();

        let wire_msg = WireMsg::for_dst_accumulation(&key_share, src, dst, node_msg, section_key)?;

        #[cfg(feature = "test-utils")]
        let wire_msg = wire_msg.set_payload_debug(node_msg_clone);

        trace!(
            "Send {:?} for accumulation at dst to {:?}",
            wire_msg,
            recipients
        );

        self.send_messages_to_all_nodes_or_directly_handle_for_accumulation(recipients, wire_msg)
            .await
    }
}
