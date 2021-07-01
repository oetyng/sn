// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    node::{NodeCmd, NodeMsg, NodeQueryResponse, NodeSystemCmd, NodeSystemQueryResponse},
    Aggregation, DstLocation, MessageId, SrcLocation,
};
use crate::node::{
    network::Network,
    node_ops::{MsgType, NodeDuties, NodeDuty, OutgoingMsg},
    Node, Result,
};
use crate::routing::{Prefix, XorName};
use crate::types::{PublicKey, SectionElders};
use std::collections::BTreeSet;

use super::role::ElderRole;

impl Node {
    /// https://github.com/rust-lang/rust-clippy/issues?q=is%3Aissue+is%3Aopen+eval_order_dependence
    #[allow(clippy::eval_order_dependence)]
    pub(crate) async fn get_section_elders(
        network_api: &Network,
        msg_id: MessageId,
        origin: SrcLocation,
    ) -> Result<NodeDuties> {
        let elders = SectionElders {
            prefix: network_api.our_prefix().await,
            names: network_api.our_elder_names().await,
            key_set: network_api.our_public_key_set().await?,
        };
        Ok(vec![NodeDuty::Send(OutgoingMsg {
            msg: MsgType::Node(NodeMsg::NodeQueryResponse {
                response: NodeQueryResponse::System(NodeSystemQueryResponse::GetSectionElders(
                    elders,
                )),
                correlation_id: msg_id,
                id: MessageId::in_response_to(&msg_id),
            }),
            section_source: false, // strictly this is not correct, but we don't expect responses to a response..
            dst: origin.to_dst(),  // this will be a section
            aggregation: Aggregation::AtDestination, // None,
        })])
    }

    ///
    pub(crate) async fn notify_section_of_our_storage(network_api: &Network) -> Result<NodeDuty> {
        let node_id = PublicKey::from(network_api.public_key().await);
        Ok(NodeDuty::Send(OutgoingMsg {
            msg: MsgType::Node(NodeMsg::NodeCmd {
                cmd: NodeCmd::System(NodeSystemCmd::StorageFull {
                    section: node_id.into(),
                    node_id,
                }),
                id: MessageId::new(),
            }),
            section_source: false, // sent as single node
            dst: DstLocation::Section(node_id.into()),
            aggregation: Aggregation::None,
        }))
    }
}

/// Push our state to the given dst
pub(crate) async fn push_state(
    elder: &ElderRole,
    prefix: Prefix,
    msg_id: MessageId,
    peers: BTreeSet<XorName>,
) -> Result<NodeDuty> {
    // Create an aggregated map of all the metadata of the provided prefix
    let metadata = elder
        .meta_data
        .read()
        .await
        .get_data_exchange_packet(prefix)
        .await?;

    Ok(NodeDuty::SendToNodes {
        msg: NodeMsg::NodeCmd {
            cmd: NodeCmd::System(NodeSystemCmd::ReceiveExistingData { metadata }),
            id: msg_id,
        },
        targets: peers,
        aggregation: Aggregation::None,
    })
}
