// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::data_copy_count;
use crate::messaging::{
    data::{CmdError, DataCmd, DataQuery, ServiceMsg},
    system::{NodeQueryResponse, SystemMsg},
    DstLocation, EndUser, MessageId, MsgKind, NodeAuth, WireMsg,
};
use crate::messaging::{AuthorityProof, ServiceAuth};
use crate::node::{
    error::Result,
    routing::{api::command::Command, core::Core},
};
use crate::peer::Peer;
use crate::types::{crdts::User, log_markers::LogMarker, PublicKey, ReplicatedData};

use itertools::Itertools;
use std::{cmp::Ordering, collections::BTreeSet};
use xor_name::XorName;

impl Core {
    /// Forms a command to send the provided node error out
    pub(crate) fn send_cmd_error_response(
        &self,
        error: CmdError,
        target: Peer,
        msg_id: MessageId,
    ) -> Result<Vec<Command>> {
        let the_error_msg = ServiceMsg::CmdError {
            error,
            correlation_id: msg_id,
        };

        let dst = DstLocation::EndUser(EndUser(target.name()));

        // FIXME: define which signature/authority this message should really carry,
        // perhaps it needs to carry Node signature on a NodeMsg::QueryResponse msg type.
        // Giving a random sig temporarily
        let (msg_kind, payload) = Self::random_client_signature(&the_error_msg)?;
        let wire_msg = WireMsg::new_msg(MessageId::new(), payload, msg_kind, dst)?;

        let command = Command::SendMessage {
            recipients: vec![target],
            wire_msg,
        };

        Ok(vec![command])
    }

    /// Sign and serialize node message to be sent
    pub(crate) async fn prepare_node_msg(
        &self,
        msg: SystemMsg,
        dst: DstLocation,
    ) -> Result<Vec<Command>> {
        let msg_id = MessageId::new();
        let section_pk = self.network_knowledge().section_key().await;
        let payload = WireMsg::serialize_msg_payload(&msg)?;

        let auth = NodeAuth::authorize(section_pk, &self.node.read().await.keypair, &payload);
        let msg_kind = MsgKind::NodeAuthMsg(auth.into_inner());

        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst)?;
        let command = Command::ParseAndSendWireMsg(wire_msg);

        Ok(vec![command])
    }

    /// Handle data query
    pub(crate) async fn handle_data_query_at_adult(
        &self,
        msg_id: MessageId,
        query: &DataQuery,
        auth: ServiceAuth,
        user: EndUser,
        requesting_elder: XorName,
    ) -> Result<Vec<Command>> {
        trace!("Handling data query at adult");
        let mut commands = vec![];

        let msg = SystemMsg::NodeQueryResponse {
            response: self
                .data_storage
                .query(query, User::Key(auth.public_key))
                .await,
            correlation_id: msg_id,
            user,
        };

        // Setup node authority on this response and send this back to our elders
        let section_pk = self.network_knowledge().section_key().await;
        let dst = DstLocation::Node {
            name: requesting_elder,
            section_pk,
        };

        commands.push(Command::PrepareNodeMsgToSend { msg, dst });

        Ok(commands)
    }

    /// Handle data read
    /// Records response in liveness tracking
    /// Forms a response to send to the requester
    pub(crate) async fn handle_data_query_response_at_elder(
        &self,
        // msg_id: MessageId,
        correlation_id: MessageId,
        response: NodeQueryResponse,
        user: EndUser,
        sending_node_pk: PublicKey,
    ) -> Result<Vec<Command>> {
        let msg_id = MessageId::new();
        let mut commands = vec![];
        debug!(
            "Handling data read @ elders, received from {:?} ",
            sending_node_pk
        );

        let node_id = XorName::from(sending_node_pk);

        // let NodeQueryResponse::GetChunk(response) = response;

        // let origin = if let Some(origin) = self.pending_chunk_queries.remove(&user.0).await {
        //     // Check for data correctness now that we know the returned data XorName was tracked by us
        //     if let Ok(chunk) = &response {
        //         let received_name = XorName::from_content(chunk.value());
        //         if user.0 != received_name {
        //             warn!("Received response from the Adult did not correspond to the requested address. \n\
        //             Received content name {:?}: Requested name {:?}", received_name, user.0);
        //             warn!("Penalising the Adult and dropping this response");
        //             self.liveness.penalise_member(node_id).await;
        //             return Ok(commands);
        //         }
        //     }

        //     origin
        // } else {
        //     warn!(
        //         "Dropping query response from Adult {}. We might have already forwarded this response to the requesting client or \
        //         have not registered the client: {}",
        //         sending_node_pk, user.0
        //     );
        //     return Ok(commands);
        // };

        // // Clear expired queries from the cache.
        // self.pending_chunk_queries.remove_expired().await;

        // let query_response = QueryResponse::GetChunk(response);

        let origin = if let Some(origin) = self.pending_data_queries.remove(&user.0).await {
            origin
        } else {
            warn!(
                "Dropping chunk query response from Adult {}. We might have already forwarded this chunk to the requesting client or \
                have not registered the client: {}",
                sending_node_pk, user.0
            );
            return Ok(commands);
        };

        // Clear expired queries from the cache.
        self.pending_data_queries.remove_expired().await;

        let query_response = response.convert();

        let pending_removed = match query_response.operation_id() {
            Ok(op_id) => {
                self.liveness
                    .request_operation_fulfilled(&node_id, op_id)
                    .await
            }
            Err(error) => {
                warn!("Node problems noted when retrieving data: {:?}", error);
                false
            }
        };

        // Check for unresponsive adults here.
        for (name, count) in self.liveness.find_unresponsive_nodes().await {
            warn!(
                "Node {} has {} pending ops. It might be unresponsive",
                name, count
            );
            commands.push(Command::ProposeOffline(name));
        }

        if !pending_removed {
            trace!("Ignoring un-expected response");
            return Ok(commands);
        }

        // Send response if one is warrented
        if query_response.failed_with_data_not_found()
            || (!query_response.is_success()
                && self
                    .capacity
                    .is_full(&XorName::from(sending_node_pk))
                    .await
                    .unwrap_or(false))
        {
            // we don't return data not found errors.
            trace!("Node {:?}, reported data not found", sending_node_pk);

            return Ok(commands);
        }

        let msg = ServiceMsg::QueryResponse {
            response: query_response,
            correlation_id,
        };

        // FIXME: define which signature/authority this message should really carry,
        // perhaps it needs to carry Node signature on a NodeMsg::QueryResponse msg type.
        // Giving a random sig temporarily
        let (msg_kind, payload) = Self::random_client_signature(&msg)?;

        let dst = DstLocation::EndUser(EndUser(origin.name()));
        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst)?;

        trace!(
            "Responding with the first chunk query response to {:?}",
            dst
        );

        let command = Command::SendMessage {
            recipients: vec![origin],
            wire_msg,
        };
        commands.push(command);
        Ok(commands)
    }

    /// Handle ServiceMsgs received from EndUser
    pub(crate) async fn handle_service_msg_received(
        &self,
        msg_id: MessageId,
        msg: ServiceMsg,
        auth: AuthorityProof<ServiceAuth>,
        user: Peer,
    ) -> Result<Vec<Command>> {
        match msg {
            // These reads/writes are for adult nodes...
            ServiceMsg::Cmd(DataCmd::Register(cmd)) => {
                self.send_data_to_adults(ReplicatedData::RegisterWrite(cmd), msg_id, user)
                    .await
            }
            ServiceMsg::Cmd(DataCmd::StoreChunk(chunk)) => {
                self.send_data_to_adults(ReplicatedData::Chunk(chunk), msg_id, user)
                    .await
            }
            ServiceMsg::Query(query) => self.read_data_from_adults(query, msg_id, auth, user).await,
            _ => {
                warn!("!!!! Unexpected ServiceMsg received in routing. Was not sent to node layer: {:?}", msg);
                Ok(vec![])
            }
        }
    }

    // Used to fetch the list of holders for given data name.
    pub(crate) async fn get_adults_holding_data(&self, target: &XorName) -> BTreeSet<XorName> {
        let full_adults = self.full_adults().await;
        // TODO: reuse our_adults_sorted_by_distance_to API when core is merged into upper layer
        let adults = self.network_knowledge().adults().await;

        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let mut candidates = adults_names
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(lhs, rhs))
            .filter(|peer| !full_adults.contains(peer))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!(
            "Chunk holders of {:?} are empty adults: {:?} and full adults: {:?}",
            target,
            candidates,
            full_adults
        );

        // Full adults that are close to the chunk, shall still be considered as candidates
        // to allow chunks stored to empty adults can be queried when nodes become full.
        let close_full_adults = if let Some(closest_empty) = candidates.iter().next() {
            full_adults
                .iter()
                .filter_map(|name| {
                    if target.cmp_distance(name, closest_empty) == Ordering::Less {
                        Some(*name)
                    } else {
                        None
                    }
                })
                .collect::<BTreeSet<_>>()
        } else {
            // In case there is no empty candidates, query all full_adults
            full_adults
        };

        candidates.extend(close_full_adults);
        candidates
    }

    // Used to fetch the list of holders for given name of data.
    pub(crate) async fn get_adults_who_should_store_data(
        &self,
        target: XorName,
    ) -> BTreeSet<XorName> {
        let full_adults = self.full_adults().await;
        // TODO: reuse our_adults_sorted_by_distance_to API when core is merged into upper layer
        let adults = self.network_knowledge().adults().await;

        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let candidates = adults_names
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(lhs, rhs))
            .filter(|peer| !full_adults.contains(peer))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!(
            "Target chunk holders of {:?} are empty adults: {:?} and full adults that were ignored: {:?}",
            target,
            candidates,
            full_adults
        );

        candidates
    }

    /// Handle incoming data msgs.
    pub(crate) async fn handle_service_message(
        &self,
        msg_id: MessageId,
        msg: ServiceMsg,
        dst_location: DstLocation,
        auth: AuthorityProof<ServiceAuth>,
        user: Peer,
    ) -> Result<Vec<Command>> {
        trace!("{:?} {:?}", LogMarker::ServiceMsgToBeHandled, msg);
        if let DstLocation::EndUser(_) = dst_location {
            warn!(
                "Service msg has been dropped as its destination location ({:?}) is invalid: {:?}",
                dst_location, msg
            );
            return Ok(vec![]);
        }

        self.handle_service_msg_received(msg_id, msg, auth, user)
            .await
    }
}
