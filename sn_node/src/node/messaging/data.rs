// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::NodeContext, flow_ctrl::fault_detection::FaultsCmd, messaging::Peers, Cmd, Error, MyNode,
    Result,
};
use crate::storage::{Error as StorageError, StorageLevel};

use sn_interface::messaging::data::ClientMsg;
use sn_interface::messaging::system::NodeDataResponse;
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{DataCmd, DataQuery},
        system::{NodeDataCmd, NodeDataQuery, NodeEvent, NodeMsg},
        AuthorityProof, ClientAuth, MsgId,
    },
    types::{log_markers::LogMarker, Keypair, Peer, PublicKey, ReplicatedData},
};

use qp2p::SendStream;
use xor_name::XorName;

use itertools::Itertools;
use std::collections::BTreeSet;
use tracing::info;

impl MyNode {
    // Instruct data holders to store the data awaiting for their confirmation response to ack the client
    pub(crate) async fn store_data_at_nodes_and_ack_to_client(
        context: NodeContext,
        data_cmd: DataCmd,
        data: ReplicatedData,
        msg_id: MsgId,
        client_stream: SendStream,
        source_client: Peer,
    ) -> Result<Vec<Cmd>> {
        let data_name = data.name();
        let recipients = Self::target_data_holders(&context, data_name);

        // make sure the expected replication factor is achieved
        if data_copy_count() > recipients.len() {
            error!("InsufficientNodeCount for storing data reliably for {msg_id:?}");
            let error = Error::InsufficientNodeCount {
                prefix: context.network_knowledge.prefix(),
                expected: data_copy_count() as u8,
                found: recipients.len() as u8,
            };

            debug!("Will send error response back to client");

            let cmd = MyNode::send_cmd_error_response_over_stream(
                context,
                data_cmd,
                error,
                msg_id,
                client_stream,
                source_client,
            );
            return Ok(vec![cmd]);
        }

        info!("Replicating data from {msg_id:?} {data_name:?} to holders: {recipients:?}");

        // TODO: general ReplicateData flow could go bidi?
        // Right now we've a new msg for just one datum.
        // Atm that's perhaps more bother than its worth..

        let cmd = Cmd::SendAndTrackResponses {
            msg_id,
            msg: NodeMsg::NodeDataCmd(NodeDataCmd::StoreData(data)),
            context,
            recipients,
            causing_msg: ClientMsg::Cmd(data_cmd),
            client_stream,
        };

        Ok(vec![cmd])
    }

    /// Find target node, sends a bidi msg, awaiting response, and then sends this on to the client
    pub(crate) async fn read_data_and_respond_to_client(
        context: NodeContext,
        query: DataQuery,
        msg_id: MsgId,
        auth: AuthorityProof<ClientAuth>,
        source_client: Peer,
        client_stream: SendStream,
    ) -> Result<Vec<Cmd>> {
        // We accept that we might be sending a WireMsg to ourselves.
        // The extra load is not that big. But we can optimize this later if necessary.

        // We generate the operation id to track the response from the node
        // by using the query msg id, which shall be unique per query.
        // let operation_id = OperationId::from(&Bytes::copy_from_slice(msg_id.as_ref()));
        let address = query.variant.address();
        trace!(
            "{:?} preparing to query other nodes for data at {address:?} with op_id: {msg_id:?}",
            LogMarker::DataQueryReceviedAtElder,
        );

        let recipients = Self::target_data_holders(&context, *address.name());

        // We accept the chance that we will be querying an Elder that the client already queried directly.
        // The extra load is not that big. But we can optimize this later if necessary.

        // Query only the nth node
        let recipient = if let Some(peer) = recipients.iter().nth(query.node_index) {
            *peer
        } else {
            debug!("No recipients found for {msg_id:?}");
            let error = Error::InsufficientNodeCount {
                prefix: context.network_knowledge.prefix(),
                expected: query.node_index as u8 + 1,
                found: recipients.len() as u8,
            };

            let cmd = MyNode::send_query_error_response_over_stream(
                context,
                error,
                &query.variant,
                source_client,
                msg_id,
                client_stream,
            );
            return Ok(vec![cmd]);
        };

        // Form a msg to the node
        let msg = NodeMsg::NodeDataQuery(NodeDataQuery {
            query: query.variant.clone(),
            auth: auth.into_inner(),
        });

        let cmd = Cmd::SendAndTrackResponses {
            msg_id,
            msg,
            context,
            recipients: BTreeSet::from([recipient]),
            causing_msg: ClientMsg::Query(query),
            client_stream,
        };

        Ok(vec![cmd])
    }

    /// Registered holders not present in provided list of members
    /// will no longer be tracked for faults.
    pub(crate) async fn fault_detection_retain_only(
        &mut self,
        adults: BTreeSet<XorName>,
        elders: BTreeSet<XorName>,
    ) {
        // stop tracking liveness of absent holders
        if let Err(error) = self
            .fault_cmds_sender
            .send(FaultsCmd::UpdateNodes(adults, elders))
            .await
        {
            warn!("Could not send RetainNodes through fault_cmds_tx: {error}");
        };
    }

    /// Adds the new adult to the Liveness trackers.
    pub(crate) async fn add_new_adult_to_trackers(&mut self, adult: XorName) {
        info!("Adding new Adult: {adult} to trackers");
        if let Err(error) = self.fault_cmds_sender.send(FaultsCmd::AddNode(adult)).await {
            warn!("Could not send AddNode through fault_cmds_tx: {error}");
        };
    }

    /// Tracks an outgoing msg to data holders, and its corresponding responses.
    pub(crate) fn track_data_response(
        &mut self,
        msg_id: MsgId,
        client_name: XorName,
        causing_msg: ClientMsg,
        client_stream: SendStream,
        expected_responders: BTreeSet<Peer>,
    ) {
        self.data_responses.track(
            msg_id,
            client_name,
            causing_msg,
            client_stream,
            expected_responders,
        );
    }

    /// Updates a pending request with a msg from data holders.
    pub(crate) async fn update_data_response(
        &mut self,
        peer: Peer,
        response: NodeDataResponse,
    ) -> Result<Option<Cmd>> {
        let correlation_id = *response.correlation_id();
        if let Some((msg, client_name, send_stream)) = self.data_responses.update(peer, response) {
            return MyNode::send_client_response(
                msg,
                correlation_id,
                client_name,
                send_stream,
                self.context(),
            )
            .await;
        }
        self.clear_expired_data_trackers();
        Ok(None)
    }

    pub(crate) fn clear_expired_data_trackers(&mut self) {
        let timed_out_nodes = self.data_responses.clear_expired();
        for node in timed_out_nodes {
            error!("No response from {node:?} after timeout. Marking node as faulty");
            self.track_node_issue(node.name(), sn_fault_detection::IssueType::Communication);
        }
    }

    /// Used to fetch the list of holders for given name of data.
    /// Sorts members by closeness to data address, returns data_copy_count of them
    fn target_data_holders(context: &NodeContext, target: XorName) -> BTreeSet<Peer> {
        // TODO: reuse our_members_sorted_by_distance_to API when core is merged into upper layer
        let members = context.network_knowledge.members();

        trace!("Total members known about: {:?}", members.len());

        let candidates = members
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(&lhs.name(), &rhs.name()))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!("Target holders of {:?} are : {:?}", target, candidates,);

        candidates
    }

    /// Replicate data in the batch locally and then trigger further update reqeusts
    /// Requests for more data will go to sending node if there is more to come, or to the next
    /// furthest nodes if there was no data sent.
    pub(crate) async fn replicate_data_batch(
        context: &NodeContext,
        sender: Peer,
        data_batch: Vec<ReplicatedData>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        let section_pk = PublicKey::Bls(context.network_knowledge.section_key());
        let node_keypair = Keypair::Ed25519(context.keypair.clone());

        let mut is_full = false;
        let data_batch_is_empty = data_batch.is_empty();

        let mut new_storage_level_passed = false;

        for data in data_batch {
            let store_result = context
                .data_storage
                .store(&data, section_pk, node_keypair.clone())
                .await;

            // This may return a DatabaseFull error... but we should have reported StorageError::NotEnoughSpace
            // well before this
            match store_result {
                Ok(StorageLevel::NoChange) => debug!("Data item stored."),
                Ok(StorageLevel::Updated(_level)) => {
                    debug!("Data item stored.");
                    // we add a new node for every level of used space increment
                    if !new_storage_level_passed && !context.joins_allowed {
                        new_storage_level_passed = true;
                        cmds.push(Cmd::SetJoinsAllowed(true));
                    }
                }
                Err(StorageError::NotEnoughSpace) => {
                    // storage full
                    error!("Not enough space to store more data");

                    let node_id = PublicKey::from(context.keypair.public);
                    let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                        node_id,
                        data_address: data.address(),
                        full: true,
                    });
                    is_full = true;

                    cmds.push(MyNode::send_to_elders(context, msg))
                }
                Err(error) => {
                    // the rest seem to be non-problematic errors.. (?)
                    error!("Problem storing data, but it was ignored: {error}");
                }
            }
        }

        // As long as the data batch is not empty, we send back a query again
        // to continue the replication process (like pageing).
        // This means there that there will be a number of repeated `give-me-data -> here_you_go` msg
        // exchanges, until there is no more data missing on this node.
        if !is_full && !data_batch_is_empty {
            let data_i_have = context.data_storage.data_addrs().await;
            trace!(
                "{:?} - as batch was not empty",
                LogMarker::DataReorganisationUnderway
            );
            let msg = NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(data_i_have));
            let cmd = Cmd::send_msg(msg, Peers::Single(sender), context.clone());
            cmds.push(cmd);
        } else if is_full {
            warn!("Not attempting further retrieval of missing data as we are full");
        }

        Ok(cmds)
    }
}
