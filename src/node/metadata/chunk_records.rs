// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::btree_set;
use crate::messaging::{
    data::{ChunkDataExchange, ChunkRead, ChunkWrite, CmdError, QueryResponse},
    node::{NodeCmd, NodeMsg, NodeQuery},
    Authority, EndUser, MessageId, ServiceOpSig,
};
use crate::node::{
    capacity::{Capacity, CHUNK_COPY_COUNT},
    error::convert_to_error_message,
    network::Network,
    node_ops::{NodeDuties, NodeDuty},
    Error, Result,
};
use crate::routing::Prefix;
use crate::types::{Chunk, ChunkAddress, PublicKey};
use tracing::{info, warn};

use std::{
    collections::BTreeSet,
    fmt::{self, Display, Formatter},
};
use xor_name::XorName;

use super::{
    adult_liveness::AdultLiveness, build_client_error_response, build_client_query_response,
    build_forward_query_response,
};

/// Operations over the data type Blob.
pub(super) struct ChunkRecords {
    capacity: Capacity,
    adult_liveness: AdultLiveness,
}

impl ChunkRecords {
    pub(super) fn new(capacity: Capacity) -> Self {
        Self {
            capacity,
            adult_liveness: AdultLiveness::new(),
        }
    }

    pub(super) async fn get_data_of(&self, prefix: Prefix) -> ChunkDataExchange {
        // Prepare full_adult details
        let full_adults = self.capacity.full_adults_matching(prefix).await;
        ChunkDataExchange { full_adults }
    }

    pub(super) async fn update(&self, chunk_data: ChunkDataExchange) {
        let ChunkDataExchange { full_adults } = chunk_data;
        self.capacity.insert_full_adults(full_adults).await
    }

    /// Registered holders not present in provided list of members
    /// will be removed from adult_storage_info and no longer tracked for liveness.
    pub(super) async fn retain_members_only(&self, members: BTreeSet<XorName>) -> Result<()> {
        // full adults
        self.capacity.retain_members_only(&members).await;

        // stop tracking liveness of absent holders
        self.adult_liveness.retain_members_only(members);

        Ok(())
    }

    pub(super) async fn write(
        &self,
        write: ChunkWrite,
        msg_id: MessageId,
        auth: Authority<ServiceOpSig>,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        use ChunkWrite::*;
        match write {
            New(data) => self.store(data, msg_id, auth, origin).await,
            DeletePrivate(address) => self.delete(address, auth, origin).await,
        }
    }

    /// Adds a given node to the list of full nodes.
    pub(super) async fn increase_full_node_count(&self, node_id: PublicKey) {
        let full_adults = self.capacity.full_adults_count().await;
        info!("No. of full Adults: {:?}", full_adults);
        info!("Increasing full Adults count");
        self.capacity
            .insert_full_adults(btree_set!(XorName::from(node_id)))
            .await;
    }

    /// Removes a given node from the list of full nodes.
    #[allow(unused)] // TODO: Remove node from full list at 50% ?
    async fn decrease_full_adults_count_if_present(&mut self, node_name: XorName) {
        let full_adults = self.capacity.full_adults_count().await;
        info!("No. of full Adults: {:?}", full_adults);
        info!("Checking if {:?} is present as full_node", node_name);
        self.capacity
            .remove_full_adults(btree_set!(node_name))
            .await;
    }

    async fn store(
        &self,
        chunk: Chunk,
        msg_id: MessageId,
        auth: Authority<ServiceOpSig>,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        if let Err(error) = validate_chunk_owner(&chunk, &auth.public_key) {
            return self.send_error(error, msg_id, origin).await;
        }

        // Let's send chunks to adults
        let target_holders = self.capacity.get_chunk_holder_adults(chunk.name()).await;
        info!("Storing {} copies of the chunk", target_holders.len());

        if CHUNK_COPY_COUNT > target_holders.len() {
            return self
                .send_error(
                    Error::NoAdults(self.capacity.our_prefix().await),
                    msg_id,
                    origin,
                )
                .await;
        }

        Ok(NodeDuty::SendToNodes {
            msg_id: MessageId::new(),
            msg: NodeMsg::NodeCmd(NodeCmd::Chunks {
                cmd: ChunkWrite::New(chunk),
                data_signed: auth.into_inner(),
                origin,
            }),
            targets: target_holders,
            aggregation: true,
        })
    }

    /// Needs attention!
    pub(super) async fn record_adult_read_liveness(
        &self,
        correlation_id: MessageId,
        response: QueryResponse,
        src: XorName,
        network: &Network,
    ) -> Result<NodeDuties> {
        if !matches!(response, QueryResponse::GetChunk(_)) {
            return Err(Error::Logic(format!(
                "Got {:?}, but only `GetChunk` query responses are supposed to exist in this flow.",
                response
            )));
        }
        let mut duties = vec![];
        // Removing correlation ids is bound to cause troubles,
        // as `DataNotFound` can come in before the `Ok` response comes in.
        if let Some((_address, end_user, origin_msg_id)) = self
            .adult_liveness
            .record_adult_read_liveness(&correlation_id, &src, response.is_success())
        {
            // If a full adult responds with error. Drop the response
            if (!response.is_success() && self.capacity.is_full(&src).await)
                || (matches!(
                    response,
                    QueryResponse::GetChunk(Err(crate::messaging::data::Error::DataNotFound(_)))
                ))
            {
                info!("REMOVED CORRELATION ID: {}", correlation_id);
                // We've already responded already with a success or the returned error is `DataNotFound`
                // so do nothing
            } else {
                info!("REMOVED CORRELATION ID: {}", correlation_id);
                let our_prefix = network.our_prefix().await;
                // If in same section, i.e. we are the ClientElders, then send response directly.
                // Otherwise, i.e. we are the DataElders, forwarding using ForwardServiceMsg.
                if our_prefix.matches(&end_user.xorname) {
                    duties.push(NodeDuty::Send(build_client_query_response(
                        response,
                        origin_msg_id,
                        end_user,
                    )));
                } else {
                    let section_pk_option = if let Ok(section_pk) =
                        network.get_section_pk_by_name(&end_user.xorname).await
                    {
                        section_pk.bls()
                    } else {
                        None
                    };
                    let section_pk = if let Some(section_pk) = section_pk_option {
                        section_pk
                    } else {
                        error!(
                            "Cannot get EndUser({:?})'s section's section key.",
                            end_user.xorname
                        );
                        return Err(Error::NoSectionPublicKey);
                    };
                    duties.push(NodeDuty::Send(build_forward_query_response(
                        response,
                        origin_msg_id,
                        end_user,
                        section_pk,
                    )?));
                }
            }
        } else if response.is_success() {
            info!(
                "NO READ OP FOUND FOR CORRELATION ID: {} (response: {:?})",
                correlation_id, response
            );
        }
        let mut unresponsive_adults = Vec::new();
        for (name, count) in self.adult_liveness.find_unresponsive_adults() {
            warn!(
                "Adult {} has {} pending ops. It might be unresponsive",
                name, count
            );
            unresponsive_adults.push(name);
        }
        if !unresponsive_adults.is_empty() {
            duties.push(NodeDuty::ProposeOffline(unresponsive_adults));
        }
        Ok(duties)
    }

    async fn send_error(
        &self,
        error: Error,
        msg_id: MessageId,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let error = convert_to_error_message(error);
        Ok(NodeDuty::Send(build_client_error_response(
            CmdError::Data(error),
            msg_id,
            origin,
        )))
    }

    async fn delete(
        &self,
        address: ChunkAddress,
        auth: Authority<ServiceOpSig>,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let targets = self.capacity.get_chunk_holder_adults(address.name()).await;

        Ok(NodeDuty::SendToNodes {
            msg_id: MessageId::new(),
            msg: NodeMsg::NodeCmd(NodeCmd::Chunks {
                cmd: ChunkWrite::DeletePrivate(address),
                data_signed: auth.into_inner(),
                origin,
            }),
            targets,
            aggregation: true,
        })
    }

    pub(super) async fn republish_chunk(&self, chunk: Chunk) -> Result<NodeDuty> {
        let target_holders = self.capacity.get_chunk_holder_adults(chunk.name()).await;
        let msg_id = MessageId::new();

        info!(
            "Republishing chunk {:?} to holders {:?} with MessageId {:?}",
            chunk.address(),
            &target_holders,
            msg_id
        );

        Ok(NodeDuty::SendToNodes {
            msg_id,
            msg: NodeMsg::NodeCmd(NodeCmd::ReplicateChunk(chunk)),
            targets: target_holders,
            aggregation: false,
        })
    }

    pub(super) async fn read(
        &self,
        read: &ChunkRead,
        msg_id: MessageId,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        match read {
            ChunkRead::Get(address) => self.get(*address, msg_id, origin).await,
        }
    }

    async fn get(
        &self,
        address: ChunkAddress,
        msg_id: MessageId,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        let targets = self.capacity.get_chunk_holder_adults(address.name()).await;

        if targets.is_empty() {
            return self
                .send_error(
                    Error::NoAdults(self.capacity.our_prefix().await),
                    msg_id,
                    origin,
                )
                .await;
        }

        // deterministic msg id based on content
        let new_msg_id = MessageId::from_content(&(msg_id, address))?;

        if self
            .adult_liveness
            .new_read(new_msg_id, address, origin, msg_id, targets.clone())
        {
            let msg = NodeMsg::NodeQuery(NodeQuery::Chunks {
                query: ChunkRead::Get(address),
                origin,
            });

            Ok(NodeDuty::SendToNodes {
                msg_id: new_msg_id,
                msg,
                targets,
                aggregation: false,
            })
        } else {
            info!(
                "Operation with MessageId {:?} is already in progress",
                msg_id
            );
            Ok(NodeDuty::NoOp)
        }
    }
}

fn validate_chunk_owner(chunk: &Chunk, requester: &PublicKey) -> Result<()> {
    if chunk.is_private() {
        chunk
            .owner()
            .ok_or_else(|| Error::InvalidOwner(*requester))
            .and_then(|chunk_owner| {
                if chunk_owner != requester {
                    Err(Error::InvalidOwner(*requester))
                } else {
                    Ok(())
                }
            })
    } else {
        Ok(())
    }
}

impl Display for ChunkRecords {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "ChunkRecords")
    }
}
