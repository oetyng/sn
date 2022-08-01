// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::cmds::Cmd,
    messaging::{OutgoingMsg, Recipients},
    Error, Node, Result,
};

use sn_dbc::{
    Commitment, Hash, IndexedSignatureShare, KeyImage, RingCtTransaction, SpentProof,
    SpentProofContent, SpentProofShare,
};
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{
            CmdError, DataCmd, DataQueryVariant, EditRegister, Error as ErrorMsg, ServiceMsg,
            SignedRegisterEdit, SpentbookCmd,
        },
        system::{NodeQueryResponse, SystemMsg},
        AuthorityProof, EndUser, MsgId, ServiceAuth,
    },
    types::{
        log_markers::LogMarker,
        register::{Permissions, Policy, Register, User},
        Keypair, Peer, PublicKey, RegisterCmd, ReplicatedData, SPENTBOOK_TYPE_TAG,
    },
};

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use xor_name::XorName;

#[cfg(feature = "traceroute")]
use sn_interface::messaging::Entity;

impl Node {
    /// Forms a `CmdError` msg to send back to the client
    pub(crate) fn send_cmd_error_response(
        &self,
        error: CmdError,
        target: Peer,
        msg_id: MsgId,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Result<Vec<Cmd>> {
        let the_error_msg = ServiceMsg::CmdError {
            error,
            correlation_id: msg_id,
        };
        self.send_cmd_response(
            target,
            the_error_msg,
            #[cfg(feature = "traceroute")]
            traceroute,
        )
    }

    /// Forms a `CmdAck` msg to send back to the client
    pub(crate) fn send_cmd_ack(
        &self,
        target: Peer,
        msg_id: MsgId,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Result<Vec<Cmd>> {
        let the_ack_msg = ServiceMsg::CmdAck {
            correlation_id: msg_id,
        };
        self.send_cmd_response(
            target,
            the_ack_msg,
            #[cfg(feature = "traceroute")]
            traceroute,
        )
    }

    /// Forms a cmd to send a cmd response error/ack to the client
    fn send_cmd_response(
        &self,
        target: Peer,
        msg: ServiceMsg,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Result<Vec<Cmd>> {
        let cmd = Cmd::SendMsg {
            msg: OutgoingMsg::Service(msg),
            recipients: Recipients::from_single(target),
            #[cfg(feature = "traceroute")]
            traceroute,
        };
        Ok(vec![cmd])
    }

    /// Handle data query
    pub(crate) async fn handle_data_query_at_adult(
        &self,
        correlation_id: MsgId,
        query: &DataQueryVariant,
        auth: ServiceAuth,
        user: EndUser,
        requesting_elder: Peer,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        let response = self
            .data_storage
            .query(query, User::Key(auth.public_key))
            .await;

        trace!("data query response at adult is: {:?}", response);
        let msg = SystemMsg::NodeQueryResponse {
            response,
            correlation_id,
            user,
        };

        cmds.push(Cmd::SendMsg {
            msg: OutgoingMsg::System(msg),
            recipients: Recipients::from_single(requesting_elder),
            #[cfg(feature = "traceroute")]
            traceroute,
        });

        Ok(cmds)
    }

    /// Handle data read
    /// Records response in liveness tracking
    /// Forms a response to send to the requester
    pub(crate) async fn handle_data_query_response_at_elder(
        &mut self,
        correlation_id: MsgId,
        response: NodeQueryResponse,
        user: EndUser,
        sending_node_pk: PublicKey,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        let op_id = response.operation_id()?;
        debug!(
            "Handling data read @ elders, received from {:?}, op id: {:?}",
            sending_node_pk, op_id
        );

        let node_id = XorName::from(sending_node_pk);

        let querys_peers = self.pending_data_queries.remove(&op_id);
        // Clear expired queries from the cache.
        self.pending_data_queries.remove_expired();

        // First check for waiting peers. If no one is waiting, we drop the response
        let waiting_peers = if let Some(peers) = querys_peers {
            peers
        } else {
            warn!(
                "Dropping chunk query response from Adult {}. We might have already forwarded this chunk to the requesting client or the client connection cache has expired: {}",
                sending_node_pk, user.0
            );

            return Ok(cmds);
        };

        if waiting_peers.is_empty() {
            // nothing to do
            return Ok(cmds);
        }

        let query_response = response.convert();

        let pending_removed = self
            .dysfunction_tracking
            .request_operation_fulfilled(&node_id, op_id);

        if !pending_removed {
            trace!("Ignoring un-expected response");
            return Ok(cmds);
        }

        // dont reply if data not found (but do keep peers around...)
        if query_response.failed_with_data_not_found()
            || (!query_response.is_success()
                && self
                    .capacity
                    .is_full(&XorName::from(sending_node_pk))
                    .unwrap_or(false))
        {
            // lets requeue waiting peers in case another adult has the data...
            // if no more responses come in this query should eventually time out
            // TODO: What happens if we keep getting queries / client for some data that's always not found?
            // We need to handle that
            let _prev = self.pending_data_queries.set(op_id, waiting_peers, None);
            trace!(
                "Node {:?}, reported data not found {:?}",
                sending_node_pk,
                op_id
            );
            return Ok(cmds);
        }

        let msg = ServiceMsg::QueryResponse {
            response: query_response,
            correlation_id,
        };

        cmds.push(Cmd::SendMsg {
            msg: OutgoingMsg::Service(msg),
            recipients: Recipients::Peers(waiting_peers),
            #[cfg(feature = "traceroute")]
            traceroute,
        });

        Ok(cmds)
    }

    /// Handle incoming service msgs. Though NOT queries, as this requires
    /// mutable access to the Node
    pub(crate) async fn handle_valid_service_msg(
        &self,
        msg_id: MsgId,
        msg: ServiceMsg,
        origin: Peer,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?} {:?}", LogMarker::ServiceMsgToBeHandled, msg);

        // extract the data from the request
        let data = match msg {
            // These reads/writes are for adult nodes...
            ServiceMsg::Cmd(DataCmd::Register(cmd)) => ReplicatedData::RegisterWrite(cmd),
            ServiceMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
                key_image,
                tx,
                spent_proofs,
                spent_transactions,
            })) => {
                // Generate and sign spent proof share
                if let Some(spent_proof_share) =
                    self.gen_spent_proof_share(&key_image, &tx, &spent_proofs, &spent_transactions)?
                {
                    // Store spent proof share to adults
                    let reg_cmd = self.gen_register_cmd(&key_image, &spent_proof_share)?;
                    ReplicatedData::SpentbookWrite(reg_cmd)
                } else {
                    return Ok(vec![]);
                }
            }
            ServiceMsg::Cmd(DataCmd::StoreChunk(chunk)) => ReplicatedData::Chunk(chunk),
            _ => {
                warn!(
                    "!!!! Unexpected ServiceMsg received, and it was not handled: {:?}",
                    msg
                );
                return Ok(vec![]);
            }
        };

        // build the replication cmds
        let mut cmds = self.replicate_data(
            data,
            #[cfg(feature = "traceroute")]
            traceroute.clone(),
        )?;
        // make sure the expected replication factor is achieved
        if data_copy_count() > cmds.len() {
            error!("InsufficientAdults for storing data reliably");
            let error = CmdError::Data(ErrorMsg::InsufficientAdults {
                prefix: self.network_knowledge().prefix(),
                expected: data_copy_count() as u8,
                found: cmds.len() as u8,
            });
            return self.send_cmd_error_response(
                error,
                origin,
                msg_id,
                #[cfg(feature = "traceroute")]
                traceroute,
            );
        }

        cmds.extend(self.send_cmd_ack(
            origin,
            msg_id,
            #[cfg(feature = "traceroute")]
            traceroute,
        )?);

        Ok(cmds)
    }

    /// Handle incoming service msgs.
    pub(crate) async fn handle_valid_query_msg(
        &mut self,
        msg_id: MsgId,
        msg: ServiceMsg,
        auth: AuthorityProof<ServiceAuth>,
        origin: Peer,
        #[cfg(feature = "traceroute")] traceroute: Vec<Entity>,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?} {:?}", LogMarker::QueryServiceMsgToBeHandled, msg);
        // Check we're handling a query and continue
        match msg {
            ServiceMsg::Query(query) => {
                return self
                    .read_data_from_adults(
                        query,
                        msg_id,
                        auth,
                        origin,
                        #[cfg(feature = "traceroute")]
                        traceroute,
                    )
                    .await;
            }
            _ => {
                warn!(
                    "Unexpected ServiceMsg received while handling QUERY msgs, and it was not handled: {:?}",
                    msg
                );
                Ok(vec![])
            }
        }
    }

    // Private helper to generate spent proof share
    fn gen_spent_proof_share(
        &self,
        key_image: &KeyImage,
        tx: &RingCtTransaction,
        spent_proofs: &BTreeSet<SpentProof>,
        spent_transactions: &BTreeSet<RingCtTransaction>,
    ) -> Result<Option<SpentProofShare>> {
        trace!(
            "Processing DBC spend request for key image: {:?}",
            key_image
        );

        // Verify the SpentProofs signatures are all valid
        let mut spent_proofs_keys = BTreeSet::new();
        for proof in spent_proofs.iter() {
            if !proof
                .spentbook_pub_key
                .verify(&proof.spentbook_sig, proof.content.hash().as_ref())
            {
                debug!(
                    "Dropping DBC spend request since a SpentProof signature is invalid: {:?}",
                    proof.spentbook_pub_key
                );
                return Ok(None);
            }
            let _ = spent_proofs_keys.insert(proof.spentbook_pub_key);
        }

        // ...and verify the SpentProofs are signed by section keys known to us,
        // unless the public key of the SpentProof is the genesis key
        spent_proofs_keys
            .iter()
            .for_each(|pk| if !self.network_knowledge.verify_section_key_is_known(pk) {
                warn!("Invalid DBC spend request (key_image: {:?}) since a SpentProof is not signed by a section known to us: {:?}", key_image, pk);
                // TODO: temporarily allowing spent proofs signed by section keys we are not aware of.
                // We shall return an error to the client so it can update us with a valid proof chain.
                //return Ok(None);
            });

        // Obtain Commitments from the TX
        let mut public_commitments_info = Vec::<(KeyImage, Vec<Commitment>)>::new();
        for mlsag in tx.mlsags.iter() {
            // For each public key in ring, look up the matching Commitment
            // using the SpentProofs and spent TX set provided by the client.
            let commitments: Vec<Commitment> = mlsag
                .public_keys()
                .iter()
                .flat_map(|input_pk| {
                    spent_proofs.iter().flat_map(move |proof| {
                        // Make sure the spent proof corresponds to any of the spent TX provided,
                        // and the TX output PK matches the ring PK
                        spent_transactions.iter().filter_map(|spent_tx| {
                            let tx_hash = Hash::from(spent_tx.hash());
                            if tx_hash == proof.transaction_hash() {
                                spent_tx
                                    .outputs
                                    .iter()
                                    .find(|output| output.public_key() == &input_pk.clone())
                                    .map(|output| output.commitment())
                            } else {
                                None
                            }
                        })
                    })
                })
                .collect();

            if commitments.len() != mlsag.public_keys().len() {
                debug!("Dropping DBC spend request since the number of SpentProofs ({}) does not match the number of input public keys ({:?})",
                        commitments.len(),
                        mlsag.public_keys(),
                    );
                return Ok(None);
            }

            public_commitments_info.push((mlsag.key_image.into(), commitments));
        }

        // Do not sign invalid TX.
        let tx_public_commitments: Vec<Vec<Commitment>> = public_commitments_info
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect();

        if let Err(err) = tx.verify(&tx_public_commitments) {
            debug!(
                "Dropping DBC spend request since TX failed to verify: {:?}",
                err
            );
            return Ok(None);
        }

        // TODO:
        // Check the key_image wasn't already spent with a different TX (i.e. double spent)

        // Grab the commitments specific to the spent KeyImage
        let public_commitments: Vec<Commitment> = public_commitments_info
            .into_iter()
            .flat_map(|(k, v)| if &k == key_image { v } else { vec![] })
            .collect();

        let content = SpentProofContent {
            key_image: *key_image,
            transaction_hash: Hash::from(tx.hash()),
            public_commitments,
        };

        let sap = self.network_knowledge.authority_provider();

        let (index, sig_share) = self
            .section_keys_provider
            .sign_with(content.hash().as_ref(), &sap.section_key())?;

        Ok(Some(SpentProofShare {
            content,
            spentbook_pks: sap.public_key_set(),
            spentbook_sig_share: IndexedSignatureShare::new(index as u64, sig_share),
        }))
    }

    // Private helper to generate the RegisterCmd to write the SpentProofShare
    // as an entry in the Spentbook (Register).
    fn gen_register_cmd(
        &self,
        key_image: &KeyImage,
        spent_proof_share: &SpentProofShare,
    ) -> Result<RegisterCmd> {
        let mut permissions = BTreeMap::new();
        let _ = permissions.insert(User::Anyone, Permissions::new(true));

        // use our own keypair for generating the register command
        let own_keypair = Keypair::Ed25519(self.info().keypair);
        let owner = User::Key(own_keypair.public_key());
        let policy = Policy { owner, permissions };

        let mut register = Register::new(
            owner,
            XorName::from_content(&key_image.to_bytes()),
            SPENTBOOK_TYPE_TAG,
            policy,
            u16::MAX,
        );

        let entry = Bytes::from(rmp_serde::to_vec_named(spent_proof_share).map_err(|err| {
            Error::SpentbookError(format!(
                "Failed to serialise SpentProofShare to insert it into the spentbook (Register): {:?}",
                err
            ))
        })?);

        let (_, op) = register.write(entry.to_vec(), BTreeSet::default())?;
        let op = EditRegister {
            address: *register.address(),
            edit: op,
        };

        let signature = own_keypair.sign(&bincode::serialize(&op)?);
        let signed_edit = SignedRegisterEdit {
            op,
            auth: ServiceAuth {
                public_key: own_keypair.public_key(),
                signature,
            },
        };

        Ok(RegisterCmd::Edit(signed_edit))
    }
}
