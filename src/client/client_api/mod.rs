// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod batching;
mod blob_apis;
mod blob_storage;
mod commands;
mod queries;
mod register_apis;

use crate::client::{config_handler::Config, connections::Session, errors::Error};
use crate::messaging::cmd::BatchedWrites;
use crate::messaging::data::{CmdError, DataCmd, ServiceMsg};
use crate::messaging::payment::{CostInquiry, GuaranteedQuote, PaymentReceipt};
use crate::messaging::query::Query;
use crate::messaging::{ServiceAuth, WireMsg};
use crate::types::{Chunk, ChunkAddress, Keypair, PublicKey};
use lru::LruCache;
use rand::rngs::OsRng;
use std::collections::BTreeSet;
use std::{
    path::Path,
    {collections::HashSet, net::SocketAddr, sync::Arc},
};
use tokio::{
    sync::{mpsc::Receiver, RwLock},
    time::Duration,
};
use tracing::{debug, info, trace};

// Number of attempts to make when trying to bootstrap to the network
const NUM_OF_BOOTSTRAPPING_ATTEMPTS: u8 = 1;
const BLOB_CACHE_CAP: usize = 150;

/// Client object
#[derive(Clone, Debug)]
pub struct Client {
    keypair: Keypair,
    incoming_errors: Arc<RwLock<Receiver<CmdError>>>,
    session: Session,
    blob_cache: Arc<RwLock<LruCache<ChunkAddress, Chunk>>>,
    pub(crate) query_timeout: Duration,
}

/// Easily manage connections to/from The Safe Network with the client and its APIs.
/// Use a random client for read-only or one-time operations.
/// Supply an existing, SecretKey which holds a SafeCoin balance to be able to perform
/// write operations.
impl Client {
    /// Create a Safe Network client instance. Either for an existing SecretKey (in which case) the client will attempt
    /// to retrieve the history of the key's balance in order to be ready for any token operations. Or if no SecreteKey
    /// is passed, a random keypair will be used, which provides a client that can only perform Read operations (at
    /// least until the client's SecretKey receives some token).
    ///
    /// # Examples
    ///
    /// TODO: update once data types are crdt compliant
    ///
    pub async fn new(
        optional_keypair: Option<Keypair>,
        config_file_path: Option<&Path>,
        bootstrap_config: Option<HashSet<SocketAddr>>,
        query_timeout: u64,
    ) -> Result<Self, Error> {
        let mut rng = OsRng;

        let keypair = match optional_keypair {
            Some(id) => {
                info!("Client started for specific pk: {:?}", id.public_key());
                id
            }
            None => {
                let keypair = Keypair::new_ed25519(&mut rng);
                info!(
                    "Client started for new randomly created pk: {:?}",
                    keypair.public_key()
                );
                keypair
            }
        };

        let mut qp2p_config = Config::new(config_file_path, bootstrap_config).await.qp2p;
        // We use feature `no-igd` so this will use the echo service only
        qp2p_config.forward_port = true;

        // Incoming error notifiers
        let (err_sender, err_receiver) = tokio::sync::mpsc::channel::<CmdError>(10);

        let client_pk = keypair.public_key();

        // Create the session with the network
        let mut session = Session::new(client_pk, qp2p_config, err_sender)?;

        // Bootstrap to the network, connecting to the section responsible
        // for our client public key
        debug!("Bootstrapping to the network...");
        attempt_bootstrap(&mut session).await?;

        let client = Self {
            keypair,
            session,
            incoming_errors: Arc::new(RwLock::new(err_receiver)),
            query_timeout: Duration::from_secs(query_timeout),
            blob_cache: Arc::new(RwLock::new(LruCache::new(BLOB_CACHE_CAP))),
        };

        Ok(client)
    }

    /// Return the client's FullId.
    ///
    /// Useful for retrieving the PublicKey or KeyPair in the event you need to _sign_ something
    ///
    /// # Examples
    ///
    /// TODO: update once data types are crdt compliant
    ///
    pub fn keypair(&self) -> Keypair {
        self.keypair.clone()
    }

    /// Return the client's PublicKey.
    ///
    /// # Examples
    ///
    /// TODO: update once data types are crdt compliant
    ///
    pub fn public_key(&self) -> PublicKey {
        self.keypair().public_key()
    }

    // Private helper to obtain payment proof for a data command, send it to the network,
    // and also apply the payment to local replica actor.
    async fn pay_and_send_data_command(&self, cmds: Vec<DataCmd>) -> Result<(), Error> {
        // Get quote for write
        let quote = self
            .get_quote(CostInquiry {
                chunks: BTreeSet::new(),
                reg_ops: BTreeSet::new(),
            })
            .await?;
        // Generate payment matching the quote
        let payment = self.generate_payment(quote).await?;

        // The _actual_ message
        let cmd = BatchedWrites {
            uploads: Vec::new(),
            edits: Vec::new(),
            payment,
        };

        // Send the message to the network
        self.send_cmd(cmd).await
    }

    ///
    pub async fn get_quote(&self, inquiry: CostInquiry) -> Result<GuaranteedQuote, Error> {
        let peer = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(1, 2, 3, 4)),
            1234,
        );

        let dst = inquiry.dst_address();
        self.session
            .send_get_section_query(inquiry.dst_address(), &peer)
            .await?;
        let query = Query::Payment(inquiry);
        let msg = ServiceMsg::Query(query.clone());
        let serialized = WireMsg::serialize_msg_payload(&msg)?;

        let auth = ServiceAuth {
            public_key: self.keypair.public_key(),
            signature: self.keypair.sign(&serialized),
        };

        let _ = self.session.send_query(query, auth, serialized).await;
        unimplemented!()
    }

    ///
    pub async fn generate_payment(&self, _quote: GuaranteedQuote) -> Result<PaymentReceipt, Error> {
        unimplemented!()
        // Ok(PaymentReceipt {
        //     data: BTreeSet::new(),
        //     sig,
        //     key_set,
        // })
    }

    #[cfg(test)]
    pub async fn expect_cmd_error(&mut self) -> Option<CmdError> {
        self.incoming_errors.write().await.recv().await
    }
}

/// Utility function that bootstraps a client to the network. If there is a failure then it retries.
/// After a maximum of three attempts if the boostrap process still fails, then an error is returned.
async fn attempt_bootstrap(session: &mut Session) -> Result<(), Error> {
    let mut attempts: u8 = 0;
    loop {
        match session.bootstrap().await {
            Ok(()) => return Ok(()),
            Err(err) => {
                attempts += 1;
                if attempts < NUM_OF_BOOTSTRAPPING_ATTEMPTS {
                    trace!(
                        "Error connecting to network! {:?}\nRetrying... ({})",
                        err,
                        attempts
                    );
                } else {
                    return Err(err);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::utils::test_utils::{create_test_client, create_test_client_with};
    use anyhow::Result;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[tokio::test(flavor = "multi_thread")]
    async fn client_creation() -> Result<()> {
        let _client = create_test_client(None).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn client_nonsense_bootstrap_fails() -> Result<()> {
        let mut nonsense_bootstrap = HashSet::new();
        let _ = nonsense_bootstrap.insert(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            3033,
        ));
        //let setup = create_test_client_with(None, Some(nonsense_bootstrap)).await;
        //assert!(setup.is_err());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn client_creation_with_existing_keypair() -> Result<()> {
        let mut rng = OsRng;
        let full_id = Keypair::new_ed25519(&mut rng);
        let pk = full_id.public_key();

        let client = create_test_client_with(Some(full_id), None).await?;
        assert_eq!(pk, client.public_key());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn long_lived_connection_survives() -> Result<()> {
        let client = create_test_client(None).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(40)).await;

        let _ = client.store_private_blob(&[0, 1, 2, 3, 4]).await?;

        Ok(())
    }
}
