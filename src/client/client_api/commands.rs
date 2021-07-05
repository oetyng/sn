// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Client;
use crate::client::Error;
use crate::messaging::{
    data::{DataCmd, ServiceMsg},
    cmd::ChargedOps,
    ServiceAuth, WireMsg,
};
use crate::types::{PublicKey, Signature};
use bytes::Bytes;
use tracing::debug;

impl Client {
    /// Send signed ops to the network.
    /// This is to be part of a public API, for the user to
    /// provide the serialised and already signed command.
    pub async fn send_signed_command(
        &self,
        ops: ChargedOps,
        client_pk: PublicKey,
        serialised_cmd: Bytes,
        signature: Signature,
    ) -> Result<(), Error> {
        debug!("Sending: {:?}", cmd);
        let auth = ServiceAuth {
            public_key: client_pk,
            signature,
        };

        self.session
            .send_cmd(ops, auth, serialised_cmd)
            .await
    }

    // Send a DataCmd to the network without awaiting for a response.
    // This function is a helper private to this module.
    pub(crate) async fn send_cmd(&self, cmd: ChargedOps) -> Result<(), Error> {
        let client_pk = self.public_key();
        let msg = ServiceMsg::Cmd(cmd.clone());
        let serialised_cmd = WireMsg::serialize_msg_payload(&msg)?;
        let signature = self.keypair.sign(&serialised_cmd);

        self.send_signed_command(cmd, client_pk, serialised_cmd, signature)
            .await
    }
}
