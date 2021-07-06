// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::role::{ElderRole, Role};
use crate::messaging::client::DataExchange;
use crate::node::{
    capacity::OpCost,
    capacity::{AdultsStorageInfo, Capacity, CapacityReader, CapacityWriter},
    metadata::{adult_reader::AdultReader, Metadata},
    node_api::BlsKeyManager,
    node_ops::NodeDuty,
    payments::Payments,
    Node, Result,
};
use crate::types::{NodeAge, PublicKey};
use sn_dbc::Mint;
use std::collections::BTreeMap;
use tracing::info;
use xor_name::XorName;

impl Node {
    /// Level up a newbie to an oldie on promotion
    pub async fn level_up(&self) -> Result<()> {
        let adult_storage_info = AdultsStorageInfo::new();
        let adult_reader = AdultReader::new(self.network_api.clone());
        let capacity_reader = CapacityReader::new(adult_storage_info.clone(), adult_reader.clone());
        let capacity_writer = CapacityWriter::new(adult_storage_info.clone(), adult_reader.clone());
        let capacity = Capacity::new(capacity_reader.clone(), capacity_writer);

        //
        // start handling metadata
        let meta_data = Metadata::new(
            &self.node_info.path(),
            self.used_space.clone(),
            capacity.clone(),
        )
        .await?;

        //
        // start handling payments
        let store_cost = OpCost::new(self.network_api.clone(), capacity_reader.clone());
        let reward_wallets = crate::node::payments::RewardWallets::new(BTreeMap::<
            XorName,
            (NodeAge, PublicKey),
        >::new());
        let key_mgr = BlsKeyManager::new(self.network_api.clone());
        let mint = Mint::new(key_mgr);
        let payments = Payments::new(store_cost, reward_wallets, mint);

        *self.role.write().await = Role::Elder(ElderRole::new(meta_data, payments, false));

        Ok(())
    }

    /// Continue the level up and handle more responsibilities.
    pub(crate) async fn synch_state(elder: &ElderRole, metadata: DataExchange) -> Result<NodeDuty> {
        if *elder.received_initial_sync.read().await {
            info!("We are already received the initial sync from our section. Ignoring update");
            return Ok(NodeDuty::NoOp);
        }
        // --------- merge in provided metadata ---------
        elder.meta_data.write().await.update(metadata).await?;

        *elder.received_initial_sync.write().await = true;

        Ok(NodeDuty::NoOp)
    }
}
