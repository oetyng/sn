// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{metadata::Metadata, node_api::BlsKeyManager, payments::Payments};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub(crate) struct ElderRole {
    // data operations
    pub meta_data: Arc<RwLock<Metadata>>,
    // payments
    pub payments: Arc<RwLock<Payments<BlsKeyManager>>>,
    // denotes if we received initial sync
    pub received_initial_sync: Arc<RwLock<bool>>,
}

impl ElderRole {
    pub fn new(
        meta_data: Metadata,
        payments: Payments<BlsKeyManager>,
        received_initial_sync: bool,
    ) -> Self {
        ElderRole {
            meta_data: Arc::new(RwLock::new(meta_data)),
            payments: Arc::new(RwLock::new(payments)),
            received_initial_sync: Arc::new(RwLock::new(received_initial_sync)),
        }
    }
}
