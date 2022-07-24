// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(feature = "back-pressure")]
use sn_interface::types::Peer;

/// Commands for interacting with Comm.
#[derive(Debug, Clone)]
pub enum Cmd {
    #[cfg(feature = "back-pressure")]
    /// Set message rate for peer to the desired msgs per second
    Regulate {
        /// The peer requesting regulation of rate of msgs sent to it.
        peer: Peer,
        /// Desired msgs per second.
        msgs_per_s: f64,
    },
}
