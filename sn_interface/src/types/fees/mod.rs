// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

/// Flow:
///  1. Client informs Elder s/he wishes to spend a dbc, and attaches the dbc id (a `PublicKey`).
///  2. Elder calculates required fee, and returns that plus the Elder's well-known reward public key to Client.
///  5. Client includes necessary Dbc output in the intended spend, with the fee amount, deriving a new dbc id
///     using `elder_reward_key` which is used in the Dbc output to denote the new Dbc destined to the Elder.
///  6. Client then constructs the `FeeCiphers`to be included in the `Spend` request.
///     The FeeCiphers consists of the following fields:
///         a. `derivation_index_cipher`: The encrypted derivation index used to derive the new dbc id.
///         b. `amount_cipher`: The encrypted amount + blinding factor (`RevealedAmount`) which was used in the Dbc output.
///  7. Client sends the `Spend` request to the Elder.
///  9. Elder verifies that:
///       a. the spend contains an output for them
///       b. the fee ciphers can be decrypted
///       c. the tx contains an output for a key derived from the Elder reward key using the decrypted derivation index
///       d. the amount in that output is the same as the decrypted amount
///       e. the decrypted amount is at most 1% less than the required fee at the time
///  10. Elder is satisfied, signs the spend and forwards it to data holders.
///
///      Note 1: The fee paid to the Elder is not actually accessible until the Elder can fetch the `SpendProof` from
///         the data holders.
///      Note 2: With 1 fee per spend, the fee amount in the dbc is not accessible to the Elder until the required fee for
///         a spend has decreased below the amount in the dbc. In effect, with this design there is currently a lock-in
///         effect on Elder rewards which require the network to grow for the amount paid to them to be accessible.
///         A more directly accessible reward design has been discussed, where Clients pay per tx instead of per spend.
///         Such a tx can contain any number of inputs to be spent, so a wise choice would be to multiply a base fee by number of inputs.
///         An example could be that the fee is be paid to the section closest to the XOR of all input dbc ids. Any Elder processing
///         a spend, would then have to find that section, query it for their reward keys and verify that outputs to them exist. However,
///         they can still not verify the amounts by this. So that example is still not feasible. TBD.
mod errors;
mod fee_ciphers;
mod priority;
mod spend_queue;

pub use self::{
    errors::{Error, Result},
    fee_ciphers::FeeCiphers,
    priority::SpendPriority,
    spend_queue::{SpendQ, SpendQSnapshot, SpendQStats},
};

use serde::{Deserialize, Serialize};
use sn_dbc::Token;

/// An Elder responds to a Client who wishes to spend a dbc,
/// informing the Client of the required fee for the spend.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequiredFee {
    pub amount: Token,
    pub reward_key: bls::PublicKey,
}
