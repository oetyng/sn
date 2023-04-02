// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! SAFE network data types.

/// Fees for a spend.
pub mod fees;
/// public key types (ed25519)
pub mod keys;
/// Standardised log markers for various events
pub mod log_markers;
/// Register data type
pub mod register;
/// Encoding utils
pub mod utils;

mod address;
mod cache;
mod chunk;
mod errors;
mod identities;

pub use crate::messaging::{
    data::{Error as DataError, RegisterCmd},
    SectionSig,
};

pub use address::{ChunkAddress, DataAddress, RegisterAddress, SpendAddress};
pub use cache::Cache;
pub use chunk::Chunk;
pub use errors::{Error, Result};
pub use identities::{ClientId, NodeId, Participant};
pub use keys::{
    keypair::{BlsKeypairShare, Encryption, Keypair, OwnerType, Signing},
    public_key::PublicKey,
    secret_key::{bls_secret_from_hex, SecretKey},
    signature::{Signature, SignatureShare},
};

use sn_dbc::{SpentProof, SpentProofShare};

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, hash::Hash};
use xor_name::XorName;

///
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum ReplicatedData {
    /// A chunk of data.
    Chunk(Chunk),
    /// A single cmd for a register.
    RegisterWrite(RegisterCmd),
    /// An entire op log of a register.
    RegisterLog(ReplicatedRegisterLog),
    /// A dbc spend share, for aggregation in spentbook.
    SpendShare(SpendShare),
    /// A dbc spend.
    Spend(DbcSpendInfo),
}

impl ReplicatedData {
    pub fn name(&self) -> XorName {
        match self {
            Self::Chunk(chunk) => *chunk.name(),
            Self::RegisterLog(log) => *log.address.name(),
            Self::RegisterWrite(cmd) => *cmd.dst_address().name(),
            Self::SpendShare(share) => share.dbc_id_xorname(),
            Self::Spend(spend) => spend.dbc_id_xorname(),
        }
    }

    pub fn address(&self) -> DataAddress {
        match self {
            Self::Chunk(chunk) => DataAddress::Bytes(*chunk.address()),
            Self::RegisterLog(log) => DataAddress::Register(log.address),
            Self::RegisterWrite(cmd) => DataAddress::Register(cmd.dst_address()),
            Self::SpendShare(share) => DataAddress::Spentbook(share.dst_address()),
            Self::Spend(spend) => DataAddress::Spentbook(spend.dst_address()),
        }
    }
}

/// Register data exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedRegisterLog {
    /// The address of the register.
    pub address: RegisterAddress,
    /// The set of cmds that were applied to the register.
    pub op_log: Vec<RegisterCmd>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct DbcSpendInfo {
    /// This is the unique identifier of the dbc being spent, of which this share is part of.
    /// If a user tries to double spend a dbc, the spend_id would be different, as the tx would
    /// contain different outputs. But the dbc_id is of course the same, as it's the same dbc being spent.
    pub dbc_id: bls::PublicKey,
    /// Unique txs, by `tx_id`, that the dbc has been part of (i.e. attempted double spends or fee updates).
    /// Such tx has a map of spend shares, by their unique `tx_share_id`.
    pub txs: BTreeMap<XorName, BTreeMap<XorName, SpendShare>>,
    /// The aggregated spends, in the order that they have aggregated.
    /// The content is as follows: BTreeMap<TxId, (index, Spend)).
    /// Att index zero is the first spend that this node saw aggregated.
    /// Client decides based on all the info it receives, if the Spend is valid.
    /// If there are more than one Spend in a majority of the infos received from nodes, the spend is invalid, and the tokens burned.
    pub tx_spend_map: BTreeMap<XorName, (usize, Spend)>,
}

impl DbcSpendInfo {
    /// This is the dbc id as an xorname.
    pub fn dbc_id_xorname(&self) -> XorName {
        XorName::from_content(&self.dbc_id.to_bytes())
    }

    /// Returns the dst address of the spentbook.
    pub fn dst_address(&self) -> SpendAddress {
        SpendAddress::new(self.dbc_id_xorname())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct SpendShare(SpentProofShare);

impl SpendShare {
    pub fn new(share: SpentProofShare) -> Self {
        Self(share)
    }

    /// This is the unique identifier of the dbc being spent, of which this share is part of.
    /// If a user tries to double spend a dbc, the spend_id would be different, as the tx would
    /// contain different outputs. But the dbc_id is of course the same, as it's the same dbc being spent.
    pub fn dbc_id(&self) -> &bls::PublicKey {
        self.0.public_key()
    }

    /// This is the dbc id as an xorname.
    pub fn dbc_id_xorname(&self) -> XorName {
        XorName::from_content(&self.dbc_id().to_bytes())
        // spend_id(&self.0.content)
    }

    /// Returns the dst address of the spentbook.
    pub fn dst_address(&self) -> SpendAddress {
        SpendAddress::new(self.dbc_id_xorname())
    }

    /// This is a unique identifier of a specific tx.
    pub fn tx_id(&self) -> XorName {
        let mut hash = [0; 32];
        hash.copy_from_slice(self.0.content.transaction_hash.as_ref());
        XorName(hash)
    }

    /// This is a unique identifier for a share of a specific tx.
    /// Shares of the same tx, have distinct `share_id`.
    pub fn tx_share_id(&self) -> XorName {
        use tiny_keccak::Hasher;
        let mut sha3 = tiny_keccak::Sha3::v256();
        sha3.update(&self.0.to_bytes());
        let mut hash = [0; 32];
        sha3.finalize(&mut hash);
        XorName(hash)
    }

    /// Return the inner proof share.
    pub fn proof_share(&self) -> &SpentProofShare {
        &self.0
    }
}

impl Hash for SpendShare {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.dbc_id().hash(state);
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct Spend(SpentProof);

impl Spend {
    pub fn new(spend: SpentProof) -> Self {
        Self(spend)
    }

    /// This is the unique identifier of the dbc being spent.
    pub fn dbc_id(&self) -> XorName {
        XorName::from_content(&self.0.public_key().to_bytes())
    }

    /// This is a unique identifier of a specific tx.
    pub fn tx_id(&self) -> XorName {
        let mut hash = [0; 32];
        hash.copy_from_slice(self.0.content.transaction_hash.as_ref());
        XorName(hash)
    }

    /// Return the dst address of the spend.
    pub fn dst_address(&self) -> SpendAddress {
        SpendAddress::new(self.dbc_id())
    }

    /// Return the inner proof.
    pub fn proof(&self) -> &SpentProof {
        &self.0
    }
}
