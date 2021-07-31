// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::data::{CmdError, Error, QueryResponse};
use crate::types::{PublicKey, SignatureShare, Token};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};
use xor_name::XorName;

/// Token cmd that is sent to network.
#[allow(clippy::large_enum_variant)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum PaymentCmd {
    /// The cmd to register the consensused transfer.
    RegisterPayment(RegisterPayment),
}

// 1. -> GetQuote(ops)
// 2. <- QuoteResponse
// 3. Aggregate responses
// 4. -> RegisterPayment(quote, payment)
// 5. <- PaymentRegistered(receipt_share)
// 6. Aggregate responses
// 7. -> Write(ops, receipt)

/// The quote must be signed by a known section key (this is at DbcSection).
/// The DBCs must be valid.
/// The provided payment must match the payees and amounts specified in the quote.
/// The set of chunk names (specified in the quote) are then guaranteed to be signed as paid for.
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct RegisterPayment {
    ///
    pub quote: GuaranteedQuote,
    ///
    pub payment: BTreeMap<PublicKey, sn_dbc::Dbc>,
}

impl RegisterPayment {
    ///
    pub fn inquiry(&self) -> &CostInquiry {
        &self.quote.quote.inquiry
    }

    /// Creates a Response containing an error, with the Response
    /// variant corresponding to the cmd variant.
    pub fn error(&self, error: Error) -> CmdError {
        CmdError::Payment(error)
    }

    /// Returns the address of the destination.
    pub fn dst_address(&self) -> XorName {
        self.inquiry().dst_address()
    }
}

/// A given piece of data, which must match the name and bytes specified,
/// is guaranteed to be accepted, if payment matching this quote
/// is provided together with the quote.
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct PaymentQuote {
    ///
    pub inquiry: CostInquiry,
    ///
    pub payable: BTreeMap<PublicKey, Token>,
}

///
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct GuaranteedQuoteShare {
    ///
    pub quote: PaymentQuote,
    ///
    pub sig: SignatureShare,
    ///
    pub key_set: bls::PublicKeySet,
}

///
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct GuaranteedQuote {
    ///
    pub quote: PaymentQuote,
    ///
    pub sig: bls::Signature,
    ///
    pub key: bls::PublicKey,
}

///
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct PaymentReceiptShare {
    ///
    pub paid_ops: CostInquiry,
    ///
    pub sig: SignatureShare,
    ///
    pub key_set: bls::PublicKeySet,
}

///
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct PaymentReceipt {
    ///
    pub paid_ops: CostInquiry,
    ///
    pub sig: bls::Signature,
    ///
    pub key: bls::PublicKey,
}

///
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct RegOpHash(pub XorName);
///
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct ChunkOpHash(pub XorName);

/// Cost inquiry for a batch of ops
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct CostInquiry {
    /// Batch of chunk ops
    pub chunk_ops: BTreeSet<ChunkOpHash>,
    /// Batch of reg ops
    pub reg_ops: BTreeSet<RegOpHash>,
}

impl CostInquiry {
    ///
    pub fn new(chunk_ops: BTreeSet<ChunkOpHash>, reg_ops: BTreeSet<RegOpHash>) -> Self {
        Self { chunk_ops, reg_ops }
    }
    /// Creates a QueryResponse containing an error, with the QueryResponse variant corresponding to the
    /// Request variant.
    pub fn error(&self, error: Error) -> QueryResponse {
        QueryResponse::GetQuote(Err(error))
    }

    /// Returns the address of the destination for the query.
    pub fn dst_address(&self) -> XorName {
        self.chunk_ops
            .iter()
            .map(|c| c.0)
            .chain(self.reg_ops.iter().map(|e| e.0))
            .reduce(xor)
            .unwrap_or_default()
    }
}

fn xor(a: XorName, b: XorName) -> XorName {
    let mut bytes = a.0;
    for (i, byte) in b.0.iter().enumerate() {
        bytes[i] ^= *byte;
    }
    XorName(bytes)
}

impl PaymentCmd {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// Request variant.
    pub fn error(&self, error: Error) -> CmdError {
        use CmdError::*;
        use PaymentCmd::*;
        match *self {
            RegisterPayment(_) => Payment(error),
        }
    }

    /// Returns the address of the destination for `request`.
    pub fn dst_address(&self) -> XorName {
        use PaymentCmd::*;
        match self {
            RegisterPayment(ref reg) => reg.dst_address(),
        }
    }
}

impl fmt::Debug for PaymentCmd {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        use PaymentCmd::*;
        write!(
            formatter,
            "PaymentCmd::{}",
            match *self {
                RegisterPayment { .. } => "RegisterPayment",
            }
        )
    }
}

// impl fmt::Debug for PaymentQuery {
//     fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//         use PaymentQuery::*;
//         match *self {
//             GetQuote { .. } => write!(formatter, "PaymentQuery::GetQuote"),
//         }
//     }
// }

#[cfg(test)]
mod test {
    use xor_name::XorName;

    #[test]
    fn blah() {
        let rand_1 = XorName::random();
        let rand_2 = XorName::random();

        assert!(rand_1 != rand_2);

        let prod_1 = xor(rand_1, rand_2);
        let prod_2 = xor(rand_2, rand_1);

        assert_eq!(prod_1, prod_2);

        let der_1 = xor(rand_2, prod_1);
        let der_2 = xor(rand_1, prod_1);

        assert_eq!(der_1, rand_1);
        assert_eq!(der_2, rand_2);
    }

    fn xor(a: XorName, b: XorName) -> XorName {
        let mut init = a.0;
        for (i, byte) in b.0.iter().enumerate() {
            init[i] ^= *byte;
        }
        XorName(init)
    }
}
