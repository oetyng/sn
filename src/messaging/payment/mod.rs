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
    pub key_set: bls::PublicKeySet,
}

///
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct PaymentReceiptShare {
    ///
    pub paid_ops: CostInquiry,
    ///
    pub sig: sn_dbc::NodeSignature,
    ///
    pub key: bls::PublicKey,
}

///
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct PaymentReceipt {
    ///
    pub paid_ops: CostInquiry,
    ///
    pub sig: bls::Signature,
    ///
    pub key_set: bls::PublicKeySet,
}

///
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct PointerEditHash(pub XorName);

/// Cost inquiry for a batch of ops
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct CostInquiry {
    /// Batch of chunks to be uploaded
    pub uploads: BTreeSet<XorName>,
    /// Batch of edits to be edited
    pub edits: BTreeSet<PointerEditHash>,
}

impl CostInquiry {
    pub fn payment_xorname(&self) -> Result<XorName, CmdError> {
        if self.uploads.is_empty() && self.edits.is_empty() {
            return Err(CmdError::Data(Error::InvalidOperation(
                "Empty inquiry".to_string(),
            )));
        }
        // TODO: XOR all the XorNames of uploads and edits
        Ok(XorName::random())
    }

    /// Creates a QueryResponse containing an error, with the QueryResponse variant corresponding to the
    /// Request variant.
    #[allow(unused)]
    pub fn error(&self, error: Error) -> QueryResponse {
        QueryResponse::GetQuote(Err(error))
    }

    /// Returns the address of the destination for the query.
    #[allow(unused)]
    pub fn dst_address(&self) -> Result<XorName, CmdError> {
        self.payment_xorname()
    }
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
            RegisterPayment(ref _reg) => XorName::random(), //XorName::from(reg.quote.signers.public_key()), // this is handled where the debit is made
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
