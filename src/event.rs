// Copyright 2015 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under (1) the MaidSafe.net Commercial License,
// version 1.0 or later, or (2) The General Public License (GPL), version 3, depending on which
// licence you accepted on initial access to the Software (the "Licences").
//
// By contributing code to the SAFE Network Software, or to this project generally, you agree to be
// bound by the terms of the MaidSafe Contributor Agreement, version 1.0.  This, along with the
// Licenses can be found in the root directory of this project at LICENSE, COPYING and CONTRIBUTOR.
//
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
//
// Please review the Licences for the specific language governing permissions and limitations
// relating to use of the SAFE Network Software.

use authority::Authority;
use messages::{RoutingMessage, ExternalRequest, ExternalResponse, SignedToken};
use name_type::NameType;
use error::InterfaceError;
use sodiumoxide::crypto::sign;

/// An Event is received at the effective close group of B of a message flow < A | B >
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Event {
    Request {
        request: ExternalRequest,
        our_authority: Authority,
        from_authority: Authority,
        response_token: Option<SignedToken>, /* Not set when the request came from
                                              * a group */
    },
    Response {
        response: ExternalResponse,
        our_authority: Authority,
        from_authority: Authority,
    },
    FailedRequest {
        request: ExternalRequest,
        our_authority: Option<Authority>,
        location: Authority,
        interface_error: InterfaceError,
    },
    FailedResponse {
        response: ExternalResponse,
        our_authority: Option<Authority>,
        location: Authority,
        interface_error: InterfaceError,
    },
    /// Refresh reports to the user the collected accounts for a given refresh event,
    /// the arguments are type_tag:u64, authority: Authority, vector_of_bytes: Vec<Vec<u8>>
    Refresh(u64, ::authority::Authority, Vec<Vec<u8>>),
    /// Churn reports whenever our close group is changed, and provides our new close group
    /// as a Vec<NameType> and the name of the node that joined or left our close group
    /// as NameType.  Our close group is sorted from our name and always includes our own name
    /// as the first element.
    Churn(Vec<::NameType>, ::NameType),
    /// DoRefresh reports that a Refresh message is circulating the effective close group
    /// of the given Authority, but that the user is outside of the close group of the churn
    /// that initiated the call for refresh.  To ensure that the account of the current user is
    /// also accumulated a DoRefresh indicates precisely one account routing will expect the
    /// user to do a ::routing::request_refresh for, if a matching account is held by the user.
    DoRefresh(u64, ::authority::Authority, ::NameType),
    Bootstrapped,
    Connected,
    Disconnected,
    /// Event::Terminated is called after RoutingNode::stop() has terminated internal processes
    Terminated,
}
