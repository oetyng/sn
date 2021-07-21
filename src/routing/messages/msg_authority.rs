// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{node::Peer, NodeMsgAuthority, SectionSigned, SrcLocation};
use crate::routing::{
    ed25519::{self},
    error::{Error, Result},
    peer::PeerUtils,
};
use bls::PublicKey as BlsPublicKey;
use std::net::SocketAddr;
use xor_name::XorName;

pub(crate) trait NodeMsgAuthorityUtils {
    fn src_location(&self) -> SrcLocation;

    fn name(&self) -> XorName;

    // If this location is `Node`, returns the corresponding `Peer` with `addr`. Otherwise error.
    fn peer(&self, addr: SocketAddr) -> Result<Peer>;

    // Verify if the section key of the NodeMsgAuthority can be trusted
    // based on a set of known keys.
    fn verify_src_section_key(&self, known_keys: &[BlsPublicKey]) -> bool;
}

impl NodeMsgAuthorityUtils for NodeMsgAuthority {
    fn src_location(&self) -> SrcLocation {
        match self {
            NodeMsgAuthority::Node(node_auth) => SrcLocation::Node {
                name: ed25519::name(&node_auth.public_key),
                section_pk: node_auth.section_pk,
            },
            NodeMsgAuthority::BlsShare(bls_share_auth) => SrcLocation::Section {
                name: bls_share_auth.src_name,
                section_pk: bls_share_auth.section_pk,
            },
            NodeMsgAuthority::Section(SectionSigned {
                src_name,
                section_pk,
                ..
            }) => SrcLocation::Section {
                name: *src_name,
                section_pk: *section_pk,
            },
        }
    }

    fn name(&self) -> XorName {
        match self {
            NodeMsgAuthority::Node(node_auth) => ed25519::name(&node_auth.public_key),
            NodeMsgAuthority::BlsShare(bls_share_auth) => bls_share_auth.src_name,
            NodeMsgAuthority::Section(SectionSigned { src_name, .. }) => *src_name,
        }
    }

    // If this location is `Node`, returns the corresponding `Peer` with `addr`. Otherwise error.
    fn peer(&self, addr: SocketAddr) -> Result<Peer> {
        match self {
            NodeMsgAuthority::Node(node_auth) => {
                Ok(Peer::new(ed25519::name(&node_auth.public_key), addr))
            }
            NodeMsgAuthority::Section(_) | NodeMsgAuthority::BlsShare(_) => {
                Err(Error::InvalidSrcLocation)
            }
        }
    }

    // Verify if the section key of the NodeMsgAuthority can be trusted
    // based on a set of known keys.
    fn verify_src_section_key(&self, known_keys: &[BlsPublicKey]) -> bool {
        match &self {
            NodeMsgAuthority::Node(_) => true,
            NodeMsgAuthority::BlsShare(bls_share_auth) => known_keys
                .iter()
                .any(|key| key == &bls_share_auth.section_pk),
            NodeMsgAuthority::Section(SectionSigned { section_pk, .. }) => {
                known_keys.iter().any(|key| key == section_pk)
            }
        }
    }
}
