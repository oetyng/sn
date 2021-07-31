// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::types::OwnerType;
use crate::{
    node::{network::Network, Error, Result},
    types::{Signature, SignatureShare},
};
use bls::PublicKeySet;

#[derive(Clone)]
pub(crate) struct ElderSigning {
    id: OwnerType,
    network: Network,
}

impl ElderSigning {
    pub(crate) async fn new(network: Network) -> Result<Self> {
        Ok(Self {
            id: OwnerType::Multi(network.our_public_key_set().await?),
            network,
        })
    }

    pub(crate) async fn our_index(&self) -> Result<usize> {
        self.network
            .our_index()
            .await
            .map_err(|_| Error::NoSectionPublicKeySet)
    }

    pub(crate) async fn public_key_set(&self) -> Result<PublicKeySet> {
        self.network
            .our_public_key_set()
            .await
            .map_err(|_| Error::NoSectionPublicKeySet)
    }

    pub(crate) async fn sign<T: serde::Serialize>(&self, data: &T) -> Result<SignatureShare> {
        let chain = self.network.section_chain().await;
        let data = &bincode::serialize(data).map_err(Error::Bincode)?;
        let (index, share) = self.network.sign_as_elder(data, chain.last_key()).await?;
        Ok(SignatureShare { index, share })
    }

    pub(crate) fn verify<T: serde::Serialize>(&self, sig: &Signature, data: &T) -> bool {
        self.id.verify(sig, data)
    }

    fn id(&self) -> OwnerType {
        self.id.clone()
    }
}
