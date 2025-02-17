// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// https://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

//! Module providing keys, keypairs, and signatures.
//!
//! The easiest way to get a `PublicKey` is to create a random `Keypair` first through one of the
//! `new` functions. A `PublicKey` can't be generated by itself; it must always be derived from a
//! secret key.

use super::super::{Error, Result};
use super::super::{PublicKey, SecretKey, Signature, SignatureShare};

use bls::{self, serde_impl::SerdeSecret, PublicKeySet};
use ed25519_dalek::Signer;
use rand::{CryptoRng, Rng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
/// Entity that owns the data or tokens.
pub enum OwnerType {
    /// Single owner
    Single(PublicKey),
    /// Multi sig owner
    Multi(PublicKeySet),
}

impl OwnerType {
    /// Returns the owner public key
    pub fn public_key(&self) -> PublicKey {
        match self {
            Self::Single(key) => *key,
            Self::Multi(key_set) => PublicKey::Bls(key_set.public_key()),
        }
    }

    /// Tries to get the key set in case this is a Multi owner.
    pub fn public_key_set(&self) -> Result<PublicKeySet> {
        match self {
            Self::Single(_) => Err(Error::InvalidOwnerNotPublicKeySet),
            Self::Multi(key_set) => Ok(key_set.clone()),
        }
    }

    ///
    pub fn verify<T: Serialize>(&self, signature: &Signature, data: &T) -> bool {
        let data = match bincode::serialize(&data) {
            Err(_) => return false,
            Ok(data) => data,
        };
        match signature {
            Signature::Bls(sig) => {
                if let OwnerType::Multi(set) = self {
                    set.public_key().verify(sig, data)
                } else {
                    false
                }
            }
            ed @ Signature::Ed25519(_) => self.public_key().verify(ed, data).is_ok(),
            Signature::BlsShare(share) => {
                if let OwnerType::Multi(set) = self {
                    let pubkey_share = set.public_key_share(share.index);
                    pubkey_share.verify(&share.share, data)
                } else {
                    false
                }
            }
        }
    }
}

/// Ability to sign and validate data/tokens, as well as specify the type of ownership of that data
pub trait Signing {
    ///
    fn id(&self) -> OwnerType;
    ///
    fn sign<T: Serialize>(&self, data: &T) -> Result<Signature>;
    ///
    fn verify<T: Serialize>(&self, sig: &Signature, data: &T) -> bool;
}

impl Signing for Keypair {
    fn id(&self) -> OwnerType {
        match self {
            Keypair::Ed25519(pair) => OwnerType::Single(PublicKey::Ed25519(pair.public)),
            Keypair::BlsShare(share) => OwnerType::Multi(share.public_key_set.clone()),
        }
    }

    fn sign<T: Serialize>(&self, data: &T) -> Result<Signature> {
        let bytes = bincode::serialize(data).map_err(|e| Error::Serialisation(e.to_string()))?;
        Ok(self.sign(&bytes))
    }

    fn verify<T: Serialize>(&self, signature: &Signature, data: &T) -> bool {
        self.id().verify(signature, data)
    }
}

/// Wrapper for different keypair types.
#[derive(Serialize, Deserialize, Clone, custom_debug::Debug)]
pub enum Keypair {
    /// Ed25519 keypair.
    Ed25519(#[debug(skip)] Arc<ed25519_dalek::Keypair>),
    /// BLS keypair share.
    BlsShare(Arc<BlsKeypairShare>),
}

// Need to manually implement this due to a missing impl in `Ed25519::Keypair`.
impl PartialEq for Keypair {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ed25519(keypair), Self::Ed25519(other_keypair)) => {
                // TODO: After const generics land, remove the `to_vec()` calls.
                keypair.to_bytes().to_vec() == other_keypair.to_bytes().to_vec()
            }
            (Self::BlsShare(keypair), Self::BlsShare(other_keypair)) => keypair == other_keypair,
            _ => false,
        }
    }
}

// Need to manually implement this due to a missing impl in `Ed25519::Keypair`.
impl Eq for Keypair {}

impl Keypair {
    /// Constructs a random Ed25519 keypair.
    pub fn new_ed25519<T: CryptoRng + Rng>(rng: &mut T) -> Self {
        let keypair = ed25519_dalek::Keypair::generate(rng);
        Self::Ed25519(Arc::new(keypair))
    }

    /// Constructs a BLS keypair share.
    pub fn new_bls_share(
        index: usize,
        secret_share: bls::SecretKeyShare,
        public_key_set: PublicKeySet,
    ) -> Self {
        Self::BlsShare(Arc::new(BlsKeypairShare {
            index,
            secret: SerdeSecret(secret_share.clone()),
            public: secret_share.public_key_share(),
            public_key_set,
        }))
    }

    /// Returns the public key associated with this keypair.
    pub fn public_key(&self) -> PublicKey {
        match self {
            Self::Ed25519(keypair) => PublicKey::Ed25519(keypair.public),
            Self::BlsShare(keypair) => PublicKey::BlsShare(keypair.public),
        }
    }

    /// Returns the secret key associated with this keypair.
    pub fn secret_key(&self) -> Result<SecretKey> {
        match self {
            Self::Ed25519(keypair) => {
                let bytes = keypair.secret.to_bytes();
                match ed25519_dalek::SecretKey::from_bytes(&bytes) {
                    Ok(sk) => Ok(SecretKey::Ed25519(sk)),
                    Err(_) => Err(Error::FailedToParse(
                        "Could not deserialise Ed25519 secret key".to_string(),
                    )),
                }
            }
            Self::BlsShare(keypair) => Ok(SecretKey::BlsShare(keypair.secret.clone())),
        }
    }

    /// Signs with the underlying keypair.
    pub fn sign(&self, data: &[u8]) -> Signature {
        match self {
            Self::Ed25519(keypair) => Signature::Ed25519(keypair.sign(data)),
            Self::BlsShare(keypair) => Signature::BlsShare(SignatureShare {
                index: keypair.index,
                share: keypair.secret.sign(data),
            }),
        }
    }
}

impl From<ed25519_dalek::SecretKey> for Keypair {
    fn from(secret: ed25519_dalek::SecretKey) -> Self {
        let keypair = ed25519_dalek::Keypair {
            public: (&secret).into(),
            secret,
        };

        Self::Ed25519(Arc::new(keypair))
    }
}

/// BLS keypair share.
#[derive(Clone, PartialEq, Serialize, Deserialize, custom_debug::Debug)]
pub struct BlsKeypairShare {
    /// Share index.
    pub index: usize,
    /// Secret key share.
    #[debug(skip)]
    pub secret: SerdeSecret<bls::SecretKeyShare>,
    /// Public key share.
    pub public: bls::PublicKeyShare,
    /// Public key set. Necessary for producing proofs.
    pub public_key_set: PublicKeySet,
}

#[cfg(test)]
mod tests {
    use super::super::super::utils;
    use super::*;

    fn gen_keypairs() -> Vec<Keypair> {
        let mut rng = rand::thread_rng();
        let bls_secret_key = bls::SecretKeySet::random(1, &mut rng);
        vec![
            Keypair::new_ed25519(&mut rng),
            Keypair::new_bls_share(
                0,
                bls_secret_key.secret_key_share(0),
                bls_secret_key.public_keys(),
            ),
        ]
    }

    // Test serialising and deserialising key pairs.
    #[test]
    fn serialisation_key_pair() -> Result<()> {
        let keypairs = gen_keypairs();

        for keypair in keypairs {
            let encoded = utils::serialise(&keypair)?;
            let decoded: Keypair = utils::deserialise(&encoded)?;

            assert_eq!(decoded, keypair);
        }

        Ok(())
    }
}
