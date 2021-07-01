// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

/// Common utility functions for writing test cases.
#[cfg(any(test, feature = "testing"))]
pub mod test_utils;

use crate::client::Error;
use bincode::{deserialize, serialize};
use miscreant::{Aead, Aes128SivAead};
use rand::distributions::{Alphanumeric, Distribution, Standard};
use rand::rngs::OsRng;
use rand::{self, Rng};
use serde::{Deserialize, Serialize};

/// Length of the symmetric encryption key.
pub const SYM_ENC_KEY_LEN: usize = 32;

/// Length of the nonce used for symmetric encryption.
pub const SYM_ENC_NONCE_LEN: usize = 16;

/// Symmetric encryption key
pub type SymEncKey = [u8; SYM_ENC_KEY_LEN];

/// Symmetric encryption nonce
pub type SymEncNonce = [u8; SYM_ENC_NONCE_LEN];

#[derive(Serialize, Deserialize)]
struct SymmetricEnc {
    nonce: SymEncNonce,
    cipher_text: Vec<u8>,
}

/// Generates a symmetric encryption key
pub fn generate_sym_enc_key() -> SymEncKey {
    rand::random()
}

/// Generates a nonce for symmetric encryption
pub fn generate_nonce() -> SymEncNonce {
    rand::random()
}

/// Symmetric encryption.
/// If `nonce` is `None`, then it will be generated randomly.
pub fn symmetric_encrypt(
    plain_text: &[u8],
    secret_key: &SymEncKey,
    nonce: Option<&SymEncNonce>,
) -> Result<Vec<u8>, Error> {
    let nonce = match nonce {
        Some(nonce) => *nonce,
        None => generate_nonce(),
    };

    let mut cipher = Aes128SivAead::new(secret_key);
    let cipher_text = cipher.encrypt(&nonce, &[], plain_text);

    Ok(serialize(&SymmetricEnc { nonce, cipher_text })?)
}

/// Symmetric decryption.
pub fn symmetric_decrypt(cipher_text: &[u8], secret_key: &SymEncKey) -> Result<Vec<u8>, Error> {
    let SymmetricEnc { nonce, cipher_text } = deserialize::<SymmetricEnc>(cipher_text)?;
    let mut cipher = Aes128SivAead::new(secret_key);
    cipher
        .decrypt(&nonce, &[], &cipher_text)
        .map_err(|_| Error::SymmetricDecipherFailure)
}

/// Generates a `String` from `length` random UTF-8 `char`s.  Note that the NULL character will be
/// excluded to allow conversion to a `CString` if required, and that the actual `len()` of the
/// returned `String` will likely be around `4 * length` as most of the randomly-generated `char`s
/// will consume 4 elements of the `String`.
pub fn generate_random_string(length: usize) -> String {
    let mut rng = OsRng;
    ::std::iter::repeat(())
        .map(|()| rng.gen::<char>())
        .filter(|c| *c != '\u{0}')
        .take(length)
        .collect()
}

/// Generates a readable `String` using provided `length` and only ASCII characters.
pub fn generate_readable_string(length: usize) -> String {
    let mut rng = OsRng;
    ::std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .take(length)
        .collect()
}

/// Generates a random vector using provided `length`.
pub fn generate_random_vector<T>(length: usize) -> Vec<T>
where
    Standard: Distribution<T>,
{
    let mut rng = OsRng;
    ::std::iter::repeat(())
        .map(|()| rng.gen::<T>())
        .take(length)
        .collect()
}

/// Convert binary data to a diplay-able format
#[inline]
pub fn bin_data_format(data: &[u8]) -> String {
    let len = data.len();
    if len < 8 {
        return format!("[ {:?} ]", data);
    }

    format!(
        "[ {:02x} {:02x} {:02x} {:02x}..{:02x} {:02x} {:02x} {:02x} ]",
        data[0],
        data[1],
        data[2],
        data[3],
        data[len - 4],
        data[len - 3],
        data[len - 2],
        data[len - 1]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIZE: usize = 10;

    // Test `generate_random_string` and that the results are not repeated.
    #[test]
    fn random_string() {
        let str0 = generate_random_string(SIZE);
        let str1 = generate_random_string(SIZE);
        let str2 = generate_random_string(SIZE);

        assert_ne!(str0, str1);
        assert_ne!(str0, str2);
        assert_ne!(str1, str2);

        assert_eq!(str0.chars().count(), SIZE);
        assert_eq!(str1.chars().count(), SIZE);
        assert_eq!(str2.chars().count(), SIZE);
    }

    // Test `generate_random_vector` and that the results are not repeated.
    #[test]
    fn random_vector() {
        let vec0 = generate_random_vector::<u8>(SIZE);
        let vec1 = generate_random_vector::<u8>(SIZE);
        let vec2 = generate_random_vector::<u8>(SIZE);

        assert_ne!(vec0, vec1);
        assert_ne!(vec0, vec2);
        assert_ne!(vec1, vec2);

        assert_eq!(vec0.len(), SIZE);
        assert_eq!(vec1.len(), SIZE);
        assert_eq!(vec2.len(), SIZE);
    }
}
