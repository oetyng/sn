// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(test)]
mod test_client;

use crate::client::Error;
use crate::types::Keypair;
use anyhow::{anyhow, Context, Result as AnyhowResult};
use dirs_next::home_dir;
use std::path::Path;
use std::{collections::HashSet, fs::File, io::BufReader, net::SocketAddr};
#[cfg(test)]
pub use test_client::{create_test_client, create_test_client_with, init_logger};

use exponential_backoff::Backoff;
use std::future::Future;
use std::time::Duration;

///
pub type ClientResult<T> = Result<T, Error>;

///
pub async fn run_w_backoff_delayed<F, Fut, T>(f: F, retries: u8) -> ClientResult<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = ClientResult<T>>,
{
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    run_w_backoff_base(f, retries, Error::NoResponse).await
}

///
pub async fn run_w_backoff<F, Fut, T>(f: F, retries: u8) -> ClientResult<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = ClientResult<T>>,
{
    run_w_backoff_base(f, retries, Error::NoResponse).await
}

async fn run_w_backoff_base<F, Fut, T, E>(f: F, retries: u8, on_fail: E) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let backoff = get_backoff_policy(retries);
    for duration in &backoff {
        match f().await {
            Ok(val) => return Ok(val),
            Err(_) => tokio::time::sleep(duration).await,
        }
    }

    Err(on_fail)
}

fn get_backoff_policy(retries: u8) -> Backoff {
    let min = Duration::from_millis(100);
    let max = Duration::from_secs(10);
    Backoff::new(retries as u32, min, max)
}

// Relative path from $HOME where to read the genesis node connection information from
const GENESIS_CONN_INFO_FILEPATH: &str = ".safe/node/node_connection_info.config";

/// Generates a random BLS secret and public keypair.
pub fn gen_ed_keypair() -> Keypair {
    let mut rng = rand::thread_rng();
    Keypair::new_ed25519(&mut rng)
}

/// Read local network bootstrapping/connection information
pub fn read_network_conn_info() -> AnyhowResult<HashSet<SocketAddr>> {
    let user_dir = home_dir().ok_or_else(|| anyhow!("Could not fetch home directory"))?;
    let conn_info_path = user_dir.join(Path::new(GENESIS_CONN_INFO_FILEPATH));

    let file = File::open(&conn_info_path).with_context(|| {
        format!(
            "Failed to open node connection information file at '{}'",
            conn_info_path.display(),
        )
    })?;
    let reader = BufReader::new(file);
    let contacts: HashSet<SocketAddr> = serde_json::from_reader(reader).with_context(|| {
        format!(
            "Failed to parse content of node connection information file at '{}'",
            conn_info_path.display(),
        )
    })?;

    Ok(contacts)
}

#[cfg(test)]
#[macro_export]
/// Helper for tests to retry an operation awaiting for a successful response result
macro_rules! retry_loop {
    ($async_func:expr) => {
        loop {
            match $async_func.await {
                Ok(val) => break val,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(2000)).await,
            }
        }
    };
}

#[cfg(test)]
#[macro_export]
/// Helper for tests to retry an operation awaiting for a successful response result
macro_rules! retry_err_loop {
    ($async_func:expr) => {
        loop {
            match $async_func.await {
                Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(2000)).await,
                Err(err) => break err,
            }
        }
    };
}

#[cfg(test)]
#[macro_export]
/// Helper for tests to retry an operation awaiting for a specific result
macro_rules! retry_loop_for_pattern {
    ($async_func:expr, $pattern:pat $(if $cond:expr)?) => {
        loop {
            let result = $async_func.await;
            match &result {
                $pattern $(if $cond)? => break result,
                Ok(_) | Err(_) => tokio::time::sleep(std::time::Duration::from_millis(2000)).await,
            }
        }
    };
}
