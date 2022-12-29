// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! `sn_node` provides the interface to a Safe network node. The resulting executable is the node
//! for the Safe network.

use sn_client::Client;

use eyre::Result;
use tiny_keccak::{Hasher, Sha3};
use tokio::time::{sleep, Duration};
use xor_name::XorName;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::fmt()
        // NOTE: uncomment this line for pretty printed log output.
        .with_thread_names(true)
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        // .event_format(LogFormatter::default())
        .try_init()
        .unwrap_or_else(|_| println!("Error initializing logger"));

    // Read the data addresses and hashes from disk.
    let addresses_from_disk = read_addresses_from_disk()?;

    // Try get the data from the network.
    verify_data(addresses_from_disk).await?;

    // All Good!

    Ok(())
}

/// Reads data addresses from file, and tries to get them from the network.
pub async fn verify_data(addresses_from_disk: Vec<(XorName, [u8; 32])>) -> Result<()> {
    println!("Getting the data from the network..");

    let client = Client::builder().build().await?;

    for (address, hash) in addresses_from_disk {
        println!("...reading bytes at address {:?} ...", address);
        let mut bytes = client.read_bytes(address).await;

        let mut attempts = 0;
        while bytes.is_err() && attempts < 10 {
            attempts += 1;
            println!(
                "another attempt {attempts} ...reading bytes at address {:?} ...",
                address
            );
            // do some retries to ensure we're not just timing out by chance
            sleep(Duration::from_millis(100)).await;
            bytes = client.read_bytes(address).await;
        }

        let bytes = bytes?;
        println!("Bytes read from {:?}:", address);

        let mut hasher = Sha3::v256();
        let mut output = [0; 32];
        hasher.update(&bytes);
        hasher.finalize(&mut output);

        assert_eq!(output, hash);
    }

    println!("All okay");

    Ok(())
}

fn read_addresses_from_disk() -> Result<Vec<(XorName, [u8; 32])>> {
    println!("Reading existing data from disk..");
    use std::{fs, io::Read};
    type Error = sn_interface::network_knowledge::Error;

    let mut path = dirs_next::home_dir().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "Home directory not found")
    })?;
    path.push(".safe");
    path.push("stored_data");
    path.push("addresses.dat");

    let mut data_stored_file = fs::File::open(&path).map_err(|err| {
        Error::FileHandling(format!(
            "Error opening SectionTree file from {}: {:?}",
            path.display(),
            err
        ))
    })?;

    let mut data_stored_content = vec![];
    let _ = data_stored_file
        .read_to_end(&mut data_stored_content)
        .map_err(|err| {
            Error::FileHandling(format!(
                "Error reading SectionTree from {}: {:?}",
                path.display(),
                err
            ))
        })?;

    let stored_data_addresses = serde_json::from_slice(&data_stored_content)
        .map_err(|err| Error::Deserialisation(err.to_string()))?;

    Ok(stored_data_addresses)
}
