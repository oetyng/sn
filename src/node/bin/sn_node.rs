// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! sn_node provides the interface to Safe routing.  The resulting executable is the node
//! for the Safe network.
// boop
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/maidsafe/QA/master/Images/maidsafe_logo.png",
    html_favicon_url = "https://maidsafe.net/img/favicon.ico",
    test(attr(deny(warnings)))
)]
// For explanation of lint checks, run `rustc -W help`.
#![forbid(unsafe_code)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results
)]

use anyhow::{anyhow, Error as AnyhowError, Result};
use safe_network::node::{add_connection_info, set_connection_info, Config, Error, Node};
use self_update::{cargo_crate_version, Status};
use std::{io::Write, process};
use structopt::{clap, StructOpt};
use tokio::time::{sleep, Duration};
use tracing::{self, error, info};
use tracing_subscriber::filter::EnvFilter;
const MODULE_NAME: &str = "safe_network";

const BOOTSTRAP_RETRY_TIME: u64 = 3; // in minutes
use safe_network::routing;

/// Runs a Safe Network node.
fn main() {
    let sn_node_thread = std::thread::Builder::new()
        .name("sn_node".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(run_node())?;
            Ok::<(), AnyhowError>(())
        });

    match sn_node_thread {
        Ok(thread) => match thread.join() {
            Ok(_) => {}
            Err(err) => println!("Failed to run node: {:?}", err),
        },
        Err(err) => println!("Failed to run node: {:?}", err),
    }
}

async fn run_node() -> Result<()> {
    let config = match Config::new() {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("Failed to create Config: {:?}", e);
            exit(1);
            return Ok(());
        }
    };

    if let Some(c) = &config.completions() {
        match c.parse::<clap::Shell>() {
            Ok(shell) => match gen_completions_for_shell(shell) {
                Ok(buf) => {
                    std::io::stdout().write_all(&buf).unwrap_or_else(|e| {
                        println!("Failed to print shell completions. {}", e);
                    });
                }
                Err(e) => println!("{}", e),
            },
            Err(e) => println!("Unknown completions option. {}", e),
        }
        // we exit program on both success and error.
        return Ok(());
    }

    // ==============
    // Set up logging
    // ==============

    let filter = match EnvFilter::try_from_env("RUST_LOG") {
        Ok(filter) => filter,
        // If we have an error (ie RUST_LOG not set or otherwise), we check the verbosity flags
        Err(_) => {
            // we manually determine level filter instead of using tracing EnvFilter.
            let level_filter = config.verbose();
            let module_log_filter = format!("{}={}", MODULE_NAME, level_filter.to_string());

            EnvFilter::from_default_env().add_directive(
                module_log_filter
                    .parse()
                    .map_err(|_| anyhow!("Could not parse module log filter"))?,
            )
        }
    };

    let _optional_guard = if let Some(log_dir) = config.log_dir() {
        println!("Starting logging to file");
        let file_appender = tracing_appender::rolling::hourly(log_dir, "sn_node.log");

        // configure how tracing non-blocking works: https://tracing.rs/tracing_appender/non_blocking/struct.nonblockingbuilder#method.default
        let non_blocking_builder = tracing_appender::non_blocking::NonBlockingBuilder::default();

        let (non_blocking, guard) = non_blocking_builder
            // lose lines and keep perf, or exert backpressure?
            .lossy(false)
            // optionally change buffered lines limit
            // .buffered_lines_limit(buffered_lines_limit)
            .finish(file_appender);

        tracing_subscriber::fmt()
            .with_writer(non_blocking)
            // eg : RUST_LOG=my_crate=info,my_crate::my_mod=debug,[my_span]=trace
            .with_env_filter(filter)
            .with_thread_names(true)
            // here we choose log style output... do we want json for prod + analysis?
            // can be compact, pretty, json...
            .compact()
            // .pretty()
            // .json()
            .init();

        Some(guard)
    } else {
        println!("Starting logging to stdout");

        tracing_subscriber::fmt().init();

        None
    };

    if config.update() || config.update_only() {
        match update() {
            Ok(status) => {
                if let Status::Updated { .. } = status {
                    println!("Node has been updated. Please restart.");
                    exit(0);
                }
            }
            Err(e) => error!("Updating node failed: {:?}", e),
        }

        if config.update_only() {
            exit(0);
        }
    }

    let message = format!(
        "Running {} v{}",
        Config::clap().get_name(),
        env!("CARGO_PKG_VERSION")
    );
    info!("\n\n{}\n{}", message, "=".repeat(message.len()));

    let log = format!(
        "The network is not accepting nodes right now. Retrying after {} minutes",
        BOOTSTRAP_RETRY_TIME
    );

    let (node, event_stream) = loop {
        match Node::new(&config).await {
            Ok(result) => break result,
            Err(Error::Routing(routing::Error::TryJoinLater)) => {
                println!("{}", log);
                info!("{}", log);
            }
            Err(Error::Routing(routing::Error::NodeNotReachable(addr))) => {
                let err_msg = format!(
                    "Unfortunately we are unable to establish a connection to your machine ({}) either through a \
                    public IP address, or via IGD on your router. Please ensure that IGD is enabled on your router - \
                    if it is and you are still unable to add your node to the testnet, then skip adding a node for this \
                    testnet iteration. You can still use the testnet as a client, uploading and downloading content, etc. \
                    https://safenetforum.org/",
                    addr
                );
                println!("{}", err_msg);
                error!("{}", err_msg);
                exit(1);
            }
            Err(Error::JoinTimeout) => {
                let message = format!("Encountered a timeout while trying to join the network. Retrying after {} minutes.", BOOTSTRAP_RETRY_TIME);
                println!("{}", &message);
                error!("{}", &message);
            }
            Err(e) => {
                println!("Cannot start node due to error: {:?}. If this is the first node on the network \
                 pass the local address to be used using --first. Exiting", e);
                error!("Cannot start node due to error: {:?}. If this is the first node on the network \
                 pass the local address to be used using --first. Exiting", e);
                exit(1);
            }
        }
        sleep(Duration::from_secs(BOOTSTRAP_RETRY_TIME * 60)).await;
    };

    let our_conn_info = node.our_connection_info();

    if config.is_first() {
        set_connection_info(our_conn_info).unwrap_or_else(|err| {
            error!("Unable to write our connection info to disk: {:?}", err);
        });
    } else {
        add_connection_info(our_conn_info).unwrap_or_else(|err| {
            error!("Unable to add our connection info to disk: {:?}", err);
        });
    }

    match node.run(event_stream).await {
        Ok(()) => {
            exit(0);
            Ok(())
        }
        Err(e) => {
            println!("Cannot start node due to error: {:?}", e);
            error!("Cannot start node due to error: {:?}", e);
            exit(1);
            Ok(())
        }
    }
}

fn exit(exit_code: i32) {
    process::exit(exit_code);
}

fn update() -> Result<Status, Box<dyn (::std::error::Error)>> {
    info!("Checking for updates...");
    let target = self_update::get_target();

    let releases = self_update::backends::github::ReleaseList::configure()
        .repo_owner("maidsafe")
        .repo_name("safe_network")
        .with_target(&target)
        .build()?
        .fetch()?;

    if !releases.is_empty() {
        tracing::debug!("Target for update is {}", target);
        tracing::debug!("Found releases: {:#?}\n", releases);
        let bin_name = if target.contains("pc-windows") {
            "sn_node.exe"
        } else {
            "sn_node"
        };
        let status = self_update::backends::github::Update::configure()
            .repo_owner("maidsafe")
            .repo_name("safe_network")
            .target(&target)
            .bin_name(&bin_name)
            .show_download_progress(true)
            .no_confirm(true)
            .current_version(cargo_crate_version!())
            .build()?
            .update()?;
        println!("Update status: '{}'!", status.version());
        Ok(status)
    } else {
        println!("Current version is '{}'", cargo_crate_version!());
        println!("No releases are available for updates");
        Ok(Status::UpToDate(
            "No releases are available for updates".to_string(),
        ))
    }
}

fn gen_completions_for_shell(shell: clap::Shell) -> Result<Vec<u8>, String> {
    // Get exe path
    let exe_path =
        std::env::current_exe().map_err(|err| format!("Can't get the exec path: {}", err))?;

    // get filename without preceding path as std::ffi::OsStr (C string)
    let exec_name_ffi = match exe_path.file_name() {
        Some(v) => v,
        None => {
            return Err(format!(
                "Can't extract file_name of executable from path {}",
                exe_path.display()
            ))
        }
    };

    // Convert OsStr to string.  Can fail if OsStr contains any invalid unicode.
    let exec_name = match exec_name_ffi.to_str() {
        Some(v) => v.to_string(),
        None => {
            return Err(format!(
                "Can't decode unicode in executable name '{:?}'",
                exec_name_ffi
            ))
        }
    };

    // Generates shell completions for <shell> and prints to stdout
    let mut buf: Vec<u8> = vec![];
    Config::clap().gen_completions_to(exec_name, shell, &mut buf);

    Ok(buf)
}
