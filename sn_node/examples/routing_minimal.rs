// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Example of a minimal routing node.
//!
//! This example node can connect to the other nodes, perform section operations like split,
//! relocation, promotion and demotion and print important updates to the stdout. It doesn't
//! currently do anything else, in particular it doesn't support sending/receiving user messages or
//! voting for user observations. Such features might be added in the future.
//!
//! # Usage
//!
//! Run as a standalone binary:
//!
//!     minimal ARGS...
//!
//! Or via cargo (release mode recommended):
//!
//!     cargo run --release --example minimal -- ARGS...
//!
//! Run with `--help` (or `-h`) to see the command line options and their explanation.
//!
//!
//! # Multiple nodes
//!
//! It is possible to start multiple nodes with a single invocation of this example. Such nodes
//! would all run within the same process, but each in its own thread. See the `--count` (or `-c`)
//! command-line option for more details.
//!

use sn_node::node::{
    cfg::create_test_max_capacity_and_root_storage, new_test_api, Config, Event, EventReceiver,
    MembershipEvent, MessagingEvent, NodeTestApi as NodeApi,
};

use clap::Parser;
use eyre::Result;
use futures::future::join_all;
use std::{
    convert::TryInto,
    iter,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::task::JoinHandle;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Minimal example node.
#[derive(Debug, clap::StructOpt)]
struct Options {
    /// Whether this is the first node ("genesis node") of the network. Only one node can be first.
    #[clap(short, long)]
    first: bool,
    /// IP address to bind to. Default is localhost which is fine when spawning example nodes
    /// on a single machine. If one wants to run nodes on multiple machines, an address that is
    /// reachable from all of them must be used.
    ///
    /// Note: unspecified (0.0.0.0) currently doesn't work due to limitation of the underlying
    /// networking layer.
    ///
    /// If starting multiple nodes (see --count), then this option applies only to the first one.
    /// The rest are started as regular nodes.
    #[clap(short, long, value_name = "IP")]
    ip: Option<IpAddr>,
    /// Port to listen to. If omitted, a randomly assigned port is used.
    ///
    /// If starting multiple nodes (see --count), then this is used as base port and the actual
    /// port of `n-th` node is calculated as `port + n` (if available)
    #[clap(short, long, value_name = "PORT")]
    port: Option<u16>,
    /// Number of nodes to start. Each node is started in its own thread. Default is to start just
    /// one node.
    #[clap(
        short,
        long,
        default_value_t,
        value_name = "COUNT",
        hide_default_value = true
    )]
    count: usize,
    /// Enable verbose output. Can be specified multiple times for increased verbosity.
    #[clap(short, parse(from_occurrences))]
    verbosity: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Options::from_args();
    init_log(opts.verbosity);

    let handles: Vec<_> = if opts.count <= 1 {
        let handle = start_single_node(opts.first, opts.ip, opts.port).await?;
        iter::once(handle).collect()
    } else {
        start_multiple_nodes(opts.count, opts.first, opts.ip, opts.port).await?
    };
    let _res = join_all(handles).await;

    Ok(())
}

// Starts a single node and block until it terminates.
async fn start_single_node(
    first: bool,
    ip: Option<IpAddr>,
    port: Option<u16>,
) -> Result<JoinHandle<()>> {
    start_node(0, first, ip, port).await
}

// Starts `count` nodes and blocks until all of them terminate.
//
// If `first` is true, the first spawned node will start as the first (genesis) node. The rest will
// always start as regular nodes.
async fn start_multiple_nodes(
    count: usize,
    first: bool,
    ip: Option<IpAddr>,
    base_port: Option<u16>,
) -> Result<Vec<JoinHandle<()>>> {
    let mut handles = Vec::new();
    let first_index = if first {
        let first_handle = start_node(0, true, ip, base_port).await?;
        handles.push(first_handle);
        1
    } else {
        0
    };

    for index in first_index..count {
        let handle = start_node(index, false, ip, base_port).await?;
        handles.push(handle);
    }
    Ok(handles)
}

// Starts a single node and blocks until it terminates.
async fn start_node(
    index: usize,
    first: bool,
    ip: Option<IpAddr>,
    base_port: Option<u16>,
) -> Result<JoinHandle<()>> {
    let ip = ip.unwrap_or_else(|| Ipv4Addr::LOCALHOST.into());
    let local_port = base_port
        .map(|base_port| {
            index
                .try_into()
                .ok()
                .and_then(|offset| base_port.checked_add(offset))
                .expect("port out of range")
        })
        .unwrap_or_else(rand::random);

    info!("Node #{} starting...", index);

    let (_, root) = create_test_max_capacity_and_root_storage()?;

    let config = Config {
        first,
        local_addr: Some(SocketAddr::new(ip, local_port)),
        root_dir: Some(root),
        ..Default::default()
    };

    let join_timeout = Duration::from_secs(3 * 60);

    let (node, event_receiver) = new_test_api(&config, join_timeout)
        .await
        .expect("Failed to instantiate a Node");

    info!("Node #{} connected - name: {}", index, node.name().await,);

    let handle = run_node(index, node, event_receiver);

    Ok(handle)
}

// Spawns a task to run the node until terminated.
fn run_node(index: usize, mut node: NodeApi, mut event_receiver: EventReceiver) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        while let Some(event) = event_receiver.next().await {
            if !handle_event(index, &mut node, event).await {
                break;
            }
        }
    })
}

// Handles the event emitted by the node.
async fn handle_event(index: usize, node: &mut NodeApi, event: Event) -> bool {
    use MembershipEvent::*;
    use MessagingEvent::*;
    match event {
        Event::Messaging(e) => match e {
            SystemMsgReceived { msg, auth, dst, .. } => info!(
                "Node #{} received message - auth: {:?}, route: {:?}, content: {:?}",
                index,
                auth.into_inner(),
                dst,
                msg
            ),
            ServiceMsgReceived { msg, user, .. } => info!(
                "Node #{} received message from user: {:?}, msg: {:?}",
                index, user, msg
            ),
        },
        Event::Membership(e) => match e {
            MemberJoined {
                name,
                previous_name,
                age,
            } => {
                info!(
                    "Node #{} member joined - name: {}, previous_name: {:?}, age: {}",
                    index, name, previous_name, age
                );
            }
            MemberLeft { name, age } => {
                info!("Node #{} member left - name: {}, age: {}", index, name, age);
            }
            ChurnJoinMissError => {
                info!("Node #{} detected churn join miss and will restart", index);
            }
            SectionSplit {
                elders,
                self_status_change,
            } => {
                info!(
                    "Node #{} section split - elders: {:?}, node elder status change: {:?}",
                    index, elders, self_status_change
                );
            }
            EldersChanged {
                elders,
                self_status_change,
            } => {
                info!(
                    "Node #{} elders changed - elders: {:?}, node elder status change: {:?}",
                    index, elders, self_status_change
                );
            }
            RelocationStarted { previous_name } => info!(
                "Node #{} relocation started - previous_name: {}",
                index, previous_name
            ),
            Relocated { previous_name, .. } => {
                let new_name = node.name().await;
                info!(
                    "Node #{} relocated - old name: {}, new name: {}",
                    index, previous_name, new_name,
                );
            }
            AdultsChanged {
                remaining,
                added,
                removed,
            } => info!(
                "Node #{} adults changed - remaining: {:?}, added: {:?}, removed: {:?}",
                index, remaining, added, removed
            ),
        },
        _ => {
            // Currently ignore the other event variants. This might change in the future,
            // if we come up with something interesting to use those events for.
        }
    }

    true
}

fn init_log(verbosity: u8) {
    const BIN_NAME: &str = module_path!();
    const CRATE_NAME: &str = "sn_node";
    let filter = match verbosity {
        0 => EnvFilter::new(format!("{}=info,{}=warn", BIN_NAME, CRATE_NAME)),
        1 => EnvFilter::new(format!("{},{}=info", BIN_NAME, CRATE_NAME)),
        2 => EnvFilter::new(format!("{},{}=debug", BIN_NAME, CRATE_NAME)),
        3 => EnvFilter::new(format!("{},{}=trace", BIN_NAME, CRATE_NAME)),
        _ => EnvFilter::new("trace"),
    };

    tracing_subscriber::fmt().with_env_filter(filter).init()
}
