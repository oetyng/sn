// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod cmd_ctrl;
mod dispatcher;

use self::cmd_ctrl::CmdCtrl;
use self::dispatcher::{CmdProcessor, Dispatcher};

use crate::node::{
    api::cmds::Cmd,
    core::{Measurements, MsgEvent, Node},
    EventStream, Result,
};
use crate::types::log_markers::LogMarker;

use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task, time::MissedTickBehavior};

const PROBE_INTERVAL: Duration = Duration::from_secs(30);
const LINK_CLEANUP_INTERVAL: Duration = Duration::from_secs(120);
const DYSFUNCTION_CHECK_INTERVAL: Duration = Duration::from_secs(60);
const EVENT_CHANNEL_SIZE: usize = 2000;

#[derive(Clone)]
pub(crate) struct FlowControl {
    cmd_ctrl: CmdCtrl,
    dispatcher: Arc<Dispatcher>,
}

impl FlowControl {
    pub(crate) fn new(
        node: Arc<Node>,
        monitoring: Measurements,
        incoming_conns: mpsc::Receiver<MsgEvent>,
    ) -> (Self, EventStream) {
        let (event_sender, event_receiver) = mpsc::channel(EVENT_CHANNEL_SIZE);
        let dispatcher = Arc::new(Dispatcher::new(node));
        let cmd_ctrl = CmdCtrl::new(
            CmdProcessor::new(dispatcher.clone()),
            monitoring,
            event_sender,
        );

        dispatcher.clone().write_prefixmap_to_disk();

        let ctrl = Self {
            cmd_ctrl,
            dispatcher,
        };

        ctrl.clone().start_connection_listening(incoming_conns);
        ctrl.clone().start_network_probing();
        ctrl.clone().start_dysfunction_detection();
        ctrl.clone().start_cleaning_peer_links();

        (ctrl, EventStream::new(event_receiver))
    }

    pub(crate) async fn process(&self, cmd: Cmd) -> Result<()> {
        let _watcher = self.cmd_ctrl.push(cmd).await?;

        // loop {
        //     match watcher.await_change().await {
        //         CtrlStatus::Finished => {
        //             return Ok(());
        //         }
        //         CtrlStatus::Enqueued => {
        //             // this block should be unreachable, as Enqueued is the initial state
        //             // but let's handle it anyway..
        //             tokio::time::sleep(Duration::from_millis(100)).await;
        //             continue;
        //         }
        //         CtrlStatus::MaxRetriesReached(_) => {
        //             break;
        //         }
        //         CtrlStatus::WatcherDropped => {
        //             // the send job is dropped for some reason,
        //             break;
        //         }
        //         CtrlStatus::Error(error) => {
        //             continue; // await change on the same recipient again
        //         }
        //     }
        // }

        Ok(())
    }

    fn start_connection_listening(self, incoming_conns: mpsc::Receiver<MsgEvent>) {
        // Start listening to incoming connections.
        let _handle = task::spawn(handle_connection_events(self, incoming_conns));
    }

    fn start_network_probing(self) {
        info!("Starting to probe network");
        let _handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(PROBE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                // Send a probe message if we are an elder
                let node = &self.dispatcher.node;
                if node.is_elder().await && !node.network_knowledge().prefix().await.is_empty() {
                    match node.generate_probe_msg().await {
                        Ok(cmd) => {
                            info!("Sending probe msg");
                            if let Err(e) = self.cmd_ctrl.push(cmd).await {
                                error!("Error sending a probe msg to the network: {:?}", e);
                            }
                        }
                        Err(error) => error!("Problem generating probe msg: {:?}", error),
                    }
                }
            }
        });
    }

    fn start_cleaning_peer_links(self) {
        info!("Starting cleaning up network links");
        let _handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(LINK_CLEANUP_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = interval.tick().await;

            loop {
                let _ = interval.tick().await;
                if let Err(e) = self.cmd_ctrl.push(Cmd::CleanupPeerLinks).await {
                    error!(
                        "Error requesting a cleaning up of unused PeerLinks: {:?}",
                        e
                    );
                }
            }
        });
    }

    fn start_dysfunction_detection(self) {
        info!("Starting dysfunction checking");
        let _handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(DYSFUNCTION_CHECK_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                let unresponsive_nodes =
                    match self.dispatcher.node.get_dysfunctional_node_names().await {
                        Ok(nodes) => nodes,
                        Err(error) => {
                            error!("Error getting dysfunctional nodes: {error}");
                            BTreeSet::default()
                        }
                    };

                if !unresponsive_nodes.is_empty() {
                    debug!("{:?} : {unresponsive_nodes:?}", LogMarker::ProposeOffline);
                    if let Err(e) = self
                        .cmd_ctrl
                        .push(Cmd::ProposeOffline(unresponsive_nodes))
                        .await
                    {
                        error!("Error sending Propose Offline for dysfunctional nodes: {e:?}");
                    }
                }

                match self
                    .dispatcher
                    .node
                    .notify_about_newly_suspect_nodes()
                    .await
                {
                    Ok(suspect_cmds) => {
                        for cmd in suspect_cmds {
                            if let Err(e) = self.cmd_ctrl.push(cmd).await {
                                error!("Error processing suspect node cmds: {:?}", e);
                            }
                        }
                    }
                    Err(error) => {
                        error!("Error getting suspect nodes: {error}");
                    }
                };
            }
        });
    }
}

// Listen for incoming connection events and handle them.
async fn handle_connection_events(ctrl: FlowControl, mut incoming_conns: mpsc::Receiver<MsgEvent>) {
    while let Some(event) = incoming_conns.recv().await {
        match event {
            MsgEvent::Received {
                sender,
                wire_msg,
                original_bytes,
            } => {
                debug!(
                    "New message ({} bytes) received from: {:?}",
                    original_bytes.len(),
                    sender
                );

                let span = {
                    let node = &ctrl.dispatcher.node;
                    trace_span!("handle_message", name = %node.info.read().await.name(), ?sender, msg_id = ?wire_msg.msg_id())
                };
                let _span_guard = span.enter();

                trace!(
                    "{:?} from {:?} length {}",
                    LogMarker::DispatchHandleMsgCmd,
                    sender,
                    original_bytes.len(),
                );

                #[cfg(feature = "test-utils")]
                let wire_msg = if let Ok(msg) = wire_msg.into_msg() {
                    wire_msg.set_payload_debug(msg)
                } else {
                    wire_msg
                };

                let cmd = Cmd::HandleMsg {
                    sender,
                    wire_msg,
                    original_bytes: Some(original_bytes),
                };

                let _res = ctrl.cmd_ctrl.push(cmd).await;
            }
        }
    }

    error!("Fatal error, the stream for incoming connections has been unexpectedly closed. No new connections or messages can be received from the network from here on.");
}
