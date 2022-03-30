// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::CmdProcessor;

use crate::node::{api::cmds::Cmd, Error, Result};

use custom_debug::Debug;
use priority_queue::PriorityQueue;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::RwLock, time::Instant};

// A command/subcommand id e.g. "963111461", "963111461.0"
type CmdId = String;

type Priority = i32;

const MAX_RETRIES: usize = 1; // drops on first error..
const DEFAULT_DESIRED_RATE: f64 = 50.0; // 50 msgs / s
const SLEEP_TIME: Duration = Duration::from_millis(20);

#[derive(Clone)]
pub(crate) struct CmdCtrl {
    cmd_queue: Arc<RwLock<PriorityQueue<SendJob, Priority>>>,
    finished: MsgThroughput,
    attempted: MsgThroughput,
    desired_rate: Arc<RwLock<f64>>, // cmds per s
    stopped: Arc<RwLock<bool>>,
    processor: CmdProcessor,
}

impl CmdCtrl {
    pub(crate) fn new(processor: CmdProcessor) -> Self {
        let session = Self {
            cmd_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            finished: MsgThroughput::default(),
            attempted: MsgThroughput::default(),
            desired_rate: Arc::new(RwLock::new(DEFAULT_DESIRED_RATE)),
            stopped: Arc::new(RwLock::new(false)),
            processor,
        };

        let session_clone = session.clone();
        let _ = tokio::task::spawn(async move { session_clone.keep_sending().await });

        session
    }

    #[allow(unused)]
    pub(crate) async fn throughput(&self) -> f64 {
        self.finished.value()
    }

    #[allow(unused)]
    pub(crate) async fn success_ratio(&self) -> f64 {
        self.finished.value() / self.attempted.value()
    }

    pub(crate) async fn extend(&self, cmds: Vec<Cmd>) -> Vec<Result<SendWatcher>> {
        let mut results = vec![];

        for cmd in cmds {
            let _ = results.push(self.push(cmd).await);
        }

        results
    }

    pub(crate) async fn push(&self, cmd: Cmd) -> Result<SendWatcher> {
        let id: CmdId = rand::random::<u32>().to_string();
        self.push_internal(id, cmd).await
    }

    async fn push_internal(&self, id: CmdId, cmd: Cmd) -> Result<SendWatcher> {
        if self.stopped().await {
            // should not happen (be reachable)
            return Err(Error::InvalidState);
        }

        let priority = cmd.priority();

        let (watcher, reporter) = status_watching();

        let job = SendJob {
            id,
            cmd,
            retries: 0,
            reporter,
        };

        let _ = self.cmd_queue.write().await.push(job, priority);

        Ok(watcher)
    }

    #[allow(unused)]
    pub(crate) async fn update_send_rate(&self, desired_rate: f64) {
        *self.desired_rate.write().await = desired_rate;
    }

    // consume self
    // NB that clones could still exist, however they would be in the disconnected state
    // if only accessing via session map (as intended)
    #[allow(unused)]
    pub(crate) async fn stop(self) {
        *self.stopped.write().await = true;
        self.cmd_queue.write().await.clear();
    }

    // could be accessed via a clone
    async fn stopped(&self) -> bool {
        *self.stopped.read().await
    }

    async fn keep_sending(&self) {
        loop {
            if self.stopped().await {
                break;
            }

            let expected_rate = { *self.desired_rate.read().await };
            let actual_rate = self.attempted.value();
            if actual_rate > expected_rate {
                let diff = actual_rate - expected_rate;
                let diff_ms = Duration::from_millis((diff * 1000_f64) as u64);
                tokio::time::sleep(diff_ms).await;
                continue;
            } else if self.cmd_queue.read().await.is_empty() {
                tokio::time::sleep(SLEEP_TIME).await;
                continue;
            }

            #[cfg(feature = "test-utils")]
            {
                let queue = self.cmd_queue.read().await;
                debug!("Cmd queue length: {}", queue.len());
                // for (job, priority) in queue.iter() {
                //     debug!("Prio: {}, Job: {:?}", priority, job);
                // }
            }

            let queue_res = { self.cmd_queue.write().await.pop() };
            if let Some((mut job, prio)) = queue_res {
                if job.retries >= MAX_RETRIES {
                    // break this loop, report error to other nodes
                    // await decision on how to continue

                    // (or send event on a channel (to report error to other nodes), then sleep for a very long time, then try again?)

                    job.reporter
                        .send(CtrlStatus::MaxRetriesReached(job.retries));

                    continue; // this means we will drop this cmd entirely!
                }
                let clone = self.clone();
                let _ = tokio::spawn(async move {
                    match clone.processor.process_cmd(job.cmd.clone(), &job.id).await {
                        Ok(cmds) => {
                            job.reporter.send(CtrlStatus::Finished);
                            clone.finished.increment(); // on success
                                                        // todo: handle the watchers..
                            let _watchers = clone.extend(cmds).await;
                        }
                        Err(err) => {
                            job.retries += 1;
                            job.reporter.send(CtrlStatus::Error(Arc::new(err)));
                            let _ = clone.cmd_queue.write().await.push(job, prio);
                        }
                    }
                    clone.attempted.increment(); // both on fail and success
                });
                // trace!(
                //     "{:?} {} cmd_id={}",
                //     LogMarker::CmdHandlingSpawned,
                //     cmd_display,
                //     &job.id,
                // );
            }
        }
    }
}

#[derive(Clone, Debug)]
struct MsgThroughput {
    msgs: Arc<AtomicUsize>,
    since: Instant,
}

impl Default for MsgThroughput {
    fn default() -> Self {
        Self {
            msgs: Arc::new(AtomicUsize::new(0)),
            since: Instant::now(),
        }
    }
}

impl MsgThroughput {
    fn increment(&self) {
        let _ = self.msgs.fetch_add(1, Ordering::SeqCst);
    }

    // msgs / s
    fn value(&self) -> f64 {
        let msgs = self.msgs.load(Ordering::SeqCst);
        let seconds = (Instant::now() - self.since).as_secs();
        msgs as f64 / f64::max(1.0, seconds as f64)
    }
}

#[derive(Debug)]
pub(crate) struct SendJob {
    cmd: Cmd,
    id: CmdId,
    retries: usize,
    reporter: StatusReporting,
}

impl PartialEq for SendJob {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for SendJob {}

impl std::hash::Hash for SendJob {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        //self.cmd.hash(state);
        self.id.hash(state);
        self.retries.hash(state);
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CtrlStatus {
    Enqueued,
    Finished,
    Error(Arc<Error>),
    MaxRetriesReached(usize),
    #[allow(unused)]
    WatcherDropped,
}

pub(crate) struct SendWatcher {
    receiver: tokio::sync::watch::Receiver<CtrlStatus>,
}

impl SendWatcher {
    /// Reads current status
    #[allow(unused)]
    pub(crate) fn status(&self) -> CtrlStatus {
        self.receiver.borrow().clone()
    }

    /// Waits until a new status arrives.
    #[allow(unused)]
    pub(crate) async fn await_change(&mut self) -> CtrlStatus {
        if self.receiver.changed().await.is_ok() {
            self.receiver.borrow_and_update().clone()
        } else {
            CtrlStatus::WatcherDropped
        }
    }
}

#[derive(Debug)]
struct StatusReporting {
    sender: tokio::sync::watch::Sender<CtrlStatus>,
}

impl StatusReporting {
    fn send(&self, status: CtrlStatus) {
        // todo: ok to drop error here?
        let _ = self.sender.send(status);
    }
}

fn status_watching() -> (SendWatcher, StatusReporting) {
    let (sender, receiver) = tokio::sync::watch::channel(CtrlStatus::Enqueued);
    (SendWatcher { receiver }, StatusReporting { sender })
}
