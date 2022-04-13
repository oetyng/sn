// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::CmdProcessor;

use crate::node::{
    api::{
        cmds::{Cmd, CmdJob},
        event::CmdProcessing,
    },
    Error, Event, Result,
};

use custom_debug::Debug;
use priority_queue::PriorityQueue;
use std::time::SystemTime;
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, RwLock},
    time::Instant,
};

type Priority = i32;

const MAX_RETRIES: usize = 1; // drops on first error..
const DEFAULT_DESIRED_RATE: f64 = 50.0; // 50 msgs / s
const SLEEP_TIME: Duration = Duration::from_millis(20);

const ORDER: Ordering = Ordering::SeqCst;

#[derive(Clone)]
pub(crate) struct CmdCtrl {
    cmd_queue: Arc<RwLock<PriorityQueue<EnqueuedJob, Priority>>>,
    finished: MsgThroughput,
    attempted: MsgThroughput,
    desired_rate: Arc<RwLock<f64>>, // cmds per s
    stopped: Arc<RwLock<bool>>,
    processor: CmdProcessor,
    id_counter: Arc<AtomicU64>,
    event_sender: mpsc::Sender<Event>,
}

impl CmdCtrl {
    pub(crate) fn new(processor: CmdProcessor, event_sender: mpsc::Sender<Event>) -> Self {
        let session = Self {
            cmd_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            finished: MsgThroughput::default(),
            attempted: MsgThroughput::default(),
            desired_rate: Arc::new(RwLock::new(DEFAULT_DESIRED_RATE)),
            stopped: Arc::new(RwLock::new(false)),
            processor,
            id_counter: Arc::new(AtomicU64::new(0)),
            event_sender,
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
        if self.stopped().await {
            // should not happen (be reachable)
            return Err(Error::InvalidState);
        }

        let priority = cmd.priority();

        let (watcher, reporter) = status_watching();

        let job = EnqueuedJob {
            job: CmdJob::new(self.id_counter.fetch_add(1, ORDER), cmd, SystemTime::now()),
            retries: 0,
            reporter,
        };

        info!("Enqueued: {:?}", job.job);

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

    async fn notify(&self, event: Event) {
        if self.event_sender.send(event).await.is_err() {
            error!("Event receiver has been closed");
        }
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
            if let Some((mut enqueued, prio)) = queue_res {
                if enqueued.retries >= MAX_RETRIES {
                    // break this loop, report error to other nodes
                    // await decision on how to continue

                    // (or send event on a channel (to report error to other nodes), then sleep for a very long time, then try again?)

                    enqueued
                        .reporter
                        .send(CtrlStatus::MaxRetriesReached(enqueued.retries));

                    continue; // this means we will drop this cmd entirely!
                }
                let clone = self.clone();
                let _ = tokio::spawn(async move {
                    if enqueued.retries == 0 {
                        clone
                            .notify(Event::CmdProcessing(CmdProcessing::Started {
                                job: enqueued.job.clone(),
                                time: SystemTime::now(),
                            }))
                            .await;
                    } else {
                        clone
                            .notify(Event::CmdProcessing(CmdProcessing::Retrying {
                                job: enqueued.job.clone(),
                                retry: enqueued.retries,
                                time: SystemTime::now(),
                            }))
                            .await;
                    }
                    match clone
                        .processor
                        .process_cmd(enqueued.job.cmd().clone())
                        .await
                    {
                        Ok(cmds) => {
                            enqueued.reporter.send(CtrlStatus::Finished);
                            clone.finished.increment(); // on success

                            // todo: handle the watchers..
                            // todo: use parent cmd prio? factor for that q: are the subcmds always _part of the completion_ of the parent cmd, or can they also be unrelated to that?
                            let _watchers = clone.extend(cmds).await;
                            clone
                                .notify(Event::CmdProcessing(CmdProcessing::Finished {
                                    job: enqueued.job,
                                    time: SystemTime::now(),
                                }))
                                .await;
                        }
                        Err(error) => {
                            clone
                                .notify(Event::CmdProcessing(CmdProcessing::Failed {
                                    job: enqueued.job.clone(),
                                    retry: enqueued.retries,
                                    time: SystemTime::now(),
                                    error: format!("{:?}", error),
                                }))
                                .await;
                            enqueued.retries += 1;
                            enqueued.reporter.send(CtrlStatus::Error(Arc::new(error)));
                            let _ = clone.cmd_queue.write().await.push(enqueued, prio);
                        }
                    }
                    clone.attempted.increment(); // both on fail and success
                });
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
pub(crate) struct EnqueuedJob {
    job: CmdJob,
    retries: usize,
    reporter: StatusReporting,
}

impl PartialEq for EnqueuedJob {
    fn eq(&self, other: &Self) -> bool {
        self.job.id() == other.job.id()
    }
}

impl Eq for EnqueuedJob {}

impl std::hash::Hash for EnqueuedJob {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        //self.job.hash(state);
        self.job.id().hash(state);
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
