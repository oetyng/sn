// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::cmds::Cmd;

use chrono::{DateTime, Utc};
use custom_debug::Debug;
use std::{sync::Arc, time::SystemTime};

/// A struct for the job of controlling the flow
/// of a [`Cmd`] in the system.
///
/// An id is assigned to it, its parent id (if any),
/// a priority by which it is ordered in the queue
/// among other pending cmd jobs, and the time the
/// job was instantiated.
#[derive(Debug, Clone)]
pub struct CmdJob {
    id: usize,
    parent_id: Option<usize>,
    cmd: Cmd,
    priority: i32,
    created_at: SystemTime,
}

impl CmdJob {
    pub(crate) fn new(
        id: usize,
        parent_id: Option<usize>,
        cmd: Cmd,
        created_at: SystemTime,
    ) -> Self {
        let priority = cmd.priority();
        Self {
            id,
            parent_id,
            cmd,
            priority,
            created_at,
        }
    }

    /// The id of the job.
    pub fn id(&self) -> usize {
        self.id
    }

    /// The parent id of the job, is the id
    /// of the job that resulted in this job being created.
    pub fn parent_id(&self) -> Option<usize> {
        self.parent_id
    }

    /// The cmd to be handled.
    pub fn cmd(&self) -> &Cmd {
        &self.cmd
    }

    /// The priority of this job.
    pub fn priority(&self) -> i32 {
        self.priority
    }

    /// The time the job was created.
    pub fn created_at(&self) -> SystemTime {
        self.created_at
    }
}

/// Informing on the processing of an individual cmd.
#[derive(custom_debug::Debug, Clone)]
pub enum CmdProcessLog {
    /// The job has started.
    Started {
        /// The job.
        job: Arc<CmdJob>,
        /// The time it started.
        time: SystemTime,
    },
    /// The job is being retried.
    Retrying {
        /// The job.
        job: Arc<CmdJob>,
        /// The re-attempt number, 1 is the first retry.
        retry: usize,
        /// The time it started the retry.
        time: SystemTime,
    },
    /// The job has finished.
    Finished {
        /// The job.
        job: Arc<CmdJob>,
        /// The time it finished.
        time: SystemTime,
    },
    /// The job failed.
    Failed {
        /// The job.
        job: Arc<CmdJob>,
        /// The number which it failed on. 0 is the first attempt, 1 is the first retry.
        retry: usize,
        /// The time it failed.
        time: SystemTime,
        /// The error it failed with.
        error: String,
    },
}

impl CmdProcessLog {
    /// Get the job being processed.
    pub fn cmd_job(&self) -> &CmdJob {
        match self {
            Self::Started { job, .. }
            | Self::Retrying { job, .. }
            | Self::Finished { job, .. }
            | Self::Failed { job, .. } => job,
        }
    }
}

impl std::fmt::Display for CmdProcessLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started { job, time } => {
                let cmd = job.cmd();
                let queued_for = time
                    .duration_since(job.created_at())
                    .unwrap_or_default()
                    .as_millis();
                let time: DateTime<Utc> = (*time).into();
                write!(
                    f,
                    "{}: Started id: {}, parent: {:?} prio: {}, queued for {} ms. Cmd: {}",
                    time.to_rfc3339(),
                    job.id(),
                    job.parent_id(),
                    job.priority(),
                    queued_for,
                    cmd,
                )
            }
            Self::Retrying { job, retry, time } => {
                let time: DateTime<Utc> = (*time).into();
                write!(
                    f,
                    "{}: Retry #{} of id: {}, prio: {}",
                    time.to_rfc3339(),
                    retry,
                    job.id(),
                    job.priority(),
                )
            }
            Self::Finished { job, time } => {
                let time: DateTime<Utc> = (*time).into();
                write!(
                    f,
                    "{}: Finished id: {}, prio: {}",
                    time.to_rfc3339(),
                    job.id(),
                    job.priority(),
                )
            }
            Self::Failed {
                job,
                retry,
                time,
                error,
            } => {
                let time: DateTime<Utc> = (*time).into();
                write!(
                    f,
                    "{}: Failed id: {}, prio: {}, on try #{}, due to: {}",
                    time.to_rfc3339(),
                    job.id(),
                    job.priority(),
                    retry,
                    error,
                )
            }
        }
    }
}
