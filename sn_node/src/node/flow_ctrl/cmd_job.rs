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
use std::time::SystemTime;

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

    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn parent_id(&self) -> Option<usize> {
        self.parent_id
    }

    pub(crate) fn cmd(&self) -> &Cmd {
        &self.cmd
    }

    pub(crate) fn priority(&self) -> i32 {
        self.priority
    }

    pub(crate) fn created_at(&self) -> SystemTime {
        self.created_at
    }
}

/// Informing on the processing of an individual cmd.
#[derive(custom_debug::Debug, Clone)]
pub enum CmdProcessLog {
    ///
    Started {
        ///
        job: CmdJob,
        ///
        time: SystemTime,
    },
    ///
    Retrying {
        ///
        job: CmdJob,
        ///
        retry: usize,
        ///
        time: SystemTime,
    },
    ///
    Finished {
        ///
        job: CmdJob,
        ///
        time: SystemTime,
    },
    ///
    Failed {
        ///
        job: CmdJob,
        ///
        retry: usize,
        ///
        time: SystemTime,
        ///
        error: String,
    },
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
