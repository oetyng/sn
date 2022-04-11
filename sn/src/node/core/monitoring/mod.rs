// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod load_sampling;
mod sampling;

use crate::btree_set;

use self::load_sampling::LoadSampling;
use self::sampling::Sampling;

use std::time::Duration;
use sysinfo::LoadAvg;
use tokio::time::MissedTickBehavior;

const INITIAL_MSGS_PER_S: f64 = 500.0;
const INITIAL_CMDS_PER_S: f64 = 250.0;

const INTERVAL_ONE_MINUTE: u8 = 1;
const INTERVAL_FIVE_MINUTES: u8 = 5;
const INTERVAL_FIFTEEN_MINUTES: u8 = 15;

// Used to measure local activity per s, specifically cmds handled, and msgs received.

#[derive(Debug, Clone)]
pub(crate) struct Measurements {
    cmd_sampling: Sampling,
    msg_sampling: Sampling,
    load_sampling: LoadSampling,
}

impl Measurements {
    pub(crate) fn new() -> Self {
        let intervals = btree_set![
            INTERVAL_ONE_MINUTE,
            INTERVAL_FIVE_MINUTES,
            INTERVAL_FIFTEEN_MINUTES
        ];

        let instance = Self {
            cmd_sampling: Sampling::new("cmds".to_string(), INITIAL_CMDS_PER_S, intervals.clone()),
            msg_sampling: Sampling::new("msgs".to_string(), INITIAL_MSGS_PER_S, intervals),
            load_sampling: LoadSampling::new(),
        };

        // start background sampler
        let clone = instance.clone();
        let _ = tokio::task::spawn(async move {
            clone.run_sampler().await;
        });

        instance
    }

    pub(crate) fn increment_cmds(&self) {
        self.cmd_sampling.increment();
    }

    pub(crate) fn increment_msgs(&self) {
        self.msg_sampling.increment();
    }

    pub(crate) async fn max_cmds_per_s(&self) -> f64 {
        self.cmd_sampling.weighted_value().await
    }

    pub(crate) async fn max_msgs_per_s(&self) -> f64 {
        self.msg_sampling.weighted_value().await
    }

    async fn run_sampler(&self) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(60 * INTERVAL_ONE_MINUTE as u64));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut count: u8 = 0;

        loop {
            let _instant = interval.tick().await;
            self.load_sampling.sample().await;
            let load = &self.load_sampling.value().await;
            count += 1;

            debug!("Load sample {:?}", load);

            if count == INTERVAL_FIFTEEN_MINUTES {
                count = 0; // reset count
                           // do 15, 5 and 1 min measurements
                self.measure(INTERVAL_ONE_MINUTE, load).await;
                self.measure(INTERVAL_FIVE_MINUTES, load).await;
                self.measure(INTERVAL_FIFTEEN_MINUTES, load).await;
            } else if count == INTERVAL_FIVE_MINUTES {
                // do 5 and 1 min measurements
                self.measure(INTERVAL_ONE_MINUTE, load).await;
                self.measure(INTERVAL_FIVE_MINUTES, load).await;
            } else {
                // on every iter
                // do 1 min measurements
                self.measure(INTERVAL_ONE_MINUTE, load).await;
            }
        }
    }

    async fn measure(&self, period: u8, load: &LoadAvg) {
        self.cmd_sampling.measure(period, load).await;
        self.msg_sampling.measure(period, load).await;
    }
}
