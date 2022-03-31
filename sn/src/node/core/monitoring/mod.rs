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
use tokio::time::MissedTickBehavior;

const INITIAL_MSGS_PER_S: f64 = 100.0;
const INITIAL_CMDS_PER_S: f64 = 50.0;

const SAMPLING_INTERVAL_ONE: u8 = 1;
const SAMPLING_INTERVAL_FIVE: u8 = 5;
const SAMPLING_INTERVAL_FIFTEEN: u8 = 15;

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
            SAMPLING_INTERVAL_ONE,
            SAMPLING_INTERVAL_FIVE,
            SAMPLING_INTERVAL_FIFTEEN
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
        let mut interval = tokio::time::interval(Duration::from_secs(SAMPLING_INTERVAL_ONE as u64));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut count: u8 = 0;

        loop {
            let _instant = interval.tick().await;
            self.load_sampling.sample().await;

            if count == SAMPLING_INTERVAL_FIFTEEN {
                count = 0; // reset count
                           // do 15, 5 and 1 min measurements
                self.measure(SAMPLING_INTERVAL_ONE).await;
                self.measure(SAMPLING_INTERVAL_FIVE).await;
                self.measure(SAMPLING_INTERVAL_FIFTEEN).await;
            } else if count == SAMPLING_INTERVAL_FIVE {
                // do 5 and 1 min measurements
                self.measure(SAMPLING_INTERVAL_ONE).await;
                self.measure(SAMPLING_INTERVAL_FIVE).await;
            } else {
                // on every iter
                // do 1 min measurements
                self.measure(SAMPLING_INTERVAL_ONE).await;
            }
        }
    }

    async fn measure(&self, period: u8) {
        let load = self.load_sampling.value().await;
        debug!("Load sample {:?} (for period {:?} min)", load, period);
        self.cmd_sampling.measure(period, &load).await;
        self.msg_sampling.measure(period, &load).await;
    }
}
