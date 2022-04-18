// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub(crate) mod load_sampling;
pub(crate) mod sampling;

use self::load_sampling::LoadSampling;
use self::sampling::Sampling;

use std::time::Duration;
use tokio::time::MissedTickBehavior;

pub(crate) const INITIAL_MSGS_PER_S: f64 = 500.0;
pub(crate) const INITIAL_CMDS_PER_S: f64 = 250.0;

const INTERVAL_ONE_MINUTE: u8 = 1;
pub(crate) const INTERVAL_FIFTEEN_MINUTES: u8 = 15;

// Used to measure local activity per s, using 15 min moving average.
// Specifically this measures cmds handled, and msgs received.
// The value is updated every minute.

#[derive(Debug, Clone)]
pub(crate) struct Measurements {
    pub(crate) cmd_sampling: Sampling,
    pub(crate) msg_sampling: Sampling,
    pub(crate) load_sampling: LoadSampling,
}

impl Measurements {
    pub(crate) fn new() -> Self {
        let instance = Self {
            cmd_sampling: Sampling::new(
                "cmds".to_string(),
                INITIAL_CMDS_PER_S,
                INTERVAL_FIFTEEN_MINUTES,
            ),
            msg_sampling: Sampling::new(
                "msgs".to_string(),
                INITIAL_MSGS_PER_S,
                INTERVAL_FIFTEEN_MINUTES,
            ),
            load_sampling: LoadSampling::new(),
        };

        // start background sampler
        let clone = instance.clone();
        let _ = tokio::task::spawn(async move {
            clone.run_sampler().await;
        });

        instance
    }

    pub(crate) async fn increment_cmds(&self) {
        self.cmd_sampling.increment().await;
    }

    pub(crate) async fn increment_msgs(&self) {
        self.msg_sampling.increment().await;
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

        loop {
            let _instant = interval.tick().await;
            self.load_sampling.sample().await;
            let load = &self.load_sampling.value().await;

            debug!("Load sample {:?}", load);

            self.cmd_sampling.measure(load.fifteen).await;
            self.msg_sampling.measure(load.fifteen).await;
        }
    }
}
