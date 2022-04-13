// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

const MAX_CPU_LOAD: f64 = 80.0; // unit: percent

const ORDER: Ordering = Ordering::SeqCst;

#[derive(Debug, Clone)]
pub(super) struct Sampling {
    name: String,
    period: u8,
    default_load_per_event: f64,
    samples: Arc<RwLock<VecDeque<AtomicUsize>>>,
    weighted_value: Arc<RwLock<f64>>,
    initial_value: f64,
}

impl Sampling {
    pub(super) fn new(name: String, initial_value: f64, period: u8) -> Self {
        Self {
            name,
            period,
            default_load_per_event: MAX_CPU_LOAD / initial_value, // unit: percent-seconds per msg
            samples: Arc::new(RwLock::new(VecDeque::from([AtomicUsize::new(0)]))),
            weighted_value: Arc::new(RwLock::new(initial_value)),
            initial_value,
        }
    }

    pub(super) async fn weighted_value(&self) -> f64 {
        *self.weighted_value.read().await
    }

    pub(super) async fn increment(&self) {
        // increments the latest entry, which represents current interval
        if let Some(entry) = self.samples.read().await.back() {
            let _ = entry.fetch_add(1, ORDER);
        }
    }

    pub(super) async fn measure(&self, period_load: f64) {
        let (avg, len) = {
            let samples = self.samples.read().await;
            let len = samples.len();
            let sum: usize = samples.iter().map(|s| s.load(ORDER)).sum();
            // unit: events per [period]
            let event_count = usize::max(1, sum) as f64;
            let avg = f64::max(self.initial_value, event_count / len as f64);
            (avg, len)
        };

        // unit: percent-seconds per event
        // (percent-seconds per [period] / events per [period] => percent-seconds per event)
        let load_per_event = period_load / avg;

        debug!(
            "Load per {} {:?} ({} min moving avg)",
            self.name, load_per_event, len
        );

        // unit: events / s
        // (percent / percent-seconds per event => events / s)
        let max_events_per_s = MAX_CPU_LOAD / f64::min(self.default_load_per_event, load_per_event);

        *self.weighted_value.write().await = max_events_per_s;

        debug!(
            "Max {} per s {:?} ({} min moving avg)",
            self.name, max_events_per_s, len
        );

        let mut samples = self.samples.write().await;

        // if full, drop oldest entry
        if len == self.period as usize {
            let _ = samples.pop_front();
        }

        // insert new
        samples.push_back(AtomicUsize::new(0));
    }
}
