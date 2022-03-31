// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use sysinfo::LoadAvg;
use tokio::sync::RwLock;

const SAMPLING_INTERVAL_ONE: u8 = 1;
const SAMPLING_INTERVAL_FIVE: u8 = 5;
const SAMPLING_INTERVAL_FIFTEEN: u8 = 15;

const MAX_CPU_LOAD: f64 = 0.8; // unit: percent

const ORDER: Ordering = Ordering::SeqCst;

#[derive(Debug, Clone)]
pub(super) struct Sampling {
    name: String,
    period_counters: BTreeMap<u8, Counter>,
    events_per_s: BTreeMap<u8, Arc<RwLock<f64>>>,
    weighted_value: Arc<RwLock<f64>>,
    default_load_per_event: f64,
    initial_value: f64,
}

impl Sampling {
    pub(super) fn new(name: String, initial_value: f64, intervals: BTreeSet<u8>) -> Self {
        let mut period_counters = BTreeMap::new();
        let mut events_per_s = BTreeMap::new();

        for i in intervals {
            let _ = period_counters.insert(i, Counter::new());
            let _ = events_per_s.insert(i, Arc::new(RwLock::new(initial_value)));
        }

        Self {
            name,
            period_counters,
            events_per_s,
            weighted_value: Arc::new(RwLock::new(initial_value)),
            default_load_per_event: MAX_CPU_LOAD / initial_value, // unit: percent-seconds per msg
            initial_value,
        }
    }

    pub(super) async fn weighted_value(&self) -> f64 {
        *self.weighted_value.read().await
    }

    pub(super) fn increment(&self) {
        self.period_counters
            .iter()
            .for_each(|(_, count)| count.increment());
    }

    pub(super) async fn measure(&self, period: u8, load: &LoadAvg) {
        if let Some(counter) = self.period_counters.get(&period) {
            counter.snapshot();

            // unit: events per [period]
            let event_count = usize::max(1, counter.read()) as f64;

            debug!(
                "{} count {:?} (for period {:?} min)",
                self.name, event_count, period
            );

            // unit: percent-seconds per event
            // (percent-seconds per [period] / events per [period] => percent-seconds per event)
            let load_per_event = if period == SAMPLING_INTERVAL_ONE {
                load.one / event_count
            } else if period == SAMPLING_INTERVAL_FIVE {
                load.five / event_count
            } else if period == SAMPLING_INTERVAL_FIFTEEN {
                load.fifteen / event_count
            } else {
                self.default_load_per_event
            };

            debug!(
                "Load per {} {:?} (for period {:?} min)",
                self.name, load_per_event, period
            );

            // unit: events / s
            // (percent / percent-seconds per event => events / s)
            let max_events_per_s =
                MAX_CPU_LOAD / f64::max(self.default_load_per_event, load_per_event);

            debug!(
                "Max {} per s {:?} (for period {:?} min)",
                self.name, max_events_per_s, period
            );

            if let Some(period_value) = self.events_per_s.get(&period) {
                *period_value.write().await = max_events_per_s;
            }

            *self.weighted_value.write().await = self.calc_avg().await;
        }
    }

    async fn calc_avg(&self) -> f64 {
        let mut sum = 0.0;
        let mut number_of_points = 0;

        for (_, point) in self.events_per_s.iter() {
            sum += *point.read().await;
            number_of_points += 1;
        }

        if number_of_points > 0 {
            sum / number_of_points as f64
        } else {
            // should be unreachable, since self.events_per_s is always > 0 len
            self.initial_value
        }
    }
}

#[derive(Debug, Clone)]
struct Counter {
    running: Arc<AtomicUsize>,
    snapshot: Arc<AtomicUsize>,
}

impl Counter {
    pub(crate) fn new() -> Self {
        Self {
            running: Arc::new(AtomicUsize::new(0)),
            snapshot: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn read(&self) -> usize {
        self.snapshot.load(ORDER)
    }

    pub(crate) fn increment(&self) {
        let _ = self.running.fetch_add(1, ORDER);
    }

    pub(crate) fn snapshot(&self) {
        if let Ok(previous_val) = self.running.fetch_update(ORDER, ORDER, |_| Some(0)) {
            self.snapshot.store(previous_val, ORDER);
        }
    }
}
