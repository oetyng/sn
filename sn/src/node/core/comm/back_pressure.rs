// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{node::core::Measurements, types::Peer};

use itertools::Itertools;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::RwLock, time::Instant};

const MIN_REPORT_INTERVAL: Duration = Duration::from_secs(60);
const REPORT_TTL: Duration = Duration::from_secs(300); // 5 minutes

const SANITY_MAX_PER_S_AND_PEER: f64 = 100.0;
const SANITY_MIN_PER_S_AND_PEER: f64 = 1.0; // 1 every s

type OutgoingReports = BTreeMap<Peer, (Instant, f64)>;

#[derive(Clone)]
pub(crate) struct BackPressure {
    monitoring: Measurements,
    our_reports: Arc<RwLock<OutgoingReports>>,
    last_eviction: Arc<RwLock<Instant>>,
}

impl BackPressure {
    pub(crate) fn new(monitoring: Measurements) -> Self {
        Self {
            monitoring,
            our_reports: Arc::new(RwLock::new(OutgoingReports::new())),
            last_eviction: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub(crate) async fn count_msg(&self) {
        self.monitoring.increment_msgs().await;
    }

    /// Sent to nodes calling us, if the value has changed significantly.
    pub(crate) async fn tolerated_msgs_per_s(&self, caller: &Peer) -> Option<f64> {
        let now = Instant::now();
        let sent = { self.our_reports.read().await.get(caller).copied() };
        let tolerated_msgs_per_s = match sent {
            Some((then, _)) => {
                // do not refresh too often
                if now > then && now - then > MIN_REPORT_INTERVAL {
                    self.try_get_new_value(caller, now).await
                } else {
                    return None; // send None if too short time has elapsed
                }
            }
            None => self.try_get_new_value(caller, now).await,
        };

        tolerated_msgs_per_s
    }

    async fn try_get_new_value(&self, caller: &Peer, now: Instant) -> Option<f64> {
        // first, try evict expired (placed in this block, we reduce the frequency of this check)
        let last_eviction = { *self.last_eviction.read().await };
        // only try evict when there's any likelihood of there being any expired..
        if now > last_eviction && now - last_eviction > REPORT_TTL {
            self.evict_expired(now).await;
        }

        // then measure stuff

        let msgs_per_s = self.monitoring.max_msgs_per_s().await;
        let num_callers = { self.our_reports.read().await.len() as f64 };

        // avoid divide by 0 errors
        let msgs_per_s_and_peer = msgs_per_s / f64::max(1.0, num_callers);

        // make sure not more than sanity max
        let msgs_per_s_and_peer = f64::min(SANITY_MAX_PER_S_AND_PEER, msgs_per_s_and_peer);

        // make sure not less than sanity min
        let msgs_per_s_and_peer = f64::max(SANITY_MIN_PER_S_AND_PEER, msgs_per_s_and_peer);

        debug!("Number of callers {:?}", num_callers);
        debug!("Msgs per s and peer {:?}", msgs_per_s_and_peer);

        let prev = self.our_reports.read().await.get(caller).copied();

        // bound update rates by require some minimum level of change
        let significant_change = |change_ratio| {
            // if current val is 5 % worse, or 10 % better, then update our peer with it
            0.95 >= change_ratio || change_ratio >= 1.1 || change_ratio == 0.0
        };

        let (record_changes, update_sender) = if let Some((_, previous)) = prev {
            let change_ratio = msgs_per_s_and_peer / previous;
            if significant_change(change_ratio) {
                // we want to store the value, and update the node
                (true, true)
            } else {
                debug!(
                    "No significant change of backpressure value (previous: {}, ratio: {})",
                    previous, change_ratio
                );
                // we neither want to store the value, nor update the sender
                (false, false)
            }
        } else {
            let change_ratio = msgs_per_s_and_peer / SANITY_MAX_PER_S_AND_PEER;
            if significant_change(change_ratio) {
                // we want to store the value, and update the node
                (true, true)
            } else {
                debug!(
                    "No significant change of sender default backpressure value (ratio: {})",
                    change_ratio
                );
                // we want to store the value, but not update the sender
                (true, false)
            }
        };

        if record_changes {
            let _ = self
                .our_reports
                .write()
                .await
                .insert(*caller, (now, msgs_per_s_and_peer));
        }

        if update_sender {
            Some(msgs_per_s_and_peer)
        } else {
            None
        }
    }

    async fn evict_expired(&self, now: Instant) {
        let expired = {
            self.our_reports
                .read()
                .await
                .iter()
                .filter_map(|(key, (last_seen, _))| {
                    let last_seen = *last_seen;
                    if now > last_seen && now - last_seen > REPORT_TTL {
                        Some(*key)
                    } else {
                        None
                    }
                })
                .collect_vec()
        };

        for id in expired {
            let _prev = self.our_reports.write().await.remove(&id);
        }

        *self.last_eviction.write().await = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::core::monitoring::{
        load_sampling::LoadSampling, sampling::Sampling, Measurements, INITIAL_CMDS_PER_S,
        INITIAL_MSGS_PER_S, INTERVAL_FIFTEEN_MINUTES,
    };
    use crate::node::network_knowledge::test_utils::gen_addr;
    use eyre::Result;
    use tokio::{sync::RwLock, time::Instant};
    use xor_name;

    #[tokio::test]
    // Do not trigger tolerated_msgs_per_s() if called by the same peer within the MIN_REPORT_INTERVAL
    async fn report_intervals() -> Result<()> {
        let backpressure = setup_backpressure();
        let peer = Peer::new(xor_name::rand::random(), gen_addr());
        let _ = backpressure
            .our_reports
            .write()
            .await
            .insert(peer, (Instant::now(), 20_f64));
        assert_eq!(None, backpressure.tolerated_msgs_per_s(&peer).await);

        // after MIN_REPORT_INTERVAL
        let _ = backpressure
            .our_reports
            .write()
            .await
            .insert(peer, (Instant::now() - MIN_REPORT_INTERVAL, 20_f64));
        assert_eq!(
            Some(SANITY_MAX_PER_S_AND_PEER),
            backpressure.tolerated_msgs_per_s(&peer).await
        );
        Ok(())
    }

    #[tokio::test]
    // Remove entries from our_reports if it's last_seen > REPORT_TTL
    async fn evict_old_reports() -> Result<()> {
        let mut backpressure = setup_backpressure();
        let peer1 = Peer::new(xor_name::rand::random(), gen_addr());
        let peer2 = Peer::new(xor_name::rand::random(), gen_addr());

        let mut our_reports: OutgoingReports = OutgoingReports::new();
        let now = Instant::now();
        let _ = our_reports.insert(peer1, (now, 10_f64));

        // last_seen > REPORT_TTL; peer2 will be evicted
        let _ = our_reports.insert(peer2, (Instant::now() - REPORT_TTL, 20_f64));
        backpressure.our_reports = Arc::new(RwLock::new(our_reports));
        backpressure.evict_expired(Instant::now()).await;

        assert_eq!(
            Some((now, 10_f64)),
            backpressure.our_reports.read().await.get(&peer1).copied()
        );
        assert_eq!(None, backpressure.our_reports.read().await.get(&peer2));
        Ok(())
    }

    #[tokio::test]
    // If new peer, just add it to our_reports but don't update sender (return None)
    async fn new_peer_with_insignificant_change() -> Result<()> {
        let backpressure = setup_backpressure();
        let peer = Peer::new(xor_name::rand::random(), gen_addr());
        let now = Instant::now();
        let msgs_per_s = backpressure.try_get_new_value(&peer, now).await;

        assert_eq!(None, msgs_per_s);
        assert_eq!(
            Some((now, SANITY_MAX_PER_S_AND_PEER)),
            backpressure.our_reports.read().await.get(&peer).copied()
        );

        Ok(())
    }

    #[tokio::test]
    // record_change = true, update_sender = true.
    async fn old_peer_with_significant_change() -> Result<()> {
        let backpressure = setup_backpressure();
        let peer = Peer::new(xor_name::rand::random(), gen_addr());
        let now = Instant::now();

        // change_ratio <= 0.95
        let _ = backpressure
            .our_reports
            .write()
            .await
            .insert(peer, (now, 110_f64));
        let msgs_per_s = backpressure.try_get_new_value(&peer, now).await;
        assert_eq!(Some(SANITY_MAX_PER_S_AND_PEER), msgs_per_s);
        assert_eq!(
            Some((now, SANITY_MAX_PER_S_AND_PEER)),
            backpressure.our_reports.read().await.get(&peer).copied()
        );

        // change_ratio >= 1.1
        let _ = backpressure
            .our_reports
            .write()
            .await
            .insert(peer, (now, 85_f64));
        let msgs_per_s = backpressure.try_get_new_value(&peer, now).await;
        assert_eq!(Some(SANITY_MAX_PER_S_AND_PEER), msgs_per_s);
        assert_eq!(
            Some((now, SANITY_MAX_PER_S_AND_PEER)),
            backpressure.our_reports.read().await.get(&peer).copied()
        );

        Ok(())
    }

    #[tokio::test]
    // record_change = false, update_sender = false.
    async fn old_peer_with_insignificant_change() -> Result<()> {
        let backpressure = setup_backpressure();
        let peer = Peer::new(xor_name::rand::random(), gen_addr());
        let now = Instant::now();

        // !(change_ratio <= 0.95)
        let report = (now, 105_f64);
        let _ = backpressure.our_reports.write().await.insert(peer, report);
        let msgs_per_s = backpressure.try_get_new_value(&peer, now).await;
        assert_eq!(None, msgs_per_s);
        assert_eq!(
            Some(report),
            backpressure.our_reports.read().await.get(&peer).copied()
        );

        // !(change_ratio >= 1.1)
        let report = (now, 95_f64);
        let _ = backpressure.our_reports.write().await.insert(peer, report);
        let msgs_per_s = backpressure.try_get_new_value(&peer, now).await;
        assert_eq!(None, msgs_per_s);
        assert_eq!(
            Some(report),
            backpressure.our_reports.read().await.get(&peer).copied()
        );

        Ok(())
    }

    fn setup_backpressure() -> BackPressure {
        let monitoring = Measurements {
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
        BackPressure {
            monitoring,
            our_reports: Arc::new(RwLock::new(OutgoingReports::new())),
            last_eviction: Arc::new(RwLock::new(Instant::now())),
        }
    }
}
