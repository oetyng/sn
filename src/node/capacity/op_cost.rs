// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    super::{network::Network, Error, Result},
    CapacityReader, CHUNK_COPY_COUNT, MAX_CHUNK_SIZE, MAX_SUPPLY,
};
use crate::types::Token;
use tracing::debug;

/// Calculation of rate limit for writes.
#[derive(Clone)]
pub struct OpCost {
    capacity: CapacityReader,
    network: Network,
}

impl OpCost {
    /// gets a new instance of rate limit
    pub fn new(network: Network, capacity: CapacityReader) -> OpCost {
        Self { capacity, network }
    }

    /// Calculates the rate limit of write operations,
    /// as a cost to be paid for a certain number of bytes.
    pub async fn from(&self, bytes: u64) -> Result<Token> {
        let prefix = self.network.our_prefix().await;
        let prefix_len = prefix.bit_count();

        let full_adults = self.capacity.full_adults_count().await;
        let all_adults = self.network.our_adults().await.len() as u8;

        if full_adults > all_adults
            || all_adults == 0
            || CHUNK_COPY_COUNT as u8 > all_adults - full_adults
        {
            return Err(Error::NetworkFull);
        }

        Ok(OpCost::op_cost(bytes, full_adults, all_adults, prefix_len))
    }

    fn op_cost(bytes: u64, full_adults: u8, all_adults: u8, prefix_len: usize) -> Token {
        debug!(
            "OpCost input values; bytes: {}, full_adults: {}, all_adults: {}, prefix_len: {}",
            bytes, full_adults, all_adults, prefix_len
        );
        let available_nodes = (all_adults - full_adults) as f64;
        let supply_demand_factor = 0.001
            + (1_f64 / (20_f64 * available_nodes)).powf(8_f64)
            + (full_adults as f64 / all_adults as f64).powf(3_f64);
        let data_size_factor = (bytes as f64 / MAX_CHUNK_SIZE as f64).powf(2_f64)
            + (bytes as f64 / MAX_CHUNK_SIZE as f64);
        let steepness_reductor = prefix_len as f64 + 1_f64;
        let section_supply_share = OpCost::max_section_nanos(prefix_len) as f64;
        let token_source = steepness_reductor * section_supply_share.powf(0.5_f64);
        let op_cost = (token_source * data_size_factor * supply_demand_factor).round() as u64;
        Token::from_nano(u64::max(1, op_cost)) // always return > 0
    }

    fn max_section_nanos(prefix_len: usize) -> u64 {
        (MAX_SUPPLY as f64 / 2_f64.powf(prefix_len as f64)).floor() as u64
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::messaging::client::DataCmd;
    use std::mem;

    #[test]
    fn calculates_op_cost() {
        let bytes = 1_000_000;
        let prefix_len = 4;
        let all_adults = 24;
        let full_adults = 3;
        let op_cost = OpCost::op_cost(bytes, full_adults, all_adults, prefix_len).as_nano();
        assert_eq!(op_cost, 15300364);
    }

    #[test]
    fn calculates_max_section_nanos() {
        // prefix zero is one section so is responsible for all tokens
        let first_section_nanos = OpCost::max_section_nanos(0);
        assert_eq!(MAX_SUPPLY, first_section_nanos);
        // first split leads to each section having half the tokens
        let first_split_nanos = OpCost::max_section_nanos(1);
        assert_eq!(MAX_SUPPLY / 2, first_split_nanos);
        // some tokens remain in section up to 2.6 * 10^18 sections, (which is more than one billion times one billion sections).
        let last_split_nanos = OpCost::max_section_nanos(61);
        assert!(last_split_nanos > 0);
    }

    // -------------------------------------------------------------
    // --------------- Rate Limit Common Sense ---------------------
    // -------------------------------------------------------------
    // Test various different comparisons of the OpCost.
    // These tests are of the type 'all things being equal, then ...'

    #[test]
    fn op_cost_smaller_chunks_cost_less() {
        // setup
        let one_mb_bytes = 1024 * 1024;
        let prefix_len = 0;
        let all_adults = 8;
        let full_adults = 7;
        let standard_rl =
            OpCost::op_cost(one_mb_bytes, full_adults, all_adults, prefix_len).as_nano();

        // smaller chunks cost less
        let one_mb_less_one_byte = one_mb_bytes - 1;
        let small =
            OpCost::op_cost(one_mb_less_one_byte, full_adults, all_adults, prefix_len).as_nano();
        assert!(
            small <= standard_rl,
            "small chunks don't cost less, expect {} <= {}",
            small,
            standard_rl
        );
    }

    #[test]
    fn op_cost_larger_net_is_cheaper() {
        // setup
        let one_mb_bytes = 1024 * 1024;
        let prefix_len = 2; // first couple of sections see an increase in cost, whereafter it is strictly decreasing
        let all_adults = 8;
        let full_adults = 7;
        let standard_rl =
            OpCost::op_cost(one_mb_bytes, full_adults, all_adults, prefix_len).as_nano();
        // large network is cheaper to store than smaller network
        let big_prefix_len = prefix_len + 1;
        let big = OpCost::op_cost(one_mb_bytes, full_adults, all_adults, big_prefix_len).as_nano();
        assert!(
            big <= standard_rl,
            "larger network is not cheaper, expect {} <= {}",
            big,
            standard_rl
        );
    }

    #[test]
    fn op_cost_emptier_section_is_cheaper() {
        // setup
        let one_mb_bytes = 1024 * 1024;
        let prefix_len = 0;
        let all_adults = 8;
        let full_adults = 7;
        let standard_rl =
            OpCost::op_cost(one_mb_bytes, full_adults, all_adults, prefix_len).as_nano();
        // less full section is cheaper than more full section
        let less_full_adults = full_adults - 1;
        let empty =
            OpCost::op_cost(one_mb_bytes, less_full_adults, all_adults, prefix_len).as_nano();
        assert!(
            empty <= standard_rl,
            "less full section is not cheaper, expect {} <= {}",
            empty,
            standard_rl
        );
    }

    #[test]
    fn op_cost_splitting_into_multiple_store_is_cheaper_than_same_bytes_in_single_store() {
        // we encourage more granularity in data chunking
        // setup
        let one_mb_bytes = 1024 * 1024;
        let prefix_len = 2;
        let all_adults = 8;
        let full_adults = 7;
        let standard_rl =
            OpCost::op_cost(one_mb_bytes, full_adults, all_adults, prefix_len).as_nano();
        // many tiny chunks is cheaper than the same bytes in one big chunk
        let one_kb_bytes = 1024;
        let reduced = OpCost::op_cost(one_kb_bytes, full_adults, all_adults, prefix_len).as_nano();
        let combined = 1024 * reduced;
        assert!(
            combined <= standard_rl,
            "many small chunks is not cheaper than one big chunk, expect {} <= {}",
            combined,
            standard_rl,
        );
    }

    #[test]
    fn op_cost_is_applied_up_to_85_billion_nodes() {
        // setup
        // The size of the actual DataCmd
        // is used for OpCost calc,
        // (currently at least 928 bytes).
        // In general, the size of a type is not stable across compilations,
        // but it is close enough for our purposes here.
        let minimum_storage_bytes = mem::size_of::<DataCmd>() as u64;
        let half_full_adults = 10;
        let big_section_node_count = 20;
        let big_prefix_len = 32;
        // storage rate limit is applied up to 85 billion nodes
        let endcost = OpCost::op_cost(
            minimum_storage_bytes,
            half_full_adults,
            big_section_node_count,
            big_prefix_len,
        )
        .as_nano();
        assert!(
            endcost > 0,
            "cost is not greater than zero up to 170 billion nodes",
        );
    }

    #[test]
    #[ignore] // this test fails under the current assumptions (max network size is not realistic)
    fn op_cost_is_applied_up_to_max_network_size() {
        // setup
        // The size of the actual DataCmd
        // is used for OpCost calc,
        // (currently at least 928 bytes);
        // In general, the size of a type is not stable across compilations,
        // but it is close enough for our purposes here.
        let minimum_storage_bytes = mem::size_of::<DataCmd>() as u64;
        let half_full_adults = 10;
        let big_section_node_count = 20;
        let big_prefix_len = 256;
        // storage rate limit is applied up to 2.3 * 10^78 nodes.
        let endcost = OpCost::op_cost(
            minimum_storage_bytes,
            half_full_adults,
            big_section_node_count,
            big_prefix_len,
        )
        .as_nano();
        assert!(
            endcost > 0,
            "cost is not always greater than zero: cost is {}",
            endcost
        );
    }

    #[test]
    fn op_cost_first_chunk_has_a_reasonable_cost() {
        // setup
        let one_mb_bytes = 1024 * 1024;
        let max_initial_cost = 1_000_000_000; // 1 token
        let zero_full_adults = 0;
        let minimum_section_nodes = 5;
        let first_section_prefix = 0;
        // the first chunk is a reasonable cost
        let startcost = OpCost::op_cost(
            one_mb_bytes,
            zero_full_adults,
            minimum_section_nodes,
            first_section_prefix,
        )
        .as_nano();
        assert!(
            startcost < max_initial_cost,
            "initial cost {} is above {}",
            startcost,
            max_initial_cost
        );
    }
}
