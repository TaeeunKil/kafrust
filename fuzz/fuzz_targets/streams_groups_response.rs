#![no_main]

use kafrust_protocol::api::streams_group_describe::StreamsGroupDescribeResponseV0;
use kafrust_protocol::api::streams_group_heartbeat::StreamsGroupHeartbeatResponseV0;
use kafrust_protocol::codec::{DecodeLimits, Decoder};
use libfuzzer_sys::fuzz_target;

fn limits() -> DecodeLimits {
    DecodeLimits::new().with_max_array_elements(1_024)
}

fuzz_target!(|data: &[u8]| {
    let mut describe = Decoder::with_limits(data, limits());
    let _ = StreamsGroupDescribeResponseV0::decode_body(&mut describe);

    let mut heartbeat = Decoder::with_limits(data, limits());
    let _ = StreamsGroupHeartbeatResponseV0::decode_body(&mut heartbeat);
});
