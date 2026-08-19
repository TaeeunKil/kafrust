#![no_main]

use kafrust_protocol::api::consumer_group_describe::{
    ConsumerGroupDescribeResponseV0, ConsumerGroupDescribeResponseV1,
};
use kafrust_protocol::api::share_group_describe::ShareGroupDescribeResponseV1;
use kafrust_protocol::codec::{DecodeLimits, Decoder};
use libfuzzer_sys::fuzz_target;

fn limits() -> DecodeLimits {
    DecodeLimits::new().with_max_array_elements(1_024)
}

fuzz_target!(|data: &[u8]| {
    let mut classic = Decoder::with_limits(data, limits());
    let _ = ConsumerGroupDescribeResponseV0::decode_body(&mut classic);

    let mut consumer = Decoder::with_limits(data, limits());
    let _ = ConsumerGroupDescribeResponseV1::decode_body(&mut consumer);

    let mut share = Decoder::with_limits(data, limits());
    let _ = ShareGroupDescribeResponseV1::decode_body(&mut share);
});
