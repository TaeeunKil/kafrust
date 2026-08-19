#![no_main]

use kafrust_protocol::api::describe_share_group_offsets::{
    DescribeShareGroupOffsetsResponseV0, DescribeShareGroupOffsetsResponseV1,
};
use kafrust_protocol::codec::{DecodeLimits, Decoder};
use libfuzzer_sys::fuzz_target;

fn limits() -> DecodeLimits {
    DecodeLimits::new().with_max_array_elements(1_024)
}

fuzz_target!(|data: &[u8]| {
    let mut v0 = Decoder::with_limits(data, limits());
    let _ = DescribeShareGroupOffsetsResponseV0::decode_body(&mut v0);

    let mut v1 = Decoder::with_limits(data, limits());
    let _ = DescribeShareGroupOffsetsResponseV1::decode_body(&mut v1);
});
