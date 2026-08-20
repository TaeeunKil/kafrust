#![no_main]

use kafrust_protocol::api::list_groups::{
    ListGroupsResponseV1, ListGroupsResponseV4, ListGroupsResponseV5,
};
use kafrust_protocol::codec::{DecodeLimits, Decoder};
use libfuzzer_sys::fuzz_target;

fn limits() -> DecodeLimits {
    DecodeLimits::new().with_max_array_elements(1_024)
}

fuzz_target!(|data: &[u8]| {
    let mut v1 = Decoder::with_limits(data, limits());
    let _ = ListGroupsResponseV1::decode_body(&mut v1);

    let mut v4 = Decoder::with_limits(data, limits());
    let _ = ListGroupsResponseV4::decode_body(&mut v4);

    let mut v5 = Decoder::with_limits(data, limits());
    let _ = ListGroupsResponseV5::decode_body(&mut v5);
});
