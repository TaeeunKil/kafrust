#![no_main]

use kafrust_protocol::api::offset_fetch::{
    OffsetFetchResponseV10, OffsetFetchResponseV2, OffsetFetchResponseV9,
};
use kafrust_protocol::codec::{DecodeLimits, Decoder};
use libfuzzer_sys::fuzz_target;

fn limits() -> DecodeLimits {
    DecodeLimits::new().with_max_array_elements(1_024)
}

fuzz_target!(|data: &[u8]| {
    let mut v2 = Decoder::with_limits(data, limits());
    let _ = OffsetFetchResponseV2::decode_body(&mut v2);

    let mut v9 = Decoder::with_limits(data, limits());
    let _ = OffsetFetchResponseV9::decode_body(&mut v9);

    let mut v10 = Decoder::with_limits(data, limits());
    let _ = OffsetFetchResponseV10::decode_body(&mut v10);
});
