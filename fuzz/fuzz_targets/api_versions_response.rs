#![no_main]

use kafrust_protocol::api::api_versions::{ApiVersionsResponseV0, ApiVersionsResponseV3};
use kafrust_protocol::codec::{DecodeLimits, Decoder};
use libfuzzer_sys::fuzz_target;

fn limits() -> DecodeLimits {
    DecodeLimits::new().with_max_array_elements(1_024)
}

fuzz_target!(|data: &[u8]| {
    let mut v0 = Decoder::with_limits(data, limits());
    let _ = ApiVersionsResponseV0::decode_body(&mut v0);

    let mut v3 = Decoder::with_limits(data, limits());
    let _ = ApiVersionsResponseV3::decode_body(&mut v3);
});
