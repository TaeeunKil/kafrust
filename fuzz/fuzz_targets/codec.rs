#![no_main]

use kafrust_protocol::codec::{DecodeLimits, Decoder};
use libfuzzer_sys::fuzz_target;

fn limits() -> DecodeLimits {
    DecodeLimits::new()
        .with_max_array_elements(1_024)
        .with_max_decompressed_record_bytes(8 * 1024 * 1024)
}

fuzz_target!(|data: &[u8]| {
    let mut decoder = Decoder::with_limits(data, limits());
    let _ = decoder.read_i8();
    let _ = decoder.read_bool();
    let _ = decoder.read_i16();
    let _ = decoder.read_i32();
    let _ = decoder.read_i64();
    let _ = decoder.read_uuid();
    let _ = decoder.read_f64();
    let _ = decoder.read_string();
    let _ = decoder.read_nullable_string();
    let _ = decoder.read_bytes();
    let _ = decoder.read_nullable_bytes();
    let _ = decoder.read_unsigned_varint();
    let _ = decoder.read_varint();
    let _ = decoder.read_varlong();
    let _ = decoder.read_varint_bytes();
    let _ = decoder.read_varint_nullable_bytes();
    let _ = decoder.read_compact_string();
    let _ = decoder.read_compact_nullable_string();
    let _ = decoder.read_compact_bytes();
    let _ = decoder.read_compact_nullable_bytes();
    let _ = decoder.read_array("fuzz array", |_| Ok::<_, kafrust_protocol::Error>(()));
    let _ = decoder.read_compact_array("fuzz compact array", |_| {
        Ok::<_, kafrust_protocol::Error>(())
    });
    let _ = decoder.read_tagged_fields();
});
