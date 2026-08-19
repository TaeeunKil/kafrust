#![no_main]

use kafrust_protocol::frame::decode_frame;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_frame(data);
});
