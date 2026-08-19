#![no_main]

use kafrust_protocol::record_batch::{compress_bytes, decompress_bytes, RecordBatchCompression};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let compression = match data.first().copied().map(|value| value % 5) {
        Some(0) => RecordBatchCompression::None,
        Some(1) => RecordBatchCompression::Gzip,
        Some(2) => RecordBatchCompression::Snappy,
        Some(3) => RecordBatchCompression::Lz4,
        Some(4) => RecordBatchCompression::Zstd,
        None => return,
        Some(_) => return,
    };
    let payload = &data[1..];

    if let Ok(compressed) = compress_bytes(compression, payload) {
        let _ = decompress_bytes(compression, &compressed, 8 * 1024 * 1024);
    }
    let _ = decompress_bytes(compression, payload, 8 * 1024 * 1024);
});
