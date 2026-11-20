//! Zstd record batch encode (Produce v7) and decode (fetch record set)
//! through the public protocol API.
//!
//! Run with: cargo bench -p kafrust-protocol --bench zstd_batch

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kafrust_protocol::api::fetch::decode_message_set;
use kafrust_protocol::api::produce::{
    encoded_record_batch_set_len_with_compression, ProducePartitionV3, ProduceRequestV7,
    ProduceTopicV3, RecordBatchIdentity, RecordBatchMessage,
};
use kafrust_protocol::codec::DecodeLimits;
use kafrust_protocol::record_batch::RecordBatchCompression;

/// Uncompressed payload sizes (keys plus values) to benchmark.
const SIZES: [(&str, usize); 3] = [
    ("1KiB", 1 << 10),
    ("16KiB", 16 << 10),
    ("256KiB", 256 << 10),
];

/// Xorshift64 with a fixed seed, so every run sees the same records.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }
}

/// JSON event records keyed by user, until keys plus values reach `target`
/// bytes.
fn event_records(target: usize) -> Vec<RecordBatchMessage> {
    const EVENTS: [&str; 7] = [
        "page_view",
        "click",
        "add_to_cart",
        "checkout",
        "login",
        "logout",
        "search",
    ];
    const PATHS: [&str; 7] = [
        "/",
        "/products",
        "/products/detail",
        "/cart",
        "/checkout",
        "/account/settings",
        "/search",
    ];
    const REGIONS: [&str; 5] = [
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-1",
    ];
    const AGENTS: [&str; 4] = [
        "Mozilla/5.0 (X11; Linux x86_64)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5)",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X)",
        "okhttp/4.12.0",
    ];
    const STATUS: [u16; 10] = [200, 200, 200, 200, 200, 201, 304, 400, 404, 500];

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut ts = 1_700_000_000_000u64;
    let mut records = Vec::new();
    let mut total = 0;
    while total < target {
        ts += rng.below(50);
        let user = rng.below(50_000);
        let key = format!("user-{user}");
        let value = format!(
            "{{\"ts\":{ts},\"user_id\":{user},\"session\":\"{:016x}\",\"event\":\"{}\",\
             \"path\":\"{}\",\"item_id\":{},\"status\":{},\"latency_ms\":{},\
             \"region\":\"{}\",\"user_agent\":\"{}\"}}",
            rng.next(),
            rng.pick(&EVENTS),
            rng.pick(&PATHS),
            rng.below(10_000),
            rng.pick(&STATUS),
            5 + rng.below(400),
            rng.pick(&REGIONS),
            rng.pick(&AGENTS),
        );
        total += key.len() + value.len();
        records.push(RecordBatchMessage::new(
            Some(key.into_bytes()),
            Some(value.into_bytes()),
            ts as i64,
        ));
    }
    records
}

fn produce_request(records: Vec<RecordBatchMessage>) -> ProduceRequestV7 {
    ProduceRequestV7 {
        correlation_id: 1,
        client_id: Some("bench".to_owned()),
        transactional_id: None,
        acks: 1,
        timeout_ms: 1_000,
        topics: vec![ProduceTopicV3 {
            name: "events".to_owned(),
            partitions: vec![ProducePartitionV3 {
                partition_index: 0,
                records,
                compression: RecordBatchCompression::Zstd,
                identity: RecordBatchIdentity::NON_IDEMPOTENT,
            }],
        }],
    }
}

/// The record set at the end of an encoded single-partition Produce request,
/// which is what a broker returns in a Fetch response.
fn record_set(request: &[u8], records: &[RecordBatchMessage]) -> Vec<u8> {
    let len = encoded_record_batch_set_len_with_compression(records, RecordBatchCompression::Zstd)
        .unwrap();
    let start = request.len() - len;
    let declared = i32::from_be_bytes(request[start - 4..start].try_into().unwrap());
    assert_eq!(declared as usize, len);
    request[start..].to_vec()
}

fn bench_zstd_batch(c: &mut Criterion) {
    let limits = DecodeLimits::new();
    let mut encode = c.benchmark_group("kafrust_zstd_encode");
    let mut cases = Vec::new();
    for (label, size) in SIZES {
        let records = event_records(size);
        let count = records.len();
        let raw: usize = records
            .iter()
            .map(|r| r.key.as_ref().map_or(0, Vec::len) + r.value.as_ref().map_or(0, Vec::len))
            .sum();
        let request = produce_request(records);

        let wire = request.encode().unwrap();
        let set = record_set(&wire, &request.topics[0].partitions[0].records);
        eprintln!(
            "kafrust zstd {label}: {count} records, {raw} B keys+values -> {} B record batch",
            set.len()
        );
        assert_eq!(decode_message_set(&set, limits).unwrap().len(), count);

        encode.throughput(Throughput::Bytes(raw as u64));
        encode.bench_with_input(
            BenchmarkId::from_parameter(label),
            &request,
            |b, request| b.iter(|| black_box(request.encode().unwrap())),
        );
        cases.push((label, raw, set));
    }
    encode.finish();

    let mut decode = c.benchmark_group("kafrust_zstd_decode");
    for (label, raw, set) in &cases {
        decode.throughput(Throughput::Bytes(*raw as u64));
        decode.bench_with_input(BenchmarkId::from_parameter(label), set, |b, set| {
            b.iter(|| black_box(decode_message_set(set, limits).unwrap()));
        });
    }
    decode.finish();
}

criterion_group!(benches, bench_zstd_batch);
criterion_main!(benches);
