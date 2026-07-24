# Performance Benchmarks

kafrust includes an opt-in live Kafka benchmark. It measures complete
high-level Produce and Fetch operations rather than isolated codec functions.
Results are diagnostic baselines, not CI pass/fail thresholds.

## Run Locally

Create a single-partition topic, set `KAFRUST_TOPIC`, then run:

```bash
cargo run --release -p kafrust --example throughput_benchmark
```

The example prints one JSON object containing:

- producer and consumer records per second
- producer and consumer MiB per second
- Produce batch p50, p95, and p99 latency
- broker request and high-level retry counts
- record count, batch size, payload size, and compression

The consumer starts from the first measured Produce offset, so an existing
topic can be reused without counting older records.

## Configuration

| Variable | Default | Meaning |
| --- | ---: | --- |
| `KAFRUST_BENCH_RECORDS` | `20000` | measured records |
| `KAFRUST_BENCH_BATCH_SIZE` | `200` | records per Produce batch |
| `KAFRUST_BENCH_PAYLOAD_BYTES` | `1024` | value bytes per record |
| `KAFRUST_BENCH_WARMUP_BATCHES` | `3` | unmeasured warmup batches |
| `KAFRUST_COMPRESSION` | `none` | `none`, `gzip`, `snappy`, `lz4`, or `zstd` |

The standard connection and security environment variables used by the other
examples are also supported.

## GitHub Baseline

The manual `Kafka Benchmark` workflow runs against a single Apache Kafka 4.3.1
KRaft broker. It measures 100-byte, 1-KiB, and 10-KiB uncompressed records plus
a 1-KiB Zstd profile, then publishes `benchmark-results.jsonl` as a 90-day
artifact.

Compare runs only when the runner class, Kafka version, record count, batch
size, and compression match. Shared GitHub runners are useful for detecting
large regressions, but their results are not stable enough for small percentage
claims.
