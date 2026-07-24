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
| `KAFRUST_BENCH_MAX_BATCH_BYTES` | `921600` | maximum encoded Produce chunk |
| `KAFRUST_COMPRESSION` | `none` | `none`, `gzip`, `snappy`, `lz4`, or `zstd` |

The standard connection and security environment variables used by the other
examples are also supported. The default encoded batch limit stays below
Kafka's default broker message-size limit; larger logical batches are split
into multiple Produce requests.

## GitHub Baseline

The manual `Kafka Benchmark` workflow runs against a single Apache Kafka 4.3.1
KRaft broker. It measures 100-byte, 1-KiB, and 10-KiB uncompressed records plus
a 1-KiB Zstd profile, then publishes `benchmark-results.jsonl` as a 90-day
artifact.

Compare runs only when the runner class, Kafka version, record count, batch
size, and compression match. Shared GitHub runners are useful for detecting
large regressions, but their results are not stable enough for small percentage
claims.

## Published Baseline

Run [`30057817575`](https://github.com/TaeeunKil/kafrust/actions/runs/30057817575)
completed on 2026-07-24 using the documented Kafka 4.3.1 single-broker profile,
20,000 records, batches of 200, and a 900-KiB encoded chunk limit.

| Payload | Compression | Produce records/s | Produce MiB/s | Batch p50 | Batch p95 | Batch p99 | Fetch records/s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 B | none | 55,139 | 5.26 | 3.424 ms | 5.400 ms | 6.700 ms | 704,242 |
| 1 KiB | none | 47,883 | 46.76 | 4.070 ms | 4.957 ms | 5.848 ms | 305,362 |
| 10 KiB | none | 2,437 | 23.79 | 70.533 ms | 112.491 ms | 115.600 ms | 88,355 |
| 1 KiB | Zstd | 50,555 | 49.37 | 3.555 ms | 5.865 ms | 6.979 ms | 458,840 |

The first complete safety-limit baseline, run
[`30057137300`](https://github.com/TaeeunKil/kafrust/actions/runs/30057137300),
measured 1,273 records/s for the 1-KiB uncompressed profile and 1,737 records/s
with Zstd. Table-based CRC calculation and logarithmic batch sizing raised
those profiles by 37.6x and 29.1x respectively. These are within-project
before/after results, not claims of parity with another Kafka client.

## Soak And Failure Injection

The `soak` example repeatedly produces a bounded batch and fetches it back from
the reported offset. It verifies acknowledged and consumed record counts,
requires final request and buffered-record gauges to be zero, and can require
an observed operation failure followed by recovery:

```bash
KAFRUST_SOAK_SECONDS=300 \
KAFRUST_SOAK_REQUIRE_FAILURE=true \
cargo run --release -p kafrust --example soak
```

The `Kafka Soak` workflow runs this profile weekly for five minutes against
Kafka 4.3.1. It stops the broker one third of the way through the run, waits ten
seconds, restarts it, and requires the client workload to observe and recover
from the outage. Manual runs default to two minutes and accept configurable
duration and outage inputs.

Manual run
[`30058270907`](https://github.com/TaeeunKil/kafrust/actions/runs/30058270907)
validated the profile on 2026-07-24. During 60 seconds it roundtripped 1,038,200
records, observed 145 failed high-level operations and 1,011 internal retries,
recovered after the broker restart, and finished with both in-flight request
and buffered-record gauges at zero.
