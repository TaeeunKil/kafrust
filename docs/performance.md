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
- Kafka request roundtrip p50, p95, and p99 upper-bound estimates from
  `ClientMetricsSnapshot`
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
claims. `batch_p50_ms`, `batch_p95_ms`, and `batch_p99_ms` measure the complete
high-level Produce call. `request_p50_ms`, `request_p95_ms`, and
`request_p99_ms` are fixed-bucket upper-bound estimates for all Kafka request
roundtrips recorded by the shared metrics handle, including Produce, Metadata,
and Fetch requests. The request values are approximate and should be compared
with matching operation profiles.

## Published Baseline

The latest merged-main benchmark run
[`31757363941`](https://github.com/TaeeunKil/kafrust/actions/runs/31757363941)
completed on 2026-08-14 with the Kafka 4.3.1 single-broker profile,
20,000 records, batches of 200, and a 900-KiB encoded chunk limit. The new
request percentile fields were emitted for all four profiles and every profile
completed with zero retries.

| Payload | Compression | Produce records/s | Produce MiB/s | Batch p50 | Batch p95 | Batch p99 | Request p50 | Request p95 | Request p99 | Fetch records/s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 B | none | 136,098 | 12.98 | 1.468 ms | 1.895 ms | 2.354 ms | 5 ms | 5 ms | 10 ms | 683,460 |
| 1 KiB | none | 64,072 | 62.57 | 2.926 ms | 3.772 ms | 5.870 ms | 5 ms | 5 ms | 5 ms | 370,967 |
| 10 KiB | none | 3,733 | 36.45 | 52.910 ms | 55.283 ms | 65.096 ms | 5 ms | 5 ms | 5 ms | 98,517 |
| 1 KiB | Zstd | 71,767 | 70.08 | 2.550 ms | 3.799 ms | 4.623 ms | 1 ms | 5 ms | 5 ms | 516,817 |

The request percentile columns are approximate fixed-bucket upper bounds over
all Produce, Metadata, and Fetch roundtrips in each run. They are not directly
comparable to the high-level batch latency columns.

The previous merged-main benchmark run
[`31574062876`](https://github.com/TaeeunKil/kafrust/actions/runs/31574062876)
completed on 2026-08-12 with the same Kafka 4.3.1 single-broker profile,
20,000 records, batches of 200, and a 900-KiB encoded chunk limit.

| Payload | Compression | Produce records/s | Produce MiB/s | Batch p50 | Batch p95 | Batch p99 | Fetch records/s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 B | none | 115,388 | 11.00 | 1.682 ms | 2.527 ms | 3.042 ms | 782,220 |
| 1 KiB | none | 55,938 | 54.63 | 3.260 ms | 5.043 ms | 5.886 ms | 468,429 |
| 10 KiB | none | 3,292 | 32.15 | 60.528 ms | 62.515 ms | 64.186 ms | 120,303 |
| 1 KiB | Zstd | 64,355 | 62.85 | 2.961 ms | 3.995 ms | 4.928 ms | 462,430 |

These measurements supersede the older selected-profile table below for
current-main tracking. They are still diagnostic baselines, not claims of
throughput parity with another Kafka client.

An earlier merged-main benchmark run
[`31621648602`](https://github.com/TaeeunKil/kafrust/actions/runs/31621648602)
completed with the same profile after the Admin read-retry changes:

| Payload | Compression | Produce records/s | Produce MiB/s | Batch p50 | Batch p95 | Batch p99 | Fetch records/s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 B | none | 142,018 | 13.54 | 1.354 ms | 1.825 ms | 2.300 ms | 1,069,780 |
| 1 KiB | none | 68,037 | 66.44 | 2.707 ms | 3.482 ms | 4.309 ms | 590,492 |
| 10 KiB | none | 3,773 | 36.84 | 52.703 ms | 56.099 ms | 57.524 ms | 124,382 |
| 1 KiB | Zstd | 68,922 | 67.31 | 2.505 ms | 4.376 ms | 4.643 ms | 605,726 |

All four profiles completed with zero retries. These are hosted-runner
diagnostic baselines, not cross-client throughput claims.

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

The latest merged-main 120-second run
[`31574065286`](https://github.com/TaeeunKil/kafrust/actions/runs/31574065286)
roundtripped 6,223,500 records across a ten-second broker outage, observed 136
high-level operation errors, 685 failed requests, and 950 retries, recovered
successfully, and finished with zero in-flight requests and buffered records.

The newer five-minute merged-main run
[`31621654970`](https://github.com/TaeeunKil/kafrust/actions/runs/31621654970)
processed 16,773,500 1-KiB records across a ten-second broker outage, observed
147 high-level operation errors, 774 failed requests, and 1,028 retries, then
recovered successfully with zero in-flight requests and buffered records.

The latest five-minute merged-main run
[`31631358207`](https://github.com/TaeeunKil/kafrust/actions/runs/31631358207)
processed 16,847,700 1-KiB records across a ten-second broker outage, observed
148 high-level operation errors, 782 failed requests, and 1,035 retries, then
recovered successfully with zero in-flight requests and buffered records.

The scheduled five-minute run
[`31568595989`](https://github.com/TaeeunKil/kafrust/actions/runs/31568595989)
roundtripped 17,019,900 1-KiB records across the same ten-second outage,
observed 190 high-level operation errors, 1,118 failed requests, and 1,329
retries, recovered successfully, and finished with zero in-flight requests and
buffered records. The result artifact measured approximately 56.7k records/s
over the full window.

The latest 20,000-record benchmark run
[`31574062876`](https://github.com/TaeeunKil/kafrust/actions/runs/31574062876)
against Kafka 4.3.1 measured 115,388 records/s for 100-byte payloads, 55,938
records/s for 1-KiB payloads, 3,292 records/s for 10-KiB payloads, and 64,355
records/s for 1-KiB Zstd payloads. Each profile completed with zero retries;
these are hosted-runner baselines, not cross-client parity claims.

The latest 20,000-record benchmark run
[`31631563194`](https://github.com/TaeeunKil/kafrust/actions/runs/31631563194)
against Kafka 4.3.1 measured 118,556 records/s for 100-byte payloads, 54,006
records/s for 1-KiB payloads, 3,030 records/s for 10-KiB payloads, and 60,486
records/s for 1-KiB Zstd payloads. Each profile completed with zero retries;
these are hosted-runner baselines, not cross-client parity claims.
