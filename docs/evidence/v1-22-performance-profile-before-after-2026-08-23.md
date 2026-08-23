# V1-22 Profile Before/After Diagnostic (2026-08-23)

This record compares the bounded source profile before and after the buffered
delivery-wait change. It is optimization evidence only: neither run is a
qualified V1-22 result, a locked baseline, or a publication gate.

## Run identity

- Before source: `8203f4baa5d92222ef08183d96acdcd4e5c428aa`
- Before workflow: [32630662251](https://github.com/TaeeunKil/kafrust/actions/runs/32630662251)
- After source: `fa419f29244091069be8f97a6375e08c0cc713b8`
- After workflow: [32631000062](https://github.com/TaeeunKil/kafrust/actions/runs/32631000062)
- Broker: `apache/kafka:4.3.1`, single-node KRaft, PLAINTEXT
- Runner: `ubuntu-latest`; each matrix job retained kernel/CPU identity
- Workload: 1-KiB values, no compression, 10-second warmup, 60-second
  measured window, 10-second samples, profile-specific worker/batch settings
- Resource counters: `/usr/bin/time -v`; Linux `perf stat` was attempted but
  was unavailable because the hosted runner reported `perf_event_paranoid=4`
- Every job in both workflows reconciled produced/consumed records and ended
  with zero in-flight and buffered gauges.

## Results

Throughput is produced records per second. Context switches are voluntary plus
involuntary counters from the timed process; they are diagnostic counters, not
an SLO threshold.

| Profile | Before throughput | After throughput | Before context switches | After context switches | Before max RSS | After max RSS | After p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| immediate, 1 worker, batch 200 | 73,146.67 | 74,106.67 | 54,533 | 68,371 | 38,700 KiB | 38,964 KiB | 1 ms |
| immediate, 4 workers, batch 200 | 184,430.00 | 197,360.00 | 398,242 | 407,184 | 39,012 KiB | 38,824 KiB | 5 ms |
| buffered, 4 workers, batch 200 | 13,453.33 | 13,480.00 | 1,956,709 | 524,281 | 38,956 KiB | 39,044 KiB | 25 ms |
| direct-consumer, 1 worker, batch 1 | 2,058.63 | 2,014.80 | 280,940 | 274,818 | 38,820 KiB | 38,864 KiB | 1 ms |

## Decision

Concurrent waiting for buffered delivery handles reduced buffered-path context
switches by approximately 73% without changing its roughly 13.5k records/s
throughput or final correctness gauges. The change is retained as a bounded
resource improvement, but it does not close the material workload-specific
throughput gap found against rust-rdkafka. The next V1-22 profiling slice must
measure buffered queue batching and repeated encoded-size work before another
runtime optimization is selected.

## Clone-free encoded-size diagnostic

The next bounded pair isolates the buffered byte-check allocation path. The
implementation keeps the same encoded-length calculation and timestamp
semantics, but builds record-batch messages from borrowed pending records
instead of cloning each `ProducerRecord` into intermediate `BatchRecord` values.

- Before source: `fa419f29244091069be8f97a6375e08c0cc713b8`
- Before workflow: [32631000062](https://github.com/TaeeunKil/kafrust/actions/runs/32631000062)
- After source: `e4f4f607be4228fa802be115d8cb92ad665ba56d`
- After workflow: [32631701269](https://github.com/TaeeunKil/kafrust/actions/runs/32631701269)
- Same workload identity: Kafka 4.3.1 single-node KRaft, PLAINTEXT,
  1-KiB values, no compression, 10-second warmup, 60-second measured window,
  10-second samples, and the four profile paths above
- Both workflows retained the same hosted-runner kernel and broker image
  digest; `perf stat` remained unavailable because `perf_event_paranoid=4`

| Profile | Before throughput | After throughput | Before context switches | After context switches | Before max RSS | After max RSS | Before p99 | After p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| immediate, 1 worker, batch 200 | 74,106.67 | 70,440.00 | 68,371 | 53,146 | 38,964 KiB | 38,840 KiB | 1 ms | 1 ms |
| immediate, 4 workers, batch 200 | 197,360.00 | 196,916.67 | 407,184 | 405,274 | 38,824 KiB | 39,052 KiB | 5 ms | 5 ms |
| buffered, 4 workers, batch 200 | 13,480.00 | 18,283.33 | 524,281 | 608,403 | 39,044 KiB | 38,944 KiB | 25 ms | 10 ms |
| direct-consumer, 1 worker, batch 1 | 2,014.80 | 1,966.98 | 274,818 | 268,273 | 38,864 KiB | 39,088 KiB | 1 ms | 1 ms |

The buffered profile rose by approximately 35.6% in this pair and its p99
bucket fell from 25 ms to 10 ms. Context switches increased for that path in
this particular run, so the result is evidence for a promising throughput
direction rather than a universal regression or SLO result. The immediate and
direct-consumer paths were effectively unchanged within hosted-runner noise.
A replication workflow
[32632004251](https://github.com/TaeeunKil/kafrust/actions/runs/32632004251)
on the same source code produced 16,216.67 buffered records/s, p99 10 ms,
38,988 KiB max RSS, and 1,829,585 context switches, with zero retries,
losses, duplicates, and drained gauges. Thus both after runs remained above
the 13,480 records/s predecessor, but the surrounding profiles also varied
substantially; the pair is not enough to establish a stable percentage gain.
The next slice must repeat this workload under a controlled repetition set and
inspect queue batching before any baseline or publication claim.

## Flush-threshold scan diagnostic

Commit `07705c5` narrows the buffered enqueue threshold check. The worker now
counts and re-encodes only the topic/partition group that received the newest
request. This is safe because the check runs immediately after append and
expired requests only remove entries; it preserves the existing record-count,
encoded-byte, flush, and delivery semantics. The old path rebuilt every group
and re-encoded every group on every enqueue.

Two same-source profile runs were retained:

- [32634691272](https://github.com/TaeeunKil/kafrust/actions/runs/32634691272)
- [32634877270](https://github.com/TaeeunKil/kafrust/actions/runs/32634877270)

Both used source `07705c518f2f2c3e9f924c0a0f77ecf1f272742f`, Kafka 4.3.1
single-node KRaft/PLAINTEXT, 1-KiB values, no compression, 10-second warmup,
60-second measured window, 10-second samples, and the four profile paths above.
All eight jobs reconciled produced/consumed records with zero retries,
unknown outcomes, loss, duplicates, and final in-flight/buffered records.

| Profile | Run 32634691272 throughput | Run 32634877270 throughput | p99 (both) | Context switches (run 1 / run 2) |
| --- | ---: | ---: | ---: | ---: |
| immediate, 1 worker, batch 200 | 76,756.67 | 76,613.33 | 1 ms | 60,648 / 78,559 |
| immediate, 4 workers, batch 200 | 182,610.00 | 196,560.00 | 5 ms | 405,121 / 407,164 |
| buffered, 4 workers, batch 200 | 15,403.33 | 16,203.33 | 10 ms | 378,946 / 1,349,630 |
| direct-consumer, 1 worker, batch 1 | 3,054.02 | 1,999.48 | 1 ms | 420,372 / 272,426 |

The buffered throughput is below the earlier 18,283 records/s pair and near
the 16,217 records/s replication, while context switches vary from 378,946 to
1,349,630. This supports retaining the allocation/scan reduction as a
semantics-preserving diagnostic, but does not establish a stable throughput or
resource percentage, lock a baseline, qualify an SLO, or authorize a release.

## Non-claims

- Not an eight-hour, five-repetition, six-profile SLO campaign.
- Not a locked baseline or regression pass.
- Not published-artifact or secured/three-broker evidence.
- Not a universal Kafka performance ranking and not `1.0.0` readiness.
