# Hosted benchmark profile diagnostic rerun (2026-09-04)

## Scope

The pushed source commit `a8199d66b75cae90db4de33b3f7db629a6b0eacc` passed all
four profiles in
[Kafka Benchmark Profile Diagnostic run 33821968768](https://github.com/TaeeunKil/kafrust/actions/runs/33821968768).
The broker was Kafka 4.3.1 single-node KRaft with PLAINTEXT security. Each
profile used a 10-second warmup, 60-second measured window, ten-second samples,
1-KiB records, and no compression. Every descriptor is explicitly
`qualified: false` because this was a bounded profile diagnostic.

| Profile | Produced/consumed | Requests | p50/p95/p99 | RSS baseline → terminal | Artifact ID |
| --- | ---: | ---: | --- | ---: | ---: |
| immediate, 1 worker, batch 200 | 4,367,400 / 4,367,400 | 43,674 | 1 / 1 / 1 ms | 9,043,968 → 9,043,968 B | 9918632030 |
| immediate, 4 workers, batch 200 | 12,720,600 / 12,720,600 | 127,206 | 1 / 5 / 5 ms | 17,801,216 → 17,801,216 B | 9918636490 |
| buffered, 4 workers, batch 200 | 1,151,400 / 1,151,400 | 17,092 | 1 / 5 / 10 ms | 15,427,584 → 15,427,584 B | 9918629559 |
| direct-consumer, 1 worker | 146,630 / 146,630 | 293,260 | 1 / 1 / 1 ms | 6,135,808 → 6,135,808 B | 9918632837 |

All profiles reported zero failed requests, retries, unknown outcomes, loss,
and duplicates. Record-ID reconciliation was qualified in each descriptor with
matching expected and observed digests, and `in_flight_requests` and
`buffered_records` were zero at the end of every profile. The hosted runner did
not provide usable `perf stat` counters; `/usr/bin/time` resource artifacts
were retained instead.

## Boundary

These results are reproducible short source diagnostics for profile behavior,
reconciliation, and resource-gauge draining. They are not the six-profile,
five-repetition, two-hour-warmup/six-hour-measurement V1-22 campaign, a locked
baseline, a universal performance ranking, or release authorization. No
version bump or crates.io publication follows from this run.
