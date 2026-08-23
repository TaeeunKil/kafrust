# V1-22 Published Competitor Comparison (2026-08-23)

Run [32626740940](https://github.com/TaeeunKil/kafrust/actions/runs/32626740940)
compared the exact published `kafrust 0.3.6` pair with `rust-rdkafka 0.39.0`
from one fresh external Cargo project. Kafka 4.3.1 KRaft ran one isolated topic
per implementation and repetition. Both implementations processed five
independent repetitions of 20,000 unique 1-KiB records with batches of 200.

## Reconciled result

All ten implementation rows reported `unique_records=20,000`,
`duplicate_count=0`, `loss_count=0`, and the same payload SHA-256 digest
`7384f0e0012fab42060df529e06bdc32a348caff3dcf143281fed226af91cffa`.

| Implementation | Median Produce records/s | Median Consume records/s | Loss | Duplicates |
| --- | ---: | ---: | ---: | ---: |
| kafrust 0.3.6 | 77,157.32 | 286,135.19 | 0 | 0 |
| rust-rdkafka 0.39.0 | 171,065.45 | 501,449.83 | 0 | 0 |

The published client reached 45.1% of the comparison client's median Produce
throughput and 57.1% of its median Consume throughput for this workload. This
is contextual workload evidence, not a universal ranking or a release
threshold; the comparator did not measure latency, RSS, allocation, retry
ratios, or fault behavior.

## Decision

The five-repetition result confirms the earlier three-repetition signal and
keeps the V1-22 re-plan active: profile batching, request concurrency,
allocation, queue behavior, and operation latency on the target workload;
retain before/after evidence for any optimization; then rerun the full
six-profile, topology/security, five-repetition SLO campaign. No `0.3.7`, RC,
or `1.0.0` publication is justified by this comparison alone.

The raw `comparison-results.jsonl` and ten stderr logs are retained by the
workflow artifact `kafrust-rust-rdkafka-comparison-32626740940` for 90 days.
