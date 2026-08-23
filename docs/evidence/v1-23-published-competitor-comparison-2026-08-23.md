# V1-23 Published Competitor Comparison (2026-08-23)

Status: comparison evidence; not a parity or production-SLO qualification.

Run: [32619987006](https://github.com/TaeeunKil/kafrust/actions/runs/32619987006)

The workflow resolved `kafrust 0.3.6` from crates.io in a fresh Rust project and
compared it with `rust-rdkafka 0.39.0` on Kafka 4.3.1. Both implementations
used isolated topics, 20,000 unique 1-KiB records, batches of 200, and three
independent repetitions. Every row reported `unique_records=20,000`,
`duplicate_count=0`, `loss_count=0`, and the same payload SHA-256 digest
`7384f0e0012fab42060df529e06bdc32a348caff3dcf143281fed226af91cffa`.

## Throughput result

| Implementation | Median Produce records/s | Median Consume records/s |
| --- | ---: | ---: |
| kafrust 0.3.6 | 76,625.07 | 272,659.38 |
| rust-rdkafka 0.39.0 | 153,296.60 | 590,466.70 |

Under this workload and hosted runner, kafrust measured approximately 50.0% of
rust-rdkafka Produce throughput and 46.2% of Consume throughput. The result is
material enough to keep V1-22 performance/SLO work and target-workload
profiling open before any stable-release decision. It is not a universal
benchmark ranking: the runner, broker, payload, batching, concurrency,
compression, and client configuration are all workload-specific, and the
comparison workflow is informational rather than a pass/fail parity gate.

The artifact also confirms the migration comparator's content reconciliation
path for the published 0.3.6 boundary. It does not establish failure,
security, transaction, group, memory, or service-canary parity.
