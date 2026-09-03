# V1-23 Reference Migration Smoke (2026-09-03)

The current pushed source `12fe52a6b1e3a6007a3fed49a1091680893a5623`
(`12fe52a`) passed the reproducible reference migration smoke in
[Migration Reference Canary run 33717760410](https://github.com/TaeeunKil/kafrust/actions/runs/33717760410).
The workflow used Kafka 4.3.1 KRaft, isolated per-implementation topics, and
the stable Rust toolchain. The comparison-only fixture built the published
`rust-rdkafka` reference alongside the kafrust source checkout; this does not
introduce a C dependency into kafrust.

Both implementations processed 1,000 unique business IDs with 1-KiB payloads
and batch size 100:

| implementation | produce seconds | produce records/s | consume seconds | consume records/s | unique | loss | duplicates |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| kafrust | 0.052061 | 19,208.07 | 0.051984 | 19,236.74 | 1,000 | 0 | 0 |
| rust-rdkafka | 0.042912 | 23,303.75 | 0.047370 | 21,110.20 | 1,000 | 0 | 0 |

The normalized payload SHA-256 was identical for both implementations:
`98fb6a3dfe9a9ac1765160a42e05b2c63e0ed231af678f370de194c0f5044e26`.
The uploaded raw results are `migration-kafrust.json` and
`migration-rdkafka.json` in the run artifact `kafrust-migration-canary-33717760410`.

This refreshes the reproducible reference smoke from the current source. It is
not a named-service canary, forward cutover, fault/credential-rotation test,
rollback result, million-record qualification, V1-22 SLO evidence, or release
authorization. V1-23 remains blocked until a service owner and approved
production-like canary environment are registered.
