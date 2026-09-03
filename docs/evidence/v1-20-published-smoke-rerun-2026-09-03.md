# V1-20 Published Smoke Rerun (2026-09-03)

Run [33714006944](https://github.com/TaeeunKil/kafrust/actions/runs/33714006944)
executed the exact registry pair `kafrust 0.3.6` / `kafrust-protocol 0.3.6`
from fresh external Cargo projects. The workflow head was
`bc0b40e9edc94d4a9b4921b6a4422b7d53823802` (`bc0b40e`), which changes only
published-smoke startup readiness; the published artifact bytes remain the
immutable pair documented in
[`v1-20-published-0.3.6-boundary-2026-08-23.md`](v1-20-published-0.3.6-boundary-2026-08-23.md).

All twelve matrix jobs passed and uploaded one external `Cargo.lock` plus one
captured fixture output each (12 artifacts, 90-day retention):

- Kafka 3.7.2 classic, gzip, snappy, lz4, and zstd
- Kafka 3.7.2 SASL_PLAINTEXT/PLAIN
- Kafka 3.7.2 SASL_SSL/SCRAM-SHA-256 and SCRAM-SHA-512
- Kafka 3.8.1 classic
- Kafka 3.9.1 classic
- Kafka 4.0.0 classic
- Kafka 4.3.1 KIP-848 consumer

The workflow now waits for a real group-coordinator response by describing a
fresh, nonexistent group and accepting the terminal `GroupIdNotFound` response
as proof that the coordinator is serving requests. This was verified locally
against `apache/kafka:4.0.0`; the earlier runs
[33713326912](https://github.com/TaeeunKil/kafrust/actions/runs/33713326912)
and [33713589954](https://github.com/TaeeunKil/kafrust/actions/runs/33713589954)
remain retained startup-race diagnostics and are not counted as product
failures.

This is a named published-artifact smoke refresh only. It does not close the
full V1-20 accepted matrix, mechanism-specific security rows, repeated fault
campaigns, V1-21/V1-22 long-duration or SLO gates, the V1-23 service canary,
API freeze, RC, or `1.0.0` release readiness.
