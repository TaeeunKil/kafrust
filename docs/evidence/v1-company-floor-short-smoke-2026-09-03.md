# Company workstation Kafka 3.7.2 floor short smoke (2026-09-03)

- source_commit: `4f81471bd56fecc47a37f23e95a6eb4a09872d13`
- host: company Windows x64 workstation
- local runtime: WSL2 Ubuntu-T9, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- broker: Kafka 3.7.2 single-node KRaft
- broker image: `apache/kafka@sha256:8bd63e1bd445e5e19427a4bdbcc3d23bf6efd774b058a41b36ba87fda7623e34`
- isolated resources: container `kafrust-company-floor-20260903`, host port
  `19092`, and topic prefix `kafrust-company-floor-20260903`
- evidence level: Local deterministic diagnostic only

## Checks that passed

Against the isolated Kafka 3.7.2 broker, the following completed successfully:

- `cargo test -p kafrust --test broker_roundtrip -- --nocapture`: 13 tests
  passed. API/metadata and group-coordinator cases connected to the broker;
  Share-specific cases were intentionally skipped because Kafka 3.7.2 and the
  run configuration did not provide a Share topic or failover phase.
- `producer_send` with normal and idempotent delivery.
- `producer_buffered` with normal and idempotent delivery, including fetch
  and key/value reconciliation for three records per run.
- `consumer_group_poll` with the classic protocol, assignment, poll, offset
  commit, and clean leave.
- `admin_create_topic` create, describe, partition expansion, list, classic
  and incremental config mutation, and delete.

The diagnostic used only uniquely named resources and removed only its own
container at exit. It did not prune Docker or change existing containers,
networks, or volumes.

## Boundary

This is a short floor-line connectivity diagnostic, not accepted-floor
qualification. It does not establish all required security/workload profiles,
three-broker behavior, published-artifact compatibility, V1-20 completion,
long campaigns, service canary readiness, or release authorization.
