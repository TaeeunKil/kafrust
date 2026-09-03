# Current-source company workstation short Kafka smoke (2026-09-03)

- source_commit: `37c5baa78bcf28d963df69a8eea91027c47bf631`
- host: company Windows x64 workstation
- local runtime: WSL2 Ubuntu-T9, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- broker: Kafka 4.3.1 single-node KRaft
- broker image: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- isolated resources: container `kafrust-company-current-20260903`, host port
  `19092`, and topic prefix `kafrust-company-current-20260903`
- evidence level: Local deterministic diagnostic only

## Checks that passed

The current pushed head also passed the stable/Rust 1.81.0 matrix in
[CI run 33729541055](https://github.com/TaeeunKil/kafrust/actions/runs/33729541055).
Against the isolated broker, the following completed successfully:

- `cargo test -p kafrust --test broker_roundtrip -- --nocapture`: 13 tests
  passed. The API/metadata and group-coordinator cases connected to the
  broker; share-specific cases were intentionally skipped because no share
  topic or failover phase was configured.
- `producer_send` with normal and idempotent delivery.
- `producer_buffered` with normal and idempotent delivery, including fetch
  and key/value reconciliation for three records per run.
- `consumer_group_poll` with classic and KIP-848 (`consumer`) protocols,
  assignment, poll, offset commit, and clean leave.
- `admin_create_topic` create, describe, partition expansion, list, classic
  and incremental config mutation, and delete.

The diagnostic used only uniquely named resources and removed only its own
container at exit. It did not prune Docker, change existing containers,
networks, or volumes. The Windows host and WSL capacity guards were not
modified.

## Boundary

This is current-source broker connectivity and short behavior evidence for the
company x86_64 workstation. It does not close V1-03/V1-04/V1-05 live or
published exit criteria, the accepted-floor matrix, V1-21/V1-22 long campaigns,
V1-23 service canary, release-candidate checks, or `0.3.7`/`1.0.0` release
authorization.
