# Company workstation Share short smoke (2026-09-03)

- source_commit: `74ee4dc87cc13a2c7dd0b3d69a433aeba29d0fe4`
- host: company Windows x64 workstation
- local runtime: WSL2 Ubuntu-T9, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- broker: Kafka 4.3.1 single-node KRaft with Share coordinator enabled
- broker image: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- isolated resources: container `kafrust-company-share-20260903`, host port
  `19092`, and topic prefix `kafrust-company-share-20260903`
- evidence level: Local deterministic diagnostic only

## Checks that passed

The isolated broker was configured with the Share coordinator and `classic`,
`consumer`, and `share` rebalance protocols. A seeded Share topic was then
used for:

- `share_consumer_roundtrip_when_broker_is_configured`: passed in 35.44s.
- `share_group_offset_mutations_when_broker_is_configured`: passed in 2.44s.
- `share_group_state_lifecycle_when_broker_is_configured`: passed in 0.11s.

The diagnostic used only uniquely named resources and removed only its own
container at exit. It did not prune Docker or change existing containers,
networks, or volumes.

## Boundary

This is a short single-node Share diagnostic. It does not close V1-10, prove
the two-member/three-broker secure 10,000-record and 20-cycle gate, qualify
every acknowledgement-loss branch, or authorize a package release.
