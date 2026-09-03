# Company workstation partition-queue short smoke (2026-09-04)

## Scope

- source commit: `c68088526cae753edde00310df1697ef0f40eedf`
- host: company Windows x64 workstation
- runtime: Ubuntu-T9 WSL2, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- broker: Kafka 4.3.1 single-node KRaft
- image digest: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- isolated container: `kafrust-company-partition-queue-20260904`
- isolated topic: `kafrust-company-partition-queue-20260904`

## Checks

The pushed `producer_send` example produced one record at partition 0,
offset 0. The `consumer_partition_queue` example assigned that partition,
split its queue, polled once, and drained exactly one queued record with the
expected key `kafrust-key` and value `hello from kafrust`. It reported
`partition queue delivered 1 records` and exited successfully.

The uniquely named container and topic were removed on exit. No existing
company Docker container, network, volume, or image was pruned or modified.

## Boundary

This is local deterministic/current-source single-node diagnostic evidence for
one partition-queue delivery. It does not close published artifact
qualification, accepted-floor/security coverage, backpressure saturation,
leader movement, long campaigns, service canary, or release authorization.
