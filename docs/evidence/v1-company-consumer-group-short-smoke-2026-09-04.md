# Company workstation consumer-group short smoke (2026-09-04)

## Scope

- source commit: `108b4329fd022890ecfa0155c62bfcc28a1f1f2f`
- host: company Windows x64 workstation
- runtime: Ubuntu-T9 WSL2, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- broker: Kafka 4.3.1 single-node KRaft
- image digest: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- isolated container: `kafrust-company-consumer-20260904`
- isolated topics: `kafrust-company-consumer-20260904` and
  `kafrust-company-consumer-modern-20260904`
- isolated groups: `kafrust-company-consumer-group-20260904` and
  `kafrust-company-consumer-modern-group-20260904`

## Checks

Each topic was created and populated with the pushed `producer_send` example
before starting its consumer. The classic `consumer_group_poll` example then
joined one assignment, fetched record offset `0`, committed one polled record,
and left cleanly. A second run with
`KAFRUST_GROUP_PROTOCOL=consumer` exercised the KIP-848 path and observed the
same join, fetch, commit, and leave sequence.

The consumer example's `poll()` is a single bounded fetch; it does not wait for
future records. A controlled join-then-produce ordering therefore returned zero
records before the producer ran, which is expected API behavior rather than a
broker or client failure. The evidence run uses the documented pre-populated
topic ordering so the record/commit path is observable.

The uniquely named container and topics were removed on exit. No existing
company Docker container, network, volume, or image was pruned or modified.

## Boundary

This is local deterministic/current-source single-node diagnostic evidence for
the classic and KIP-848 group examples. It does not close published artifact
qualification, secure or accepted-floor churn, partition-leader/coordinator
failover, Share qualification, long campaigns, migration canary, or release
authorization. Partition-queue mode was not part of this run.
