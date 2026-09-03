# Company workstation pushed-source short Kafka smoke (2026-09-04)

## Scope

- source commit: `0674907732524dc115088aab22f79daf5a42d624`
- host: company Windows x64 workstation
- runtime: Ubuntu-T9 WSL2, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- broker: Kafka 4.3.1 single-node KRaft
- image digest: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- isolated container: `kafrust-company-current-20260904-rerun2`
- isolated topics: `kafrust-company-current-20260904-rerun2` and
  `kafrust-company-data-20260904-rerun2`

## Checks

The broker-roundtrip target passed all 13 tests. The data-plane case selected
Produce v13 with topic IDs, Fetch v13, Metadata v12, ListOffsets v1,
OffsetForLeaderEpoch v3, and ApiVersions v3, then completed a one-record
Produce/Fetch roundtrip.

The pushed-source examples also passed:

- idempotent immediate producer: one record produced at partition 1, offset 0;
- idempotent buffered producer: three records produced and fetched with matching
  keys and values;
- Admin topic lifecycle: create, describe, partition expansion, list, classic
  and incremental config mutation, and delete.

The smoke container and its uniquely named topics were removed by the test
script on exit. No existing company container, network, volume, or image was
pruned or modified. The classic group example was not included in this rerun;
its earlier successful short evidence remains separate.

## Boundary

This is local deterministic/current-source broker evidence only. It does not
close published artifact qualification, accepted-floor security coverage,
three-broker movement, Share qualification, V1-21/V1-22 long campaigns,
V1-23 service canary, or release authorization.
