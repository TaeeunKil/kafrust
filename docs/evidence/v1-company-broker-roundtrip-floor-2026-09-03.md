# Company workstation Kafka 3.7.2 floor roundtrip recheck (2026-09-03)

## Scope

Source commit `3dc0d9ca2ed97359d4297267a117fd32d52da998` was exercised from
WSL2 Ubuntu-T9 (`x86_64`, Rust 1.81.0) against an isolated Kafka 3.7.2
single-node KRaft broker. This is the planned floor-line counterpart to the
Kafka 4.3.1 company roundtrip recheck.

The mounted WSL checkout has pre-existing CRLF-only working-tree differences.
No reset, cleanup, Docker prune, or mutation of unrelated resources was used.

## Procedure and result

The uniquely named container `kafrust-company-roundtrip-floor-20260903` used
host port `19093` and was removed by exit cleanup. The broker image was
`apache/kafka:3.7.2`, digest
`sha256:8bd63e1bd445e5e19427a4bdbcc3d23bf6efd774b058a41b36ba87fda7623e34`.
Kafka 3.7.2 advertises ListGroups v4, so the run explicitly set
`KAFRUST_EXPECT_LIST_GROUPS_VERSION=4`.

```text
KAFRUST_BOOTSTRAP_SERVERS=localhost:19093
KAFRUST_EXPECTED_BROKERS=1
KAFRUST_EXPECT_LIST_GROUPS_VERSION=4
KAFRUST_DATA_PLANE_TOPIC=kafrust-roundtrip-floor-company-20260903
cargo test -p kafrust --test broker_roundtrip -- --nocapture

data_plane_version_log produce_advertised_max=10 produce_high_level_without_topic_id=9 produce_high_level_with_topic_id=9 fetch_advertised_max=16 fetch_high_level=13 metadata_v12=12 list_offsets_v1=1 offset_for_leader_epoch_v3=3 api_versions_v3=3
data_plane_roundtrip_log topic=kafrust-roundtrip-floor-company-20260903 list_offsets_error=0 list_offsets_offset=0 offset_for_leader_epoch_error=0 end_offset=-1
data_plane_high_level_log produce_version=9 fetch_version=13 produced_offset=0 fetched_records=1
test result: ok. 13 passed; 0 failed
```

An initial invocation used the Kafka 4.x ListGroups v5 expectation and failed
at that environment assertion (`advertised max 4`). It was rerun unchanged
apart from the corrected floor-line expectation and passed; no product code was
changed.

Share-specific phases were intentionally skipped because no Share topic or
failover phase was configured in this single-node run.

## Boundary

This is short local floor-line diagnostic evidence. It does not close the
accepted-floor security/workload matrix, three-broker movement, published
qualification, long campaigns, service canary, or release authorization.
