# Company workstation broker roundtrip recheck (2026-09-03)

## Scope

Source commit `3dc0d9ca2ed97359d4297267a117fd32d52da998` was exercised from
the company WSL2 Ubuntu-T9 environment (`x86_64`, Rust 1.81.0) against an
isolated Kafka 4.3.1 single-node KRaft broker. This recheck runs the complete
`broker_roundtrip` integration target after the strict data-plane response
boundary and client regression changes.

The mounted WSL checkout has pre-existing CRLF-only working-tree differences.
No reset, cleanup, Docker prune, or mutation of unrelated containers,
networks, volumes, or images was performed.

## Procedure and result

Only the uniquely named container `kafrust-company-roundtrip-20260903` and
host port `19092` were used. The broker image was
`apache/kafka:4.3.1`, digest
`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
The container was removed by the script exit cleanup.

```text
KAFRUST_BOOTSTRAP_SERVERS=localhost:19092
KAFRUST_EXPECTED_BROKERS=1
KAFRUST_EXPECT_LIST_GROUPS_VERSION=5
KAFRUST_DATA_PLANE_TOPIC=kafrust-roundtrip-company-20260903
cargo test -p kafrust --test broker_roundtrip -- --nocapture

data_plane_version_log produce_advertised_max=13 produce_high_level_without_topic_id=12 produce_high_level_with_topic_id=13 fetch_advertised_max=18 fetch_high_level=13 metadata_v12=12 list_offsets_v1=1 offset_for_leader_epoch_v3=3 api_versions_v3=3
data_plane_roundtrip_log topic=kafrust-roundtrip-company-20260903 list_offsets_error=0 list_offsets_offset=0 offset_for_leader_epoch_error=0 end_offset=-1
data_plane_high_level_log produce_version=13 fetch_version=13 produced_offset=0 fetched_records=1
test result: ok. 13 passed; 0 failed
```

The Share-specific cases were intentionally skipped because no Share topic or
failover phase was configured in this single-node run. The test target's
non-Share broker checks and data-plane roundtrip completed successfully.

## Boundary

This is short local current-source diagnostic evidence. It does not satisfy
the accepted-floor security/workload matrix, three-broker leader movement,
published-artifact qualification, long campaigns, service-canary readiness,
or release authorization.
