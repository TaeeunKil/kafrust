# Company workstation data-plane response-boundary smoke — 2026-09-03

## Scope

Source commit `1b4cb5f952261325dd0c20d6348829d3dd7a8e4f` was exercised in the
company WSL2 Ubuntu-T9 environment (`x86_64`, Rust 1.81.0) against an isolated
Kafka 4.3.1 KRaft broker. The run checks that the selected data-plane response
decoders, including the strict trailing-byte boundary, still handle valid
broker responses and the high-level roundtrip.

The WSL checkout had pre-existing CRLF-only working-tree differences across
the mounted Windows files. No reset, cleanup, prune, or unrelated container,
network, volume, or image mutation was performed.

## Procedure and result

Only the explicitly named container `kafrust-company-dataplane-20260903` was
created on host port `19092` and removed after the test. The broker image was
`apache/kafka:4.3.1`, digest
`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.

```text
KAFRUST_BOOTSTRAP_SERVERS=localhost:19092
KAFRUST_EXPECTED_BROKERS=1
KAFRUST_EXPECT_LIST_GROUPS_VERSION=5
KAFRUST_DATA_PLANE_TOPIC=kafrust-dataplane-20260903-f982
cargo test -p kafrust --test broker_roundtrip \
  api_versions_and_metadata_roundtrip_when_broker_is_configured -- --nocapture

data_plane_version_log produce_advertised_max=13 produce_high_level_without_topic_id=12 produce_high_level_with_topic_id=13 fetch_advertised_max=18 fetch_high_level=13 metadata_v12=12 list_offsets_v1=1 offset_for_leader_epoch_v3=3 api_versions_v3=3
data_plane_roundtrip_log topic=kafrust-dataplane-20260903-f982 list_offsets_error=0 list_offsets_offset=0 offset_for_leader_epoch_error=0 end_offset=-1
data_plane_high_level_log produce_version=13 fetch_version=13 produced_offset=0 fetched_records=1
test result: ok. 1 passed; 0 failed
```

Post-run Docker inventory retained the pre-existing exited
`jacmel-local-fat-test-*` containers; the named Kafka container was absent.
Docker reported 18.15 GB of images and 110.5 MB of local volumes, with no
prune operation.

## Boundary

This is a short local source diagnostic. It does not satisfy the accepted-floor
security/workload matrix, three-broker leader movement, published
compatibility, long campaigns, service-canary readiness, or release gates.
