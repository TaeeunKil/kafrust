# Company workstation data-plane version log (2026-09-03)

- source_commit: `411008987edc687f8487f910514594c8f272e412`
- host: company Windows x64 workstation
- local runtime: WSL2 Ubuntu-T9, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- evidence level: Local deterministic diagnostic only

## Procedure

The opt-in `api_versions_and_metadata_roundtrip_when_broker_is_configured`
test was run from the Windows Rust toolchain against two uniquely named,
single-node KRaft containers. `KAFRUST_DATA_PLANE_TOPIC` enabled the bounded
probe path. The test created a one-partition topic through `AdminClient`,
waited for leader-ready metadata, sent ListOffsets v1 and
OffsetForLeaderEpoch v3 requests, sent one high-level producer record, fetched
that exact offset through the direct consumer, and deleted the topic. No
existing Docker resource was pruned or changed.

Commands (the environment values differ only by broker profile):

```text
KAFRUST_BOOTSTRAP_SERVERS=localhost:19092
KAFRUST_EXPECTED_BROKERS=1
KAFRUST_EXPECT_LIST_GROUPS_VERSION=5 (4 on Kafka 3.7.2)
KAFRUST_DATA_PLANE_TOPIC=<unique probe topic>
cargo test -p kafrust --test broker_roundtrip \
  api_versions_and_metadata_roundtrip_when_broker_is_configured -- --nocapture
```

## Observed logs

| Broker image | Produce advertised max | High-level Produce (no topic ID / topic ID) | Fetch advertised max | High-level Fetch | Metadata | ListOffsets | OffsetForLeaderEpoch | ApiVersions | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (4.3.1) | 13 | 12 / 13 | 18 | 13 | 12 | 1 | 3 | 3 | passed; one record fetched |
| `apache/kafka@sha256:8bd63e1bd445e5e19427a4bdbcc3d23bf6efd774b058a41b36ba87fda7623e34` (3.7.2) | 10 | 9 / 9 | 16 | 13 | 12 | 1 | 3 | 3 | passed; one record fetched |

The exact test output included:

```text
data_plane_roundtrip_log ... list_offsets_error=0 ... offset_for_leader_epoch_error=0 ...
data_plane_high_level_log ... produced_offset=0 fetched_records=1
```

The output is a live capability/selection record for these two local
single-node profiles. Produce v13 is selected only when the topic-ID path is
available; the floor profile correctly falls back to Produce v9. The test
does not claim three-broker movement, security compatibility, published
artifact compatibility, long-campaign SLOs, service-canary readiness, or
release authorization.

## Repository validation

The source commit passed `cargo fmt --all`,
`cargo check --workspace --all-targets`,
`cargo test --workspace --all-features`,
`cargo clippy --workspace --all-targets --all-features -- -D warnings`,
`cargo doc --workspace --all-features --no-deps`, and `git diff --check`.
The exact-head CI result is recorded at
<https://github.com/TaeeunKil/kafrust/actions/runs/33739185161>.

