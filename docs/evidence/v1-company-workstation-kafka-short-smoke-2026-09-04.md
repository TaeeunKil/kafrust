# Current-source company workstation short Kafka smoke (2026-09-04)

- source_commit: `0ce95cabb5add692ab9b7e1465dfb6555c54d7ae`
- host: company Windows x64 workstation
- local runtime: WSL2 Ubuntu-T9, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- broker: Kafka 4.3.1 single-node KRaft
- broker image: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- isolated resources: container `kafrust-company-current-20260904`, host port
  `19092`, and topic prefix `kafrust-company-current-20260904`
- run window: 2026-09-03 UTC (2026-09-04 KST), under four minutes
- evidence level: Local deterministic diagnostic only

## Checks that passed

The source commit is the pushed `main` head and its stable/Rust 1.81.0 CI
matrix is green in [CI run 33795160602](https://github.com/TaeeunKil/kafrust/actions/runs/33795160602).
Against the isolated broker, the following completed successfully:

- `cargo test -p kafrust --test broker_roundtrip -- --nocapture
  --test-threads=1`: 13 tests passed. The data-plane case negotiated Kafka
  4.3.1 Produce 13 (topic-ID path), Fetch 13, Metadata 12, ListOffsets 1,
  OffsetForLeaderEpoch 3, and ApiVersions 3, then completed one Produce/Fetch
  roundtrip. Share-specific cases were intentionally skipped because no Share
  topic or failover phase was configured.
- `producer_send`: one record produced at partition 0, offset 0.
- `producer_buffered`: three records were produced and fetched back with
  matching keys and values.
- `consumer_group_poll`: classic group assignment, poll, four-record offset
  commit, and clean leave completed.
- `admin_create_topic`: create, describe, partition expansion, list, classic
  and incremental config mutation, and delete completed.

The run used only uniquely named resources and removed only its own container
at exit. It did not prune Docker or alter existing containers, networks,
volumes, host capacity guards, or DNS settings.

## Boundary

This is current-source broker connectivity and short behavior evidence for the
company x86_64 workstation. It does not close V1-03/V1-04/V1-05 live or
published exit criteria, the accepted-floor matrix, Share qualification,
three-broker movement, V1-21/V1-22 long campaigns, V1-23 service canary,
release-candidate checks, or `0.3.7`/`1.0.0` release authorization.
