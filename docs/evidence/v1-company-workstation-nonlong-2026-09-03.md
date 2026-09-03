# Company workstation non-long validation

- date_utc: 2026-09-03
- source_commit: `951f0ead4db414007ffe1b1ad28755f00be3c711`
- host: company Windows x64 workstation
- local runtime: WSL2 Ubuntu-T9, Linux `x86_64`, Rust 1.81.0; Windows stable Rust 1.97.1 for the optional `otlp` example
- Docker: 29.5.3, root `/var/lib/docker`, approximately 858 GiB free at start
- broker images: Kafka 4.3.1 `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`; Kafka 3.7.2 `sha256:8bd63e1bd445e5e19427a4bdbcc3d23bf6efd774b058a41b36ba87fda7623e34`
- evidence level: local deterministic diagnostic only

The exact-head CI for `951f0ead` also passed on Rust stable and Rust 1.81.0 in
[run 33701142154](https://github.com/TaeeunKil/kafrust/actions/runs/33701142154).

This record covers short, isolated broker checks that are possible on the
company workstation. It is deliberately not a V1-21 fault soak, V1-22 SLO
campaign, V1-23 service canary, or release qualification. Existing Docker
containers, networks, and named/anonymous volumes were inventoried before and
after; only explicitly named `kafrust-company-*` containers and their dangling
anonymous volumes were removed. No Docker prune or existing-resource mutation
was used.

## Checks that passed

The following were run against an isolated Kafka 4.3.1 single-node KRaft
broker, with a one- or three-partition topic chosen to match each example's
preconditions:

- `cargo test -p kafrust --test broker_roundtrip -- --nocapture`: 13 passed.
- `admin_create_topic`: create, metadata read, partition expansion, topic
  listing, classic and incremental config mutation, and delete passed.
- `producer_send`: normal and idempotent delivery passed.
- `producer_send_batch`: normal batch plus gzip, snappy, lz4, and zstd
  compression passed.
- `producer_buffered`: normal and idempotent buffered delivery/fetch passed.
- `producer_transactional` on a one-partition topic: committed and aborted
  records, buffered transactions, read-uncommitted/read-committed isolation,
  and transactional group offsets passed. A first attempt on a three-partition
  topic was discarded because the example intentionally reads all transaction
  records from one partition.
- `consumer_fetch` and `consumer_partition_queue`: fetched and queued records
  with matching offsets and values.
- `consumer_group_poll`: classic and KIP-848 consumer protocols joined,
  assigned all three partitions, committed offsets, and left cleanly.
- `consumer_group_regex`: classic and KIP-848 regex assignment passed.
- `consumer_group_auto_commit` and `consumer_group_heartbeat_rejoin` passed.
- `consumer_group_offset_reset` and `consumer_retention_recovery` passed,
  including DeleteRecords watermark recovery.
- `admin_describe_producers`, `admin_describe_transactions`,
  `admin_list_transactions`, `admin_describe_group`,
  `admin_consumer_group_offsets`, and `admin_delete_group_offsets` passed.
- `streams_group_smoke` and `streams_group_multi_member` passed against an
  isolated Kafka 4.3.1 broker configured for the Streams protocol.
- ShareConsumer roundtrip, ShareGroup offset mutation, and ShareGroup state
  lifecycle passed against Kafka 4.3.1 with the workflow's share coordinator
  settings. The heartbeat failover variant was not claimed because it requires
  a broker failure and multi-broker topology.
- `admin_describe_quorum` and `admin_describe_topic_partitions` passed.
- The telemetry plugin image built from
  `scripts/kafka-client-telemetry-plugin/Dockerfile`, emitted
  `KAFRUST_TELEMETRY_PLUGIN_READY`, and the Windows stable `otlp` example
  produced four OTLP pushes plus a terminating push. Broker logs retained both
  `terminating=false` and `terminating=true` non-zero payload markers.
- The same 13-test broker roundtrip suite was also run from WSL against the
  Kafka 3.7.2 image with all tests passing (share-specific variables unset, so
  share tests intentionally skipped).

## Infrastructure notes

WSL's generated resolver initially timed out for Docker Hub and crates.io even
though the Windows host network was healthy. A root-only temporary resolver
override using the workstation DNS was used for image/dependency downloads and
restored on exit; `/etc/resolv.conf` ended with its original generated content.
The WSL Rust 1.81 toolchain cannot build the optional telemetry dependency set
because the resolved Windows-only `security-framework 3.7.0` manifest requires
Cargo's edition2024 support. Windows stable Rust 1.97 was used for that optional
example; this does not alter the repository's MSRV policy or CI result.

## Remaining boundaries

- No six-hour/24-hour fault campaign or five-repetition eight-hour SLO campaign
  was started.
- No named production-like migration service or approved canary exists.
- These results do not close V1-03 through V1-18, V1-20, V1-21, V1-22, or
  V1-23, and do not justify `0.3.7`, `1.0.0`, a tag, or crates.io publication.
- No qualification-ledger row was added; this is retained local diagnostic
  evidence only.
