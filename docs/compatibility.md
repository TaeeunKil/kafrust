# Compatibility

kafrust compatibility claims are scoped to behavior that has been verified against a real broker. Protocol types can exist before the high-level client path has been validated against every broker version or deployment mode.

## Current Compatibility Claim

The `0.2.x` alpha line is verified against Apache Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 KRaft brokers over plaintext TCP in the single-node profile. Kafka 3.7.2 is also verified in a three-broker plaintext profile. TLS, SASL/PLAIN over SASL_PLAINTEXT, and SASL/SCRAM-SHA-256 over SASL_SSL are verified against Kafka 3.7.2 for single-node broker roundtrip, producer, direct consumer, and consumer group smoke paths. SASL/SCRAM-SHA-512 client exchanges are implemented, but the live broker profile is not claimed yet.

| Broker | Mode | Security | Verification | Status |
| --- | --- | --- | --- | --- |
| Apache Kafka 3.7.2 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, manual run `29995762812` on 2026-07-23 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | PLAINTEXT | `Live Kafka Smoke` multi-broker job, manual run `29995762812` on 2026-07-23 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | TLS | `Live Kafka Smoke` TLS job, manual run `29995762812` on 2026-07-23 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_PLAINTEXT with SASL/PLAIN | `Live Kafka Smoke` SASL_PLAINTEXT job, manual run `29995762812` on 2026-07-23 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_SSL with SCRAM-SHA-256 | `Live Kafka Smoke` SASL_SSL SCRAM job, manual run `29995762812` on 2026-07-23 | Passing |
| Apache Kafka 3.8.1 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, manual run `30062587935` on 2026-07-24 | Passing |
| Apache Kafka 3.9.1 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, manual run `30062587935` on 2026-07-24 | Passing |
| Apache Kafka 4.3.1 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, manual run `29995762812` on 2026-07-23 | Passing |

## Verified Paths

The Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext smoke paths cover:

- `ApiVersions v0` and `Metadata v1` roundtrips.
- `FindCoordinator v1` for consumer group coordinator discovery.
- Controller-routed CreateTopics v2 followed by Metadata v1 description.
  Manual run `30059517473` passed this path against Kafka 3.7.2 and Kafka
  4.3.1.
- Admin cluster/topic inspection, CreateTopics v2, bounded metadata propagation,
  DescribeConfigs v1, and DeleteTopics v3. Manual run `30060723690` passed this
  lifecycle against Kafka 3.7.2 and Kafka 4.3.1.
- IncrementalAlterConfigs v0 followed by DescribeConfigs v1 verification.
  Manual run `30061073263` passed this update-and-readback path against Kafka
  3.7.2 and Kafka 4.3.1.
- Coordinator-routed DescribeGroups v1. Manual run `30061497355` passed this
  path against Kafka 3.7.2 and Kafka 4.3.1 plaintext brokers plus the Kafka
  3.7.2 TLS, SASL_PLAINTEXT, and SASL_SSL profiles.
- Coordinator-routed OffsetDelete v0 with separate group-level and
  partition-level outcomes is covered by byte-level and injected-broker tests.
  Manual run `30062203069` passed offset deletion after group session expiry
  on Kafka 3.7.2 and 4.3.1 plaintext brokers, TLS, SASL_PLAINTEXT, SASL_SSL,
  and the three-broker profile. The three-broker job then passed its existing
  broker-stop failover sequence.
- Manual run `30062587935` passed the complete plaintext path on Kafka 3.8.1
  and 3.9.1, including all four compression codecs, idempotent and
  transactional production, direct and group consumption, topic/config admin,
  group description, and offset deletion.
- High-level producer metadata lookup, leader routing, negotiated Produce API selection, single-record send, batch send, gzip-, Snappy-, LZ4-, and Zstd-compressed batch send, and buffered send with `acks=1`. Against Kafka 3.7.2, the current path selects Produce v3 RecordBatch for Gzip, Snappy, and LZ4, and Produce v7 for Zstd.
- Opt-in idempotent single-record, batch, and buffered produce using
  `InitProducerId v0`, `acks=all`, and partition-scoped RecordBatch producer
  identity and sequence metadata. Manual run `29991254722` passed these paths
  against Kafka 3.7.2 and Kafka 4.3.1.
- Opt-in alpha transactional produce using transaction coordinator discovery,
  transactional `InitProducerId v0`, `AddPartitionsToTxn v0`, Produce v3/v7,
  and `EndTxn v0`. Manual run `29994041530` passed a committed transaction
  followed by an aborted transaction against Kafka 3.7.2 and Kafka 4.3.1.
- Direct and group consumer `ReadCommitted` isolation through Fetch v4.
  Transactional/control RecordBatch metadata is preserved for filtering,
  control records are hidden, and aborted transaction records are excluded.
  Manual run `29995122439` compared `ReadUncommitted` and `ReadCommitted`
  results after real commit and abort flows on Kafka 3.7.2 and Kafka 4.3.1.
- Transactional consumer group offset integration through
  `Producer::send_group_offsets_to_transaction`, `AddOffsetsToTxn v0`, and
  generation-fenced `TxnOffsetCommit v3`. Manual run `30063099869` passed a
  read-committed group poll followed by atomic output production and the
  generation-fenced group offset commit on Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 plaintext brokers plus the Kafka 3.7.2 TLS, SASL_PLAINTEXT, SASL_SSL,
  and three-broker profiles.
- Gzip Produce v3 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused tests and the plaintext live smoke profile.
- Snappy Produce v3 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused tests using Kafka-compatible Xerial framing and by the
  plaintext single-node and multi-broker live smoke profiles.
- LZ4 Produce v3 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused standard-frame and decompression-limit tests and by the
  plaintext single-node and multi-broker live smoke profiles.
- Zstd Produce v7 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused standard-frame, declared-window, and decompression-limit
  tests and by the plaintext single-node and multi-broker live smoke profiles.
- Direct consumer fetch from an assigned topic partition using Fetch v4 response decoding. The v4 path is required because Kafka 4.x no longer accepts Fetch v2.
- Consumer group join, sync, heartbeat, poll, and offset commit through the alpha classic consumer group path with range assignment.

The Kafka 3.7.2 multi-broker plaintext smoke path covers:

- A three-broker KRaft cluster with comma-separated bootstrap servers and a replicated smoke topic.
- Metadata roundtrip with at least three brokers visible to kafrust.
- Controller discovery, CreateTopics v2, and follow-up Metadata v1 description
  through three externally advertised broker addresses. Manual run
  `30059517473` passed this path.
- The complete admin lifecycle, including all-topic listing, DescribeConfigs
  v1, bounded metadata propagation, and DeleteTopics v3. Manual run
  `30060723690` passed before the existing broker-stop failover checks.
- IncrementalAlterConfigs v0 update and readback also passed in manual run
  `30061073263` before the three-broker failover sequence.
- The `broker_roundtrip` example against multi-broker advertised listener metadata.
- High-level producer single-record send with explicit partition routing,
  buffered send, batch send with explicit partition routing, and gzip-,
  Snappy-, LZ4-, and Zstd-compressed batch send with explicit partition routing
  across the replicated smoke topic.
- Long-lived producer metadata refresh after stopping the broker that leads the
  selected partition between two sends from the same producer instance.
- Direct consumer fetch from an assigned topic partition.
- Long-lived direct consumer metadata refresh after stopping the broker that
  leads the selected partition between two fetches from the same consumer
  instance.
- Consumer group join, sync, heartbeat, poll, and offset commit through the alpha classic consumer group path.
- First configured bootstrap broker stop followed by batch producer, direct consumer, and consumer group checks through the remaining brokers.

The Kafka 3.7.2 TLS smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips through `SecurityProtocol::Tls`.
- `FindCoordinator v1` for consumer group coordinator discovery through `SecurityProtocol::Tls`.
- The `broker_roundtrip` example through `SecurityProtocol::Tls`.
- High-level producer metadata lookup, leader routing, single-record send, batch send, and buffered send through `SecurityProtocol::Tls`.
- Direct consumer fetch from an assigned topic partition through `SecurityProtocol::Tls`.
- Consumer group join, sync, heartbeat, poll, and offset commit through `SecurityProtocol::Tls`.

The Kafka 3.7.2 SASL_PLAINTEXT smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips through `SecurityProtocol::SaslPlaintext` using SASL/PLAIN.
- `FindCoordinator v1` for consumer group coordinator discovery through `SecurityProtocol::SaslPlaintext`.
- The `broker_roundtrip` example through `SecurityProtocol::SaslPlaintext`.
- High-level producer metadata lookup, leader routing, single-record send, batch send, and buffered send through `SecurityProtocol::SaslPlaintext`.
- Direct consumer fetch from an assigned topic partition through `SecurityProtocol::SaslPlaintext`.
- Consumer group join, sync, heartbeat, poll, and offset commit through `SecurityProtocol::SaslPlaintext`.

The Kafka 3.7.2 SASL_SSL SCRAM smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips through `SecurityProtocol::SaslTls` using SASL/SCRAM-SHA-256.
- `FindCoordinator v1` for consumer group coordinator discovery through `SecurityProtocol::SaslTls`.
- The `broker_roundtrip` example through `SecurityProtocol::SaslTls`.
- High-level producer metadata lookup, leader routing, single-record send, batch send, and buffered send through `SecurityProtocol::SaslTls`.
- Direct consumer fetch from an assigned topic partition through `SecurityProtocol::SaslTls`.
- Consumer group join, sync, heartbeat, poll, and offset commit through `SecurityProtocol::SaslTls`.
- TLS certificate validation with an extra DER root certificate configured through `tls_root_certificate_der`.

## Not Yet Claimed

The current compatibility claim does not cover:

- TLS workflows beyond the listed TLS smoke examples.
- SASL workflows beyond the listed SASL_PLAINTEXT and SASL_SSL smoke examples.
- SASL/SCRAM-SHA-512 live broker profiles.
- Secured multi-broker clusters, broader consumer-group failover beyond the listed coordinator reconnect checks, rack awareness, or partition expansion.
- Transactional buffered production, multi-broker transaction failover, and
  transaction failure-injection profiles.
- Live broker idempotence failure-injection profiles. The ambiguous-response
  duplicate path is covered by a deterministic injected broker test.
- Kafka APIs that are not listed in the verified paths.

## Updating Compatibility

When a new broker version or deployment mode is verified:

1. Run the `Live Kafka Smoke` workflow manually against the target branch, or add a focused workflow job for that broker profile.
2. Record the broker version, deployment mode, security mode, verification command or workflow, date, and result in this document.
3. Update `docs/roadmap.md` if the result changes a milestone status or known limit.
4. If a failure is found, open the issue with the closest template:
   - protocol bug for encoding, decoding, API keys, versions, and Kafka error-code handling
   - client runtime bug for connection, timeout, retry, metadata, producer, consumer, or group behavior
   - API design question for public API naming, builders, defaults, and Kafka concept exposure
