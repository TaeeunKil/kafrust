# Compatibility

kafrust compatibility claims are scoped to behavior that has been verified against a real broker. Protocol types can exist before the high-level client path has been validated against every broker version or deployment mode.

## Current Compatibility Claim

The `0.2.x` alpha line is verified against Apache Kafka 3.7.2 KRaft brokers over plaintext TCP in both single-node and three-broker profiles. TLS, SASL/PLAIN over SASL_PLAINTEXT, and SASL/SCRAM-SHA-256 over SASL_SSL are verified for single-node broker roundtrip, producer, direct consumer, and consumer group smoke paths. SASL/SCRAM-SHA-512 client exchanges are implemented, but the live broker profile is not claimed yet.

| Broker | Mode | Security | Verification | Status |
| --- | --- | --- | --- | --- |
| Apache Kafka 3.7.2 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, latest manual smoke run `29986018854` on 2026-07-23 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | PLAINTEXT | `Live Kafka Smoke` multi-broker job, latest manual smoke run `29986018854` on 2026-07-23 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | TLS | `Live Kafka Smoke` TLS job, latest manual smoke run `29986018854` on 2026-07-23 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_PLAINTEXT with SASL/PLAIN | `Live Kafka Smoke` SASL_PLAINTEXT job, latest manual smoke run `29986018854` on 2026-07-23 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_SSL with SCRAM-SHA-256 | `Live Kafka Smoke` SASL_SSL SCRAM job, latest manual smoke run `29986018854` on 2026-07-23 | Passing |

## Verified Paths

The Kafka 3.7.2 plaintext smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips.
- `FindCoordinator v1` for consumer group coordinator discovery.
- High-level producer metadata lookup, leader routing, negotiated Produce API selection, single-record send, batch send, gzip-, Snappy-, and LZ4-compressed batch send, and buffered send with `acks=1`. Against Kafka 3.7.2, the current path selects Produce v3 RecordBatch.
- Gzip Produce v3 RecordBatch encoding and Fetch v2 RecordBatch decoding are
  covered by focused tests and the plaintext live smoke profile.
- Snappy Produce v3 RecordBatch encoding and Fetch v2 RecordBatch decoding are
  covered by focused tests using Kafka-compatible Xerial framing and by the
  plaintext single-node and multi-broker live smoke profiles.
- LZ4 Produce v3 RecordBatch encoding and Fetch v2 RecordBatch decoding are
  covered by focused standard-frame and decompression-limit tests and by the
  plaintext single-node and multi-broker live smoke profiles.
- Zstd Produce v3 RecordBatch encoding and Fetch v2 RecordBatch decoding are
  covered by focused standard-frame, declared-window, and decompression-limit
  tests. Live broker verification is pending.
- Direct consumer fetch from an assigned topic partition using Fetch v2 response decoding.
- Consumer group join, sync, heartbeat, poll, and offset commit through the alpha classic consumer group path with range assignment.

The Kafka 3.7.2 multi-broker plaintext smoke path covers:

- A three-broker KRaft cluster with comma-separated bootstrap servers and a replicated smoke topic.
- Metadata roundtrip with at least three brokers visible to kafrust.
- The `broker_roundtrip` example against multi-broker advertised listener metadata.
- High-level producer single-record send with explicit partition routing, buffered send, batch send with explicit partition routing, and gzip-, Snappy-, and LZ4-compressed batch send with explicit partition routing across the replicated smoke topic.
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
- Zstd live broker verification, secured compression profiles, idempotent
  producers, transactions, or high-throughput batching.
- A full Kafka broker version matrix.
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
