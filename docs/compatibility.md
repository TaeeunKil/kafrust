# Compatibility

kafrust compatibility claims are scoped to behavior that has been verified against a real broker. Protocol types can exist before the high-level client path has been validated against every broker version or deployment mode.

## Current Compatibility Claim

The `0.2.x` alpha line is verified against a single-node Apache Kafka 3.7.2 KRaft broker over plaintext TCP. TLS and SASL/PLAIN over SASL_PLAINTEXT are verified for broker roundtrip, producer, direct consumer, and consumer group smoke paths.

| Broker | Mode | Security | Verification | Status |
| --- | --- | --- | --- | --- |
| Apache Kafka 3.7.2 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, latest manual smoke run `27399394544` on 2026-06-12 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | TLS | `Live Kafka Smoke` TLS job, latest manual smoke run `27399394544` on 2026-06-12 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_PLAINTEXT with SASL/PLAIN | `Live Kafka Smoke` SASL_PLAINTEXT job, latest manual smoke run `27399394544` on 2026-06-12 | Passing |

## Verified Paths

The Kafka 3.7.2 plaintext smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips.
- `FindCoordinator v1` for consumer group coordinator discovery.
- High-level producer metadata lookup, leader routing, negotiated Produce API selection, single-record send, batch send, and buffered send with `acks=1`. Against Kafka 3.7.2, the current path selects Produce v3 RecordBatch.
- Direct consumer fetch from an assigned topic partition using Fetch v2 response decoding.
- Consumer group join, sync, heartbeat, poll, and offset commit through the alpha classic consumer group path with range assignment.

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

## Not Yet Claimed

The current compatibility claim does not cover:

- TLS workflows beyond the listed TLS smoke examples.
- SASL workflows beyond the listed SASL_PLAINTEXT smoke examples.
- SASL_SSL broker profiles.
- Multi-broker clusters, leader failover, rack awareness, or partition expansion.
- Idempotent producers, transactions, compression, or high-throughput batching.
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
