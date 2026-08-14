# Public API Audit

This audit tracks the current `kafrust` public surface for M12 API
stabilization. The goal is not to freeze the API yet. The goal is to make each
public type intentional before the project grows more protocol coverage.

## Root Re-Exports

The `kafrust` crate currently re-exports the high-level client surface from
`crates/kafrust/src/lib.rs`.

| Area | Public items | Stability intent | Notes before `1.0` |
| --- | --- | --- | --- |
| Low-level client | `Client`, `ClientConfig` | Alpha, stabilization candidate | Keep Kafka terms visible. `ClientConfig::validate` provides connection-free startup preflight. Validate whether direct `Client` request methods should remain public or become expert-level only. |
| Security config | `SecurityProtocol`, `SaslMechanism`, `SaslCredentials` | Alpha, stabilization candidate | Keep `SecurityProtocol` aligned with Kafka `security.protocol` and `SaslMechanism` aligned with Kafka mechanism names such as `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`. Revisit `SaslCredentials::password` docs before `1.0` because it intentionally exposes secret material to callers. |
| Producer | `Acks`, `Compression`, `Partitioner`, `ProducerConfig`, `Producer`, `BufferedProducer`, `ProducerRecord`, `ProducerDelivery`, `RecordMetadata`, `Header`, `ProducerBatchReport`, `ProducerBatchRecordOutcome`, `ProducerBatchFailure` | Alpha, stabilization candidate | Keep producer concepts explicit: topic, partition, key, value, headers, acks, compression, metadata, and custom routing. `ProducerConfig::validate` provides connection-free startup preflight. Review batch failure naming after idempotent producer and retry semantics mature. |
| Direct consumer | `ConsumerConfig`, `Consumer`, `ConsumerRecord`, `ConsumerAssignment` | Alpha | Direct partition assignment is intentionally narrow. `ConsumerConfig::validate` provides connection-free startup preflight. Review assignment mutation and offset naming before broader fetch features. |
| Consumer group | `ConsumerGroupConfig`, `ConsumerGroup`, `ConsumerGroupHeartbeat`, `RebalanceListener`, `RebalanceEvent`, `RebalancePhase` | Alpha, highest churn risk | The group path exposes concrete and regex topic subscriptions, explicit per-record commit queueing, synchronous before/after assignment callbacks, and `ConsumerGroupConfig::validate` startup preflight. Keep metadata resolution, commit lifecycle, callback ordering, cancellation, and background-heartbeat semantics explicit before `1.0`. |
| Admin | `AdminClient` and typed topic, group, security, quota, reassignment, producer, and transaction result types | Alpha, stabilization candidate | Keep controller, coordinator, leader, and all-broker routing visible. Preserve partial-result semantics for mutations and explicit reconciliation boundaries for ambiguous writes before `1.0`. |
| Errors | `Error`, `Result`, `BrokerErrorKind` | Alpha, stabilization candidate | Common startup configuration failures now use `Error::InvalidConfiguration { field, reason }`; consumer-group assignment deadlines use `Error::ConsumerGroupAssignmentTimeout { timeout_ms }`; Admin mutations whose transmitted response is lost use `Error::AdminMutationOutcomeUnknown { operation }`; keep Kafka broker error codes observable and preserve dedicated variants for missing bootstrap, TLS, SASL, and transaction-outcome failures. |
| Protocol escape hatch | `protocol` (`kafrust_protocol`) | Alpha, expert-level | Keep as a convenience re-export for now. Do not promote protocol request/response structs into the `kafrust` root prelude unless a high-level client API requires them. |
| Version | `version()` | Stable enough | Keep as a simple compile-time package version helper. |

## Module Visibility

The crate also exposes public modules:

- `client`
- `config`
- `consumer`
- `error`
- `group`
- `producer`

These modules are part of the alpha public API because users can import through
module paths as well as root re-exports. Before `1.0`, decide whether all module
paths remain supported or whether the root re-export surface becomes the
documented stable entry point.

## Protocol Crate Boundary

`kafrust-protocol` remains a separate alpha crate for Kafka wire-format
mechanics. The high-level `kafrust` crate should not hide that boundary:

- protocol structs may be useful for tests, debugging, and expert integrations
- protocol structs can change as Kafka API version coverage grows
- high-level client APIs should return Kafka concepts, not raw wire structs,
  unless the method is explicitly a low-level broker request

Current decision: keep `kafrust::protocol` as an alpha expert escape hatch and
avoid adding more root-level protocol type re-exports.

## Pre-`1.0` Review Checklist

Before claiming a stable candidate API:

- confirm each root re-export is used by examples, docs, or a clear downstream
  workflow
- document any intentionally expert-level API separately from the common
  producer/consumer path
- remove or hide public helpers that only exist for internal implementation
- keep builder defaults documented with their Kafka meaning
- make common misconfiguration failures explicit error variants
- add migration notes for any renamed type, method, variant, or changed default
