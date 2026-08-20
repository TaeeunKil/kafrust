# Public API Audit

This audit tracks the current `kafrust` public surface for M12 API
stabilization. The goal is not to freeze the API yet. The goal is to make each
public type intentional before the project grows more protocol coverage.

## Root Re-Exports

The `kafrust` crate currently re-exports the high-level client surface from
`crates/kafrust/src/lib.rs`.

| Area | Public items | Stability intent | Notes before `1.0` |
| --- | --- | --- | --- |
| Low-level client | `Client`, `ClientConfig` | Alpha, stabilization candidate | Keep Kafka terms visible. `ClientConfig::validate` provides connection-free startup preflight. Flexible capability discovery prefers `ApiVersions` v4, falls back to v3, and exposes opt-in v5 cluster/node identity encoding. Validate whether direct `Client` request methods should remain public or become expert-level only. |
| Security config | `SecurityProtocol`, `SaslMechanism`, `SaslCredentials`, TLS client certificate/key configuration on `ClientConfig` | Alpha, stabilization candidate | Keep `SecurityProtocol` aligned with Kafka `security.protocol` and `SaslMechanism` aligned with Kafka mechanism names such as `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`. TLS client certificates and private keys are validated as a pair and the private key is redacted by `ClientConfig::Debug`. Revisit `SaslCredentials::password` docs before `1.0` because it intentionally exposes secret material to callers. |
| Producer | `Acks`, `Compression`, `Partitioner`, `ProducerConfig`, `Producer`, `BufferedProducer`, `BufferedProducerHandle`, `ProducerRecord`, `ProducerDelivery`, `RecordMetadata`, `Header`, `ProducerBatchReport`, `ProducerBatchRecordOutcome`, `ProducerBatchFailure` | Alpha, stabilization candidate | Keep producer concepts explicit: topic, partition, key, value, headers, acks, compression, metadata, and custom routing. `ProducerConfig::validate` provides connection-free startup preflight. `BufferedProducerHandle` provides cloneable concurrent enqueueing only for non-transactional buffered producers; callers must stop handles before owner close. Review batch failure naming after idempotent producer and retry semantics mature. |
| Direct consumer | `ConsumerConfig`, `Consumer`, `ConsumerRecord`, `ConsumerAssignment` | Alpha | Direct partition assignment is intentionally narrow. `ConsumerConfig::validate` provides connection-free startup preflight. Review assignment mutation and offset naming before broader fetch features. |
| Consumer group | `ConsumerGroupConfig`, `ConsumerGroup`, `ConsumerGroupHeartbeat`, `RebalanceListener`, `RebalanceEvent`, `RebalancePhase` | Alpha, highest churn risk | The group path exposes concrete and regex topic subscriptions, explicit per-record commit queueing, synchronous before/after assignment callbacks, `ConsumerGroup::topic_id` for stable KIP-848 topic UUID handoff, and `ConsumerGroupConfig::validate` startup preflight. Keep metadata resolution, commit lifecycle, callback ordering, cancellation, and background-heartbeat semantics explicit before `1.0`. |
| Kafka Streams group | `StreamsGroupConfig`, `StreamsGroupSession`, `StreamsGroupSessionHandle`, `StreamsGroupSessionAssignment`, and `kafrust::streams` API 88 protocol types | Alpha, expert-level | This is a Kafka Streams group membership/heartbeat layer, not a Streams DSL or task processor. `StreamsGroupSessionHandle` moves one joined session into a bounded background task that owns heartbeat scheduling, task-state commands, assignment snapshots, and graceful close. The Kafka 4.3.1 live gate in [`32372002532`](https://github.com/TaeeunKil/kafrust/actions/runs/32372002532) qualifies that bounded single-broker lifecycle; published-artifact qualification, automatic assignment/task-runtime reconciliation, complete application compatibility, and multi-member or coordinator-failure evidence remain before `1.0`. |
| Admin | `AdminClient`, `DescribeClusterOptions`, `DescribeClusterEndpointType`, `FeatureUpdate`, `FeatureUpgradeType`, `UpdateFeaturesOptions`, `ConfigResourceType`, `ListConfigResourcesOptions`, `ListGroupsOptions`, `UnregisterBrokerResult`, `StreamsGroupDescription`, `StreamsGroupTopology`, `StreamsGroupSubtopology`, `StreamsGroupTopic`, `StreamsGroupMember`, `StreamsGroupAssignment`, `ShareGroupStateInitializeTopic`, `ShareGroupStateReadTopic`, `ShareGroupStateWriteTopic`, `ShareGroupStateDeleteTopic`, and typed topic, group, state, security, quota, reassignment, producer, and transaction result types | Alpha, stabilization candidate | Keep controller, coordinator, leader, and all-broker routing visible. Preserve partial-result semantics for mutations and explicit reconciliation boundaries for ambiguous writes before `1.0`. `DescribeCluster` API 60 is exposed through the opt-in options path with Metadata fallback; `UpdateFeatures` v1 is preferred when advertised; v0 fallback must not silently weaken validation-only or unsafe-downgrade requests. API 64 `UnregisterBroker` is controller-routed with explicit unknown-outcome handling. API 74 preserves the Kafka 3.9 v0 client-metrics shape and the Kafka 4.1+ v1 resource shape; high-level `list_groups` now negotiates ListGroups v4/v5 per broker and `list_groups_with_options` adds state/type filters while preserving the v1 low-level path; member-aware Admin offsets negotiate OffsetFetch/OffsetCommit v10 through Metadata v12 topic-ID resolution (complete topic UUID builders can skip discovery) and fall back to v9; StreamsGroupDescribe API 89 is source-covered with live Kafka Streams qualification open; Share Group State APIs 83-87 prefer v1 fields and reject lossy v0 downgrades; opt-in DescribeConfigs v4 and live Share Group State qualification remain open. |
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
- `streams`
- `producer`

These modules are part of the alpha public API because users can import through
module paths as well as root re-exports. Before `1.0`, decide whether all module
paths remain supported or whether the root re-export surface becomes the
documented stable entry point.

All high-level connection-oriented builders now accept a complete
`ClientConfig` through `with_client_config(...)` and expose a matching
`build_config()` preflight that validates and returns the configuration without
opening a broker connection. `AdminClient::new(...)` exposes the same preflight
on the constructed admin handle. This keeps bootstrap servers, controller
bootstrap servers, security, decode limits, and shared metrics consistent when
an application creates more than one client. The individual typed setters
remain supported for local overrides; the integration test in
`crates/kafrust/tests/public_surface.rs` is the compile-time contract for this
common builder surface.

## Protocol Crate Boundary

`kafrust-protocol` remains a separate alpha crate for Kafka wire-format
mechanics. The high-level `kafrust` crate should not hide that boundary:

- protocol structs may be useful for tests, debugging, and expert integrations
- protocol structs can change as Kafka API version coverage grows
- high-level client APIs should return Kafka concepts, not raw wire structs,
  unless the method is explicitly a low-level broker request

Current decision: keep `kafrust::protocol` as an alpha expert escape hatch and
avoid adding more root-level protocol type re-exports.

The low-level `FetchRequestV13` and `FetchRequestV14` protocol structs expose
Kafka's legacy top-level `replica_id` field because it remains part of the
wire schema through v14; Kafka moves replica identity into the tagged
`FetchReplicaStateV15` structure starting with v15. Expert callers should use
`-1` for ordinary consumer requests. The high-level `Client::fetch_v13` and
`Client::fetch_v14` methods supply that value automatically.

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
