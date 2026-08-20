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
| Kafka Streams group | `StreamsGroupConfig`, `StreamsGroupSession`, `StreamsGroupSessionHandle`, `StreamsGroupSessionAssignment`, `StreamsTaskRuntime`, `StreamsTaskId`, `StreamsTaskTransition`, and `kafrust::streams` API 88 protocol types | Alpha, expert-level | This is a Kafka Streams group membership/heartbeat layer, not a Streams DSL or task processor. `StreamsGroupSessionHandle` moves one joined session into a bounded background task that owns heartbeat scheduling, task-state commands, assignment snapshots, and graceful close. `StreamsTaskRuntime` applies Kafka's nullable role updates, canonicalizes task identity, rejects conflicting assignments, and emits deterministic lifecycle transitions; applications still own consumer assignment, state restoration, and processing. Kafka 4.3.1 live gates [`32373425539`](https://github.com/TaeeunKil/kafrust/actions/runs/32373425539) and [`32374858753`](https://github.com/TaeeunKil/kafrust/actions/runs/32374858753) qualify single-broker two-member churn and three-broker coordinator-stop recovery. The published `0.3.3` public surface compiles from a fresh external project on stable and Rust 1.81 in [`32380345199`](https://github.com/TaeeunKil/kafrust/actions/runs/32380345199), and the published single-broker runtime gate passes in [`32381356444`](https://github.com/TaeeunKil/kafrust/actions/runs/32381356444); complete application compatibility remains before `1.0`. |
| Admin | `AdminClient`, `DescribeClusterOptions`, `DescribeClusterEndpointType`, `FeatureUpdate`, `FeatureUpgradeType`, `UpdateFeaturesOptions`, `ConfigResourceType`, `ListConfigResourcesOptions`, `ListGroupsOptions`, `UnregisterBrokerResult`, `StreamsGroupDescription`, `StreamsGroupTopology`, `StreamsGroupSubtopology`, `StreamsGroupTopic`, `StreamsGroupMember`, `StreamsGroupAssignment`, `ShareGroupStateInitializeTopic`, `ShareGroupStateReadTopic`, `ShareGroupStateWriteTopic`, `ShareGroupStateDeleteTopic`, and typed topic, group, state, security, quota, reassignment, producer, and transaction result types | Alpha, stabilization candidate | Keep controller, coordinator, leader, and all-broker routing visible. Preserve partial-result semantics for mutations and explicit reconciliation boundaries for ambiguous writes before `1.0`. `DescribeCluster` API 60 is exposed through the opt-in options path with Metadata fallback; `UpdateFeatures` v1 is preferred when advertised; v0 fallback must not silently weaken validation-only or unsafe-downgrade requests. API 64 `UnregisterBroker` is controller-routed with explicit unknown-outcome handling. API 74 preserves the Kafka 3.9 v0 client-metrics shape and the Kafka 4.1+ v1 resource shape; high-level `list_groups` now negotiates ListGroups v4/v5 per broker and `list_groups_with_options` adds state/type filters while preserving the v1 low-level path; member-aware Admin offsets negotiate OffsetFetch/OffsetCommit v10 through Metadata v12 topic-ID resolution (complete topic UUID builders can skip discovery) and fall back to v9; StreamsGroupDescribe API 89 is source-covered with live Kafka Streams qualification open; Share Group State APIs 83-87 prefer v1 fields and reject lossy v0 downgrades; opt-in DescribeConfigs v4 is source-covered and externally verified on Kafka 4.3.1 in published `0.3.3`; replicated Kafka 4.3.1 Share Group State failover is live-qualified in [`32398034582`](https://github.com/TaeeunKil/kafrust/actions/runs/32398034582), while broader unstable-API version, security, and long-running evidence remain open. |
| Errors | `Error`, `Result`, `BrokerErrorKind` | Alpha, stabilization candidate | Common startup configuration failures now use `Error::InvalidConfiguration { field, reason }`; consumer-group assignment deadlines use `Error::ConsumerGroupAssignmentTimeout { timeout_ms }`; Admin mutations whose transmitted response is lost use `Error::AdminMutationOutcomeUnknown { operation }`; keep Kafka broker error codes observable and preserve dedicated variants for missing bootstrap, TLS, SASL, and transaction-outcome failures. |
| Protocol escape hatch | `protocol` (`kafrust_protocol`) | Alpha, expert-level | Keep as a convenience re-export for now. Do not promote protocol request/response structs into the `kafrust` root prelude unless a high-level client API requires them. |
| Version | `version()` | Stable enough | Keep as a simple compile-time package version helper. |

The Admin qualification above now has both current-source and published-artifact
evidence for replicated Share Group State failover: the live Kafka 4.3.1 gate
passed in [`32398034582`](https://github.com/TaeeunKil/kafrust/actions/runs/32398034582),
and the published `kafrust 0.3.3` external-project gate passed in
[`32399284180`](https://github.com/TaeeunKil/kafrust/actions/runs/32399284180).
This remains an unstable broker-internal API qualification; broader version,
security, long-running, and general Kafka replacement evidence remain open.
The published `DescribeCluster` API 60 broker-bootstrap baseline passed from
fresh `kafrust 0.3.3` projects on Kafka 3.7.2 and 4.3.1 in
[`32400851719`](https://github.com/TaeeunKil/kafrust/actions/runs/32400851719)
and [`32400851830`](https://github.com/TaeeunKil/kafrust/actions/runs/32400851830).
The published `kafrust 0.3.4` controller-bootstrap path then passed both Kafka
versions in [`32403253526`](https://github.com/TaeeunKil/kafrust/actions/runs/32403253526)
and [`32403253688`](https://github.com/TaeeunKil/kafrust/actions/runs/32403253688).
`DescribeClusterEndpointType::Controllers` now requires and uses
`ClientConfig::controller_bootstrap_servers`; omitting that configuration is
rejected explicitly rather than attempting a broker-listener request.
The published ShareConsumer surface also passed a 300-second two-member
ownership run from `kafrust 0.3.4` in
[`32404294014`](https://github.com/TaeeunKil/kafrust/actions/runs/32404294014):
each member retained three partitions, accepted and consumed 30 records, and
closed with no in-flight requests or failed requests. This is bounded
long-running ownership evidence; member-loss, backpressure SLO, and full
replacement compatibility remain open.
The same published `kafrust 0.3.4` surface then passed four forced
member-loss/rejoin cycles in
[`32405501232`](https://github.com/TaeeunKil/kafrust/actions/runs/32405501232):
ownership alternated across the two members, all 24 partition/offset pairs were
unique, and the final survivor reported all six partitions with no in-flight or
failed requests. Higher-cycle churn, backpressure SLO, and full replacement
compatibility remain open.

The published `kafrust 0.3.4` `DescribeCluster` gate also qualifies the public
`AdminClient::describe_features` read path through ApiVersions v3 on Kafka
3.7.2 and 4.3.1 in [`32406914244`](https://github.com/TaeeunKil/kafrust/actions/runs/32406914244)
and [`32406914237`](https://github.com/TaeeunKil/kafrust/actions/runs/32406914237).
The external artifacts observed supported/finalized feature counts of `1/1`
and `1/6`, with finalized epochs `68` and `80`, respectively. Feature mutation,
security, and broader version matrices remain open.

The published `kafrust 0.3.4` external gate also qualifies
`AdminClient::describe_consumer_groups_modern` (ConsumerGroupDescribe API 69)
on Kafka 4.3.1 in [`32408765709`](https://github.com/TaeeunKil/kafrust/actions/runs/32408765709).
The fixture joined a real KIP-848 member and observed `state=Stable`, group and
assignment epochs `2/2`, `member_type=1`, `member_epoch=2`, and both current and
target assignment for topic partition 0. Kafka versions that do not advertise
API 69, security variants, and multi-member churn remain separate gates.

The published `kafrust 0.3.4` external gate also qualifies
`AdminClient::describe_share_groups` (stable ShareGroupDescribe API 77) on
Kafka 4.3.1 in [`32410690294`](https://github.com/TaeeunKil/kafrust/actions/runs/32410690294).
The fixture kept a real ShareConsumer member active and observed `state=Stable`,
group/assignment epochs `3/3`, `member_epoch=3`, the subscribed topic and
partition 0 assignment, and `authorized_operations=3400`. Kafka 4.0's removed
early-access v0, security variants, and multi-member Admin reads remain separate
gates.

The published `kafrust 0.3.4` external gate also exercises the public
OAUTHBEARER surface on Kafka 3.7.2 in
[`32411655133`](https://github.com/TaeeunKil/kafrust/actions/runs/32411655133).
The fixture used `SecurityProtocol::SaslTls`,
`ClientConfig::sasl_oauthbearer_provider`, `AdminClient::describe_cluster`,
`ProducerConfig::with_client_config`, and
`ConsumerConfig::with_client_config` from a fresh Cargo project resolved from
crates.io. It passed the broker's built-in unsecured validator and read back
the produced record. Signed OIDC/JWKS, provider discovery, and
provider-specific failure behavior remain unqualified.

The published `kafrust 0.3.4` signed OAUTHBEARER gate also exercises the same
public API against a Kafka 3.7.2 broker backed by a local OIDC/JWKS validator
in [`32412721829`](https://github.com/TaeeunKil/kafrust/actions/runs/32412721829).
The fresh crates.io project supplied an RS256 token through
`ClientConfig::sasl_oauthbearer_provider` and completed
`AdminClient::describe_cluster`, `ProducerConfig::with_client_config`, and
`ConsumerConfig::with_client_config` after Kafka validated its signature,
issuer, audience, and subject. External provider discovery and rotation remain
unqualified.

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
