# Project Strategy

kafrust is a pure Rust Kafka client project. It is not a Kafka broker or Kafka-compatible server project.

The strategic goal is to become a practical Rust-native client for applications that need Kafka protocol compatibility without librdkafka, C bindings, or a required C toolchain.

## Replacement Target

kafrust should replace a Kafka client dependency in selected Rust applications before it tries to replace every feature of mature Kafka client stacks.

In scope:

- pure Rust Kafka wire protocol implementation
- producer, consumer, and classic consumer group client APIs
- Kafka concepts kept visible in public APIs
- Tokio-based async runtime behavior
- plaintext, TLS, and SASL client connectivity
- compatibility claims backed by live broker smoke tests
- small, auditable feature slices with focused protocol and runtime tests

Out of scope:

- replacing Apache Kafka brokers
- implementing a Kafka-compatible broker, proxy, controller, or storage engine
- wrapping librdkafka, C client bindings, or a required C toolchain
- hiding Kafka topics, partitions, offsets, acknowledgements, groups, commits, or broker errors behind generic queue abstractions
- claiming broad broker compatibility before the broker version and deployment profile has been tested

## Existing Alternatives

For production Kafka workloads that need broad feature coverage today, [`rust-rdkafka`](https://docs.rs/rdkafka/latest/rdkafka/) remains the practical default in Rust. It is mature and feature-rich because it wraps librdkafka, including support for areas such as mature producer and consumer behavior, admin operations, metrics, and exactly-once workflows.

That does not make kafrust redundant. kafrust's reason to exist is different:

- no librdkafka dependency
- no required C toolchain
- protocol code that can be audited and tested in Rust
- a smaller API surface while behavior is still stabilizing
- Kafka terminology preserved instead of abstracted away

Pure Rust alternatives also exist. This comparison is a source-documented
snapshot from 2026-08-19, not an independent benchmark:

- [`krafka`](https://github.com/hupe1980/krafka) is the closest broad
  pure-Rust competitor. Its current `main` manifest and docs.rs page are both
  `0.19.0` as of this snapshot. The current README claims Kafka 3.9+
  protocol parity, classic and KIP-848
  groups, ShareConsumer/KIP-932, transactions, OAUTHBEARER, AWS MSK IAM,
  broad Admin coverage, telemetry, Prometheus metrics, a fault-injecting fake
  broker, 2,350+ tests, and six fuzz targets. It is currently ahead of kafrust
  in pure-Rust feature breadth and test infrastructure. Its optional Zstd path
  requires a C toolchain, while kafrust's supported codecs remain pure Rust.
  Kafrust also verifies an older Kafka 3.7-to-current broker window.
- [`rskafka`](https://github.com/influxdata/rskafka) explicitly targets simple
  distributed write-ahead-log workloads. Its README excludes offset tracking,
  consumer groups, and transactions, so it is not pursuing the same broad
  replacement target.
- [`kafkit-client`](https://docs.rs/kafkit-client/latest/kafkit_client/) is a
  newer native async Rust client aimed at Kafka 4.0+ and KRaft. Its published
  `0.1.9` documentation lists KIP-848 groups, ShareConsumer, transactions,
  Admin, TLS/SASL, compression, and metrics. It deliberately drops classic
  group and older-broker compatibility, so it is a modern-broker competitor,
  not a drop-in replacement for every Kafka deployment.
- [`kafka-rust`](https://github.com/kafka-rust/kafka-rust) provides established
  producer and consumer APIs and is being maintained again, but its documented
  tested broker range currently ends at Kafka 3.1 and it does not claim all
  newer Kafka features.
- [`kacrab`](https://github.com/pirumu/kacrab) is another broad, Rust-native
  competitor. Its current `0.4.0` docs claim Kafka 4.3 producer, consumer,
  share-consumer, and 62-operation Admin surfaces, classic and KIP-848 groups,
  transactions, four codecs, TLS/SASL, generated protocol code, broker-matrix
  CI, and fuzzing. It accepts a broader broker floor than kafrust but its
  documented verified matrix is centered on 3.3.2, 3.6.2, 3.9.0, 4.0.0, and
  4.3.0. It also offers feature-gated TLS and codec choices whose dependency
  posture must be checked separately when a strict no-native-toolchain build is
  required. Its queue semantics and Java-shaped configuration make it a direct
  comparison for the replacement goal, not merely a protocol library.

These projects are references and competitors, not sources to copy blindly.
Feature claims must be validated in kafrust's own protocol tests, failure
injection, and live broker matrix.

Use this decision rule:

- If the user needs mature production Kafka features immediately, recommend `rust-rdkafka`.
- If the user needs a pure Rust client, no C dependency, and can accept alpha limits, kafrust is the project to grow.
- If the user needs a Kafka-compatible broker, kafrust is the wrong repository unless the project scope is deliberately changed.

## Completion Tiers

These tiers define what "usable" and "complete" mean for this project. Dates are planning estimates for a small project, not commitments.

### Alpha Client

Current state after the published v0.3.0 alpha release.

Expected use:

- experiments
- local broker checks
- simple internal tools
- protocol and API evaluation

Required evidence:

- crates.io release
- docs.rs build
- fresh published-crate compile smoke
- scheduled or manual Kafka live smoke

Known limits:

- plaintext remains the default networking path; TLS, SASL/PLAIN, and SASL_SSL/SCRAM-SHA-256 are live-verified for broker roundtrip, producer, direct consumer, and consumer group smoke paths on single-node Kafka 3.7.2 profiles
- the default build has no C toolchain dependency, but the optional TLS feature currently uses the rustls ring provider and may require native build tooling
- narrow broker compatibility matrix
- incomplete protocol coverage
- pre-1.0 public API

### Constrained Internal Client

Estimated effort: 2-4 months.

Target users can use kafrust for constrained internal workloads where Kafka deployment assumptions are known and narrow.

Required work:

- stabilize common producer and direct consumer workflows
- improve consumer group lifecycle behavior
- add focused failure tests for disconnects, stale metadata, and coordinator movement
- document exact supported broker profiles
- keep live smoke passing on every release candidate

Non-goal:

- broad replacement for `rust-rdkafka`

### Production-Like Plaintext Client

Estimated effort: 4-8 months.

Target users can test kafrust against production-like plaintext Kafka deployments.

Required work:

- multi-broker compatibility and leader failover verification
- broker version matrix beyond Kafka 3.7.2
- stronger reconnect behavior
- memory and backpressure limits
- producer and consumer performance benchmarks
- compatibility tests for record shapes, headers, partition expansion, and rebalances

Non-goal:

- secured enterprise Kafka deployments

### Common Enterprise Client

Estimated effort: 6-12 months.

Target users can connect kafrust to common company Kafka deployments.

Required work:

- TLS
- SASL PLAIN
- SASL SCRAM
- credential-safe errors and tracing
- documented plaintext, TLS, and SASL broker profiles
- live smoke or manual verification for secured broker profiles

This is the earliest tier where "real use" becomes plausible for many organizations.

### Broad Rust Kafka Client Replacement

Estimated effort: 18-36 months for a small team or sustained focused effort.

Target users can consider kafrust as a serious alternative to mature Kafka clients for a broad set of Rust services.

This is the complete replacement target for this repository: kafrust should be able to replace a Kafka client dependency in Rust applications. It does not mean replacing Apache Kafka brokers or implementing server-side Kafka storage, replication, controllers, or group coordination.

Required work:

- idempotent producer
- transactions and read-committed consumer behavior
- compression support
- admin APIs
- mature consumer group rebalancing
- metrics and structured tracing spans
- extensive broker version and deployment matrix
- load, soak, and failure-injection testing
- migration notes and semver discipline

This tier is where comparisons with `rust-rdkafka` and pure Rust alternatives become meaningful. It should not be promised from the current alpha line. The execution path for this target is tracked in roadmap milestones M13 through M21.

### Current Competitive Gap (2026-08-19)

The published `0.3.0` line has already closed the basic connectivity,
compression, security, multi-broker recovery, idempotence, transactions,
Admin MVP, observability, and migration-documentation gates recorded in the
roadmap. The current development branch has also added the first high-level
ShareConsumer runtime on top of the stable KIP-932 v1 wire path. It is still
behind the broad pure-Rust competitors in three areas:

- modern protocol breadth: the high-level Share Consumer/KIP-932 runtime and
  live qualification (the runtime exists, now negotiates KIP-1206 ShareFetch
  v2 for strict record limits, and has an opt-in cancellable heartbeat task and
  foreground coordinator rediscovery, and implements KIP-1222 renewal with
  broker lock-timeout tracking and Renew redelivery replacement; the Kafka
  4.3.1 single-node poll/Renew/expiry-redelivery/Accept path passed in
  [run 32213499877](https://github.com/TaeeunKil/kafrust/actions/runs/32213499877),
  and the three-broker fresh-consumer failover path passed in
  [run 32214201983](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983),
  while an active-heartbeat coordinator-movement path passed in
  [run 32215845737](https://github.com/TaeeunKil/kafrust/actions/runs/32215845737)
  and three independent matrix attempts passed in
  [run 32216383214](https://github.com/TaeeunKil/kafrust/actions/runs/32216383214),
  plus three consecutive in-process coordinator churn cycles in
  [run 32219147942](https://github.com/TaeeunKil/kafrust/actions/runs/32219147942);
  expiry/reconciliation soak and ambiguous acknowledgement reconciliation
  remain open),
  KIP-714 beyond the new low-level wire path (the high-level provider,
  scheduler, broker-negotiated pure-Rust compression, and optional built-in
  OTLP generation now exist, but live broker telemetry remains), remaining
  modern Admin paths, and protocol-parity automation;
- production hardening: more complete group lifecycle behavior, ambiguous
  transaction and mutation outcomes, resource ceilings, fuzzing, fault-
  injected broker coverage, longer secured multi-broker soaks, and
  assignment/rebalance fault matrices;
- adoption surface: stable 1.0 APIs, wider migration compatibility, published
  examples, and repeatable performance/SLO evidence across representative
  workloads.

`DescribeQuorum` is now live-qualified through the explicit controller-listener
path on Kafka 3.7.2 and 4.3.1. The remaining gap is broader modern Admin and
KRaft controller coverage, not this API's basic request/response path.
ConsumerGroupDescribe API key 69 is now available through the typed protocol,
low-level client, and high-level Admin path; its live KIP-848 qualification is
now runs through the existing Kafka 4.3.1 member-aware Admin workflow, with the
workflow result retained as the qualification evidence.

### Estimate To Surpass

These are effort ranges, not calendar promises:

- **Match krafka's current feature checklist:** roughly 6-12 months of
  focused full-time engineering, or about 9-18 calendar months at the current
  solo-project pace. The remaining large slices are Share Groups beyond the
  current alpha runtime, KIP-714 OTLP/compression/live qualification, modern
  protocol parity, fuzzing, and the associated live tests.
- **Match the current broad pure-Rust field (`krafka` plus `kacrab`):** roughly
  9-18 months of focused work from this branch, because feature names alone do
  not close the broker-version, failure, security, and release-evidence gap.
- **Surpass krafka on operational credibility:** roughly 12-24 months. This
  requires repeatable Kafka 3.7 through current matrices, secured and
  multi-broker fault injection, fuzzing, soak data, bounded-resource behavior,
  and migration evidence rather than only more APIs.
- **Credibly replace rust-rdkafka for broad Rust services:** roughly 18-36
  months. This includes the compatibility long tail, callback/configuration
  migration surface, old-broker behavior, production SLOs, and release
  discipline. Pure Rust should be the advantage; absolute throughput against
  a mature C client is not a realistic universal success criterion.

The practical "surpassed" gate is therefore: a representative service can
remove `rust-rdkafka`, compile against kafrust, pass its producer/consumer/
group/admin/security/transaction tests, survive the documented fault matrix,
and retain a rollback path. Feature count alone does not satisfy that gate.

### Kafka Broker Replacement

This is not a kafrust completion tier.

Replacing Apache Kafka as a broker means implementing a distributed log server, controller behavior, storage, replication, leader election, group coordination, transactions, admin surfaces, operational tooling, and compatibility behavior. That is a separate multi-year server project and would require a new repository strategy.

## Strategic Priorities

The next work should be ordered by user unlock, not by protocol completeness alone:

1. Security and connectivity: TLS and SASL are required for common company Kafka deployments.
2. Multi-broker behavior: metadata refresh, leader changes, and failover need live verification.
3. Compression: common record batches need snappy, gzip, lz4, and zstd compatibility.
4. Admin APIs: topic, config, and group administration are needed for integration tests and service bootstrap.
5. Idempotent producer: duplicate-safe retries are required for serious producer replacement.
6. Transactions and read-committed consumers: exactly-once workflows are required for broad replacement.
7. Observability and limits: metrics, structured spans, memory limits, and benchmark baselines are required for production operation.
8. Compatibility matrix and migration guide: replacement decisions need dated broker evidence and clear migration paths.

Do not expand public APIs just to look complete. Add public surface only when the protocol behavior and runtime behavior are tested enough to document.

## Success Criteria

kafrust is successful if it becomes the best choice for Rust users who value pure Rust implementation, auditability, and simple Kafka-native APIs enough to accept a narrower feature set than librdkafka-backed clients.

kafrust is not successful if it merely becomes a smaller, less compatible clone of `rust-rdkafka` without a clear pure-Rust advantage.
