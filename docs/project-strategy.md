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

Pure Rust alternatives also exist, but the general-purpose space remains open. [`rskafka`](https://docs.rs/rskafka/latest/rskafka/) explicitly positions itself as a minimal implementation for simple distributed write-ahead-log workloads, not a general-purpose Kafka implementation. Older Rust Kafka clients can be useful references, but kafrust should not copy their API shape by default.

Use this decision rule:

- If the user needs mature production Kafka features immediately, recommend `rust-rdkafka`.
- If the user needs a pure Rust client, no C dependency, and can accept alpha limits, kafrust is the project to grow.
- If the user needs a Kafka-compatible broker, kafrust is the wrong repository unless the project scope is deliberately changed.

## Completion Tiers

These tiers define what "usable" and "complete" mean for this project. Dates are planning estimates for a small project, not commitments.

### Alpha Client

Current state after the v0.2.x releases.

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

- plaintext remains the default networking path; TLS transport is feature-gated and live-verified for broker roundtrips; SASL/PLAIN is implemented with mock-broker coverage but is not live-verified yet
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
