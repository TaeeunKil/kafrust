# kafrust

[English](README.md) | [한국어](README.ko.md)

[![Crates.io](https://img.shields.io/crates/v/kafrust.svg)](https://crates.io/crates/kafrust)
[![Docs.rs](https://docs.rs/kafrust/badge.svg)](https://docs.rs/kafrust)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

A pure Rust Kafka client with no librdkafka or C toolchain dependency.

kafrust is an alpha Kafka client for Rust applications that need Kafka protocol
compatibility without wrapping `librdkafka`. The project keeps Kafka concepts
visible in public APIs: bootstrap servers, client IDs, topics, partitions,
offsets, acknowledgements, metadata refresh, consumer groups, heartbeats, and
commits.

Current release: `0.2.7`.

Use kafrust today for experiments, local broker checks, simple internal tools,
and API evaluation. For broad production Kafka workloads that need mature
features immediately, `rust-rdkafka` is still the practical Rust default.

## Table of Contents

- [Background](#background)
- [Install](#install)
  - [Requirements](#requirements)
- [Usage](#usage)
  - [Admin](#admin)
  - [Producer](#producer)
  - [Transactional Producer](#transactional-producer)
  - [Buffered Producer](#buffered-producer)
  - [Direct Consumer](#direct-consumer)
  - [Consumer Group](#consumer-group)
- [Compatibility](#compatibility)
- [Current Limits](#current-limits)
- [API](#api)
- [Documentation](#documentation)
- [Translations](#translations)
- [Contributing](#contributing)
- [License](#license)

## Background

kafrust is a Kafka client, not a Kafka broker or Kafka-compatible server.

The strategic goal is to become a practical Rust-native client for applications
that value a pure Rust implementation, auditable protocol code, and no required
C toolchain in the default build. The project deliberately does not wrap
`librdkafka`, and it does not try to hide Kafka's operational model behind a
generic queue abstraction. The optional `tls` feature currently uses the
`rustls` ring provider and may require native build tooling in some environments.

The near-term roadmap focuses on:

- secured client connectivity with TLS and SASL
- multi-broker metadata, leader, and failover behavior
- consumer group hardening
- compression support
- observability, limits, benchmarks, and compatibility evidence

See [Project Strategy](docs/project-strategy.md) for replacement targets,
non-goals, existing alternatives, and completion tiers.

## Install

Add kafrust to a Rust project:

```toml
[dependencies]
kafrust = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Or with Cargo:

```sh
cargo add kafrust@0.2
```

### Requirements

- Rust `1.81` or newer.
- A Tokio runtime in the application.
- A Kafka broker for runtime client calls.
- No `librdkafka` or C client bindings; the default build requires no C
  toolchain. The optional `tls` feature may require native build tooling for
  its ring crypto provider.

## Usage

Set broker addresses with `KAFRUST_BOOTSTRAP_SERVERS` when running examples.
Use Kafka's comma-separated bootstrap format for multiple brokers, for example
`localhost:19092,localhost:19093`. If the variable is omitted, the examples use
`localhost:9092`.
Smoke examples also accept `KAFRUST_SECURITY_PROTOCOL`,
`KAFRUST_SASL_USERNAME`, `KAFRUST_SASL_PASSWORD`, and
`KAFRUST_SASL_MECHANISM` for secured broker checks. Set
`KAFRUST_SASL_MECHANISM=oauthbearer` with `KAFRUST_SASL_TOKEN` for the
OAUTHBEARER path; `KAFRUST_SASL_TOKEN_PATH` can be used instead when the
application owns a rotating token file and wants the provider-backed path.
`KAFRUST_SASL_USERNAME` is optional for its authorization identity.

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_TOPIC=kafrust-smoke \
cargo run -p kafrust --example producer_send
```

### Admin

```rust
use kafrust::{AdminClient, ClientConfig, CreateTopicsOptions, NewTopic};

let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let cluster = admin.describe_cluster().await?;
println!("controller: {}", cluster.controller_id());

let result = admin
    .create_topics(
        &[NewTopic::new("orders", 6, 3).config("cleanup.policy", "compact")],
        CreateTopicsOptions::new(),
    )
    .await?;

for topic in result.topics() {
    println!("{}: Kafka error {}", topic.name(), topic.error_code());
}
```

`describe_cluster` and `list_topics` provide typed Metadata v1 views, and
topic config APIs expose values, sources, synonyms, and incremental
Set/Delete/Append/Subtract operations. `describe_consumer_groups` discovers
each group coordinator and preserves member protocol bytes.
`list_groups` queries all advertised brokers, while `delete_consumer_groups`
routes each group to its coordinator and retains per-group errors.
`create_partitions` routes CreatePartitions v0 to the controller for automatic
or explicit topic expansion. `delete_consumer_group_offsets` routes
OffsetDelete v0 to the coordinator and
preserves top-level and per-partition Kafka errors.
CreateTopics v2 and DeleteTopics v3 discover the active controller and preserve
per-topic partial success and error responses. See [Admin API](docs/admin-api.md).
`describe_acls`, `create_acls`, and `delete_acls` expose typed ACL bindings,
filters, and per-entry authorization outcomes; qualify them against an
authorizer-enabled broker before production rollout.
`describe_user_scram_credentials` and `alter_user_scram_credentials` expose
typed SCRAM credential administration, including controller routing and
per-user outcomes; the Kafka 3.7.2 SASL_SSL roundtrip is live-verified.
`alter_partition_reassignments` and `list_partition_reassignments` expose
controller-routed replica target changes, cancellation, and bounded ongoing
status inspection; the Kafka 3.7.2 three-broker path is live-verified.
`delete_records` routes DeleteRecords v1 to each current partition leader and
preserves per-partition low watermarks and broker errors for partial deletion.
`describe_producers` routes DescribeProducers v0 to each current partition
leader and exposes producer IDs, epochs, sequences, and active transaction
offsets. `describe_transactions` discovers each transactional ID's
coordinator and preserves transaction state, producer identity, and topic
partition membership.
Teams evaluating replacement of a librdkafka-backed application should follow
the staged [rust-rdkafka migration guide](docs/migration-from-rust-rdkafka.md).

### Producer

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-producer")
        .acks(Acks::Leader)
        .build()
        .await?;

    let metadata = producer
        .send(
            ProducerRecord::to("kafrust-smoke")
                .key("example-key")
                .value("hello from kafrust"),
        )
        .await?;

    println!(
        "produced {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );

    Ok(())
}
```

When no partition is specified, keyed records use Kafka-compatible Murmur2
partitioning. Keyless records stay on one partition for a complete send or
buffered flush, then rotate to the next available partition for the topic.

### Transactional Producer

Transactional production is available as an opt-in alpha:

```rust
use kafrust::{ProducerConfig, ProducerRecord};

let mut producer = ProducerConfig::new(["localhost:9092"])
    .transactional_id("orders-writer")
    .build()
    .await?;

producer.begin_transaction()?;
producer
    .send(ProducerRecord::to("kafrust-smoke").value("committed value"))
    .await?;
producer.commit_transaction().await?;
```

Commit, abort, read-committed isolation, and transactional consumer group
offset commits are verified against Kafka `3.7.2` and `4.3.1`.

### Buffered Producer

Use `ProducerConfig::build_buffered` when records should be queued and flushed
by linger time, record count, byte count, `flush`, or `close`.

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-buffered-producer")
        .acks(Acks::Leader)
        .linger_ms(10)
        .max_records_per_batch(100)
        .build_buffered()
        .await?;

    let delivery = producer
        .send(ProducerRecord::to("kafrust-smoke").value("buffered value"))
        .await?;

    let metadata = delivery.await?;
    producer.close().await?;

    println!("buffered record offset {}", metadata.offset());

    Ok(())
}
```

### Direct Consumer

Direct consumers fetch from explicit topic partitions and offsets. This path is
useful before consumer group behavior is needed.

```rust
use kafrust::ConsumerConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut consumer = ConsumerConfig::new(["localhost:9092"])
        .client_id("example-consumer")
        .build()
        .await?;

    consumer.assign("kafrust-smoke", 0, 0);
    for record in consumer.poll().await? {
        println!(
            "fetched {}-{}@{} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.value().map(String::from_utf8_lossy)
        );
    }

    Ok(())
}
```

### Consumer Group

The current consumer group API is an alpha classic or KIP-848 consumer group
path with dynamic or static membership, range, round-robin, or opt-in
cooperative-sticky assignment for classic groups, join, heartbeat, poll,
offset fetch/commit, explicit leave, and earliest/latest reset for partitions
that have no committed offset. The
cooperative-sticky path includes protocol, staged assignment, multi-member
ownership transfer, transient-member rollback, and member-loss recovery. These
cooperative failure paths are live-verified in the Kafka `3.7.2` three-broker
profile. The KIP-848 path, including flexible offset fetch/commit and background
heartbeat rejoin, is live-verified against Kafka `4.3.1`; the group API itself
remains pre-`1.0`.

```rust
use kafrust::{ConsumerGroupConfig, OffsetResetPolicy};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut group = ConsumerGroupConfig::new(["localhost:9092"], "example-group")
        .client_id("example-consumer-group")
        .group_instance_id("example-consumer-1")
        .offset_reset_policy(OffsetResetPolicy::Earliest)
        .subscribe("kafrust-smoke")
        .join()
        .await?;

    let records = group.poll().await?;
    group.commit_offsets().await?;

    println!("processed {} records", records.len());

    Ok(())
}
```

For interval-based queued commits, use the opt-in bounded worker and observe
its handle before shutdown:

```rust
let mut commit_worker = group
    .spawn_commit_worker(std::time::Duration::from_secs(1))
    .await?;

for record in &records {
    process(record.value())?;
    group.commit_record(record)?;
}

commit_worker.stop().await?;
group.leave().await?;
```

The worker coalesces offsets per partition and synchronizes its generation and
assignment state across `group.rejoin()`. Check `try_wait()` in long-running
applications so terminal commit or generation errors are not hidden.

## Compatibility

kafrust compatibility claims are limited to behavior verified against real
brokers.

| kafrust | Broker | Mode | Security | Status |
| --- | --- | --- | --- | --- |
| `0.2.x` | Apache Kafka `3.7.2` | single-node KRaft | `PLAINTEXT` | Passing live smoke |
| `0.2.x` | Apache Kafka `4.3.1` | single-node KRaft | `PLAINTEXT` | Passing live smoke |
| `0.2.x` | Apache Kafka `3.7.2` | single-node KRaft | `TLS` | Passing live smoke |
| `0.2.x` | Apache Kafka `3.7.2` | single-node KRaft | `SASL_PLAINTEXT` with SASL/PLAIN | Passing live smoke |
| `0.2.x` | Apache Kafka `3.7.2` | single-node KRaft | `SASL_SSL` with SCRAM-SHA-256 | Passing live smoke |
| `0.2.x` | Apache Kafka `3.7.2` | single-node KRaft | `SASL_SSL` with SCRAM-SHA-512 | Passing live smoke |
| `0.2.x` | Apache Kafka `3.8.1` | single-node KRaft | `PLAINTEXT` | Passing live smoke |
| `0.2.x` | Apache Kafka `3.9.1` | single-node KRaft | `PLAINTEXT` | Passing live smoke |

Verified paths currently include:

- `ApiVersions v0` and flexible `ApiVersions v3` capability roundtrips, plus
  `Metadata v1` roundtrips.
- High-level producer single-record, batch, and buffered sends.
- Direct topic-partition fetch with Fetch v4 response decoding.
- Classic consumer group join, sync, heartbeat, poll, and offset commit.
- KIP-848 consumer group assignment, member-epoch heartbeat, OffsetFetch v9,
  OffsetCommit v9, background rejoin, and explicit leave against Kafka `4.3.1`.

See [Compatibility](docs/compatibility.md) and
[Broker Roundtrip](docs/broker-roundtrip.md) for the current evidence.

## Current Limits

- Public APIs are pre-`1.0` and can change between minor releases.
- Plaintext TCP remains the default networking path.
- TLS transport is available behind the non-default `tls` crate feature and is
  verified against Kafka `3.7.2` for broker roundtrip, producer, direct
  consumer, and consumer group smoke paths. TLS certificate server name
  validation defaults to the bootstrap host and can be overridden with
  `tls_server_name(name)`. DER-encoded extra root certificates can be added with
  `tls_root_certificate_der(bytes)`. The current ring crypto provider can
  require native build tooling for this optional feature.
- SASL/PLAIN authentication is verified against Kafka `3.7.2` over
  `SaslPlaintext` for broker roundtrip, producer, direct consumer, and consumer
  group smoke paths. SASL/SCRAM-SHA-256 and SCRAM-SHA-512 are verified over
  `SaslTls`; the SHA-512 profile covers broker roundtrip, producer, batch,
  buffered producer, direct consumer, and consumer group poll paths.
- SASL/OAUTHBEARER token authentication is available through
  `sasl_oauthbearer` and `sasl_oauthbearer_with_username` on the client,
  producer, consumer, and consumer-group builders. Async token providers are
  available through the corresponding `*_provider` builders and are called for
  each new broker authentication. It is covered by injected handshake tests
  and a Kafka 3.7.2 SASL_SSL smoke using the broker's built-in unsecured
  validator (`Live Kafka Smoke` run `31478375106`). OAUTHBEARER initial
  authentication and provider re-authentication use flexible
  `SaslAuthenticate v2`; PLAIN and SCRAM continue to use `v1`. A signed
  JWT/JWKS OIDC fixture also passes Kafka's validator, the Java Kafka client,
  and kafrust static and provider-backed paths in the
  [`Live Kafka Smoke` OIDC job](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94075906934).
  External provider-specific behavior remains separately qualified.
- Single-node plaintext compatibility is verified against Kafka `3.7.2`,
  `3.8.1`, `3.9.1`, and `4.3.1`. Secured and multi-broker profiles remain
  verified against `3.7.2`.
- Three-broker coordinator and leader failover is verified for the documented
  producer, direct consumer, classic consumer group, KIP-848 consumer group,
  and transaction paths. Topic
  partition expansion is verified through CreatePartitions v0 and Metadata v1;
  rack-aware client routing is not yet claimed.
- `ProducerConfig::partitioner` supports thread-safe custom routing for records
  without explicit partitions across immediate, batch, and buffered sends.
- Gzip, Snappy, and LZ4 compression use Produce v3 RecordBatch encoding; Zstd
  requires and negotiates Produce v7. Fetch v4 decodes all four codecs. They are
  verified against Kafka `3.7.2` plaintext single-node and multi-broker smoke
  profiles and the single-node TLS profile. Snappy uses Kafka-compatible Xerial
  framing and accepts raw Snappy blocks when decoding.
- Idempotent single-record, batch, and buffered sends are available as an
  opt-in alpha. Transactional immediate, batch, and buffered sends support
  explicit begin, commit, and abort. `IsolationLevel::ReadCommitted` hides
  aborted transaction records for direct and group consumers, and current
  group assignments can be committed through immediate or buffered
  `send_group_offsets_to_transaction`. Transaction coordinator and
  producer/direct-consumer/group recovery after a broker stop are live-verified
  in the three-broker profile. Client quota, SCRAM credential administration,
  and partition reassignment are available through `AdminClient`. Shared request, retry,
  broker-error, producer,
  consumer, batch, and buffered-queue metrics are available together with
  high-level operation and `kafka.request` spans.
- `acks=0` fire-and-forget sends are supported for immediate and batch
  producer paths. The request is written and flushed without waiting for a
  broker response, so returned offsets are `-1` and broker acceptance or
  partition-level errors cannot be confirmed. This path is live-verified
  against Kafka `3.7.2`, `3.8.1`, `3.9.1`, and `4.3.1` single-node plaintext
  profiles.

## API

Primary public entry points:

- `Client` for low-level Kafka request roundtrips.
- `AdminClient` and typed cluster, topic, configuration, consumer-group, ACL,
  and client-quota administration types.
- `ProducerConfig`, `Producer`, `BufferedProducer`, and `ProducerRecord`.
- `Compression` for opt-in producer RecordBatch compression.
- `ConsumerConfig`, `Consumer`, `ConsumerAssignment`, `ConsumerRecord`, and
  `ConsumerPartitionQueue` for bounded per-partition delivery.
- `ConsumerGroupConfig`, `ConsumerGroup`, `ConsumerGroupProtocol`, and
  `ConsumerGroupHeartbeat`.
- `SecurityProtocol`, `SaslMechanism`, and `SaslCredentials` for plaintext,
  TLS, and SASL connection modes.
- `ClientMetrics` and `ClientMetricsSnapshot` for request-level observability.
- `Error::ResponseTooLarge` and `max_response_bytes` builders for bounded
  broker response allocation.
- `max_decode_array_elements` and `max_decompressed_record_bytes` builders for
  bounded protocol collections and compressed Fetch record batches.
- `kafrust::protocol` for the companion `kafrust-protocol` crate.

Generated API documentation:

- [`kafrust`](https://docs.rs/kafrust/0.2.7/kafrust/)
- [`kafrust-protocol`](https://docs.rs/kafrust-protocol/0.2.7/kafrust_protocol/)

## Documentation

- [Contributing](CONTRIBUTING.md)
- [Agent instructions](AGENTS.md)
- [Agentic development workflow](docs/agentic-development.md)
- [Project strategy](docs/project-strategy.md)
- [Performance benchmarks](docs/performance.md)
- [Roadmap](docs/roadmap.md)
- [Broker roundtrip](docs/broker-roundtrip.md)
- [Compatibility](docs/compatibility.md)
- [Migrating from rust-rdkafka](docs/migration-from-rust-rdkafka.md)
- [API stability](docs/api-stability.md)
- [Public API audit](docs/public-api-audit.md)
- [Producer API direction](docs/producer-api.md)
- [Producer buffering and linger design](docs/producer-buffering.md)
- [Consumer API direction](docs/consumer-api.md)
- [Consumer group direction](docs/consumer-groups.md)
- [Release preparation](docs/release.md)

## Translations

`README.md` is the canonical English README.

Available translations:

- [English](README.md)
- [한국어](README.ko.md)

If more translated READMEs are added, name them with BCP 47 language tags, such
as `README.ja.md`, and keep release facts, compatibility claims, and limits
aligned with this file.

## Contributing

Issues and pull requests are accepted on GitHub.

Before contributing:

- Read [Contributing](CONTRIBUTING.md) and [Agent instructions](AGENTS.md).
- Keep kafrust a pure Rust Kafka client.
- Do not introduce `librdkafka`, C client bindings, or a required C toolchain.
- Preserve Kafka user-facing concepts in public APIs.
- Use Conventional Commits.
- Add focused tests for protocol behavior or observable client behavior.
- Update docs when public behavior, project direction, or workflow changes.

## License

MIT OR Apache-2.0 (c) kafrust contributors.

See [MIT License](LICENSE-MIT) and
[Apache License, Version 2.0](LICENSE-APACHE).
