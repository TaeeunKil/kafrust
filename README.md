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

Current release: `0.2.1`.

Use kafrust today for experiments, local broker checks, simple internal tools,
and API evaluation. For broad production Kafka workloads that need mature
features immediately, `rust-rdkafka` is still the practical Rust default.

## Table of Contents

- [Background](#background)
- [Install](#install)
  - [Requirements](#requirements)
- [Usage](#usage)
  - [Producer](#producer)
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
C toolchain. The project deliberately does not wrap `librdkafka`, and it does
not try to hide Kafka's operational model behind a generic queue abstraction.

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

- Rust `1.75` or newer.
- A Tokio runtime in the application.
- A Kafka broker for runtime client calls.
- No `librdkafka`, C client bindings, or required C toolchain.

## Usage

Set the broker address with `KAFRUST_BOOTSTRAP_SERVERS` when running examples.
If the variable is omitted, the examples use `localhost:9092`.
Smoke examples also accept `KAFRUST_SECURITY_PROTOCOL`,
`KAFRUST_SASL_USERNAME`, and `KAFRUST_SASL_PASSWORD` for secured broker checks.

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_TOPIC=kafrust-smoke \
cargo run -p kafrust --example producer_send
```

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

The current consumer group API is an alpha classic consumer group path with
join, sync, heartbeat, poll, and offset commit support.

```rust
use kafrust::ConsumerGroupConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut group = ConsumerGroupConfig::new(["localhost:9092"], "example-group")
        .client_id("example-consumer-group")
        .subscribe("kafrust-smoke")
        .join()
        .await?;

    let records = group.poll().await?;
    group.commit_offsets().await?;

    println!("processed {} records", records.len());

    Ok(())
}
```

## Compatibility

kafrust compatibility claims are limited to behavior verified against real
brokers.

| kafrust | Broker | Mode | Security | Status |
| --- | --- | --- | --- | --- |
| `0.2.x` | Apache Kafka `3.7.2` | single-node KRaft | `PLAINTEXT` | Passing live smoke |

Verified paths currently include:

- `ApiVersions v0` and `Metadata v1` roundtrips.
- High-level producer single-record, batch, and buffered sends.
- Direct topic-partition fetch with Fetch v2 response decoding.
- Classic consumer group join, sync, heartbeat, poll, and offset commit.

See [Compatibility](docs/compatibility.md) and
[Broker Roundtrip](docs/broker-roundtrip.md) for the current evidence.

## Current Limits

- Public APIs are pre-`1.0` and can change between minor releases.
- Plaintext TCP remains the default networking path.
- TLS transport is available behind the non-default `tls` crate feature and is
  verified against Kafka `3.7.2` for broker roundtrip, producer, direct
  consumer, and consumer group smoke paths. TLS certificate server name
  validation defaults to the bootstrap host and can be overridden with
  `tls_server_name(name)`.
- SASL/PLAIN authentication is verified against Kafka `3.7.2` over
  `SaslPlaintext` for broker roundtrip, producer, direct consumer, and consumer
  group smoke paths; `SaslTls` and broader SASL workflows are not claimed yet.
- Broker compatibility is verified against Kafka `3.7.2` only.
- Multi-broker clusters, leader failover, rack awareness, and partition
  expansion are not yet claimed.
- Idempotent producers, transactions, compression, admin APIs, and broad
  observability are not implemented yet.
- `acks=0` remains unsupported because the current request loop expects a broker
  response.

## API

Primary public entry points:

- `Client` for low-level Kafka request roundtrips.
- `ProducerConfig`, `Producer`, `BufferedProducer`, and `ProducerRecord`.
- `ConsumerConfig`, `Consumer`, `ConsumerAssignment`, and `ConsumerRecord`.
- `ConsumerGroupConfig`, `ConsumerGroup`, and `ConsumerGroupHeartbeat`.
- `SecurityProtocol` for plaintext, TLS, and SASL connection modes.
- `kafrust::protocol` for the companion `kafrust-protocol` crate.

Generated API documentation:

- [`kafrust`](https://docs.rs/kafrust/0.2.1/kafrust/)
- [`kafrust-protocol`](https://docs.rs/kafrust-protocol/0.2.1/kafrust_protocol/)

## Documentation

- [Contributing](CONTRIBUTING.md)
- [Agent instructions](AGENTS.md)
- [Agentic development workflow](docs/agentic-development.md)
- [Project strategy](docs/project-strategy.md)
- [Roadmap](docs/roadmap.md)
- [Broker roundtrip](docs/broker-roundtrip.md)
- [Compatibility](docs/compatibility.md)
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
