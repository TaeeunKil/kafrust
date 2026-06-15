# kafrust

[![Crates.io](https://img.shields.io/crates/v/kafrust.svg)](https://crates.io/crates/kafrust)
[![Docs.rs](https://docs.rs/kafrust/badge.svg)](https://docs.rs/kafrust)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)

A pure Rust Kafka client with no librdkafka or C toolchain dependency.

`kafrust` is the high-level client crate in the kafrust workspace. It provides
Tokio-based producer, direct consumer, and alpha classic consumer group APIs on
top of the companion [`kafrust-protocol`](https://docs.rs/kafrust-protocol)
wire-format crate.

Current release: `0.2.1`.

This crate is alpha. Use it for experiments, local broker checks, simple
internal tools, and API evaluation. For broad production Kafka workloads that
need mature features immediately, `rust-rdkafka` remains the practical Rust
default today.

## Design Goals

- Keep Kafka concepts visible in public APIs.
- Stay pure Rust with no `librdkafka`, C client binding, or required C
  toolchain.
- Make protocol and runtime behavior auditable through small, tested slices.
- Claim compatibility only when a real broker profile has been verified.

The public model intentionally exposes Kafka terms such as bootstrap servers,
client IDs, topics, partitions, offsets, acknowledgements, metadata refresh,
consumer groups, generations, members, heartbeats, and commits.

## Install

```toml
[dependencies]
kafrust = "0.2"
tokio = { version = "1", features = ["macros", "rt"] }
```

For a multi-threaded application runtime, enable Tokio's `rt-multi-thread`
feature in the application.

## Producer

```rust,no_run
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-producer")
        .acks(Acks::Leader)
        .build()
        .await?;

    let metadata = producer
        .send(
            ProducerRecord::to("orders")
                .key("order-123")
                .value("created")
                .header("source", "checkout"),
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

## Batch Producer

`Producer::send_batch` returns metadata in input order. Use
`Producer::send_batch_report` when partial per-record failures need to be
inspected without losing successful records.

```rust,no_run
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-batch-producer")
        .acks(Acks::Leader)
        .max_records_per_batch(500)
        .max_batch_bytes(64 * 1024)
        .build()
        .await?;

    let report = producer
        .send_batch_report([
            ProducerRecord::to("orders").key("order-124").value("created"),
            ProducerRecord::to("orders").key("order-125").value("created"),
        ])
        .await?;

    for outcome in report.records() {
        if let Some(metadata) = outcome.metadata() {
            println!("{}-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());
        }
        if let Some(failure) = outcome.failure() {
            eprintln!(
                "record {} failed on {}-{}: {}",
                failure.record_index(),
                failure.topic(),
                failure.partition(),
                failure.error()
            );
        }
    }

    Ok(())
}
```

## Buffered Producer

`ProducerConfig::build_buffered` creates an opt-in buffered producer. Records are
flushed by linger time, record count, byte count, explicit `flush`, or `close`.

```rust,no_run
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-buffered-producer")
        .acks(Acks::Leader)
        .linger_ms(10)
        .max_records_per_batch(100)
        .build_buffered()
        .await?;

    let delivery = producer
        .send(ProducerRecord::to("orders").value("buffered value"))
        .await?;

    let metadata = delivery.await?;
    producer.close().await?;

    println!("buffered record offset {}", metadata.offset());

    Ok(())
}
```

## Direct Consumer

The direct consumer path fetches from explicit topic partitions and offsets.
This is useful when consumer group behavior is not needed.

```rust,no_run
use kafrust::ConsumerConfig;

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut consumer = ConsumerConfig::new(["localhost:9092"])
        .client_id("example-consumer")
        .max_poll_records(500)
        .build()
        .await?;

    consumer.assign("orders", 0, 0);

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

## Consumer Group

The consumer group API is an alpha classic consumer group path with join, sync,
heartbeat, poll, and offset commit support.

```rust,no_run
use kafrust::ConsumerGroupConfig;

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut group = ConsumerGroupConfig::new(["localhost:9092"], "orders-reader")
        .client_id("example-consumer-group")
        .subscribe("orders")
        .join()
        .await?;

    let records = group.poll().await?;
    group.commit_offsets().await?;

    println!("processed {} records", records.len());

    Ok(())
}
```

## Security Protocols

`SecurityProtocol` models Kafka connection modes:

- `Plaintext`
- `Tls`
- `SaslPlaintext`
- `SaslTls`

Plaintext is the default transport. TLS transport is available only when the
non-default `tls` crate feature is enabled:

```toml
kafrust = { version = "0.2", features = ["tls"] }
```

Without that feature, `SecurityProtocol::Tls` returns `Error::Unsupported`
before connecting.

TLS server name validation defaults to the bootstrap host. Use
`tls_server_name(name)` on `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, or
`ConsumerGroupConfig` when the bootstrap address differs from the broker
certificate subject alternative name.

Use `tls_root_certificate_der(bytes)` to add DER-encoded root certificates while
still keeping platform roots enabled.

SASL/PLAIN credentials can be stored on the shared client configuration with
`sasl_plain(username, password)`. This does not change the selected
`SecurityProtocol`; choose `SaslPlaintext` or `SaslTls` separately. Credential
debug output redacts the password. The `SaslPlaintext` broker roundtrip,
producer, direct consumer, and consumer group smoke paths are verified against
Kafka `3.7.2`; `SaslTls` and broader SASL workflows are not claimed yet.

The default build does not include TLS dependencies. The current `tls` feature
uses the `rustls` ring crypto provider, which can require native build tooling
in some environments; this is not part of the default kafrust toolchain.

## Compatibility

The `0.2.x` alpha line is verified against a single-node Apache Kafka `3.7.2`
KRaft broker over `PLAINTEXT`.

Verified high-level paths include:

- `ApiVersions v0` and `Metadata v1` roundtrips.
- Producer single-record, batch, and buffered sends.
- Direct topic-partition fetch using Fetch v2 response decoding.
- Classic consumer group join, sync, heartbeat, poll, and offset commit.

## Current Limits

- APIs are pre-`1.0` and can change between minor versions.
- TLS is feature-gated, currently uses the `rustls` ring crypto provider, and
  is verified for the broker roundtrip, producer, direct consumer, and consumer
  group smoke paths against Kafka `3.7.2`;
  SASL/PLAIN is verified for the `SaslPlaintext` broker roundtrip, producer,
  direct consumer, and consumer group smoke paths.
- Broker compatibility is verified against Kafka `3.7.2` only.
- Multi-broker clusters, leader failover, rack awareness, and partition
  expansion are not yet claimed.
- Idempotent producers, transactions, compression, admin APIs, and broad
  observability are not implemented yet.
- `acks=0` remains unsupported because the current request loop expects a broker
  response.

## Examples

Run examples from the repository with a local Kafka broker:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_TOPIC=kafrust-smoke \
cargo run -p kafrust --example producer_send
```

The smoke examples accept `KAFRUST_SECURITY_PROTOCOL`,
`KAFRUST_SASL_USERNAME`, and `KAFRUST_SASL_PASSWORD` so the same examples can be
run against plaintext, TLS, and SASL/PLAIN broker profiles.

Available examples include:

- `broker_roundtrip`
- `producer_send`
- `producer_send_batch`
- `producer_buffered`
- `consumer_fetch`
- `find_group_coordinator`
- `consumer_group_poll`

## Project Docs

- Repository: <https://github.com/TaeeunKil/kafrust>
- Roadmap: <https://github.com/TaeeunKil/kafrust/blob/main/docs/roadmap.md>
- Compatibility: <https://github.com/TaeeunKil/kafrust/blob/main/docs/compatibility.md>
- API stability: <https://github.com/TaeeunKil/kafrust/blob/main/docs/api-stability.md>
- Public API audit: <https://github.com/TaeeunKil/kafrust/blob/main/docs/public-api-audit.md>
- Project strategy: <https://github.com/TaeeunKil/kafrust/blob/main/docs/project-strategy.md>
