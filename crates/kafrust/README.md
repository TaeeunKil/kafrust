# kafrust

[![Crates.io](https://img.shields.io/crates/v/kafrust.svg)](https://crates.io/crates/kafrust)
[![Docs.rs](https://docs.rs/kafrust/badge.svg)](https://docs.rs/kafrust)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)

A pure Rust Kafka client with no librdkafka or C toolchain dependency.

`kafrust` is the high-level client crate in the kafrust workspace. It provides
Tokio-based admin, producer, direct consumer, and alpha classic consumer group
APIs on top of the companion
[`kafrust-protocol`](https://docs.rs/kafrust-protocol) wire-format crate.

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

## Admin

```rust,no_run
use kafrust::{AdminClient, ClientConfig, CreateTopicsOptions, NewTopic};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let result = admin
    .create_topics(
        &[NewTopic::new("orders", 6, 3).config("cleanup.policy", "compact")],
        CreateTopicsOptions::new(),
    )
    .await?;

for topic in result.topics() {
    println!("{}: Kafka error {}", topic.name(), topic.error_code());
}
# Ok(())
# }
```

CreateTopics preserves Kafka's per-topic partial results and routes through
the active controller. See the repository's `docs/admin-api.md` for automatic
and manual replica assignment.

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
use kafrust::{Acks, Compression, ProducerConfig, ProducerRecord};

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
use kafrust::{Acks, Compression, ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-batch-producer")
        .acks(Acks::Leader)
        .compression(Compression::Gzip)
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

`Compression::Gzip`, `Compression::Snappy`, and `Compression::Lz4` use Produce
API v3 RecordBatch encoding. `Compression::Zstd` requires Produce API v7.
Snappy output uses Kafka-compatible Xerial framing; LZ4 and Zstd output use
their standard frames as expected by RecordBatch v2. Brokers without the
required Produce API version return an explicit `Unsupported` error when
compression is enabled.

## Transactional Producer

Use `transactional_id` to enable the alpha transactional producer:

```rust,no_run
use kafrust::{ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .transactional_id("orders-writer")
        .build()
        .await?;

    producer.begin_transaction()?;
    producer
        .send(ProducerRecord::to("orders").value("committed value"))
        .await?;
    producer.commit_transaction().await?;
    Ok(())
}
```

The high-level commit, abort, read-committed isolation, and transactional
consumer group offset paths are verified against Kafka `3.7.2` and `4.3.1`.

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
        .buffer_capacity(1024)
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

SASL credentials can be stored on the shared client configuration with
`sasl_plain(username, password)`, `sasl_scram_sha_256(username, password)`, or
`sasl_scram_sha_512(username, password)`. This does not change the selected
`SecurityProtocol`; choose `SaslPlaintext` or `SaslTls` separately. Credential
debug output redacts the password. The SASL/PLAIN `SaslPlaintext` broker
roundtrip, producer, direct consumer, and consumer group smoke paths are
verified against Kafka `3.7.2`; SASL/SCRAM-SHA-256 over `SaslTls` is verified
for those same paths. SASL/SCRAM-SHA-512 is implemented and covered by focused
tests, but its live broker profile is not claimed yet.

The default build does not include TLS dependencies. The current `tls` feature
uses the `rustls` ring crypto provider, which can require native build tooling
in some environments; this is not part of the default kafrust toolchain.

## Request Metrics

`ClientMetrics` provides lock-free operational counters shared by all broker
connections and buffered producers created from a configuration:

```rust,no_run
use kafrust::{ClientMetrics, ProducerConfig};

# async fn example() -> kafrust::Result<()> {
let metrics = ClientMetrics::new();
let producer = ProducerConfig::new(["localhost:9092"])
    .metrics(metrics.clone())
    .build()
    .await?;

// Send records with `producer`, then export a point-in-time snapshot.
let snapshot = metrics.snapshot();
println!(
    "requests={} failures={} broker_errors={} max_latency={:?}",
    snapshot.requests_started,
    snapshot.requests_failed,
    snapshot.broker_errors,
    snapshot.max_latency,
);
# Ok(())
# }
```

Snapshots include request success, failure, timeout, and cancellation counts,
non-zero Kafka error codes observed in decoded broker responses, high-level
operation retry attempts, request and response payload bytes, in-flight
requests, current and maximum outstanding buffered records, and total and
maximum latency. Successful produced records, topic-partition Produce chunks,
and records returned by consumer APIs are also counted. Retry attempts cover
producer sends, consumer fetches, metadata reconnects, and transactional
coordinator operations, plus automatic consumer-group rejoins. Individual
atomic fields are sampled independently, so a snapshot taken while requests
are changing is not a transactional view. Request metrics and `tracing` spans
contain operational metadata only; key, value, request, and response payload
contents are not recorded.

Debug-level operation spans cover immediate and buffered producer sends,
flush/close, batch sends, transaction completion and offset attachment,
direct-consumer poll/fetch, and consumer-group join, poll, heartbeat, and
offset commit. Their names use the `kafka.producer.*`, `kafka.consumer.*`, and
`kafka.consumer_group.*` prefixes. Broker `kafka.request` spans execute as
children, so subscribers can attribute wire latency to one user operation.

The buffered producer command queue defaults to 1024 records. Configure it with
`ProducerConfig::buffer_capacity`; values below one become one. When the queue
is full, `BufferedProducer::send` waits for capacity instead of allocating an
unbounded queue or dropping an accepted record.

## Decode Memory Limits

Broker response frame allocation is limited to `100 MiB` by default. Set
`max_response_bytes(bytes)` on `ClientConfig`, `ProducerConfig`,
`ConsumerConfig`, or `ConsumerGroupConfig` when a workload needs a different
limit. A broker frame length above the configured limit returns
`Error::ResponseTooLarge { size, max }` before allocating the response payload.

Kafka arrays are limited to `1,000,000` elements and an uncompressed fetched
record batch is limited to `64 MiB` by default. Configure these limits with
`max_decode_array_elements(elements)` and
`max_decompressed_record_bytes(bytes)` on the same configuration builders.
Declared arrays, record counts, and record headers are checked before reserving
their vectors. Gzip, Snappy, LZ4, and Zstd decoders enforce the batch limit and
return `protocol::Error::LimitExceeded { kind, actual, max }`.

The limit applies independently to each broker request. It must be large enough
for metadata, fetch, and other expected responses; setting it below a valid
response size fails that operation instead of partially decoding it.

## Compatibility

The `0.2.x` alpha line is verified against single-node Apache Kafka `3.7.2` and
`4.3.1` KRaft brokers over `PLAINTEXT`.

Verified high-level paths include:

- `ApiVersions v0` and `Metadata v1` roundtrips.
- Producer single-record, batch, and buffered sends.
- Direct topic-partition fetch using Fetch v4 response decoding.
- Classic consumer group join, sync, heartbeat, poll, and offset commit.

## Current Limits

- APIs are pre-`1.0` and can change between minor versions.
- TLS is feature-gated, currently uses the `rustls` ring crypto provider, and
  is verified for the broker roundtrip, producer, direct consumer, and consumer
  group smoke paths against Kafka `3.7.2`;
  SASL/PLAIN is verified for the `SaslPlaintext` broker roundtrip, producer,
  direct consumer, and consumer group smoke paths.
- SASL/SCRAM-SHA-256 is verified over `SaslTls` for the broker roundtrip,
  producer, direct consumer, and consumer group smoke paths; SASL/SCRAM-SHA-512
  is implemented and covered by focused tests, but its live broker profile is
  not claimed yet.
- Broker compatibility is verified against Kafka `3.7.2` and `4.3.1` for the
  single-node plaintext profile. Secured and multi-broker profiles currently
  remain verified against `3.7.2`.
- Multi-broker clusters, leader failover, rack awareness, and partition
  expansion are not yet claimed.
- Idempotent single-record, batch, and buffered sends are available through
  `ProducerConfig::enable_idempotence(true)`. Transactional immediate and batch
  sends support explicit begin, commit, and abort.
  `IsolationLevel::ReadCommitted` hides aborted transaction records for direct
  and group consumers, and current group assignments can be committed through
  `Producer::send_offsets_to_transaction`. Transactional buffered sends and
  admin APIs beyond CreateTopics are not implemented yet. Shared request,
  retry, broker-error, producer, consumer, batch, and buffered-queue metrics
  are available together with high-level operation and request spans.
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
`KAFRUST_SASL_USERNAME`, `KAFRUST_SASL_PASSWORD`, and
`KAFRUST_SASL_MECHANISM` so the same examples can be run against plaintext,
TLS, and SASL broker profiles. `KAFRUST_SASL_MECHANISM` defaults to `plain` and
also accepts `scram-sha-256` or `scram-sha-512`.
`KAFRUST_BOOTSTRAP_SERVERS` accepts Kafka's comma-separated bootstrap format for
multiple brokers, for example `localhost:19092,localhost:19093`.

Available examples include:

- `broker_roundtrip`
- `producer_send`
- `producer_send_batch`
- `producer_buffered`
- `producer_transactional`
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
