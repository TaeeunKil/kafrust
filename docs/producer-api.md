# Producer API Direction

The producer API should preserve Kafka concepts while using Rust builders and typed options.

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

let mut producer = ProducerConfig::new(["localhost:9092"])
    .client_id("orders-api")
    .request_timeout_ms(30_000)
    .max_retries(1)
    .max_records_per_batch(500)
    .max_batch_bytes(64 * 1024)
    .acks(Acks::All)
    .build()
    .await?;

let record = ProducerRecord::to("orders")
    .key("order-123")
    .value("created")
    .header("source", "checkout");

let metadata = producer.send(record).await?;
println!("{}-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());

let batch_metadata = producer
    .send_batch([
        ProducerRecord::to("orders").key("order-124").value("created"),
        ProducerRecord::to("orders").key("order-125").value("created"),
    ])
    .await?;
for metadata in batch_metadata {
    println!("{}-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());
}

let batch_report = producer
    .send_batch_report([
        ProducerRecord::to("orders").key("order-126").value("created"),
        ProducerRecord::to("payments").key("payment-456").value("authorized"),
    ])
    .await?;
for outcome in batch_report.records() {
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
```

Enable duplicate-safe retries for the current single-record producer path:

```rust
let mut producer = ProducerConfig::new(["localhost:9092"])
    .enable_idempotence(true)
    .build()
    .await?;
```

Use a transactional ID to opt into the alpha transactional producer path:

```rust
let mut producer = ProducerConfig::new(["localhost:9092"])
    .transactional_id("orders-writer")
    .transaction_timeout_ms(60_000)
    .build()
    .await?;

producer.begin_transaction()?;
producer
    .send(ProducerRecord::to("orders").key("order-126").value("created"))
    .await?;
producer.commit_transaction().await?;
```

Transactional producers require an active transaction before `send` or
`send_batch`. `commit_transaction` and `abort_transaction` complete the current
transaction through the Kafka transaction coordinator. After polling a
consumer group, pass its current assignments to the producer before committing:

```rust
let group_metadata = group.metadata();
let offsets = group.assignments().to_vec();

producer.begin_transaction()?;
producer
    .send(ProducerRecord::to("output").value("processed"))
    .await?;
producer
    .send_group_offsets_to_transaction(&group_metadata, &offsets)
    .await?;
producer.commit_transaction().await?;
```

The group offsets become visible only with the transaction commit.
`IsolationLevel::ReadCommitted` is available on consumer configurations.

The buffered path exposes the same transaction boundary as asynchronous worker
commands:

```rust
let mut producer = ProducerConfig::new(["localhost:9092"])
    .transactional_id("orders-buffered-writer")
    .linger_ms(10)
    .build_buffered()
    .await?;

producer.begin_transaction().await?;
let delivery = producer
    .send(ProducerRecord::to("orders").value("created"))
    .await?;
producer.commit_transaction().await?;
let metadata = delivery.await?;
```

Buffered commit flushes every accepted record before EndTxn. A delivery failure
prevents commit and leaves the transaction active for an explicit abort.
Buffered abort also completes pending delivery handles before EndTxn. Closing
an active buffered transaction is rejected until it is committed or aborted.

The public model intentionally keeps Kafka terms visible:

- topic
- partition
- key
- value
- headers
- timestamp
- acknowledgements
- record metadata

When a record has no explicit partition, kafrust hashes a present key with
Kafka's Java-compatible Murmur2 algorithm and selects from the topic's sorted
partition IDs. This keeps the same serialized key on the same partition as the
standard Kafka clients. Records without either an explicit partition or key
use batch-sticky round-robin routing: all keyless records for one topic in a
single batch or buffered flush use one partition, and the next completed send
or batch rotates to the next available partition. Retries retain the selected
partition until the operation reaches a terminal result.

The first producer implementation should stay byte-first. Serialization adapters can be added later without forcing serde or another encoding choice into the core client.

Buffered producer and linger behavior is planned as a separate opt-in path. See [Producer Buffering And Linger Design](producer-buffering.md) for the intended implementation direction.

Run the opt-in producer example against a local broker and an existing or auto-created topic:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example producer_send
```

Run `producer_keyless` against a topic with multiple partitions to observe
batch-sticky rotation. `KAFRUST_EXPECT_PARTITIONS` optionally accepts a
comma-separated expected sequence for compatibility checks.

Run the buffered producer smoke example to enqueue multiple records, await delivery handles, and fetch the produced records back:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example producer_buffered
```

The `producer_send_batch` example also accepts `KAFRUST_BATCH_PARTITIONS` as a
comma-separated list of explicit partition indexes. This is useful for
multi-broker smoke profiles that need one batch call to route records through
multiple partition leaders.
The `producer_send` example accepts `KAFRUST_PARTITION` to send one record to
an explicit partition. Set `KAFRUST_ENABLE_IDEMPOTENCE=true` to initialize a
producer ID and send the record with producer epoch and partition sequence
metadata.
Set `KAFRUST_EXPECT_PARTITION` to make the example fail unless explicit or
key-derived routing produces the expected partition.
The `producer_failover` example sends two records through the same producer
instance to one explicit partition. It accepts `KAFRUST_PARTITION` and
`KAFRUST_FAILOVER_PAUSE_MS`, so orchestrated smoke workflows can stop the
current partition leader during the pause and verify metadata refresh plus
retry on the second send.

Current implementation status:

- `ProducerConfig`, `ProducerRecord`, `Acks`, `Compression`, and `RecordMetadata` are public API types.
- `ProducerConfig::build` creates a producer backed by a Kafka broker connection.
- `ProducerConfig::enable_idempotence(true)` selects `acks=all`, requires at
  least one retry, initializes a producer ID through `InitProducerId v0`, and
  tracks the next sequence independently per topic partition. The current
  alpha applies this behavior to single-record, batch, and buffered sends.
  Batch sequence reservations remain stable across request and partial-record
  retries, and advance the acknowledged partition sequence only after a
  successful Produce response.
- An idempotent `DUPLICATE_SEQUENCE_NUMBER` response is treated as successful
  delivery because it identifies an already appended retry. Its
  `RecordMetadata` uses offset `-1` and timestamp `None` because Kafka does not
  return the original append metadata. `OUT_OF_ORDER_SEQUENCE_NUMBER`,
  `INVALID_PRODUCER_EPOCH`, and `PRODUCER_FENCED` make that producer instance
  defunct; the failing call and future send calls return the fatal broker code.
  An injected-broker regression test covers an ambiguous connection loss,
  byte-identical retry, duplicate response, and single sequence advancement.
- `ProducerConfig::transactional_id` enables idempotence and initializes the
  producer through the Kafka transaction coordinator. `begin_transaction`,
  `commit_transaction`, and `abort_transaction` expose the transaction
  lifecycle; transactional sends register partitions before Produce. The
  high-level commit and abort path is verified against Kafka 3.7.2 and 4.3.1.
- `BufferedProducer` supports async `begin_transaction`, `commit_transaction`,
  `abort_transaction`, and generation-fenced
  `send_group_offsets_to_transaction`. Transaction boundaries are serialized
  with queued sends by the background worker.
- `Producer::send_group_offsets_to_transaction` adds a consumer group to the
  active transaction and commits the supplied `ConsumerAssignment` next
  offsets through generation-fenced `TxnOffsetCommit v3`. The
  consume-transform-produce path is verified
  against Kafka 3.7.2 and 4.3.1.
- `Producer::send` performs metadata lookup, connects to the partition leader, negotiates Produce API support with `ApiVersions`, and chooses Produce v7 for Zstd, Produce v3 RecordBatch for other RecordBatch features, or Produce v2 MessageSet.
- `ProducerConfig::request_timeout_ms` controls the request timeout used for metadata and produce roundtrips.
- `ProducerConfig::security_protocol` stores the Kafka security protocol for producer broker connections. `Plaintext` is the default transport; TLS requires the non-default `tls` crate feature; `tls_server_name(name)` overrides the certificate validation name when the bootstrap host differs from the broker certificate; `tls_root_certificate_der(bytes)` adds DER-encoded root certificates while keeping platform roots enabled; `sasl_plain(username, password)`, `sasl_scram_sha_256(username, password)`, and `sasl_scram_sha_512(username, password)` provide SASL credentials for `SaslPlaintext` or `SaslTls`.
- `ProducerConfig::max_retries` controls retry attempts for stale metadata, unknown topic-partition entries in cached metadata, missing leader or broker metadata, transient leader errors classified by `BrokerErrorKind`, request timeouts, and connection I/O failures.
- `ProducerConfig::max_records_per_batch` limits how many records are sent in one Produce request for a topic-partition group. Values below 1 are treated as 1.
- `ProducerConfig::max_batch_bytes` limits the encoded Kafka record-set bytes sent in one Produce request for a topic-partition group. Values below 1 are treated as 1, and an oversized single record is still sent by itself.
- `ProducerConfig::linger_ms` stores the configured linger duration for the opt-in buffered producer path. The immediate `send` and `send_batch` APIs do not wait on linger.
- `ProducerConfig::buffer_capacity` sets the bounded buffered-producer command capacity, defaulting to 1024. Values below 1 are treated as 1; `BufferedProducer::send` applies async backpressure while the queue is full.
- `ProducerConfig::compression` stores the configured producer record batch
  compression policy. `Compression::None` is the default.
  `Compression::Gzip`, `Compression::Snappy`, `Compression::Lz4`, and
  `Compression::Zstd` encode compressed RecordBatch payloads. Gzip, Snappy, and
  LZ4 require Produce API v3; Zstd requires Produce API v7. Missing broker
  support returns `Unsupported`. Snappy output uses Kafka-compatible Xerial
  framing; LZ4 and Zstd output use their standard frames as expected by
  RecordBatch v2.
- `ProducerConfig::build_buffered` creates a `BufferedProducer` with configurable bounded enqueue, per-record `ProducerDelivery` handles, `flush`, `close`, and `is_closed`.
- `BufferedProducer::send` queues records for the buffered path, and `flush` or `close` sends accepted records through the existing batch Produce path before completing delivery handles from per-record outcomes.
- `BufferedProducer` automatically flushes queued records when a topic and explicit-partition buffer reaches `max_records_per_batch` or `max_batch_bytes`, or when the oldest queued record reaches `linger_ms`. `linger_ms(0)` schedules a flush without intentional delay.
- Producer metadata is cached by topic and refreshed when a retriable send failure invalidates that topic cache entry.
- Retryable metadata request I/O failures reconnect the producer's bootstrap
  metadata client before the next metadata refresh attempt.
- Producer send operations emit `tracing` events with operational metadata, but not key or value payload contents.
- `Producer::send_batch` accepts multiple `ProducerRecord` values, groups them by topic, partition, and leader, sends each group in one Produce request, and returns `RecordMetadata` in input order.
- `Producer::send_batch_report` returns per-record success or failure outcomes in input order, so broker Produce response errors can be inspected without losing partial successes.
- `Producer::send_batch` remains the convenience API and returns the first per-record Produce response error as `Err(Error)`.
- Record headers are encoded with Kafka RecordBatch magic v2 through Produce API v3.
- When a broker only supports Produce API v2, records without headers fall back to the legacy MessageSet path.
- Records with headers return `Unsupported` if the target broker does not support Produce API v3.
- `acks=0` is rejected for now because the current client request loop expects a broker response.
- Stale metadata style produce errors are retried once after refreshing metadata.
- Batch sends retry request-level retriable failures according to `ProducerConfig::max_retries`.
- Retryable broker Produce response failures retry only the failed input records; records that already succeeded are not sent again by that batch call.
- The `producer_buffered` example and `Live Kafka Smoke` workflow cover buffered enqueue, delivery handles, and fetch-back verification against Kafka 3.7.2.
