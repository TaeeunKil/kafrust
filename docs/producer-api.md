# Producer API Direction

The producer API should preserve Kafka concepts while using Rust builders and typed options.

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

let mut producer = ProducerConfig::new(["localhost:9092"])
    .client_id("orders-api")
    .request_timeout_ms(30_000)
    .max_retries(1)
    .acks(Acks::All)
    .build()
    .await?;

let record = ProducerRecord::to("orders")
    .key("order-123")
    .value("created")
    .header("source", "checkout");

let metadata = producer.send(record).await?;
println!("{}-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());
```

The public model intentionally keeps Kafka terms visible:

- topic
- partition
- key
- value
- headers
- timestamp
- acknowledgements
- record metadata

The first producer implementation should stay byte-first. Serialization adapters can be added later without forcing serde or another encoding choice into the core client.

Run the opt-in producer example against a local broker and an existing or auto-created topic:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example producer_send
```

Current implementation status:

- `ProducerConfig`, `ProducerRecord`, `Acks`, and `RecordMetadata` are public API types.
- `ProducerConfig::build` creates a producer backed by a Kafka broker connection.
- `Producer::send` performs metadata lookup, connects to the partition leader, negotiates Produce API support with `ApiVersions`, and chooses Produce v3 RecordBatch or Produce v2 MessageSet.
- `ProducerConfig::request_timeout_ms` controls the request timeout used for metadata and produce roundtrips.
- `ProducerConfig::max_retries` controls retry attempts for stale metadata, transient leader errors classified by `BrokerErrorKind`, request timeouts, and connection I/O failures.
- Producer metadata is cached by topic and refreshed when a retriable send failure invalidates that topic cache entry.
- Record headers are encoded with Kafka RecordBatch magic v2 through Produce API v3.
- When a broker only supports Produce API v2, records without headers fall back to the legacy MessageSet path.
- Records with headers return `Unsupported` if the target broker does not support Produce API v3.
- `acks=0` is rejected for now because the current client request loop expects a broker response.
- Stale metadata style produce errors are retried once after refreshing metadata.
