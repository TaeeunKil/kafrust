# Consumer API Direction

The first consumer path is direct topic/partition fetch. Consumer groups, commits, and rebalancing come later; this keeps the first milestone focused on Kafka wire compatibility and visible offsets.

```rust
use kafrust::ConsumerConfig;

let mut consumer = ConsumerConfig::new(["localhost:9092"])
    .client_id("orders-reader")
    .request_timeout_ms(30_000)
    .build()
    .await?;

consumer.assign("orders", 0, 0);
let records = consumer.poll().await?;
for record in records {
    println!("{}-{}@{}", record.topic(), record.partition(), record.offset());
}
```

Run the opt-in fetch example against a local broker:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example consumer_fetch
```

Current implementation status:

- `ConsumerConfig`, `Consumer`, and `ConsumerRecord` are public API types.
- `Consumer::fetch` accepts topic, partition, and offset directly.
- `Consumer::assign` and `Consumer::poll` provide a stream-like path that advances assigned partition offsets after records are returned.
- Fetch uses metadata lookup and partition leader routing.
- `ConsumerConfig::request_timeout_ms` controls the request timeout used for metadata and fetch roundtrips.
- The decoder supports legacy MessageSet records and RecordBatch v2 records.
- Consumer groups and offset commits are intentionally out of scope for the MVP.
