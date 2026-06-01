# Consumer API Direction

The first consumer path is direct topic/partition fetch. Consumer groups, commits, and rebalancing come later; this keeps the first milestone focused on Kafka wire compatibility and visible offsets.

```rust
use kafrust::ConsumerConfig;

let mut consumer = ConsumerConfig::new(["localhost:9092"])
    .client_id("orders-reader")
    .build()
    .await?;

let records = consumer.fetch("orders", 0, 0).await?;
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
- Fetch uses metadata lookup and partition leader routing.
- The first decoder supports legacy MessageSet records used by the current producer path.
- Consumer groups and offset commits are intentionally out of scope for the MVP.
