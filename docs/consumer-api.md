# Consumer API Direction

The first consumer path is direct topic/partition fetch. Consumer groups, commits, and rebalancing come later; this keeps the first milestone focused on Kafka wire compatibility and visible offsets.

```rust
use kafrust::ConsumerConfig;

let mut consumer = ConsumerConfig::new(["localhost:9092"])
    .client_id("orders-reader")
    .request_timeout_ms(30_000)
    .max_retries(1)
    .max_poll_records(500)
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
- `ConsumerConfig::security_protocol` stores the Kafka security protocol for consumer broker connections. `Plaintext` is the default transport; TLS requires the non-default `tls` crate feature; `tls_server_name(name)` overrides the certificate validation name when the bootstrap host differs from the broker certificate; `tls_root_certificate_der(bytes)` adds DER-encoded root certificates while keeping platform roots enabled; `sasl_plain(username, password)`, `sasl_scram_sha_256(username, password)`, and `sasl_scram_sha_512(username, password)` provide SASL credentials for `SaslPlaintext` or `SaslTls`.
- `ConsumerConfig::max_retries` controls retry attempts for stale metadata, unknown topic-partition entries in cached metadata, missing leader or broker metadata, transient fetch broker errors, request timeouts, and connection I/O failures.
- `ConsumerConfig::max_poll_records` limits how many records one `poll` call returns.
- Consumer metadata is cached by topic and refreshed when a retriable fetch failure invalidates that topic cache entry.
- Consumer poll and fetch operations emit `tracing` events with operational metadata, but not key or value payload contents.
- The decoder supports legacy MessageSet records, RecordBatch v2 records, and partial trailing MessageSet entries in Fetch responses.
- Consumer groups and offset commits are available through the separate alpha `ConsumerGroupConfig` path.
