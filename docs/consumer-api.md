# Consumer API Direction

The direct consumer path provides explicit topic/partition fetch and local
assignment control. Consumer groups, commits, and rebalancing are available
through the separate alpha group API.

```rust
use kafrust::{ConsumerConfig, IsolationLevel};

let mut consumer = ConsumerConfig::new(["localhost:9092"])
    .client_id("orders-reader")
    .request_timeout_ms(30_000)
    .max_retries(1)
    .max_poll_records(500)
    .isolation_level(IsolationLevel::ReadCommitted)
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

The `consumer_failover` example fetches the same topic partition twice through
one consumer instance. It accepts `KAFRUST_PARTITION`, `KAFRUST_OFFSET`, and
`KAFRUST_FAILOVER_PAUSE_MS`, so orchestrated smoke workflows can stop the
current partition leader during the pause and verify metadata refresh plus
retry on the second fetch.

## Position Control

```rust
consumer.assign("orders", 0, 42);
assert_eq!(consumer.position("orders", 0), Some(42));

consumer.pause("orders", 0)?;
assert!(consumer.assignments()[0].is_paused());
consumer.seek("orders", 0, 10)?;
consumer.resume("orders", 0)?;
```

`position` returns the next offset that poll will fetch. `seek` changes that
in-memory position without committing it to Kafka. Paused assignments remain
visible and retain their position, but `poll` skips their fetch requests.
Seeking, pausing, or resuming a partition that is not assigned returns
`Error::UnassignedTopicPartition`.

## Partition Watermarks

```rust
let watermarks = consumer.fetch_watermarks("orders", 0).await?;
println!(
    "retained offsets: {}..{}",
    watermarks.low(),
    watermarks.high()
);
```

`fetch_watermarks` uses Metadata v1 to route two ListOffsets v1 requests to
the partition leader. `low` is the earliest retained offset and `high` is the
next offset after the current log end. The partition does not need to be
assigned. Retriable metadata, leader, connection, timeout, and broker errors
use the same bounded retry policy as fetch operations.

Current implementation status:

- `ConsumerConfig`, `Consumer`, and `ConsumerRecord` are public API types.
- `Consumer::fetch` accepts topic, partition, and offset directly.
- `Consumer::assign` and `Consumer::poll` provide a stream-like path that advances assigned partition offsets after records are returned.
- `Consumer::position`, `seek`, `pause`, and `resume` provide explicit local
  assignment control.
- `Consumer::fetch_watermarks` exposes leader-routed earliest and latest
  partition offsets.
- Fetch uses metadata lookup and partition leader routing.
- `ConsumerConfig::request_timeout_ms` controls the request timeout used for metadata and fetch roundtrips.
- `ConsumerConfig::security_protocol` stores the Kafka security protocol for consumer broker connections. `Plaintext` is the default transport; TLS requires the non-default `tls` crate feature; `tls_server_name(name)` overrides the certificate validation name when the bootstrap host differs from the broker certificate; `tls_root_certificate_der(bytes)` adds DER-encoded root certificates while keeping platform roots enabled; `sasl_plain(username, password)`, `sasl_scram_sha_256(username, password)`, and `sasl_scram_sha_512(username, password)` provide SASL credentials for `SaslPlaintext` or `SaslTls`.
- `ConsumerConfig::max_retries` controls retry attempts for stale metadata, unknown topic-partition entries in cached metadata, missing leader or broker metadata, transient fetch broker errors, request timeouts, and connection I/O failures.
- `ConsumerConfig::max_poll_records` limits how many records one `poll` call returns.
- `ConsumerConfig::isolation_level` selects `ReadUncommitted` (the default) or
  `ReadCommitted`. Read-committed Fetch v4 responses hide control records and
  records belonging to aborted transaction ranges. Poll assignment offsets
  still advance past records hidden by isolation filtering.
- Consumer metadata is cached by topic and refreshed when a retriable fetch failure invalidates that topic cache entry.
- Retryable metadata request I/O failures reconnect the consumer's bootstrap
  metadata client before the next metadata refresh attempt.
- Consumer poll and fetch operations emit `tracing` events with operational metadata, but not key or value payload contents.
- The decoder supports legacy MessageSet records, RecordBatch v2 records,
  transactional/control batch metadata, and partial trailing MessageSet entries
  in Fetch responses.
- Consumer groups and offset commits are available through the separate alpha `ConsumerGroupConfig` path.
