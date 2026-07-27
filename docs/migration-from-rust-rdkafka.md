# Migrating from rust-rdkafka

This guide maps common `rust-rdkafka` 0.39 application patterns to kafrust
0.2.x. It is a staged migration guide, not a drop-in compatibility claim.
`rust-rdkafka` wraps librdkafka and has substantially broader production
coverage. kafrust is a Tokio-based pure Rust client with a smaller, typed
configuration surface.

Reference APIs:

- [`rust-rdkafka` crate documentation](https://docs.rs/rdkafka/latest/rdkafka/)
- [`FutureProducer`](https://docs.rs/rdkafka/latest/rdkafka/producer/struct.FutureProducer.html)
- [`StreamConsumer`](https://docs.rs/rdkafka/latest/rdkafka/consumer/struct.StreamConsumer.html)
- [`rust-rdkafka` AdminClient](https://docs.rs/rdkafka/latest/rdkafka/admin/struct.AdminClient.html)
- [kafrust compatibility claim](compatibility.md)

## Dependency and Runtime

Replace the dependency:

```toml
[dependencies]
kafrust = "0.2.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Enable `kafrust`'s `tls` feature for TLS or SASL over TLS. kafrust currently
requires Tokio. It does not provide rust-rdkafka's alternate async-runtime
adapter or synchronous polling clients.

## Configuration Mapping

rust-rdkafka accepts librdkafka configuration keys as strings. kafrust exposes
the currently supported settings through typed builders.

| rust-rdkafka key or concept | kafrust API | Notes |
| --- | --- | --- |
| `bootstrap.servers` | `ProducerConfig::new`, `ConsumerConfig::new`, `ConsumerGroupConfig::new`, `ClientConfig::new` | Pass an iterator of broker addresses. |
| `client.id` | `.client_id(...)` | Available on all high-level configs. |
| `request.timeout.ms` | `.request_timeout_ms(...)` | Applies to kafrust request roundtrips. |
| `acks=1` | `.acks(Acks::Leader)` | Default producer behavior. |
| `acks=all` | `.acks(Acks::All)` | Required automatically by idempotence. |
| `acks=0` | No equivalent | kafrust requires a broker response. |
| `compression.type` | `.compression(Compression::{Gzip, Snappy, Lz4, Zstd})` | Codec support is feature-complete for the verified producer path. |
| `enable.idempotence` | `.enable_idempotence(true)` | Initializes producer identity and partition sequences. |
| `transactional.id` | `.transactional_id(...)` | Enables the alpha transactional path. |
| `group.id` | `ConsumerGroupConfig::new(brokers, group_id)` | Group ID is a required typed argument. |
| `auto.offset.reset=earliest` | `.offset_reset_policy(OffsetResetPolicy::Earliest)` | Used only when an assigned partition has no committed offset. |
| `auto.offset.reset=latest` | `.offset_reset_policy(OffsetResetPolicy::Latest)` | Resolves the current log end from the partition leader during group join. |
| `isolation.level` | `.isolation_level(IsolationLevel::ReadCommitted)` | Supported by direct and group consumers. |
| `max.poll.records` | `.max_poll_records(...)` | Bounds records returned by one poll. |
| `security.protocol` | `.security_protocol(SecurityProtocol::...)` | Prefer the SASL convenience methods for credentials. |
| `sasl.mechanism=PLAIN` | `.sasl_plain(username, password)` | Use with SASL_PLAINTEXT or SASL_TLS. |
| `sasl.mechanism=SCRAM-SHA-256` | `.sasl_scram_sha_256(username, password)` | Live verified over SASL_SSL. |
| `sasl.mechanism=SCRAM-SHA-512` | `.sasl_scram_sha_512(username, password)` | Implemented, not yet live-profile qualified. |
| statistics callback | `ClientMetrics::snapshot()` | kafrust exposes counters/gauges and tracing, not librdkafka statistics JSON. |

Do not silently discard an old configuration map. Classify every key as
mapped, intentionally removed, or blocking the migration.

## Producer

Typical rust-rdkafka:

```rust,ignore
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

let producer: FutureProducer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .set("client.id", "orders-api")
    .set("enable.idempotence", "true")
    .create()?;

let delivery = producer
    .send(
        FutureRecord::to("orders")
            .key("order-123")
            .payload("created"),
        Timeout::Never,
    )
    .await?;
```

kafrust:

```rust,ignore
use kafrust::{ProducerConfig, ProducerRecord};

let mut producer = ProducerConfig::new(["localhost:9092"])
    .client_id("orders-api")
    .enable_idempotence(true)
    .build()
    .await?;

let delivery = producer
    .send(
        ProducerRecord::to("orders")
            .key("order-123")
            .value("created"),
    )
    .await?;
```

`FutureProducer` is internally polled and cloneable. kafrust's direct
`Producer` is mutable and request-oriented. For concurrent enqueueing, use
`BufferedProducer`; its delivery handles provide per-record completion.

Use `send_batch_report` when one call spans topics or partitions and the
application must preserve per-record failures. Do not translate it to a single
all-or-nothing error.

## Consumer Group

Typical rust-rdkafka:

```rust,ignore
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};

let consumer: StreamConsumer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .set("group.id", "orders-service")
    .set("enable.auto.commit", "false")
    .create()?;
consumer.subscribe(&["orders"])?;

let message = consumer.recv().await?;
process(message.payload())?;
consumer.commit_message(&message, CommitMode::Async)?;
```

kafrust:

```rust,ignore
use kafrust::{ConsumerGroupConfig, OffsetResetPolicy};

let mut group = ConsumerGroupConfig::new(
    ["localhost:9092"],
    "orders-service",
)
.client_id("orders-reader")
.offset_reset_policy(OffsetResetPolicy::Earliest)
.subscribe("orders")
.join()
.await?;

let records = group.poll().await?;
for record in &records {
    process(record.value())?;
}
group.commit_offsets().await?;
```

The semantic difference matters: kafrust currently returns a bounded batch and
commits the current next offsets for its assignments. It does not expose
rust-rdkafka's asynchronous per-message commit queue, rebalance callbacks,
partition queue splitting, or regex subscription. The current group
implementation uses the classic protocol with range or round-robin assignment.

For processing that can approach the session timeout, use
`spawn_heartbeat_task` with `poll_with_heartbeat`. The task is explicit and
must be stopped and observed by the application.

## Direct Consumer

Use kafrust's direct consumer when the old application manually assigned
partitions and offsets:

```rust,ignore
use kafrust::ConsumerConfig;

let mut consumer = ConsumerConfig::new(["localhost:9092"])
    .max_poll_records(500)
    .build()
    .await?;
consumer.assign("orders", 0, 42);

for record in consumer.poll().await? {
    println!("{}-{}@{}", record.topic(), record.partition(), record.offset());
}
```

There is no current equivalent for every rust-rdkafka seek, pause, resume, or
watermark-query workflow. Treat those calls as migration blockers until an
explicit kafrust API and compatibility test exist.

## Transactions

Map `init_transactions` plus `begin_transaction` to a producer configured with
`transactional_id`; kafrust initializes the producer during `build`:

```rust,ignore
let mut producer = ProducerConfig::new(["localhost:9092"])
    .transactional_id("orders-transformer")
    .build()
    .await?;

producer.begin_transaction()?;
producer
    .send(ProducerRecord::to("output").value("processed"))
    .await?;
producer
    .send_group_offsets_to_transaction(
        &group.metadata(),
        group.assignments(),
    )
    .await?;
producer.commit_transaction().await?;
```

The current transaction path is live verified for commit, abort, and atomic
group-offset commit. Transactional buffered production and multi-broker
transaction failure injection are not yet qualified.

## Admin

Both clients preserve partial outcomes for operations such as topic creation.
kafrust currently provides:

- cluster description and topic listing
- topic creation and deletion
- topic config description and incremental alteration
- consumer group description
- selected consumer group offset deletion

Migration blockers include rust-rdkafka admin operations with no current
kafrust equivalent, including partition creation, group deletion, ACLs,
quotas, replica reassignment, and SCRAM credential administration.

See [Admin API](admin-api.md) for typed request and response examples.

## Capability Gate

| Workload requirement | Migration status |
| --- | --- |
| Tokio producer with keys, values, headers, batching, or buffered delivery | Candidate |
| Gzip, Snappy, LZ4, or Zstd production | Candidate on verified broker profiles |
| Idempotent producer | Candidate with workload-specific failure testing |
| Direct assigned-partition consumer | Candidate |
| Classic range-assigned consumer group | Candidate with rebalance testing |
| TLS, SASL/PLAIN, or SASL/SCRAM-SHA-256 | Candidate on documented profiles |
| Transactions and read-committed consumption | Alpha candidate |
| `acks=0` fire-and-forget | Blocked |
| Non-Tokio runtime or synchronous client | Blocked |
| Custom partitioner or rebalance callback | Blocked |
| Cooperative assignor or consumer group protocol selection | Blocked |
| Full librdkafka config passthrough | Blocked by design |
| Broad admin, ACL, quota, or credential management | Blocked |

"Candidate" means the API exists and relevant project tests pass. It does not
replace workload-specific qualification against the target brokers, security
settings, message sizes, failure modes, and throughput.

## Rollout Procedure

1. Inventory all rust-rdkafka client types, config keys, callbacks, and admin
   calls in the service.
2. Stop if any required behavior is marked blocked above.
3. Build a kafrust adapter behind the service's existing messaging interface;
   do not change business logic in the same patch.
4. Run both clients against a disposable topic and compare topic, partition,
   key, value, headers, timestamp, offsets, commit behavior, and errors.
5. Run the target broker/security profile with leader and coordinator failure
   injection.
6. Compare throughput, tail latency, memory growth, retry counts, and duplicate
   behavior under the service's real record distribution.
7. Canary kafrust with rollback to rust-rdkafka available.
8. Remove rust-rdkafka only after the canary covers restart, rebalance, broker
   loss, deploy, and credential rotation scenarios.

Record the exact kafrust version and the dated compatibility workflow used for
the decision. Re-run qualification for every pre-1.0 kafrust upgrade.
