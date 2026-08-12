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
kafrust = "0.2.4"
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
| `acks=0` | `.acks(Acks::None)` | Writes and flushes without waiting for a response; returned offsets are `-1` and broker acceptance is not confirmed. |
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
| `sasl.mechanism=SCRAM-SHA-512` | `.sasl_scram_sha_512(username, password)` | Live verified over SASL_SSL against Kafka 3.7.2. |
| `sasl.mechanism=OAUTHBEARER` | `.sasl_oauthbearer(token)`, `.sasl_oauthbearer_with_username(username, token)`, or the matching `*_provider` builder | Live verified only against Kafka 3.7.2's built-in unsecured validator; production OAuth/OIDC provider and signed JWT/JWKS policy are not claimed. |
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

The producer keeps authenticated leader connections and negotiated ApiVersions
capabilities for sequential sends to the same broker. A transport or protocol
failure evicts that connection before retry, so applications do not need to
implement connection recycling around ordinary producer retries.

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
implementation supports the classic protocol with range or round-robin
assignment and an explicitly selected KIP-848 consumer protocol path.

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

kafrust exposes `position`, `seek`, `pause`, and `resume` on both direct and
group consumers. These calls operate on current in-memory assignments; seek
does not commit an offset. Map rust-rdkafka's `fetch_watermarks` to kafrust's
async `fetch_watermarks`; kafrust uses the configured request timeout instead
of accepting a timeout argument on each call and returns
values through `PartitionWatermarks::low()` and `PartitionWatermarks::high()`.

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

The current transaction path supports immediate and buffered commit, abort, and
atomic group-offset commit. A buffered commit flushes accepted records before
EndTxn and refuses to commit after a delivery failure. Transaction coordinator
broker-stop recovery with `read_committed` verification is qualified on the
Kafka 3.7.2 three-broker SASL/PLAIN profile in
[`Live Kafka Smoke` run `31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594).
Broader transaction failure-injection matrices are not yet qualified.

## Admin

Both clients preserve partial outcomes for operations such as topic creation.
kafrust currently provides:

- cluster description and topic listing
- topic creation and deletion
- topic config description and incremental alteration
- consumer group description
- consumer group listing and deletion
- selected consumer group offset deletion
- topic partition expansion with automatic or explicit replica assignment
- controller-routed partition reassignment and bounded in-progress status polling

ACL describe, create, and delete operations now have typed kafrust equivalents
through `AdminClient`. Their wire, mock-broker, and Kafka 3.7.2
StandardAuthorizer live paths are verified in `Live Kafka Smoke` run
`31457478358`, but a production migration still needs qualification against
the service principal's actual broker permissions.

Partition reassignment is implemented through typed `AdminClient` APIs and its
submission plus completion polling are live-verified on the Kafka 3.7.2
three-broker profile in `Live Kafka Smoke` run `31462962605`. SCRAM credential
administration is implemented and live-verified over Kafka 3.7.2 SASL_SSL in
`Live Kafka Smoke` run `31461980967`. Client quota describe/alter is implemented and live-verified
against Kafka 3.7.2 StandardAuthorizer in `Live Kafka Smoke` run
`31459874329`; production migration still requires the target principal's
actual broker permissions and quota policy.

See [Admin API](admin-api.md) for typed request and response examples.

## Capability Gate

| Workload requirement | Migration status |
| --- | --- |
| Tokio producer with keys, values, headers, batching, or buffered delivery | Candidate |
| Gzip, Snappy, LZ4, or Zstd production | Candidate on verified broker profiles |
| Idempotent producer | Candidate; broker-stop recovery is live-verified on the documented three-broker profile, but qualify target-specific ambiguous, fencing, and throughput failures |
| Direct assigned-partition consumer | Candidate |
| Classic range-assigned consumer group | Candidate with rebalance testing |
| TLS, SASL/PLAIN, or SASL/SCRAM-SHA-256 | Candidate on documented profiles |
| SASL/OAUTHBEARER | Candidate only for the documented Kafka 3.7.2 unsecured-validator smoke; async token-provider callbacks exist, but qualify the production OAuth/OIDC provider, token policy, and authorization behavior |
| Transactions and read-committed consumption | Alpha candidate; transaction coordinator broker-stop recovery is verified on the documented Kafka 3.7.2 three-broker SASL/PLAIN profile, but broader target-specific failure and throughput qualification remains |
| `acks=0` fire-and-forget | Verified on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 single-node plaintext smoke; qualify workload loss/error semantics |
| Non-Tokio runtime or synchronous client | Blocked |
| Custom partitioner | Candidate; `ProducerConfig::partitioner` covers records without an explicit partition across immediate, batch, and buffered paths |
| Rebalance callback | Blocked; use explicit poll/rejoin lifecycle until callback ordering and cancellation semantics are stabilized |
| `cooperative-sticky` assignor and consumer group protocol selection | Candidate on the verified Kafka 3.7.2 three-broker transfer and failure profiles; qualify target workload callbacks and timing |
| KIP-848 consumer group protocol (`ConsumerGroupHeartbeat`) | Candidate on the verified Kafka 4.3.1 PLAINTEXT profiles, including assignment, foreground/background heartbeat, rejoin, OffsetFetch v9, OffsetCommit v9, leave, and three-broker coordinator broker-stop recovery in [`Live Kafka Smoke` run `31555896968`](https://github.com/TaeeunKil/kafrust/actions/runs/31555896968); qualify target broker and broader failure workloads before production migration |
| Full librdkafka config passthrough | Blocked by design |
| ACL describe/create/delete with an authorizer-enabled broker | Verified on Kafka 3.7.2; qualify target permissions and policy |
| Client quota describe/alter | Verified on Kafka 3.7.2 StandardAuthorizer; qualify target permissions and quota policy |
| SCRAM credential administration | Verified on Kafka 3.7.2 SASL_SSL; qualify target permissions and credential policy |
| Replica reassignment | Verified on Kafka 3.7.2 three-broker smoke; qualify target broker permissions and failure behavior |

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
