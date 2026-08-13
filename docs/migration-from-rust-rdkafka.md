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
kafrust = "0.2.18"
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
| `enable.auto.commit=true` | `.enable_auto_commit(true).auto_commit_interval_ms(...)` | Queues current assignment positions after successful polls and flushes them through a bounded background worker; defaults to `false` for explicit-commit compatibility. Classic and Kafka 4.3.1 KIP-848 paths are live-verified in [`31593984640`](https://github.com/TaeeunKil/kafrust/actions/runs/31593984640). |
| `enable.auto.commit=false` | omit `.enable_auto_commit(true)` | Use `commit_offsets` or `commit_record` plus `commit_queued_offsets` explicitly. |
| `auto.offset.reset=earliest` | `.offset_reset_policy(OffsetResetPolicy::Earliest)` | Used only when an assigned partition has no committed offset. |
| `auto.offset.reset=latest` | `.offset_reset_policy(OffsetResetPolicy::Latest)` | Resolves the current log end from the partition leader during group join. |
| `isolation.level` | `.isolation_level(IsolationLevel::ReadCommitted)` | Supported by direct and group consumers. |
| message headers | `ConsumerRecord::headers()` | Returns `ConsumerRecordHeader` values in wire order; `value()` is nullable because Kafka permits null header values. Legacy MessageSet records have no headers. |
| partition leader epoch | `ConsumerRecord::leader_epoch()` | Preserves the RecordBatch partition leader epoch; legacy MessageSet records return `-1`. |
| offset for leader epoch | `Consumer::offset_for_leader_epoch(...)` | Routes OffsetForLeaderEpoch v3 to the current partition leader and returns the broker-reported epoch end offset. It is a recovery primitive, not automatic consumer-position correction. |
| `max.poll.records` | `.max_poll_records(...)` | Bounds records returned by one poll. |
| `security.protocol` | `.security_protocol(SecurityProtocol::...)` | Prefer the SASL convenience methods for credentials. |
| `sasl.mechanism=PLAIN` | `.sasl_plain(username, password)` | Use with SASL_PLAINTEXT or SASL_TLS. |
| `sasl.mechanism=SCRAM-SHA-256` | `.sasl_scram_sha_256(username, password)` | Live verified over SASL_SSL. |
| `sasl.mechanism=SCRAM-SHA-512` | `.sasl_scram_sha_512(username, password)` | Live verified over SASL_SSL against Kafka 3.7.2. |
| `sasl.mechanism=OAUTHBEARER` | `.sasl_oauthbearer(token)`, `.sasl_oauthbearer_with_username(username, token)`, or the matching `*_provider` builder | Live verified against Kafka 3.7.2's built-in validator and a signed local OIDC/JWKS fixture, including Java client, static-token, and provider-backed paths in [`Live Kafka Smoke` OIDC job 31584760474`](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94078116567). OAUTHBEARER uses flexible `SaslAuthenticate v2`; external provider-specific behavior remains open. |
| statistics callback | `ClientMetrics::snapshot()` | kafrust exposes counters/gauges and tracing, not librdkafka statistics JSON. |

Configuration is validated before kafrust opens a broker connection. Invalid
timeouts, response or decode limits, fetch limits, empty group subscriptions,
zero commit or heartbeat intervals, and invalid transaction settings return the typed
`Error::InvalidConfiguration { field, reason }` variant. An empty bootstrap
server list remains `Error::MissingBootstrapServer`; a blank address inside a
non-empty list is a configuration error. This is intentionally different from
retryable broker or transport failures and should be handled as a startup
configuration failure in an adapter.
Call the matching `*.validate()` method on the client, producer, direct
consumer, or consumer-group config when an adapter needs to preflight startup
configuration before it starts its connection lifecycle.

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
commits the current next offsets for its assignments. `commit_record` plus
`commit_queued_offsets` provides an explicit, coalescing per-message commit
queue. `spawn_commit_worker` adds an opt-in bounded interval worker that shares
rejoin state and retries transport/coordinator-transition errors; partition
queue splitting is available through bounded `split_partition_queue` handles
for assigned direct or group partitions; a full queue returns
`Error::PartitionQueueFull` without advancing past the first rejected record.
`ConsumerGroupConfig::subscribe_pattern`
provides client-side regex
topic subscription by resolving the broker's Metadata v1 topic list before each
join or rejoin; it is not a broker-side subscription protocol. A synchronous
`RebalanceListener` now covers the initial join after-snapshot, before/after
rejoin snapshots, and KIP-848 assignment changes from foreground or background
heartbeats. The current group implementation supports the classic protocol
with range or round-robin assignment and an explicitly selected KIP-848
consumer protocol path.

For Kafka-style automatic commits, enable
`ConsumerGroupConfig::enable_auto_commit(true)`. The worker queues the current
assignment positions after each successful poll and observes rejoin state for
classic and KIP-848 groups. This is an at-least-once processing tradeoff: a
successful poll can be committed before application processing completes. The
worker is bounded and its terminal failure is returned by a later poll.

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
For log recovery, map an epoch-specific offset lookup to
`Consumer::offset_for_leader_epoch(topic, partition, current_leader_epoch,
leader_epoch)`. It returns `LeaderEpochOffset::leader_epoch()` and
`LeaderEpochOffset::end_offset()`; the application remains responsible for
deciding whether and how to seek after comparing the result with its local
fetch state.

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
If the EndTxn response is lost, kafrust returns the typed
`Error::TransactionOutcomeUnknown` and marks the producer `Defunct`; the caller
must discard it and reinitialize a producer with the same transactional ID
instead of assuming that the transaction aborted. Broader transaction
failure-injection matrices are not yet qualified.

## Admin

Both clients preserve partial outcomes for operations such as topic creation.
kafrust currently provides:

- cluster description and topic listing
- topic creation and deletion
- topic config description and incremental alteration
- consumer group description
- consumer group listing and deletion
- selected consumer group offset deletion
- consumer group committed offset listing and administrative alteration
- record deletion before partition offsets with leader-routed DeleteRecords
- active producer sequence inspection with leader-routed DescribeProducers
- transactional ID state inspection with coordinator-routed DescribeTransactions
- topic partition expansion with automatic or explicit replica assignment
- controller-routed partition reassignment and bounded in-progress status polling

ACL describe, create, and delete operations now have typed kafrust equivalents
through `AdminClient`. Their wire, mock-broker, and Kafka 3.7.2
StandardAuthorizer live paths are verified in `Live Kafka Smoke` run
`31457478358`, but a production migration still needs qualification against
the service principal's actual broker permissions.

Partition reassignment is implemented through typed `AdminClient` APIs and its
submission plus completion polling are live-verified on the Kafka 3.7.2
three-broker profile in `Live Kafka Smoke` run `31462962605`. The read-only
listing path also re-discovers the controller after a dropped request with
focused mock-broker coverage; qualify target permissions and broker-stop
behavior before production migration. SCRAM credential
administration is implemented and live-verified over Kafka 3.7.2 SASL_SSL in
`Live Kafka Smoke` run `31461980967`. Client quota describe/alter is implemented and live-verified
against Kafka 3.7.2 StandardAuthorizer in `Live Kafka Smoke` run
`31459874329`; production migration still requires the target principal's
actual broker permissions and quota policy.

See [Admin API](admin-api.md) for typed request and response examples.

`AdminClient::describe_cluster` and `AdminClient::list_topics` retry metadata
transport and timeout failures within the bounded retry budget. `list_topics`
also retries transient topic/partition metadata errors and still returns final
topic-level metadata errors as typed partial results rather than turning them
into a whole-operation failure. Both paths recovered from a
pre-transmission broker stop with `retries=1` on the Kafka 3.7.2 three-broker
profile in [`Live Kafka Smoke` run `31620595346`](https://github.com/TaeeunKil/kafrust/actions/runs/31620595346).

For the rust-rdkafka `AdminClient::fetch_offsets` and
`AdminClient::alter_consumer_group_offsets` workflows, use
`list_consumer_group_offsets` and `alter_consumer_group_offsets`. The kafrust
methods preserve per-topic and per-partition outcomes and use classic
OffsetFetch v2 and OffsetCommit v2 semantics. For a joined KIP-848 member,
pass `ConsumerGroup::metadata()` to
`list_consumer_group_offsets_with_member` and
`alter_consumer_group_offsets_with_member`; these use OffsetFetch v9 and
OffsetCommit v9 with the current member epoch and preserve stale-epoch errors.

## Capability Gate

| Workload requirement | Migration status |
| --- | --- |
| Tokio producer with keys, values, headers, batching, or buffered delivery | Candidate |
| Gzip, Snappy, LZ4, or Zstd production | Candidate on verified broker profiles |
| Idempotent producer | Candidate; broker-stop recovery is live-verified on the documented three-broker profile, but qualify target-specific ambiguous, fencing, and throughput failures |
| Direct assigned-partition consumer | Candidate |
| Classic range-assigned consumer group | Candidate with rebalance testing |
| Explicit per-message commit queue | Candidate; `commit_record` coalesces per-partition offsets and `commit_queued_offsets` flushes them under the current generation. The record-fetch and OffsetCommit path is live-verified across Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1, including KIP-848 on 4.3.1, in [`Live Kafka Smoke` run `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247). `spawn_commit_worker` adds bounded interval flush, retry, rejoin synchronization, and explicit shutdown; classic and KIP-848 worker paths are live-qualified in [`Live Kafka Smoke`, run `31563953123`](https://github.com/TaeeunKil/kafrust/actions/runs/31563953123). `split_partition_queue` now provides bounded direct/group per-partition delivery; focused tests and the Kafka 3.7.2 through 4.3.1 live examples in [`31566523106`](https://github.com/TaeeunKil/kafrust/actions/runs/31566523106), including the KIP-848 queue path in [`31566898432`](https://github.com/TaeeunKil/kafrust/actions/runs/31566898432), plus multi-broker coordinator/broker-stop failover in [`31567226615`](https://github.com/TaeeunKil/kafrust/actions/runs/31567226615), cover routing and full-queue position preservation |
| Regex topic subscription | Verified for initial and explicit rejoin two-topic assignment on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext smoke, including the corrected KIP-848 path on 4.3.1, in [`Live Kafka Smoke` run `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247); qualify topic-discovery permissions on secured target brokers |
| TLS, SASL/PLAIN, or SASL/SCRAM-SHA-256 | Candidate on documented profiles |
| SASL/OAUTHBEARER | Candidate on the Kafka 3.7.2 built-in-validator and signed local OIDC/JWKS fixture profiles, including static/provider-backed authentication and flexible `SaslAuthenticate v2` re-authentication; external provider-specific behavior, detached refresh workers, and production token/authorization policy remain open |
| Transactions and read-committed consumption | Alpha candidate; transaction coordinator broker-stop recovery is verified on the documented Kafka 3.7.2 three-broker SASL/PLAIN profile, and safe same-transactional-ID producer reinitialization after SCRAM coordinator failure is verified in run `31572745537`; transparent continuation and broader target-specific failure/throughput qualification remain |
| `acks=0` fire-and-forget | Verified on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 single-node plaintext smoke; qualify workload loss/error semantics |
| Non-Tokio runtime or synchronous client | Blocked |
| Custom partitioner | Candidate; `ProducerConfig::partitioner` covers records without an explicit partition across immediate, batch, and buffered paths |
| Rebalance callback | Candidate; `RebalanceListener` exposes synchronous initial-join, before/after rejoin, and foreground/background KIP-848 assignment snapshots; qualify target callback timing and cancellation behavior |
| `cooperative-sticky` assignor and consumer group protocol selection | Candidate on the verified Kafka 3.7.2 three-broker transfer and failure profiles; qualify target workload callbacks and timing |
| KIP-848 consumer group protocol (`ConsumerGroupHeartbeat`) | Candidate on the verified Kafka 4.3.1 PLAINTEXT profiles, including assignment, foreground/background heartbeat, rejoin, OffsetFetch v9, OffsetCommit v9, leave, and three-broker coordinator broker-stop recovery in [`Live Kafka Smoke` run `31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371); qualify target broker and broader failure workloads before production migration |
| Full librdkafka config passthrough | Blocked by design |
| ACL describe/create/delete with an authorizer-enabled broker | Verified on Kafka 3.7.2; DescribeAcls v1 also retries a dropped read request within the bounded AdminClient budget, with focused mock-broker coverage; qualify target permissions, policy, and broker-stop behavior |
| Client quota describe/alter | Verified on Kafka 3.7.2 StandardAuthorizer; DescribeClientQuotas v0 also retries a dropped read request within the bounded AdminClient budget, with focused mock-broker coverage; qualify target permissions, quota policy, and broker-stop behavior |
| SCRAM credential administration | Verified on Kafka 3.7.2 SASL_SSL; `DescribeUserScramCredentials v0` also retries a dropped read request within the bounded AdminClient budget with focused mock-broker coverage; controller-routed SCRAM alter retries only pre-transmission discovery; qualify target permissions, credential policy, and broker-stop behavior |
| Consumer-group offset listing and administrative alteration | Candidate; classic OffsetFetch v2 and OffsetCommit v2 are live-verified on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 in [`Live Kafka Smoke` run `31595485915`](https://github.com/TaeeunKil/kafrust/actions/runs/31595485915), with multi-broker, TLS, SASL_PLAINTEXT, and SASL_SSL/SCRAM coverage in [`31597505667`](https://github.com/TaeeunKil/kafrust/actions/runs/31597505667). KIP-848 member-aware OffsetFetch v9 and OffsetCommit v9 are live-verified on Kafka 4.3.1 PLAINTEXT single-node and three-broker profiles plus three-broker SASL_PLAINTEXT and SASL_SSL/SCRAM profiles in [`Live Kafka Smoke` run `31607006237`](https://github.com/TaeeunKil/kafrust/actions/runs/31607006237); qualify target authorization and broader member-failure workloads |
| Consumer-group inspection | Candidate; DescribeGroups v1 discovers each group's coordinator and preserves state, protocol, member identity, assignments, and per-group errors. In-flight coordinator-stop recovery with a pre-transmission gate and recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778); qualify target authorization and group lifecycle policy |
| Consumer-group listing | Candidate; ListGroups v1 queries every advertised broker, deduplicates and sorts group listings, and preserves protocol type, coordinator ID, throttle time, and broker errors. In-flight broker-stop recovery restarts the broker during the bounded reconnect loop and records `retries=7` on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31616181960`](https://github.com/TaeeunKil/kafrust/actions/runs/31616181960); qualify target authorization and behavior while a coordinator broker remains unavailable |
| Consumer-group committed offsets | Candidate; OffsetFetch v2 lists committed offsets and OffsetCommit v2 applies exact administrative offsets through the current group coordinator while preserving typed partition errors. In-flight coordinator-stop recovery for both paths uses a pre-transmission gate and recorded `retries=1` on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778); qualify target authorization and stale-commit policy |
| Topic configuration inspection | Candidate; DescribeConfigs v1 preserves typed resource and entry values, config sources, synonyms, sensitive/read-only flags, throttle time, and resource errors. A pre-transmission broker-stop recovery with recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31613935963`](https://github.com/TaeeunKil/kafrust/actions/runs/31613935963); qualify target authorization and broker policy |
| Record deletion | Candidate; DeleteRecords v1 is leader-routed, retries fixed-offset operations after transient transport or leader movement, and preserves low watermarks and per-partition errors. An in-flight leader-stop recovery with a pre-transmission gate and recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778); qualify retention policy and destructive-operation controls on the target broker |
| Active producer inspection | Candidate; DescribeProducers v0 is leader-routed and preserves producer IDs, epochs, sequences, transaction offsets, and per-partition errors. In-flight leader-stop recovery with a pre-transmission gate and recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778); qualify target authorization and operational alerting |
| Transaction inspection | Candidate; DescribeTransactions v0 discovers and groups IDs by transaction coordinator and preserves state, producer identity, timeout, and topic partitions. In-flight coordinator-stop recovery with a pre-transmission gate and recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778); qualify target authorization and coordinator-failure behavior |
| Active transaction listing | Candidate; `AdminClient::list_transactions` queries every metadata broker, aggregates transaction-state shards, supports state and producer-ID filters, and negotiates ListTransactions v1 with v0 fallback. The complete 17-job matrix passed the listing example on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plus the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31648660947`](https://github.com/TaeeunKil/kafrust/actions/runs/31648660947); qualify target authorization and duration-filter workloads |
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
