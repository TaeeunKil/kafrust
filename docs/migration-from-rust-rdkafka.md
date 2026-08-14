# Migrating from rust-rdkafka

This guide maps common `rust-rdkafka` 0.39 application patterns to kafrust
0.3.x. It is a staged migration guide, not a drop-in compatibility claim.
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
kafrust = "0.3.0"
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
| `auto.offset.reset=earliest` | `.offset_reset_policy(OffsetResetPolicy::Earliest)` | Resolves the retained low watermark when an assigned partition has no committed offset, and recovers a committed assignment whose offset is no longer retained. |
| `auto.offset.reset=latest` | `.offset_reset_policy(OffsetResetPolicy::Latest)` | Resolves the current log end from the partition leader during group join and recovers a committed out-of-range assignment from that end. |
| `isolation.level` | `.isolation_level(IsolationLevel::ReadCommitted)` | Supported by direct and group consumers. |
| message headers | `ConsumerRecord::headers()` | Returns `ConsumerRecordHeader` values in wire order; `value()` is nullable because Kafka permits null header values. Legacy MessageSet records have no headers. |
| partition leader epoch | `ConsumerRecord::leader_epoch()` | Preserves the RecordBatch partition leader epoch; legacy MessageSet records return `-1`. |
| offset for leader epoch | `Consumer::offset_for_leader_epoch(...)` | Routes OffsetForLeaderEpoch v3 to the current partition leader and returns the broker-reported epoch end offset. Assigned `Consumer::poll` also uses it for automatic direct-consumer truncation recovery when Metadata v12 is available; group rebalance recovery remains separate. |
| `max.poll.records` | `.max_poll_records(...)` | Bounds records returned by one poll. |
| `security.protocol` | `.security_protocol(SecurityProtocol::...)` | Prefer the SASL convenience methods for credentials. |
| `sasl.mechanism=PLAIN` | `.sasl_plain(username, password)` | Use with SASL_PLAINTEXT or SASL_TLS. |
| `sasl.mechanism=SCRAM-SHA-256` | `.sasl_scram_sha_256(username, password)` | Live verified over SASL_SSL. |
| `sasl.mechanism=SCRAM-SHA-512` | `.sasl_scram_sha_512(username, password)` | Live verified over SASL_SSL against Kafka 3.7.2. |
| `sasl.mechanism=OAUTHBEARER` | `.sasl_oauthbearer(token)`, `.sasl_oauthbearer_with_username(username, token)`, or the matching `*_provider` builder | Live verified against Kafka 3.7.2's built-in validator and a signed local OIDC/JWKS fixture, including Java client, static-token, and provider-backed paths in [`Live Kafka Smoke` OIDC job 31584760474`](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94078116567). OAUTHBEARER uses flexible `SaslAuthenticate v2`; external provider-specific behavior remains open. |
| delegation token lifecycle | `AdminClient::create_delegation_token`, `describe_delegation_tokens`, `renew_delegation_token`, `expire_delegation_token` | Requires an authenticated SASL or mutual-TLS channel and a broker-side delegation-token secret. The HMAC is credential material and is redacted from `Debug` and tracing. The lifecycle example is opt-in; qualify it against the target broker and authorization policy. |
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
with range, round-robin, eager sticky, or cooperative-sticky assignment and an
explicitly selected KIP-848 consumer protocol path. Select
`ConsumerGroupAssignmentStrategy::Sticky` for Kafka's eager sticky behavior;
it is distinct from cooperative-sticky because transfers are applied in the
same SyncGroup assignment.

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
The published `kafrust 0.2.28` artifact also passed an external
abort-then-commit plus `ReadCommitted` smoke on Kafka 3.7.2 classic, Kafka
4.3.1 KIP-848, and Kafka 3.7.2 SASL_SSL/SCRAM in
[`Published Crate Smoke` run `31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869),
which also passed the published group offset commit/restore flow across all
seven profiles.
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

The published `kafrust 0.2.28` artifact passed a fresh external Admin lifecycle
in [`Published Crate Smoke` run `31731934027`](https://github.com/TaeeunKil/kafrust/actions/runs/31731934027):
it created a topic with `NewTopic`, verified it through `list_topics` and
`describe_topic_configs`, and deleted it through `AdminClient` across the
documented protocol, security, and compression profiles. This is representative
runtime coverage; qualify the remaining Admin APIs, permissions, and failure
policies against the target deployment.

ACL describe, create, and delete operations now have typed kafrust equivalents
through `AdminClient`. Their wire, mock-broker, and Kafka 3.7.2
StandardAuthorizer live paths are verified in `Live Kafka Smoke` run
`31457478358`, but a production migration still needs qualification against
the service principal's actual broker permissions.

The published `0.2.28` restricted-authorization gate adds external evidence:
Kafka 3.7.2 and 4.3.1 `StandardAuthorizer` profiles authenticated a non-superuser
over SASL_SSL/SCRAM-SHA-256, allowed the configured cluster/topic/group paths,
and preserved denied topic-config, topic-create, and topic-delete outcomes in
runs [`31741997691`](https://github.com/TaeeunKil/kafrust/actions/runs/31741997691)
and [`31742115305`](https://github.com/TaeeunKil/kafrust/actions/runs/31742115305).
This qualifies one real permission policy; compare the target principal's ACLs
and all required Admin calls before migration.

The follow-up published mutation gate passed the same two broker versions in
runs [`31742788549`](https://github.com/TaeeunKil/kafrust/actions/runs/31742788549)
and [`31742924984`](https://github.com/TaeeunKil/kafrust/actions/runs/31742924984).
It verified allowed and denied `IncrementalAlterConfigs`, group commit and
Admin OffsetFetch, Admin OffsetCommit v2 reset, and consumption from the reset
position after group rejoin. This is representative authorization evidence;
it does not establish parity for every Admin mutation or failure policy.

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
`delete_consumer_groups` and `delete_consumer_group_offsets` retry transient
coordinator responses through fresh discovery within the bounded Admin retry
budget. A transport failure after either mutation is transmitted remains
single-attempt and returns `Error::AdminMutationOutcomeUnknown { operation }`;
callers must reconcile the broker-side outcome before replaying an ambiguous
request. The same typed boundary applies to the other non-idempotent Admin
mutations, including topic, ACL, quota, SCRAM, delegation-token, config,
leader-election, reassignment, log-dir, and offset writes. `DeleteRecords` is
the documented idempotent exception and preserves its retry path, but its final
partition results still require inspection.

The current-source CreateTopics response-drop gate demonstrates the migration
behavior against Kafka 3.7.2 and 4.3.1: the request can be applied even when
the call returns `AdminMutationOutcomeUnknown`, so an adapter must reconcile
before issuing a second create request. A matching DeleteTopics gate confirms
the same rule for deletion and requires a list operation to verify that the
topic is gone. These are evidence for those two operations only; do not
generalize them to every Admin mutation without matching operation-level
qualification. CreateTopics passed in
[`31770443512`](https://github.com/TaeeunKil/kafrust/actions/runs/31770443512)
and [`31770443484`](https://github.com/TaeeunKil/kafrust/actions/runs/31770443484);
DeleteTopics passed in
[`31771419625`](https://github.com/TaeeunKil/kafrust/actions/runs/31771419625)
and [`31771419124`](https://github.com/TaeeunKil/kafrust/actions/runs/31771419124).
CreatePartitions passed in
[`31771635710`](https://github.com/TaeeunKil/kafrust/actions/runs/31771635710)
and [`31771636082`](https://github.com/TaeeunKil/kafrust/actions/runs/31771636082),
with reconciliation through the topic's partition count.
IncrementalAlterConfigs passed in
[`31771864914`](https://github.com/TaeeunKil/kafrust/actions/runs/31771864914)
and [`31771865024`](https://github.com/TaeeunKil/kafrust/actions/runs/31771865024),
with reconciliation through DescribeConfigs.
Classic AlterConfigs passed in
[`31772009182`](https://github.com/TaeeunKil/kafrust/actions/runs/31772009182)
and [`31772008771`](https://github.com/TaeeunKil/kafrust/actions/runs/31772008771),
also with DescribeConfigs reconciliation.
CreateAcls response loss passed with DescribeAcls reconciliation in
[`31772403290`](https://github.com/TaeeunKil/kafrust/actions/runs/31772403290)
and [`31772403077`](https://github.com/TaeeunKil/kafrust/actions/runs/31772403077),
and DeleteAcls response loss passed with absence reconciliation in
[`31772470761`](https://github.com/TaeeunKil/kafrust/actions/runs/31772470761)
and [`31772470590`](https://github.com/TaeeunKil/kafrust/actions/runs/31772470590).
The ACL gates used Kafka StandardAuthorizer with an explicit test superuser;
repeat authorization and policy checks for the target principal before
migrating a production workload.
AlterClientQuotas response loss also passed with DescribeClientQuotas
reconciliation in
[`31772731756`](https://github.com/TaeeunKil/kafrust/actions/runs/31772731756)
and [`31772731963`](https://github.com/TaeeunKil/kafrust/actions/runs/31772731963).
The quota gate verifies one user quota only; repeat it with the target
principal, quota keys, and broker policy before migration.
AlterUserScramCredentials response loss passed with
DescribeUserScramCredentials reconciliation in
[`31772992221`](https://github.com/TaeeunKil/kafrust/actions/runs/31772992221)
and [`31772992381`](https://github.com/TaeeunKil/kafrust/actions/runs/31772992381).
The gate used a deterministic test credential and verifies its mechanism and
iteration count; repeat credential policy, authentication, and authorization
checks for the production migration.

## Capability Gate

| Workload requirement | Migration status |
| --- | --- |
| Tokio producer with keys, values, headers, batching, or buffered delivery | Candidate |
| Gzip, Snappy, LZ4, or Zstd production | Candidate on verified broker profiles; the published `0.2.28` artifact completed external producer/fetch, transactional `ReadCommitted`, and group offset-restore roundtrips for all four codecs against Kafka 3.7.2 in [`Published Crate Smoke` run `31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869). Qualify target-specific throughput, batch sizes, and failure behavior |
| Published producer/consumer performance baseline | Candidate for baseline comparison; the published `0.2.28` artifact completed fresh external 10,000-record, 1-KiB, batch-size-200 runs with no compression and Zstd on Kafka 3.7.2 and 4.3.1 in [`Published Performance Smoke` run `31744206188`](https://github.com/TaeeunKil/kafrust/actions/runs/31744206188). The run measured 43.7k-48.9k producer records/s and 210.6k-268.3k consumer records/s with zero retries and zero final queue gauges; compare against rust-rdkafka using the target service's own payloads, batching, concurrency, and broker topology |
| Direct published rust-rdkafka performance comparison | Candidate for the recorded profile only; a fresh external project compared published `kafrust 0.2.28` with `rust-rdkafka 0.39.0` on Kafka 4.3.1 using 2,000 1-KiB records in batches of 100. Kafrust measured 51,834 producer records/s and 129,875 consumer records/s; rust-rdkafka measured 48,452 producer records/s and 252,306 consumer records/s in [`Published rust-rdkafka Comparison`, run `31753172293`](https://github.com/TaeeunKil/kafrust/actions/runs/31753172293). This is not API/feature parity or a production SLO; repeat with the target service's payloads, concurrency, compression, and broker topology |
| Current-source rust-rdkafka performance comparison | A fresh external project compared repository-source kafrust commit `1528862` with `rust-rdkafka 0.39.0` on Kafka 4.3.1 using 20,000 1-KiB records in batches of 200. Kafrust measured 49,161.76 producer records/s and 226,166.96 consumer records/s; rust-rdkafka measured 84,235.49 producer records/s and 220,147.27 consumer records/s in [`Published rust-rdkafka Comparison`, run `31767095380`](https://github.com/TaeeunKil/kafrust/actions/runs/31767095380). This is a current-source baseline only, not API/feature parity or a production SLO; repeat with the target service's payloads, concurrency, compression, and broker topology |
| Published kafrust versus rust-rdkafka performance comparison | A fresh external project compared crates.io `kafrust 0.2.30` with `rust-rdkafka 0.39.0` on Kafka 4.3.1 using 20,000 1-KiB records in batches of 200. Kafrust measured 51,834.49 producer records/s and 233,242 consumer records/s; rust-rdkafka measured 87,752.37 producer records/s and 176,675.91 consumer records/s in [`Published rust-rdkafka Comparison`, run `31768138519`](https://github.com/TaeeunKil/kafrust/actions/runs/31768138519). This confirms the published artifact for one baseline only, not API/feature parity or a production SLO; repeat with the target service's payloads, concurrency, compression, and broker topology |
| Published broker-restart soak | Candidate for the tested recovery profile; the published `0.2.28` artifact ran a fresh external 120-second Kafka 4.3.1 workload through a ten-second broker outage, reconciled 7,229,000 records, observed 173 operation errors and 1,210 retries, and ended with zero in-flight/buffered records in [`Published Soak Smoke` run `31744827441`](https://github.com/TaeeunKil/kafrust/actions/runs/31744827441). Qualify the target service's multi-broker topology, concurrent clients, deploy/restart behavior, and rollback path |
| Published `0.2.30` broker-restart soak | Candidate for the tested single-node recovery profile; the published `0.2.30` artifact ran a fresh external 300-second Kafka 4.3.1 workload through a ten-second broker outage, reconciled 21,597,600 records, observed 180 operation errors and 1,243 retries, and ended with zero in-flight/buffered records in [`Published Soak Smoke` run `31768319413`](https://github.com/TaeeunKil/kafrust/actions/runs/31768319413). Qualify the target service's secured/multi-broker topology, concurrent clients, deploy/restart behavior, and rollback path |
| Published multi-broker broker-restart soak | Candidate for the tested plaintext topology; the published `0.2.28` artifact ran a fresh external 120-second Kafka 4.3.1 three-broker workload with three replicated partitions through a ten-second broker outage, reconciled 4,918,800 records, and ended with zero in-flight/buffered records in [`Published Multi-Broker Soak Smoke` run `31746182158`](https://github.com/TaeeunKil/kafrust/actions/runs/31746182158). Qualify the target service's security profile, simultaneous broker-loss behavior, concurrent clients, deploy/restart behavior, and rollback path |
| Published `0.2.30` multi-broker broker-restart soak | Candidate for the tested plaintext three-broker topology; the published `0.2.30` artifact ran a fresh external 120-second Kafka 4.3.1 workload with three replicated partitions through a ten-second broker outage, reconciled 4,404,900 records, observed 1 operation error and 1,021 retries, and ended with zero in-flight/buffered records in [`Published Multi-Broker Soak Smoke` run `31768320764`](https://github.com/TaeeunKil/kafrust/actions/runs/31768320764). Qualify the target service's secured/simultaneous-loss behavior, concurrent clients, deploy/restart behavior, and rollback path |
| Published secured multi-broker broker-restart soak | Candidate for the tested SASL_SSL/SCRAM profile; the published `0.2.28` artifact with `tls` ran a fresh external 120-second Kafka 4.3.1 three-broker workload with three replicated partitions through a ten-second broker outage, reconciled 2,288,700 records, and ended with zero in-flight/buffered records in [`Published Secure Multi-Broker Soak Smoke` run `31747389166`](https://github.com/TaeeunKil/kafrust/actions/runs/31747389166). Qualify the target service's simultaneous broker-loss behavior, concurrent clients, deploy/restart behavior, credential rotation, and rollback path |
| Published simultaneous broker-loss soak | Candidate for the tested plaintext topology; the published `0.2.28` artifact passed fresh external Kafka 3.7.2 and 4.3.1 three-broker workloads through simultaneous ten-second outages of brokers 1 and 2, reconciled 4,620,200 and 4,423,200 records respectively, and ended with zero in-flight/buffered records in [`31748860976`](https://github.com/TaeeunKil/kafrust/actions/runs/31748860976) and [`31748293446`](https://github.com/TaeeunKil/kafrust/actions/runs/31748293446). Qualify the target service's secured simultaneous-loss behavior, concurrent clients, deploy/restart behavior, and rollback path |
| Idempotent producer | Candidate; broker-stop recovery is live-verified on the documented three-broker profile, and the published `0.2.28` artifact produced through a replacement leader in [`Published Multi-Broker Smoke` run `31735177161`](https://github.com/TaeeunKil/kafrust/actions/runs/31735177161), authenticated `SASL_SSL/SCRAM-SHA-256` profiles in [`31738997447`](https://github.com/TaeeunKil/kafrust/actions/runs/31738997447) and [`31739154764`](https://github.com/TaeeunKil/kafrust/actions/runs/31739154764), and secured coordinator-plus-leader faults in [`31739763944`](https://github.com/TaeeunKil/kafrust/actions/runs/31739763944) and [`31739927915`](https://github.com/TaeeunKil/kafrust/actions/runs/31739927915), but qualify target-specific ambiguous, fencing, and throughput failures |
| Direct assigned-partition consumer | Candidate |
| Classic range-assigned consumer group | Candidate with rebalance testing; published `0.2.28` two-member ownership and record delivery passed on Kafka 3.7.2 in [`Published Group Rebalance Smoke` run `31736939236`](https://github.com/TaeeunKil/kafrust/actions/runs/31736939236), and the authenticated SASL_SSL/SCRAM-SHA-256 path passed in [`31740436499`](https://github.com/TaeeunKil/kafrust/actions/runs/31740436499) |
| Classic eager sticky consumer group | Candidate; previous-assignment user data and focused balance/transfer tests are implemented; qualify target workload rebalance timing |
| Explicit per-message commit queue | Candidate; `commit_record` coalesces per-partition offsets and `commit_queued_offsets` flushes them under the current generation. The record-fetch and OffsetCommit path is live-verified across Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1, including KIP-848 on 4.3.1, in [`Live Kafka Smoke` run `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247). The published `0.2.28` artifact additionally committed a record, left, rejoined with the same group ID, and resumed at the committed offset without replay across classic, KIP-848, SASL_SSL/SCRAM, and codec profiles in [`Published Crate Smoke` run `31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869). The same published artifact preserved the committed group position across a three-broker leader stop and replacement-leader record in [`Published Multi-Broker Smoke` run `31735177161`](https://github.com/TaeeunKil/kafrust/actions/runs/31735177161). `spawn_commit_worker` adds bounded interval flush, retry, rejoin synchronization, and explicit shutdown; classic and KIP-848 worker paths are live-qualified in [`Live Kafka Smoke`, run `31563953123`](https://github.com/TaeeunKil/kafrust/actions/runs/31563953123). `split_partition_queue` now provides bounded direct/group per-partition delivery; focused tests and the Kafka 3.7.2 through 4.3.1 live examples in [`31566523106`](https://github.com/TaeeunKil/kafrust/actions/runs/31566523106), including the KIP-848 queue path in [`31566898432`](https://github.com/TaeeunKil/kafrust/actions/runs/31566898432), plus multi-broker coordinator/broker-stop failover in [`31567226615`](https://github.com/TaeeunKil/kafrust/actions/runs/31567226615), cover routing and full-queue position preservation |
| Regex topic subscription | Verified for initial and explicit rejoin two-topic assignment on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext smoke, including the corrected KIP-848 path on 4.3.1, in [`Live Kafka Smoke` run `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247); qualify topic-discovery permissions on secured target brokers |
| TLS, SASL/PLAIN, or SASL/SCRAM-SHA-256 | Candidate on documented profiles; published `0.2.28` external projects passed three-broker `SASL_SSL/SCRAM-SHA-256` leader failover for Kafka 3.7.2 classic and Kafka 4.3.1 KIP-848 in [`31738997447`](https://github.com/TaeeunKil/kafrust/actions/runs/31738997447) and [`31739154764`](https://github.com/TaeeunKil/kafrust/actions/runs/31739154764), coordinator-plus-leader faults in [`31739763944`](https://github.com/TaeeunKil/kafrust/actions/runs/31739763944) and [`31739927915`](https://github.com/TaeeunKil/kafrust/actions/runs/31739927915), and two sequential leader failures in [`31743322062`](https://github.com/TaeeunKil/kafrust/actions/runs/31743322062) and [`31743497415`](https://github.com/TaeeunKil/kafrust/actions/runs/31743497415); restricted StandardAuthorizer permission behavior also passed in [`31741997691`](https://github.com/TaeeunKil/kafrust/actions/runs/31741997691) and [`31742115305`](https://github.com/TaeeunKil/kafrust/actions/runs/31742115305); qualify target ACLs, unclean-election behavior, and broader security/workload matrices |
| SASL/OAUTHBEARER | Candidate on the Kafka 3.7.2 built-in-validator and signed local OIDC/JWKS fixture profiles, including static/provider-backed authentication and flexible `SaslAuthenticate v2` re-authentication; external provider-specific behavior, detached refresh workers, and production token/authorization policy remain open |
| Transactions and read-committed consumption | Alpha candidate; transaction coordinator broker-stop recovery is verified on the documented Kafka 3.7.2 three-broker SASL/PLAIN profile, safe same-transactional-ID producer reinitialization after SCRAM coordinator failure is verified in run `31572745537`, the ambiguous EndTxn plus `read_committed` reconciliation gate passed in [`31708995196`](https://github.com/TaeeunKil/kafrust/actions/runs/31708995196/job/94476744970), and the published `0.2.28` artifact passed abort/commit plus `ReadCommitted` runtime checks across classic, KIP-848, SASL_SSL/SCRAM, and codec profiles in [`31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869). Published coordinator-stop recovery passed on Kafka 3.7.2 plaintext in [`31738090052`](https://github.com/TaeeunKil/kafrust/actions/runs/31738090052) and authenticated SASL_SSL/SCRAM profiles for Kafka 3.7.2 and 4.3.1 in [`31741012713`](https://github.com/TaeeunKil/kafrust/actions/runs/31741012713) and [`31741137784`](https://github.com/TaeeunKil/kafrust/actions/runs/31741137784). The caller must discard a producer after `TransactionOutcomeUnknown`; transparent continuation and broader target-specific failure/throughput qualification remain |
| `acks=0` fire-and-forget | Verified on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 single-node plaintext smoke; qualify workload loss/error semantics |
| Non-Tokio runtime or synchronous client | Blocked |
| Custom partitioner | Candidate; `ProducerConfig::partitioner` covers records without an explicit partition across immediate, batch, and buffered paths |
| Rebalance callback | Candidate; `RebalanceListener` exposes synchronous initial-join, before/after rejoin, and foreground/background KIP-848 assignment snapshots; qualify target callback timing and cancellation behavior |
| Classic `sticky` assignor | Candidate; Subscription v0 previous-assignment user data accepts Kafka's v0/v1 schemas and applies eager transfers; qualify target workload callbacks and timing |
| `cooperative-sticky` assignor and consumer group protocol selection | Candidate on the verified Kafka 3.7.2 three-broker transfer and failure profiles; qualify target workload callbacks and timing |
| KIP-848 consumer group protocol (`ConsumerGroupHeartbeat`) | Candidate on the verified Kafka 4.3.1 PLAINTEXT profiles, including assignment, foreground/background heartbeat, rejoin, OffsetFetch v9, OffsetCommit v9, leave, and three-broker coordinator broker-stop recovery in [`Live Kafka Smoke` run `31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371). Published `0.2.28` verifies same-group offset restore after an immediate leave/rejoin in [`Published Crate Smoke` run `31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869), published three-broker leader failover in [`Published Multi-Broker Smoke` run `31735762087`](https://github.com/TaeeunKil/kafrust/actions/runs/31735762087), two-member ownership plus record delivery in [`Published Group Rebalance Smoke` run `31736362411`](https://github.com/TaeeunKil/kafrust/actions/runs/31736362411), authenticated three-broker leader failover in [`Published Secure Multi-Broker Smoke` run `31739154764`](https://github.com/TaeeunKil/kafrust/actions/runs/31739154764), authenticated coordinator-plus-leader recovery in [`31739927915`](https://github.com/TaeeunKil/kafrust/actions/runs/31739927915), and authenticated two-member ownership plus record delivery in [`31740567979`](https://github.com/TaeeunKil/kafrust/actions/runs/31740567979); qualify target broker, broader assignor, and failure workloads before production migration |
| Full librdkafka config passthrough | Blocked by design |
| ACL describe/create/delete with an authorizer-enabled broker | Verified on Kafka 3.7.2; DescribeAcls v1 also retries a dropped read request within the bounded AdminClient budget, with focused mock-broker coverage. Published `0.2.28` restricted-user StandardAuthorizer paths passed on Kafka 3.7.2 and 4.3.1, including allowed cluster/topic/group operations, denied topic-config/create/delete outcomes, and allowed/denied config mutation in [`31741997691`](https://github.com/TaeeunKil/kafrust/actions/runs/31741997691), [`31742115305`](https://github.com/TaeeunKil/kafrust/actions/runs/31742115305), [`31742788549`](https://github.com/TaeeunKil/kafrust/actions/runs/31742788549), and [`31742924984`](https://github.com/TaeeunKil/kafrust/actions/runs/31742924984); qualify target permissions, policy, every Admin API, and broker-stop behavior |
| Client quota describe/alter | Verified on Kafka 3.7.2 StandardAuthorizer; DescribeClientQuotas v0 also retries a dropped read request within the bounded AdminClient budget, with focused mock-broker coverage; qualify target permissions, quota policy, and broker-stop behavior |
| SCRAM credential administration | Verified on Kafka 3.7.2 SASL_SSL; `DescribeUserScramCredentials v0` also retries a dropped read request within the bounded AdminClient budget with focused mock-broker coverage; controller-routed SCRAM alter retries only pre-transmission discovery; qualify target permissions, credential policy, and broker-stop behavior |
| Delegation token lifecycle | Implemented with negotiated Create/Describe v1-v3 and Renew/Expire v1-v2, flexible encoding, controller routing, and HMAC redaction; focused wire and mock-broker coverage passes; current-source CreateDelegationToken response-drop reconciliation passed over authenticated SASL/PLAIN on Kafka 3.7.2 and 4.3.1 in [`31773884142`](https://github.com/TaeeunKil/kafrust/actions/runs/31773884142) and [`31773883953`](https://github.com/TaeeunKil/kafrust/actions/runs/31773883953); target authorization, secret distribution, and the remaining lifecycle/failure workloads remain required |
| Consumer-group offset listing and administrative alteration | Candidate; classic OffsetFetch v2 and OffsetCommit v2 are live-verified on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 in [`Live Kafka Smoke` run `31595485915`](https://github.com/TaeeunKil/kafrust/actions/runs/31595485915), with multi-broker, TLS, SASL_PLAINTEXT, and SASL_SSL/SCRAM coverage in [`31597505667`](https://github.com/TaeeunKil/kafrust/actions/runs/31597505667). KIP-848 member-aware OffsetFetch v9 and OffsetCommit v9 are live-verified on Kafka 4.3.1 PLAINTEXT single-node and three-broker profiles plus three-broker SASL_PLAINTEXT and SASL_SSL/SCRAM profiles in [`Live Kafka Smoke` run `31607006237`](https://github.com/TaeeunKil/kafrust/actions/runs/31607006237). A member-aware OffsetCommit v9 response-drop gate also passed with a joined member and OffsetFetch/CLI reconciliation in [`31777089953`](https://github.com/TaeeunKil/kafrust/actions/runs/31777089953); qualify target authorization, member-aware offset deletion, and broader member-failure workloads |
| Consumer-group inspection | Candidate; DescribeGroups v1 discovers each group's coordinator and preserves state, protocol, member identity, assignments, and per-group errors. In-flight coordinator-stop recovery with a pre-transmission gate and recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778), and active-group inspection is verified from the published `0.2.28` artifact across classic and KIP-848 profiles in [`Published Crate Smoke` run `31737581786`](https://github.com/TaeeunKil/kafrust/actions/runs/31737581786); qualify target authorization and group lifecycle policy |
| Consumer-group listing | Candidate; ListGroups v1 queries every advertised broker, deduplicates and sorts group listings, and preserves protocol type, coordinator ID, throttle time, and broker errors. In-flight broker-stop recovery restarts the broker during the bounded reconnect loop and records `retries=7` on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31616181960`](https://github.com/TaeeunKil/kafrust/actions/runs/31616181960), and active-group listing is verified from the published `0.2.28` artifact across classic and KIP-848 profiles in [`Published Crate Smoke` run `31737581786`](https://github.com/TaeeunKil/kafrust/actions/runs/31737581786); qualify target authorization and behavior while a coordinator broker remains unavailable |
| Consumer-group deletion and selected offset deletion | Candidate; DeleteGroups v1 and OffsetDelete v0 preserve group and partition outcomes and retry transient coordinator responses with fresh discovery in focused mock-broker tests. Current-source OffsetDelete response-drop reconciliation passed after establishing an offset on Kafka 3.7.2 and 4.3.1 in [`31774990676`](https://github.com/TaeeunKil/kafrust/actions/runs/31774990676) and [`31774990554`](https://github.com/TaeeunKil/kafrust/actions/runs/31774990554); the transmitted delete is classified as unknown rather than replayed and OffsetFetch confirms removal. Current-source DeleteGroups response-drop reconciliation also passed after making the group visible through ListGroups on Kafka 3.7.2 and 4.3.1 in [`31775333815`](https://github.com/TaeeunKil/kafrust/actions/runs/31775333815) and [`31775333736`](https://github.com/TaeeunKil/kafrust/actions/runs/31775333736); the transmitted delete is classified as unknown rather than replayed and ListGroups confirms absence. Active-member behavior, member-aware failure, and target authorization qualification remain open |
| Consumer-group committed offsets | Candidate; OffsetFetch v2 lists committed offsets and OffsetCommit v2 applies exact administrative offsets through the current group coordinator while preserving typed partition errors. In-flight coordinator-stop recovery for both paths uses a pre-transmission gate and recorded `retries=1` on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778). Current-source OffsetCommit response-drop reconciliation passed after coordinator readiness on Kafka 3.7.2 and 4.3.1 in [`31774729128`](https://github.com/TaeeunKil/kafrust/actions/runs/31774729128) and [`31774729263`](https://github.com/TaeeunKil/kafrust/actions/runs/31774729263); the transmitted mutation is classified as unknown rather than replayed. KIP-848 member-aware OffsetFetch v9 is also verified from the published `0.2.28` artifact across classic/KIP-848, security, and codec profiles in [`Published Crate Smoke` run `31737581786`](https://github.com/TaeeunKil/kafrust/actions/runs/31737581786). Restricted-user published Admin OffsetFetch/OffsetCommit v2 reset and post-rejoin consumption passed on Kafka 3.7.2 and 4.3.1 in [`31742788549`](https://github.com/TaeeunKil/kafrust/actions/runs/31742788549) and [`31742924984`](https://github.com/TaeeunKil/kafrust/actions/runs/31742924984); qualify target authorization, stale-commit behavior, active-member behavior, and broader member-aware failure policy |
| Topic configuration inspection | Candidate; DescribeConfigs v1 preserves typed resource and entry values, config sources, synonyms, sensitive/read-only flags, throttle time, and resource errors. A pre-transmission broker-stop recovery with recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31613935963`](https://github.com/TaeeunKil/kafrust/actions/runs/31613935963), and the published `0.2.28` external Admin lifecycle reads `cleanup.policy` through `describe_topic_configs` in [`Published Crate Smoke` run `31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869). Restricted-user allow/deny behavior for topic configs passed on Kafka 3.7.2 and 4.3.1 in [`31741997691`](https://github.com/TaeeunKil/kafrust/actions/runs/31741997691) and [`31742115305`](https://github.com/TaeeunKil/kafrust/actions/runs/31742115305); qualify target authorization and broker policy |
| Topic configuration alteration | Candidate; classic AlterConfigs v1 exposes complete-map `TopicConfigUpdate` replacement and incremental AlterConfigs v0 remains available. Focused wire and injected-broker tests pass, and the plaintext lifecycle is live-verified on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plus the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke`, run `31669906872`](https://github.com/TaeeunKil/kafrust/actions/runs/31669906872). Published restricted-user `IncrementalAlterConfigs` allow/deny behavior passed on Kafka 3.7.2 and 4.3.1 in [`31742788549`](https://github.com/TaeeunKil/kafrust/actions/runs/31742788549) and [`31742924984`](https://github.com/TaeeunKil/kafrust/actions/runs/31742924984); qualify secured target authorization and post-transmission mutation behavior |
| Record deletion | Candidate; DeleteRecords v1 is leader-routed, retries fixed-offset operations after transient transport or leader movement, and preserves low watermarks and per-partition errors. An in-flight leader-stop recovery with a pre-transmission gate and recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778); qualify retention policy and destructive-operation controls on the target broker |
| Active producer inspection | Candidate; DescribeProducers v0 is leader-routed and preserves producer IDs, epochs, sequences, transaction offsets, and per-partition errors. In-flight leader-stop recovery with a pre-transmission gate and recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778); qualify target authorization and operational alerting |
| Transaction inspection | Candidate; DescribeTransactions v0 discovers and groups IDs by transaction coordinator and preserves state, producer identity, timeout, and topic partitions. In-flight coordinator-stop recovery with a pre-transmission gate and recorded `retries=1` is live-verified on the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778); qualify target authorization and coordinator-failure behavior |
| Active transaction listing | Candidate; `AdminClient::list_transactions` queries every metadata broker, aggregates transaction-state shards, supports state and producer-ID filters, and negotiates ListTransactions v1 with v0 fallback. The complete 17-job matrix passed the listing example on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plus the Kafka 3.7.2 three-broker profile in [`Live Kafka Smoke` run `31648660947`](https://github.com/TaeeunKil/kafrust/actions/runs/31648660947); qualify target authorization and duration-filter workloads |
| Replica reassignment | Candidate; controller-routed `AlterPartitionReassignments` v0 and bounded `ListPartitionReassignments` polling are live-verified on Kafka 3.7.2 and 4.3.1 three-broker profiles. A response-drop gate returns `AdminMutationOutcomeUnknown` without replay and reconciles final replica metadata in [`31776694068`](https://github.com/TaeeunKil/kafrust/actions/runs/31776694068) and [`31776695970`](https://github.com/TaeeunKil/kafrust/actions/runs/31776695970); authorization, cancellation, broker-loss, and data-movement qualification remain open |

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
