# kafrust

[English](README.md) | [한국어](README.ko.md)

[![Crates.io](https://img.shields.io/crates/v/kafrust.svg)](https://crates.io/crates/kafrust)
[![Docs.rs](https://docs.rs/kafrust/badge.svg)](https://docs.rs/kafrust)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

A pure Rust Kafka client with no librdkafka or C toolchain dependency.

kafrust is an alpha Kafka client for Rust applications that need Kafka protocol
compatibility without wrapping `librdkafka`. The project keeps Kafka concepts
visible in public APIs: bootstrap servers, client IDs, topics, partitions,
offsets, acknowledgements, metadata refresh, consumer groups, heartbeats, and
commits.

Current release: `0.3.5`.

Use kafrust today for experiments, local broker checks, simple internal tools,
and API evaluation. For broad production Kafka workloads that need mature
features immediately, `rust-rdkafka` is still the practical Rust default.

## Table of Contents

- [Background](#background)
- [Install](#install)
  - [Requirements](#requirements)
- [Usage](#usage)
  - [Admin](#admin)
  - [Producer](#producer)
  - [Transactional Producer](#transactional-producer)
  - [Buffered Producer](#buffered-producer)
  - [Direct Consumer](#direct-consumer)
  - [Consumer Group](#consumer-group)
  - [Share Group](#share-group)
  - [Streams Group](#streams-group)
- [Compatibility](#compatibility)
- [Current Limits](#current-limits)
- [API](#api)
- [Documentation](#documentation)
- [Translations](#translations)
- [Contributing](#contributing)
- [License](#license)

## Background

kafrust is a Kafka client, not a Kafka broker or Kafka-compatible server.

The strategic goal is to become a practical Rust-native client for applications
that value a pure Rust implementation, auditable protocol code, and no required
C toolchain in the default build. The project deliberately does not wrap
`librdkafka`, and it does not try to hide Kafka's operational model behind a
generic queue abstraction. The optional `tls` feature currently uses the
`rustls` ring provider and may require native build tooling in some environments.

The near-term roadmap focuses on:

- secured client connectivity with TLS and SASL
- multi-broker metadata, leader, and failover behavior
- consumer group hardening
- compression support
- observability, limits, benchmarks, and compatibility evidence

See [Project Strategy](docs/project-strategy.md) for replacement targets,
non-goals, existing alternatives, and completion tiers.

## Install

Add kafrust to a Rust project:

```toml
[dependencies]
kafrust = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Or with Cargo:

```sh
cargo add kafrust@0.3
```

### Requirements

- Rust `1.81` or newer.
- A Tokio runtime in the application for async APIs. The optional
  `blocking` feature owns a dedicated multi-thread runtime for synchronous
  producer, buffered producer, direct-consumer, consumer-group, Share, Streams,
  and an expanded Admin surface instead.
- A Kafka broker for runtime client calls.
- No `librdkafka` or C client bindings; the default build requires no C
  toolchain. The optional `tls` feature may require native build tooling for
  its ring crypto provider.

## Usage

Set broker addresses with `KAFRUST_BOOTSTRAP_SERVERS` when running examples.
Use Kafka's comma-separated bootstrap format for multiple brokers, for example
`localhost:19092,localhost:19093`. If the variable is omitted, the examples use
`localhost:9092`.
Smoke examples also accept `KAFRUST_SECURITY_PROTOCOL`,
`KAFRUST_SASL_USERNAME`, `KAFRUST_SASL_PASSWORD`, and
`KAFRUST_SASL_MECHANISM` for secured broker checks. Set
`KAFRUST_SASL_MECHANISM=oauthbearer` with `KAFRUST_SASL_TOKEN` for the
OAUTHBEARER path; `KAFRUST_SASL_TOKEN_PATH` can be used instead when the
application owns a rotating token file and wants the provider-backed path.
`KAFRUST_SASL_USERNAME` is optional for its authorization identity.

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_TOPIC=kafrust-smoke \
cargo run -p kafrust --example producer_send
```

### Admin

```rust
use kafrust::{AdminClient, ClientConfig, CreateTopicsOptions, NewTopic};

let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let cluster = admin.describe_cluster().await?;
println!("controller: {}", cluster.controller_id());

let result = admin
    .create_topics(
        &[NewTopic::new("orders", 6, 3).config("cleanup.policy", "compact")],
        CreateTopicsOptions::new(),
    )
    .await?;

for topic in result.topics() {
    println!("{}: Kafka error {}", topic.name(), topic.error_code());
}
```

`describe_cluster` and `list_topics` provide typed Metadata v1 views. The
opt-in `describe_cluster_with_options` path negotiates Kafka DescribeCluster
API 60 and preserves cluster ID, endpoint type, rack, and authorized-operation
metadata with Metadata fallback. Explicit controller endpoint requests use
`ClientConfig::controller_bootstrap_servers`; the published controller path is
qualified on Kafka 3.7.2 and 4.3.1. Topic config APIs expose values, sources, synonyms, and incremental
Set/Delete/Append/Subtract operations. `describe_consumer_groups` discovers
each group coordinator and preserves member protocol bytes.
`list_groups` queries all advertised brokers and negotiates ListGroups v4/v5
when available, while `list_groups_with_options` adds state/type filters.
The low-level `Client::list_groups_v1` method remains available for exact
legacy wire compatibility. `delete_consumer_groups` routes each group to its
coordinator and retains per-group errors.
`create_partitions` routes CreatePartitions v0 to the controller for automatic
or explicit topic expansion. `delete_consumer_group_offsets` routes
OffsetDelete v0 to the coordinator and
preserves top-level and per-partition Kafka errors.
`describe_quorum` inspects the KRaft metadata quorum through negotiated
DescribeQuorum v0-v2; set `ClientConfig::controller_bootstrap_servers` when the
controller listener is advertised separately. Kafka 3.7.2 and 4.3.1 are live-
qualified through the controller-listener workflow.
`add_raft_voter` and `remove_raft_voter` expose controller-routed KRaft voter
membership mutations with typed ambiguous-outcome handling. Their protocol and
injected-controller coverage is present, but live dynamic-quorum qualification
is still required before production compatibility is claimed.
`unregister_broker` exposes Kafka's controller-routed UnregisterBroker API 64
v0 with typed throttle and broker-error results; live unregister and
re-registration qualification remains open.
Share Group State APIs 83-87 are available through typed `AdminClient` methods.
Share-group membership and group-admin operations use the ordinary Group
coordinator, while durable state uses Kafka 4.x KIP-932 FindCoordinator v6
with a per-resource `group:topic-id:partition` key. The topic-id segment uses
Kafka's URL-safe Base64-without-padding `Uuid::toString()` representation.
Write and summary v1
fields are preserved without silent downgrade. Multi-topic and multi-partition
requests are split by the KIP-932 coordinator returned for each resource and
their partition-level results are merged; injected coverage verifies two
different coordinators. The Kafka 4.3.1 live replicated-state gate passed in
[`32351976899`](https://github.com/TaeeunKil/kafrust/actions/runs/32351976899),
including coordinator reassignment after broker loss and post-failover read,
summary, and delete. This remains an unstable broker-internal API qualification,
not general ShareConsumer or `rust-rdkafka` replacement evidence.
`list_consumer_group_offsets` and `alter_consumer_group_offsets` expose typed
classic consumer-group offset inspection and administrative reset through the
group coordinator, preserving partition-level outcomes.
`delete_consumer_groups` and `delete_consumer_group_offsets` retry transient
coordinator responses through fresh discovery within the bounded admin retry
budget; transport failures after mutation transmission remain single-attempt
because the broker-side outcome is ambiguous.
CreateTopics v2 and DeleteTopics v3 discover the active controller, retry
transient discovery failures before transmission, and preserve per-topic
partial success and error responses. See [Admin API](docs/admin-api.md).
ACL, client-quota, and incremental topic-config mutations retry bootstrap
connection failures before transmitting a request. Once a mutation request is
sent, transport failures remain single-attempt because the broker-side outcome
is ambiguous and must be reconciled explicitly.
`describe_acls`, `create_acls`, and `delete_acls` expose typed ACL bindings,
filters, and per-entry authorization outcomes; qualify them against an
authorizer-enabled broker before production rollout.
`describe_user_scram_credentials` and `alter_user_scram_credentials` expose
typed SCRAM credential administration, including controller routing and
per-user outcomes; the read-only describe path retries transient transport and
broker failures within the bounded AdminClient budget, and the Kafka 3.7.2
SASL_SSL roundtrip is live-verified.
Delegation-token lifecycle APIs cover create, describe, renew, and expire with
negotiated Kafka versions and redacted HMAC debug output. They require an
authenticated SASL or mutual-TLS channel and a broker-side delegation-token
secret; the complete opt-in lifecycle is exercised by the
`admin_delegation_tokens` example.
`alter_partition_reassignments` and `list_partition_reassignments` expose
controller-routed replica target changes, cancellation, and bounded ongoing
status inspection; the read-only listing path re-discovers the controller after
transient failures, and the Kafka 3.7.2 three-broker path is live-verified.
`elect_leaders` exposes controller-routed preferred and one-shot unclean leader
elections with negotiated ElectLeaders v0-v2 responses; preferred no-op error
84 remains observable and unclean election requires an explicit operator
choice. The Kafka 3.7.2 three-broker preferred-election path is live-qualified;
secured routing remains a separate gate. See [Admin API](docs/admin-api.md) for
the recovery warning and example.
`describe_log_dirs` exposes broker-local replica size, offset lag, future-log,
cordoned, and volume-capacity state through negotiated DescribeLogDirs v1-v5;
the API preserves broker-specific results instead of flattening them into
cluster metadata.
`alter_replica_log_dirs` adds explicit broker-local replica storage movement
with negotiated AlterReplicaLogDirs v1-v2 responses and no replay after an
ambiguous send. Observe completion with `describe_log_dirs` before relying on
the destination path.
`describe_cluster` and `list_topics` likewise retry transport and timeout
failures, and `list_topics` retries transient topic/partition metadata errors
while preserving final topic-level metadata errors.
`list_groups` also retries its initial Metadata discovery before enumerating
advertised brokers.
`delete_records` routes DeleteRecords v1 to each current partition leader and
preserves per-partition low watermarks and broker errors for partial deletion;
fixed-offset deletions retry retryable Metadata responses, transient leader
movement, and transport failures through fresh metadata.
`describe_producers` routes DescribeProducers v0 to each current partition
leader and exposes producer IDs, epochs, sequences, and active transaction
offsets; retryable Metadata responses, transient leader movement, and transport
failures are retried through fresh metadata. `describe_transactions` discovers
each transactional ID's
coordinator, retries transient coordinator failures, and preserves transaction
state, producer identity, and topic partition membership.
Teams evaluating replacement of a librdkafka-backed application should follow
the staged [rust-rdkafka migration guide](docs/migration-from-rust-rdkafka.md).

### Producer

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-producer")
        .acks(Acks::Leader)
        .build()
        .await?;

    let metadata = producer
        .send(
            ProducerRecord::to("kafrust-smoke")
                .key("example-key")
                .value("hello from kafrust"),
        )
        .await?;

    println!(
        "produced {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );

    Ok(())
}
```

When no partition is specified, keyed records use Kafka-compatible Murmur2
partitioning. Keyless records stay on one partition for a complete send or
buffered flush, then rotate to the next available partition for the topic.

### Transactional Producer

Transactional production is available as an opt-in alpha:

```rust
use kafrust::{ProducerConfig, ProducerRecord};

let mut producer = ProducerConfig::new(["localhost:9092"])
    .transactional_id("orders-writer")
    .build()
    .await?;

producer.begin_transaction()?;
producer
    .send(ProducerRecord::to("kafrust-smoke").value("committed value"))
    .await?;
producer.commit_transaction().await?;
```

Commit, abort, read-committed isolation, and transactional consumer group
offset commits are verified against Kafka `3.7.2` and `4.3.1`. Transaction
coordinator registration prefers flexible v3 APIs when advertised and falls
back to v0 for older brokers.

### Buffered Producer

Use `ProducerConfig::build_buffered` when records should be queued and flushed
by linger time, record count, byte count, `flush`, or `close`.

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-buffered-producer")
        .acks(Acks::Leader)
        .linger_ms(10)
        .max_records_per_batch(100)
        .build_buffered()
        .await?;

    let delivery = producer
        .send(ProducerRecord::to("kafrust-smoke").value("buffered value"))
        .await?;

    let metadata = delivery.await?;
    producer.close().await?;

    println!("buffered record offset {}", metadata.offset());

    Ok(())
}
```

### Direct Consumer

Direct consumers fetch from explicit topic partitions and offsets. This path is
useful before consumer group behavior is needed.

```rust
use kafrust::ConsumerConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut consumer = ConsumerConfig::new(["localhost:9092"])
        .client_id("example-consumer")
        .build()
        .await?;

    consumer.assign("kafrust-smoke", 0, 0);
    for record in consumer.poll().await? {
        println!(
            "fetched {}-{}@{} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.value().map(String::from_utf8_lossy)
        );
    }

    Ok(())
}
```

Fetched records also expose Kafka RecordBatch headers through
`record.headers()`. Each `ConsumerRecordHeader` preserves wire order, and its
`value()` returns `Option<&[u8]>` so null header values remain distinguishable
from empty values. Legacy MessageSet records expose no headers.

Direct and group consumers negotiate topic-UUID Fetch v13 when the selected
broker advertises it and Metadata v12 resolves a stable topic ID; otherwise
they fall back to Fetch v12, then v11. For rack-aware reads, set
`ConsumerConfig::client_rack("rack-a")` (or the matching
`ConsumerGroupConfig` builder); kafrust sends the rack ID using the flexible
Fetch schema and follows Kafka's `preferred_read_replica` response on the next
fetch. Without a rack, the same session-capable path uses an empty rack ID.
Fetch v4 remains the fallback for brokers that do not advertise v11 or v12.
Wire-level and injected multi-broker routing tests cover this path. The Kafka 3.7.2
three-broker `broker.rack` plus `RackAwareReplicaSelector` profile passed in
[`Live Kafka Smoke`, run `31640494509`](https://github.com/TaeeunKil/kafrust/actions/runs/31640494509),
including live Fetch v12 requests and preferred-replica routing.
Fetch v11/v12/v13 requests reuse a broker-scoped fetch session across sequential
polls; assignment changes, local position controls, reconnects, and fetch errors
reset that session. Fetch v4 remains a compatibility fallback without a
fetch-session claim.
The low-level `Client::fetch_v13` method additionally supports Kafka 4.x
topic-UUID fetch requests for callers that already resolved metadata IDs, and
`fetch_v14` through `fetch_v18` expose the corresponding tiered-storage,
replica-state, node-endpoint, directory-ID, and high-watermark wire variants.
High-level consumers use v13 when metadata IDs are available and retain the
older name-based fallback for incomplete broker capabilities.
The complete 17-job matrix for the general direct-consumer negotiation passed in
[`31673377685`](https://github.com/TaeeunKil/kafrust/actions/runs/31673377685).
The complete 17-job matrix for this session path passed in
[`31671783977`](https://github.com/TaeeunKil/kafrust/actions/runs/31671783977),
including the Kafka 3.7.2 three-broker rack-aware follow-up request.

### Consumer Group

The current consumer group API is an alpha classic or KIP-848 consumer group
path with dynamic or static membership, range, round-robin, eager sticky, or
opt-in cooperative-sticky assignment for classic groups, join, heartbeat, poll,
offset fetch/commit, explicit leave, and earliest/latest reset for partitions
that have no committed offset. The
the sticky path uses Kafka's previous-assignment user data and eager transfer;
the cooperative-sticky path includes protocol, staged assignment, multi-member
ownership transfer, transient-member rollback, and member-loss recovery. These
cooperative failure paths are live-verified in the Kafka `3.7.2` three-broker
profile. The KIP-848 path, including flexible offset fetch/commit and background
heartbeat rejoin, is live-verified against Kafka `4.3.1`; the group API itself
remains pre-`1.0`.

```rust
use kafrust::{ConsumerGroupConfig, OffsetResetPolicy};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut group = ConsumerGroupConfig::new(["localhost:9092"], "example-group")
        .client_id("example-consumer-group")
        .group_instance_id("example-consumer-1")
        .offset_reset_policy(OffsetResetPolicy::Earliest)
        .subscribe("kafrust-smoke")
        .join()
        .await?;

    let records = group.poll().await?;
    group.commit_offsets().await?;

    println!("processed {} records", records.len());

    Ok(())
}
```

For interval-based queued commits, use the opt-in bounded worker and observe
its handle before shutdown:

```rust
let mut commit_worker = group
    .spawn_commit_worker(std::time::Duration::from_secs(1))
    .await?;

for record in &records {
    process(record.value())?;
    group.commit_record(record)?;
}

commit_worker.stop().await?;
group.leave().await?;
```

The worker coalesces offsets per partition and synchronizes its generation and
assignment state across `group.rejoin()`. Check `try_wait()` in long-running
applications so terminal commit or generation errors are not hidden.

If an OffsetCommit may have reached Kafka but its response is lost, the group
returns `Error::ConsumerGroupCommitOutcomeUnknown` with the group ID, member
ID, generation/member epoch, and exact topic-partition next offsets. The
ambiguous request is not replayed automatically; reconcile those offsets before
issuing a newer commit. This rule applies to direct commits and the bounded
background worker.

## Share Group

The current development branch also contains an alpha `ShareConsumer` for the
stable KIP-932 v1 APIs, with optional KIP-1206 ShareFetch v2 acquisition modes
and KIP-1222 renewal acknowledgement support.
A share group is a work-queue model: each acquired
record is acknowledged independently with `Accept`, `Release`, `Reject`, or
`Gap`, rather than by committing a partition position.

```rust
use kafrust::{ShareAcknowledgementType, ShareConsumerConfig};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut consumer = ShareConsumerConfig::new(["localhost:9092"], "orders")
        .subscribe("orders")
        .build()
        .await?;

    loop {
        let records = consumer.poll().await?;
        for record in &records {
            process(record.value())?;
            consumer.acknowledge(record, ShareAcknowledgementType::Accept)?;
        }
        consumer.commit().await?;
    }
}
```

The API is pre-1.0, but the published `0.3.5` artifact now includes this
surface. It has focused protocol tests and an injected-broker wire roundtrip
test, plus an opt-in cancellable background heartbeat task. A Kafka 4.3.1
single-node live smoke passed the KIP-1222 renewal, expiry/redelivery, and
final acceptance path in [run 32213499877](https://github.com/TaeeunKil/kafrust/actions/runs/32213499877).
A three-broker fresh-consumer failover path passed in [run 32214201983](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983),
and an active-heartbeat coordinator failover path passed in [run 32215845737](https://github.com/TaeeunKil/kafrust/actions/runs/32215845737).
Three independent repeats of the active-heartbeat path also passed in [run 32216383214](https://github.com/TaeeunKil/kafrust/actions/runs/32216383214).
The published artifact additionally passed a fresh external single-record
runtime in [run 32384767744](https://github.com/TaeeunKil/kafrust/actions/runs/32384767744)
and a 64-record acknowledgement/commit soak in [run 32385522647](https://github.com/TaeeunKil/kafrust/actions/runs/32385522647).
It also passed a fresh external three-broker leader-failover path in
[run 32386637555](https://github.com/TaeeunKil/kafrust/actions/runs/32386637555),
including broker 1 leader loss and post-failover acceptance through surviving
bootstrap servers.
The published active-heartbeat path also passed three consecutive dynamic
coordinator-loss cycles in [run 32387564503](https://github.com/TaeeunKil/kafrust/actions/runs/32387564503),
with the heartbeat task remaining alive through recovery.
The published two-member ownership path also passed in
[run 32388813780](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780):
two external members joined one Share group, each accepted three records, and
all six seeded partitions were observed exactly once across the members.
The same published workflow also passed a 60-second 384-record extension in
[run 32389641275](https://github.com/TaeeunKil/kafrust/actions/runs/32389641275),
with each member accepting 192 records and all partition/offset pairs unique.
The published member-loss path also passed in
[run 32390219711](https://github.com/TaeeunKil/kafrust/actions/runs/32390219711):
after member 2 was force-terminated, member 1 reacquired all six partitions and
accepted one record from each.
The published `0.3.5` artifact also passed a fresh external three-broker
leader-failover run in
[run 32423091397](https://github.com/TaeeunKil/kafrust/actions/runs/32423091397):
after the selected partition leader was stopped, the surviving published
producer and ShareConsumer completed the post-failover path through the
replacement leader.
The published `0.3.5` acknowledgement soak also passed in
[run 32423629077](https://github.com/TaeeunKil/kafrust/actions/runs/32423629077):
64 unique records were acquired, acknowledged, and committed with 64 unique
offsets from a fresh external project.
The same-group repeated churn path also passed in
[run 32391027028](https://github.com/TaeeunKil/kafrust/actions/runs/32391027028):
member 2 rejoined after the first loss and later took over all six partitions
after member 1 stopped, with 12 unique offsets.
In-process repeated recovery, long-running qualification, and ambiguous
acknowledgement responses are surfaced as a typed unknown-outcome error and are
not replayed automatically. Multi-broker long-running ownership,
higher-cycle churn, longer backpressure, and broader published-artifact
qualification remain open. `BatchOptimized` is the default acquisition mode;
`RecordLimit` uses KIP-1206 and requires a broker advertising ShareFetch v2;
`Renew` uses ShareAcknowledge v2 and retains the record for later completion.
See [Share Consumer](docs/share-consumer.md) for the exact alpha contract.

## Streams Group

The published `0.3.3` artifact includes an alpha `StreamsGroupSession` and
`StreamsTaskRuntime` for
Kafka's dedicated Streams group heartbeat protocol. It publishes the initial
topology, tracks member and endpoint epochs, reports task state, reconnects and
rejoins within a bounded retry budget, and leaves with
`shutdown_application=true`. It is a broker membership layer only; it does not
execute a Kafka Streams DSL, state stores, or stream-processing tasks. A joined
session can be moved into a bounded `StreamsGroupSessionHandle`, which owns
periodic heartbeats and publishes assignment snapshots. The application can
use `StreamsTaskRuntime` to turn nullable broker updates into deterministic
task lifecycle transitions, and must await `close()` for a graceful leave.

```rust,no_run
use kafrust::streams::{
    StreamsGroupHeartbeatSubtopology, StreamsGroupHeartbeatTopology,
};
use kafrust::{StreamsGroupConfig, StreamsGroupSession};

# async fn example() -> kafrust::Result<()> {
let topology = StreamsGroupHeartbeatTopology {
    epoch: 1,
    subtopologies: vec![StreamsGroupHeartbeatSubtopology {
        subtopology_id: "subtopology-0".to_owned(),
        source_topics: vec!["orders".to_owned()],
        source_topic_regex: Vec::new(),
        state_changelog_topics: Vec::new(),
        repartition_sink_topics: Vec::new(),
        repartition_source_topics: Vec::new(),
        copartition_groups: Vec::new(),
    }],
};
let session = StreamsGroupSession::join(
    StreamsGroupConfig::new(["localhost:9092"], "orders-streams", topology),
)
.await?;
let handle = session.spawn_heartbeat_task();
let mut task_runtime = kafrust::StreamsTaskRuntime::new();
let _transitions = handle.reconcile_task_runtime(&mut task_runtime)?;
handle.heartbeat_now().await?;
handle.close().await?;
# Ok(())
# }
```

See [Streams group](docs/streams-group.md) for the alpha contract and the
manual/weekly live Kafka 4.3.1 qualification workflow.

## Client Telemetry

KIP-714 client telemetry is available through the low-level `Client` methods
and a high-level `TelemetryClient` that negotiates broker support, keeps the
subscription state, bounds payloads, retries an outdated subscription, applies
push-interval jitter, compresses OTLP payloads with the strongest broker-
accepted pure-Rust codec, and sends a terminating push during shutdown. The
optional `otlp` feature includes a built-in `ClientMetricsTelemetryProvider`;
the Kafka 3.7.2 broker-plugin gate passed in the [`Live Client Telemetry`](https://github.com/TaeeunKil/kafrust/actions/runs/32422305042), including subscription mutation recovery, payload verification, and the terminating push. Secured, multi-broker, and long-running telemetry remain separate qualification gates.
See [Client Telemetry](docs/telemetry.md).

## Compatibility

kafrust compatibility claims are limited to behavior verified against real
brokers.

The v1 qualification target uses Kafka `3.7.2` as its floor, with `3.8.1`,
`3.9.1`, `4.0.0`, and `4.3.1` as continuity/pinned profiles. It is KRaft-only:
single-node baseline, three-broker failover, and explicit controller listeners
for routed Admin are in scope; ZooKeeper and managed-service equivalence are
unclaimed. Tokio async is required, `blocking` is an owned-runtime adapter,
and alternate runtimes/general synchronous APIs are excluded. The exact
security/workload boundary and immutable results are maintained in the
[v1.0 support contract](docs/compatibility.md#v10-support-contract) and
[qualification ledger](docs/evidence/qualification-ledger.md).

| kafrust | Broker | Mode | Security | Status |
| --- | --- | --- | --- | --- |
| `0.3.x` | Apache Kafka `3.7.2` | single-node KRaft | `PLAINTEXT` | Passing live smoke |
| `0.3.x` | Apache Kafka `4.3.1` | single-node KRaft | `PLAINTEXT` | Passing live smoke |
| `0.3.x` | Apache Kafka `3.7.2` | single-node KRaft | `TLS` | Passing live smoke |
| `0.3.x` | Apache Kafka `3.7.2` | single-node KRaft | `SASL_PLAINTEXT` with SASL/PLAIN | Passing live smoke |
| `0.3.x` | Apache Kafka `3.7.2` | single-node KRaft | `SASL_SSL` with SCRAM-SHA-256 | Passing live smoke |
| `0.3.x` | Apache Kafka `3.7.2` | single-node KRaft | `SASL_SSL` with SCRAM-SHA-512 | Passing live smoke |
| `0.3.x` | Apache Kafka `3.8.1` | single-node KRaft | `PLAINTEXT` | Passing live smoke |
| `0.3.x` | Apache Kafka `3.9.1` | single-node KRaft | `PLAINTEXT` | Passing live smoke |
| `0.3.x` | Apache Kafka `3.7.2` | three-broker KRaft | `PLAINTEXT` with broker racks | Passing live smoke |

Verified paths currently include:

- `ApiVersions v0` and flexible `ApiVersions v3` capability roundtrips, plus
  `Metadata v1` roundtrips.
- High-level producer single-record, batch, and buffered sends.
- Direct topic-partition fetch with Fetch v4 response decoding, plus focused
  flexible Fetch v12 and legacy Fetch v11 rack-aware negotiation and
  preferred-replica routing tests.
- Classic consumer group join, sync, heartbeat, poll, and offset commit.
- KIP-848 consumer group assignment, member-epoch heartbeat, negotiated
  OffsetFetch/OffsetCommit v10 with v9 fallback, member-aware
  administrative offset listing/alteration,
  background rejoin, and explicit leave against Kafka `4.3.1` (including the
  PLAINTEXT single-node and three-broker Admin offset smoke in
  [`31607006237`](https://github.com/TaeeunKil/kafrust/actions/runs/31607006237),
  including SASL_PLAINTEXT and SASL_SSL/SCRAM profiles).
- In-flight leader-routed DeleteRecords and DescribeProducers plus
  coordinator-routed DescribeTransactions, DescribeGroups, OffsetFetch, and
  exact-offset OffsetCommit, plus broker-routed DescribeConfigs and ListGroups
  recovery after deterministic pre-transmission gates and broker stops on the
  Kafka `3.7.2` three-broker profile in
  [`31616181960`](https://github.com/TaeeunKil/kafrust/actions/runs/31616181960)
  at commit `17cae6e`; all eight gates passed, with ListGroups recording
  `retries=7` while its broker restarted.
- The same profile also stopped broker 1 before Metadata v1 transmission and
  recovered both `describe_cluster` and `list_topics`, each with `retries=1`,
  in [`31620595346`](https://github.com/TaeeunKil/kafrust/actions/runs/31620595346)
  at commit `b90a7e9`.
- After adding safe pre-transmission controller discovery retries for Admin
  writes, the complete 17-job matrix passed at commit `256847f` in
  [`31624278107`](https://github.com/TaeeunKil/kafrust/actions/runs/31624278107).
- The complete 17-job matrix also passed at commit `25d614a` in
  [`31627790408`](https://github.com/TaeeunKil/kafrust/actions/runs/31627790408)
  after the ACL authorizer example added bounded post-create visibility polling.
- The latest complete 17-job matrix passed at commit `1a844d8` in
  [`31648660947`](https://github.com/TaeeunKil/kafrust/actions/runs/31648660947).
  It included ListTransactions across the Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 single-node profiles plus the 3.7.2 multi-broker profile. It also
  included topic-ID Produce v13/v12/v11/v9 negotiation (Kafka 4.3.1 selected v13;
  Kafka 3.8.1 and 3.9.1 selected v11; Kafka 3.7.2 selected v9), rack-aware
  Fetch v12, DeleteRecords and DescribeProducers leader-stop recovery, and
  the supported security, ACL, failover, and KIP-848 profiles.

See [Compatibility](docs/compatibility.md) and
[Broker Roundtrip](docs/broker-roundtrip.md) for the current evidence.

## Current Limits

- Public APIs are pre-`1.0` and can change between minor releases.
- The optional `blocking` feature provides synchronous adapters for the core
  producer, manually assigned direct-consumer APIs, joined consumer groups,
  and the broad broker/controller Admin surface. They must be used outside an
  existing Tokio runtime. A general alternate-runtime abstraction remains
  async/open.
- Plaintext TCP remains the default networking path.
- TLS transport is available behind the non-default `tls` crate feature and is
  verified against Kafka `3.7.2` for broker roundtrip, producer, direct
  consumer, and consumer group smoke paths. TLS certificate server name
  validation defaults to the bootstrap host and can be overridden with
  `tls_server_name(name)`. DER-encoded extra root certificates can be added with
  `tls_root_certificate_der(bytes)`. The current ring crypto provider can
  require native build tooling for this optional feature.
- Mutual TLS client certificate chains and DER private keys are available
  through `tls_client_certificate_der` and `tls_client_private_key_der` on the
  shared config and high-level builders. The pair is validated, rejected for
  plaintext, and the private key is redacted from `Debug`; live mTLS
  qualification is provided by the manual
  [`live-mtls.yml`](.github/workflows/live-mtls.yml) workflow and remains open
  until a passing run is recorded.
- SASL/PLAIN authentication is verified against Kafka `3.7.2` over
  `SaslPlaintext` for broker roundtrip, producer, direct consumer, and consumer
  group smoke paths. SASL/SCRAM-SHA-256 and SCRAM-SHA-512 are verified over
  `SaslTls`; the SHA-512 profile covers broker roundtrip, producer, batch,
  buffered producer, direct consumer, and consumer group poll paths.
- SASL/OAUTHBEARER token authentication is available through
  `sasl_oauthbearer` and `sasl_oauthbearer_with_username` on the client,
  producer, consumer, and consumer-group builders. Async token providers are
  available through the corresponding `*_provider` builders and are called for
  each new broker authentication. It is covered by injected handshake tests
  and a Kafka 3.7.2 SASL_SSL smoke using the broker's built-in unsecured
  validator (`Live Kafka Smoke` run `31478375106`). OAUTHBEARER initial
  initial authentication uses flexible `SaslAuthenticate v2`, while provider
  re-authentication uses Kafka-compatible `SaslAuthenticate v1`; PLAIN and
  SCRAM continue to use `v1`. A signed
  JWT/JWKS OIDC fixture also passes Kafka's validator, the Java Kafka client,
  and kafrust static and provider-backed paths in the
  [`Live Kafka Smoke` OIDC job](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94075906934).
  `CachedOAuthBearerTokenProvider` single-flights concurrent refreshes shared by
  cloned client configurations and rejects source results that are already
  expired; external provider-specific behavior remains separately qualified.
- Single-node plaintext compatibility is verified against Kafka `3.7.2`,
  `3.8.1`, `3.9.1`, and `4.3.1`. Secured and multi-broker profiles remain
  verified against `3.7.2`.
- Three-broker coordinator and leader failover is verified for the documented
  producer, direct consumer, classic consumer group, KIP-848 consumer group,
  and transaction paths. Topic
  partition expansion is verified through CreatePartitions v0 and Metadata v1.
  Rack-aware client routing prefers flexible Fetch v12, falls back to Fetch v11
  and then Fetch v4, and follows the broker's preferred-replica response. The
  Kafka 3.7.2 three-broker rack-aware profile is live-qualified in
  [`31640494509`](https://github.com/TaeeunKil/kafrust/actions/runs/31640494509);
  this covers replica selection, not every possible rack or security topology.
- `ProducerConfig::partitioner` supports thread-safe custom routing for records
  without explicit partitions across immediate, batch, and buffered sends.
- `ProducerConfig::delivery_timeout_ms` bounds the total immediate or batch
  delivery time across metadata, Produce requests, retries, and backoff; the
  default is 120 seconds. Buffered records use the same deadline from enqueue
  through Produce, while `linger_ms` independently controls batching latency.
- Gzip, Snappy, LZ4, and Zstd compression prefer topic-ID Produce v13 when the
  broker advertises it and Metadata v12 returns a topic UUID; otherwise they
  use flexible Produce v12, then v11, then v9, RecordBatch encoding when the
  broker advertises it, with Produce v7/v3 fallbacks. Fetch v4
  decodes all four codecs. They are
  verified against Kafka `3.7.2` plaintext single-node and multi-broker smoke
  profiles and the single-node TLS profile. Snappy uses Kafka-compatible Xerial
  framing and accepts raw Snappy blocks when decoding.
- Idempotent single-record, batch, and buffered sends are available as an
  opt-in alpha. Transactional immediate, batch, and buffered sends support
  explicit begin, commit, and abort. `IsolationLevel::ReadCommitted` hides
  aborted transaction records for direct and group consumers, and current
  group assignments can be committed through immediate or buffered
  `send_group_offsets_to_transaction`. Transaction coordinator and
  producer/direct-consumer/group recovery after a broker stop are live-verified
  in the three-broker profile. Client quota, SCRAM credential administration,
  partition reassignment, and modern KIP-848 `ConsumerGroupDescribe` are
  available through `AdminClient`. Shared request, retry,
  broker-error, producer,
  consumer, batch, and buffered-queue metrics are available together with
  approximate request-latency percentiles and high-level operation and
  `kafka.request` spans.
- `acks=0` fire-and-forget sends are supported for immediate and batch
  producer paths. The request is written and flushed without waiting for a
  broker response, so returned offsets are `-1` and broker acceptance or
  partition-level errors cannot be confirmed. This path is live-verified
  against Kafka `3.7.2`, `3.8.1`, `3.9.1`, and `4.3.1` single-node plaintext
  profiles.

## API

Primary public entry points:

- `Client` for low-level Kafka request roundtrips.
- `AdminClient` and typed cluster, topic, configuration, consumer-group, ACL,
  and client-quota administration types.
- `ProducerConfig`, `Producer`, `BufferedProducer`, and `ProducerRecord`.
- `Compression` for opt-in producer RecordBatch compression.
- `ConsumerConfig`, `Consumer`, `ConsumerAssignment`, `ConsumerRecord`, and
  `ConsumerPartitionQueue` for bounded per-partition delivery.
- `ConsumerGroupConfig`, `ConsumerGroup`, `ConsumerGroupProtocol`, and
  `ConsumerGroupHeartbeat`.
- `SecurityProtocol`, `SaslMechanism`, and `SaslCredentials` for plaintext,
  TLS, and SASL connection modes.
- `ClientMetrics` and `ClientMetricsSnapshot` for request-level observability,
  including approximate `p50`/`p95`/`p99` latency queries.
- `ClientConfig::validate`, `ProducerConfig::validate`,
  `ConsumerConfig::validate`, and `ConsumerGroupConfig::validate` for startup
  preflight without opening a broker connection.
- `Error::ResponseTooLarge` and `max_response_bytes` builders for bounded
  broker response allocation.
- `Error::InvalidConfiguration` for invalid builder values detected before
  opening a broker connection, including zero timeouts, invalid decode limits,
  invalid fetch limits, empty group subscriptions, and invalid transaction
  settings.
- `max_decode_array_elements` and `max_decompressed_record_bytes` builders for
  bounded protocol collections and compressed Fetch record batches.
- `kafrust::protocol` for the companion `kafrust-protocol` crate.

Generated API documentation:

- [`kafrust`](https://docs.rs/kafrust/0.3.5/kafrust/)
- [`kafrust-protocol`](https://docs.rs/kafrust-protocol/0.3.5/kafrust_protocol/)

## Documentation

- [Contributing](CONTRIBUTING.md)
- [Agent instructions](AGENTS.md)
- [Agentic development workflow](docs/agentic-development.md)
- [Project strategy](docs/project-strategy.md)
- [Competitor source audit](docs/competitor-source-audit-2026-08-20.md)
- [Performance benchmarks](docs/performance.md)
- [Roadmap](docs/roadmap.md)
- [v1.0 milestone program](docs/milestones/v1.0/README.md)
- [Broker roundtrip](docs/broker-roundtrip.md)
- [Compatibility](docs/compatibility.md)
- [Migrating from rust-rdkafka](docs/migration-from-rust-rdkafka.md)
- [API stability](docs/api-stability.md)
- [Public API audit](docs/public-api-audit.md)
- [Producer API direction](docs/producer-api.md)
- [Producer buffering and linger design](docs/producer-buffering.md)
- [Consumer API direction](docs/consumer-api.md)
- [Consumer group direction](docs/consumer-groups.md)
- [Share Consumer](docs/share-consumer.md)
- [Streams group](docs/streams-group.md)
- [Client telemetry](docs/telemetry.md)
- [Release preparation](docs/release.md)

## Translations

`README.md` is the canonical English README.

Available translations:

- [English](README.md)
- [한국어](README.ko.md)

If more translated READMEs are added, name them with BCP 47 language tags, such
as `README.ja.md`, and keep release facts, compatibility claims, and limits
aligned with this file.

## Contributing

Issues and pull requests are accepted on GitHub.

Before contributing:

- Read [Contributing](CONTRIBUTING.md) and [Agent instructions](AGENTS.md).
- Keep kafrust a pure Rust Kafka client.
- Do not introduce `librdkafka`, C client bindings, or a required C toolchain.
- Preserve Kafka user-facing concepts in public APIs.
- Use Conventional Commits.
- Add focused tests for protocol behavior or observable client behavior.
- Update docs when public behavior, project direction, or workflow changes.

## License

MIT OR Apache-2.0 (c) kafrust contributors.

See [MIT License](LICENSE-MIT) and
[Apache License, Version 2.0](LICENSE-APACHE).
