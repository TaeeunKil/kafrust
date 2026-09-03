# kafrust

[![Crates.io](https://img.shields.io/crates/v/kafrust.svg)](https://crates.io/crates/kafrust)
[![Docs.rs](https://docs.rs/kafrust/badge.svg)](https://docs.rs/kafrust)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)

A pure Rust Kafka client with no librdkafka or C client binding dependency.

`kafrust` is the high-level client crate in the kafrust workspace. It provides
Tokio-based admin, producer, direct consumer, and alpha classic/KIP-848
consumer group APIs on top of the companion
[`kafrust-protocol`](https://docs.rs/kafrust-protocol) wire-format crate.
The optional `blocking` feature provides synchronous adapters for direct and
bounded-buffered producers, direct consumer, consumer group, Share, Streams,
and expanded Admin operations.

Current release: `0.3.6`.

This crate is alpha. Use it for experiments, local broker checks, simple
internal tools, and API evaluation. For broad production Kafka workloads that
need mature features immediately, `rust-rdkafka` remains the practical Rust
default today.

## Design Goals

- Keep Kafka concepts visible in public APIs.
- Stay pure Rust with no `librdkafka` or C client binding. The default build
  requires no C toolchain; the optional TLS feature may require native build
  tooling for its `rustls` ring crypto provider.
- Make protocol and runtime behavior auditable through small, tested slices.
- Claim compatibility only when a real broker profile has been verified.

The public model intentionally exposes Kafka terms such as bootstrap servers,
client IDs, topics, partitions, offsets, acknowledgements, metadata refresh,
consumer groups, generations, members, heartbeats, and commits.

## Admin

```rust,no_run
use kafrust::{AdminClient, ClientConfig, CreateTopicsOptions, NewTopic};

# async fn example() -> kafrust::Result<()> {
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
# Ok(())
# }
```

Cluster and topic listing use typed Metadata v1 views, and topic configuration
inspection and alteration preserve sources, sensitivity, synonyms, operation
kinds, validation mode, and partial resource errors. CreateTopics and
DeleteTopics route through the active controller, while group descriptions
and deletion route through each group coordinator. Group listing queries all
advertised brokers. ACL describe/create/delete operations preserve typed
bindings and partial authorization results; qualify them against an
authorizer-enabled broker before production rollout. Client quota
describe/alter operations preserve typed entities, floating-point values, and
per-entity results; both paths are live-verified in the documented Kafka
3.7.2 StandardAuthorizer profile. Consumer-group offset listing and
administrative alteration use typed classic OffsetFetch v2 and OffsetCommit v2
results. See the repository's `docs/admin-api.md` for details.
For group discovery, `AdminClient::list_groups` negotiates ListGroups v4/v5
when advertised and preserves broker-reported group state and type;
`list_groups_with_options` adds broker-side state/type filters, while the
low-level `Client::list_groups_v1` method remains available for exact legacy
wire compatibility.
API 74 configuration-resource discovery is available through
`AdminClient::list_config_resources`; Kafka 3.9-era brokers use the v0
client-metrics compatibility shape when an exact `ClientMetrics` filter is
requested, while Kafka 4.1+ brokers use the v1 typed resource path. The v1
path and opt-in DescribeConfigs v4 metadata path share the manual Kafka 4.x
qualification workflow, but remain outside the live compatibility claim until
a passing run is recorded. `AdminClient::describe_cluster_with_options` similarly uses
DescribeCluster API 60 when advertised and preserves cluster-level metadata;
the default remains the legacy Metadata path for compatibility.
The controller-routed `AdminClient::unregister_broker` method exposes Kafka's
UnregisterBroker API 64 v0 and preserves explicit unknown-outcome handling for
transport failures after transmission; live broker unregister qualification is
not yet claimed.
Share Group State APIs 83-87 are available through typed `AdminClient` methods
and share-coordinator routing. Write and summary v1 fields are preserved
without silent downgrade; live state lifecycle qualification remains open.
Delegation-token lifecycle APIs cover create, describe, renew, and expire with
negotiated Kafka versions. They require an authenticated SASL or mutual-TLS
channel and a broker-side delegation-token secret; use the
`admin_delegation_tokens` example for the complete opt-in lifecycle.

## Streams Group

The development branch includes an alpha `StreamsGroupSession` for Kafka's
dedicated Streams group heartbeat protocol. It publishes topology, tracks
member and endpoint epochs, reports task state, retries coordinator recovery,
and leaves with `shutdown_application=true`. It is a membership layer only,
not a Kafka Streams DSL or task processor; the caller currently drives
`heartbeat()`. See the repository's `docs/streams-group.md` for the contract
and live qualification workflow.

## Install

```toml
[dependencies]
kafrust = "0.3"
tokio = { version = "1", features = ["macros", "rt"] }
```

For a multi-threaded application runtime, enable Tokio's `rt-multi-thread`
feature in the application.

### Blocking clients

Enable the optional feature when an integration boundary cannot expose async
methods:

```toml
[dependencies]
kafrust = { version = "0.3", features = ["blocking"] }
```

`BlockingProducer`, `BlockingBufferedProducer`,
`BlockingBufferedProducerHandle`, `BlockingConsumer`, `BlockingConsumerGroup`,
`BlockingShareConsumer`, `BlockingStreamsGroupSession`, and
`BlockingAdminClient` own a dedicated multi-thread Tokio runtime and preserve
the async clients' retry, compression, idempotence, transaction, direct-fetch,
  group poll/heartbeat/commit, Share poll/acknowledge/commit, Streams
  heartbeat/task-state/close, topic lifecycle, ACL/security, quota, storage,
  reassignment, transaction-diagnostic, group-listing, group-offset, feature,
  and topic-configuration behavior. Construct and use them from a thread that
  is not inside another Tokio runtime. The blocking group adapter performs
foreground heartbeats during `poll`; configure automatic commits on
`ConsumerGroupConfig` when a background commit worker is required. Detached
group, Share, or Streams heartbeat task handles remain asynchronous for
explicit lifecycle management.

## Producer

```rust,no_run
use kafrust::{Acks, Compression, ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-producer")
        .acks(Acks::Leader)
        .build()
        .await?;

    let metadata = producer
        .send(
            ProducerRecord::to("orders")
                .key("order-123")
                .value("created")
                .header("source", "checkout"),
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
partitioning and keyless records use the producer's batch-sticky routing. A
custom `ProducerConfig::partitioner` callback can override routing for records
without explicit partitions; explicit partitions always take precedence.

## Batch Producer

`Producer::send_batch` returns metadata in input order. Use
`Producer::send_batch_report` when partial per-record failures need to be
inspected without losing successful records.

```rust,no_run
use kafrust::{Acks, Compression, ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-batch-producer")
        .acks(Acks::Leader)
        .compression(Compression::Gzip)
        .max_records_per_batch(500)
        .max_batch_bytes(64 * 1024)
        .build()
        .await?;

    let report = producer
        .send_batch_report([
            ProducerRecord::to("orders").key("order-124").value("created"),
            ProducerRecord::to("orders").key("order-125").value("created"),
        ])
        .await?;

    for outcome in report.records() {
        if let Some(metadata) = outcome.metadata() {
            println!("{}-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());
        }
        if let Some(failure) = outcome.failure() {
            eprintln!(
                "record {} failed on {}-{}: {}",
                failure.record_index(),
                failure.topic(),
                failure.partition(),
                failure.error()
            );
        }
    }

    Ok(())
}
```

`Compression::Gzip`, `Compression::Snappy`, `Compression::Lz4`, and
`Compression::Zstd` prefer topic-ID Produce API v13 when Metadata v12 supplies
a non-zero topic UUID, then flexible Produce API v12, v11, and v9, RecordBatch
encoding when available, then fall back to Produce API v7 or v3. Brokers without the required
Produce API version return an explicit `Unsupported` error when compression is
enabled.
Snappy output uses Kafka-compatible Xerial framing; LZ4 and Zstd output use
their standard frames as expected by RecordBatch v2. Brokers without the
required Produce API version return an explicit `Unsupported` error when
compression is enabled.

## Transactional Producer

Use `transactional_id` to enable the alpha transactional producer:

```rust,no_run
use kafrust::{ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .transactional_id("orders-writer")
        .build()
        .await?;

    producer.begin_transaction()?;
    producer
        .send(ProducerRecord::to("orders").value("committed value"))
        .await?;
    producer.commit_transaction().await?;
    Ok(())
}
```

The high-level commit, abort, read-committed isolation, and transactional
consumer group offset paths are verified against Kafka `3.7.2` and `4.3.1`.
`Producer::transaction_status()` and the matching buffered-producer method
expose `Ready`, `InTransaction`, and terminal `Defunct` states. If an `EndTxn`
response is lost,
`commit_transaction` or `abort_transaction` returns
`Error::TransactionOutcomeUnknown`; discard that producer because Kafka may
have applied the requested outcome, and create a new producer with the same
transactional ID for recovery. The old transaction is never reported as
successfully committed or aborted from a lost response.

## Buffered Producer

`ProducerConfig::build_buffered` creates an opt-in buffered producer. Records are
flushed by linger time, record count, byte count, explicit `flush`, or `close`.
Dropping the owning `BufferedProducer` aborts its worker so it is not detached;
use `close()` when accepted records must be flushed gracefully.

```rust,no_run
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-buffered-producer")
        .acks(Acks::Leader)
        .linger_ms(10)
        .max_records_per_batch(100)
        .buffer_capacity(1024)
        .build_buffered()
        .await?;

    let delivery = producer
        .send(ProducerRecord::to("orders").value("buffered value"))
        .await?;

    let metadata = delivery.await?;
    producer.close().await?;

    println!("buffered record offset {}", metadata.offset());

    Ok(())
}
```

For concurrent non-transactional enqueueing, call `producer.handle()` and
clone the returned `BufferedProducerHandle` into the Tokio tasks that produce
records. The handle preserves bounded-queue backpressure and per-record
delivery results. Stop using all handles before closing the owning producer;
transactional buffered producers reject handles to keep transaction ordering
explicit.

## Direct Consumer

The direct consumer path fetches from explicit topic partitions and offsets.
This is useful when consumer group behavior is not needed.

```rust,no_run
use kafrust::ConsumerConfig;

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut consumer = ConsumerConfig::new(["localhost:9092"])
        .client_id("example-consumer")
        .max_poll_records(500)
        .build()
        .await?;

    consumer.assign("orders", 0, 0);

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

Fetched records expose Kafka RecordBatch headers through `record.headers()`.
`ConsumerRecordHeader::value()` returns an optional byte slice because Kafka
allows null header values; legacy MessageSet records have an empty header list.

Direct and group consumers negotiate Fetch v12, then v11, when the selected
broker advertises those versions. For rack-aware reads, set
`ConsumerConfig::client_rack("rack-a")` (or the matching
`ConsumerGroupConfig` builder). Fetch v12 carries the rack ID using the flexible
schema and follows Kafka's `preferred_read_replica` response when supported;
without a rack, the same session-capable path uses an empty rack ID. Fetch v4
remains the fallback for brokers that do not advertise v11 or v12. The protocol
and injected routing tests pass. The Kafka 3.7.2 three-broker `broker.rack` plus
`RackAwareReplicaSelector` profile is live-qualified in
[`31640494509`](https://github.com/TaeeunKil/kafrust/actions/runs/31640494509).
Fetch v11/v12 requests reuse a broker-scoped fetch session across sequential
polls. Assignment changes, seek/pause/resume, reconnects, and fetch errors
discard the session; the Fetch v4 fallback does not claim session reuse.
The complete 17-job matrix for the general direct-consumer negotiation passed in
[`31673377685`](https://github.com/TaeeunKil/kafrust/actions/runs/31673377685).
The complete 17-job matrix for this path passed in
[`31671783977`](https://github.com/TaeeunKil/kafrust/actions/runs/31671783977),
including the Kafka 3.7.2 three-broker rack-aware follow-up request.

## Consumer Group

The consumer group API is an alpha classic or KIP-848 consumer group path with
dynamic or static membership, range, round-robin, or opt-in cooperative-sticky
assignment for classic groups, join, heartbeat, poll, offset fetch/commit, and
explicit leave support. The
cooperative-sticky path includes protocol, staged assignment, multi-member
ownership transfer, transient-member rollback, and member-loss recovery. These
cooperative failure paths are live-verified in the Kafka `3.7.2` three-broker
profile. The KIP-848 path, including OffsetFetch v9, OffsetCommit v9, and
background heartbeat rejoin, is live-verified against Kafka `4.3.1`; the group
API itself remains pre-`1.0`.

```rust,no_run
use kafrust::ConsumerGroupConfig;

#[tokio::main(flavor = "current_thread")]
async fn main() -> kafrust::Result<()> {
    let mut group = ConsumerGroupConfig::new(["localhost:9092"], "orders-reader")
        .client_id("example-consumer-group")
        .group_instance_id("orders-reader-1")
        .subscribe("orders")
        .join()
        .await?;

    let records = group.poll().await?;
    group.commit_offsets().await?;

    println!("processed {} records", records.len());

    Ok(())
}
```

## Security Protocols

`SecurityProtocol` models Kafka connection modes:

- `Plaintext`
- `Tls`
- `SaslPlaintext`
- `SaslTls`

Plaintext is the default transport. TLS transport is available only when the
non-default `tls` crate feature is enabled:

```toml
kafrust = { version = "0.3", features = ["tls"] }
```

Without that feature, `SecurityProtocol::Tls` returns `Error::Unsupported`
before connecting.

TLS server name validation defaults to the bootstrap host. Use
`tls_server_name(name)` on `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, or
`ConsumerGroupConfig` when the bootstrap address differs from the broker
certificate subject alternative name.

Use `tls_root_certificate_der(bytes)` to add DER-encoded root certificates while
still keeping platform roots enabled.

Mutual TLS uses repeatable `tls_client_certificate_der` calls for the client
certificate chain plus `tls_client_private_key_der` for the private key. The
pair is validated before connection, rejected for plaintext, and the private
key is redacted from `ClientConfig` debug output. Live mTLS broker evidence is
still a release gate.
The repository examples also accept `KAFRUST_TLS_CLIENT_CERT_DER_PATH` and
`KAFRUST_TLS_CLIENT_KEY_DER_PATH` so producer, consumer, group, and Admin
examples use the same mTLS settings as the broker-roundtrip workflow.

SASL credentials can be stored on the shared client configuration with
`sasl_plain(username, password)`, `sasl_scram_sha_256(username, password)`, or
`sasl_scram_sha_512(username, password)`. This does not change the selected
`SecurityProtocol`; choose `SaslPlaintext` or `SaslTls` separately. Credential
debug output redacts the password. The SASL/PLAIN `SaslPlaintext` broker
roundtrip, producer, direct consumer, and consumer group smoke paths are
verified against Kafka `3.7.2`; SASL/SCRAM-SHA-256 and SCRAM-SHA-512 over
`SaslTls` are verified for the documented live smoke paths. The SHA-512 profile
covers broker roundtrip, producer, batch, buffered producer, direct consumer,
and consumer group poll paths.

SASL/OAUTHBEARER token authentication is available through
`sasl_oauthbearer(token)` or
`sasl_oauthbearer_with_username(username, token)`. Async token providers are
available through `sasl_oauthbearer_provider` and
`sasl_oauthbearer_with_username_and_provider`; kafrust calls them for each new
broker authentication. The RFC 7628 initial response and secret redaction are
covered by injected handshake tests, and Kafka 3.7.2 SASL_SSL is covered by a
dedicated smoke using the broker's built-in unsecured validator (`Live Kafka
Smoke` run `31478375106`). OAUTHBEARER initial authentication and provider
re-authentication use Kafka's flexible `SaslAuthenticate v2` wire response;
PLAIN and SCRAM continue to use `v1`. The low-level `Client` exposes the
broker-advertised `session_lifetime_ms` for an application-owned refresh
schedule. Provider-backed OAUTHBEARER connections also re-authenticate on the
existing connection before requests after half of that lifetime. A signed
JWT/JWKS policy passes the Kafka 3.7.2 OIDC fixture, Java client, static-token,
and provider-backed paths in the [`Live Kafka Smoke` OIDC job](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94075906934),
while external provider-specific behavior and detached refresh workers remain
unclaimed. Provider callbacks are bounded by
`ClientConfig::request_timeout_ms`; an expired callback returns
`Error::OAuthBearerTokenTimeout` without including token material.

The default build does not include TLS dependencies. The current `tls` feature
uses the `rustls` ring crypto provider, which can require native build tooling
in some environments; this is not part of the default kafrust toolchain.

## Request Metrics

`ClientMetrics` provides lock-free operational counters shared by all broker
connections and buffered producers created from a configuration:

```rust,no_run
use kafrust::{ClientMetrics, ProducerConfig};

# async fn example() -> kafrust::Result<()> {
let metrics = ClientMetrics::new();
let producer = ProducerConfig::new(["localhost:9092"])
    .metrics(metrics.clone())
    .build()
    .await?;

// Send records with `producer`, then export a point-in-time snapshot.
let snapshot = metrics.snapshot();
println!(
    "requests={} failures={} broker_errors={} max_latency={:?}",
    snapshot.requests_started,
    snapshot.requests_failed,
    snapshot.broker_errors,
    snapshot.max_latency,
);
# Ok(())
# }
```

Snapshots include request success, failure, timeout, and cancellation counts,
non-zero Kafka error codes observed in decoded broker responses, high-level
operation retry attempts, request and response payload bytes, in-flight
requests, current and maximum outstanding buffered records, and total and
maximum latency. Successful produced records, topic-partition Produce chunks,
and records returned by consumer APIs are also counted. Retry attempts cover
producer sends, consumer fetches, metadata reconnects, and transactional
coordinator operations, plus automatic consumer-group rejoins. Individual
atomic fields are sampled independently, so a snapshot taken while requests
are changing is not a transactional view. Request metrics and `tracing` spans
contain operational metadata only; key, value, request, and response payload
contents are not recorded.

Debug-level operation spans cover immediate and buffered producer sends,
flush/close, batch sends, transaction completion and offset attachment,
direct-consumer poll/fetch, and consumer-group join, poll, heartbeat, and
offset commit. Their names use the `kafka.producer.*`, `kafka.consumer.*`, and
`kafka.consumer_group.*` prefixes. Broker `kafka.request` spans execute as
children, so subscribers can attribute wire latency to one user operation.

The buffered producer command queue defaults to 1024 records. Configure it with
`ProducerConfig::buffer_capacity`; values below one become one. When the queue
is full, `BufferedProducer::send` waits for capacity instead of allocating an
unbounded queue or dropping an accepted record.

`ProducerConfig::delivery_timeout_ms` sets the total deadline for immediate and
batch deliveries, including metadata lookup, retries, and retry backoff. It
defaults to 120 seconds and returns `Error::RequestTimedOut` after retiring a
possibly interrupted connection. Buffered records use the same deadline from
enqueue through Produce; `linger_ms` independently controls batching latency.

## Decode Memory Limits

Broker response frame allocation is limited to `100 MiB` by default. Set
`max_response_bytes(bytes)` on `ClientConfig`, `ProducerConfig`,
`ConsumerConfig`, or `ConsumerGroupConfig` when a workload needs a different
limit. A broker frame length above the configured limit returns
`Error::ResponseTooLarge { size, max }` before allocating the response payload.
After a request has been sent, a timeout, transport error, or framing error
retires that low-level connection. High-level retry paths establish a fresh
connection rather than risking reuse of a stream with a partial response.

Kafka arrays are limited to `1,000,000` elements and an uncompressed fetched
record batch is limited to `64 MiB` by default. Configure these limits with
`max_decode_array_elements(elements)` and
`max_decompressed_record_bytes(bytes)` on the same configuration builders.
Declared arrays, record counts, and record headers are checked before reserving
their vectors. Gzip, Snappy, LZ4, and Zstd decoders enforce the batch limit and
return `protocol::Error::LimitExceeded { kind, actual, max }`.

The limit applies independently to each broker request. It must be large enough
for metadata, fetch, and other expected responses; setting it below a valid
response size fails that operation instead of partially decoding it.

## Configuration Validation

High-level builders validate configuration before opening a broker connection.
Call `ClientConfig::validate`, `ProducerConfig::validate`,
`ConsumerConfig::validate`, or `ConsumerGroupConfig::validate` to run the same
startup preflight explicitly before deciding when to connect.
Invalid timeouts, response or decode limits, negative fetch values, zero poll
limits, fetch bounds, poll limits, group IDs or subscriptions, zero commit or
heartbeat intervals, and invalid transaction settings return
`Error::InvalidConfiguration { field, reason }`. Empty bootstrap lists retain
the dedicated `Error::MissingBootstrapServer` variant, while blank bootstrap
entries are reported as invalid configuration. This makes startup failures
deterministic and avoids turning local configuration mistakes into network
retries.

## Compatibility

The `0.3.x` alpha line is verified against single-node Apache Kafka `3.7.2`,
`3.8.1`, `3.9.1`, and `4.3.1` KRaft brokers over `PLAINTEXT`. Secured and
three-broker profiles are verified against Kafka `3.7.2`.

Verified high-level paths include:

- Low-level `Client` `ApiVersions v0` and flexible `ApiVersions v4` capability
  roundtrips with a v3 fallback, plus opt-in v5 cluster/node identity encoding
  and `Metadata v1` roundtrips.
- Producer single-record, batch, and buffered sends.
- Direct topic-partition fetch using Fetch v4 response decoding.
- Classic consumer group join, sync, heartbeat, poll, and offset commit.
- Cooperative-sticky multi-member transfer, transient-member rollback, and
  member-loss recovery in the Kafka `3.7.2` three-broker profile.

## Current Limits

- APIs are pre-`1.0` and can change between minor versions.
- The optional `blocking` feature supplies synchronous direct and
  bounded-buffered producer, direct consumer, consumer-group, Share, Streams,
  and broad broker/controller Admin adapters. A general alternate-runtime
  abstraction remains open, and blocking adapters cannot be called from inside
  a Tokio runtime.
- TLS is feature-gated, currently uses the `rustls` ring crypto provider, and
  is verified for the broker roundtrip, producer, direct consumer, and consumer
  group smoke paths against Kafka `3.7.2`;
  SASL/PLAIN is verified for the `SaslPlaintext` broker roundtrip, producer,
  direct consumer, and consumer group smoke paths.
- SASL/SCRAM-SHA-256 and SCRAM-SHA-512 are verified over `SaslTls` for the
  documented broker roundtrip, producer, direct consumer, and consumer group
  smoke paths; the SHA-512 profile also covers batch and buffered producer
  paths.
- SASL/OAUTHBEARER is implemented with token-only and authorization-identity
  builders plus async token-provider builders, and live-verified against Kafka
  3.7.2's built-in validator and a signed local OIDC/JWKS fixture. Flexible
  SASL Authenticate v2 session-lifetime metadata is exposed by the low-level
  client, and provider-backed connections re-authenticate before requests as
  the session expires. External provider-specific behavior and detached
  refresh workers remain unclaimed.
- Broker compatibility is verified against Kafka `3.7.2`, `3.8.1`, `3.9.1`,
  and `4.3.1` for the single-node plaintext profile. Secured and multi-broker
  profiles are verified against `3.7.2`.
- Multi-broker clusters, coordinator and leader failover, and partition
  expansion are verified in the documented `3.7.2` profiles. Rack-aware client
  routing prefers flexible Fetch v12, falls back through Fetch v11 to Fetch v4,
  and follows the preferred-replica response. The three-broker `3.7.2`
  rack-aware profile is live-qualified in
  [`31640494509`](https://github.com/TaeeunKil/kafrust/actions/runs/31640494509).
- Idempotent single-record, batch, and buffered sends are available through
  `ProducerConfig::enable_idempotence(true)`. Transactional immediate and batch
  sends support explicit begin, commit, and abort.
  If an immediate or batch idempotent send is canceled after Produce
  transmission, the producer returns `Error::IdempotentProducerDefunct` on the
  next operation and must be discarded so its uncertain sequence is not reused.
  Transaction coordinator partition/group registration prefers flexible
  `AddPartitionsToTxn` and `AddOffsetsToTxn` v3 on brokers that advertise it,
  with v0 fallback for Kafka 3.7-era compatibility.
  `IsolationLevel::ReadCommitted` hides aborted transaction records for direct
  and group consumers, and current group assignments can be committed through
  generation-fenced `Producer::send_group_offsets_to_transaction`.
  Transactional buffered sends and controller-routed partition reassignment are
  available. SCRAM credential administration and partition reassignment are
  available through typed `AdminClient` APIs; both have Kafka 3.7.2 live smoke
  paths, including three-broker reassignment completion polling.
  Controller-routed preferred and one-shot unclean leader elections are also
  available through `AdminClient::elect_leaders`; unclean election is an
  explicit data-loss-sensitive operation and should be used only with a
  recovery policy.
  Broker-local storage inspection is available through
  `AdminClient::describe_log_dirs`, including replica size, offset lag, future
  logs, and negotiated volume capacity fields.
  Broker-local replica movement is available through
  `AdminClient::alter_replica_log_dirs`; the request is explicitly targeted at
  one broker and is not replayed after an ambiguous send.
  Shared request, retry, broker-error, producer, consumer, batch, and
  buffered-queue metrics are available together with high-level operation and
  request spans.
- `acks=0` fire-and-forget sends are supported for immediate and batch
  producer paths. The request is written and flushed without waiting for a
  broker response, so returned offsets are `-1` and broker acceptance or
  partition-level errors cannot be confirmed. This path is live-verified
  against Kafka `3.7.2`, `3.8.1`, `3.9.1`, and `4.3.1` single-node plaintext
  profiles.

## Examples

Run examples from the repository with a local Kafka broker:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_TOPIC=kafrust-smoke \
cargo run -p kafrust --example producer_send
```

The smoke examples accept `KAFRUST_SECURITY_PROTOCOL`,
`KAFRUST_SASL_USERNAME`, `KAFRUST_SASL_PASSWORD`, and
`KAFRUST_SASL_MECHANISM` so the same examples can be run against plaintext,
TLS, and SASL broker profiles. `KAFRUST_SASL_MECHANISM` defaults to `plain` and
also accepts `scram-sha-256`, `scram-sha-512`, or `oauthbearer`. The
OAUTHBEARER path reads its token from `KAFRUST_SASL_TOKEN` and treats
`KAFRUST_SASL_USERNAME` as an optional authorization identity. Set
`KAFRUST_SASL_TOKEN_PATH` instead to use the example's provider-backed file
callback, which reads a fresh token for each broker authentication.
`KAFRUST_BOOTSTRAP_SERVERS` accepts Kafka's comma-separated bootstrap format for
multiple brokers, for example `localhost:19092,localhost:19093`.

Available examples include:

- `broker_roundtrip`
- `producer_send`
- `producer_send_batch`
- `producer_buffered`
- `producer_transactional`
- `consumer_fetch`
- `find_group_coordinator`
- `consumer_group_poll`
- `admin_reassign_partitions`
- `admin_elect_leaders`
- `admin_describe_log_dirs`
- `admin_alter_replica_log_dirs`

## Project Docs

- Repository: <https://github.com/TaeeunKil/kafrust>
- Roadmap: <https://github.com/TaeeunKil/kafrust/blob/main/docs/roadmap.md>
- Compatibility: <https://github.com/TaeeunKil/kafrust/blob/main/docs/compatibility.md>
- API stability: <https://github.com/TaeeunKil/kafrust/blob/main/docs/api-stability.md>
- Public API audit: <https://github.com/TaeeunKil/kafrust/blob/main/docs/public-api-audit.md>
- Project strategy: <https://github.com/TaeeunKil/kafrust/blob/main/docs/project-strategy.md>
