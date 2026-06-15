# Consumer Group Direction

Consumer groups are being added incrementally after the direct consumer path. The alpha path supports the classic consumer group protocol with the range assignor.

```rust
use kafrust::ConsumerGroupConfig;
use std::time::Duration;

let mut group = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
    .client_id("orders-reader")
    .request_timeout_ms(30_000)
    .max_retries(1)
    .max_poll_records(500)
    .subscribe("orders")
    .join()
    .await?;

let mut heartbeat = group
    .spawn_heartbeat_task(Duration::from_secs(3))
    .await?;
let records = group.poll_with_heartbeat(&mut heartbeat).await?;
group.commit_offsets().await?;
heartbeat.stop().await?;
```

Offset fetches and commits are coordinator-scoped Kafka requests. The lower-level `Client` methods remain available for protocol-focused experiments, but the alpha user path is `ConsumerGroupConfig`.

## Join And Assignment

`ConsumerGroupConfig::join` discovers the coordinator, sends `JoinGroup v2`, computes range assignments if this member is the leader, sends `SyncGroup v2`, fetches committed offsets for assigned partitions, and builds a direct `Consumer` for fetching records.

The alpha path uses the classic consumer group protocol with range assignment. Assignment state keeps Kafka group ID, member ID, generation ID, topic, partition, and next offset visible through the public API.

## Polling And Rejoin

`ConsumerGroup::poll` sends a foreground heartbeat before fetching assigned partitions. If that heartbeat reports a rebalance, stale generation, stale member ID, stale coordinator, coordinator connection I/O error, or coordinator request timeout, `poll` rejoins the group before fetching records.

Use `ConsumerGroup::spawn_heartbeat_task` when application work between polls can approach the group session timeout. It starts an opt-in background heartbeat loop on a separate coordinator connection. The returned `ConsumerGroupHeartbeat` records the group ID, member ID, and generation ID it belongs to.

Pass the heartbeat handle to `ConsumerGroup::poll_with_heartbeat` when a background heartbeat task is running. This checks for early task completion before polling. If the task ended with a rejoinable group error, `poll_with_heartbeat` rejoins before polling. If the group already rejoined through another path, `poll_with_heartbeat` stops the stale same-group heartbeat task before polling so the old generation does not keep sending heartbeats.

Call `ConsumerGroupHeartbeat::try_wait` to observe early task completion without polling. Call `ConsumerGroupHeartbeat::stop` to shut the task down and observe any broker error returned by the task.

## Offset Commits

`ConsumerGroup::commit_offsets` commits the current next offsets for assigned partitions. If Kafka reports a rejoinable generation, member, rebalance, or coordinator error, or if the coordinator request fails with I/O or timeout, `commit_offsets` rejoins the group and returns the original commit error instead of retrying the old assignment offsets under the new generation. After that, callers should poll the refreshed assignment state before deciding whether to commit again.

Current implementation status:

- FindCoordinator v1 request/response protocol types exist.
- `Client::find_group_coordinator` can ask Kafka for a group coordinator.
- JoinGroup v2 request/response protocol types exist.
- SyncGroup v2 request/response protocol types exist.
- Heartbeat v2 request/response protocol types exist.
- `Client::join_group_v2`, `Client::sync_group_v2`, and `Client::heartbeat_v2` can send coordinator-scoped group membership requests.
- Classic consumer protocol subscription and assignment v0 payloads can be encoded and decoded for JoinGroup/SyncGroup metadata.
- Internal range assignment can compute SyncGroup assignment payloads from JoinGroup member subscriptions and topic metadata.
- OffsetFetch v2 request/response protocol types exist.
- OffsetCommit v2 request/response protocol types exist.
- `Client::offset_fetch_v2` and `Client::offset_commit_v2` can send coordinator-scoped offset requests.
- `ConsumerGroupConfig`, `ConsumerGroup`, and `ConsumerGroupHeartbeat` provide a minimal join, sync, heartbeat, background heartbeat, poll, rejoin, and commit path.
- `ConsumerGroupConfig::request_timeout_ms` controls coordinator, metadata, fetch, heartbeat, and commit request timeouts.
- `ConsumerGroupConfig::security_protocol` stores the Kafka security protocol for coordinator and fetch connections. `Plaintext` is the default transport; TLS requires the non-default `tls` crate feature; `tls_server_name(name)` overrides the certificate validation name when the bootstrap host differs from the broker certificate; `tls_root_certificate_der(bytes)` adds DER-encoded root certificates while keeping platform roots enabled; `sasl_plain(username, password)`, `sasl_scram_sha_256(username, password)`, and `sasl_scram_sha_512(username, password)` provide SASL credentials for `SaslPlaintext` or `SaslTls`.
- `ConsumerGroupConfig::max_retries` is passed through to the direct fetch path after group assignment.
- `ConsumerGroupConfig::max_poll_records` is passed through to the direct poll path after group assignment.
- Broker error codes can be classified with `BrokerErrorKind` for common coordinator, generation, and rebalance errors.
- Consumer group join, heartbeat, background heartbeat, rejoin, and commit operations emit `tracing` events with operational metadata.
- Rebalance handling can rejoin during `ConsumerGroup::poll` after coordinator, generation, member, or rebalance heartbeat errors.
- Offset commit handling rejoins on coordinator, generation, member, rebalance, coordinator I/O, or coordinator timeout commit errors and returns the original commit error so callers can decide when to poll and commit again.
- `ConsumerGroup::poll_with_heartbeat` observes a background heartbeat task before polling, rejoins when the task ended with a rejoinable group error, and stops same-group heartbeat handles that belong to an older member ID or generation ID. It does not restart a new background heartbeat task after rejoin.

Run the opt-in coordinator example against a local broker:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-group cargo run -p kafrust --example find_group_coordinator
```

Run the opt-in group poll example:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-group KAFRUST_TOPIC=orders cargo run -p kafrust --example consumer_group_poll
```

The opt-in broker roundtrip test also covers coordinator discovery when `KAFRUST_BOOTSTRAP_SERVERS` is set.
