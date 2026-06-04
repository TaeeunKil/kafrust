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

Under the hood, `ConsumerGroupConfig::join` discovers the coordinator, sends `JoinGroup v2`, computes range assignments if this member is the leader, sends `SyncGroup v2`, fetches committed offsets for assigned partitions, and builds a direct `Consumer` for fetching records. `ConsumerGroup::poll` sends a heartbeat before fetching assigned partitions, rejoins the group when the heartbeat reports a rebalance, stale generation, stale member ID, or stale coordinator, and `ConsumerGroup::commit_offsets` commits the current next offsets. `ConsumerGroup::spawn_heartbeat_task` is an opt-in background heartbeat loop on a separate coordinator connection; pass the handle to `ConsumerGroup::poll_with_heartbeat` to observe task completion before polling and rejoin when the task ended with a rejoinable group error. Call `ConsumerGroupHeartbeat::try_wait` to observe early task completion without polling, and call `ConsumerGroupHeartbeat::stop` to shut it down and observe any broker error returned by the task.

Offset fetches and commits are coordinator-scoped Kafka requests. The lower-level `Client` methods remain available for protocol-focused experiments, but the alpha user path is `ConsumerGroupConfig`.

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
- `ConsumerGroupConfig::max_retries` is passed through to the direct fetch path after group assignment.
- `ConsumerGroupConfig::max_poll_records` is passed through to the direct poll path after group assignment.
- Broker error codes can be classified with `BrokerErrorKind` for common coordinator, generation, and rebalance errors.
- Consumer group join, heartbeat, background heartbeat, rejoin, and commit operations emit `tracing` events with operational metadata.
- Rebalance handling can rejoin during `ConsumerGroup::poll` after coordinator, generation, member, or rebalance heartbeat errors.
- `ConsumerGroup::poll_with_heartbeat` observes a background heartbeat task before polling and rejoins when the task ended with a rejoinable group error. It does not restart a new background heartbeat task after rejoin.

Run the opt-in coordinator example against a local broker:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-group cargo run -p kafrust --example find_group_coordinator
```

Run the opt-in group poll example:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-group KAFRUST_TOPIC=orders cargo run -p kafrust --example consumer_group_poll
```

The opt-in broker roundtrip test also covers coordinator discovery when `KAFRUST_BOOTSTRAP_SERVERS` is set.
