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

## Static Membership

Set `ConsumerGroupConfig::group_instance_id` to give a process a stable
identity across restarts:

```rust
use kafrust::ConsumerGroupConfig;

# async fn example() -> kafrust::Result<()> {
let group = ConsumerGroupConfig::new(["localhost:9092"], "orders-service")
    .group_instance_id("orders-service-1")
    .subscribe("orders")
    .join()
    .await?;

assert_eq!(
    group.metadata().group_instance_id(),
    Some("orders-service-1")
);
# Ok(())
# }
```

Static members use `JoinGroup v5`, `SyncGroup v3`, `Heartbeat v3`, and
`OffsetCommit v7`. The instance ID must be non-empty and unique among active
members of the same group. Kafka error 82 (`FENCED_INSTANCE_ID`) is returned
to the application instead of being treated as rejoinable; this normally means
two processes use the same instance ID.

## Join And Assignment

`ConsumerGroupConfig::join` discovers the coordinator, sends `JoinGroup v2`
and `SyncGroup v2` for dynamic members or v5/v3 for static members, computes
range assignments if this member is the leader, fetches committed offsets for
assigned partitions, and builds a direct `Consumer` for fetching records.

The alpha path uses the classic consumer group protocol. Range assignment is
the default; `ConsumerGroupConfig::assignment_strategy` can select
`ConsumerGroupAssignmentStrategy::RoundRobin`. Assignment state keeps Kafka
group ID, member ID, generation ID, topic, partition, and next offset visible
through the public API.

## Offset Reset

Committed offsets always take precedence. For a newly assigned partition with
no committed offset, select a reset policy explicitly:

```rust
use kafrust::{ConsumerGroupConfig, OffsetResetPolicy};

# async fn example() -> kafrust::Result<()> {
let group = ConsumerGroupConfig::new(["localhost:9092"], "orders-service")
    .offset_reset_policy(OffsetResetPolicy::Earliest)
    .subscribe("orders")
    .join()
    .await?;
# Ok(())
# }
```

`Earliest` and `Latest` fetch fresh topic metadata, route `ListOffsets v1` to
each assigned partition leader, and surface topic- or partition-scoped broker
errors. `Offset(n)` starts uncommitted partitions at an explicit absolute
offset. The default remains `Offset(0)` for compatibility with the pre-policy
API, and the existing `start_offset(n)` builder is equivalent to
`offset_reset_policy(OffsetResetPolicy::Offset(n))`.

## Polling And Rejoin

`ConsumerGroup::poll` sends a foreground heartbeat before fetching assigned partitions. If that heartbeat reports a rebalance, stale generation, stale member ID, stale coordinator, coordinator connection I/O error, or coordinator request timeout, `poll` rejoins the group before fetching records.

Use `ConsumerGroup::spawn_heartbeat_task` when application work between polls can approach the group session timeout. It starts an opt-in background heartbeat loop on a separate coordinator connection. The returned `ConsumerGroupHeartbeat` records the group ID, member ID, and generation ID it belongs to.

Pass the heartbeat handle to `ConsumerGroup::poll_with_heartbeat` when a
background heartbeat task is running. This checks for early task completion
before polling. If the task ended with a rejoinable group error,
`poll_with_heartbeat` rejoins and replaces the handle with a new task using the
same interval. If the group rejoins through another path before or during the
foreground poll, `poll_with_heartbeat` stops the stale same-group task and
replaces it for the current member and generation.

Call `ConsumerGroupHeartbeat::try_wait` to observe early task completion without polling. Call `ConsumerGroupHeartbeat::stop` to shut the task down and observe any broker error returned by the task.

The `consumer_group_heartbeat_rejoin` example starts two members concurrently
to force a classic-group rebalance and verifies that the first member's mutable
heartbeat handle is replaced with its new member and generation identity.

## Leaving

Call `ConsumerGroup::leave` when processing is complete. It consumes the group
handle and sends LeaveGroup v3 with both the broker member ID and any configured
static instance ID, allowing Kafka to remove the member without waiting for the
session timeout. Stop a separately spawned background heartbeat handle before
calling `leave`.

## Offset Commits

`ConsumerGroup::commit_offsets` commits the current next offsets for assigned partitions. If Kafka reports a rejoinable generation, member, rebalance, or coordinator error, or if the coordinator request fails with I/O or timeout, `commit_offsets` rejoins the group and returns the original commit error instead of retrying the old assignment offsets under the new generation. After that, callers should poll the refreshed assignment state before deciding whether to commit again.

Current implementation status:

- `ConsumerGroupConfig::isolation_level` forwards `ReadUncommitted` or
  `ReadCommitted` to the group's fetch consumer. Read-committed polls hide
  aborted transaction records and Kafka control records.
- `ConsumerGroup::metadata` and `ConsumerGroup::assignments` can be passed to
  `Producer::send_group_offsets_to_transaction` after polling so the current
  next offsets are committed atomically with transactional output records and
  fenced by the current generation and member identity.
- FindCoordinator v1 request/response protocol types exist.
- `Client::find_group_coordinator` can ask Kafka for a group coordinator.
- JoinGroup v2 request/response protocol types exist.
- SyncGroup v2 request/response protocol types exist.
- Heartbeat v2 request/response protocol types exist.
- Static member JoinGroup v5, SyncGroup v3, Heartbeat v3, and OffsetCommit v7
  protocol types and high-level routing exist.
- `ConsumerGroup::leave` uses LeaveGroup v3 for dynamic and static members and
  preserves top-level and member-scoped broker errors.
- `Client::join_group_v2`, `Client::sync_group_v2`, and `Client::heartbeat_v2` can send coordinator-scoped group membership requests.
- Classic consumer protocol subscription and assignment v0 payloads can be encoded and decoded for JoinGroup/SyncGroup metadata.
- Internal range assignment can compute SyncGroup assignment payloads from JoinGroup member subscriptions and topic metadata.
- Internal round-robin assignment follows sorted member/topic/partition order
  and skips members that do not subscribe to the current topic.
- OffsetFetch v2 request/response protocol types exist.
- ListOffsets v1 request/response protocol types exist and the group join path
  uses them for leader-routed earliest/latest offset reset.
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
- `ConsumerGroup::poll_with_heartbeat` observes a background heartbeat task
  before polling, rejoins when the task ended with a rejoinable group error,
  and replaces completed or stale same-group heartbeat handles with a new task
  for the current member and generation using the original interval.

Run the opt-in coordinator example against a local broker:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-group cargo run -p kafrust --example find_group_coordinator
```

Run the opt-in group poll example:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-group KAFRUST_TOPIC=orders cargo run -p kafrust --example consumer_group_poll
```

Add `KAFRUST_GROUP_INSTANCE_ID=orders-reader-1` to run the example as a static
group member. Set `KAFRUST_ASSIGNMENT_STRATEGY=roundrobin` to select the
round-robin assignor.

Run the earliest/latest reset verification example:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-reset KAFRUST_TOPIC=orders cargo run -p kafrust --example consumer_group_offset_reset
```

The opt-in broker roundtrip test also covers coordinator discovery when `KAFRUST_BOOTSTRAP_SERVERS` is set.
