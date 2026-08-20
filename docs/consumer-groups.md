# Consumer Group Direction

Consumer groups are being added incrementally after the direct consumer path. The alpha path supports the classic consumer group protocol with range, round-robin, eager sticky, and an opt-in cooperative-sticky assignor, plus a selectable KIP-848 consumer protocol path.

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

`ConsumerGroupHeartbeat::stop` sends cancellation to the background task and
waits for it to finish. Cancellation also interrupts an in-flight heartbeat
request, so shutdown does not wait for the broker request timeout when the
broker has stopped responding. Dropping the handle requests the same shutdown
and aborts the task as a final cleanup fallback.

Offset fetches and commits are coordinator-scoped Kafka requests. The lower-level `Client` methods remain available for protocol-focused experiments, but the alpha user path is `ConsumerGroupConfig`.

## Regex Topic Subscription

Use `subscribe_pattern` when a group should follow a family of topic names:

```rust
use kafrust::ConsumerGroupConfig;

# async fn example() -> kafrust::Result<()> {
let mut group = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
    .subscribe_pattern(r"^orders-[0-9]+$")
    .join()
    .await?;

let records = group.poll().await?;
# let _ = records;
# Ok(())
# }
```

This is client-side regular-expression matching. Before every classic or
KIP-848 join/rejoin, kafrust requests the broker's Metadata v1 topic list,
filters successful topic entries with the Rust `regex` engine, sorts and
deduplicates the result, and sends the concrete topic names in the group
subscription. A rejoin therefore sees topics created after the previous join.
The configured pattern must match at least one visible topic, and the client
must have permission to discover those topics. `subscribe_pattern` replaces
concrete subscriptions; calling `subscribe` afterwards replaces the pattern.
Use `topic_pattern_ref` when inspecting a configured pattern and `topics()`
when inspecting concrete topic names.

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
`ConsumerGroupAssignmentStrategy::RoundRobin`,
`ConsumerGroupAssignmentStrategy::Sticky`, or
`ConsumerGroupAssignmentStrategy::CooperativeSticky`. Eager sticky members
advertise the previous assignment in Subscription v0 `user_data`, preserve
valid ownership where possible, rebalance to a deterministic near-even
assignment, and apply transfers in the current SyncGroup response. Cooperative
sticky members instead advertise Subscription v1 owned partitions and stage
transfers across rejoin cycles. The two strategies must not be mixed in one
classic group. Multi-member cooperative transfer, member-loss, and rollback
behavior are live-qualified in the documented three-broker profile, while
target-workload callback timing still requires qualification.
Assignment state keeps Kafka group ID, member ID, generation ID, topic,
partition, and next offset visible through the public API.

If a KIP-848 member does not receive an assignment before
`rebalance_timeout_ms`, `join` returns
`Error::ConsumerGroupAssignmentTimeout { timeout_ms }`. This is a typed
deadline failure rather than an unsupported-feature result, so applications
can distinguish delayed assignment from protocol or broker errors.

## Rebalance Listener

Applications that need an explicit assignment lifecycle can register a
synchronous listener:

```rust
use kafrust::{ConsumerGroupConfig, RebalancePhase};

let group = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
    .rebalance_listener(|event| match event.phase() {
        RebalancePhase::Before => println!("leaving {} assignments", event.assignments().len()),
        RebalancePhase::After => println!("received {} assignments", event.assignments().len()),
    })
    .subscribe("orders");
```

`RebalanceListener` receives an `After` snapshot for initial join and
`Before`/`After` snapshots for classic and KIP-848 rejoin plus broker-assigned
KIP-848 assignment changes. Callbacks run synchronously on the task invoking
`join`, `heartbeat`, `poll`, or `poll_with_heartbeat`; keep them bounded and do
not re-enter the same group handle from the callback.

## KIP-848 Consumer Protocol

Select Kafka's newer broker-side consumer protocol explicitly:

```rust
use kafrust::{ConsumerGroupConfig, ConsumerGroupProtocol};

# async fn example() -> kafrust::Result<()> {
let mut group = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
    .group_protocol(ConsumerGroupProtocol::Consumer)
    .subscribe("orders")
    .join()
    .await?;

let records = group.poll().await?;
group.commit_offsets().await?;
group.leave().await?;
# let _ = records;
# Ok(())
# }
```

The high-level path uses `ConsumerGroupHeartbeat v0` (API key 68), Metadata
v12 topic UUIDs, broker-side assignment, member epochs, assignment updates,
foreground and background heartbeat/rejoin, offset fetch/commit, and explicit
heartbeat leave.
`ConsumerGroupAssignmentStrategy` is a classic-protocol setting and must stay
at its default when selecting KIP-848; use `server_assignor` to request a
broker-side assignor. When `poll_with_heartbeat` receives a KIP-848 background
response, the heartbeat task shares member epoch and assignment state with the
owning group handle. Updated assignments are applied by the foreground group
handle, while a `null` assignment response preserves the current assignment.
The session token changes on rejoin so an older heartbeat task is stopped before
it can send requests for the new member epoch. Kafka 4.x compatibility is
scoped to the verified Kafka 4.3.1 profile documented in
`docs/compatibility.md`.

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

The same `Earliest` and `Latest` policies also recover a committed assignment
when its next offset is no longer retained and the broker returns
`OFFSET_OUT_OF_RANGE`: the group fetch resolves the current low watermark or
log end and retries once. `ConsumerGroup::poll` inherits this behavior from
the direct consumer path; explicit `Offset(n)` keeps the broker error visible
instead of silently moving the committed position.

## Position Control

`ConsumerGroup` exposes `position`, `seek`, `pause`, and `resume` for its
current assignment. These operations change local fetch state and do not send
offset commits. Pause state survives a group rejoin when this member keeps the
same topic partition, and a seek position is preserved when that partition
remains assigned across an automatic or explicit rejoin. A newly assigned
partition starts from its broker-committed offset or configured reset policy;
local position is not copied from a partition that was removed and later
reassigned.

```rust
let assignment = group.assignments().first().unwrap();
let topic = assignment.topic().to_owned();
let partition = assignment.partition();

group.pause(&topic, partition)?;
group.seek(&topic, partition, 0)?;
group.resume(&topic, partition)?;
```

Assignment changes remain Kafka-visible: operations against a partition this
member does not currently own return `Error::UnassignedTopicPartition`.

For independent processing of one current assignment, use the same bounded
partition queue API as the direct consumer:

```rust
let assignment = group.assignments().first().unwrap();
let topic = assignment.topic().to_owned();
let partition = assignment.partition();
let mut partition_queue = group.split_partition_queue(&topic, partition)?;

group.poll().await?;
while let Some(record) = partition_queue.try_recv() {
    process(record)?;
}
```

Configure the queue bound with
`ConsumerGroupConfig::partition_queue_capacity`. A full queue returns
`Error::PartitionQueueFull`; the group assignment position remains at the
first record that could not be accepted. When a rejoin removes or resets a
partition, its split queue is closed and the application must create a new
queue for the new assignment.

`ConsumerGroup::fetch_watermarks` delegates to the group's direct consumer and
does not require the requested partition to be in the current assignment. It
returns the earliest retained offset and the latest log-end offset without
changing local position or committed offsets.

`ConsumerGroup::offset_for_leader_epoch` exposes the same explicit epoch
recovery lookup for assigned or unassigned partitions. It returns the broker's
reported epoch end offset without changing group position or committing an
offset.

When a fetched RecordBatch supplies a partition leader epoch, group OffsetCommit
v7/v9 requests carry that observed epoch instead of always sending `-1`. This
keeps committed offsets tied to the broker epoch while preserving `-1` for
legacy MessageSet or otherwise unknown epochs.

## Polling And Rejoin

`ConsumerGroup::poll` sends a foreground heartbeat before fetching assigned partitions. If that heartbeat reports a rebalance, stale generation, stale member ID, stale coordinator, coordinator connection I/O error, or coordinator request timeout, `poll` rejoins the group before fetching records.

Applications can also request a rejoin explicitly:

```rust
group.rejoin().await?;
```

An explicit rejoin refreshes client-side regex topic discovery before joining,
rebuilds the current assignment, preserves pause state for partitions that
remain assigned, and retains queued per-record commit offsets only for those
partitions. Rebalance callbacks receive the same `Before` and `After` events as
an automatic rejoin.

Use `ConsumerGroup::spawn_heartbeat_task` when application work between polls can approach the group session timeout. It starts an opt-in background heartbeat loop on a separate coordinator connection. The returned `ConsumerGroupHeartbeat` records the group ID, member ID, and generation ID it belongs to. For KIP-848 groups, member epoch and broker assignment updates are shared with the owning group and applied by `poll_with_heartbeat`.

Pass the heartbeat handle to `ConsumerGroup::poll_with_heartbeat` when a
background heartbeat task is running. This checks for early task completion
before polling. If the task ended with a rejoinable group error,
`poll_with_heartbeat` rejoins and replaces the handle with a new task using the
same interval. If the group rejoins through another path before or during the
foreground poll, `poll_with_heartbeat` stops the stale same-group task and
replaces it for the current member and generation.

Call `ConsumerGroupHeartbeat::try_wait` to observe early task completion without polling. Call `ConsumerGroupHeartbeat::stop` to shut the task down and observe any broker error returned by the task.

The `consumer_group_heartbeat_rejoin` example starts two members concurrently
to force a group rebalance and verifies that the first member's mutable
heartbeat handle tracks the new member epoch. The Kafka 4.3.1 KIP-848 version
of this scenario is live-verified in `Live Kafka Smoke` run `31492612082`.

## Leaving

Call `ConsumerGroup::leave` when processing is complete. It consumes the group
handle and sends LeaveGroup v3 with both the broker member ID and any configured
static instance ID, allowing Kafka to remove the member without waiting for the
session timeout. Stop a separately spawned background heartbeat handle before
calling `leave`.

## Offset Commits

`ConsumerGroup::commit_offsets` commits the current next offsets for assigned partitions. If Kafka reports a rejoinable generation, member, rebalance, or coordinator error, or if the coordinator request fails with I/O or timeout, `commit_offsets` rejoins the group and returns the original commit error instead of retrying the old assignment offsets under the new generation. After that, callers should poll the refreshed assignment state before deciding whether to commit again.

For per-record processing, queue the record's next offset and flush the queue
explicitly:

```rust
for record in &records {
    process(record.value())?;
    group.commit_record(record)?;
}
group.commit_queued_offsets().await?;
```

`commit_record` performs no network I/O and coalesces offsets independently per
topic partition, keeping only the greatest next offset. `commit_queued_offsets`
sends only currently assigned queued partitions using the current group
generation. A queued partition lost during rejoin is discarded; a failed flush
keeps the queue so the caller can retry after observing the returned error.
`commit_offsets` also clears queued offsets that it covers. This is an explicit
flush queue.

For interval-based queued commits, start the bounded background worker after
joining:

```rust
use std::time::Duration;

let mut commit_worker = group
    .spawn_commit_worker(Duration::from_secs(1))
    .await?;

for record in &records {
    process(record.value())?;
    group.commit_record(record)?;
}

while group.pending_commit_count() != 0 {
    if commit_worker.try_wait().await?.is_some() {
        return Err(kafrust::Error::Unsupported(
            "background commit worker stopped before flushing offsets",
        ));
    }
    tokio::time::sleep(Duration::from_millis(25)).await;
}

commit_worker.stop().await?;
group.leave().await?;
```

The worker coalesces offsets per topic-partition, retries transport and
coordinator-transition errors within `max_retries`, and shares the current
generation, member identity, protocol, and assignment state across
`ConsumerGroup::rejoin`. `try_wait` exposes terminal commit or generation
errors. `stop` should be called before application shutdown; `leave` also waits
for a signaled worker to finish before sending LeaveGroup. This is an opt-in
commit worker, not a broker-side auto-commit setting.

KIP-848 groups prefer OffsetCommit v10 when the coordinator advertises the
UUID-based Kafka 4.x schema and the assignment has complete topic IDs. The
member epoch remains in `GenerationIdOrMemberEpoch`; brokers without v10 use
OffsetCommit v9. Classic groups retain the existing v2/v7 routing. KIP-848
assignment initialization
negotiates OffsetFetch v10 when the coordinator advertises it and Metadata
v12 returned a complete set of non-zero topic UUIDs. The UUID response is
mapped back to the existing topic-name assignment model. Brokers that only
advertise OffsetFetch v9, or assignments without complete UUID metadata, use
OffsetFetch v9 with the member ID and member epoch. Classic assignment
initialization continues to use OffsetFetch v2.

For member-aware Admin offset operations, `ConsumerGroup::topic_id(topic)`
exposes the UUID cached during this assignment. Pass it to
`ConsumerGroupOffsetQuery::topic_id` and `ConsumerGroupOffset::topic_id` to
avoid an extra metadata lookup. Name-only Admin calls resolve topic UUIDs from
Metadata v12 when the coordinator supports v10, and otherwise retain the v9
fallback.

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
- KIP-848 `ConsumerGroupHeartbeat v0` request/response protocol types exist,
  including flexible headers, UUID topic partitions, nullable arrays, and
  tagged fields.
- Metadata v12 exposes UUID-to-name mappings needed to apply KIP-848
  assignments.
- `Client::consumer_group_heartbeat_v0` can send the KIP-848 heartbeat over a
  coordinator-scoped connection; focused injected-broker coverage verifies
  the request and response framing.
- `ConsumerGroupProtocol::Consumer` provides foreground join, assignment
  application, heartbeat/rejoin, background heartbeat state sharing, offset
  commit, and explicit leave behavior; its dedicated live broker qualification
  remains pending.
- Static member JoinGroup v5, SyncGroup v3, Heartbeat v3, and OffsetCommit v7
  protocol types and high-level routing exist.
- `ConsumerGroup::leave` uses LeaveGroup v3 for dynamic and static members and
  preserves top-level and member-scoped broker errors.
- `Client::join_group_v2`, `Client::sync_group_v2`, and `Client::heartbeat_v2` can send coordinator-scoped group membership requests.
- Classic consumer protocol subscription and assignment v0 payloads can be encoded and decoded for JoinGroup/SyncGroup metadata.
- Internal range assignment can compute SyncGroup assignment payloads from JoinGroup member subscriptions and topic metadata.
- Internal round-robin assignment follows sorted member/topic/partition order
  and skips members that do not subscribe to the current topic.
- Sticky Subscription v0 `user_data` encoding accepts Kafka's version 0 and
  version 1 previous-assignment schemas, keeps generation metadata when
  available, and applies eager transfers in the current assignment. Leader-side
  subscription parsing accepts the append-only classic envelope through
  versions 0, 1, 2, and 3, including generation and rack fields. Focused tests
  cover the wire bytes, generation roundtrip, versioned envelope, balance, and
  immediate transfer behavior.
- Cooperative-sticky Subscription v1 encoding and staged ownership transfer
  are implemented with focused assignment tests. Kafka 3.7.2 three-broker
  protocol/example coverage passed in `Live Kafka Smoke` run `31464021305`;
  multi-member failure qualification remains pending.
- OffsetFetch v2 request/response protocol types exist.
- ListOffsets v1 request/response protocol types exist and the group join path
  uses them for leader-routed earliest/latest offset reset.
- OffsetCommit v2 request/response protocol types exist.
- `Client::offset_fetch_v2` and `Client::offset_commit_v2` can send coordinator-scoped offset requests.
- `ConsumerGroupConfig`, `ConsumerGroup`, and `ConsumerGroupHeartbeat` provide a minimal join, sync, heartbeat, background heartbeat, poll, rejoin, and commit path.
- Group assignments expose local position, seek, pause, and resume controls.
- Group consumers expose assignment-independent partition watermark queries.
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
- KIP-848 background heartbeats share member epoch and broker assignment state
  with the owning group. Assignment updates are applied once per heartbeat
  response, and nullable assignment responses preserve the existing local
  assignment. Rejoin session tokens prevent an older task from updating a new
  group member.

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
round-robin assignor, `sticky` to select Kafka's eager sticky assignor, or
`cooperative-sticky` to select the cooperative assignor.

Run the earliest/latest reset verification example:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-reset KAFRUST_TOPIC=orders cargo run -p kafrust --example consumer_group_offset_reset
```

The opt-in broker roundtrip test also covers coordinator discovery when `KAFRUST_BOOTSTRAP_SERVERS` is set.

The `consumer_group_offset_reset` example also qualifies a controlled retained-log
boundary for classic groups. In the Kafka 3.7.2 three-broker profile, it commits
an offset, uses Admin `DeleteRecords` to move the low watermark beyond that
offset, and verifies that `OffsetResetPolicy::Earliest` recovers the group at
the retained boundary and reads a post-delete record. This does not claim
arbitrary retention timing or unclean-election data-loss recovery.

The group implementation also has a development-only combined-fault smoke path:
the live matrix can place the target partition leader on the group coordinator,
stop that one broker, and verify foreground rejoin plus consumption of a record
written by the replacement leader. The Kafka 3.7.2 plaintext classic path and
the Kafka 4.3.1 plaintext KIP-848 path are qualified in
[`Live Kafka Smoke` run `31723663771`](https://github.com/TaeeunKil/kafrust/actions/runs/31723663771).
The KIP-848 path uses the same protocol-selectable example with
`KAFRUST_GROUP_PROTOCOL=consumer`. The Kafka 3.7.2 classic combined path also
passed over authenticated `SASL_PLAINTEXT` in
[`Live Kafka Smoke` run `31725607371`](https://github.com/TaeeunKil/kafrust/actions/runs/31725607371).
The Kafka 4.3.1 KIP-848 combined path also passed over authenticated
`SASL_SSL` with SCRAM-SHA-256 in
[`Live Kafka Smoke` run `31727573855`](https://github.com/TaeeunKil/kafrust/actions/runs/31727573855).
Broader transaction and combined-fault matrices remain separate gates.
