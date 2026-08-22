# V1-07 Direct Consumer Integrity

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-03

## User-Visible Objective

Ensure a directly assigned consumer preserves record shape, position, leader
epoch, isolation, and reset behavior across reconnects, retention boundaries,
leader movement, pause/resume, and bounded delivery.

## Non-Goals

- No group membership or rebalance behavior.
- No claim that retry can recover records removed by retention or unclean
  election.
- No unbounded prefetch or hidden auto-commit behavior.
- No promotion of Fetch v14-v18 without the V1-03 decision and live evidence.

## Scope

- `crates/kafrust/src/{consumer,client,config,error,metrics}.rs`
- `crates/kafrust-protocol/src/{record_batch,api/fetch,api/list_offsets,
  api/offset_for_leader_epoch,api/metadata}.rs`
- direct consumer cache and broker-scoped Fetch session state
- `ConsumerAssignment`, `ConsumerRecord`, headers, nullable values, timestamps,
  isolation, watermarks, position, seek, pause/resume, partition queues, and
  reset policy
- consumer fault tests, examples, compatibility and migration docs

## Work Packages

1. Decide and test null versus empty key/value/header representation in the
   stable record API.
2. Complete record-batch boundary, compression, timestamp, control-record, and
   isolation fixtures.
3. Fault Fetch before/after transmission, invalidate poisoned connections and
   sessions, rediscover metadata, and resume from the last delivered position.
4. Cover preferred replicas, leader epochs, truncation, retention-driven
   `OFFSET_OUT_OF_RANGE`, earliest/latest/absolute reset, and position changes.
5. Define partition-queue saturation, fairness, cancellation, assignment
   mutation, and shutdown behavior.
6. Qualify the exact published artifact on floor/current multi-broker profiles.

## Current Execution Record (2026-08-22)

V1-07 is now `In progress`. The direct consumer already has a bounded
partition-queue contract, explicit pause/resume/seek operations, leader-epoch
tracking, preferred-replica handling, read-committed filtering, out-of-range
reset policy, and Fetch-response reconnect behavior. The deterministic slice is
covered by `direct_consumer_reconnects_after_fetch_response_loss`,
`split_partition_queue_reports_backpressure_without_skipping_records`,
`resets_out_of_range_assignment_to_earliest_offset`,
`fetches_offset_for_leader_epoch_from_partition_leader`, and the record-batch
codec/decompression limit tests in `kafrust-protocol`.

The current-source Live Kafka Smoke run is being used to refresh the accepted
broker/version evidence. Golden record-shape fixtures, preferred-replica and
retention fault matrices, 100,000-record published reconciliation, and final
queue/resource gates remain open; no broker data-loss recovery or published
artifact claim is made.

## Failure And Lifecycle Contract

- A lost Fetch response is safe to retry because delivery has not been exposed
  to the caller. That unobserved retry alone does not expose a record twice;
  explicit seek/reset/rewind, reassignment, or application replay can
  intentionally deliver the same offset again and is outside this narrow claim.
- Position advances only according to the documented poll/queue contract.
- Session state is broker- and consumer-owned and is discarded on connection,
  assignment, position, or protocol session errors.
- Retention/truncation returns a typed broker/reset outcome; it never invents a
  missing record.
- Cancellation and shutdown release bounded queues and in-flight connections
  without detaching a task.

## Verification

- Golden record fixtures cover null/empty key/value/header, multiple headers,
  timestamps, all codecs, committed/aborted/control batches, corrupt CRC,
  truncation, oversized data, and decompression limits.
- Scripted-broker cases cover response loss, stale metadata, preferred-replica
  failure, invalid Fetch session epoch, leader epoch divergence, cancellation,
  queue saturation, and close.
- Published accepted-floor and pinned-current three-broker profiles consume at
  least 100,000 uniquely identified records through two leader movements,
  retention reset, pause/resume, seek, and final watermark checks.
- Final queues and request gauges are zero; seeded, previously observed
  producer-acknowledged IDs missing because of the injected retention boundary
  are reported, not hidden.

## Exit Criteria

1. Stable record types preserve every supported Kafka null/empty/header field.
2. All allocation and queue boundaries fail with typed errors before unbounded
   growth.
3. Response loss, leader movement, session reset, and retention tests preserve
   documented position semantics.
4. Both published profiles account for all expected IDs and final resources.
5. Compatibility and migration docs distinguish client recovery from broker
   data loss.

## Migration And Rollback

Map assignment, seek, position, watermark, isolation, and offset-reset behavior
from rust-rdkafka. Record-shape changes require migration examples. Rollback
must retain corrupt-input and no-invented-data regressions.

## Conventional Commit Plan

1. `test(consumer): cover record and fetch integrity boundaries`
2. `fix(consumer): preserve position across recoverable faults`
3. `ci(consumer): qualify published direct-consumer recovery`
4. `docs(consumer): define retention and record-shape semantics`

## Evidence Record On Completion

Record Fetch versions, record shapes/codecs, fault and reset points, expected
retention loss, delivered IDs/positions, queue peaks/final gauges, artifact and
broker profiles, and explicit group/data-loss non-claims.
