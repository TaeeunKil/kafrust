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

### Record shape and Fetch cancellation boundaries (2026-09-03)

At source commit `df43749cfd277509c8173ac5f68beb8bced866bd`, the stable record
mapping is covered by an explicit null-versus-empty regression for keys, values,
and header values. A second scripted-broker regression observes a Fetch v12
frame, cancels the caller while its response is withheld, and verifies that no
Fetch session is cached or reused; the next Fetch uses a fresh connection and
returns the record. The detailed record is
[`v1-direct-consumer-integrity-2026-09-03.md`](../../evidence/v1-direct-consumer-integrity-2026-09-03.md).

Both focused tests and the 29-test fault-injection target passed on company
Ubuntu-T9 WSL2. This closes the direct deterministic slice only; retention,
leader movement, published reconciliation, and final queue/resource gates
remain open.

### Pushed-source partition-queue smoke (2026-09-04)

The pushed source `c68088526cae753edde00310df1697ef0f40eedf` was run on the
company Windows x64 workstation's Ubuntu-T9 WSL2 environment against an
isolated Kafka 4.3.1 broker. `producer_send` created one record at partition 0,
offset 0; `consumer_partition_queue` assigned and split that partition,
polled once, and drained exactly one queued record with the expected key and
value. This is short single-node diagnostic evidence only; saturation,
backpressure, leader movement, retention, and published reconciliation remain
open. See
[`v1-company-partition-queue-short-smoke-2026-09-04.md`](../../evidence/v1-company-partition-queue-short-smoke-2026-09-04.md).

### Pushed-source consumer controls smoke (2026-09-04)

At source `a981a35a32db0ca61b5aa1a391a58a0ccf9f184c`, the company WSL2
workstation ran `consumer_position_control` against an isolated Kafka 4.3.1
broker. Direct and group watermarks were valid; pause suppressed delivery,
resume and seek restored delivery, and positions advanced after the expected
record was fetched. The paired `consumer_group_offset_reset` run also covered
earliest/latest reset and committed out-of-range recovery. This remains short
single-node diagnostic evidence; published retention, leader movement, and
reconciliation gates remain open. See
[`v1-company-consumer-controls-short-smoke-2026-09-04.md`](../../evidence/v1-company-consumer-controls-short-smoke-2026-09-04.md).

### Fetch session epoch reset (2026-09-07)

At source commit `7f94dfdd5ed42576493ed43cd2f6c4dd1d5e9f7c`, a scripted broker
first establishes Fetch v12 session `17`, then returns
`INVALID_FETCH_SESSION_EPOCH` after observing the stale session/epoch pair.
The consumer discards the session and metadata route, reconnects, retries with
session `0`, and delivers the expected record at offset `42` with an empty
final session cache. The detailed record is
[`v1-direct-consumer-fetch-session-reset-2026-09-07.md`](../../evidence/v1-direct-consumer-fetch-session-reset-2026-09-07.md).

This closes the deterministic invalid-session recovery boundary only; live
leader movement, retention, published reconciliation, and final queue/resource
gates remain open.

### Latest offset reset boundary (2026-09-07)

At source commit `758b8f9`, a scripted broker first returns
`OFFSET_OUT_OF_RANGE` for an assignment at offset `100`, then reports
watermarks `(low=4, high=9)`. The direct consumer configured with
`OffsetResetPolicy::Latest` sends the bounded low/high ListOffsets lookup,
selects the high watermark, retries Fetch at offset `9`, returns no record, and
advances its assignment position to `9`. The detailed record is
 [`v1-direct-consumer-offset-reset-high-watermark-2026-09-07.md`](../../evidence/v1-direct-consumer-offset-reset-high-watermark-2026-09-07.md).

This closes the deterministic latest-reset boundary only; retention behavior on
a live broker, leader movement, published reconciliation, and final
queue/resource gates remain open.

### Partition-queue cancellation boundary (2026-09-07)

At source commit `90e1ad3`, a scripted broker supplies one Fetch record after
the split queue receiver is dropped. The consumer detects the closed queue,
removes the route, returns the record through the ordinary `poll()` result, and
advances the assignment position from `42` to `43`. The detailed record is
[`v1-direct-consumer-partition-queue-cancellation-2026-09-07.md`](../../evidence/v1-direct-consumer-partition-queue-cancellation-2026-09-07.md).

This closes the deterministic dropped-receiver boundary only; queue fairness
under sustained multi-partition load, live retention and leader movement,
published reconciliation, and final resource gates remain open.

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
