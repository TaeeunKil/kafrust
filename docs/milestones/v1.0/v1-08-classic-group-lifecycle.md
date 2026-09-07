# V1-08 Classic Consumer Group Lifecycle

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-07

## User-Visible Objective

Stabilize classic consumer-group membership, assignment, heartbeat, commit,
rejoin, offset restoration, callback, and shutdown behavior for dynamic and
static members under documented faults.

## Non-Goals

- No KIP-848 behavior; V1-09 owns it.
- No generic shared cache for membership connections.
- No promise for assignors not explicitly supported.
- No automatic recovery of broker-lost or retention-deleted records.

## Scope

- `crates/kafrust/src/{group,consumer,client,config,error,metrics}.rs`
- classic APIs OffsetCommit 8, OffsetFetch 9, FindCoordinator 10, JoinGroup 11,
  Heartbeat 12, LeaveGroup 13, and SyncGroup 14
- `crates/kafrust-protocol/src/consumer_group.rs` subscription/assignment bytes
- range, round-robin, sticky/eager if retained, and cooperative-sticky assignors
- dynamic/static membership, foreground/background heartbeat, commit queue and
  worker, rebalance listener, explicit leave, and offset reset/restoration
- classic group examples and live/published group workflows

## Work Packages

1. Freeze the public lifecycle state machine and assignment ownership rules.
2. Verify exact assignor bytes and deterministic partition ownership for mixed
   subscriptions, expansion, and member churn.
3. Cover coordinator discovery/movement, unknown member, illegal generation,
   rebalance in progress, static-member fencing, and leader/coordinator
   colocation faults.
4. Define callback ordering, callback error/panic policy without production
   panics, cancellation, heartbeat ownership, and stale-task prevention.
5. Decide and document `max.poll.interval`/long-processing behavior: implement
   an enforceable contract or mark it unsupported with operational guidance.
6. Verify commit queue/worker synchronization and committed-offset restoration
   across rejoin, retention, normal leave, and abrupt member loss.
7. Make commit ambiguity executable at the public boundary. Use a dedicated
   `ConsumerGroupCommitOutcomeUnknown` carrying group/member/generation and
   exact offsets after possible transmission. An exact-offset retry is allowed
   only when serialized under the same identity before any newer commit; direct
   and background paths use the same rule.

## Current Execution Record (2026-08-22)

V1-08 is now `In progress`. The public error surface includes the typed
`ConsumerGroupCommitOutcomeUnknown` variant and the
`ConsumerGroupCommitOffset` identity type (topic, partition, and next offset).
Direct classic and KIP-848 OffsetCommit paths, plus the bounded background
commit worker, classify an I/O failure, request timeout, oversized response, or
protocol decode failure as ambiguous only after the client marks the request as
possibly transmitted. They preserve group ID, member ID, generation/member
epoch, and the exact requested offsets. Pre-transmission failures remain their
original error, and the typed outcome is never retried or converted into a
successful commit. A foreground worker flush preserves the same typed error
through its acknowledgement channel.

The scripted-broker regression
`consumer_group_offset_commit_response_loss_returns_exact_unknown` proves the
classic direct path with a dropped OffsetCommit v2 response: it reports
`orders-group`/`member-1`/generation `1`, the exact `orders-0@0` next offset,
and zero retries. Unit coverage also asserts the typed outcome is not in the
background retry class, and the API snapshot records the new public identity
type. Source commit `8a29d1e` passed the required local validation: 466
`kafrust` unit tests, 284 protocol tests, 19 fault-injection tests, 5 public
surface tests, 10 doctests, Clippy with `-D warnings`, documentation, the
data-plane manifest checker, and the public API snapshot checker.

Published floor/current classic lifecycle profiles, dynamic/static churn,
callback ordering, heartbeat ownership, coordinator movement, and exact
offset-restoration gates remain open. This record makes no published-artifact,
40-cycle churn, or data-loss claim.

### Long-processing and `max.poll.interval` policy (2026-09-03)

The classic group API does not expose or enforce a client-side
`max.poll.interval.ms` timer. The broker remains authoritative: callers must
return to `poll_with_heartbeat` within the broker's configured interval, even
when a background heartbeat task is running. Applications that need longer
work must move processing outside the group-poll call, pause or split bounded
partition queues, or choose a broker interval that covers the workload. A
missed interval is surfaced through the normal rebalance/rejoin path; kafrust
does not promise to prevent broker-initiated removal or to hide duplicate work
after an application pause.

This is an explicit unsupported-client-enforcement decision for work package 5,
not a live `max.poll.interval` qualification. The published churn and offset
restoration gates remain open.

### Pushed-source company group smoke (2026-09-04)

At source `108b4329fd022890ecfa0155c62bfcc28a1f1f2f`, the company Windows x64
workstation's Ubuntu-T9 WSL2 x86_64 environment ran the classic
`consumer_group_poll` example against an isolated Kafka 4.3.1 KRaft broker.
With a uniquely populated topic, the example joined one assignment, fetched
record offset 0, committed the polled record, and left cleanly. The run is
bounded single-node diagnostic evidence; published security/churn,
coordinator-failover, and exact offset-restoration gates remain open. See
[`v1-company-consumer-group-short-smoke-2026-09-04.md`](../../evidence/v1-company-consumer-group-short-smoke-2026-09-04.md).

The same controls smoke exercised the classic group's earliest/latest reset
and committed out-of-range recovery path after a bounded DeleteRecords
operation. It is one current-source single-node diagnostic only; published
churn and exact restoration remain open. See
[`v1-company-consumer-controls-short-smoke-2026-09-04.md`](../../evidence/v1-company-consumer-controls-short-smoke-2026-09-04.md).

### Published 40-cycle churn replay (2026-09-04)

The published `0.3.6` group workflow completed 40 abrupt second-member
drop/rejoin cycles on both Kafka 3.7.2 classic and Kafka 4.3.1 KIP-848. Every
cycle reacquired all six partitions and restored committed offsets; each run
recorded 240 ownership observations, zero loss, zero duplicates, and zero
final in-flight/buffered gauges. The exact broker image identities, lockfile
digest, and workflow results are in
[`v1-published-group-churn-40-cycle-2026-09-04.md`](../../evidence/v1-published-group-churn-40-cycle-2026-09-04.md).

These are bounded 40-cycle diagnostics. The workflow's separate 100-cycle
qualification flag remains false, and the callback / heartbeat matrix,
ambiguity families, and remaining V1-08 exit criteria stay open.

### Published secure 40-cycle churn diagnostic (2026-09-04)

The published `0.3.6` SASL_SSL/SCRAM-SHA-256 workflow then completed the same
40 abrupt-member-drop cycles on Kafka 3.7.2 classic and Kafka 4.3.1 KIP-848
consumer. Each profile restored all six committed offsets without loss or
duplicates and ended with zero in-flight/buffered gauges. The immutable run
metadata, broker image identities, and the initial timeout calibration are in
[`v1-published-secure-group-churn-40-cycle-2026-09-04.md`](../../evidence/v1-published-secure-group-churn-40-cycle-2026-09-04.md).

The abrupt-drop 3.7.2 profile completed in 9m 1s (run 33830994497); the
KIP-848 profile completed in 34m 46s (run 33832439518) because each secure
rejoin took about 51 seconds. The corresponding normal-leave profiles also
passed: 3.7.2 in 9m 14s (run 33834878818) and KIP-848 in 5m 29s (run
33834880832). These remain bounded diagnostics; the 100-cycle flag, long
campaign, and release evidence are separate.

### Corrected distinct-offset detector rerun (2026-09-04)

After review found that the original helper could hide a distinct second
record in one partition, the corrected helper was rerun from source
`ebe694e54c54a373ca63b9f19029247e8dfe93b1`. The Kafka 3.7.2 classic abrupt
drop profile passed in [33837897323](https://github.com/TaeeunKil/kafrust/actions/runs/33837897323)
(9m 4s), and the normal `LeaveGroup` profile passed in
[33837897633](https://github.com/TaeeunKil/kafrust/actions/runs/33837897633)
(9m 4s). Both retained first offsets per partition, allowed same-offset
redelivery during expected pre-commit rebalances, rejected distinct offsets or
unexpected payloads, completed 40 cycles over six partitions, and drained all
final gauges. This replaces the historical zero-duplicate fields as the direct
bounded secure classic diagnostic evidence; the 100-cycle and remaining
callback/heartbeat/long-campaign gates stay open.

### Deterministic callback ordering correction (2026-09-07)

The coordinator-loss assignment-restoration regression exposed a duplicate
`After` callback during rejoin. The internal classic and KIP-848 join helpers
already notified `After`, and `ConsumerGroup::rejoin` notified it again after
restoring assignment state. Source commit `42169a3` added an explicit helper
notification flag: initial joins still emit one `After`, while rejoin helpers
remain silent and `rejoin` emits the single final callback after the
assignment, paused state, and commit-worker membership are synchronized.

The real listener sequence is now asserted as `After(generation 1)`,
`Before(generation 1)`, `After(generation 2)` with one assignment in each
snapshot. The deterministic evidence is
[`v1-classic-rebalance-callback-order-2026-09-07.md`](../../evidence/v1-classic-rebalance-callback-order-2026-09-07.md).
Published callback/heartbeat matrices, callback panic policy, and long-running
qualification remain open.

## Failure And Lifecycle Contract

- The group session exclusively owns member ID, generation, assignment, and
  coordinator heartbeat connection.
- Rejoinable broker errors trigger bounded rediscovery/rejoin; fencing and
  authorization remain terminal.
- A transmitted OffsetCommit with a lost response returns
  `ConsumerGroupCommitOutcomeUnknown` with group/member/generation and offsets,
  unless the same-identity/no-newer-commit serialized retry rule is proved. It
  is never converted to a generic transport error or allowed to overwrite a
  later commit.
- Before/after rebalance callbacks observe deterministic snapshots and no stale
  background task applies an old generation.
- Graceful shutdown stops workers, commits only when explicitly configured,
  leaves, joins tasks, and reports cleanup failures.

## Verification

- Deterministic tests cover all retained assignors, 0/1/many partitions,
  dynamic/static IDs, callback ordering, coordinator response loss, heartbeat
  loss, stale generations, commit ambiguity, retention reset, saturation,
  cancellation, and close.
- Direct and worker commit tests drop the response before and after a newer
  queued offset, proving exact safe-retry ordering or the typed unknown result.
- Published accepted-floor three-broker profiles run plaintext and
  SASL_SSL/SCRAM-SHA-256 with two members, six partitions, at least 20 normal
  and 20 abrupt churn cycles, partition expansion, and coordinator-plus-leader
  failure.
- Ownership is complete and disjoint each cycle; restored offsets match the
  last known committed positions; final workers/connections/queues are zero.

## Exit Criteria

1. Every classic lifecycle transition and assignor has deterministic coverage.
2. The long-processing/max-poll contract is implemented or explicitly excluded.
3. Both published security profiles pass 40 churn cycles with no simultaneous
   duplicate ownership and exact offset restoration.
4. Commit ambiguity carries executable group/member/generation/offset identity,
   and direct/background paths never report it as confirmed success or reorder
   it over a newer commit.
5. API, callback, shutdown, migration, and evidence records are consistent.

## Migration And Rollback

Map group ID, instance ID, assignor, session/heartbeat/max-poll settings,
rebalance callbacks, manual/queued commits, and offset resets. Rollback must
preserve committed offsets and the previous assignor protocol name; staged
cooperative changes cannot be reverted by assigning a partition to two members.

## Conventional Commit Plan

1. `test(group): complete classic lifecycle fault matrix`
2. `fix(group): preserve membership and offset state on rejoin`
3. `ci(group): qualify published classic churn`
4. `docs(group): stabilize classic lifecycle contract`

## Evidence Record On Completion

Record assignor/protocol versions, member and partition counts, churn/fault
cycles, generations, callback sequence, commit/replay counts, restored offsets,
final resources, artifact/security profile, and data-loss non-claim.
