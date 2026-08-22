# V1-09 KIP-848 Consumer Group Lifecycle

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-07

## User-Visible Objective

Stabilize Kafka's consumer group protocol with member/assignment epochs,
broker-provided assignments, regex subscriptions, modern offset APIs, secure
coordinator recovery, and deterministic shutdown.

## Non-Goals

- No claim on brokers that do not advertise the KIP-848 APIs.
- No classic-protocol emulation inside the modern session.
- No reuse of a membership connection without its member/epoch lease.
- No ShareConsumer behavior.

## Scope

- `crates/kafrust/src/{group,consumer,client,config,error,metrics}.rs`
- ConsumerGroupHeartbeat API 68 v0/v1 and ConsumerGroupDescribe API 69 v0/v1
- Metadata v12 topic UUIDs, OffsetFetch/OffsetCommit v10 with v9 fallback, and
  Fetch versions selected by V1-03
- explicit and regex subscriptions, nullable assignments, topic discovery,
  client-generated v1 member ID, foreground/background heartbeats, rejoin
  token, offset restoration, Admin modern description, and explicit leave
- KIP-848 source and published workflows

## Work Packages

1. Freeze member epoch, assignment epoch, nullable assignment, leave epoch, and
   stale-task state transitions.
2. Verify v0 explicit-topic and v1 regex bytes, keep the client-generated v1
   member ID stable across reconnect/rejoin/leave for one consumer lifetime,
   and cover dynamic topic creation/removal and unknown UUID refresh.
3. Classify v10 UUID-to-v9 name offset fallback per operation. Reject it when
   UUID identity is required; if a name fallback is retained, document the
   stable-name precondition and test the delete/recreate race without calling
   the downgrade lossless.
4. Fault coordinator discovery/heartbeat, repeated rebalance responses,
   partition leader, combined coordinator/leader, and secured connections.
5. Verify assignment callback/commit worker synchronization and no offset write
   from a stale epoch. A possibly transmitted commit returns the same typed
   `ConsumerGroupCommitOutcomeUnknown` identity/offset contract as V1-08 unless
   an exact retry is serialized before any newer commit under the same epoch.
6. Cover empty assignment, concurrent members, member loss, replacement member,
   cancellation, and shutdown.

## Current Execution Record (2026-08-22)

V1-09 is now `In progress`. The current KIP-848 path owns a dedicated
member/epoch session, sends ConsumerGroupHeartbeat v0/v1 with nullable
assignments, resolves Metadata v12 topic UUIDs, selects OffsetFetch/OffsetCommit
v10 when advertised, and falls back to v9 only when the name-based identity is
available. Explicit and regex subscriptions are supported; regex assignments
refresh topic metadata when a new UUID is not yet known. Rejoin carries the
same member ID and owned partitions, synchronizes the commit worker, and
restores offsets before replacing the assignment.

The deterministic fault slice covers repeated rebalance responses, coordinator
reconnect, v9 offset-fetch fallback, v10 UUID offset commit, empty assignments,
and regex-created topic/UUID refresh. The regression
`consumer_protocol_rejoins_and_fetches_after_rebalance_error` now also asserts
the member ID remains stable across the rejoin while generation 2 restores the
record position; `consumer_protocol_regex_refreshes_unknown_topic_uuid_assignment`
covers dynamic regex topic discovery. V1-08's typed
`ConsumerGroupCommitOutcomeUnknown` classifier is shared by the KIP-848 v9/v10
direct and background commit paths.

Published pinned-current plaintext and SASL_SSL/SCRAM churn, full member-loss
cycles, stale-task cancellation, delete/recreate name-fallback races, and
exact offset-restoration evidence remain open. No modern-group published
artifact or 40-cycle claim is made.

## Failure And Lifecycle Contract

- The group handle and heartbeat task share one fenced member/assignment state;
  a rejoin token prevents a stale task from mutating a new session.
- Nullable assignment preserves ownership; an explicit empty assignment clears
  it according to Kafka semantics.
- Retryable coordinator/rebalance responses rediscover/rejoin inside session
  budgets; fencing/authorization are terminal.
- Transmitted offset mutation ambiguity is surfaced with group/member/epoch and
  offset identities and is not replayed blindly or allowed to overwrite a newer
  commit.
- Shutdown sends the supported leave epoch, cancels I/O, joins tasks, and
  releases assignments exactly once.

## Verification

- Deterministic v0/v1, lifetime member-ID stability, UUID refresh, v10/v9
  identity fallback/rejection, nullable/empty assignment, stale heartbeat,
  commit ambiguity, cancellation, and close cases.
- Published pinned-current three-broker profiles for PLAINTEXT and
  SASL_SSL/SCRAM-SHA-256 with two members, six partitions, regex-created topics,
  20 coordinator and 20 partition-leader churn cycles.
- Every cycle asserts disjoint/complete ownership, monotonically valid member
  epochs, exact committed-offset restoration, and zero final tasks/queues.

## Exit Criteria

1. All modern epoch and assignment transitions have deterministic tests.
2. Regex and UUID discovery handle initial and dynamic topics without manual
   rejoin when Kafka semantics allow it.
3. v10 is selected where advertised; every v9 fallback is either rejected for
   identity loss or carries the tested name-stability precondition and explicit
   topic delete/recreate non-claim.
4. One consumer's v1 member ID survives every reconnect/rejoin/leave sequence;
   a new consumer instance gets a distinct identity.
5. Both published profiles pass 40 churn cycles with exact ownership/offsets.
6. Errors, metrics, migration notes, and evidence ledger match the contract.

## Migration And Rollback

Document the switch between classic and `consumer` group protocols, supported
broker floor, regex behavior, assignor differences, callback ordering, and
offset metadata. Rollback to classic requires a clean leave/restart and must not
reuse KIP-848 member epochs as classic generations.

## Conventional Commit Plan

1. `test(group): cover KIP-848 epoch and regex faults`
2. `fix(group): fence stale modern membership tasks`
3. `ci(group): qualify published KIP-848 churn`
4. `docs(group): stabilize modern group lifecycle`

## Evidence Record On Completion

Record API versions, UUID/regex events, member/assignment epochs, churn/fault
counts, ownership/offset results, security, artifact, final resources, and
classic/non-supported-broker non-claims.
