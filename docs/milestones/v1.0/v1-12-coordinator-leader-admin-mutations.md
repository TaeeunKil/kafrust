# V1-12 Coordinator And Leader Admin Mutations

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-02

## User-Visible Objective

Make coordinator-, leader-, broker-, and all-broker Admin operations preserve
resource ownership, active-member semantics, partial results, and ambiguous
write outcomes through movement and response loss.

## Non-Goals

- No controller-routed writes from V1-11.
- No blanket replay rule after transmission; each operation is classified by
  state idempotence and identity ordering.
- No generic shared cache for membership- or session-bound connections.
- No claim that DeleteRecords can recover deleted broker data.

## Scope

- `crates/kafrust/src/{admin,client,error,broker_client_cache}.rs`
- group/coordinator APIs OffsetCommit 8, OffsetFetch 9, FindCoordinator 10,
  DeleteGroups 42, OffsetDelete 47, DescribeGroups 15,
  ConsumerGroupDescribe 69, ShareGroupDescribe 77,
  DescribeShareGroupOffsets 90 v0/v1 (including v1 lag semantics),
  AlterShareGroupOffsets 91 v0, and DeleteShareGroupOffsets 92 v0
- leader/broker APIs DeleteRecords 21, DescribeProducers 61,
  AlterReplicaLogDirs 34, DescribeLogDirs 35, and broker-local listing/reads
- transaction coordinator diagnostic APIs 65/66 where they are Admin reads
- member-aware OffsetFetch/OffsetCommit v10 with v9 fallback and Metadata v12
  topic UUID resolution
- coordinator/leader failover examples, ambiguity tests, and published workflows

## Work Packages

1. Allocate every non-controller/non-bootstrap Admin method to coordinator,
   partition leader, broker, or all-broker ownership in the operation ledger.
2. Test discovery failure, owner movement, response loss, partial resource
   results, authorization, cancellation, and shutdown per family.
3. Cover active consumer members, member ID/epoch/generation fencing, v10/v9
   fallback, topic UUID discovery, and exact-offset restoration. Reject a v9
   downgrade when UUID identity is required; any retained name fallback carries
   a stable-name precondition and topic delete/recreate race non-claim.
4. Verify leader-routed DeleteRecords and storage operations report partial
   outcomes and never imply recoverability of removed records.
5. Verify read-only operations can retry safely. Classify mutations
   individually: fixed-target DeleteRecords may retry the identical offset
   within one budget because it is state-idempotent, while identity-sensitive
   or non-idempotent writes switch to a typed unknown after possible
   transmission unless their own ledger proves a safe serialized retry.
6. Run common published floor/current failover and active-member profiles.

## Current Execution Record (2026-08-22)

V1-12 is now `In progress`. Coordinator-, leader-, and broker-owned Admin
paths preserve their route owner and partial resource results through
coordinator discovery, leader movement, and per-request connection invalidation.
Read-only owner changes may retry within the bounded budget. Offset and
Share-offset operations retain member/epoch/topic-identity semantics, while
ambiguous non-idempotent writes use the typed unknown boundary; fixed-target
DeleteRecords remains a separately classified state-idempotent operation.

Existing deterministic coverage includes coordinator OffsetFetch/OffsetCommit,
DeleteGroups and OffsetDelete, member-aware v10/v9 fallback, DeleteRecords,
DescribeProducers, log-dir routing, ShareGroupDescribe/offset mutations, and
transaction diagnostics. Partial top-level and per-partition error fixtures
remain intact. The complete owner ledger, delete/recreate UUID race, three-broker
leader failover, and published active-member profiles remain open; no data
recovery or complete Admin compatibility claim is made.

### DeleteRecords response-loss retry (2026-09-04)

Source `983aab126d5c033f5611bda33dfbd75b9ec8faec` adds a scripted regression
that reads a complete DeleteRecords v1 request, drops only its response, and
asserts the one allowed retry sends byte-identical topic/partition/offset
targets after metadata rediscovery. The partial result remains typed and the
retry metric is exactly one. This is the fixed-target idempotence boundary only;
other mutation retries and published leader-failover profiles remain open. See
[`v1-admin-delete-records-response-loss-2026-09-04.md`](../../evidence/v1-admin-delete-records-response-loss-2026-09-04.md).

## Failure And Lifecycle Contract

- Read-only operations may rediscover/retry inside their budget.
- Transmitted offset, Share-offset, and storage mutations follow their ledgered
  identity/idempotence rule; genuinely ambiguous non-idempotent writes are not
  replayed.
- Active-member fencing/authorization is a confirmed Kafka result and remains
  typed at top-level and partition/resource level.
- Routing state is invalidated after movement; a session-bound member identity
  is not placed in the Admin idle cache.
- DeleteRecords success changes the data boundary permanently; retries and
  rollback instructions cannot recreate deleted data.
- Retrying DeleteRecords uses the exact same topic/partition/target offset and
  budget. It can only converge on the same-or-later low watermark and must not
  be generalized to another Admin mutation.

## Verification

- Deterministic classification and before/after-transmission coverage for every
  mutation in this routing group; safe retry tests for every read family.
- Exact v10 selection; v9 rejection or documented stable-name precondition,
  including a topic delete/recreate race fixture.
- DeleteRecords response-loss tests prove the identical target may retry and
  converges without changing the requested offset; every other mutation asserts
  its ledgered request count and unknown/safe-retry result.
- Published accepted-floor classic and pinned-current KIP-848/Share profiles,
  each with active members plus coordinator movement; three-broker leader
  profiles for DeleteRecords/log-dir operations.
- Every partial result retains all requested topic/partition/broker entries and
  exact error codes.

## Exit Criteria

1. 100% of operations in this routing group have owner and failure classes.
2. Every response-loss mutation follows its explicit idempotence/identity rule;
   fixed-target DeleteRecords safely retries the same target, while unsafe
   writes return a typed unknown without replay.
3. Active-member and v10/v9 paths are live verified from the exact artifact.
4. Leader/coordinator movement preserves all requested partial results.
5. Data-loss warnings, reconciliation steps, migration notes, and ledger rows
   are complete.

## Migration And Rollback

Preserve group IDs, member metadata, topic IDs, offsets, leader ownership, and
per-resource errors when mapping from rust-rdkafka. Reconcile unknown offsets
with an offset read and unknown storage/deletion state with the corresponding
Admin read; never issue an inverse write before observation.

## Conventional Commit Plan

1. `test(admin): cover coordinator and leader fault routing`
2. `fix(admin): preserve member and partition outcomes`
3. `ci(admin): qualify active-member and leader failover`
4. `docs(admin): define routed mutation reconciliation`

## Evidence Record On Completion

Record operation/API/version/route, active member identity class, fallback,
fault point, request count, partial results, reconciliation, broker topology,
artifact/security, and irreversible-data non-claim.
