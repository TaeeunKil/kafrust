# V1-05 Idempotent Producer Fault Matrix

- Status: Planned
- Target evidence: Published artifact
- Dependencies: V1-04

## User-Visible Objective

Preserve producer ID, epoch, partition sequence, and record identity across all
supported idempotent retries so acknowledged records are neither lost nor
duplicated under the documented leader and response-loss faults.

## Non-Goals

- No transactional commit/abort guarantee; V1-06 owns transactions.
- No guarantee after the broker has lost committed data through unclean
  election.
- No transparent recovery from fencing or invalid producer identity.
- No universal throughput comparison.

## Scope

- `crates/kafrust/src/{producer,client,error,metrics}.rs`
- `crates/kafrust-protocol/src/api/{produce,init_producer_id}.rs`
- Produce API 0 selected versions and InitProducerId API 22 v0/v2
- `crates/kafrust/tests/fault_injection.rs`, scripted broker, producer failover
  examples, published multi-broker workflows
- immediate, batch, and buffered idempotent modes

## Work Packages

1. Create a fault table for connection loss before write, partial/complete
   write, lost/delayed/duplicate response, leader movement, and metadata hint.
2. Prove retry reuses the exact encoded batch, producer ID/epoch, base sequence,
   and record ordering.
3. Cover duplicate-sequence success and terminal
   `OUT_OF_ORDER_SEQUENCE_NUMBER`, `INVALID_PRODUCER_EPOCH`, and
   `PRODUCER_FENCED` responses with no retry.
4. Define sequence exhaustion/rollover behavior before integer overflow.
5. Verify per-partition concurrency and batch splitting cannot reorder
   sequences.
6. Run bounded repeated live faults from the exact published artifact.

## Failure And Lifecycle Contract

- A safe retry retains the same producer identity and batch sequence.
- Duplicate-sequence acknowledgement resolves the original delivery once.
- Out-of-order, invalid epoch, or fencing makes the producer terminal; later
  sends fail before transmission.
- Cancellation does not reuse an uncertain sequence for a different batch.
- Shutdown joins workers and preserves all already-returned delivery outcomes.
- Unclean-election data loss is terminal/reconciliation-required, not retryable
  client recovery.

## Verification

Deterministic matrix:

- every fault phase for immediate, batch, and buffered modes;
- exact request-frame equality across retry;
- duplicate response, delayed old response, out-of-order response, fencing,
  epoch change, leader hint, sequence maximum, cancellation, and shutdown;
- no later frame after terminal identity errors.

Published live matrix:

- three-broker accepted-floor PLAINTEXT and pinned-current
  SASL_SSL/SCRAM-SHA-256;
- ten leader-loss/response-loss cycles per profile and at least 100,000 uniquely
  identified records;
- reconcile by topic/partition/offset and payload ID;
- zero missing acknowledged IDs, zero duplicate IDs, and zero final in-flight
  or buffered gauges.

## Exit Criteria

1. The deterministic table covers every fault phase and terminal broker error.
2. Sequence exhaustion is tested without overflow or identity reuse.
3. Both external artifact profiles complete ten cycles and reconcile at least
   100,000 IDs with zero acknowledged loss/duplicates.
4. Terminal producers reject all subsequent sends before transmission.
5. Metrics, errors, compatibility limits, and evidence rows match observed
   semantics.

## Migration And Rollback

Document the exact configuration needed for idempotence and any constraints on
acks/retries/in-flight ordering. On a terminal identity error, replace the
producer only after callers decide how to reconcile outstanding records. A
rollback must not weaken the no-sequence-reuse regression.

## Conventional Commit Plan

1. `test(producer): expand idempotent fault matrix`
2. `fix(producer): preserve identity and sequence on retry`
3. `ci(producer): qualify published idempotent failover`
4. `docs(producer): define terminal and reconciliation behavior`

## Evidence Record On Completion

Record artifact, Produce/InitProducerId versions, fault phase/cycle count,
record totals, producer ID/epoch transitions, terminal errors, retry/duplicate
metrics, reconciliation result, and unclean-election non-claim.
