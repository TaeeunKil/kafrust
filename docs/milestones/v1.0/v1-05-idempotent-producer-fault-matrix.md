# V1-05 Idempotent Producer Fault Matrix

- Status: In progress
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

## Current Execution Record (2026-08-22)

V1-05 is now `In progress`. The existing producer state machine and scripted
broker tests provide the deterministic first slice of the fault matrix:

| Fault or invariant | Deterministic evidence | Result |
| --- | --- | --- |
| Lost Produce response and reconnect | `idempotent_producer_retries_dropped_response_with_same_batch_sequence`; `retries_ambiguous_idempotent_batch_with_the_same_sequence` | exact Produce frame is replayed with the same producer identity and base sequence |
| Duplicate sequence acknowledgement | `batch_duplicate_outcomes`; the dropped-response integration test | resolves once with an unknown offset and does not allocate a new sequence |
| Out-of-order, invalid epoch, and fencing | `idempotent_producer_fatal_sequence_errors_are_terminal`; `classifies_idempotent_produce_error_dispositions` | producer becomes terminal and the next send emits no frame |
| Batch splitting and partition ordering | `preserves_reserved_batch_sequences_across_retries_and_chunks`; `reserves_batch_sequences_independently_per_partition` | reserved sequences remain partition-scoped and ordered across retries |
| Sequence boundary | `wraps_idempotent_sequence_after_i32_max` | wraps at the Kafka sequence modulus without integer overflow |

The deterministic suite and the full workspace validation pass on the pushed
candidate commit `5571ca3`; the stable and Rust 1.81.0 matrix is green in
[CI run 32548809314](https://github.com/TaeeunKil/kafrust/actions/runs/32548809314).
This does not close the milestone: buffered-mode
fault coverage, the complete before/partial/after-write table, ten-cycle
published profiles, and the 100,000-record reconciliation gate remain open.

The current-source company workstation smoke additionally exercised immediate
and buffered idempotent sends against isolated Kafka 4.3.1 and reconciled the
buffered records by fetch. The bounded diagnostic is recorded in
[`v1-company-workstation-current-short-smoke-2026-09-03.md`](../../evidence/v1-company-workstation-current-short-smoke-2026-09-03.md);
it contains no injected fault and does not count toward the published fault
matrix or ten-cycle gate.

At pushed head `e51384d`, the Windows workstation reran all 19 deterministic
`fault_injection` tests. Producer response-loss replay and terminal sequence
errors, transaction ambiguity, consumer/group recovery, Share ambiguity, and
Admin routing all passed. The record is
[`v1-company-short-fault-protocol-smoke-2026-09-03.md`](../../evidence/v1-company-short-fault-protocol-smoke-2026-09-03.md).
The suite still does not provide buffered fault injection or the published
ten-cycle/100,000-record reconciliation gate.

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
