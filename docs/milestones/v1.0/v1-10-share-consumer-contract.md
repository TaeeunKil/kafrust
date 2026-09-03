# V1-10 ShareConsumer Contract

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-07, V1-09

## User-Visible Objective

Stabilize the high-level ShareConsumer contract for acquisition, Renew,
Accept/Release/Reject, acknowledgement ambiguity, redelivery, multi-member
ownership, backpressure, and clean session shutdown on stable Kafka Share APIs.

## Non-Goals

- No support for Kafka 4.0 early-access Share v0 schemas removed from stable
  Kafka releases.
- No stabilization of broker-internal Share Group State APIs; V1-14 classifies
  those separately.
- No exactly-once claim for Share processing.
- No automatic replay of an acknowledgement with an unknown broker outcome.

## Scope

- `crates/kafrust/src/{share_consumer,client,config,error,metrics}.rs`
- ShareGroupHeartbeat API 76 v1, ShareFetch API 78 v1/v2, and
  ShareAcknowledge API 79 v1/v2
- `ShareConsumerConfig`, consumer/heartbeat handles, `ShareRecord`, acquire
  modes, acknowledgement modes/types, renewal and reconciliation APIs
- metadata/leader routing, per-broker Share sessions/epochs, coordinator
  membership with a client-generated lifetime-stable v1 member ID, lock
  timeout, redelivery, queue bounds, and close
- Share fault, live, secure, multi-member, repeated-loss, and published workflows

The applied/unapplied ambiguity model follows Kafka's
[KIP-932 record states](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/255070434/KIP-932%2BQueues%2Bfor%2BKafka):
Accept moves a record to `Acknowledged`, Reject can archive it, and Release can
make it eligible for another attempt.

## Work Packages

1. Freeze the public record and acknowledgement state machine.
2. Verify v2 `RecordLimit` fails explicitly when unavailable and v1
   `BatchOptimized` fallback does not silently weaken requested limits.
3. Cover lock renewal, expiry, same-offset replacement, and all acknowledgement
   types with exact broker-session ownership; prove the ShareGroupHeartbeat v1
   member ID survives reconnect/rejoin/leave for one consumer lifetime.
4. Drop/delay acknowledgements at both sides of the broker-application boundary.
   Return `ShareAcknowledgementOutcomeUnknown`, block replay, and reset the
   affected session. Model the two outcomes separately: an unapplied or
   Release acknowledgement can produce broker redelivery, while an applied
   Accept (or Reject/archive) can legitimately produce no redelivery and remain
   unresolvable at the stable client boundary.
5. Exercise multi-member assignment, member/coordinator/leader loss,
   backpressure, cancellation, heartbeat shutdown, leave, and final cleanup.
6. Run bounded secure published profiles; V1-21 extends duration.

## Current Execution Record (2026-08-22)

V1-10 is now `In progress`. The ShareConsumer implementation has a typed
record/acknowledgement state machine, v1/v2 acquisition negotiation,
per-broker ShareFetch session ownership, lock renewal, bounded acquisition,
coordinator heartbeat, explicit close, and reconciliation for an ambiguous
ShareAcknowledge. A lost response marks the exact pending record unknown,
discards the affected broker session, blocks acknowledgement replay, and
allows the next poll to observe either eligible redelivery or no redelivery;
the client does not infer which broker-side state was applied.

The deterministic slice covers v2 record-limit negotiation, lost Accept and
Release responses, session reset, Release redelivery, unknown-outcome close,
record acquisition filtering, and the `ShareAcknowledgementRequired` boundary.
The scripted regression `share_consumer_reconciles_lost_acknowledgement_with_redelivery`
now also asserts the broker member ID remains stable across reconciliation and
redelivery. Published pinned-current two-member secure profiles, all
acknowledgement types under delayed/lost responses, multi-member churn, and the
10,000-record gate remain open; no exactly-once or published-artifact claim is
made.

### Share acknowledgement cancellation after transmission (2026-09-04)

The direct `ShareConsumer::commit` path now guards ShareAcknowledge v1/v2 and
ShareFetch-v2-with-renew requests against caller cancellation after the request
frame is observed. Cancellation marks each affected pending acknowledgement as
`acknowledgement_outcome_unknown`, discards the broker Share session, and keeps
the broker connection out of the cache. A subsequent commit returns the typed
unknown-outcome error instead of replaying the acknowledgement. The focused
regression passed on Windows and company WSL2 (Rust 1.81.0); the WSL2
`fault_injection` target also passed all 29 tests. The immutable record is
[`v1-share-ack-cancellation-2026-09-04.md`](../../evidence/v1-share-ack-cancellation-2026-09-04.md).
This closes only the direct cancellation boundary; published secure
multi-member coverage, long campaigns, and the 10,000-record exit gate remain
open.

## Failure And Lifecycle Contract

- ShareFetch sessions and epochs are broker/session-owned and never enter a
  generic cache without the matching lease.
- The client-generated Share v1 member ID is stable for one consumer lifetime;
  reconnect/rejoin/leave cannot silently mint a replacement identity.
- A lost acknowledgement is never replayed; reconciliation discards the
  affected session. Redelivery proves a record became eligible again, but lack
  of redelivery does not prove whether Accept/Reject was applied. The error or
  reconciliation handle retains topic ID, partition, offset/range, and
  acknowledgement type through `close()` and remains unknown unless a
  separately qualified broker-state or application-business observation
  resolves it.
- `close()` releases known records, skips unknown acknowledgements, closes
  sessions, leaves the group, joins heartbeat work, then returns the most
  relevant error.
- Queue saturation is bounded and observable; cancellation does not detach
  ownership.
- Lock expiry replaces retained state only for a broker-redelivered record; an
  applied Accept is already `Acknowledged` and is not expected to redeliver.

## Verification

- Deterministic cases for v1/v2 negotiation, record limit, Renew/expiry,
  Accept/Release/Reject, split leaders, response loss, delayed response,
  coordinator churn, queue saturation, cancellation, and close.
- Separate injected branches for Accept/Release/Reject/Renew prove (a) request
  not applied and eligible redelivery where the acknowledgement type permits,
  and (b) acknowledgement applied then response lost, including Accept with no
  redelivery. Absence of redelivery is never converted into confirmed success.
- Published pinned-current three-broker PLAINTEXT and
  SASL_SSL/SCRAM-SHA-256 profiles with two members, six partitions, at least
  10,000 unique records, 20 alternating member-loss cycles, and 20
  acknowledgement-response-loss reconciliations.
- Zero unaccounted record IDs outside declared unknown/applied states, no
  duplicate accepted partition/offset pair, and zero final gauges.

## Exit Criteria

1. The stable public acknowledgement state machine has complete deterministic
   transition coverage.
2. v1/v2 negotiation never weakens configured record-limit semantics.
3. Both published profiles meet the exact 10,000-record/20-cycle/20-ambiguity
   gate with applied/unapplied response-loss branches, correct conditional
   redelivery, preserved unresolved outcomes, and zero final resources.
4. No unknown acknowledgement is automatically replayed.
5. Share docs, API audit, migration limits, metrics, and ledger rows agree.

## Migration And Rollback

Share has no direct classic-consumer semantic equivalent. Migration must map
processing outcomes explicitly and retain topic/partition/offset identity for
reconciliation. Rollback closes Share sessions before switching consumers and
must tolerate either Kafka redelivery or no redelivery for unknown
acknowledgements; business IDs are required when the application needs stronger
reconciliation.

## Conventional Commit Plan

1. `test(share): complete acknowledgement and ownership matrix`
2. `fix(share): preserve session and redelivery safety`
3. `ci(share): qualify secure published multi-member recovery`
4. `docs(share): stabilize the ShareConsumer contract`

## Evidence Record On Completion

Record Share API versions, acquire/ack modes, member/partition/record counts,
loss and ambiguity cycles, redelivery and duplicate results, queue peaks/final
gauges, artifact/security profile, and exactly-once/state-API non-claims.

### Bounded published follow-up (2026-09-03)

The `0.3.6` published artifact passed the short ShareGroupDescribe,
multi-broker, multi-member, state-failover, acknowledgement (64 cycles), and
supported 180-second member-loss workflows from one workflow head. The
shortened 30-second member-loss diagnostic is retained as a failed input
boundary; the supported-window rerun passed. These rows are recorded in
[`v1-published-short-surface-smoke-2026-09-03.md`](../../evidence/v1-published-short-surface-smoke-2026-09-03.md).
The exact 10,000-record/20-cycle/ambiguity exit gate remains open.

The company workstation also passed a short Kafka 4.3.1 Share diagnostic at
pushed head `74ee4dc`: ShareConsumer roundtrip, ShareGroup offset mutations,
and ShareGroup state lifecycle. The exact record is
[`v1-company-share-short-smoke-2026-09-03.md`](../../evidence/v1-company-share-short-smoke-2026-09-03.md).
It is single-node local evidence and does not count toward the secure
multi-member, three-broker, 10,000-record, or 20-cycle exit gate.
