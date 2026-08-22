# V1-06 Transaction Outcome Safety

- Status: Planned
- Target evidence: Published artifact
- Dependencies: V1-05

## User-Visible Objective

Make commit, abort, fencing, coordinator failure, and response-loss outcomes
explicit so a caller never mistakes an ambiguous transaction for committed or
replays it through a still-usable producer.

## Non-Goals

- No distributed application exactly-once claim beyond Kafka transactions and
  the documented consumer-offset workflow.
- No automatic resolution of a lost EndTxn response.
- No reuse of a producer after `TransactionOutcomeUnknown` or fencing.
- No recovery claim for data already lost by the broker.
- No client use of AddPartitionsToTxn v4+; Kafka reserves its batched
  verify-only shape for broker-side use.

## Scope

Source:

- `crates/kafrust/src/{producer,client,consumer,admin,error,metrics}.rs`
- protocol APIs FindCoordinator 10, legacy InitProducerId 22 v0/v2,
  AddPartitionsToTxn 24 v0/v3, AddOffsetsToTxn 25 v0/v3, legacy EndTxn 26
  v0/v3, legacy TxnOffsetCommit 28 at v4 or lower, Produce 0, Fetch 1,
  DescribeTransactions 65, and ListTransactions 66; full TV2 additionally owns
  InitProducerId v5, TxnOffsetCommit v5, and EndTxn v5
- Kafka finalized `transaction.version` and the complete KIP-890 protocol
  boundary. The conservative v1 path is legacy TV0/TV1: transactional Produce
  is capped at v11, partitions/offsets are added explicitly, and the current
  EndTxn v3 flow remains coherent. TV2 may replace it only as a complete unit:
  finalized `transaction.version >= 2`, transactional Produce v12/v13,
  InitProducerId v5 abortable errors, implicit partition/offset addition,
  TxnOffsetCommit v5, EndTxn v5 response producer-ID/epoch handling,
  `TRANSACTION_ABORTABLE`, and epoch transitions.
- transaction fault tests, response-drop proxies, transaction examples, and
  current/published transaction workflows

Version decisions are grounded in the Apache Kafka 4.3.1
[Produce](https://raw.githubusercontent.com/apache/kafka/4.3.1/clients/src/main/resources/common/message/ProduceRequest.json),
[InitProducerId](https://raw.githubusercontent.com/apache/kafka/4.3.1/clients/src/main/resources/common/message/InitProducerIdRequest.json),
[TxnOffsetCommit](https://raw.githubusercontent.com/apache/kafka/4.3.1/clients/src/main/resources/common/message/TxnOffsetCommitRequest.json),
[EndTxn](https://raw.githubusercontent.com/apache/kafka/4.3.1/clients/src/main/resources/common/message/EndTxnRequest.json),
and [AddPartitionsToTxn](https://raw.githubusercontent.com/apache/kafka/4.3.1/clients/src/main/resources/common/message/AddPartitionsToTxnRequest.json)
schemas.

## Work Packages

1. Freeze the v1 transaction protocol decision. Never mix legacy explicit-add
   calls or EndTxn v3 with TV2 Produce/TxnOffsetCommit semantics. If TV2 is not
   fully implemented, mechanically cap transactional Produce at v11 even when
   Kafka 4.3.1 advertises v12/v13.
2. Freeze transaction states from uninitialized through active, committing,
   aborting, unknown, fenced, and defunct.
3. Add deterministic failures before/after every transaction protocol write,
   including delayed stale responses and cancellation.
4. Verify coordinator rediscovery is allowed before a new mutation is sent but
   no post-transmission mutation is replayed unless Kafka makes it idempotent
   for the retained identity.
5. Cover transaction timeout, producer epoch fencing, concurrent same-ID
   producers, leader loss, and sequence errors.
6. Reconcile unknown commit/abort through `DescribeTransactions`, a replacement
   producer with the same transactional ID, and `read_committed` observation;
   document where Kafka cannot prove the result immediately.
7. If TV2 is retained, test finalized-feature discovery and epoch changes only
   at transaction boundaries, including TV1-to-TV2 upgrade, broker feature
   epoch changes, downgrade/restart behavior, and every abortable error.
8. Qualify published floor/current broker profiles.

## Failure And Lifecycle Contract

- A lost transmitted EndTxn response returns
  `Error::TransactionOutcomeUnknown { operation }` and makes the producer
  defunct.
- Fencing and invalid producer epoch are terminal and prevent later writes.
- The transaction protocol version is fixed for one transaction. A finalized
  feature change cannot switch Produce/add-offset/EndTxn semantics mid-flight;
  a TV2 transition requires the documented epoch boundary.
- Pre-transmission discovery/connect failures may retry inside the total
  operation/transaction budget.
- Cancellation after possible transmission follows the same unknown-outcome
  boundary as transport loss.
- Shutdown never silently commits an active transaction; its abort/close result
  remains observable.
- `read_committed` exposes committed data only and excludes aborted/in-flight
  data within the tested broker contract.

## Verification

Deterministic:

- one before/after-transmission case for every mutation API;
- commit and abort response loss, coordinator/leader movement, timeout,
  fencing, duplicate/delayed responses, cancellation, and shutdown;
- no extra mutation frame after unknown outcome;
- committed/aborted/in-flight fetch isolation and offset-commit atomicity.
- a Kafka 4.3.1 `transaction.version=2` fixture proves either the coherent TV2
  flow and returned-epoch transition or the mechanically enforced legacy
  Produce-v11 cap; no mixed request sequence is accepted.

Published live:

- accepted-floor classic and pinned-current KIP-848, both three-broker; at
  least one SASL_SSL/SCRAM-SHA-256 profile;
- ten coordinator/leader fault cycles and 100 commit plus 100 abort
  transactions per profile;
- unique record IDs and group offsets reconcile exactly; zero visible aborted
  IDs under `read_committed` and zero final resource gauges.

## Exit Criteria

1. The legacy-or-TV2 decision is explicit and a frame-sequence test proves no
   transaction mixes the two protocols.
2. Every transaction mutation has a tested pre/post-transmission classification.
3. Unknown, fencing, abortable, and timeout errors produce documented terminal
   states.
4. Read-committed and transactional-offset assertions pass in both published
   profiles for the exact artifact.
5. No ambiguous mutation is replayed and all 200 transaction outcomes per
   profile (400 total across the two required profiles) are accounted for as
   committed, aborted, or explicitly unknown.
6. Migration, recovery, metrics, compatibility limits, and ledger rows agree.

## Migration And Rollback

Map `transactional.id`, transaction timeout, `send_offsets_to_transaction`,
commit, abort, and read-committed behavior from rust-rdkafka. On unknown
outcome, stop the producer, reconcile, then create a replacement; never replay
business operations merely because the client disconnected. Preserve this
safety boundary through rollback.

## Conventional Commit Plan

1. `test(transaction): cover mutation response-loss matrix`
2. `fix(transaction): preserve unknown and defunct states`
3. `ci(transaction): qualify published fault recovery`
4. `docs(transaction): define reconciliation and read-committed contract`

## Evidence Record On Completion

Record every negotiated API version, transactional IDs/epochs, fault point,
commit/abort/unknown totals, visibility/offset results, security/topology,
resource gauges, and data-loss non-claim.
