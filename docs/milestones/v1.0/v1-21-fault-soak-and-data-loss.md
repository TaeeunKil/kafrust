# V1-21 Fault Soak And Data-Loss Semantics

- Status: Planned
- Target evidence: Published artifact
- Dependencies: V1-15, V1-16, V1-18, V1-20

## User-Visible Objective

Demonstrate sustained recovery for documented retryable faults and explicit,
non-misleading terminal/reconciliation behavior for ambiguous responses,
retention, and unclean-election data loss.

## Non-Goals

- No claim that a client retry can restore broker-lost data.
- No universal availability or durability SLA.
- No arbitrary fault injection without expected record/offset/identity outcomes.
- No use of one successful 600-second run as a production SLO.

## Scope

- producer/idempotent/transaction/direct consumer/classic/KIP-848/Share/Admin
  and retained telemetry/Streams surfaces from the exact V1-20 artifact
- single broker/leader/coordinator/controller loss; combined and simultaneous
  broker loss; restart; delayed/dropped/duplicated/out-of-order response; auth
  re-establishment; retention/truncation; controlled unclean election
- response-drop proxies, scripted broker, soak example/workflows, unique record
  reconciliation, metrics/resource artifacts, compatibility and recovery docs
- three-broker replicated topics with explicit acks/min ISR and fault schedule

## Work Packages

1. Define expected success, retry, duplicate, unknown, fenced, terminal, and
   data-loss outcomes before each fault campaign.
2. Add deterministic controlled unclean-election/truncation cases that separate
   broker data loss from client behavior.
3. Run repeated leader, coordinator, controller, and simultaneous-loss faults
   across core client families, including response-loss ambiguity.
4. Run secure credential/session recovery during faults without reusing poisoned
   connections.
5. Reconcile every unique record/transaction/acknowledgement/offset/Admin
   operation and retain final resource metrics.
6. Schedule repeat campaigns; V1-25 reruns the final 24-hour RC gate. The
   current approximately 35-minute soak job cannot supply that evidence: add a
   versioned fault/campaign manifest, sufficient job timeout or resumable
   sharding, immutable artifact identity, and retained segment reconciliation.

## Failure And Lifecycle Contract

- Retryable pre-send/read faults stay within finite budgets and preserve owner
  identity.
- Non-idempotent post-send writes return unknown outcomes and use documented
  reconciliation, never blind replay.
- Idempotent/transactional retries preserve their identity rules; fencing is
  terminal.
- Retention and unclean-election missing records are reported as broker data
  loss/boundary movement, not hidden as successful recovery.
- Cancellation/shutdown during an outage drains or explicitly fails every
  accepted item and joins all tasks.

## Verification

Campaign gates on the exact published artifact:

- three independent six-hour pinned-current SASL_SSL/SCRAM-SHA-256 three-broker
  campaigns with scheduled leader, coordinator, combined, and simultaneous
  two-broker outages;
- one six-hour accepted-floor classic/plaintext campaign;
- at least 100 classic, KIP-848, and Share member-loss/rejoin cycles;
- at least 100 response-loss outcomes for each retained ambiguity family:
  non-idempotent Produce, transaction EndTxn/offset mutation, classic/modern
  group OffsetCommit, Share acknowledgement, and Admin mutation. Applied Share
  acknowledgements may remain typed unknown without redelivery and are counted
  as preserved unknowns, not falsely reconciled successes;
- controlled retention and unclean-election fixtures with predeclared expected
  lost/non-lost IDs;
- zero unaccounted acknowledged loss outside the predeclared retention/
  truncation and unclean-election broker-data-loss fixtures, zero duplicate IDs
  for idempotent/transactional guarantees, and zero final tasks/queues/
  connections/in-flight/buffered gauges.
- Each retained campaign record includes workflow SHA, artifact/version,
  broker image digest, fault schedule, shard/segment boundaries, and configured
  timeout; concatenated segments prove continuous identity and reconciliation.

## Exit Criteria

1. All four six-hour campaigns pass with their predeclared outcomes.
2. Each group family completes 100 churn cycles and every explicitly listed
   ambiguity family records 100 applied/unapplied/reconciled-or-preserved-
   unknown outcomes.
3. Every acknowledged ID outside the explicit retention/truncation and
   unclean-election fixtures is accounted for; both data-loss fixture results
   match the documented non-recovery semantics.
4. Resource gauges drain and no secret appears in artifacts.
5. Recovery runbooks, compatibility limits, SLO inputs, and ledger rows are
   complete.

## Migration And Rollback

Applications must retain idempotency/business IDs and a rollback client path.
Rollback during faults stops new traffic, closes the kafrust owner, preserves
transactional/group identity as documented, switches configuration/artifact,
and reconciles unknown writes before replay. Broker data restoration remains an
operator concern.

## Conventional Commit Plan

1. `test(fault): define retry and data-loss outcome fixtures`
2. `ci(soak): add repeated secured fault campaigns`
3. `docs(recovery): publish ambiguity and data-loss runbooks`
4. `docs(evidence): record long fault campaigns`

## Evidence Record On Completion

Record artifact, broker/min-ISR/acks/security topology, six-hour run and fault
schedules, records/transactions/members/ambiguities, expected/observed loss and
duplicates, retry/error totals, resource series/final gauges, and SLA non-claim.
