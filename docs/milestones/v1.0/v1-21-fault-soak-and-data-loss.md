# V1-21 Fault Soak And Data-Loss Semantics

- Status: In progress
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

## Preparation Record (2026-08-22)

V1-21 remains `In progress` because the exact V1-20 published pair and the V1-15,
V1-16, and V1-18 artifact gates are not complete. Historical published soak
rows remain immutable evidence for their named profiles only. The checked-in
soak workflow currently proves a bounded broker-restart smoke with final queue
gauges, but its five-minute/default single-node capacity cannot satisfy the
required six-hour campaigns, 100-cycle family gates, or controlled
unclean-election fixtures. The preparation manifest at
[`v1-21-fault-campaign-manifest.json`](../../evidence/v1-21-fault-campaign-manifest.json)
and `scripts/check_v1_fault_campaign_manifest.py` now make those thresholds
machine-checkable. The published multi-broker fixture now has a segmented
campaign descriptor with exact artifact/workflow/broker identities and a
six-hour-compatible job timeout, but its runner-local segments explicitly mark
record-ID reconciliation and cross-segment continuity as unqualified. No
historical or bounded run is being promoted into the new milestone exit gate.

The result-bundle adjudicator
[`check_v1_fault_results.py`](../../../scripts/check_v1_fault_results.py) now
defines the promotion boundary for future artifacts. It requires every named
campaign, contiguous segment indexes, one exact published artifact digest,
qualified per-segment and cross-segment record-ID reconciliation with zero
unaccounted loss/duplicates, drained gauges, and zero secret-scan findings. It also sums six-hour duration,
100 member-loss cycles, and 100 outcomes per ambiguity family, and requires the
controlled data-loss fixtures to match their predeclared outcomes. Existing
diagnostic descriptors intentionally fail this adjudicator because their
cross-segment continuity claim is unqualified. Active work is limited to
hardening and qualifying this campaign harness and its published-artifact
diagnostics until the long-duration gates can run.

### Executable fault schedule hardening (2026-08-23)

The published multi-broker soak workflow now accepts an ordered fault schedule
such as `leader@25,coordinator@50,combined@70,simultaneous@85` instead of
injecting only one broker stop at one-third of a segment. The schedule parser
rejects unknown modes, non-increasing or terminal percentages, and durations
outside the 60-second to six-hour campaign bounds; focused tests cover those
boundaries. A one-segment campaign can now truthfully mark continuity as
qualified because production and reconciliation occur in one retained broker
run, while multi-segment runner-local descriptors remain explicitly
unqualified. The published SASL_SSL/SCRAM multi-broker workflow uses the same
schedule and six-hour timeout contract. No long campaign is promoted by this
workflow change alone.

### Published bounded diagnostic (2026-08-23)

The exact published `0.3.6` pair passed one 60-second Kafka 4.3.1 three-broker
restart segment in [run 32618344222](https://github.com/TaeeunKil/kafrust/actions/runs/32618344222).
The segment used `Acks::All`, recorded the broker image RepoDigest and final
gauges, and uploaded descriptor artifact 9487644196. Two preceding failures
remain retained: `Acks::Leader` left 100 records unreconciled after restart in
[32617622923](https://github.com/TaeeunKil/kafrust/actions/runs/32617622923),
and a fixture gauge-field ordering error failed [32618011465](https://github.com/TaeeunKil/kafrust/actions/runs/32618011465).
The formatter and acknowledgement policy were corrected before the passing
diagnostic. This is evidence for the execution harness only, not a six-hour,
100-cycle, ambiguity-family, controlled-data-loss, or V1-21 completion claim.

### Published secure simultaneous-loss diagnostic (2026-08-23)

The secure simultaneous-loss fixture first failed in
[32632600261](https://github.com/TaeeunKil/kafrust/actions/runs/32632600261)
and [32633284143](https://github.com/TaeeunKil/kafrust/actions/runs/32633284143)
because it advanced sequence identity before a failed `NOT_ENOUGH_REPLICAS`
batch and discarded the unresolved records. Commit `15741d8` enables
idempotence, retains failed batches for replay, and rejects an unresolved
pending batch at the hard deadline.

The corrected exact published `0.3.6` run
[32633658046](https://github.com/TaeeunKil/kafrust/actions/runs/32633658046)
passed a 180-second Kafka 4.3.1 three-broker SASL_SSL/SCRAM-SHA-256 segment
with simultaneous broker 1/2 outage. It reconciled 6,089,400 attempted,
acknowledged, and consumed unique records with zero loss/duplicates, matching
identity digest, `recovered=true`, and drained final gauges. The segment
observed 300 operation errors, 2 failed requests, 5 retries, and 30,000
transient unknown attempts that were recovered by replay. This strengthens
per-segment fault evidence, but continuity remains runner-local and
unqualified; the six-hour, 100-cycle, ambiguity-family, controlled-data-loss,
and repeated-campaign gates remain open.

### Scheduled multi-fault bounded diagnostic (2026-08-23)

The hardened schedule was exercised against the exact published `0.3.6` pair
in [run 32638787704](https://github.com/TaeeunKil/kafrust/actions/runs/32638787704)
on Kafka 4.3.1 three-broker KRaft. A single 120.005-second segment injected
leader loss at 25%, coordinator loss at 50%, combined broker loss at 70%, and
simultaneous two-broker loss at 85%, with ten-second outages. It processed
3,553,800 attempted, acknowledged, and consumed unique records; six failed
requests and 15 retries recovered with zero unknown outcomes, loss, duplicates,
or final resource gauges. The retained descriptor records the immutable broker
image digest, lockfile digest, schedule, reconciliation digest, and
`continuity_claim: qualified` for this one-run segment. This validates the
multi-fault harness only; it is not a six-hour campaign, 100-cycle or
ambiguity-family result, controlled data-loss evidence, or V1-21 completion.

### Bounded current-source diagnostic runs (2026-08-22)

The first 60-second manual run on source `3fdfc778` (run
[32554050050](https://github.com/TaeeunKil/kafrust/actions/runs/32554050050))
exercised Kafka 4.3.1 KRaft with a ten-second broker stop and processed
2,886,500 records. The client-side assertions reached `recovered=true` with
zero final in-flight and buffered gauges, but the workflow failed its `jq`
check because the example passed `buffered_records` and
`max_in_flight_requests` to the JSON formatter in the wrong order. The failure
is retained as harness evidence, not promoted as a client fault.

Commit `f7a5fcff` corrected that field order. The rerun
[32554367028](https://github.com/TaeeunKil/kafrust/actions/runs/32554367028)
passed the same gate: 60.000 seconds, 3,481,600 records, 156 high-level
operation errors, 219 failed requests, 1,091 retries, `recovered=true`,
`in_flight_requests=0`, `buffered_records=0`, and observed peaks of one
in-flight request and zero buffered records. This is a bounded single-node
diagnostic smoke only; it does not satisfy the six-hour, multi-broker,
secured, unclean-election, or published-artifact exit criteria above.

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
