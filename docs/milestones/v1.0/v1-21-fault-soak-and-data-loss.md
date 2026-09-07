# V1-21 Fault Soak And Data-Loss Semantics

- Status: In progress (long campaigns pending; runner/capacity preflight recovered 2026-09-03)
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

The same schedule then passed over the published `0.3.6`
SASL_SSL/SCRAM-SHA-256 path in [run 32639181770](https://github.com/TaeeunKil/kafrust/actions/runs/32639181770).
The 120.002-second Kafka 4.3.1 three-broker segment processed 2,943,900
attempted, acknowledged, and consumed unique records at 24,532 records/s;
four failed requests and 11 retries stayed below the 1% retry budget, with zero
unknown outcomes, loss, duplicates, and final gauges. The descriptor retains the
same four-event schedule, exact package/broker identities, reconciliation digest,
and qualified one-run continuity. This strengthens secured per-segment evidence
only; the six-hour campaigns and all downstream V1-21 gates remain open.

### 100-cycle Share member-loss harness (2026-08-23)

The published Share repeated-member-loss workflow now accepts a bounded
`cycles` input (defaulting to the historical eight-cycle diagnostic) and
computes expected ownership records, per-member output, and the final-cycle
diagnostic dynamically. It rejects values above the V1-21 bound of 100 and
uploads a JSON campaign summary with `qualified: true` only when exactly 100
cycles are requested. This makes the required 100-cycle published gate
executable without promoting the existing eight-cycle evidence. The Share
qualification run remains in progress; the classic group result is recorded
separately below.

The first parallel six-hour execution set exposed an infrastructure failure,
not a client result: the Kafka 3.7.2 floor run exhausted the hosted runner
disk while its failure hook emitted unbounded Docker logs, and one secured
repeat was terminated without a result artifact. The multi-broker workflows
now cap each broker's JSON log rotation at three 50-MiB files and retain only
the final 200 diagnostic lines on failure. Those runs are classified as
infrastructure failures and must be rerun; they do not pass or fail the
client's fault semantics.

The first rerun with bounded broker logs still exhausted a runner because the
published clients printed every retryable Produce/Fetch error while brokers
were unavailable. The plaintext and secured soak clients now retain the same
error counters and reconciliation behavior but rate-limit repeated error
diagnostics to one line per ten seconds. The affected run is still
infrastructure-only evidence; the six-hour campaign must be rerun from the
HEAD containing this client-side log bound.

The rerun workflows now default to a pinned `self-hosted` runner label and
reject `ubuntu-*`, `windows-*`, and `macos-*` labels before allocating the
long-running job. [GitHub-hosted jobs](https://docs.github.com/en/actions/reference/limits)
cannot provide setup plus a six-hour soak within their six-hour execution
ceiling. At the time of this preparation snapshot, no self-hosted runner was
registered for this repository, so the pending runs remain infrastructure
diagnostics. A WSL2 runner was later registered; the current execution record
below covers that capacity transition. Registration clears only the
infrastructure prerequisite: a campaign still requires its immutable
descriptor and adjudicator pass before promotion.

The published classic/KIP-848 group-rebalance fixture was likewise raised from
its previous 64-cycle guard to a maximum of 100 cycles. Its timeout now scales
with the requested cycle count, and the workflow timeout was extended so a
100-cycle run is not silently cut off. These changes make the two remaining
group-family executions possible. The exact published `0.3.6` classic run
[32642112754](https://github.com/TaeeunKil/kafrust/actions/runs/32642112754)
completed all 100 cycles and uploaded the immutable summary with the published
lockfile digest and broker image identity; it closes only the classic 100-cycle
sub-gate. The exact published KIP-848 consumer run
[32642114151](https://github.com/TaeeunKil/kafrust/actions/runs/32642114151)
also completed all 100 cycles with the same zero-loss/duplicate and drained
gauge result on Kafka 4.3.1. Both rows are promoted only for their exact group
protocol and broker profiles; Share, ambiguity-family, long-soak, and
controlled-data-loss gates remain open.

### Published classic 100-cycle member-loss result (2026-08-23)

The published `kafrust 0.3.6` classic group run on Kafka 3.7.2 completed 100
forced member-loss/rejoin cycles across six partitions. The retained summary
reports `cycle_count=100`, `records_per_cycle=6`, zero loss and duplicates, and
zero final in-flight/buffered gauges. It records source `7cc0b55`, the exact
published version, lockfile SHA-256, and broker image identity. This qualifies
the classic group sub-gate; together with the KIP-848 row above, both group
100-cycle sub-gates are covered. Share, ambiguity-family, long-soak, and
controlled data-loss gates remain open.

Two Share qualification attempts with member dwell values of one and ten
seconds also failed before the first post-loss seed was consumed: the surviving
member exited before coordinator assignment recovered. They are retained as
harness timing failures, not client data-loss evidence; the 100-cycle attempt
must use the existing 120-second member dwell that passes the bounded workflow.

### Six-hour campaign rerun set (2026-08-23)

After the broker-log and client-error-rate limits passed exact-head CI
32644139214, the four manifest campaigns were relaunched from source
7c24f7399325b5d0ab6f91f6e2ecd4d5b49985ec:

- plaintext Kafka 3.7.2 floor: run 32644605379
- secured Kafka 4.3.1 repetition 1: run 32644605637
- secured Kafka 4.3.1 repetition 2: run 32644605633
- secured Kafka 4.3.1 repetition 3: run 32644605743

All use a single contiguous segment, the manifest six-hour duration, ten-second
broker outages, and the declared increasing fault schedule. The four ledger
rows record these as In progress with result not-run. They become qualifying
evidence only after the immutable segment descriptors, reconciliation,
resource-gauge drain, and fault-result adjudicator all pass; an infrastructure
failure is retained as non-qualification rather than converted into a client
claim.

### Stale hosted-run cancellation (2026-08-23)

By 23:34 KST, the Share 100-cycle run `32642115585` had been in progress for
more than ten hours without a result artifact, and the four six-hour runs had
been in progress for more than nine hours. Four of the six-hour runs
(`32644605379`, `32644605637`, `32644605743`) and the Share run were cancelled
after the stale-run check; `32644605633` received the same cancellation request
but remained stuck in the GitHub `in_progress` state at the time of recording.
None produced a descriptor or campaign summary, so all are infrastructure-only
non-results. The hosted-runner timeout/capacity problem is now prevented by the
self-hosted runner guard above; no client fault claim or V1-21 promotion is
derived from these runs.

The outstanding secure run `32644605633` later accepted the cancellation
request and completed as `cancelled`; all four six-hour runs and the Share
100-cycle run are now terminal infrastructure non-results with no descriptor
or summary artifact.

### Long-campaign capacity re-audit (2026-08-24)

The repository runner inventory has one registered self-hosted runner, but it
is offline and not usable for a campaign. The exact-head CI pass in
[32646817241](https://github.com/TaeeunKil/kafrust/actions/runs/32646817241)
confirms the manifest and hosted-label guard, while the workstation has no
Docker executable for a local substitute. The four six-hour campaigns and the
Share 100-cycle run therefore remain unqualified infrastructure gates; no
campaign is dispatched or promoted by reducing the duration or changing the
runner requirement. The audit is retained in
[`v1-long-campaign-capacity-audit-2026-08-24.md`](../../evidence/v1-long-campaign-capacity-audit-2026-08-24.md).

### Rate-limited lifetime diagnostic workflow (2026-09-04)

To make a small long-duration check safe without weakening the V1-21 gate, the
repository now provides
[`published-multi-soak-lifetime-diagnostic.yml`](../../.github/workflows/published-multi-soak-lifetime-diagnostic.yml).
It uses a three-broker/RF3 topology, a global 1,000-records/s limiter, 256-byte
values, a two-hour maximum, run-scoped Docker names, before/after capacity
artifacts, and a 40-GiB disk watermark abort. Each broker is capped at 1 CPU,
2 GiB memory, and 512 PIDs, with Docker JSON logs limited to three 50 MiB
files, giving the diagnostic a declared 3-CPU/6-GiB broker-container budget.
The helper rejects a zero rate, injects one fixed 10-second broker-1 restart
halfway through the run, and the workflow descriptor is always
`qualified=false`.

This workflow is a runner-lifetime, broker-recovery, cleanup, and gauge-drain
diagnostic. It is not a shortened V1-21 throughput campaign: the exact
10,000-records/s, 1-KiB, six-hour manifest, fault families, and adjudication
requirements remain unchanged. It has not been dispatched while the current
self-hosted runner is offline/stopped.

Pushed head `730dd77` passed the repository CI safety and formatting checks in
[33847498831](https://github.com/TaeeunKil/kafrust/actions/runs/33847498831) on
stable and Rust 1.81. This proves the workflow is accepted by the repository
checks, not that the diagnostic or the official campaign has run.

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

### WSL2 runner interruption and first long campaign failure (2026-08-24)

The available Ubuntu-T9 WSL2 environment passed a non-qualification 60-second
diagnostic after installing the missing `python` and `jq` host utilities. The
repeat diagnostic
[32648820867](https://github.com/TaeeunKil/kafrust/actions/runs/32648820867)
passed runner selection, Docker Kafka startup, published `0.3.6` build,
broker restart, descriptor validation, and artifact upload. It is explicitly
not a six-hour result.

The first actual manifest campaign, `pinned-secured-six-hour-1`, was dispatched
from source `54c8e21` as
[32649020906](https://github.com/TaeeunKil/kafrust/actions/runs/32649020906)
with Kafka 4.3.1, SASL_SSL/SCRAM-SHA-256, the four-event fault schedule, and
`duration_seconds=21600`. The WSL distribution then stopped and returned
`Wsl/Service/CreateInstance/E_FAIL`/`E_UNEXPECTED` on restart; the GitHub run
eventually completed as `failure` at `2026-08-23T18:07:57Z` with no downloadable
artifact. No V1-21 ledger row or milestone completion claim is made. The
runner must be restored, the orphaned run's failure retained, and this exact
manifest campaign rerun before any result adjudication.

The host inspection identified why the WSL recovery is failing: the `T:`
volume holding `Ubuntu-T9` has only `31,256,576` free bytes (`29.8 MiB`),
while its `ext4.vhdx` is approximately `773.8 GiB`. The WSL and Hyper-V
services are running, so this is a host-capacity failure rather than a
campaign or client result. Freeing safe host storage and confirming the WSL
instance can start is required before any V1-21 rerun; the VHDX must not be
deleted or repaired in place without a backup.

### Live recovery update (2026-08-24)

The backup was moved to `C:\Users\user\Backups\Ubuntu-full.tar` and the old
`T:\Backups\Ubuntu-full.tar` copy was removed. `Ubuntu-T9` now boots with
`185 GiB` free on `/dev/sdd`. Live Docker accounting identifies the previous
capacity failure: the three exited
`kafrust-published-secure-multi-soak-{1,2,3}` containers from run
`32649020906` each retain a `211 GB` writable layer, for `631.7 GB` total;
Docker build cache adds `85.89 GB` (`61.44 GB` reclaimable). Their Kafka log
trees are stale failure-run data and produce no V1-21 evidence.

The configured `wsl-ubuntu-t9` listener was restarted manually because no
systemd service was installed. GitHub now reports one idle online runner with
the required `self-hosted`, `Linux`, `X64`, `docker`, and `wsl2` labels. The
runner recovery removes the infrastructure blocker, but V1-21 remains open:
the stale containers/cache must be cleaned, then the exact six-hour manifest
must be rerun and adjudicated before any ledger row can pass.

The root cause and prevention runbook are consolidated in
[`v1-wsl-capacity-incident-2026-08-24.md`](../../evidence/v1-wsl-capacity-incident-2026-08-24.md).
The long-campaign workflows now check the Windows volume that owns the WSL
VHDX before dispatch and always clean campaign-scoped Docker resources after
diagnostics. These controls prevent repeat host exhaustion but do not turn the
failed run into V1-21 evidence.

The three stale containers were removed with their anonymous data volumes and
the unused Docker build cache was pruned. WSL then reported `110 GiB` used and
`846 GiB` available. A fresh export completed with exit code `0` at
`T:\Backups\Ubuntu-full-2026-08-24.tar` (`115,386,419,200` bytes). The old C:
copy was deleted only after this success; the new export is a same-volume
recovery copy, not independent disaster-recovery storage. V1-21 still requires
the exact six-hour manifest rerun and adjudication.

### Company capacity recovery and short self-hosted diagnostic (2026-09-03)

The stale same-volume export was later inspected and, under the previously
authorized backup cleanup, removed as the exact 115,386,419,200-byte file. No
other T: content, the WSL VHDX, or existing Docker resources was changed. The
capacity guard now passes with 736 GiB free on `/mnt/t` and 854 GiB free under
`/var/lib/docker`. A temporary resolver override brought the installed
`wsl-ubuntu-t9` service online and idle; no persistent WSL network setting was
changed.

The published `0.3.6` 120-second self-hosted diagnostic
[33716428169](https://github.com/TaeeunKil/kafrust/actions/runs/33716428169)
ran Kafka 4.3.1 three-broker leader/coordinator/combined/simultaneous faults.
It reconciled 3,012,600 unique records with zero loss/duplicates/unknown
outcomes, 4 failed requests, 11 retries, and drained gauges. This is evidence
that the execution path is usable again, not a six-hour campaign result. The
full four six-hour campaigns, 100-cycle/ambiguity families, controlled
data-loss fixtures, and adjudication remain open. Details are in
[`v1-company-capacity-recovery-2026-09-03.md`](../../evidence/v1-company-capacity-recovery-2026-09-03.md)
and [`v1-company-selfhosted-short-fault-2026-09-03.md`](../../evidence/v1-company-selfhosted-short-fault-2026-09-03.md).

The published short-surface follow-up also passed the bounded 180-second
Share member-loss window; a 30-second shortened-input attempt failed its
three-to-six partition reassignment assertion and is retained separately.
Neither result is promoted to the six-hour V1-21 campaign or its adjudicated
100-cycle/data-loss evidence.

### Runner DNS recovery follow-up (2026-09-04)

The generated WSL resolver regressed to `10.255.255.254` after the prior short
diagnostic, leaving the installed listener offline even though its service and
Docker were active. A root-only, temporary override using `168.126.63.1` and
`8.8.8.8`, followed by a runner-service-only restart, restored DNS and returned
the listener to `Listening for Jobs`; GitHub now reports it online and idle.
The WSL VM, Docker daemon, and existing resources were not restarted or
pruned. This restores execution capacity but is not a persistent resolver fix,
and no long campaign is dispatched until an online preflight is repeated.

The immediate recovery check then passed in
[run 33817682088](https://github.com/TaeeunKil/kafrust/actions/runs/33817682088):
the exact published `0.3.6` pair processed 1,736,700 unique 1-KiB records for
60.002 seconds across leader, coordinator, and combined events, with four
failed requests, eight retries, zero unknown/loss/duplicate outcomes, and
drained gauges. This confirms the temporary DNS-recovered runner path only;
the six-hour campaigns and all V1-21 qualification gates remain open. See
[`v1-company-selfhosted-short-dns-recovery-2026-09-04.md`](../../evidence/v1-company-selfhosted-short-dns-recovery-2026-09-04.md).

### Persistent resolver policy staged (2026-09-04)

The Ubuntu-T9 host now has `[network] generateResolvConf = false` in
`/etc/wsl.conf`, a regular `/etc/resolv.conf` with the verified resolvers, and
dated rollback copies. Only the runner service was restarted; Docker and all
existing resources were left untouched. GitHub reports the runner online and
idle. An approved `wsl --shutdown` was not performed, so the policy still
needs one controlled restart verification before an unattended six-hour
campaign. No V1-21 long campaign was dispatched from this change.

### Self-hosted lifecycle cancellation (2026-09-04)

A bounded published smoke was accepted by `wsl-ubuntu-t9` after the resolver
policy was staged, but [run 33824960369](https://github.com/TaeeunKil/kafrust/actions/runs/33824960369)
was cancelled during `Install Rust` when the runner service stopped at
10:14:19 KST. The WSL boot then powered off and the service returned later;
Kafka startup was never reached and no campaign Docker resources or client
artifact were created. This is a host lifecycle non-result, not a Kafka,
Rust, capacity, or fault-soak failure.

The journal records repeated WSL poweroff/reboot cycles and a runner unit with
`Restart=no`; no matching systemd timer was found, and the Windows-side actor
that initiated the shutdown is not identified. The event therefore keeps the
runner lifecycle gate open. It is not V1-21 evidence and must not be counted
as a failed campaign result. Before dispatching a six-hour campaign, establish
a host-level lifetime guarantee (for example, an operator-held foreground WSL
session), verify one complete short smoke without service interruption, and
complete the separately approved full-restart resolver check. Full campaign
duration, family counts, ambiguity outcomes, data-loss fixtures, and
adjudication remain required.

### Foreground WSL lifetime retry (2026-09-04)

The same bounded published path was rerun while an operator-held foreground
WSL process kept Ubuntu-T9 alive. [Run 33825722908](https://github.com/TaeeunKil/kafrust/actions/runs/33825722908)
completed the 120.663-second Kafka 4.3.1 three-broker smoke with 3,624,900
produced and consumed unique 1-KiB records, one operation error, 17 failed
requests, and 31 retries. Recovery was true with zero unknown outcomes, loss,
or duplicates, and final in-flight/buffered gauges of zero. The descriptor and
artifact details are in
[`v1-company-selfhosted-foreground-lifetime-smoke-2026-09-04.md`](../../evidence/v1-company-selfhosted-foreground-lifetime-smoke-2026-09-04.md).

This validates only the bounded execution path under an explicit foreground
lifetime guard. It does not prove unattended WSL lifetime, resolver persistence
across a full restart, or any V1-21 duration/family/data-loss gate; the exact
six-hour campaigns remain open.

After cleanup, the operator released the foreground process and the registered
runner reported offline again. This confirms that the present WSL host does not
remain resident on the enabled systemd runner alone; a host-level lifetime
mechanism is required before unattended V1-21 execution.

### Bounded long-campaign sizing (2026-09-04)

The current published multi-soak helper sends batches as fast as the broker and
client permit; it does not impose the V1-21 floor as a rate limit. The recovered
company-runner diagnostic [33817682088](https://github.com/TaeeunKil/kafrust/actions/runs/33817682088)
processed 1,736,700 1-KiB records in 60.002 seconds. With replication factor
three, that observed rate is approximately 4.97 GiB of retained broker data per
minute before Kafka segment/index and filesystem overhead. Therefore the local
helper should be treated as a bounded diagnostic only: a 10-minute run needs at
least 100 GiB free, and a 30-minute run at least 250 GiB free; a two-hour run is
not recommended on the current host and a six-hour run is not feasible at the
observed rate.

If a longer lifetime probe is useful, it must use a separately identified,
rate-limited diagnostic workload (for example 1,000 records/s with 256-byte
payloads, about 2.6 GiB/hour at replication factor three) and a disk-watermark
abort. Such a probe can exercise runner lifetime, broker restart recovery,
cleanup, and final gauge draining, but it cannot be promoted to the V1-21
10,000-records/s, 1-KiB, six-hour qualification. The exact V1-21 manifest and
its four family/adjudication gates remain unchanged and undispatched until the
runner is online/idle with persistent DNS and a verified lifetime guarantee.

The full capacity calculation and host margin are retained in
[`v1-long-campaign-capacity-audit-2026-08-24.md`](../../evidence/v1-long-campaign-capacity-audit-2026-08-24.md).

### Read-only capacity preflight refresh (2026-09-07)

The company workstation was checked again before any campaign dispatch. The
Windows `T:` volume had `736.80 GiB` free and Windows reported `7.7 GiB` free
RAM out of `31.3 GiB`. `Ubuntu-T9` was `Stopped` and the registered
`wsl-ubuntu-t9` runner was `offline` with `busy=false`, so Docker-root space
could not be measured and no workflow was dispatched. The complete observation
and its non-qualification boundary are in
[`v1-long-campaign-capacity-preflight-2026-09-07.md`](../../evidence/v1-long-campaign-capacity-preflight-2026-09-07.md).

The T: storage figure is above the prepared diagnostic's 40-GiB watermark but
does not by itself establish a runnable campaign: WSL, Docker, runner
connectivity, Docker-root capacity, and live memory must be rechecked after an
authorized start. The exact V1-21 six-hour workload and V1-22 SLO campaigns
remain pending and unchanged.
