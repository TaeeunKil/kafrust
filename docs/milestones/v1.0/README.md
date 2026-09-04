# kafrust v1.0 Milestone Program

This directory decomposes roadmap milestone M21 into small, evidence-backed
execution milestones. It is the planning source of truth for work from the
current `0.3.6` line through the `1.0.0` release. Historical implementation and
release evidence remains in [the roadmap](../../roadmap.md) and
[compatibility record](../../compatibility.md).

The target is a credible pure-Rust Kafka client for the broker profiles and
workloads that the project explicitly supports. It is not universal
`rust-rdkafka` parity, a Kafka broker, a Kafka Streams application engine, or a
promise that every Kafka API is stable at the high-level client boundary.

## Planning Baseline

The dated baseline and known contradictions are recorded in
[Planning Baseline](baseline.md). The important current facts are:

- planning date: 2026-08-21
- source baseline: `9eba7e5`
- branch state at inspection: clean `main`, synchronized with `origin/main`
- published crates: `kafrust 0.3.6` and `kafrust-protocol 0.3.6`
- exact baseline CI: passing run `32468949663`
- protocol audit: 63 modules and 76 unique Kafka API keys
- pinned schema audit: 12 high-risk schema identity/version/flexible-version
  metadata entries against Kafka 4.3.1
- historical package blocker: the source client package did not compile against
  the already-published `kafrust-protocol 0.3.5`; the coordinated `0.3.6`
  publication repaired that registry boundary

At the dated baseline, the package blocker made
[V1-00](v1-00-repository-and-package-baseline.md) the only valid first
implementation milestone. No later source capability should have been
published from that same-version package state.

## Current Execution

As of 2026-09-04, V1-00, V1-01, V1-02, and V1-19 are `Done`; V1-03 through
V1-18 and V1-20/V1-22 remain `In progress`; V1-21 is `In progress` with its
dedicated Linux x64/Docker runner and host-capacity preflight recovered, while
the long campaigns remain pending; V1-23 is `Blocked` on a named service
canary; and V1-24 through V1-26 are `Planned`.
The coordinated
`0.3.6` pair is published and resolves from fresh external projects on Rust
1.81.0 and stable. The historical regression against published protocol
`0.3.5` was reproduced for the four missing transaction type families, then
closed by the protocol-first `0.3.6` package boundary recorded in
[`v1-20-published-0.3.6-boundary-2026-08-23.md`](../../evidence/v1-20-published-0.3.6-boundary-2026-08-23.md).
The exact pushed-head CI and published diagnostic runs remain the evidence
source for later gates; neither this publication nor those diagnostics claim
V1-20 completion or `1.0.0` readiness. The company workstation now supplies
short local Kafka diagnostics, including a three-broker Streams coordinator
failover, but those runs do not replace the dedicated long-campaign or service
canary requirements.
The same published artifact also passed eleven bounded surface/failover
workflows on 2026-09-03; the grouped record is
[`v1-published-short-surface-smoke-2026-09-03.md`](../../evidence/v1-published-short-surface-smoke-2026-09-03.md).

On 2026-09-04, a published `0.3.6` three-broker Kafka 4.3.1 KIP-848 probe
also verified Metadata v12 topic-UUID continuity while partition 0 moved from
broker 1 to broker 2. The pre/post records and explicit non-claims are in
[`v1-published-topic-id-leader-movement-2026-09-04.md`](../../evidence/v1-published-topic-id-leader-movement-2026-09-04.md).
The same probe passed on the planned Kafka 3.7.2 classic floor line as well.
These are bounded V1-03 diagnostics and do not promote V1-20, V1-21, V1-22,
V1-23, or the release milestones.

The published `0.3.6` artifact also completed the bounded secure group
40-cycle drop/rejoin and normal-leave profiles on Kafka 3.7.2 classic and
Kafka 4.3.1 KIP-848 consumer. All four profiles restored committed offsets
with zero loss/duplicates and drained final gauges; the 100-cycle flag, long
campaigns, service canary, and release gates remain separate. See
[`v1-published-secure-group-churn-40-cycle-2026-09-04.md`](../../evidence/v1-published-secure-group-churn-40-cycle-2026-09-04.md).

The secure group helper was then hardened to distinguish expected same-offset
redelivery from a distinct second offset. Corrected-source reruns from
`ebe694e54c54a373ca63b9f19029247e8dfe93b1` passed all four profiles again
(Kafka 3.7.2 classic drop/leave and Kafka 4.3.1 KIP-848 consumer drop/leave),
with zero loss, zero distinct-offset duplicates, and zero final gauges. This
strengthens the bounded diagnostic evidence only; the 100-cycle family,
long-campaign, service-canary, and release gates remain open. Details are in
the linked evidence record and its four appended ledger rows.

V1-02 has since generated the all-features public API snapshot at
[`docs/evidence/public-api-snapshot.json`](../../evidence/public-api-snapshot.json):
2,374 symbols, twelve public modules, and 288 root exports. CI checks its root
surface and public-declaration digest, while the V1-24 preparation manifest
locks the current counts and feature/toolchain policy. The final freeze remains
owned by V1-24.

V1-03's data-plane manifest and malformed-response boundary tests, V1-04's
typed delivery-deadline contract, and V1-05's deterministic idempotent retry
slice are recorded in their milestone documents. V1-06 records the coherent
TV1 transaction decision and commit/abort ambiguity tests. V1-07 records direct
consumer integrity increments, and V1-08 records the exact-identity typed
OffsetCommit ambiguity contract. V1-09 records KIP-848 epoch/member-ID and
UUID/regex recovery increments. V1-10 records Share acknowledgement/session
and redelivery increments. V1-11 records common controller mutation routing
and ambiguity increments. V1-12 records coordinator/leader routing, V1-13
records security Admin handling, and V1-14 records advanced-surface
classification. V1-15 records the current owner/task/session audit, V1-16 the
credential and redaction slice, V1-17 the bounded metrics/telemetry contract,
V1-18 the decoder/resource-limit and fuzz baseline, V1-19 the staged
pure-Rust package boundary, and V1-20 the checked draft compatibility matrix.
V1-21 through V1-26 have preparation records that preserve the long-soak,
SLO, migration, freeze, RC, and stable-publication prerequisites. The `0.3.6`
publication is a bounded pre-1.0 package boundary only; it does not satisfy
the later fault, SLO, service-canary, API-freeze, RC, or stable-release gates.
These remain implementation and qualification increments, not
`1.0.0`-readiness claims.
V1-14 now also has a machine-checked Apache 4.3.1 API-key classification for
all keys 0-92, including the explicit exclusion of `UPDATE_RAFT_VOTER` (key
82). The inventory and checker are recorded in
[`v1-14-api-key-classification.json`](../../evidence/v1-14-api-key-classification.json);
this is a classification slice, not live qualification of retained expert or
experimental surfaces.
The V1-25 and V1-26 release manifests and checkers now lock the coordinated
version identities, protocol-first publication order, exact RC dependency,
metadata-only stable diff, and explicit authorization boundary; both remain
preparation inputs until their external evidence exists.

The concrete operator steps for clearing the external gates are recorded in
[`external-gate-unblock-runbook.md`](external-gate-unblock-runbook.md). It
requires a safe Linux/Docker self-hosted runner, a named V1-23 service canary,
and the two remaining scheduled weekly fuzz campaign sets; none can be
replaced by a shortened hosted run or a reference-fixture claim.

The initial capacity audit recorded in
[`v1-long-campaign-capacity-audit-2026-08-24.md`](../../evidence/v1-long-campaign-capacity-audit-2026-08-24.md)
found zero registered self-hosted runners and no Windows Docker substitute.
The follow-up WSL2 preflight registered `wsl-ubuntu-t9` and passed a
non-qualification diagnostic, but the first actual V1-21 six-hour campaign
[32649020906](https://github.com/TaeeunKil/kafrust/actions/runs/32649020906)
lost its Ubuntu-T9 instance and completed as `failure` without an artifact.
The 2026-09-03 recovery removed the explicitly authorized stale export,
restored 736 GiB host and 854 GiB Docker free space, and brought the runner
online. A 120-second published diagnostic then passed in
[33716428169](https://github.com/TaeeunKil/kafrust/actions/runs/33716428169).
V1-21 is no longer blocked on preflight, but the exact manifest campaigns must
still be run and adjudicated; V1-22 still needs its complete matrix capacity
and locked baseline. The recovery details are in
[`v1-company-capacity-recovery-2026-09-03.md`](../../evidence/v1-company-capacity-recovery-2026-09-03.md).

On 2026-09-03, the company workstation was confirmed as Windows x64 with an
x86_64 WSL2 environment. Exact source `951f0ead` passed the required Rust
validation, static V1 checkers, package-boundary profiles, deterministic SBOM,
and exact-head CI. The same workstation also ran isolated Kafka 4.3.1/3.7.2
short producer, consumer, group, Admin, Streams, Share, quorum, and telemetry
diagnostics. The detailed local record is
[`v1-company-workstation-nonlong-2026-09-03.md`](../../evidence/v1-company-workstation-nonlong-2026-09-03.md);
the UpdateFeatures cache fix and CI record remain in
[`v1-nonlong-validation-2026-09-03.md`](../../evidence/v1-nonlong-validation-2026-09-03.md).
The return preflight and fresh Kafka 4.3.1/3.7.2 13-test roundtrip results are
in [`v1-company-workstation-return-2026-09-03.md`](../../evidence/v1-company-workstation-return-2026-09-03.md).
These are deterministic/CI diagnostics only. They do not promote a live
qualification row or remove the dedicated Linux/Docker, long-campaign, or
named-service prerequisites.
The latest pushed `main` head `37c5baa` was also rerun on the same workstation
against an isolated Kafka 4.3.1 broker, covering current-source immediate and
buffered producer, classic/KIP-848 group, and Admin lifecycle examples. The
exact record is
[`v1-company-workstation-current-short-smoke-2026-09-03.md`](../../evidence/v1-company-workstation-current-short-smoke-2026-09-03.md);
it remains local diagnostic evidence only.
The same source head also passed the 19-test scripted fault suite plus the
four golden and three malformed protocol fixture tests on Windows Rust 1.81;
the run is recorded in
[`v1-company-short-fault-protocol-smoke-2026-09-03.md`](../../evidence/v1-company-short-fault-protocol-smoke-2026-09-03.md).
Kafka 3.7.2, the planned floor line, was also rerun with the same short
producer/group/Admin smoke; it is recorded separately in
[`v1-company-floor-short-smoke-2026-09-03.md`](../../evidence/v1-company-floor-short-smoke-2026-09-03.md)
and remains diagnostic-only.
The same workstation also passed the three focused Share tests against an
isolated Kafka 4.3.1 Share coordinator; the diagnostic is recorded in
[`v1-company-share-short-smoke-2026-09-03.md`](../../evidence/v1-company-share-short-smoke-2026-09-03.md)
and does not replace the V1-10 published gate.

### Current bounded lifetime diagnostic update (2026-09-04)

The small long-run diagnostic is now resource-capped as well as disk-capped:
each of its three Kafka containers is limited to 1 CPU, 2 GiB memory, and 512
PIDs, with Docker JSON logs limited to three 50 MiB files. This declares a
3-CPU/6-GiB broker-container budget while retaining the 40-GiB host/Docker
watermark, run-scoped cleanup, and fixed broker-restart recovery probe. The
workflow remains diagnostic-only (`qualified=false`) and has not been
dispatched because `wsl-ubuntu-t9` is offline; it cannot close V1-21/V1-22 or
authorize a release. The resource-cap change is on pushed commit `7558560`,
with the follow-up documentation alignment on `db8c9e2` and CI guard on
`317aca9`.

## Status And Evidence

Work status and evidence level are separate axes.

Work status:

- `Planned`: scoped but not started.
- `In progress`: implementation or qualification is active.
- `Blocked`: an explicit external or prerequisite gate prevents progress.
- `Done`: every exit criterion in the milestone document has passed.
- `Superseded`: replaced by a linked decision or milestone whose mapped exit
  criteria and evidence are themselves complete; it is never a shortcut around
  unfinished work.

Evidence level:

- `Design`: scope and contracts only.
- `Local deterministic`: focused unit, protocol, or scripted-broker evidence.
- `CI`: required repository checks passed on an exact pushed commit.
- `Live current-source`: a named live broker profile passed from that commit.
- `Packaged candidate`: tarballs were built and verified without workspace
  path dependencies.
- `Published artifact`: a fresh external project resolved the exact registry
  artifact and passed the stated profile.
- `Service canary`: a representative service passed migration, fault, and
  rollback gates.

`Done` does not imply the highest evidence level. Each milestone declares one
unconditional base evidence rung; all lower applicable rungs, explicit numeric
gates, and any higher conditional evidence named by that milestone still apply.
Target evidence must use exactly one label from the list above. An exclusion is
a classified outcome enforced at CI, not an evidence level. M21 is not done
until V1-26 is complete.

## Readiness Model

Percentages are planning estimates, never completion claims. The existing
strategy ranges are re-baselined with one explicit model:

| Dimension | Weight | 2026-08-21 estimate | Weighted contribution |
| --- | ---: | ---: | ---: |
| Protocol and API breadth | 25% | 75-85% | 18.75-21.25% |
| High-level runtime semantics | 40% | 45-55% | 18-22% |
| Production evidence and release maturity | 35% | 35-45% | 12.25-15.75% |
| **Weighted readiness band** | **100%** |  | **49-59%** |

The band is intentionally close to the earlier 50-60% estimate. It prevents
the 75-85% protocol count from being presented as replacement readiness.
Milestones advance by binary exit gates, not by manually incrementing this
percentage. Recalculate the band only when source and evidence inventories are
updated together.

## Candidate v1 Support Matrix

This is a planning matrix, not a compatibility claim. V1-01 must accept,
change, or explicitly reject every row before implementation milestones use
it as a release gate.

| Role | Exact broker line | Planned use |
| --- | --- | --- |
| Floor probes | 3.3.2 and 3.6.2 | Decide whether to support them or document 3.7.2 as the v1 floor. |
| Proposed floor | 3.7.2 | Full classic-group, core data-plane, Admin, security, and compatibility qualification. |
| Continuity | 3.8.1 | Detect regressions between the floor and later protocol transitions. |
| Pre-4.x current | 3.9.1 | Core data-plane and modern Admin compatibility. |
| 4.x transition | 4.0.0 | Qualify core client behavior; do not use removed early-access Share schemas. |
| Pinned current | 4.3.1 | KIP-848, stable KIP-932 Share, Streams membership, modern Admin, and KRaft qualification. |

The matrix is pairwise, not a full Cartesian product. At minimum, plaintext
core profiles run on every accepted broker line; TLS, SASL/PLAIN,
SASL_SSL/SCRAM-SHA-256, SCRAM-SHA-512, mTLS, and signed OAUTHBEARER run on the
accepted floor and pinned-current lines where the mechanism exists. Classic
groups are required at the accepted floor. KIP-848 and stable Share profiles
are required on the pinned 4.x line. V1-01 owns the final topology and security
table.

## Program Map

All milestones are `Planned` at creation. Existing evidence is an input, not a
reason to pre-close a newly defined exit gate.

| ID | Milestone | Depends on | Base terminal evidence |
| --- | --- | --- | --- |
| V1-00 | [Repository and package baseline](v1-00-repository-and-package-baseline.md) | none | Packaged candidate |
| V1-01 | [Support contract and evidence ledger](v1-01-support-contract-and-evidence-ledger.md) | V1-00 | CI |
| V1-02 | [Public surface classification](v1-02-public-surface-classification.md) | V1-01 | CI |
| V1-03 | [Data-plane protocol qualification](v1-03-data-plane-protocol-qualification.md) | V1-02 | Live current-source |
| V1-04 | [Producer delivery deadline](v1-04-producer-delivery-deadline.md) | V1-03 | Published artifact |
| V1-05 | [Idempotent producer fault matrix](v1-05-idempotent-producer-fault-matrix.md) | V1-04 | Published artifact |
| V1-06 | [Transaction outcome safety](v1-06-transaction-outcome-safety.md) | V1-05 | Published artifact |
| V1-07 | [Direct consumer integrity](v1-07-direct-consumer-integrity.md) | V1-03 | Published artifact |
| V1-08 | [Classic group lifecycle](v1-08-classic-group-lifecycle.md) | V1-07 | Published artifact |
| V1-09 | [KIP-848 group lifecycle](v1-09-kip848-group-lifecycle.md) | V1-07 | Published artifact |
| V1-10 | [ShareConsumer contract](v1-10-share-consumer-contract.md) | V1-07, V1-09 | Published artifact |
| V1-11 | [Controller-routed Admin mutations](v1-11-controller-admin-mutations.md) | V1-02 | Published artifact |
| V1-12 | [Coordinator and leader Admin mutations](v1-12-coordinator-leader-admin-mutations.md) | V1-02 | Published artifact |
| V1-13 | [Bootstrap and security Admin mutations](v1-13-bootstrap-security-admin-mutations.md) | V1-02, V1-11 | Published artifact |
| V1-14 | [Advanced protocol surfaces](v1-14-advanced-protocol-surfaces.md) | V1-02, V1-11 | CI |
| V1-15 | [Session ownership and shutdown](v1-15-session-ownership-and-shutdown.md) | V1-04-V1-14 | Published artifact |
| V1-16 | [Security and credential lifecycle](v1-16-security-credential-lifecycle.md) | V1-15 | Published artifact |
| V1-17 | [Telemetry and metrics contract](v1-17-telemetry-and-metrics-contract.md) | V1-02, V1-15 | Published artifact |
| V1-18 | [Resource limits and fuzzing](v1-18-resource-limits-and-fuzzing.md) | V1-03, V1-15 | CI |
| V1-19 | [Pure-Rust dependency audit](v1-19-pure-rust-dependency-audit.md) | V1-02 | Packaged candidate |
| V1-20 | [Published compatibility matrix](v1-20-published-compatibility-matrix.md) | V1-03-V1-19 | Published artifact |
| V1-21 | [Fault soak and data-loss semantics](v1-21-fault-soak-and-data-loss.md) | V1-15, V1-16, V1-18, V1-20 | Published artifact |
| V1-22 | [Performance and operational SLOs](v1-22-performance-and-operational-slos.md) | V1-18, V1-20, V1-21 | Published artifact |
| V1-23 | [Migration adapter and rollback](v1-23-migration-adapter-and-rollback.md) | V1-04-V1-17 | Service canary |
| V1-24 | [Public API freeze](v1-24-public-api-freeze.md) | V1-20-V1-23 | Packaged candidate |
| V1-25 | [Release candidate](v1-25-release-candidate.md) | V1-24 | Service canary |
| V1-26 | [v1.0 release](v1-26-v1-release.md) | V1-25 | Service canary |

Milestone IDs are not crate versions. A milestone may require more than one
pre-1.0 release, and several milestones may ship together. Only V1-25 and
V1-26 prescribe semver release identities. Dependency edges order contracts
and deterministic implementation, not one publication per milestone; V1-20
may supply the shared published-artifact evidence that closes several already
implemented milestones on one candidate.

## Dependency Shape

```mermaid
flowchart LR
    B["V1-00 package baseline"] --> C["V1-01 support contract"]
    C --> S["V1-02 surface classification"]
    S --> P["V1-03 protocol qualification"]
    P --> W["V1-04..V1-10 data plane and groups"]
    S --> A["V1-11..V1-14 Admin and advanced surfaces"]
    W --> O["V1-15 session ownership"]
    A --> O
    O --> H["V1-16..V1-19 cross-cutting hardening"]
    H --> M["V1-20 compatibility matrix"]
    M --> Q["V1-21..V1-23 operations and adoption"]
    Q --> F["V1-24 API freeze"]
    F --> R["V1-25 release candidate"]
    R --> V["V1-26 v1.0"]
```

V1-03 through V1-19 may proceed in parallel only where their dependency lists
permit it. V1-20 is the convergence point: it reruns the accepted matrix with
the same candidate rather than combining evidence from unrelated commits.

## Program Exit Definition

M21 and this program are complete only when all of the following are true:

1. Every V1-00 through V1-26 milestone is `Done`, or is `Superseded` by a linked
   completed decision/milestone that maps and satisfies every inherited exit
   criterion and evidence gate.
2. Every public capability is classified as stable, expert/experimental, or
   excluded, and the classification matches the actual crate exports.
3. Every stable operation has explicit pre-transmission, post-transmission,
   retry, timeout, cancellation, shutdown, and reconciliation semantics.
4. The accepted broker/security/topology matrix passes from the same release
   candidate, with current-source and fresh external artifact evidence kept
   separate.
5. Resource, malformed-input, fault, soak, and performance gates meet their
   recorded thresholds with zero unaccounted acknowledged loss outside declared
   broker-loss fixtures and no hidden duplicates.
6. A representative migration canary can deploy, observe, and roll back
   without rewriting business logic.
7. Public API, MSRV, features, defaults, errors, deprecation policy, and the
   `kafrust`/`kafrust-protocol` semver relationship are frozen.
8. Protocol-first `1.0.0` publication, docs.rs, external-project smoke, GitHub
   release, and post-publish qualification all pass on the exact tagged source.

These gates authorize a claim only for documented supported profiles. They do
not authorize “production-ready everywhere,” Kafka broker replacement, or
complete Kafka Streams compatibility.

## How To Execute A Milestone

Use [Execution Rules](execution-rules.md) and copy [_template.md](_template.md)
when a milestone needs to be refined. One task should own one coherent work
package or one evidence gate. Do not mark a milestone done merely because its
code exists; record the exact evidence required by that milestone.
