# Pre-1.0 Release Track

This track describes the releases that can be completed by a small project
team while the broader V1 qualification program remains open. It does not
replace [V1-21](../v1.0/v1-21-fault-soak-and-data-loss.md) or
[V1-22](../v1.0/v1-22-performance-and-operational-slos.md); those documents
still define the evidence required for a broad production-readiness claim.

The release track separates a usable, well-scoped pre-1.0 package from the
later work needed for a stable 1.0 API and service-level qualification. A
release can advance when its own profile gates pass and its non-claims are
recorded. Milestone names here are release-oriented; they are not substitutes
for the V1-00 through V1-26 IDs.

## Release map

| Release | Focus | Status | Exit evidence |
| --- | --- | --- | --- |
| `0.4` | Stabilize the current client and bounded long-run path | In progress | Exact source/package identity, bounded soak, focused published smoke, and explicit limits |
| `0.5` | Portable resource profiles | Planned | Declared resource envelopes with reproducible configuration and retained resource traces |
| `0.6` | Operational recovery confidence | Planned | Targeted broker restart, coordinator/leader recovery, response-loss, and reconciliation evidence |
| `0.7` | Performance baseline and migration readiness | Planned | Repeatable profile baseline, tuning guidance, migration notes, and rollback rehearsal |
| `0.8` | Scoped 0.x release candidate and external adoption | Planned | Fresh external-project smoke, canary/rollback rehearsal, and release-candidate artifact |
| `1.0` | Optional broad stability and production qualification claim | Conditional | Full V1-00 through V1-26 program exit criteria, including V1-21/V1-22 requirements |

Before running a release gate, record its exact candidate identity, accepted
profiles, pass/fail thresholds, and evidence locations. Candidate checks and
post-publication smoke must identify their respective artifacts; a smoke run
against an older published version does not qualify the new release. The
[release preparation policy](../../release.md) continues to govern publication.

## Current workstation execution plan

The current Windows workstation is suitable for sequential 0.x qualification:
8 CPU cores/16 threads, about 31 GiB of host memory, about 15 GiB visible to
WSL2, and more than 700 GiB free on the campaign and Docker volumes. The local
policy uses one active phase at a time and treats roughly half of the WSL2
memory as the soft operating budget:

- keep measured WSL2 working set below 8 GiB and broker CPU below 4 vCPU in
  aggregate;
- keep the three broker containers at their declared 1 CPU/2 GiB caps, with
  helper and build processes included in the observed budget;
- keep at least 100 GiB free and stop a phase if its growth exceeds 20 GiB;
- do not run V1-22 jobs in parallel on this workstation.

These are resource-safety limits for local work. Lowering a rate or payload to
fit the envelope creates a scoped 0.x profile; it does not satisfy the official
V1-21 or V1-22 workload gates.

The practical schedule is:

| Work | Execution on this workstation | Expected elapsed time |
| --- | --- | --- |
| 0.4 bounded qualification | Preflight, 6-hour secure phase, 12-hour plaintext phase, reconciliation, and package smoke | 1–2 days |
| 0.5 additional envelope | One short preflight plus one secure and one plaintext bounded run per declared profile | 1–2 days per profile |
| 0.6 recovery confidence | Targeted broker/coordinator/leader restart, response-loss, and group-rejoin slices | 2–3 days |
| 0.7 local baseline | Fixed representative profiles, repeated on the same runner, with migration/rollback rehearsal | 3–5 days |
| V1-21 official gate | Four six-hour fault campaigns plus 100-cycle, ambiguity, retention, and unclean-election fixtures | At least 3–5 days, with reruns extending it |
| V1-22 official gate | 120 jobs, each two-hour warmup plus six-hour measurement | About 40 days sequentially, before reruns |

The current interrupted attempt can retain its completed secure result after
adjudication, but the missing plaintext phase must be rerun before 0.4 is
complete. A rerun should start at the missing phase after a fresh capacity
preflight rather than repeat successful phases blindly.

## 0.4 — Stabilize

The current 0.4 gate is deliberately small and reviewable:

- finish the bounded 6-hour secure and 12-hour plaintext campaigns;
- optionally extend the plaintext phase to 24 hours for additional diagnostic
  evidence without changing the 0.4 gate;
- retain exact source, broker image, workload, resource, reconciliation, and
  final-gauge evidence;
- require zero unexplained loss or duplicates and no resource-guard breach;
- rerun the published-package smoke and required Rust/package checks;
- document the tested broker, security, runtime, and workload boundary.

Passing this gate supports a scoped pre-1.0 release. It does not claim the
full V1-21 fault matrix, V1-22 performance SLO, managed-service support, or
universal `rust-rdkafka` parity.

The 12-hour plaintext run is the workstation-sized 0.4 gate. It is shorter
than the former 24-hour diagnostic and must not be described as equivalent to
that extended run; the 24-hour variant remains optional evidence.

## 0.5 — Portable profiles

Make the bounded campaign reproducible across declared resource envelopes,
including at least one additional envelope alongside the existing baseline.
Select profiles from available capacity and intended workloads; this milestone
does not require a particular device, architecture, or host-memory size. Each
profile declares its operating system, architecture, host memory, CPU and
Docker budgets, broker limits, disk reserve, rate, payload, duration, and
evidence level. Record the same source and artifact identity across profiles
while keeping their results separate.

A passing profile proves long-run behavior only within its declared envelope.
It is supplemental profile evidence unless the release manifest explicitly
names it as an accepted environment. Additional profiles do not redefine the
existing baseline or relax the V1-21/V1-22 qualification gates.

## 0.6 — Operational recovery

Expand the solo-friendly campaign around the failure modes that affect normal
client operation:

- leader, coordinator, and single-broker restart recovery;
- selected response-loss and cancellation outcomes;
- classic and KIP-848 group rejoin with exact record reconciliation;
- bounded Share/Admin checks when their fixtures are available;
- retained resource series and drained final gauges.

This milestone improves operational confidence without pretending that a
small targeted matrix is the complete V1-21 campaign.

## 0.7 — Baseline and migration

Create a repeatable, profile-specific performance baseline, publish the
configuration and backpressure guidance, and test the migration and rollback
path from `rust-rdkafka` for the supported API subset. Baselines are compared
only with the same hardware, broker, artifact, and workload profile; a local
baseline is not a universal SLO.

## 0.8 — Scoped 0.x release candidate

Build a candidate from an immutable source and package identity, run a fresh
external-project smoke, exercise the documented canary and rollback procedure,
and publish the remaining limitations. This is the point at which a service
can decide whether the scoped client is acceptable for its own workload.
This candidate does not imply an API freeze or satisfy the V1-25 `1.0` release
candidate gate; the V1-23 service-canary requirements also remain separate.

## When to pursue 1.0

Move from this track to `1.0` only when the project wants to make a broad
stability and production-qualification claim. At that point the full V1
milestones remain the source of truth: published artifact identity, complete
fault/data-loss evidence, repeated performance profiles, locked baselines,
migration canary, API freeze, and release-candidate publication.
