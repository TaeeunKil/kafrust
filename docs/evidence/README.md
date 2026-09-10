# Evidence archive

This directory stores dated qualification records, machine-readable manifests,
and diagnostic outputs. The files are deliberately retained as historical
records; they are not a single checklist and they do not all carry the same
evidence level.

## Read in this order

1. [Qualification ledger](qualification-ledger.md) for the gate-by-gate index.
2. The relevant [v1.0 milestone](../milestones/v1.0/README.md) for its exit
   criteria and required evidence level.
3. The newest dated record for the exact broker, artifact, workload, and
   environment under review.
4. The linked manifest or JSON record when a checker or workflow generated one.

## Evidence levels

The project uses these labels consistently:

- `Design` — scope or contract only
- `Local deterministic` — focused unit, protocol, or scripted-broker evidence
- `CI` — required repository checks on an exact pushed commit
- `Live current-source` — a named broker profile from that commit
- `Packaged candidate` — an isolated package artifact without workspace paths
- `Published artifact` — a fresh external project resolving the registry artifact
- `Service canary` — migration, fault, and rollback evidence from a representative service

The label describes the artifact boundary and environment. A `Done` milestone
still needs every numeric gate and every lower applicable evidence rung named by
its milestone document.

## File families

- `v1-20-*`, `v1-23-*`, and related published records document compatibility,
  migration, and release-boundary observations.
- `v1-company-*` records workstation diagnostics. They are useful for local
  reproduction and capacity checks, but are not automatically published or
  service-canary evidence.
- `v1-current-host-*` records the latest read-only runner and storage
  preflight. A capacity finding blocks unsafe dispatch; it is not a gate pass.
- `v1-local-*` records bounded workstation campaigns and their resource guards.
  The 2026-09-08 follow-up is explicitly diagnostic until its long phases finish
  and the adjudicator accepts the retained artifacts.
- `v1-0_6-*` records the bounded operational-recovery slice against published
  artifacts; its unknown outcomes and workload limits remain part of the record.
- `v1-18-*` and `v1-19-*` cover fuzzing, dependency, license, native-tooling,
  and package audits.
- `*.json` manifests and snapshots are machine-readable inputs for checkers;
  the adjacent Markdown record explains how to interpret them.

## Retention rules

Do not rewrite an old dated record to make a later run look current. Add a new
dated record, link the superseding observation, and preserve the original
result, source identity, broker image, workload, and non-claims. A passing
short smoke, workflow, or source-tree implementation must not be promoted to a
published-artifact or service-canary claim without the required boundary proof.
