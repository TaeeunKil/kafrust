# V1-XX Milestone Name

- Status: Planned
- Target evidence: Design / Local deterministic / CI / Live current-source /
  Packaged candidate / Published artifact / Service canary
- Conditional evidence: none, or one higher canonical rung tied to an explicit
  retained/selected branch
- Dependencies: V1-XX

## User-Visible Objective

State one capability or evidence outcome that a Kafka client user can observe.

## Non-Goals

- Name adjacent APIs, refactors, broker behavior, and compatibility claims that
  do not belong in this milestone.

## Scope

### Source And Ownership

- Exact current source modules.
- Connection/session/task owner and lifetime.
- Public API additions, changes, removals, or “no public API change.”

### Kafka Protocol

| API | Key | Versions | Required behavior |
| --- | ---: | --- | --- |
| Name | 0 | v0 | Header, nullable, flexible, fallback, and limit details. |

### Documentation And Automation

- Exact docs, examples, scripts, and workflow files.

## Work Packages

1. Smallest coherent implementation or evidence slice.
2. Deterministic regression that fails before the behavior is accepted.
3. Live and artifact qualification.
4. Documentation and evidence record.

## Failure And Lifecycle Contract

| Condition | Required result |
| --- | --- |
| Pre-transmission failure | Retry or terminal behavior and total budget. |
| Post-transmission loss | Replay, unknown outcome, fencing, or reconciliation. |
| Cancellation/timeout | Owner, state transition, and connection disposition. |
| Shutdown | Queue/session cleanup and error ordering. |

## Verification

### Deterministic

- Exact focused test and observable assertions.

### Live And Published

- Exact broker version, topology, security mode, workload, fault, count, and
  duration.
- Exact external-project and lockfile assertion.

### Required Local Validation

Run the complete Rust validation in `AGENTS.md` after Rust changes, plus the
protocol audits when applicable and `git diff --check` in every case.

## Exit Criteria

1. Measurable behavior with exact errors and counts.
2. Required broker and artifact evidence.
3. Documentation, migration, and evidence ledger updates.
4. Exact pushed commit has green CI and no scope-local P0/P1 issue.

## Migration And Rollback

Describe old-to-new behavior, downgrade/fallback constraints, data or identity
that must be preserved, and the smallest safe rollback.

## Conventional Commit Plan

1. `test(scope): ...`
2. `feat(scope): ...` or `fix(scope): ...`
3. `ci(scope): ...`
4. `docs(scope): ...`

## Evidence Record On Completion

Add the fields required by [Execution Rules](execution-rules.md), including an
explicit non-claim.
