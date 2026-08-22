# V1-23 Migration Adapter And Rollback

- Status: Planned
- Target evidence: Service canary
- Dependencies: V1-04-V1-17

## User-Visible Objective

Let a representative Rust service replace rust-rdkafka with kafrust through a
small adapter/configuration boundary, compare behavior, exercise faults and
credential rotation, and roll back without rewriting business logic or losing
track of ambiguous operations.

## Non-Goals

- No promise of source-compatible rust-rdkafka drop-in replacement.
- No permanent public compatibility facade unless V1-02/V1-24 approve it.
- No unsafe dual-write that can create untracked business duplicates.
- No claim of canary completion without a named representative service or
  reproducible in-repository reference service.

## Scope

- `docs/migration-from-rust-rdkafka.md` and all API direction/operation guides
- new reference service/canary fixture outside the stable library API unless
  a reviewed adapter is necessary
- mapping for producer, buffered delivery, direct consumer, classic/KIP-848
  groups, transactions, Admin, security, metrics, callbacks, errors, blocking,
  timeouts, defaults, and feature flags
- dual-client comparator using unique business IDs, shadow reads or isolated
  topics, result normalization, fault injection, deploy/observe/rollback scripts
- credential rotation, leader/coordinator loss, restart/rebalance, backpressure,
  unknown-outcome reconciliation, and rollback runbook

## Work Packages

1. Complete a row-by-row migration inventory: mapped, intentional difference,
   unsupported, or blocker; no blank common configuration/callback entries.
2. Define a narrow application-owned trait/boundary used by both clients without
   hiding Kafka concepts.
3. Build a reference service that runs the same deterministic workload through
   both implementations on isolated topics/groups and compares normalized
   records, offsets, errors, and metrics.
4. Add staged traffic selection and rollback controls; retain business IDs and
   reconcile any unknown mutations/transactions before retry.
5. Exercise deploy, credential rotation, leader/coordinator loss, rebalance,
   load/backpressure, restart, and rollback.
6. Run the harness against a named representative service environment. If none
   is available, mark the external-canary gate blocked rather than substituting
   an example.

## Preparation Record (2026-08-22)

V1-23 remains `Planned`. The repository contains the row-by-row
[`rust-rdkafka` migration guide](../../migration-from-rust-rdkafka.md) and
typed operation/error mappings, but no named representative service, owner, or
canary environment has been registered in this v1 program. The existing
examples are not a substitute for the required million-record dual-client
comparison and forward/rollback canary. The preparation manifest at
[`v1-23-migration-canary-manifest.json`](../../evidence/v1-23-migration-canary-manifest.json),
its checker, and the manual `Migration Reference Canary` workflow now provide a
reproducible 1,000-record smoke path with isolated topics plus a declared
1,000,000-record exit path. V1-23 will be explicitly marked `Blocked` if a
named service or approved reference environment is still absent when its
dependency window opens.

The baseline smoke path passed on current source commit `7a686b0` in
[Migration Reference Canary run 32552631034](https://github.com/TaeeunKil/kafrust/actions/runs/32552631034):
Kafka 4.3.1 KRaft, isolated topics, 1,000 records per implementation, and
normalized kafrust/rust-rdkafka results with zero observed loss or divergence.
Two earlier attempts are retained as diagnostic history: one exposed the
Rust 1.81/edition2024 toolchain mismatch and the next exposed the runner's
missing `libcurl` headers; the workflow now pins the external fixture to stable
and installs that comparison-only build dependency. This is still only the
baseline smoke rung; no named service, fault/cutover/rollback, or million-record
exit criterion is closed.

The same isolated smoke was rerun from source commit `6bcf1ef` in
[Migration Reference Canary run 32555867981](https://github.com/TaeeunKil/kafrust/actions/runs/32555867981).
Both implementations processed 1,000 unique 1-KiB records with batch size 100
and zero normalized divergence: kafrust produced in 0.034558s and consumed in
0.018552s; rust-rdkafka produced in 0.020658s and consumed in 0.006937s. The
run refreshes source evidence only; it is not the named-service, fault,
forward-cutover, rollback, or million-record exit gate.

The reconciliation harness was then strengthened on source commit `ec0f86b`
to embed an 8-byte business ID in every measured value and compare a SHA-256
digest plus unique/loss/duplicate counts across both implementations. The
exact-HEAD rerun [Migration Reference Canary run 32557407734](https://github.com/TaeeunKil/kafrust/actions/runs/32557407734)
passed with 1,000 unique records per implementation, zero loss, zero
duplicates, and matching digest
`98fb6a3dfe9a9ac1765160a42e05b2c63e0ed231af678f370de194c0f5044e26`.
This is stronger baseline reconciliation evidence, but it still does not
close the named-service, fault, forward-cutover, rollback, or million-record
exit gate.

## Failure And Lifecycle Contract

- Adapter errors preserve typed Kafka/broker/unknown-outcome information.
- Shadow comparison does not commit or mutate production state twice unless the
  workload is explicitly idempotent and isolated.
- Traffic cutover waits for producer deliveries, transaction resolution, group
  leave/offset checkpoint, and background task shutdown as applicable.
- Rollback preserves record IDs, committed offsets, transactional ID strategy,
  credentials, and duplicate-risk log.
- Divergence stops promotion and retains both result sets.

## Verification

- Compile tests and examples cover every mapped public family and configuration
  row.
- Reference service processes at least 1,000,000 unique records across producer,
  group, transaction/read-committed, and common Admin bootstrap flows with zero
  unexplained record/offset/outcome divergence.
- Canary performs credential rotation, leader/coordinator loss, rebalance,
  backpressure, restart, forward cutover, and rollback; each stage has explicit
  health/abort criteria.
- Rollback completes within the service's recorded objective, not a universal
  hard-coded promise, and subsequent processing resumes from reconciled state.

## Exit Criteria

1. 100% of migration inventory rows are mapped/different/unsupported/blocker.
2. The reference million-record comparator has zero unexplained divergence.
3. Every named fault and credential event passes forward and rollback paths.
4. A named service canary passes its forward, fault, observation, and rollback
   gates.
5. Migration, rollback, incompatibility, and evidence records are complete.

If no named service/environment is available, set V1-23 to `Blocked`; V1-24
and V1-25 cannot close, and the absence is not an alternate exit criterion.

## Migration And Rollback

This entire milestone is the migration/rollback contract. Preserve the old
client dependency and deploy path through the accepted RC observation period.
Do not remove rollback until V1-26 post-publish verification succeeds.

## Conventional Commit Plan

1. `test(migration): add dual-client reference service`
2. `feat(migration): add application canary boundary`
3. `ci(canary): exercise cutover faults and rollback`
4. `docs(migration): complete rust-rdkafka mapping`

## Evidence Record On Completion

Record service/fixture version, client artifacts, workload/record IDs, mapping
coverage, fault/cutover/rollback stages and objectives, divergence/reconcile
counts, security/topology, and source-compatibility/non-production non-claims.
