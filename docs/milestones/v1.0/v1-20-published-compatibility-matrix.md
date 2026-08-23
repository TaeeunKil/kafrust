# V1-20 Published Compatibility Matrix

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-03-V1-19

## User-Visible Objective

Pass the entire accepted broker/security/topology/workload contract from one
exact published pre-1.0 artifact pair, with fresh lockfiles and no workspace
path dependency.

## Non-Goals

- No full Cartesian product beyond V1-01's accepted pairwise matrix.
- No combining old runs from different commits into one candidate result.
- No managed-provider, ZooKeeper, Redpanda, or Confluent claim unless V1-01
  explicitly includes and qualifies it.
- No production SLO claim; V1-21/V1-22 own fault duration and performance.

## Scope

- the exact support contract and evidence ledger from V1-01
- all stable surfaces selected by V1-02 and qualified by V1-03-V1-19
- `.github/workflows/live-*.yml`, `published-*.yml`, external fixture projects,
  Docker/Kafka configuration, release/package scripts, and artifact retention
- current-source, packaged-candidate, and published-artifact rows kept separate
- stable and Rust 1.81 toolchains; default, `tls`, `blocking`, `otlp`, and
  all-feature package profiles

Minimum matrix unless V1-01 changes it:

- plaintext core profile on each accepted exact broker line;
- accepted-floor classic group and pinned-current KIP-848/Share profiles;
- accepted-floor and pinned-current TLS, mTLS, PLAIN, SCRAM-256/512, and signed
  OAUTHBEARER rows where supported;
- single-node and three-broker leader/coordinator/controller rows;
- producer, idempotent, transaction/read-committed, direct consumer, classic/
  modern group, stable Share, common Admin, telemetry if stable, and blocking if
  stable;
- all four pure-Rust codecs.

## Work Packages

1. Generate the workflow matrix from the accepted support data rather than
   duplicating version defaults across dozens of files.
2. Pin Kafka images/digests and fixture versions; record topology/security
   configuration with every job.
3. Before publication, validate every generated matrix row and external fixture:
   schema/checker, workflow YAML, formatting, candidate-source compilation, and
   the feature/toolchain combination it claims to execute.
4. After explicit authorization, publish the protocol candidate first. Wait
   until a fresh external project can fetch and build that exact registry
   version on Rust 1.81 and stable; then package and run `cargo publish
   --dry-run` for the client against the registry protocol before publishing
   the client.
5. Create fresh external projects per feature/workload family, assert exact
   lockfile versions, and forbid `[patch]`/path sources.
6. Run current-source and published matrices separately on the same source
   commit; retain results and reconcile differences.
7. Update compatibility claims only after all required rows pass.

## Current Execution Record (2026-08-23)

V1-20 is now `In progress`. The first machine-readable matrix draft is
[`v1-20-compatibility-matrix.json`](../../evidence/v1-20-compatibility-matrix.json)
and is checked by `scripts/check_v1_compatibility_matrix.py` in CI. It preserves
the V1-01 broker order (`3.7.2`, `3.8.1`, `3.9.1`, `4.0.0`, `4.3.1`), names
single-node and three-broker/controller-listener profiles, separates security
and feature profiles, and requires protocol-first exact registry resolution
without path or patch dependencies.

The checker validates twelve non-Cartesian draft profiles and the mandatory floor,
pinned secured-failover, and package-codec rows. At draft creation it did not
claim any published row. The exact `0.3.6` registry pair is now attached to
the named published rows in the qualification ledger: fresh external
lockfiles, the 10-profile continuity/codec smoke, the explicit PLAIN and
SCRAM-SHA-512 profiles, Share/Streams/group/Admin/transaction/security/failover
slices, and the exact toolchain checks all have retained results. The matrix
remains draft because mechanism-specific source coverage and the later fault/SLO,
migration, API-freeze, and stable-release gates are separate dependencies.
The source-only 17-job Live Kafka Smoke matrix passed on commit
`e6de5c5` in [run 32551145773](https://github.com/TaeeunKil/kafrust/actions/runs/32551145773),
including the SCRAM transaction failover assertion for either Kafka fencing code
47 or 90. This is current-source evidence only; publication remains separately
authorization-gated.

The same 17-job source-only matrix was rerun from source commit `4f6918b` in
[run 32555053351](https://github.com/TaeeunKil/kafrust/actions/runs/32555053351)
after the bounded soak/diagnostic corrections. All 17 jobs passed across Kafka
3.7.2, 3.8.1, 3.9.1, and 4.3.1, including plaintext/TLS/SASL profiles,
signed OAUTHBEARER, SCRAM failover, KIP-848 failover, ACL authorization,
transaction response-loss reconciliation, and three-broker leader/coordinator
failover. This refreshes current-source evidence only; it does not satisfy the
exact published `0.3.6` lockfile pair or V1-20 exit criteria.

The ordered `0.3.6` publication has now completed. The protocol and client
checksums, registry timestamps, exact external lockfile, and Rust 1.81/stable
package checks are archived in
[`v1-20-published-0.3.6-boundary-2026-08-23.md`](../../evidence/v1-20-published-0.3.6-boundary-2026-08-23.md).
This closes the published-package boundary prerequisite; the retained published
rows now cover the named broker/security/workload slices, while the full
accepted matrix and later-milestone rows remain open.

The first published smoke slice then passed all seven profiles in
[run 32613844625](https://github.com/TaeeunKil/kafrust/actions/runs/32613844625),
followed by published three-broker classic failover on Kafka 3.7.2 in
[run 32613851826](https://github.com/TaeeunKil/kafrust/actions/runs/32613851826)
and KIP-848 failover on Kafka 4.3.1 in
[run 32613855210](https://github.com/TaeeunKil/kafrust/actions/runs/32613855210).
The retained details are in
[`v1-20-published-smoke-2026-08-23.md`](../../evidence/v1-20-published-smoke-2026-08-23.md).
These are published-artifact smoke slices, not completion of the accepted
matrix or downstream fault/SLO/API-freeze gates.

The expanded published evidence is retained in
[`v1-20-published-smoke-2026-08-23.md`](../../evidence/v1-20-published-smoke-2026-08-23.md),
including the 10-profile broker/codec matrix, the repeated Share/failover
rows, and the twelve-profile security/continuity rerun in
[32616834901](https://github.com/TaeeunKil/kafrust/actions/runs/32616834901).
Passing named rows do not authorize a stable release by themselves.

The exact published pair was rerun after the documentation baseline refresh in
[32626214201](https://github.com/TaeeunKil/kafrust/actions/runs/32626214201).
All twelve `published-crate-smoke` profiles passed again from external Cargo
projects: Kafka 3.7.2/3.8.1/3.9.1/4.0.0/4.3.1 plaintext and KIP-848 rows,
SCRAM-256/SCRAM-512/PLAIN security rows, and gzip/snappy/lz4/zstd codec rows.
The detailed row list and qualification boundary are in
[`v1-20-published-smoke-rerun-2026-08-23.md`](../../evidence/v1-20-published-smoke-rerun-2026-08-23.md).
This refresh strengthens the named published rows only; it does not close the
full V1-20 matrix or downstream fault, SLO, migration, and release gates.
The workflow now uploads each profile's external `Cargo.lock` and captured
fixture output for 90 days, so future reruns retain machine-auditable package
and result inputs instead of relying on console logs alone.

The first clean run after that change is
[32626589535](https://github.com/TaeeunKil/kafrust/actions/runs/32626589535):
all twelve profiles passed and the downloaded run contained 12 lockfiles plus
12 captured outputs. The intermediate run 32626452478 is retained only as a
fixture-readiness failure diagnostic; its single Kafka 3.9.1 failure is not
promoted into the passing matrix evidence. The clean artifact details are in
[`v1-20-published-smoke-artifact-rerun-2026-08-23.md`](../../evidence/v1-20-published-smoke-artifact-rerun-2026-08-23.md).
The external smoke fixture now also waits up to ten seconds for created topic
and active group metadata to become visible, preserving a real timeout failure
while avoiding a false negative from normal KRaft metadata propagation.

## Failure And Lifecycle Contract

- One required row failure blocks the matrix; it is not averaged away.
- Infrastructure failure is rerun and recorded separately from a product
  failure; the original run remains immutable.
- A matrix retry uses the same artifact and configuration unless a superseding
  evidence row documents the change.
- Any source/package change creates a new candidate and invalidates aggregate
  completion until required rows rerun.
- Published artifacts are immutable and may be yanked, never overwritten.
- If protocol publication succeeds but client dry-run/publication fails, stop
  the release and record the partial publication. Never reuse that protocol
  version for changed bytes; prepare a new coordinated version pair (and yank
  or annotate the abandoned protocol-only candidate as policy requires) before
  retrying.

## Verification

- Matrix schema/checker proves every accepted V1-01 row has one current-source
  and one exact published-artifact result.
- Every generated row/fixture passes schema, YAML, formatting, and
  candidate-source compilation checks before either crate is published.
- External lockfiles resolve the intended client and protocol versions from the
  registry, on Rust 1.81 and stable where required. Registry visibility is
  proved by those fresh fetch/builds, not by `cargo info` (unavailable on the
  pinned Cargo 1.81 toolchain).
- Per-job results assert negotiated API versions, final resource gauges, record/
  ownership/offset outcomes, and secret scans appropriate to the workload.
- Docs.rs is checked for both exact packages, but HTTP 200 alone is not a
  runtime compatibility result.

## Exit Criteria

1. Every accepted support row passes from one source commit and exact published
   artifact pair.
2. No required fixture uses a workspace/path/patch dependency.
3. All generated fixtures validate before publication, and all
   feature/toolchain packages resolve and compile externally after the ordered
   protocol-first handoff.
4. Every row has retained logs/results and an explicit non-claim.
5. Compatibility, README, roadmap, release docs, and ledger are generated or
   reconciled from the same matrix data.

## Migration And Rollback

If a required row fails, keep the last supported artifact/profile documented and
do not broaden the claim. Roll back workflow generation independently from
runtime code. A broken published candidate may be yanked after recording its
affected profiles; use a new version for correction.

## Conventional Commit Plan

1. `ci(matrix): generate v1 compatibility profiles`
2. `test(package): verify external artifact lockfiles`
3. `docs(compat): record exact candidate matrix`
4. `chore(release): publish authorized matrix candidate`

## Evidence Record On Completion

Record the matrix manifest hash, source/artifact versions, every broker image,
topology/security/workload/toolchain/feature row, run/result artifact, final
gauges, and excluded profile list.
