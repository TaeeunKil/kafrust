# V1-26 v1.0 Release

- Status: Planned
- Target evidence: Service canary
- Dependencies: V1-25

## User-Visible Objective

Publish `kafrust 1.0.0` from the accepted release candidate behavior, verify the
actual registry artifacts and tagged source, complete post-publish canary and
matrix checks, and state the stable support contract without overstating it.

## Non-Goals

- No feature or behavior change between the accepted RC and stable release.
- No universal rust-rdkafka drop-in, managed-service, Kafka broker, or Kafka
  Streams engine claim.
- No “production-ready everywhere” label.
- No deletion of RC/failure evidence after success.

## Scope

- final coordinated package versions and protocol-first publication order from
  V1-24/V1-25
- manifest/version/release-note changes only relative to accepted RC behavior;
  generated external lockfiles are evidence, while the ignored root
  `Cargo.lock` remains uncommitted
- local/package/API/MSRV/dependency checks, crates.io, docs.rs, annotated tag,
  GitHub release, checksums and the V1-19 SBOM, external projects, critical smoke,
  full matrix scheduling, service canary and rollback readiness
- README, roadmap, compatibility, API stability, migration, release, evidence
  ledger, and post-1.0 support/security process

## Work Packages

1. Diff stable source against the accepted RC; allow only reviewed release
   identity/metadata changes. Any behavior fix returns to a new RC.
2. Run complete local, audit, package, API/semver/MSRV, dependency, and security
   validation on the release SHA. Rerun every accepted V1-19 OS/target x
   feature x toolchain row against the actual stable tarballs and regenerate
   the required SBOMs from those artifacts.
3. Publish protocol first, wait for registry resolution, verify client package
   against it in fresh Rust 1.81/stable external projects, run client `cargo
   publish --dry-run`, then publish client.
4. Verify exact crates.io metadata, docs.rs pages, package checksums/contents,
   fresh external feature projects, and critical floor/current workloads.
5. Create the annotated `v1.0.0` tag and GitHub release only for the verified
   commit; include supported profiles, migrations, known limits, and evidence.
6. Run the named post-publish canary and full matrix; keep rollback artifact and
   runbook available until acceptance.
7. Activate patch/security/advisory and broker-matrix refresh policy.

## Preparation Record (2026-08-22)

V1-26 remains `Planned`. The repository still has no `1.0.0` tag, GitHub
release, or stable registry publication. The only authorized future path is a
metadata-only change from an accepted RC, protocol-first publication, fresh
Rust 1.81/stable external resolution, docs.rs and critical smoke verification,
then the named post-publish canary and rollback. Earlier `0.3.5` evidence is
retained as historical context and cannot close this release gate.

The stable execution input is now machine-checked by
[`v1-26-release-manifest.json`](../../evidence/v1-26-release-manifest.json)
and `scripts/check_v1_release_manifest.py`. It limits the RC-to-stable diff to
release metadata, requires protocol-first publication and artifact verification
before tag/release, and keeps the post-publish canary/rollback gate explicit.
The preparation checker does not publish, tag, or mark the release complete.

## Failure And Lifecycle Contract

- Publication is irreversible; an invalid artifact is yanked/advised and
  replaced by a new patch version, never overwritten.
- If protocol publication succeeds and the client gate fails, stop, record the
  partial release, and use a new coordinated version for changed artifacts;
  never republish bytes under the protocol version already consumed.
- Docs.rs delay is tracked separately; build failure blocks final release
  completion even if crates.io upload succeeded.
- A post-publish critical regression triggers canary rollback, advisory/yank
  assessment, and a new patch/RC path.
- Unknown Kafka operation outcomes remain reconciled before application replay
  during rollout/rollback.
- Stable semver begins at publication; breaking fixes require the documented
  major-version process.

## Verification

- Stable package diff from accepted RC is limited to approved release metadata.
- Every required validation and package isolation gate passes on release SHA.
- Every V1-19 OS/target x feature x toolchain row passes from the actual stable
  tarballs, and the required SBOMs match their package/dependency digests.
- Fresh external Rust 1.81/stable projects resolve exact final versions,
  including `kafrust 1.0.0` and the protocol version selected by V1-24, for all
  accepted feature profiles with no path/patch dependency.
- Critical published floor/current core/security/group/transaction/Share rows
  pass immediately; the complete V1-20 matrix and one post-publish service
  canary complete on the exact artifacts.
- Docs.rs, crates.io, tag, GitHub release, checksums, SBOMs, and evidence links
  all match the same commit/artifacts.

## Exit Criteria

1. Both final artifacts are published in the required order and externally
   compile/run on MSRV and stable; the client is `kafrust 1.0.0` and the
   protocol artifact uses V1-24's accepted semver policy.
2. Exact docs.rs pages, annotated tag, GitHub release, package contents/hashes,
   required SBOMs, full V1-19 tarball matrix, and source SHA agree.
3. Critical smoke, complete matrix, and post-publish service canary pass; the
   rollback rehearsal remains valid.
4. No open release-blocking P0/P1 issue remains and known limits are public.
5. Roadmap M21/V1 program is marked done only after all evidence rows land.

## Migration And Rollback

Follow the V1-23 guide and release notes. Keep the previous client artifact and
configuration deployable until post-publish acceptance. If stable is broken,
rollback the service, reconcile Kafka state, communicate affected profiles,
evaluate yank/advisory, and publish a corrected patch through the same gates.

## Conventional Commit Plan

1. `chore(release): prepare 1.0.0`
2. `docs(release): publish v1 support and migration record`
3. Post-publish evidence uses `docs(evidence): record v1 qualification` without
   changing release behavior.

## Evidence Record On Completion

Record stable source/tag, package and documentation hashes/URLs/timestamps,
registry resolution, external project/toolchain/features, critical/full matrix,
canary/rollback result, issue count, support-policy activation, and every
explicit non-claim.
