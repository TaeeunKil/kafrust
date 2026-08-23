# V1-25 Release Candidate

- Status: Planned
- Target evidence: Service canary
- Dependencies: V1-24

## User-Visible Objective

Publish and qualify `kafrust 1.0.0-rc.1` from the frozen API with the complete
accepted matrix, 24-hour fault/resource evidence, fresh external projects, and
a successful representative service cutover and rollback.

## Non-Goals

- No stable `1.0.0` publication.
- No feature work during RC qualification.
- No reusing earlier pre-freeze artifact evidence as RC evidence.
- No waiving a required row because the fix appears small.

## Scope

- coordinated RC versions for `kafrust` and `kafrust-protocol` according to the
  V1-24 semver decision; the client exact-pins `=1.0.0-rc.N`, and protocol is
  packaged/published first
- release notes, package contents/hashes, crates.io, docs.rs, GitHub prerelease,
  external fixture projects, accepted compatibility matrix, fuzz/advisory/
  dependency gates, fault/soak/performance/SLO gates, migration service canary
  and rollback
- exact release commit/tag and immutable evidence ledger rows

## Work Packages

1. Cut a release branch/commit under the repository's accepted protection and
   approval policy; change only manifests/version identity and release metadata.
   Keep the ignored root `Cargo.lock` out of the commit; retain generated
   external lockfiles as evidence.
2. Freeze a campaign manifest before publication: RC version, release and
   workflow SHAs/content hashes, Kafka image digests, fixture revisions,
   target/shard durations, job timeouts, and artifact retention. Upgrade the
   current 30-second fuzz and approximately 35-minute soak workflows so their
   configured capacity can deliver the required 60-minute/24-hour gates.
3. Run full local validation, audits, package isolation, API/semver/MSRV, and
   dependency/security gates, including every OS/target x feature x toolchain
   row accepted by V1-19 against the actual RC tarballs.
4. Publish the protocol RC, wait until fresh external Rust 1.81 and stable
   projects fetch/build that exact registry artifact, package and run client
   `cargo publish --dry-run` against it, then publish the client RC.
5. Verify exact docs.rs pages and fresh external feature/workload projects.
6. Rerun V1-20's complete matrix and V1-21/V1-22 gates, including at least one
   24-hour secured multi-broker campaign and 60-minute-per-target RC fuzz budget.
7. Run the named service canary forward and rollback, then forward again if the
   service owner approves.
8. Observe at least two consecutive scheduled campaign sets with no open P0/P1
   issue. Any source fix produces `rc.2` or later and restarts required gates.
9. Complete the dated competitor comparison and version-readiness decision
   required by the execution rules. A gap that changes the support contract or
   release identity triggers a milestone/roadmap replan before RC publication.

## Preparation Record (2026-08-22)

V1-25 remains `Planned`. No `1.0.0-rc.1` manifests, tag, registry upload, or
GitHub prerelease exists. The publication sequence and explicit-authorization
boundary are recorded in [`docs/release.md`](../../release.md); an RC cannot
start until V1-24 freezes the API and the complete V1-20 through V1-23 evidence
manifest is accepted.

The execution input is now machine-checked by
[`v1-25-release-candidate-manifest.json`](../../evidence/v1-25-release-candidate-manifest.json)
and `scripts/check_v1_rc_manifest.py`. It locks the coordinated RC identity,
exact protocol prerelease pin, protocol-first sequence, 24-hour/60-minute
campaign requirements, and explicit publication-authorization boundary. A
passing checker is preparation evidence only; it does not create or publish an
RC.

## Failure And Lifecycle Contract

- A failed package, matrix, fuzz, soak, SLO, security, or canary gate blocks RC
  acceptance.
- Infrastructure reruns retain original results and use the same artifact.
- Any source change creates a new RC identity and invalidates affected evidence.
- A severe published-RC issue may cause a yank/advisory, never overwrite.
- A protocol-only partial publication is recorded and abandoned or yanked; any
  changed retry uses a new coordinated RC number for both crates.
- Rollback preserves old client availability and reconciles unknown state.

## Verification

- Every `AGENTS.md` command and plan-specific CI gate passes on the release SHA.
- Registry lockfiles contain exact RC packages with no path/patch dependency.
- The frozen campaign manifest proves workflows, timeouts, and shards can run
  the declared fuzz/soak durations on the immutable RC.
- Every V1-19 OS/target x feature x toolchain row passes from the actual RC
  tarballs, not merely the workspace or staged source.
- All accepted compatibility rows pass from the RC.
- One 24-hour pinned-current SASL_SSL/SCRAM-SHA-256 three-broker fault soak
  meets V1-21/V1-22 loss/duplicate/RSS/retry/final-resource thresholds.
- Every fuzz target receives at least 60 RC minutes with no unresolved finding.
- Named service canary and rollback pass; two consecutive scheduled sets pass.

## Exit Criteria

1. Exact RC packages, docs.rs, prerelease tag/release, and external projects pass.
2. Complete matrix, the full V1-19 tarball matrix, and all quantitative
   fault/SLO/resource/security gates pass under the frozen campaign manifest.
3. Service canary cutover and rollback pass on the exact RC.
4. Two consecutive scheduled campaign sets pass with no source change.
5. No open P0/P1 issue remains; release owner accepts the RC evidence manifest.
6. The competitor comparison and version-readiness decision support the exact
   RC identity, or the revised intermediate-release plan is archived.

## Migration And Rollback

Publish RC migration notes and keep the previous production client/version
deployable. Canary rollback follows V1-23. If `rc.1` is superseded, document the
exact issue/fix and require users to move to the later RC rather than editing
the old artifact.

## Conventional Commit Plan

1. `chore(release): prepare 1.0.0-rc.1`
2. `docs(release): publish rc migration and evidence manifest`
3. Additional fixes, if needed, use scoped `fix`/`test` commits followed by a
   new `chore(release): prepare 1.0.0-rc.N` commit.

## Evidence Record On Completion

Record release/tag SHA, package/doc hashes and registry timestamps, every matrix/
fuzz/soak/SLO/security/canary run, issue severity count, scheduled-set count,
rollback result, and stable-release/non-universal-support non-claims.
