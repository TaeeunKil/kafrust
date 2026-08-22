# V1-00 Repository And Package Baseline

- Status: Planned
- Target evidence: Packaged candidate
- Dependencies: none

## User-Visible Objective

Produce uniquely versioned `kafrust-protocol` and `kafrust` packages that
compile together outside the workspace, so every later milestone starts from
a publishable and reproducible artifact boundary.

The planned coordinated version is `0.3.6`. If release ownership chooses a
different unused version, update this plan, both manifests, the client
dependency, generated external-fixture lockfiles, and all affected workflow
defaults together before implementation. The root `Cargo.lock` remains ignored
and is not a release input unless repository policy is changed explicitly.

## Non-Goals

- No new Kafka behavior or public API.
- No crates.io publication without separate explicit release authorization.
- No retroactive tag for `0.3.4` or `0.3.5` unless the exact historical source
  provenance is verified; documentation repair is not permission to invent a
  release artifact.
- No broad roadmap archive rewrite.

## Scope

Source and automation:

- `crates/kafrust-protocol/Cargo.toml`
- `crates/kafrust/Cargo.toml`
- generated package/external-fixture lockfiles retained as evidence, not the
  ignored root `Cargo.lock`
- `.github/workflows/ci.yml`
- Git tag/GitHub release provenance and the `main` protection/release policy
- `docs/release.md`, `docs/roadmap.md`, and `docs/compatibility.md`
- published-workflow version defaults only when a release is actually prepared

Package relationship:

- build `kafrust-protocol-0.3.6.crate` first;
- make the client depend on exactly the matching compatible protocol version;
- verify the staged client against the staged protocol through an isolated
  local registry or equivalent package-only fixture;
- forbid a workspace path override in the final verification project.

## Work Packages

1. Add a regression job that reproduces the current packaged-client failure
   against published protocol `0.3.5`.
2. Bump both packages and the client dependency to the unused coordinated
   version.
3. Package, inspect, unpack, build, test, and document the protocol tarball.
4. Resolve that package from an isolated registry fixture, then package and
   verify the client with default, `tls`, `blocking`, `otlp`, and all features.
5. Replace CI's client-only `--no-verify` gate with a two-package compilation
   gate; `--no-verify` may remain only as an additional package-content check.
6. Reconcile the fact that crates.io is at `0.3.5` while GitHub releases stop at
   `v0.3.3`; add no retroactive release/tag until the exact published source
   provenance is proved.
7. Decide and record whether release commits require a protected `main`/PR gate
   or the documented solo trunk exception, then enforce the chosen exact-SHA
   checks.
8. Record exact package hashes, contents, source commit, and CI run.

## Failure And Lifecycle Contract

- A registry dependency resolution failure is a release-blocking error, not a
  reason to fall back silently to the workspace path.
- A version already present on crates.io is immutable and cannot represent new
  source contents.
- Protocol publication must precede client publication, but this milestone
  stops at a packaged candidate unless publication is separately authorized.
- Failed package verification leaves no compatibility or release claim.

## Verification

Deterministic gates:

- the old `0.3.5` package fixture fails for the four missing transaction type
  families recorded in `baseline.md`;
- the new isolated fixture resolves no dependency from the workspace;
- `cargo tree` and the external lockfile contain the coordinated versions;
- staged package builds pass on Rust 1.81 and stable for every feature profile;
- both protocol audit scripts and their checker tests pass.

Run the complete repository validation from `AGENTS.md`, then run the new
package CI job on the exact pushed commit.

## Exit Criteria

1. Both manifests and the client protocol dependency use one unused
   coordinated version; generated external lockfiles resolve that pair, while
   the ignored root `Cargo.lock` is not committed.
2. Both `.crate` files are generated from the same reviewed commit and their
   contents contain no unintended workspace files or secrets.
3. A fresh package-only project compiles the client against the matching
   protocol package for five feature profiles and on both supported Rust
   toolchains.
4. CI no longer treats client `--no-verify` packaging as sufficient.
5. GitHub release/tag provenance through `0.3.5` is audited without inventing a
   historical tag, and the future release-commit protection policy is explicit.
6. Exact pushed-commit CI is green and the evidence row records package hashes.

## Migration And Rollback

This changes package identity, not Kafka behavior. Roll back by reverting the
coordinated version/CI commit before any publication. After publication, never
reuse or overwrite the version; publish a subsequent correction and follow the
release yanking/advisory policy if necessary.

## Conventional Commit Plan

1. `test(package): reproduce registry protocol mismatch`
2. `build(release): advance coordinated crate versions`
3. `ci(package): verify staged protocol and client crates`
4. `docs(release): record package baseline evidence`

## Evidence Record On Completion

Record source SHA, selected versions, package SHA-256 hashes, file lists,
toolchains, feature profiles, isolated-registry method, and explicit statement
that this is not published-artifact evidence.
