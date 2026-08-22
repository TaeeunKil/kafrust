# V1-19 Pure-Rust Dependency Audit

- Status: In progress
- Target evidence: Packaged candidate
- Dependencies: V1-02

## User-Visible Objective

Make the no-librdkafka/no-required-C-toolchain promise reproducible per feature,
platform, and packaged artifact, with documented licenses, advisories, unsafe
boundaries, and MSRV behavior.

## Non-Goals

- No claim that every transitive dependency contains no unsafe code.
- No claim that optional TLS currently needs no native build tooling.
- No dependency rewrite when the existing dependency is justified and supported.
- No security guarantee based only on a package-name scan.

## Scope

- workspace manifests, generated external-fixture lockfiles, features, build
  scripts, and packaged contents; the ignored root `Cargo.lock` is not a
  release input unless policy changes
- default, `tls`, `blocking`, `otlp`, and all-feature dependency graphs
- Rust 1.81 MSRV and stable on supported operating systems/targets
- `#![forbid(unsafe_code)]` in kafrust crates and transitive unsafe inventory
- librdkafka/C binding/native build-tool detection, codec backend confirmation,
  licenses, duplicate versions, advisories, abandoned/yanked dependencies, and
  secret-bearing dependency behavior
- CI, release docs, README requirements, compatibility/support contract

## Work Packages

1. Generate feature-specific `cargo tree`/metadata and license/advisory reports
   from both workspace and staged packages.
2. Add a denylist for librdkafka, rdkafka-sys, Kafka C bindings, and unintended
   native codec backends.
3. Build the default package in an environment with no usable C/C++ compiler;
   record feature-specific native tooling for TLS/ring rather than hiding it.
4. Build every supported feature/platform/toolchain combination and inspect
   build scripts/link artifacts.
5. Review transitive unsafe/native/crypto code and record owner, purpose,
   version, update policy, and risk acceptance.
6. Generate a reproducible SPDX or CycloneDX SBOM from each final package pair,
   verify it against packaged dependency metadata, and add release gates for
   SBOM completeness, licenses, advisories, MSRV, package content, and
   dependency drift.

## Current Execution Record (2026-08-22)

V1-19 is now `In progress`. The coordinated `0.3.6` package candidate already
passes the staged package-boundary verifier for default, `tls`, `blocking`,
`otlp`, and all-feature external projects on Rust 1.81.0/stable in
[CI run 32545563612](https://github.com/TaeeunKil/kafrust/actions/runs/32545563612).
The workspace manifests contain no `librdkafka`, `rdkafka-sys`, or Kafka C
binding, both source crates inherit `unsafe_code = "forbid"`, and the default
codec dependency posture is Rust-native. These facts support the package
candidate boundary only; they are not a blanket transitive-unsafe claim.

The remaining audit work is feature-specific `cargo tree`/metadata capture from
staged tarballs, native-tool detection for optional TLS, license/advisory/yank
review, reviewed transitive unsafe/native ownership, reproducible package-pair
SBOM generation, and drift gates. Optional TLS/ring tooling remains an explicit
non-claim until that matrix is archived. No crates.io publication is implied by
the current packaged-candidate evidence.

The exact-HEAD package refresh on source `e772451` passed both Rust 1.81.0 and
stable in [CI run 32559004319](https://github.com/TaeeunKil/kafrust/actions/runs/32559004319).
The staged archives were `kafrust-protocol-0.3.6.crate`
(`ee191756dddae5b5d591c935416a0d06720f5ea90a7bdab8233734b0bb893768`) and
`kafrust-0.3.6.crate`
(`00f656d820b11df0d06d56c9bd6869810f28f7c14242d838a4b1bfed6c675325`); default,
`tls`, `blocking`, `otlp`, and all-feature external projects passed with no
workspace path resolution. This refreshes packaged-candidate evidence only;
the dependency/SBOM/advisory audit and published matrix remain open.

### Direct dependency graph slice (2026-08-22)

The reproducible local graph record is
[`v1-19-dependency-audit.md`](../../evidence/v1-19-dependency-audit.md).
Normal-edge trees for default, `tls`, `blocking`, `otlp`, and all-feature
profiles contained 56, 65, 56, 72, and 81 unique packages respectively, with
no `librdkafka`, `rdkafka-sys`, `kafka-sys`, or `rdkafka` package. The metadata
surface reports 18 direct client dependencies and four protocol dependencies.
The same check is wired into the main CI workflow. This is a deterministic
dependency-name check; it does not close native
TLS tooling, license/advisory/yank, transitive unsafe review, SBOM, or drift
gates.

The exact-head checker now also runs full-graph Cargo metadata (locked when
the ignored root lockfile is present) and rejects any resolved package without
`license` or `license_file` metadata. The local audit on commit
`ab87cd7ee2356195aa494ddfc9c114662611c3e2` found 71
resolved packages and zero missing license metadata; the details are in
[`v1-19-license-metadata-audit.md`](../../evidence/v1-19-license-metadata-audit.md).
This closes metadata completeness only, not license compatibility or the
remaining advisory, yank, native, unsafe, SBOM, and drift reviews.

The checker hardening for ignored-lockfile handling and Cargo diagnostic
separation passed the complete Rust 1.81.0/stable CI matrix in
[run 32561532044](https://github.com/TaeeunKil/kafrust/actions/runs/32561532044)
from source `f499ee62b5f8a066d8a1d764ad8ce7b8006094cd`; this is CI evidence for
the metadata-completeness slice, not publication or full V1-19 completion.

## Failure And Lifecycle Contract

- A required default-build C compiler/link to librdkafka or a C Kafka client is
  a release blocker.
- Optional-feature native requirements must be explicit before feature
  activation and cannot weaken the default promise.
- Critical/high applicable advisories block release unless a dated, reviewed
  exception names impact and mitigation.
- Dependency audit failure is not suppressed to make CI green.
- Package verification uses actual tarballs, not only workspace metadata.

## Verification

- Default staged packages build/test/docs on Rust 1.81 and stable in the
  no-C-toolchain environment.
- All accepted feature/platform combinations build with their documented tools.
- Dependency denylist finds zero librdkafka/C-client/native codec violations.
- License/advisory/yanked reports are archived; applicable unmitigated
  critical/high count is zero.
- Source crates retain `forbid(unsafe_code)` and transitive unsafe/native review
  has 100% owner/rationale coverage.
- Package-pair SBOM generation is reproducible, covers every packaged runtime
  dependency, and records the generator/version and artifact digest.

## Exit Criteria

1. Default packaged build demonstrably needs no C toolchain or librdkafka.
2. Every feature has an exact dependency/native-tool/platform support entry.
3. All accepted toolchain/platform builds pass from staged packages.
4. Applicable unmitigated critical/high advisories and forbidden dependencies
   are zero.
5. CI enforces dependency and SBOM drift, and README/release/support docs state
   the precise promise.

## Migration And Rollback

Dependency/provider changes preserve wire and public behavior or carry explicit
migration notes. Roll back to a previous dependency only if advisories and
compatibility permit it; never restore a forbidden C client binding.

## Conventional Commit Plan

1. `ci(deps): audit feature-specific package dependencies`
2. `build(deps): remove forbidden native requirements`
3. `docs(deps): define pure-Rust and tooling posture`
4. `ci(msrv): verify staged package feature matrix`

## Evidence Record On Completion

Record package hashes, feature/platform/toolchain matrix, no-C environment,
dependency graph/report/SBOM hashes and generator, forbidden/advisory/license
totals, reviewed unsafe/native entries, and optional-TLS non-claim.
