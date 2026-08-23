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

### Deterministic SBOM slice (2026-08-23)

Commit `1ec37af6ee47ebcf995b9927e008700a8ad584da` finalizes the
platform-neutral form of the SBOM added in `6e24a247c2aa4aa9c63086e68d753988adbfe3aa`.
It includes
[`check_v1_sbom.py`](../../scripts/check_v1_sbom.py), its focused unit tests,
and a CI drift gate. The checker resolves the locked all-feature graph for
the explicit `x86_64-unknown-linux-gnu` platform, follows normal/build edges
from both package roots, excludes dev-only edges, and emits a deterministic
CycloneDX 1.5 document. The checked-in
[`v1-19-sbom.json`](../../evidence/v1-19-sbom.json) contains 89 licensed
components and 89 complete dependency entries; its SHA-256 is recorded in
[`v1-19-sbom.md`](../../evidence/v1-19-sbom.md). The stable CI job verifies the
SBOM after the staged package boundary and requires both `0.3.6` archives.
This closes the reproducible SBOM/drift-inventory slice only; advisory,
yank, optional-TLS native tooling, and reviewed transitive unsafe/native
ownership remain open.

CI permits only transitive version re-resolution caused by platform or Cargo
index state; workspace versions, direct dependency versions, package names,
licenses, source kinds, and graph edges must remain identical.

### Native-tooling slice (2026-08-23)

Commit `83864c1058347dd753608307bdd5ab1d7eb68be3` adds
[`check_v1_native_tooling.py`](../../scripts/check_v1_native_tooling.py),
focused unit tests, a CI gate, and the machine-readable
[`v1-19-native-tooling.json`](../../evidence/v1-19-native-tooling.json).
The checker resolves the same explicit Linux target for all five accepted
feature profiles, records custom-build/link indicators, and runs the default
package with nonexistent C/C++/archiver/pkg-config tools. The default,
`blocking`, and `otlp` trees report no native candidates; `tls` and `all`
record `ring` as the optional TLS custom-build candidate. The no-C default
check passed. This documents the optional-TLS native-tooling non-claim; it
does not close the advisory, yank, or transitive unsafe/native ownership
reviews.

### License-policy slice (2026-08-23)

The dependency closure now has an explicit compatibility gate in
[`check_v1_license_policy.py`](../../scripts/check_v1_license_policy.py). It
follows the same locked all-feature runtime/build closure as the SBOM and
rejects missing expressions or identifiers outside the reviewed permissive
allowlist. The 89-package report is archived in
[`v1-19-license-policy.md`](../../evidence/v1-19-license-policy.md) and the
checker runs in both CI toolchain jobs. This closes license-expression policy
and metadata drift only; advisory/yank review, packaged notice inspection, and
transitive unsafe/native ownership remain open.

### Unsafe/native ownership inventory (2026-08-23)

[`check_v1_unsafe_native_inventory.py`](../../scripts/check_v1_unsafe_native_inventory.py)
now scans the 89-package all-feature closure and records 62 unsafe/build or
platform-boundary entries with owner and rationale fields. Both workspace
crates have zero unsafe constructs under the scanner, while optional TLS and
OS/runtime boundaries are explicitly named. The report is a review queue, not
a completion claim: the dated owner-review matrix in
[`v1-19-unsafe-native-review.md`](../../evidence/v1-19-unsafe-native-review.md)
now covers all 62 entries, classifies 46 upstream-unsafe, 10 build/codegen, and
six native/platform boundaries, and records candidate-only risk disposition.
This closes the owner/review-matrix slice, but it is not a source audit of
every upstream unsafe block or final 1.0.0 risk acceptance.

### Registry provenance slice (2026-08-23)

[`check_v1_registry_provenance.py`](../../scripts/check_v1_registry_provenance.py)
decodes the local crates.io sparse-index cache for the same closure, requiring
checksums and rejecting missing or yanked resolved versions. The 87-package
result is archived in [`v1-19-registry-provenance.md`](../../evidence/v1-19-registry-provenance.md).
It is local index evidence only and does not close live advisory or current
server-side yank review.

### Advisory snapshot slice (2026-08-23)

[`check_v1_advisories.py`](../../scripts/check_v1_advisories.py) queried the
all-feature runtime/build closure through the OSV batch API using the pinned
RustSec advisory export revision `bf5c0d245a92671908518d7e765914d437954ed6`.
All 89 resolved packages returned zero advisory matches, including zero
critical/high matches. The report is archived in
[`v1-19-advisories.md`](../../evidence/v1-19-advisories.md) and its JSON
companion. CI checks the package identities offline (transitive version
re-resolution is covered by the SBOM gate) and expires the snapshot after 30
days, forcing a fresh live review. This is a dated snapshot,
not a claim about future or undisclosed vulnerabilities, and the manual
unsafe/native owner review remains open.

### Combined CI validation (2026-08-23)

The exact pushed commit `af73b8d2eaf19e33d9e90dbc79189f74bd9ad538` passed the
Rust 1.81.0/stable matrix in
[CI run 32610559177](https://github.com/TaeeunKil/kafrust/actions/runs/32610559177),
including all dependency hardening checkers, package isolation, tests, Clippy,
and documentation. This confirms the implemented slices on both toolchains;
the remaining advisory/current-index, manual unsafe ownership, platform/package
matrix, and published-artifact gates remain open.

The follow-up exact source `68f777512f097dfdc44eceafe7d1e9127c67e0ed` passed
the Rust 1.81.0/stable matrix in
[CI run 32611666435](https://github.com/TaeeunKil/kafrust/actions/runs/32611666435),
including the offline advisory snapshot gate and its focused tests. This
confirms the advisory slice on both toolchains; manual unsafe/native ownership,
multi-platform/package evidence, and published-artifact gates remain open.

The owner-review matrix is now enforced by CI with
[`check_v1_unsafe_native_review.py`](../../scripts/check_v1_unsafe_native_review.py)
and its focused tests. This validates coverage and drift against the inventory;
it does not weaken the documented candidate-only disposition.

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
