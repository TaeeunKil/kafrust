# Release Competitive/Version Readiness — 2026-08-23

## Candidate under review

- coordinated candidate: `kafrust 0.3.6` / `kafrust-protocol 0.3.6`
- current published line: `0.3.5`
- decision: publish `0.3.6` as a pre-1.0 package-boundary release; do not call it `1.0.0`
- reference audit: [`docs/competitor-source-audit-2026-08-20.md`](../competitor-source-audit-2026-08-20.md)

## Where kafrust is stronger

- pure-Rust implementation boundary with no librdkafka/rdkafka-sys dependency;
  the default feature builds with unusable C/C++ tooling;
- broad typed protocol/Admin surface, including classic and current broker
  paths, with separate protocol and client crates;
- Rust-native default record codecs and an explicit optional-TLS native-tool
  posture rather than hiding a required C client.

## Where kafrust is weaker or not yet proven

- the `0.3.6` pair has not yet been verified as a fresh published artifact by
  the complete V1-20 matrix;
- krafka/kacrab currently provide stronger competitor-facing evidence in
  generated protocol/oracle tests, fake/real broker fault infrastructure, and
  broader published operational coverage;
- kafrust still lacks the long fault/soak/SLO campaigns, service-canary
  migration/rollback evidence, and API freeze required for the v1 program.

## Version decision

`0.3.6` is the appropriate next pre-1.0 identity for the package-boundary and
coordinated-protocol repair; its remaining published gates are now the next
qualification step.
It is not a reason to skip directly to `1.0.0`. A later `0.3.z` release should
be cut only for another coherent, verified user-visible slice; a change to the
support contract or a breaking public API requires a re-planned pre-1.0 minor
line. Any material competitor gap or failed qualification result must update
the milestone graph and roadmap before the next version is chosen.

This record is a planning decision, not publication authorization or evidence
that the candidate is ready for crates.io.

## Post-V1-19 decision update (2026-08-23)

The V1-19 packaged-candidate gate is now complete: CI passed the exact source
with the SBOM, native-tooling, license, registry/yank, OSV/RustSec advisory,
unsafe/native owner-review, staged package, Rust 1.81, and stable checks. The
publication decision remains unchanged. `0.3.6` is a coherent pre-1.0 identity,
but its fresh published compatibility matrix is still unrun and V1-21 through
V1-24 still own fault/soak, SLO, migration/rollback, and API-freeze evidence.
Those are material `1.0.0` gates, but they do not block this bounded pre-1.0
package-boundary release. The roadmap now moves to V1-20 with the ordered
protocol-first publication decision below.

## Pre-1.0 publication decision (2026-08-23)

- decision_source: `c0bb72895297ea0e85a8b3e254ca725ed7a7dff9`
- qualification: [CI run 32612740002](https://github.com/TaeeunKil/kafrust/actions/runs/32612740002)
- authorization: protocol-first `0.3.6` upload, then client upload only after
  registry visibility and a fresh client dry-run pass
- rationale: V1-19 packaged-candidate criteria, advisory snapshot, native/unsafe
  owner matrix, competitor comparison, and both-toolchain CI are green; the
  coordinated package-boundary repair is a coherent pre-1.0 user-visible slice
- explicit deferrals: V1-20 published matrix, V1-21/V1-22 fault/SLO campaigns,
  V1-23 canary/rollback, V1-24 API freeze, and all `1.0.0` claims

This is an autonomous release-gate decision, not a request for separate user
confirmation. It authorizes only the ordered `0.3.6` pre-1.0 path; any package,
registry, external-project, competitor, or validation discrepancy stops the
sequence and triggers a new milestone/version decision.

The ordered publication completed successfully. The exact registry checksums,
timestamps, and fresh external lockfile evidence are recorded in
[`v1-20-published-0.3.6-boundary-2026-08-23.md`](v1-20-published-0.3.6-boundary-2026-08-23.md).
This advances V1-20 to its published-artifact matrix; it does not make a stable
release or change the `1.0.0` deferral.
