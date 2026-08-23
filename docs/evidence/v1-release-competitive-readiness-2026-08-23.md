# Release Competitive/Version Readiness — 2026-08-23

## Candidate under review

- coordinated candidate: `kafrust 0.3.6` / `kafrust-protocol 0.3.6`
- current published line: `0.3.5`
- decision: do not call the candidate `1.0.0`; do not publish yet
- reference audit: [`docs/competitor-source-audit-2026-08-20.md`](../competitor-source-audit-2026-08-20.md)

## Where kafrust is stronger

- pure-Rust implementation boundary with no librdkafka/rdkafka-sys dependency;
  the default feature builds with unusable C/C++ tooling;
- broad typed protocol/Admin surface, including classic and current broker
  paths, with separate protocol and client crates;
- Rust-native default record codecs and an explicit optional-TLS native-tool
  posture rather than hiding a required C client.

## Where kafrust is weaker or not yet proven

- the `0.3.6` pair is still a packaged candidate, not a fresh published
  artifact verified by the complete V1-20 matrix;
- krafka/kacrab currently provide stronger competitor-facing evidence in
  generated protocol/oracle tests, fake/real broker fault infrastructure, and
  broader published operational coverage;
- kafrust still lacks the completed advisory/current-index review, manual
  transitive unsafe/native acceptance, long fault/soak/SLO campaigns, and
  service-canary migration/rollback evidence required for the v1 program.

## Version decision

`0.3.6` is the appropriate next pre-1.0 identity for the package-boundary and
coordinated-protocol repair, once its remaining package/published gates pass.
It is not a reason to skip directly to `1.0.0`. A later `0.3.z` release should
be cut only for another coherent, verified user-visible slice; a change to the
support contract or a breaking public API requires a re-planned pre-1.0 minor
line. Any material competitor gap or failed qualification result must update
the milestone graph and roadmap before the next version is chosen.

This record is a planning decision, not publication authorization or evidence
that the candidate is ready for crates.io.
