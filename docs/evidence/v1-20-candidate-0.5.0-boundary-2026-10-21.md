# Candidate `0.5.0` security package boundary (2026-10-21)

- client: `kafrust 0.5.0`
- protocol: `kafrust-protocol 0.5.0`
- publication order: protocol first, then client
- registry baseline until publication verification: `0.4.0`
- security source: `ruzstd 0.9.1`, fixing RUSTSEC-2026-0344
- compiler minimum: Rust 1.87 (up from 1.81 in published `0.4.0`)
- initial working-tree protocol archive SHA-256 (before the release commit):
  `f0ad0a50411a9509722ac1700ea55ce79b9180c5b5aff737c6bb5c147a046d48`
- initial working-tree client archive SHA-256 (before the release commit):
  `db6ca95c2445e51449e44c51c02b4b1a78913ad4bea80c0964392c5762f997a1`
- candidate decision: prepare a coordinated pre-1.0 minor release; do not make
  a production readiness or complete compatibility claim
- competitor review:
  [`v1-23-published-competitor-comparison-2026-10-21.md`](v1-23-published-competitor-comparison-2026-10-21.md)

## Package boundary

The protocol crate contains the Zstandard decompression implementation, so
publishing only the client crate would leave downstream protocol users without
the fix. Both crates must use the coordinated `0.5.0` version, and the client
must depend on `kafrust-protocol 0.5.0`. The client package cannot be published
until the protocol package is visible and resolves from crates.io without a
workspace path override.

## Required release evidence

- Workspace CI and platform compatibility on the exact release source commit.
- `cargo publish -p kafrust-protocol --dry-run --locked` succeeds.
- Publish the protocol crate, then resolve and build it from a fresh external
  project on Rust 1.87 and stable.
- `cargo publish -p kafrust --dry-run --locked` succeeds against the visible
  protocol package; then publish the client crate.
- Record registry timestamps and checksums for both archives.
- Resolve and build `kafrust 0.5.0` in a fresh external project on Rust 1.87
  and stable, run the published-crate smoke workflow, and verify docs.rs.
- Only after those checks, update the baseline manifest and published workflow
  defaults from `0.4.0` to `0.5.0`.

This candidate proceeded to publication. Its final registry evidence and
source identity are recorded in
[`v1-20-published-0.5.0-boundary-2026-10-21.md`](v1-20-published-0.5.0-boundary-2026-10-21.md)
after the ordered uploads and external resolution succeeded.
