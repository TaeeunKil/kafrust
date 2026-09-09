# `0.4.0` candidate package boundary

- date_utc: 2026-09-09
- source_commit: `628d130`
- candidate: `kafrust 0.4.0` and `kafrust-protocol 0.4.0`
- current_published_baseline: `0.3.6`
- status: candidate staged; registry publication pending
- competitor_decision: [`v1-23-published-competitor-comparison-2026-09-09.md`](v1-23-published-competitor-comparison-2026-09-09.md)

## Package bytes

The protocol package was staged first. The client package was assembled with a
temporary local patch for the matching protocol source, then compiled in fresh
external fixtures without a workspace path dependency.

- `kafrust-protocol-0.4.0.crate` SHA-256:
  `5c5f76a73b2d73a3a7644e8d91e78fdb9ae0434a071ac273410ddf2d8b0f11a`
- `kafrust-0.4.0.crate` SHA-256:
  `af7d10ff3917ee021932a075dc95dcb9f1c69bfc07992d5733119b314d7d439c`
- external feature fixtures: default, `tls`, `blocking`, `otlp`, and all
- package boundary command: `python scripts/verify_package_boundary.py --staged`
- result: all five profiles passed with coordinated `0.4.0` lockfile entries

## Bounded long-run evidence

The accepted workstation-sized `0.4` gate is retained in
[`v1-local-bounded-followup-2026-09-08.md`](v1-local-bounded-followup-2026-09-08.md):

- secured six-hour phase: 2,160,150 attempted, acknowledged, and unique
  consumed records; loss, duplicates, and unknown outcomes all zero;
- plaintext twelve-hour phase: 1,080,150 attempted, acknowledged, and unique
  consumed records; loss, duplicates, and unknown outcomes all zero;
- both phases reported `recovered=true`, zero final in-flight/buffered gauges,
  and no disk guard breach;
- plaintext helper RSS ranged from 7,184,384 to 7,634,944 bytes, helper OS
  threads from 18 to 19, sockets from 1 to 8, and free-space samples stayed
  above the 100 GiB reserve threshold.

These are bounded local diagnostics. They do not qualify the V1-21 high-load
fault campaign, V1-22 performance SLO, a service canary, or `1.0.0`.

## Verification

The candidate passed:

- `cargo fmt --all`
- `cargo check --workspace --all-targets --all-features`
- `cargo test --workspace --all-features` (530 client unit tests, 285 protocol
  unit tests, focused integration suites, and doc tests)
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `python scripts/check_v1_dependency_graph.py`
- `python scripts/check_published_workflow_versions.py` (36 defaults remain on
  published `0.3.6`)
- `python scripts/check_qualification_ledger.py`
- `git diff --check`

Publication must remain protocol-first. After `kafrust-protocol 0.4.0` is
visible on crates.io, run a fresh client dry-run against that registry artifact,
then publish `kafrust 0.4.0`. Update `published-baseline.json`, workflow
defaults, checksums, and docs.rs links only after both uploads and fresh
external resolution pass.

## Upload attempt

The first protocol-first `cargo publish -p kafrust-protocol --locked` attempt
reached crates.io but returned HTTP `403 authentication failed`. No `0.4.0`
version is visible in the registry, the client upload was not attempted, and
the published `0.3.6` baseline remains unchanged. After local Cargo registry
credentials are repaired with `cargo login`, rerun the same protocol-first
sequence from the clean release worktree.
