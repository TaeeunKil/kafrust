# `0.4.0` candidate package boundary

- date_utc: 2026-09-09
- source_commit: `9b76780`
- candidate: `kafrust 0.4.0` and `kafrust-protocol 0.4.0`
- current_published_baseline: `0.3.6`
- status: candidate staged; registry publication pending
- competitor_decision: [`v1-23-published-competitor-comparison-2026-09-09.md`](v1-23-published-competitor-comparison-2026-09-09.md)

## Package bytes

The protocol package was staged first. The client package was assembled with a
temporary local patch for the matching protocol source, then compiled in fresh
external fixtures without a workspace path dependency.

- `kafrust-protocol-0.4.0.crate` SHA-256:
  `d17616f17fff9128f7b83acd60727fc979e386f195baef5e539b1678537eed66`
- `kafrust-0.4.0.crate` SHA-256:
  `81d37581ae48fc4b618473f4a4f7ad35f36e11f247352cbcc178dcb7a8b5fef9`
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
