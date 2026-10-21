# Published `0.5.0` package boundary (2026-10-21)

- source_commit: `88429143a772ab5d9eb23f2f906d17a4ac349c69`
- client: `kafrust 0.5.0`
- protocol: `kafrust-protocol 0.5.0`
- publication order: protocol first, then client
- status: both crates are visible on crates.io; fresh external resolution passed
- security source: `ruzstd 0.9.1`, fixing RUSTSEC-2026-0344
- Rust minimum: 1.87 (the published `0.4.0` pair supports Rust 1.81)
- competitor decision:
  [`v1-23-published-competitor-comparison-2026-10-21.md`](v1-23-published-competitor-comparison-2026-10-21.md)
- candidate gate:
  [`v1-20-candidate-0.5.0-boundary-2026-10-21.md`](v1-20-candidate-0.5.0-boundary-2026-10-21.md)

## Registry publication

The crates.io API reports these exact timestamps and checksums. The uploaded
archives match the package bytes produced from source commit `8842914`.

| Crate | Published at (UTC) | SHA-256 |
| --- | --- | --- |
| `kafrust-protocol 0.5.0` | `2026-10-21T01:24:56.703434Z` | `2ff00c1aa3848541c85e71072a44657bb4fc21f426c21b5ece032f312ce351c8` |
| `kafrust 0.5.0` | `2026-10-21T01:26:38.885984Z` | `8d1c83f2bd8ae1f3ca4ac97859b4a5574dbb954db402a47eccaf37615e36874e` |

## Fresh external resolution

Fresh Cargo projects with exact registry dependencies resolved and built the
published artifacts without workspace paths or patches:

- `kafrust-protocol = "=0.5.0"`: `cargo fetch` and `cargo check --locked`
  passed on Rust 1.87 and stable.
- `kafrust = { version = "=0.5.0", features = ["tls", "blocking", "otlp"] }`:
  all-feature `cargo check --locked` passed on Rust 1.87 and stable.
- The client lockfile resolves both `kafrust 0.5.0` and
  `kafrust-protocol 0.5.0` from the crates.io registry.

## Source and registry verification

- Source CI passed on stable and Rust 1.87 in
  [run 39526241857](https://github.com/TaeeunKil/kafrust/actions/runs/39526241857).
- Windows and macOS compatibility passed in
  [run 39526241795](https://github.com/TaeeunKil/kafrust/actions/runs/39526241795).
- Local staged package boundary profiles default, TLS, blocking, OTLP, and all
  features passed; protocol and client `cargo publish --dry-run` both passed.
- [docs.rs `kafrust 0.5.0`](https://docs.rs/kafrust/0.5.0/kafrust/) and
  [docs.rs `kafrust-protocol 0.5.0`](https://docs.rs/kafrust-protocol/0.5.0/kafrust_protocol/)
  returned HTTP 200.

The published-crate smoke and live Kafka smoke runs will be recorded here
after the post-publication workflows finish.
