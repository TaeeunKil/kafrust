# V1-19 Unsafe/Native Inventory Evidence

- date_utc: 2026-08-23
- scope: all-feature runtime and build dependency closure
- platform: `x86_64-unknown-linux-gnu`
- checker: `scripts/check_v1_unsafe_native_inventory.py`
- report: [`v1-19-unsafe-native-inventory.json`](v1-19-unsafe-native-inventory.json)
- report_sha256: `6be7887b64ce8ca3bfe4f06f09ecdf1d201bca3a38a37b73a9f4b9601e18d840`

The inventory scanned the 89 resolved package sources under `src/` (and
`build.rs` where present) for Rust unsafe constructs, then recorded custom
build targets, Cargo `links` metadata, and explicit platform-boundary names.
It produced 62 review entries, six named platform/native boundaries, and zero
unsafe constructs in either workspace crate. Each entry records an owner class,
purpose/risk rationale, and a deliberately non-terminal review status.

This is a reproducible review queue, not a safety certification. The entries
still require manual source review, advisory/yank review, and release-policy
acceptance before V1-19 can be marked done. Optional TLS `ring` tooling remains
covered by the separate native-tooling report.
