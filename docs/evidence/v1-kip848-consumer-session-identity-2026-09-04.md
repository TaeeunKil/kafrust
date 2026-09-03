# KIP-848 consumer session identity evidence (2026-09-04)

## Scope

This record covers the private KIP-848 heartbeat-handle session fence used to
distinguish the current consumer instance from a replaced instance. The
deterministic test checks the same `Arc` identity as `Current`, a different
session or a missing session as `StaleGeneration`, and a different group as
`DifferentGroup`; the heartbeat task is then stopped and joined.

Source commit: `3cc672a063cc0b3f2d4d3c0119fa17284190fd3f`.

The test uses a scripted Tokio task only. It does not create a broker, Docker
resource, network listener, or long-running campaign.

## Verification

Focused command:

```text
cargo test -p kafrust consumer_heartbeat_handle_matches_session_identity
```

Result: one test passed on Windows and one on company WSL2 `Ubuntu-T9` with
Rust 1.81.0. The WSL2 run also passed all 29 `fault_injection` tests.

The required Windows workspace validation passed at this source commit:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-features` — 485 `kafrust` unit tests, 13
  broker-roundtrip tests, 29 fault-injection tests, 5 public-surface tests,
  285 protocol tests, 5 golden tests, 5 malformed-input tests, and 10
  doctests passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `git diff --check`

## Boundary and non-claims

This closes the deterministic session-identity helper boundary only. It does
not qualify stale-task cancellation under broker churn, delete/recreate
name-fallback races, multi-member ownership, exact offset restoration,
published artifacts, secure transport, long campaigns, service canaries, or
release authorization.
