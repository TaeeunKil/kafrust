# Metrics saturating arithmetic evidence (2026-09-04)

## Scope

`ClientMetrics` now updates cumulative counters and current gauges with atomic
compare-exchange loops and saturating arithmetic. A counter at `u64::MAX`
stays at that value instead of wrapping, and a cleanup decrement at zero stays
zero instead of underflowing. Peak fields retain their existing atomic maximum
semantics.

Source commit: `c526c412460af17cacd3816f6c04709b34ca31f9`.

## Verification

Focused command:

```text
cargo test -p kafrust --lib metrics::tests::metric_atomic_updates_saturate_at_u64_boundaries -- --nocapture
```

Result: one focused test passed on the company Windows checkout. The test
exercises counter overflow and gauge underflow boundaries directly.

The source also passed the following checks at the same commit:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `cargo test --workspace --all-features --lib --tests` — 490 client unit
  tests, 13 broker-roundtrip tests, 29 fault-injection tests, 5 public-surface
  tests, 285 protocol tests, 5 golden tests, and 5 malformed-input tests passed.

The exact required all-features workspace command also ran but could not link
the example target because the company MSVC installation was missing
`msvcrt.lib` (`LNK1104`). This is an environment/toolchain limitation; the
exact pushed CI matrix is the authoritative all-target validation.

## Boundary and non-claims

This closes deterministic metric overflow/underflow handling only. It does not
claim concurrent workload completeness, published telemetry collection,
secured multi-broker or long-duration qualification, absence of telemetry
backend failures, service-canary readiness, or release authorization.
