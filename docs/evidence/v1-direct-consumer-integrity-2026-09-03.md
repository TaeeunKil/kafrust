# Direct consumer integrity boundaries (2026-09-03)

## Scope

Source commit `df43749cfd277509c8173ac5f68beb8bced866bd` adds two deterministic
regressions for V1-07:

- `ConsumerRecord` preserves `Some(empty)` separately from `None` for record
  keys, values, and header values, including the timestamp sentinel.
- A Fetch v12 request is observed by a scripted broker and its response is
  withheld. Canceling the consumer future leaves no Fetch session cached; the
  next fetch opens a new connection and returns the record without reusing the
  canceled session.

## Windows verification

The required workspace validation passed after the change:

```text
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
480 passed; 0 failed (unit, integration, and doc tests)
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The focused consumer suite also passed (`50 passed; 0 failed`).

## Company WSL2 reproduction

The focused tests and the complete fault-injection target passed from the
company Windows host `DESKTOP-OTP568E`, Ubuntu-T9 WSL2 (`x86_64`, Rust 1.81.0):

```text
preserves_null_and_empty_record_fields
1 passed; 0 failed; 476 filtered out; finished in 0.02s

cancels_fetch_after_transmission_and_reconnects_without_reusing_session
1 passed; 0 failed; 476 filtered out; finished in 0.11s

cargo test -p kafrust --test fault_injection -- --nocapture
29 passed; 0 failed; finished in 0.62s
```

The run used in-memory scripted TCP fixtures. No Docker resources or external
Kafka broker were created or modified.

## Boundary

This evidence covers stable record null/empty mapping and direct Fetch
post-transmission cancellation/reconnect behavior only. It does not claim
retention or unclean-election recovery, published-artifact behavior,
multi-broker qualification, security compatibility, 100,000-record
reconciliation, long campaigns, service canaries, or release authorization.
