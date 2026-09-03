# Canceled no-response request (2026-09-03)

## Scope

At source commit `d9f1309f4e5ce8dab3afd182d087e19eb304893f`, the no-response
client path used by `acks=0` requests is canceled while its write is blocked.
The client retains the in-flight marker, rejects reuse with `NotConnected`, and
does not allow a later operation to share the uncertain connection.

## Verification

```text
cargo test -p kafrust --lib client::tests::does_not_reuse_connection_after_canceled_no_response_request -- --exact --nocapture
1 passed; 0 failed; 470 filtered out; finished in 0.01s

cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
474 passed (kafrust unit tests); 29 passed (fault_injection); all workspace
integration tests and doctests passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

An injected writer stayed pending, so cancellation occurred inside
`send_request_no_response` before completion. A second no-response request was
rejected before any write. No Docker resources were created or modified.

## Boundary

This closes the no-response low-level cancellation/reuse slice. It does not
qualify producer-level partial-write retry policy, published reconciliation,
long campaigns, multi-broker security profiles, service canary qualification,
or release authorization.
