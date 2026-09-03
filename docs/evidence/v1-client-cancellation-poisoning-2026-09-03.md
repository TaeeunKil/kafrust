# Canceled in-flight request poisoning (2026-09-03)

## Scope

At source commit `2c28eec77911420e8dbb2d5d94bb96400f0148b9`, the low-level
`Client` marks a request as in flight before socket I/O begins. If the caller
cancels that future while the request is blocked, the connection remains
unusable and cannot be returned to a later request. A subsequent request now
returns the typed `NotConnected` I/O error instead of consuming bytes from an
uncertain response.

## Verification

```text
cargo test -p kafrust --lib client::tests::does_not_reuse_connection_after_canceled_request -- --exact --nocapture
1 passed; 0 failed; 468 filtered out; finished in 0.00s

cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
472 passed (kafrust unit tests); 29 passed (fault_injection); all workspace
integration tests and doctests passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The duplex scripted broker observed the first ApiVersions request, delayed its
response, and the test canceled the client future after transmission began.
The client reported itself unusable and rejected the next ApiVersions call
with `Error::Io(NotConnected)`. No Docker resources were created or modified.

## Boundary

This closes low-level connection-reuse safety after caller cancellation. It
does not qualify partial client request writes, producer delivery receiver
cancellation, published mixed-outcome reconciliation, long campaigns,
multi-broker security profiles, service canary qualification, or release
authorization.
