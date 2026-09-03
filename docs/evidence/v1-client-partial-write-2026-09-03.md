# Partial client request write (2026-09-03)

## Scope

At source commit `c42826f36f8af9b2d368d4035a05dfb8eb189ab8`, a scripted
`AsyncWrite` accepts only the first three bytes of an encoded ApiVersions frame
and then returns `BrokenPipe`. The low-level client reports the transport
failure, records that transmission may have started, poisons the connection,
and rejects reuse with `NotConnected`.

## Verification

```text
cargo test -p kafrust --lib client::tests::does_not_reuse_connection_after_partial_request_write -- --exact --nocapture
1 passed; 0 failed; 469 filtered out; finished in 0.00s

cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
473 passed (kafrust unit tests); 29 passed (fault_injection); all workspace
integration tests and doctests passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The injected writer returned three bytes and then a `BrokenPipe` error. The
first request failed without reading a response; a second ApiVersions call was
rejected before any write. No Docker resources were created or modified.

## Boundary

This closes low-level connection reuse after a partial client request write. It
does not claim producer retry safety for partial writes, published
mixed-outcome reconciliation, long campaigns, multi-broker security profiles,
service canary qualification, or release authorization.
