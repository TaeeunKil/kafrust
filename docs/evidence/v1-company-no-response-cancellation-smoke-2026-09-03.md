# Company WSL2 no-response cancellation smoke (2026-09-03)

## Environment

- host: `DESKTOP-OTP568E`
- distribution: Ubuntu-T9 under WSL2
- architecture: `x86_64`
- Rust: `1.81.0`
- Docker: `29.5.3` (version inspected only)
- GitHub CLI: `2.45.0`
- source: `040edc5e256c6fd58888ae83eb1df79b36773212`

## Verification

```text
cargo test -p kafrust --lib client::tests::does_not_reuse_connection_after_canceled_no_response_request -- --exact --nocapture
1 passed; 0 failed; 470 filtered out; finished in 0.04s

cargo test -p kafrust --test fault_injection -- --nocapture
29 passed; 0 failed; 0 ignored; finished in 0.61s
```

The injected no-response writer remained pending until cancellation, and the
next request was rejected with `NotConnected`. The complete deterministic fault
matrix passed. No Docker containers, networks, or volumes were created,
modified, or pruned.

## Boundary

This is short company WSL2 deterministic evidence, not published-artifact,
accepted-floor, three-broker, security, long-campaign, service-canary, or
release evidence. Producer-level retry classification and published
reconciliation remain open.
