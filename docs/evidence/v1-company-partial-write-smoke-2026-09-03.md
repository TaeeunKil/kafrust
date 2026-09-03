# Company WSL2 partial-write smoke (2026-09-03)

## Environment

- host: `DESKTOP-OTP568E`
- distribution: Ubuntu-T9 under WSL2
- architecture: `x86_64`
- Rust: `1.81.0`
- Docker: `29.5.3` (version inspected only)
- GitHub CLI: `2.45.0`
- source: `abaf82af58e064b4a1c234eae4bf84b7bf24415f`

## Verification

The company checkout ran the low-level partial-write regression and the full
scripted fault matrix:

```text
cargo test -p kafrust --lib client::tests::does_not_reuse_connection_after_partial_request_write -- --exact --nocapture
1 passed; 0 failed; 469 filtered out; finished in 0.04s

cargo test -p kafrust --test fault_injection -- --nocapture
29 passed; 0 failed; 0 ignored; finished in 0.62s
```

The injected writer accepted three request bytes and then returned `BrokenPipe`;
the client rejected reuse with `NotConnected`. All 29 deterministic broker
fault tests also passed. No Docker containers, networks, or volumes were
created, modified, or pruned.

## Boundary

This is a short company WSL2 reproduction of deterministic transport behavior,
not published-artifact, accepted-floor, three-broker, security, long-campaign,
service-canary, or release evidence. Producer retry behavior for partial writes
and published reconciliation remain open.
