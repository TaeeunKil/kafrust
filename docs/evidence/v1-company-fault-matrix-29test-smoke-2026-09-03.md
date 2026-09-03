# Company WSL latest scripted fault-matrix smoke (2026-09-03)

## Environment

- workstation: company Windows host, Ubuntu-T9 WSL2
- architecture: x86_64
- Rust: 1.81.0
- Docker: 29.5.3 (no Docker resources created or modified)
- gh: 2.45.0
- source: `c1dc20943dd9ae7e7f9971a665c4ca15dfd3b8cc`
- broker: in-memory `ScriptedBroker`

## Verification

From the company WSL checkout, the complete deterministic fault-injection
target was run with two test threads:

```text
cargo test -p kafrust --test fault_injection -- --test-threads=2
running 29 tests
test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
finished in 1.92s
```

The target covers Admin, consumer, group, Share, transaction, immediate and
buffered producer paths, dropped and partial response replay, terminal
idempotent sequence errors, queue expiry, close flush, handle-owned flush,
post-write delivery deadlines, and delivery-receiver cancellation.

## Boundary

This is a short current-source company-workstation reproduction using an
in-memory scripted broker. It is not published-artifact, accepted-floor,
three-broker, security, long-campaign, service-canary, or release evidence.
The WSL-mounted Windows checkout retains pre-existing line-ending-only status
noise; no reset, clean, prune, or existing resource mutation was performed.
