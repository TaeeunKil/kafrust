# Company WSL full scripted fault-matrix smoke (2026-09-03)

## Environment

- workstation: company Windows host, Ubuntu-T9 WSL2
- architecture: x86_64
- Rust: 1.81.0
- Docker: 29.5.3 (no Docker resources created or modified)
- source: `ed71f6d3d1ac50aa0f27e3a89d3a626238a452bb`
- broker: in-memory `ScriptedBroker`

## Verification

From the company WSL checkout, the complete deterministic fault-injection
target was run with two test threads:

```text
cargo test -p kafrust --test fault_injection -- --test-threads=2
running 26 tests
test result: ok. 26 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
finished in 1.83s
```

The run includes Admin, consumer, group, Share, transaction, immediate
producer, buffered producer, dropped and partial response replay, terminal
idempotent sequence errors, queue expiry, close flush, and post-write
in-flight delivery deadline cases. The buffered in-flight case observed a
Produce request with no response and verified the typed ambiguous deadline,
zero buffered gauge, and clean worker shutdown.

## Boundary

This is a short current-source company-workstation reproduction using an
in-memory scripted broker. It is not published-artifact, accepted-floor,
three-broker, security, long-campaign, service-canary, or release evidence.
The WSL-mounted Windows checkout retains pre-existing line-ending-only status
noise; no reset, clean, prune, or existing resource mutation was performed.
