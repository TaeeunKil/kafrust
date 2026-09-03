# Company WSL partial-response fault smoke (2026-09-03)

## Environment

- workstation: company Windows host, Ubuntu-T9 WSL2
- architecture: x86_64
- Rust: 1.81.0
- source: `83ec120851e5afb71aede3b329814003e4a1d8cf`
- broker: in-memory `ScriptedBroker`; no Docker broker or existing container

## Verification

From the company WSL checkout, the complete scripted fault-injection target
was run against the source above:

```text
cargo test -p kafrust --test fault_injection -- --nocapture
running 22 tests
test result: ok. 22 passed; 0 failed; 0 ignored
finished in 0.57s
```

The run includes immediate and buffered idempotent dropped-response and
partial-response replay tests. The partial-response cases wrote only a prefix
of the response frame before connection close, then verified a byte-identical
Produce replay with the original batch sequence and duplicate-sequence
resolution.

## Boundary

This is a short company-workstation reproduction of deterministic fault tests.
It is not published-artifact, accepted-floor, three-broker, long-campaign,
service-canary, or release evidence. The WSL-mounted Windows checkout retains
pre-existing line-ending-only status noise; no reset, clean, prune, or existing
resource mutation was performed.
