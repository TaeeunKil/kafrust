# Company WSL buffered terminal-error smoke (2026-09-03)

## Environment

- workstation: company Windows host, Ubuntu-T9 WSL2
- architecture: x86_64
- Rust: 1.81.0
- source: `a46462f1f51c257b18d85c8fd265e00d1b63f8a3`
- broker: in-memory `ScriptedBroker`; no Docker broker or existing container

## Verification

The buffered fatal-sequence regression was rerun from the company WSL
checkout:

```text
cargo test -p kafrust --test fault_injection \
  buffered_idempotent_producer_fatal_sequence_errors_are_terminal \
  -- --nocapture
running 1 test
test result: ok. 1 passed; 0 failed; 22 filtered out
finished in 0.33s
```

The test covers codes 45, 47, and 90. Each first delivery returns its fatal
identity error; a second buffered delivery emits no Produce request and returns
the same error. The worker closes cleanly.

## Boundary

This is a short company-workstation reproduction of deterministic terminal
behavior. It is not published-artifact, accepted-floor, three-broker,
long-campaign, service-canary, or release evidence. Existing WSL-mounted
checkout line-ending-only status noise was preserved.
