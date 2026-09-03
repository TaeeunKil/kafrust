# Producer partial-write retry evidence (2026-09-04)

## Scope

This record covers the idempotent producer boundary where a Produce request
write succeeds only for a three-byte prefix and then returns `BrokenPipe`.
The first broker has already completed the API-version handshake; the
producer must discard that connection, refresh metadata, reconnect, and retry
the same record without allocating a new producer sequence.

Source commit: `37c3a44bad1748f6f4a5b3b311db2357617b3b99`.

The deterministic test uses an in-memory duplex stream for the first broker
and a TCP listener for the retry broker. The retry broker validates the
Produce v3 frame and finds the exact record-batch identity bytes
`producer_id=42`, `producer_epoch=3`, `base_sequence=0`. The producer returns
the successful offset, records exactly one retry, and advances its sequence to
one after the acknowledged delivery.

## Verification

Focused command:

```text
cargo test -p kafrust --lib producer::tests::retries_idempotent_producer_after_partial_produce_write_with_same_sequence -- --exact --nocapture
```

Result: one test passed on Windows and one on company WSL2 `Ubuntu-T9` with
Rust 1.81.0. The WSL2 run also passed all 29 `fault_injection` tests. The
fixture used only scripted sockets; no Docker container, network, or volume
was created or modified.

The required Windows workspace validation passed at this source commit:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-features` — 484 `kafrust` unit tests, 13
  broker-roundtrip tests, 29 fault-injection tests, 5 public-surface tests,
  285 protocol tests, 5 golden tests, 5 malformed-input tests, and 10
  doctests passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `git diff --check`

## Boundary and non-claims

This closes the local producer classification and sequence-preservation
boundary for a partial client Produce write. It does not qualify arbitrary
network faults, broker replacement, published artifacts, ten-cycle profiles,
100,000-record reconciliation, security, long campaigns, or release
authorization.
