# Idempotent send cancellation safety evidence (2026-09-04)

## Scope

This record covers an idempotent immediate or batch send canceled after the
Produce request has been observed by the scripted broker but before its
response arrives. The producer retains an in-flight outcome marker while the
request is awaited. If the future is dropped, the marker remains and every
later idempotent operation returns `Error::IdempotentProducerDefunct` before
transmitting a new sequence. A completed request clears the marker so normal
transport retries still preserve the original sequence.

Source commit: `d0e033fb2cc6bfe67f3302a29d01f4f1f9a45c0c`.

## Verification

Focused command:

```text
cargo test -p kafrust --lib producer::tests::cancels_idempotent -- --nocapture
```

Result: the immediate and batch cancellation tests passed on Windows and on
company WSL2 `Ubuntu-T9` with Rust 1.81.0. The WSL2 run also passed all 29
`fault_injection` tests. The scripted broker observed one Produce v3 frame in
each test and delayed its response; no second frame was allowed after the
future was canceled.

The required Windows workspace validation passed at this source commit:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-features` — 488 `kafrust` unit tests, 13
  broker-roundtrip tests, 29 fault-injection tests, 5 public-surface tests,
  285 protocol tests, 5 golden tests, 5 malformed-input tests, and 10
  doctests passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `git diff --check`

## Boundary and non-claims

This closes the deterministic immediate and batch cancellation fence only. It
does not claim broker acceptance, duplicate-free reconciliation, buffered
worker cancellation, published artifacts, secure transport, long campaigns,
service canaries, or release authorization. A canceled idempotent producer
must be discarded; the broker-side outcome still requires an application-level
reconciliation policy.
