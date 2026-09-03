# Buffered producer owner-drop lifecycle evidence (2026-09-04)

## Scope

This record covers the owning `BufferedProducer` being dropped while its
background worker is still alive. The owner now aborts that worker explicitly,
so the task is not detached. Pending delivery senders are dropped with the
worker and their receivers observe the existing canceled-delivery outcome.
`BufferedProducer::close()` remains the graceful path when accepted records
must be flushed before shutdown.

Source commit: `0e6c2057fe669d1522910294ec55a518ac2fda20`.

## Verification

Focused command:

```text
cargo test -p kafrust --lib producer::tests::dropping_buffered_producer_aborts_worker -- --nocapture
```

Result: one deterministic test passed on the company Windows checkout. The
test starts the worker, waits for its start signal, drops the owning producer,
and observes a guard drop signal within one second. No broker, Docker, or
external service is required.

The required Windows workspace validation passed at the same source commit:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-features` — 489 `kafrust` unit tests, 13
  broker-roundtrip tests, 29 fault-injection tests, 5 public-surface tests,
  285 protocol tests, 5 golden tests, 5 malformed-input tests, and 10
  doctests passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `git diff --check`

## Boundary and non-claims

This closes the owner-drop task-lifecycle boundary only. It does not claim
graceful flushing on drop, cancellation while socket I/O is blocked, broker
acceptance or reconciliation, 100-cycle gauge qualification, published
artifact or secure-transport qualification, long campaigns, service canaries,
or release authorization.
