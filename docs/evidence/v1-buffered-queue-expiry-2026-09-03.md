# Buffered queue expiry before Produce (2026-09-03)

## Scope

At source commit `fb2778b370e466af01f08d60e6d0ec26053cc1e5`, the real
`BufferedProducer` worker accepts a record with a 20 ms total delivery budget
and a 10 second linger. The record expires in the bounded queue before linger
flush, returns `DeliveryDeadlineExceeded` with `phase=Queue` and
`possibly_transmitted=false`, and releases its buffered-record gauge.

## Verification

```text
cargo test -p kafrust --test fault_injection \
  buffered_delivery_deadline_expires_before_produce_without_transmission \
  -- --nocapture
1 passed; 0 failed

cargo check --workspace --all-targets
cargo test --workspace --all-features --quiet
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The scripted broker was configured with no response steps, and its observation
list remained empty: the expired record caused zero Produce requests. The
buffered worker then closed cleanly.

## Boundary

This closes the real buffered queue-expiry/no-transmission slice. It does not
qualify delayed metadata/capability or post-write deadlines, cancellation and
shutdown ambiguity, published mixed-outcome profiles, long campaigns, service
canary qualification, or release authorization.
