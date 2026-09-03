# Company WSL2 transaction cancellation smoke (2026-09-03)

## Environment

- host: `DESKTOP-OTP568E`
- distribution: `Ubuntu-T9` on WSL2
- architecture: `x86_64`
- Rust: `rustc 1.81.0 (eeb90cda1 2024-09-04)`
- source commit: `34bbd443b835cd80056d330b21aa44ddc06ff6e0`
- broker: in-memory scripted fixtures; no Docker resources

## Verification

```text
cargo test -p kafrust --lib producer::tests::cancels_end_transaction_after_transmission_marks_producer_defunct -- --exact --nocapture
1 passed; 0 failed; 471 filtered out; finished in 0.14s

cargo test -p kafrust --test fault_injection -- --nocapture
29 passed; 0 failed; finished in 0.62s
```

The focused test observed the EndTxn v3 frame before canceling the commit
future. The producer became `Defunct`, reported no active transaction, and
rejected a new transaction start. The complete fault-injection target also
passed its transaction, consumer, admin, share, immediate-producer, and
buffered-producer cases.

## Boundary

This is a bounded company WSL2 smoke on scripted fixtures. It is not a
long-campaign, multi-broker, security, published-artifact, service-canary, or
release-authorization result.
