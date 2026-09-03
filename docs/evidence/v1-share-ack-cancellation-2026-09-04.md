# Share acknowledgement cancellation evidence (2026-09-04)

## Scope

This record covers the deterministic V1-10 boundary where a caller drops a
`ShareConsumer::commit` future after a ShareAcknowledge request has been
observed by the scripted broker but before the broker response arrives. The
client must not replay the acknowledgement because the broker-side outcome is
unknown.

Source commit: `2f04e650d55ff172eb30f1a07b8652e15a55d2fd`.

The implementation arms an acknowledgement cancellation guard around the
ShareAcknowledge v1/v2 and ShareFetch-v2-with-renew request futures. If the
future is dropped while armed, every affected pending record is marked
`acknowledgement_outcome_unknown`, the broker's Share session is discarded, and
the broker connection is not returned to the cache. A subsequent commit
returns `ShareAcknowledgementOutcomeUnknown` instead of replaying the request.
Normal response handling disarms the guard before applying the acknowledged or
renewed state transition.

## Windows verification

Focused command:

```text
cargo test -p kafrust --lib share_consumer::tests::cancels_acknowledgement_after_transmission_marks_outcome_unknown -- --exact --nocapture
```

Result: one focused test passed. The broker fixture observed the request frame,
the caller future was dropped, the pending record became unknown, the Share
session and broker client were discarded, and a second commit did not replay.

Required workspace validation at this source commit also passed:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-features` — 481 `kafrust` unit tests, 13
  broker-roundtrip tests, 29 fault-injection tests, 5 public-surface tests,
  285 protocol tests, 5 golden tests, 5 malformed-input tests, and 10
  doctests passed; no failures.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `git diff --check`

## Company WSL2 verification

The same focused test and the complete `fault_injection` integration target
were run from the mounted checkout on company WSL2 distribution `Ubuntu-T9`:

- runtime: `rustc 1.81.0 (eeb90cda1 2024-09-04)`
- focused Share cancellation test: 1 passed
- `fault_injection`: 29 passed, 0 failed
- transport: in-memory duplex/scripted broker fixtures
- Docker: no containers, networks, or volumes created or modified

## Boundary and non-claims

This is local deterministic source evidence for one acknowledgement-cancel
boundary. It does not qualify published artifacts, secure transport,
multi-broker or multi-member behavior, long campaigns, the 10,000-record Share
gate, a service canary, exactly-once processing, or release authorization.
The broker-side acknowledgement remains intentionally unresolved and must be
reconciled through the existing redelivery/no-redelivery contract.
