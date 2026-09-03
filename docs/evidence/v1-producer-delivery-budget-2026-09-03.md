# V1-04 Producer Delivery Budget — 2026-09-03

## Scope

`buffered_remaining_delivery_timeout` now accepts an explicit Tokio clock
anchor. The buffered worker still supplies `Instant::now()` in production,
while tests can hold the enqueue timeline fixed and inspect the exact
remaining total delivery budget.

The focused producer tests cover:

- an older and a newer buffered request, selecting the older request's 80 ms
  remainder from a 100 ms budget;
- an already expired request, returning a zero remaining budget; and
- an empty pending batch, retaining the configured 100 ms budget.

## Verification

`cargo test -p kafrust --lib producer::tests -- --nocapture` passed all 116
producer unit tests on 2026-09-03, including the three clock-anchor cases.
The required workspace check, test, clippy, documentation, and `git diff
--check` validation also passed for source commit `b838fa3`.

## Boundary

This is a deterministic budget-calculation increment. It does not close
clock-controlled coverage for every immediate, batch, or buffered entry point,
or the published mixed success/expiry reconciliation profiles. The long SLO,
broker, and release gates remain open.
