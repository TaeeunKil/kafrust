# Buffered idempotent producer cancellation evidence (2026-09-04)

## Scope

Source commit `cb5b82f` adds
`dropping_buffered_idempotent_producer_cancels_in_flight_delivery`. The test
uses an idempotent buffered producer and holds the response after the broker
observes the Produce frame. Dropping the owner aborts the worker and therefore
discards the producer state instead of allowing an uncertain sequence to be
reused.

The accepted delivery returns
`Error::Unsupported("buffered producer delivery canceled")`; the buffered
record gauge reaches zero; and a retained enqueue handle returns
`Error::Unsupported("buffered producer task stopped")` without transmitting a
new record. The scripted broker observed exactly the initialization, Metadata,
capability, and one Produce frame sequence.

## Verification

The focused test passed on Windows and on company WSL2 `Ubuntu-T9` (`x86_64`,
Rust 1.81.0). The required Windows validation also passed: 491 `kafrust` unit
tests, 36 fault-injection tests, 285 protocol tests, five golden tests, five
malformed-input tests, eight `kafrust` doctests, two protocol doctests,
formatting, check, clippy, documentation, and whitespace checks.

The scripted broker gained a test-only observation-count wait primitive so the
test synchronizes on the observed Produce frame rather than an arbitrary
sleep. No Docker or existing external resource is touched.

## Boundary

This closes buffered owner/worker cancellation after Produce transmission and
the no-reuse fence for that discarded producer. It does not claim broker-side
acceptance, reconciliation, published ten-cycle coverage, 100,000-record
qualification, long campaigns, service canary, or release authorization.
