# Buffered delivery phase expiry evidence (2026-09-04)

## Scope

Source commit `09f6731` adds deterministic coverage for the buffered producer
when its total delivery budget expires before a Produce request can start:

- `buffered_delivery_deadline_expires_during_metadata_without_produce` holds
  the Metadata response.
- `buffered_delivery_deadline_expires_during_capability_without_produce` holds
  the ApiVersions response after Metadata succeeds.

Both cases use a 20 ms delivery budget and a 10 second linger. The flush result
and the delivery handle both carry the matching typed
`DeliveryDeadlineExceeded` phase with `possibly_transmitted=false`. The
buffered-record gauge returns to zero and the scripted broker observes no
Produce frame.

## Verification

Windows required validation passed with 491 `kafrust` unit tests, 35
fault-injection tests, 285 protocol tests, five golden tests, five malformed
input tests, eight `kafrust` doctests, and two protocol doctests, plus check,
clippy, documentation, formatting, and whitespace checks.

The two focused tests also passed on company WSL2 `Ubuntu-T9` with Linux
`x86_64` and Rust 1.81.0. The test uses only an in-process scripted broker and
does not create or mutate Docker resources.

## Boundary

This closes the buffered pre-Produce Metadata and capability classification
slice. It does not claim post-Produce ambiguity, cancellation during socket
I/O, published mixed-outcome reconciliation, long campaigns, service canary,
or release authorization.
