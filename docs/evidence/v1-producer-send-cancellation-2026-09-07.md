# V1-04 producer send cancellation — 2026-09-07

- source commit: `5c4f1179e906a0f6b08e15ed38c35a54064ba024`
- focused command: `cargo test -p kafrust --test fault_injection producer_send_cancellation_does_not_reuse_in_flight_connection -- --exact --nocapture`
- full target command: `cargo test -p kafrust --test fault_injection`
- environment: Windows workspace, Cargo test profile
- focused result: passed
- full target result: 37 passed, 0 failed

The scripted broker held the first Produce response after observing the frame.
Dropping the in-flight `Producer::send` future did not return that connection to
the idle broker cache: the next send completed through a new connection and
returned offset `7`. The observation records carried distinct connection IDs
for the canceled and subsequent Produce paths, and the client metrics ended
with `in_flight_requests=0`.

This is deterministic producer-level cancellation evidence for the
post-transmission socket-read boundary. It is not published mixed-outcome
reconciliation, live broker qualification, a long campaign, or V1-04
completion.

