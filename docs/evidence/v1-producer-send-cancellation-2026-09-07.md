# V1-04 producer send cancellation — 2026-09-07

- source commits: `5c4f1179e906a0f6b08e15ed38c35a54064ba024` (immediate); `94610cbcd2fb44ece52b60fb932a69ed0390cf86` (batch)
- focused command: `cargo test -p kafrust --test fault_injection producer_send_cancellation_does_not_reuse_in_flight_connection -- --exact --nocapture`
- batch command: `cargo test -p kafrust --test fault_injection producer_batch_cancellation_does_not_reuse_in_flight_connection -- --exact --nocapture`
- full target command: `cargo test -p kafrust --test fault_injection`
- environment: Windows workspace, Cargo test profile
- focused result: passed for immediate and batch paths
- full target result: 38 passed, 0 failed

The scripted broker held the first Produce response after observing the frame.
Dropping the in-flight `Producer::send` and `Producer::send_batch` futures did
not return either connection to the idle broker cache: each subsequent send
completed through a new connection and returned offsets `7` and `8`. The
observation records carried distinct connection IDs for each canceled and
subsequent Produce path, and the client metrics ended with
`in_flight_requests=0`.

This is deterministic producer-level cancellation evidence for the
post-transmission socket-read boundary. It is not published mixed-outcome
reconciliation, live broker qualification, a long campaign, or V1-04
completion.
