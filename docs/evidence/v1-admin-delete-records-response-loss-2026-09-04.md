# DeleteRecords response-loss retry boundary (2026-09-04)

## Scope

- source commit: `983aab126d5c033f5611bda33dfbd75b9ec8faec`
- test: `admin::tests::retries_delete_records_after_response_loss_with_same_target`
- topology: in-memory scripted broker
- API: DeleteRecords v1

## Checks

The scripted broker returned metadata, read a complete DeleteRecords request,
and then dropped the connection without sending a response. With one retry
allowed, the client established a replacement connection, rediscovered
metadata, and sent a second request. The test compares the complete request
bytes and proves the retry uses the identical topic/partition/offset target.
The response preserves the existing partial partition result and the retry
counter is exactly one.

This closes the deterministic post-transmission response-loss slice for the
fixed-target, state-idempotent DeleteRecords operation. It does not generalize
safe retries to other Admin mutations.

## Boundary

Required Windows validation passed: formatting, workspace check, all-features
tests, Clippy with `-D warnings`, documentation, and whitespace checks. This is
scripted local evidence only; published floor/current active-member and
three-broker leader-failover qualification remain open.
