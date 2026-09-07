# Direct consumer Fetch session epoch reset (2026-09-07)

## Scope

Source commit `7f94dfdd5ed42576493ed43cd2f6c4dd1d5e9f7c` adds a deterministic
scripted-broker regression for `INVALID_FETCH_SESSION_EPOCH` recovery on Fetch
v12. The broker first establishes session `17`, then rejects the next request
with error code `70` after observing session `17` and request epoch `1`.

The consumer removes the invalid session, invalidates the topic metadata, and
retries through a fresh leader connection. The retry starts with session `0`
and returns the expected record at offset `42`; no stale session remains in
the consumer cache.

## Verification

The focused test is
`consumer::tests::resets_invalid_fetch_session_epoch_before_retrying`. It
asserts the first session fields, the rejected stale request, metadata and
leader reconnection, the reset session fields, the delivered offset, and an
empty final session cache. The full consumer test target (including the
consumer and adjacent share tests selected by the module filter) passed:

```text
52 passed; 0 failed
```

The test uses only in-memory scripted TCP fixtures. No Docker resource or
external Kafka broker was created or modified.

## Boundary

This is local deterministic Fetch-session evidence only. It does not claim
published artifact behavior, preferred-replica or retention qualification,
leader-movement recovery across a live cluster, 100,000-record reconciliation,
security compatibility, long campaigns, service canaries, or release
authorization.
