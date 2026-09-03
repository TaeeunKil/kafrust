# Coordinator mutation response-loss matrix (2026-09-04)

## Scope

- source commit: `dbeff56ad07ad6aed0482fee884472384d7ee48e` (classic matrix)
- follow-up source commit: `0a7b0426488cfe7040bdf71794bcccf97ff6a45d`
- tests: `admin::tests::classifies_consumer_group_offset_commit_disconnect_as_unknown`, `admin::tests::classifies_member_aware_consumer_group_offset_commit_disconnect_as_unknown`, `admin::tests::classifies_consumer_group_offset_delete_disconnect_as_unknown`, `admin::tests::classifies_delete_group_disconnect_as_unknown`
- topology: in-memory scripted broker
- APIs: OffsetCommit v2; OffsetDelete v0; DeleteGroups v1

## Checks

Four independent fixtures discovered the group coordinator, read one complete
coordinator mutation frame, and then closed the connection without a response.
Each Admin call had retries disabled and returned the exact operation-specific
`Error::AdminMutationOutcomeUnknown` variant. The member-aware fixture also
withheld OffsetCommit from ApiVersions so the public KIP-848 entry point was
forced through its v9 compatibility fallback. API key/version assertions and
full-frame reads prove that every covered write crossed the transmission
boundary; no fixture permits a blind replay.

## Boundary

Required Windows validation passed: formatting, workspace check, all-features
tests, Clippy with `-D warnings`, documentation, and whitespace checks. This is
scripted local evidence for coordinator mutation response-loss classification,
including the member-aware v9 fallback; published floor/authorization
profiles, reconciliation reads, three-broker failover, long campaigns, service
canary, and release authorization remain open.
