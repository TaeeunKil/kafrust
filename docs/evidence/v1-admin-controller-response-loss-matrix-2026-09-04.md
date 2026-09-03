# Controller mutation response-loss matrix (2026-09-04)

## Scope

- source commit: `f3124d01b0bf30f5b14f7eefdb88c81fc90b5186`
- tests: `admin::tests::classifies_{create_topics,create_partitions,delete_topics,elect_leaders,partition_reassignment,update_features,add_raft_voter,remove_raft_voter,unregister_broker}_response_loss_after_transmission`
- topology: in-memory scripted broker
- APIs: CreateTopics v2; CreatePartitions v0; DeleteTopics v3; ElectLeaders v2; AlterPartitionReassignments v0; UpdateFeatures v1; AddRaftVoter v1; RemoveRaftVoter v0; UnregisterBroker v0

## Checks

Nine independent fixtures returned controller metadata (and ApiVersions where
the operation requires it), read one complete mutation frame, and then closed
the connection without a response. Each Admin call had retries disabled and
returned the exact operation-specific
`Error::AdminMutationOutcomeUnknown` variant. API key/version assertions and
full-frame reads prove that every covered write crossed the transmission
boundary; no fixture permits a blind replay.

## Boundary

Required Windows validation passed: formatting, workspace check, all-features
tests, Clippy with `-D warnings`, documentation, and whitespace checks. This is
scripted local evidence for common controller wrappers only; security Admin
mutations, published floor/authorization profiles, reconciliation reads,
three-broker failover, long campaigns, service canary, and release
authorization remain open.
