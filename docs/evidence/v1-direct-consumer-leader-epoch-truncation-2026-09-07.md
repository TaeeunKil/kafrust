# Direct consumer leader-epoch truncation recovery (2026-09-07)

## Scope

At pushed source `a6719f1dc5b9ed1a8f7de7d6613f4987bc8f8181`, the scripted-broker
regression `recovers_assignment_after_leader_epoch_truncation` was rerun. A
Fetch at offset `100` with assignment epoch `4` receives a leader-transition
error. The consumer resolves the prior epoch through OffsetForLeaderEpoch v3,
receives end offset `50` at epoch `5`, refreshes Metadata, and retries Fetch at
offset `50` with epoch `5`. The returned record advances the assignment to
`51`.

## Verification

```text
cargo test -p kafrust --all-features recovers_assignment_after_leader_epoch_truncation -- --nocapture
1 passed; 0 failed
```

The fixture asserts the exact OffsetForLeaderEpoch v3, Metadata v12/v1, and
Fetch v12 request sequence and checks the recovered record offset, final
position, and assignment leader epoch. No external Kafka broker or Docker
resource was created or modified.

## Boundary

This is local deterministic leader-epoch/truncation evidence only. It does not
claim live retention or leader movement, unclean-election recovery, published
artifact qualification, 100,000-record reconciliation, long campaigns, service
canaries, or release authorization.
