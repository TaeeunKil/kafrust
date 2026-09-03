# V1 transactional Produce version-cap matrix — 2026-09-03

## Scope

At source commit `fd9b93938f65f7d5944175dd52225bd93b3d2af3`, the producer unit
test `caps_transactional_produce_at_v11_across_advertised_tv2_versions` checks
the transactional selector against brokers advertising Produce v11, v12, and
v13. For each advertised maximum, both the immediate and prepared-batch paths
are exercised with a topic ID present. Every path returns `ProduceVersion::V11`
and reports wire API version 11, so transactional traffic cannot select v12 or
v13 while the TV2 state machine remains unqualified.

## Verification

```text
cargo test -p kafrust caps_transactional_produce_at_v11_across_advertised_tv2_versions -- --nocapture
1 passed; 0 failed
```

The source commit also passed the required workspace format, check, test,
Clippy, documentation, and diff checks. The pushed commit is covered by the
stable/Rust 1.81 CI workflow.

## Boundary

This closes the deterministic selector guard only. It does not qualify the
Kafka 4.3.1 transaction.version=2 wire state machine, transactional live
roundtrips, three-broker movement, long campaigns, published artifacts, or
release authorization. The V1-06 TV2 fixture/coherent-state exit criterion
remains open.
