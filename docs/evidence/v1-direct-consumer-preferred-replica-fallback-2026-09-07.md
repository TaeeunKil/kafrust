# Direct consumer preferred-replica routing and fallback (2026-09-07)

## Scope

At pushed source `2e94c93bb38d0e960f2f58e0ae88c502ae21864c`, the direct consumer
preferred-replica tests were rerun as a focused deterministic pair. The
rack-aware Fetch v12 path records broker-provided preferred replica `2`, routes
the next request to that broker, and clears the preference when the preferred
broker returns `-1`. A second fixture seeds a preferred replica and returns a
leader error; the consumer clears the stale preference instead of retrying the
same route indefinitely.

## Verification

```text
cargo test -p kafrust --all-features negotiates_rack_aware_fetch_and_routes_to_preferred_replica -- --nocapture
1 passed; 0 failed
cargo test -p kafrust --all-features clears_preferred_replica_after_exhausted_fetch_failure -- --nocapture
1 passed; 0 failed
```

The fixtures assert Fetch v12 negotiation, rack `rack-a` encoding, preferred
broker routing, and preference removal after success or exhausted failure. No
external Kafka broker or Docker resource was created or modified.

## Boundary

This is local deterministic preferred-replica evidence only. It does not claim
live retention behavior, leader movement, three-broker failover, published
artifact qualification, 100,000-record reconciliation, long campaigns, service
canaries, or release authorization.
