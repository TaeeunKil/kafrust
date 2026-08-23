# V1-23 Million-Record Reference Comparison (2026-08-23)

## Scope

Migration Reference Canary run
[32645204676](https://github.com/TaeeunKil/kafrust/actions/runs/32645204676)
used source commit `c56beaab3702d41ec6c7fc10de80bf3483c7711c`, Kafka 4.3.1
KRaft, one isolated topic per implementation, 1-KiB payloads, and batches of
100. The fixture ran one million unique records through the source-checkout
`kafrust` path and one million through `rust-rdkafka 0.39.0`.

## Retained results

| Implementation | Records | Unique | Loss | Duplicates | Produce/s | Consume/s | Payload digest |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| kafrust source checkout | 1,000,000 | 1,000,000 | 0 | 0 | 31,904.29 | 31,251.06 | `bb9d4e95e5a812aa63187d56c17de87631f3def47f96e7b92a535c396eba210f` |
| rust-rdkafka 0.39.0 | 1,000,000 | 1,000,000 | 0 | 0 | 57,602.65 | 33,632.97 | same |

The two JSON result files reported equal payload digests, zero loss, and zero
duplicates. The workflow completed successfully in about 204 seconds and
uploaded `migration-kafrust.json` and `migration-rdkafka.json` as the retained
artifact `kafrust-migration-canary-32645204676`.

## Boundary

This closes the million-record *reference comparison* preparation rung only.
The fixture uses an isolated single-node source checkout and injects no
credential rotation, leader/coordinator fault, rebalance, staged forward
cutover, or rollback. It does not cover the V1-23 requirement for transactions,
Admin flows, or a named representative service/environment, and it is not
published-artifact evidence or migration approval. V1-23 remains blocked on a
named service, owner, and approved canary environment.
