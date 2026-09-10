# V1-0.6 Published Operational Recovery Evidence (2026-09-10)

This record closes the bounded 0.6 operational-recovery slice against the
published `0.4.0` crate pair. It uses fresh external projects and Kafka 4.3.1
KRaft brokers; the repository itself is not used as a path dependency.

## Package and runner identity

- registry pair: `kafrust 0.4.0` and `kafrust-protocol 0.4.0`
- published lockfile digest: `9f413c48a2a91690e656a0e60720b128f6a831c8bdf5b285566f6ef5196fb627`
- group-churn source: [`9f12c37`](https://github.com/TaeeunKil/kafrust/commit/9f12c37ac11feceb18366ab902564378c79b181a)
- plaintext multi-broker source: [`c139905`](https://github.com/TaeeunKil/kafrust/commit/c139905f4b314a99091d5cd330d393f17034a274)
- secure multi-broker source: [`b3ed66a`](https://github.com/TaeeunKil/kafrust/commit/b3ed66aff5edd78650d81214987a622c38ddffd0)
- broker: Apache Kafka `4.3.1` KRaft; hosted group image ID
  `sha256:47dccc76b32761bc57462b8753144cdbb73a16b123b1d13d3eedb92bb7952b11`
  and self-hosted multi-broker image digest
  `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- all multi-broker runs used one self-hosted WSL runner at a time; the secure
  profile used a 15-minute, 25-record/s minimum bounded workload.

## Group rejoin and member-loss matrix

Each run used six partitions and 100 cycles. The fresh external lockfile
resolved both crates at `0.4.0`; every run emitted `qualified: true`.

| protocol | member exit | workflow | cycles | records/cycle | loss | duplicate | final in-flight/buffered |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| classic | leave | [34440352817](https://github.com/TaeeunKil/kafrust/actions/runs/34440352817) | 100 | 6 | 0 | 0 | 0 / 0 |
| classic | drop | [34440355717](https://github.com/TaeeunKil/kafrust/actions/runs/34440355717) | 100 | 6 | 0 | 0 | 0 / 0 |
| KIP-848 consumer | leave | [34440358500](https://github.com/TaeeunKil/kafrust/actions/runs/34440358500) | 100 | 6 | 0 | 0 | 0 / 0 |
| KIP-848 consumer | drop | [34440361001](https://github.com/TaeeunKil/kafrust/actions/runs/34440361001) | 100 | 6 | 0 | 0 | 0 / 0 |

The four immutable summary artifact SHA-256 values are, in workflow order,
`000c1d6e90772485265c1dc83d84be84b86d80fc05bae75c6f441be4312c47c2`,
`ebb306f683f492b1be9de47cd479710262eb96160d92ef443af4233560daf4d2`,
`752c9e22e4dd4da50e3689effbb7101ff25201e1e184a96bdc2ea5ff8ee68b1b`, and
`034531f649270b75e79ebd6f98d125b83ba3bd30c994a478277c02207af47133`.

## Plaintext multi-broker recovery

Run [34439794733](https://github.com/TaeeunKil/kafrust/actions/runs/34439794733)
ran one 900.005-second, 1-KiB workload against three brokers with
`leader@20,coordinator@40,combined@60,simultaneous@80` faults. It produced,
acknowledged, and uniquely consumed `28,786,200` records. The run observed one
high-level operation error, 17 failed requests, and 36 retries during injected
outages; `recovered` was true, unknown outcomes were 0, and final
in-flight/buffered gauges were 0 / 0. The reconciliation digest was
`edafc96800dee24d268c10f1553954d92dae34c5ba9968122bd525a529cda429`.
The result and descriptor artifact SHA-256 values are
`66a2c06d1abb6b99f6bdb331d451734521c605de05a655b1f18c7e1e42a84a48` and
`f928138a70ef14fcc16c90a8be06b9fbb668c45ff1574d19b4b388d6d262dbe0`.

## Secure multi-broker recovery

The first dispatch encountered a pre-existing Docker network name and failed
before broker startup ([34441143214](https://github.com/TaeeunKil/kafrust/actions/runs/34441143214)).
The run-scoped network was absent after cleanup, so the same bounded workload
was rerun once with a new campaign ID. The successful rerun was
[34441268698](https://github.com/TaeeunKil/kafrust/actions/runs/34441268698).

It ran for 900.006 seconds with SASL/SCRAM over TLS, the same four-fault
schedule, and a 25-record/s minimum gate. It attempted, acknowledged, and
uniquely consumed `22,609,300` records at `25,121.264` records/s. It recorded
485 operation errors, 10 failed requests, 16 retries, and 48,500 unknown
outcomes during the injected outages; the record-ID reconciliation still had
loss 0 and duplicate 0, `recovered` was true, and final in-flight/buffered
gauges were 0 / 0. The retry ratio was `0.000071%` and the reconciliation
digest was `e832f2ba97ee73b60d99ff81a0f3945aebc5eef8463e729370f0ae258174fd49`.
The result and descriptor artifact SHA-256 values are
`0736f07945b966bd98af38fac4243e7af95960775e6e20f32ee067e995b12ed8` and
`6ee23699cd91c2e98528b25396a9cf4fae25452d19e31cf5a7ad33e64294a7a1`.

## Qualification boundary

These six successful runs establish the targeted 0.6 boundary for published
package group rejoin and bounded plaintext/SASL-TLS broker-restart recovery.
The secure run's unknown outcomes are retained as an ambiguity signal; they
were reconciled without loss or duplication and are not silently treated as
successes.

The evidence does not close the complete V1-20 matrix, the official high-load
V1-21 fault campaigns, V1-22 repeated performance SLO profiles, service
canary/API-freeze gates, or the optional 1.0 claim. The workflows retain final
gauges and immutable provenance but do not emit RSS, OS-thread, socket, or
disk time-series; none of those absent measurements is inferred here, and OS
threads are not described as Tokio task counts.

Because the crate source and public package boundary remain unchanged, this
is a repository milestone record. It does not authorize a `0.6.0` registry
upload; crates.io remains at the coordinated `0.4.0` pair until a new
independently consumable package boundary is implemented and verified.
