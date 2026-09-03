# V1-23 Published Competitor Comparison (2026-09-03)

The exact published `kafrust 0.3.6` artifact was compared with
`rust-rdkafka 0.39.0` in a fresh external project using
[run 33718060135](https://github.com/TaeeunKil/kafrust/actions/runs/33718060135).
The run used Kafka 4.3.1 KRaft, one isolated topic per implementation and
repetition, stable Rust, 20,000 unique 1-KiB records, batch size 200, and three
repetitions per implementation.

All six rows reconciled 20,000 unique business IDs with zero loss, zero
duplicates, and the shared payload digest
`7384f0e0012fab42060df529e06bdc32a348caff3dcf143281fed226af91cffa`.

| implementation | produce records/s (r1/r2/r3) | consume records/s (r1/r2/r3) | median produce | median consume |
| --- | --- | --- | ---: | ---: |
| kafrust | 66,874.28 / 81,174.44 / 88,821.11 | 264,209.63 / 316,323.88 / 316,018.06 | 81,174.44 | 316,018.06 |
| rust-rdkafka | 92,989.76 / 155,028.38 / 165,566.22 | 394,736.55 / 734,560.42 / 608,580.00 | 155,028.38 | 608,580.00 |

For this exact workload, kafrust measured 52.36% of the rust-rdkafka median
Produce rate and 51.93% of the median Consume rate. This is a workload-specific
diagnostic signal for V1-22 profiling and migration planning, not a universal
performance ranking, SLO, compatibility claim, or release gate. The run used
the published artifact and its dependency lockfile was checked for
`kafrust 0.3.6` and `rdkafka 0.39.0`.
