# V1 Published Group Churn 40-Cycle Diagnostic (2026-09-04)

- client: `kafrust 0.3.6` published artifact
- source_commit: `b37270bff9899f8283884304b9b57ba3e939868c`
- workload: six partitions; two-member group; abrupt second-member drop and
  rejoin; committed-offset restoration on every cycle

## Results

| Broker / group protocol | Run | Wall time | Cycles | Records per cycle | Loss | Duplicates | Final gauges | Broker image identity |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Kafka 3.7.2 / classic | [33829763775](https://github.com/TaeeunKil/kafrust/actions/runs/33829763775) | 5m 58s | 40 | 6 | 0 | 0 | in-flight=0, buffered=0 | `apache/kafka@sha256:6d8457e841c10f58f952cb3942a38b6b3c21015b643e3dd010aea37cc0b89055` |
| Kafka 4.3.1 / KIP-848 consumer | [33829798439](https://github.com/TaeeunKil/kafrust/actions/runs/33829798439) | 7m 29s | 40 | 6 | 0 | 0 | in-flight=0, buffered=0 | `apache/kafka@sha256:47dccc76b32761bc57462b8753144cdbb73a16b123b1d13d3eedb92bb7952b11` |

Each run produced 240 ownership observations (six per cycle), with every
cycle reacquiring all six partitions and restoring committed offsets. Both
workflow summaries report `cycle_count=40`, zero loss, zero duplicates, and
drained final gauges. The external lockfile SHA-256 was
`9644dadf2a339f48a3ff2b8f44f1bbe3868e79f378b3ce1f242789147373b834` for both
runs.

## Qualification boundary

These runs are bounded 40-cycle evidence for the classic and KIP-848 group
churn paths. The workflow intentionally marks them `qualified=false` because
its separate V1-21 group-family gate requires 100 cycles. They do not close
the V1-08 secure 40-cycle profile, the complete V1-08 exit criteria, the
ambiguity-family matrix, six-hour fault campaigns, service canary, or release
authorization.

