# V1 Published Short Surface Smoke — 2026-09-03

## Scope

This record captures the bounded published-artifact checks run after returning
to the company workstation. Every passing row below used the immutable
`kafrust 0.3.6` registry package and protocol dependency from the same
workflow head, `31b56aeb628ac80095f84239a16a6b8e5cb1f54e`. These are short
surface/failover checks only; they are not substitutes for the V1-21/V1-22
long campaigns or the V1-23 service canary.

## Passing runs

| Surface | Configuration | Run |
| --- | --- | --- |
| Multi-broker failover | Kafka 4.3.1, KIP-848 consumer, PLAINTEXT | [33721922313](https://github.com/TaeeunKil/kafrust/actions/runs/33721922313) |
| Secure multi-broker failover | Kafka 3.7.2, classic, SASL_SSL/SCRAM-SHA-256, leader fault | [33721926455](https://github.com/TaeeunKil/kafrust/actions/runs/33721926455) |
| Secure group rebalance | Kafka 4.3.1, KIP-848 consumer, SASL_SSL/SCRAM-SHA-256 | [33721930630](https://github.com/TaeeunKil/kafrust/actions/runs/33721930630) |
| Secure transaction failover | Kafka 3.7.2, SASL_SSL/SCRAM-SHA-256 | [33721934670](https://github.com/TaeeunKil/kafrust/actions/runs/33721934670) |
| ShareGroupDescribe | Kafka 4.3.1, Share protocol, PLAINTEXT | [33721939764](https://github.com/TaeeunKil/kafrust/actions/runs/33721939764) |
| Share multi-broker failover | Kafka 4.3.1, three-broker replicated Share state | [33721944682](https://github.com/TaeeunKil/kafrust/actions/runs/33721944682) |
| Share multi-member ownership | Kafka 4.3.1, three brokers, 20-second members, one record/partition | [33721949931](https://github.com/TaeeunKil/kafrust/actions/runs/33721949931) |
| Share group state failover | Kafka 4.3.1, replicated Share coordinator state | [33721972300](https://github.com/TaeeunKil/kafrust/actions/runs/33721972300) |
| Streams group runtime | Kafka 4.3.1, Streams group surface | [33721976423](https://github.com/TaeeunKil/kafrust/actions/runs/33721976423) |
| Share acknowledgement soak | Kafka 4.3.1, 64 acknowledgement cycles | [33721981000](https://github.com/TaeeunKil/kafrust/actions/runs/33721981000) |
| Share member-loss rebalance | Kafka 4.3.1, three brokers, 180-second surviving member | [33722189166](https://github.com/TaeeunKil/kafrust/actions/runs/33722189166) |

All eleven listed runs completed successfully, including external-project
dependency verification where the workflow provides it. The repository CI
for this documentation head also passed in [33721524284](https://github.com/TaeeunKil/kafrust/actions/runs/33721524284).

## Retained failure diagnostic

The same member-loss workflow was first run with an intentionally shortened
`run_seconds=30` input in [33721954797](https://github.com/TaeeunKil/kafrust/actions/runs/33721954797). The broker and external package build succeeded, but the
surviving member ended with `assignment=3` while the fixture requires all six
partitions after member loss. The run is retained as a failed diagnostic, not
as a passing qualification result. Re-running with the supported 180-second
window passed in 33722189166, so no source change was inferred from the
shortened-input failure.

Four duplicate dispatches were cancelled before execution and are excluded
from the evidence set: 33721971737, 33721975925, 33721980662, and 33721984700.

## Qualification boundary

These results strengthen the published 0.3.6 surface record for V1-10,
V1-14, V1-16, V1-20, and the short diagnostic portions of V1-21/V1-22. They
do not close the accepted V1-20 matrix, six-hour/long-duration fault or SLO
campaigns, the remaining V1-03–V1-18 evidence gaps, the named V1-23 service
canary, API freeze, release candidate, or `1.0.0` release gates.
