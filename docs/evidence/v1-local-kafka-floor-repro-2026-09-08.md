# Local Kafka 3.7.2 fixture and Fetch reproduction

This is bounded diagnostic evidence, not a replacement for the live Kafka CI
matrix or a V1 milestone qualification.

## Source and environment

- Baseline: immutable local snapshot
  `1e48d17be4aaba1e2bffa6d89534039e806b1a71`, also used by the
  [completed six-hour diagnostic](v1-local-lifetime-diagnostic-2026-09-07.md).
- Ubuntu-T9 WSL, Apache Kafka 3.7.2, three brokers, replication factor three.
- Each run created fresh topics and removed only its own containers/network.
- The retention example uses one partition; the fetch/failover example uses
  three partitions. No broker outage was injected in these reproduction runs.
- Local logs: `.artifacts/floor-repro-20260908/`, mirrored from
  `/home/taeeun/kafrust-campaigns/floor-repro-20260908/`.

## Observations

| Run | Retention example | Fetch example | Interpretation |
| --- | --- | --- | --- |
| Baseline | 2/5 passed; rounds 2, 3, 5 failed with code 3 | 5/5 passed | Fresh-topic fixture can fail before group recovery is exercised. |
| Trace-only snapshot | 3/5 passed; rounds 3, 4 failed with code 3 | 4/5 passed; round 1 failed with code 100 | Added error-location logging places code 3 at the first producer send. Code 100 also occurs without SASL. |
| Topic-readiness gate, baseline client | 5/5 passed | 5/5 passed | Waiting for a leader and full ISR on every broker stabilized these ten bounded attempts. |

The trace copy only adds error-location logging to the retention example;
the baseline source snapshot remains unchanged. Fetch round 1 reports
`UNKNOWN_TOPIC_ID` (100) after joining the group. This supports handling the
topic-ID refresh path in the consumer independently of authentication.

`scripts/wait_for_kafka_topic.py` checks the expected partition count,
replication factor, assigned leader, and full ISR through each broker before
the workload begins. The CI retained-log and SASL combined-failover fixtures
now use that gate. The gate does not retry or suppress a failing workload.

The earlier CI coordinator error 16 was **not reproduced** here. These results
do not establish that every previous CI failure is resolved.

The consumer change adds error 100 to the existing bounded Fetch retry path.
Scripted Fetch v13 regressions failed before the change and passed afterward:
one verifies stale UUID, error 100, metadata refresh, and successful fetch with
the new UUID; the other verifies retry exhaustion leaves the consumer position
unchanged. All 57 focused consumer tests passed. The six required workspace
validation commands also passed on Rust 1.81.0 on Windows. Live validation of
the resulting candidate is recorded separately from the baseline runs above.
