# V1 Published Topic-ID Leader-Movement Diagnostic (2026-09-04)

- source_commit: `c4d06e68d271500129a2624491034d21a1fe0f1a`
- published_client: `kafrust 0.3.6` from crates.io
- broker: Apache Kafka `4.3.1` (`apache/kafka:4.3.1`)
- topology: three-broker KRaft cluster, replication factor 3, six partitions
- security: PLAINTEXT
- group_protocol: KIP-848 `consumer`
- workflow: [Published Multi-Broker Smoke run 33827383799](https://github.com/TaeeunKil/kafrust/actions/runs/33827383799)
- workflow_wall_time: 1 minute 20 seconds (2026-09-04 01:52:13Z--01:53:33Z)

## Purpose

This bounded published-artifact probe checks that a topic UUID returned by
Metadata v12 remains stable while a replicated partition moves to a new leader.
It also checks that the published producer and KIP-848 consumer can complete a
one-record phase before and after the leader stop.

## Procedure and observed result

1. The workflow created a fresh six-partition topic and selected partition 0,
   initially led by broker 1.
2. The pre-failover phase produced and consumed one record at offset 0. Metadata
   v12 returned topic UUID
   `217dfd7fb4d9462d98c09fedc14b9b1d`.
3. The workflow stopped container `kafrust-published-multi-1`. The Kafka topic
   description reported `partition 0 moved to leader 2`.
4. The post-failover phase produced and consumed one record at offset 1. A
   second Metadata v12 query returned the same UUID
   `217dfd7fb4d9462d98c09fedc14b9b1d`; the client would fail if it changed.
5. The published dependency lock check and workflow cleanup completed
   successfully.

The exact pre/post log lines were:

```text
published multi-broker pre-failover committed kafrust-published-multi-33827383799-0@0 topic_id=217dfd7fb4d9462d98c09fedc14b9b1d
partition 0 moved to leader 2
published multi-broker post-failover resumed kafrust-published-multi-33827383799-0@1 topic_id=217dfd7fb4d9462d98c09fedc14b9b1d
```

## Interpretation

This passes the bounded published topic-ID continuity and single leader-
movement diagnostic. It is useful evidence for the V1-03 data-plane path and
the published-crate smoke surface.

The run is not an official V1-03 completion or a release authorization. It does
not provide Apache response oracles for every shape, the accepted-floor
security/workload matrix, the full V1-20 published matrix, a long campaign,
service canary, or `1.0.0` readiness. The workflow used the broker image tag;
an image digest was not recorded by this run.

## Accepted-floor replay

The same published `0.3.6` workflow was replayed against Kafka `3.7.2` using
classic consumer groups in [run 33828967587](https://github.com/TaeeunKil/kafrust/actions/runs/33828967587).
The run lasted 1 minute 18 seconds (2026-09-04 02:17:27Z--02:18:45Z), selected
partition 1 led by broker 1, and stopped that broker. Kafka then reported
partition 1 moved to leader 2. The pre-failover record completed at offset 0 and
the post-failover record at offset 1; Metadata v12 returned the unchanged topic
UUID `b258804505e74c4eb3186133ba66b260` in both phases.

```text
published multi-broker pre-failover committed kafrust-published-multi-33828967587-1@0 topic_id=b258804505e74c4eb3186133ba66b260
partition 1 moved to leader 2
published multi-broker post-failover resumed kafrust-published-multi-33828967587-1@1 topic_id=b258804505e74c4eb3186133ba66b260
```

This is a second bounded published diagnostic at the planned accepted-floor
broker line. It does not replace the complete security/workload matrix,
row-by-row V1-20 qualification, long campaigns, service canary, or release
authorization. The workflow used the broker image tag and did not retain an
image digest.
