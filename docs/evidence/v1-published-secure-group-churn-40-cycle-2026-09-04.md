# V1 Published Secure Group Churn — 40-Cycle Diagnostic (2026-09-04)

## Scope and status

This record covers bounded published-artifact SASL_SSL/SCRAM-SHA-256 group
churn diagnostics. Each profile used `kafrust 0.3.6` from crates.io, a fresh
three-broker KRaft cluster, six partitions, two group members, and an
independent group ID per cycle. The profiles cover both abrupt connection
drop and normal `LeaveGroup` exit. Each cycle checked disjoint ownership,
rejoin recovery, committed-offset restoration, and zero final
in-flight/buffered gauges.

These runs are diagnostic evidence, not the V1-21/V1-22 long campaigns. The
workflow's separate 100-cycle qualification flag is false, and this record
does not authorize a version bump, crates.io publish, service canary, or
1.0.0 release.

## Passing profiles

| Profile | Workflow | Source commit | Broker image | Job wall time | Result |
| --- | --- | --- | --- | --- | --- |
| Kafka 3.7.2 classic | [33830994497](https://github.com/TaeeunKil/kafrust/actions/runs/33830994497) | `5aa6b45164d3b5b90d4da88dee2b749334a10418` | `apache/kafka@sha256:6d8457e841c10f58f952cb3942a38b6b3c21015b643e3dd010aea37cc0b89055` | 9m 1s | passed |
| Kafka 4.3.1 KIP-848 consumer | [33832439518](https://github.com/TaeeunKil/kafrust/actions/runs/33832439518) | `5742d5b816f061daaa92358379cce94d1101ec16` | `apache/kafka@sha256:47dccc76b32761bc57462b8753144cdbb73a16b123b1d13d3eedb92bb7952b11` | 34m 46s | passed |

Normal `LeaveGroup` exit profiles:

| Profile | Workflow | Source commit | Broker image | Job wall time | Result |
| --- | --- | --- | --- | --- | --- |
| Kafka 3.7.2 classic | [33834878818](https://github.com/TaeeunKil/kafrust/actions/runs/33834878818) | `2c0d6d8165719ac9e7fe85bb7ac9f4154377225b` | `apache/kafka@sha256:6d8457e841c10f58f952cb3942a38b6b3c21015b643e3dd010aea37cc0b89055` | 9m 14s | passed |
| Kafka 4.3.1 KIP-848 consumer | [33834880832](https://github.com/TaeeunKil/kafrust/actions/runs/33834880832) | `2c0d6d8165719ac9e7fe85bb7ac9f4154377225b` | `apache/kafka@sha256:47dccc76b32761bc57462b8753144cdbb73a16b123b1d13d3eedb92bb7952b11` | 5m 29s | passed |

All four summaries report `cycle_count=40`, `records_per_cycle=6`,
`loss_count=0`, `duplicate_count=0`, and final `in_flight=0` /
`buffered=0`. Every log records cycles 1/40 through 40/40. The drop consumer
run's cycle 1 completed at 03:15:55Z and cycle 40 at 03:48:41Z, explaining the
longer KIP-848 drop/rejoin wall time; the normal-leave consumer run completed
in 5m 29s.

Immutable artifact metadata:

- Published package: `kafrust 0.3.6`.
- Published dependency lockfile SHA-256: `155a31ac7d4dfcda4e65708790acbacb42dd3fd40d778ce88c5f031851d26270`.
- Classic summary artifact: `kafrust-published-secure-group-churn-33830994497`.
- Consumer summary artifact: `kafrust-published-secure-group-churn-33832439518`.
- Classic normal-leave summary artifact: `kafrust-published-secure-group-churn-33834878818`.
- Consumer normal-leave summary artifact: `kafrust-published-secure-group-churn-33834880832`.

## Timeout calibration record

The first consumer attempt, [33830996464](https://github.com/TaeeunKil/kafrust/actions/runs/33830996464),
used source `5aa6b45164d3b5b90d4da88dee2b749334a10418`. It passed cycles
1–23, then the helper's 20-minute `40 × 30s` timeout fired before cycle 24.
No cycle assertion failed and no data-loss or duplicate condition was
reported. The helper budget was changed to 90 seconds per requested cycle and
the job limit to 60 minutes; the consumer profile then completed successfully
in run 33832439518. This failed attempt remains a calibration diagnostic and
is not counted as a passing profile.

## Remaining gates

- The bounded normal-leave and abrupt-drop profiles are both recorded above.
- Keep the 100-cycle flag and the separate long fault/SLO campaigns distinct
  from this bounded 40-cycle evidence.
- Complete remaining V1-08 callback, heartbeat, ambiguity-family, and
  approved external capacity/canary requirements before any release decision.

## Duplicate-detector hardening follow-up (2026-09-04)

After these four runs completed, a source review found that the original
`record_expected_records` helper stored only `(topic, partition)` in a set. A
second valid record for the same partition could therefore leave the set
unchanged and be hidden by the summary's `duplicate_count=0` field. The
published plaintext and secure helpers now count each expected record and fail
on a second observation or an unexpected payload value; focused regression
tests cover both cases.

That source correction postdates all four workflow runs listed above. Their
ownership, rejoin, offset, and final-gauge observations remain historical
diagnostics, but their zero-duplicate fields are not direct evidence under the
strengthened detector. Re-run the four 40-cycle profiles from the corrected
source before using duplicate absence for a group qualification or release
decision. No release or milestone completion claim is made from the historical
duplicate fields alone.
