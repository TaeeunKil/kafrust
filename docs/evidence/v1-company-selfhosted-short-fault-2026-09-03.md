# Company self-hosted short fault diagnostic (2026-09-03)

Run [33716428169](https://github.com/TaeeunKil/kafrust/actions/runs/33716428169)
executed from workflow head `72edcb3507750d2c418af6ca4e24b5002dc069cd`
(`72edcb3`) on the company WSL2 self-hosted runner
`wsl-ubuntu-t9` (`Linux`, `X64`, `docker`, `wsl2`). The fresh external project
resolved the exact published pair `kafrust 0.3.6` /
`kafrust-protocol 0.3.6`.

## Diagnostic result

- broker: Kafka 4.3.1, three-broker KRaft;
  `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- fault mode: simultaneous multi-broker restart harness;
  `leader@25,coordinator@50,combined@70,simultaneous@85`
- broker outage: 10 seconds per event
- measured duration: 120.006 seconds
- records: 3,012,600 attempted, acknowledged, and consumed uniquely
- requests: 60,293 started; 4 failed; 11 retries; 0 unknown outcomes
- operation errors: 0
- reconciliation: qualified, loss 0, duplicates 0,
  digest `8e865d153ef0ac696a5d20a1d905bb8e500a8300f51d8114d6675ffaf18b4923`
- final gauges: in-flight 0, buffered 0
- secret scan: 0 findings
- external lockfile digest: `2f81a33ed05baf0321bb7a643355bec49a5e3a8904d7cd2354e54a86314f976b`

The immutable descriptor and raw result were retained in the run artifact
`kafrust-published-fault-segment-company-short-diagnostic-2026-09-03-0-33716428169`.
The workflow's one-segment descriptor says `continuity_claim=qualified`; this
record classifies the result as a short diagnostic because 120 seconds is far
below the V1-21 six-hour requirement.

The workflow cleanup removed only its explicitly named Kafka containers and
network. Existing company Docker resources were not pruned or changed.

## Boundary

This proves company self-hosted execution, Docker capacity, published-artifact
resolution, and one bounded fault schedule. It is not a V1-21 six-hour result,
100-cycle/ambiguity-family completion, cross-segment continuity, V1-22 SLO
evidence, V1-23 service canary, API-freeze/RC evidence, or `1.0.0` release
authorization.
