# Company self-hosted short DNS-recovery diagnostic (2026-09-04)

Run [33817682088](https://github.com/TaeeunKil/kafrust/actions/runs/33817682088)
executed from workflow head `e2fbcf3865b9fcb4a05f0c7032731c1dbfced4bf` on the
company WSL2 runner `wsl-ubuntu-t9` (`Linux`, `X64`, `docker`, `wsl2`) after a
root-only temporary DNS override restored GitHub Actions connectivity. The
fresh external project resolved the exact published pair `kafrust 0.3.6` /
`kafrust-protocol 0.3.6`.

## Diagnostic result

- broker: Kafka 4.3.1, three-broker KRaft;
  `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- fault schedule: `leader@25,coordinator@50,combined@75`
- broker outage: 5 seconds per event
- measured duration: 60.002 seconds
- records: 1,736,700 attempted, acknowledged, and consumed uniquely
- requests: 34,765 started; 4 failed; 8 retries; 0 unknown outcomes
- operation errors: 0
- reconciliation: qualified, loss 0, duplicates 0,
  digest `3f8be9d5a3e1bf6b5af0e74ea75f99106ee1f849ddd8d272e4bbb5d642622627`
- final gauges: in-flight 0, buffered 0
- secret scan: 0 findings
- external lockfile digest: `2f81a33ed05baf0321bb7a643355bec49a5e3a8904d7cd2354e54a86314f976b`

The immutable descriptor and raw result were retained in the run artifact
`kafrust-published-fault-segment-company-short-dns-recovery-2026-09-04-0-33817682088`.
The one-segment descriptor reports `continuity_claim=qualified`; this record
classifies the result as a short diagnostic because 60 seconds is far below
the V1-21 six-hour requirement.

The workflow cleanup removed only its explicitly named Kafka containers and
network. Existing company Docker resources were not pruned or changed.

## Boundary

This proves that the temporary DNS recovery restored self-hosted runner
execution, Docker capacity, published-artifact resolution, and one bounded
fault schedule. It is not a V1-21 six-hour result, 100-cycle/ambiguity-family
completion, cross-segment continuity, V1-22 SLO evidence, V1-23 service canary,
API-freeze/RC evidence, or `1.0.0` release authorization. The resolver change
is temporary and must be made persistent by an authorized WSL networking policy
before unattended campaigns.
