# Local RF3 lifetime diagnostic (2026-09-07)

This report records the retained result of the local, rate-limited Kafka
4.3.1 lifetime diagnostic. The run completed successfully as a diagnostic,
but its descriptor remains `status=diagnostic` and `qualified=false`. It is a
bounded signal for process lifetime, one broker restart, record-ID
reconciliation, cleanup, and final gauge draining.

## Run scope and result

- campaign: `20260907-bounded-six-hour`
- broker: Kafka 4.3.1, replication factor 3
- workload: 100 records/s, 64-byte payloads, three partitions, configured
  duration 21,600 seconds
- fault: broker 1 restart with a 10-second outage
- resource controls: 20-GiB growth budget and 100-GiB capacity reserve
- platform: Linux `x86_64`

The retained descriptor reports:

| measure | observed |
| --- | ---: |
| attempted records | 2,160,150 |
| acknowledged records | 2,160,150 |
| consumed unique records | 2,160,150 |
| record-ID digest | `5faccf4f44bc91085e1b4b4adfcd01a13266aa092b2106fa97aa564f1a94e2b7` |
| duplicate / loss count | 0 / 0 |
| operation errors | 0 |
| requests started | 86,427 |
| failed requests / retries | 4 / 5 |
| unknown outcomes | 0 |
| recovered | `true` |
| reported runtime | 21,600.895 seconds |
| maximum in-flight / buffered | 1 / 0 |
| final in-flight / buffered gauges | 0 / 0 |
| exit code | 0 |

The local console retained repeated capacity checks at `736 GiB` free on the
WSL-owned volume and `855 GiB` free at Docker's root, above the configured
100-GiB reserve. The final console line records completion of the six-hour
campaign directory.

## Runtime clocks

The six-hour duration above is the diagnostic's measured monotonic runtime
field. The
wrapper's wall-clock records are retained separately: `launch.json` records an
approximate start at `2026-09-07T18:05:30+09:00` and an estimated finish at
`2026-09-08T00:05:30+09:00`; `windows-exit.json` records exit code `0` at
`2026-09-08T00:33:06.4357787+09:00`. This report records both clocks without
inferring a cause for their difference.

## Source and artifact mapping

The result descriptor maps the run to snapshot commit
`1e48d17be4aaba1e2bffa6d89534039e806b1a71`, also retained in
`snapshot-commit.txt`. `source-origin.json` records the original checkout head
as `ee144eed3ae1a35717498527281760b53fafdee2`, an archive SHA-256 of
`d37d0f852403d8634e202f7759f7aa8fa802cf5db597b9c8cb20c8cf8d3dc18e`, and the
dirty paths present when the source archive was made, including the launcher
and resource-check changes. The original dirty set also included
`docs/performance.md`; this report does not edit that file.

The retained source and run records are:

- [six-hour descriptor](../../.artifacts/local-soak-20260907/six-hour-descriptor.json)
- [source origin](../../.artifacts/local-soak-20260907/source-origin.json)
- [snapshot commit](../../.artifacts/local-soak-20260907/snapshot-commit.txt)
- [launch record](../../.artifacts/local-soak-20260907/launch.json)
- [Windows exit record](../../.artifacts/local-soak-20260907/windows-exit.json)
- [console log](../../.artifacts/local-soak-20260907/console.log)

The descriptor itself records `working_tree_dirty: false` for the execution
snapshot. The separate source-origin record is the provenance for the
original dirty checkout; these records are not collapsed into one source
state.

## Milestone relationship

For [V1-21 Fault Soak And Data-Loss Semantics](../milestones/v1.0/v1-21-fault-soak-and-data-loss.md),
this is bounded local recovery and reconciliation evidence only. It does not
run the exact 10,000-records/s, 1-KiB six-hour workload, the declared fault
families, the 100-cycle or ambiguity-family gates, or the controlled
data-loss fixtures. V1-21 remains `In progress`.

For [V1-15 Session Ownership And Shutdown](../milestones/v1.0/v1-15-session-ownership-and-shutdown.md),
the zero final in-flight and buffered gauges provide one bounded lifetime and
cleanup observation around a broker restart. This does not replace the
owner-by-owner inventory, 100 construct/use/fault/close cycles, or published
secured churn profile.

No new qualification-ledger row is added. The ledger requires immutable
provenance and fields that this local descriptor does not provide, including
the client/protocol versions, broker image digest, workflow identity, and
complete security, latency, and memory records. The retained descriptor's
`qualified=false` classification is preserved.

## Non-claims

- not V1-21 throughput or completion evidence;
- not V1-22 SLO evidence;
- not published-artifact evidence;
- not service-canary evidence;
- not release authorization;
- not evidence for the missing V1-15 100-cycle or published secured audit;
- not a causal explanation for the wall-clock finish difference.
