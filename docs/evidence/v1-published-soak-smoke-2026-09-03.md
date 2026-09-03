# Published Broker-Restart Soak Smoke (2026-09-03)

The published kafrust 0.3.6 artifact completed a bounded 120-second
single-node broker-restart smoke in [run 33719565892](https://github.com/TaeeunKil/kafrust/actions/runs/33719565892)
from source 003561b. The fresh external project resolved the exact
kafrust 0.3.6 dependency from crates.io and ran against Kafka 4.3.1 KRaft.
The broker was stopped after one third of the run for ten seconds and then
restarted.

| field | observed |
| --- | --- |
| duration | 120.001 seconds |
| payload/workload | 1-KiB payloads, batch size 100 |
| records | 6,375,800 produced and consumed |
| operation errors | 139 during the injected outage |
| failed requests | 159 |
| retries | 972 |
| recovery | recovered=true after the broker restart |
| final gauges | in_flight_requests=0, buffered_records=0 |
| peak gauges | max_in_flight_requests=1, max_buffered_records=0 |
| dependency check | fresh lockfile resolved kafrust 0.3.6 |

The retained result artifact is
kafrust-published-soak-33719565892/published-soak.json. Its SHA-256 is
not-recorded; the workflow artifact and run log are the immutable references.
This result demonstrates one bounded published single-node recovery profile.
It does not satisfy V1-21 six-hour campaigns, repeated fault-family gates,
V1-22 SLO qualification, V1-23 service-canary evidence, or release
authorization.

## Corrective fixture record

The preceding run [33718942779](https://github.com/TaeeunKil/kafrust/actions/runs/33718942779)
from source e702de7 reached the same client-side recovery and zero final
gauges, but failed its final workflow assertion because the JSON formatter did
not emit the two max gauge fields that the workflow checked. This was a
fixture-only failure, not a published-library failure. Commit 003561b added
those fields and the rerun above passed the unchanged gauge assertions.

