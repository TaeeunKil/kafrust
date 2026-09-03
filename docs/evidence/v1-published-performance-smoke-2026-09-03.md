# Published Performance Smoke (2026-09-03)

The published `kafrust 0.3.6` artifact completed the corrected four-profile
performance smoke in [run 33718664874](https://github.com/TaeeunKil/kafrust/actions/runs/33718664874)
from source `6d5d7ec`. The external projects used Rust 1.81, Kafka 3.7.2 and
4.3.1 KRaft, one partition, 10,000 records, 1-KiB payloads, batch size 200,
and no compression or Zstd.

| Kafka | compression | produce records/s | consume records/s | batch p50/p95/p99 ms | requests | retries | in-flight | buffered |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 3.7.2 | none | 48,454.81 | 195,548.92 | 3.664 / 5.098 / 18.782 | 66 | 0 | 0 | 0 |
| 3.7.2 | Zstd | 44,427.86 | 217,381.15 | 3.544 / 5.134 / 34.997 | 57 | 0 | 0 | 0 |
| 4.3.1 | none | 41,435.73 | 178,576.98 | 3.993 / 6.408 / 24.746 | 67 | 0 | 0 | 0 |
| 4.3.1 | Zstd | 43,846.22 | 301,361.86 | 3.631 / 5.552 / 40.051 | 58 | 0 | 0 | 0 |

Each row consumed all 10,000 records (10,240,000 bytes) and ended with
`in_flight_requests=0`, `buffered_records=0`, `max_in_flight_requests=1`, and
`max_buffered_records=0`. The workflow also verified that the fresh lockfiles
resolved `kafrust 0.3.6`.

## Corrective rerun

The preceding run [33718369244](https://github.com/TaeeunKil/kafrust/actions/runs/33718369244)
from `80bb09b` failed its final gauge assertion on all four profiles. Its
client assertions had already completed successfully, but the fixture emitted
`buffered_records` and `max_in_flight_requests` in the wrong JSON argument
positions. Commit `6d5d7ec` corrected only that test-fixture mapping; the
rerun above passed all four profiles. The failure is retained as a fixture
diagnostic and is not a published-library defect.

This is short published compatibility/performance evidence only. It is not the
V1-22 five-repetition eight-hour matrix, a locked baseline, a universal
performance claim, or release authorization.
