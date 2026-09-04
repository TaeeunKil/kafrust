# Published Performance Campaign Diagnostic (2026-09-04)

The published `kafrust 0.3.6` / `kafrust-protocol 0.3.6` pair completed a
bounded performance diagnostic from pushed source
[`9b6f510f7dbff9dd0a38a396849661062de8392f`](https://github.com/TaeeunKil/kafrust/commit/9b6f510f7dbff9dd0a38a396849661062de8392f)
in [GitHub Actions run 33845259919](https://github.com/TaeeunKil/kafrust/actions/runs/33845259919).
The run used four fresh external projects on Rust 1.81.0, with a 10-second
warmup, a 300-second measured window, 10-second samples, two workers, 1-KiB
values, and batch size 50. The matrix covered the accepted floor and pinned
current broker lines with no compression and Zstd.

| Kafka | compression | produced / consumed | records/s | p50 / p95 / p99 ms | RSS slope bytes/s | artifact |
| --- | --- | ---: | ---: | --- | ---: | --- |
| 3.7.2 | none | 22,366,850 / 22,366,850 | 74,556 | 1 / 1 / 1 | 4,123 | [9926429327](https://github.com/TaeeunKil/kafrust/actions/runs/33845259919/artifacts/9926429327) |
| 3.7.2 | Zstd | 16,235,600 / 16,235,600 | 54,119 | 1 / 5 / 5 | 50 | [9926427823](https://github.com/TaeeunKil/kafrust/actions/runs/33845259919/artifacts/9926427823) |
| 4.3.1 | none | 20,887,450 / 20,887,450 | 69,625 | 1 / 1 / 1 | 3,752 | [9926430204](https://github.com/TaeeunKil/kafrust/actions/runs/33845259919/artifacts/9926430204) |
| 4.3.1 | Zstd | 17,021,000 / 17,021,000 | 56,737 | 1 / 1 / 5 | 236 | [9926428708](https://github.com/TaeeunKil/kafrust/actions/runs/33845259919/artifacts/9926428708) |

Every profile reported zero failed requests, zero retries, zero unknown
outcomes, zero loss, zero duplicates, matching expected/observed record
digests, and zero final in-flight/buffered records. All four lockfiles resolve
the exact published pair and share SHA-256
`5ffa8147a2c178d0eee0c060aecd9bde278615b5fc5645b9d390737241c36fd4`.

This run is a useful published-artifact regression and resource diagnostic,
but it is not a locked performance baseline. The broker is single-node with
replication factor one, the measured window is five minutes, and there is one
repetition. It therefore does not satisfy the V1-22 five-repetition,
six-profile, eight-hour SLO campaign, prove competitor parity, qualify a
long-campaign resource budget, or authorize `1.0.0`.
