# Published Performance Mode Diagnostics (2026-09-03)

The published kafrust 0.3.6 artifact completed two additional bounded
performance diagnostics from source ccbe3a7: [buffered mode run 33721044749](https://github.com/TaeeunKil/kafrust/actions/runs/33721044749)
and [direct-consumer mode run 33721215334](https://github.com/TaeeunKil/kafrust/actions/runs/33721215334).
Each run used fresh external projects, Rust 1.81, two workers, 1-KiB payloads,
batch size 50, a 5-second warmup, and a 20-second measured window.

## Buffered mode

| Kafka | compression | produced/consumed | p50/p95/p99 ms | RSS slope bytes/s |
| --- | --- | ---: | --- | ---: |
| 3.7.2 | none | 271,750 / 271,750 | 1 / 1 / 5 | 4,670 |
| 3.7.2 | Zstd | 168,500 / 168,500 | 1 / 5 / 5 | 18,187 |
| 4.3.1 | none | 306,000 / 306,000 | 1 / 1 / 5 | 246 |
| 4.3.1 | Zstd | 176,300 / 176,300 | 1 / 5 / 5 | 6,718 |

## Direct-consumer mode

| Kafka | compression | produced/consumed | p50/p95/p99 ms | RSS slope bytes/s |
| --- | --- | ---: | --- | ---: |
| 3.7.2 | none | 1,787,750 / 1,787,750 | 1 / 1 / 1 | 14,582 |
| 3.7.2 | Zstd | 1,040,700 / 1,040,700 | 1 / 5 / 5 | 0 |
| 4.3.1 | none | 1,512,450 / 1,512,450 | 1 / 1 / 1 | -737 |
| 4.3.1 | Zstd | 1,059,000 / 1,059,000 | 1 / 5 / 5 | 13,107 |

All eight profiles reported zero failed requests, zero retries, zero unknown
outcomes, zero loss, zero duplicates, matching record digests, and zero final
in-flight/buffered records. The external descriptors use qualified=false and
retain the same published lockfile SHA-256
5ffa8147a2c178d0eee0c060aecd9bde278615b5fc5645b9d390737241c36fd4.

These mode-specific results extend the short profiling record only. They are
not the V1-22 five-repetition eight-hour SLO campaign, a locked baseline,
universal performance parity, or release authorization.

