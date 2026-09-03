# Published Performance Campaign Diagnostic (2026-09-03)

The published kafrust 0.3.6 artifact completed the bounded performance
diagnostic in [run 33720136913](https://github.com/TaeeunKil/kafrust/actions/runs/33720136913)
from source e89e4d6. Four fresh external projects resolved the exact registry
artifact on Rust 1.81 and ran a 5-second warmup plus 20-second measured window
with two workers, 1-KiB payloads, and batch size 50.

| Kafka | compression | produced/consumed | produce requests | p50/p95/p99 ms | RSS slope bytes/s |
| --- | --- | ---: | ---: | --- | ---: |
| 3.7.2 | none | 1,459,450 / 1,459,450 | 58,378 | 1 / 1 / 5 | 4,423 |
| 3.7.2 | Zstd | 1,019,700 / 1,019,700 | 40,788 | 1 / 5 / 5 | 5,571 |
| 4.3.1 | none | 1,438,550 / 1,438,550 | 57,542 | 1 / 1 / 1 | 491 |
| 4.3.1 | Zstd | 1,027,850 / 1,027,850 | 41,114 | 1 / 5 / 5 | 15,073 |

All four profiles reported zero failed requests, zero retries, zero unknown
outcomes, zero loss, zero duplicates, matching record digests, and zero final
in-flight/buffered records. The retained descriptors mark the run as
diagnostic and qualified=false. The four artifact directories contain the
JSONL result and descriptor for each broker/compression pair; all lockfiles
resolved kafrust 0.3.6 and share SHA-256
5ffa8147a2c178d0eee0c060aecd9bde278615b5fc5645b9d390737241c36fd4.

This is a workload-specific published-artifact diagnostic for profiling and
regression detection. It is not the V1-22 five-repetition eight-hour SLO
campaign, a locked baseline, universal performance parity, or release
authorization.

