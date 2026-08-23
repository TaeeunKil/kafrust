# V1-22 Published Performance Campaign Diagnostic (2026-08-23)

Status: diagnostic evidence only; `qualified: false`.

The new [`published-performance-campaign-diagnostic.yml`](../../.github/workflows/published-performance-campaign-diagnostic.yml)
workflow executes the timed `throughput_benchmark` harness from a fresh external
project against the exact crates.io `kafrust = "=0.3.6"` artifact. It retains
the generated lockfile hash, runner identity, broker image reference and image
ID, campaign/repetition identity, raw JSONL samples, and a descriptor. The
workflow is deliberately bounded to a maximum 60-second warmup and 300-second
measurement window; it is not permitted to claim the V1-22 eight-hour gate.

## Final bounded run

Run: [32619372203](https://github.com/TaeeunKil/kafrust/actions/runs/32619372203)

- source workflow commit: `fea58ebe137bc167209544c5e11ce6fd425da70d`
- artifact: `kafrust 0.3.6`, lockfile SHA-256
  `3dde24b9cc1a1b0d0c63e3f67b723f5db0fcdb886f4e0c3fe41c93817da0759c`
- runner: GitHub-hosted `Linux/X64`
- workload: two workers/two partitions, 5-second warmup, 20-second measured
  window, 5-second samples, 50-record batches, 1-KiB values
- matrix: Kafka 3.7.2 and 4.3.1; no compression and Zstd
- each job emitted four measured samples and a final record
- every job reconciled produced and consumed records, reported zero failed
  requests/retries, and ended with zero in-flight and buffered records

| Kafka | Compression | Produced/consumed | Approx. records/s | Final RSS |
| --- | --- | ---: | ---: | ---: |
| 3.7.2 | none | 1,860,300 | 93,015 | 8,114,176 B |
| 3.7.2 | Zstd | 1,035,600 | 51,780 | 10,833,920 B |
| 4.3.1 | none | 1,662,100 | 83,105 | 8,355,840 B |
| 4.3.1 | Zstd | 1,030,950 | 51,547.5 | 10,924,032 B |

The four retained descriptors and JSONL result files are available in the run's
90-day artifacts. The short samples are useful for checking published-artifact
execution and result shape, but they cannot establish RSS slope, regression
budgets, or steady-state retry limits over six hours.

The current timed harness has since tightened the result contract: measured
records carry deterministic partition/sequence business IDs, and the final
JSONL row now reports attempted, acknowledged, consumed, unknown, loss, and
duplicate counts plus matching expected/observed SHA-256 identity digests.
The historical artifact above predates those fields; no historical diagnostic
is retroactively promoted to `qualified: true`.

## Failure history retained

- [32619156690](https://github.com/TaeeunKil/kafrust/actions/runs/32619156690)
  reached the external compile step but exposed missing `tracing` and
  `tracing-subscriber` dependencies in the fixture template.
- [32619249449](https://github.com/TaeeunKil/kafrust/actions/runs/32619249449)
  completed the timed runs but the descriptor step assumed Docker
  `RepoDigests`, which is absent for the runner's locally tagged Kafka image.
- The fixture now declares its tracing dependencies and records the stable
  broker image reference plus image ID instead of treating a missing remote
  digest as a passing identity.

## Remaining V1-22 gates

The manifest still requires six named profiles, both single-node and three-
broker secured topologies, five repetitions, two-hour warmup plus six-hour
measurement, ten-second samples, RSS baseline/terminal/slope adjudication,
regression comparison against a locked baseline, and explicit loss/duplicate
and final-gauge checks. None of those gates is closed by this diagnostic.
