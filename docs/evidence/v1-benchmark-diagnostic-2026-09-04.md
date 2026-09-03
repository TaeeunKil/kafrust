# Current-source benchmark diagnostics (2026-09-04)

- source_commit: `ce4719b17dc1f62cc8d5ee46a56a1d7b61493e6f`
- broker: Kafka 4.3.1 single-node KRaft
- evidence level: CI diagnostic; descriptors are explicitly non-qualifying

## Profile paths

[Kafka Benchmark Profile Diagnostic run 33799886253](https://github.com/TaeeunKil/kafrust/actions/runs/33799886253)
completed all four source profiles on hosted Linux runners:

- immediate, one worker, 1-KiB payload, batch 200;
- immediate, four workers, 1-KiB payload, batch 200;
- buffered, four workers, 1-KiB payload, batch 200; and
- direct-consumer, one worker, 1-KiB payload.

Each job produced a reconciled final JSONL record, drained in-flight and
buffered gauges, and uploaded runner/resource/profile artifacts. The workflow
marks these descriptors `qualified: false` because they are short profiling
inputs rather than the locked V1-22 SLO bundle.

## Timed campaign path

[Kafka Benchmark Campaign Diagnostic run 33799889688](https://github.com/TaeeunKil/kafrust/actions/runs/33799889688)
completed a bounded immediate campaign with two workers, batch size 100,
1-KiB payloads, no compression, 2-second warmup, 10-second measurement, and
5-second samples. The final record reconciled 871,900 produced and consumed
records with zero failed requests, retries, unknown outcomes, loss, or
duplicates. Request latency was p50 1 ms, p95 5 ms, and p99 5 ms; RSS was
10,674,176 bytes at both baseline and terminal with zero measured growth; final
in-flight and buffered gauges were zero.

## Boundary

These are bounded source diagnostics for profiling and harness reconciliation.
They are not the required six-profile, five-repetition, eight-hour published
SLO campaign, a locked baseline, a universal performance ranking, or release
authorization.
