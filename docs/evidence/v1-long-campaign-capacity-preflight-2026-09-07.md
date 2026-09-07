# V1 Long-Campaign Capacity Preflight — 2026-09-07

## Scope and boundary

This is a read-only workstation preflight for the rate-limited lifetime
diagnostic and the exact V1-21/V1-22 campaign prerequisites. It is not a
campaign result, does not qualify V1-21 or V1-22, and does not authorize a
release.

The checked source head was `67c1373` (`docs(fuzz): record current head
discovery`) and the working tree was clean at observation time.

## Observed host state

Observation time: `2026-09-07 10:59:19 +09:00`.

| Resource | Observation |
| --- | --- |
| Windows `T:` free space | `736.80 GiB` |
| Windows `T:` used/total | `194.70 / 931.50 GiB` |
| Windows `C:` free space | approximately `169.4 GiB` |
| Windows physical RAM | `31.3 GiB` total, `7.7 GiB` free |
| `Ubuntu-T9` WSL state | `Stopped` |
| GitHub runner `wsl-ubuntu-t9` | `offline`, `busy=false` |
| Runner labels | `self-hosted,Linux,X64,docker,wsl2` |
| Docker root free space | not observable while WSL is stopped |

The storage figure is sufficient for the prepared two-hour diagnostic's
40-GiB watermark from a host-volume perspective. The broker container budget
remains 3 CPU / 6 GiB (three brokers capped at 1 CPU and 2 GiB each), so the
observed 7.7 GiB Windows free-RAM margin is narrow until WSL and Docker are
running. Docker-root capacity and live memory usage must be rechecked after an
authorized WSL start.

## Decision

No workflow was dispatched. The prepared diagnostic remains the only sensible
small long-run profile: three brokers, RF3, 1,000 records/s, 256-byte values,
run-scoped cleanup, a fixed broker restart, and an abort below the configured
watermark. At this rate the replicated-data lower bound is about 5.2 GiB for
two hours, before Kafka indexes and filesystem overhead.

Before any run, the operator must restore WSL, bring the registered runner
online, verify Docker-root capacity and memory, and keep the workstation awake
for the complete bounded run. A successful diagnostic would remain
`qualified=false` and would not satisfy the exact six-hour 10,000-record/s,
1-KiB V1-21 workload or any V1-22 SLO gate.

