# 0.5 local half-budget profile

Status: **verified**.

This evidence records the completed workstation-sized 0.5 profile. The run
was `r05hb09`, with profile `windows-wsl-32g-half-budget`, and output rooted at
`T:\\kafrust-campaigns\\r05hb09` (`/mnt/t/kafrust-campaigns/r05hb09` in
Ubuntu-T9 WSL). The campaign completed on 2026-09-10 with all six phases
completed and no run-scoped processes or Docker containers left behind.

## Declared envelope

- Windows host with Ubuntu-T9 WSL; one active phase at a time.
- WSL soft working-set budget: 8 GiB on a host with about 32 GiB RAM.
- Aggregate broker CPU budget: 4 vCPU; three Kafka brokers capped at 1.0 CPU
  and 2 GiB memory each, with a 512 PID limit and rotating 50 MiB logs.
- Active-phase disk reserve: 100 GiB; growth guard: 20 GiB.
- Churn phases use Kafka 3.7.2 for classic groups and Kafka 4.3.1 for
  KIP-848 groups. Both soak phases use Kafka 4.3.1 and three replicated
  partitions.

## Ordered phase results

| Phase | Broker/workload | Result | Reconciliation and recovery |
| --- | --- | --- | --- |
| Classic leave | Kafka 3.7.2, 100 cycles, 6 partitions | `protocol=Classic`, `exit=leave`, passed | Group-churn phase; record counters not applicable |
| Classic drop | Kafka 3.7.2, 100 cycles, 6 partitions | `protocol=Classic`, `exit=drop`, passed | Group-churn phase; record counters not applicable |
| KIP-848 leave | Kafka 4.3.1, 100 cycles, 6 partitions | `protocol=Consumer`, `exit=leave`, passed | Group-churn phase; record counters not applicable |
| KIP-848 drop | Kafka 4.3.1, 100 cycles, 6 partitions | `protocol=Consumer`, `exit=drop`, passed | Group-churn phase; record counters not applicable |
| Secure soak | Kafka 4.3.1 SASL/TLS, 6h, 50 records/s, 64-byte values | 21,601.479 s; 1,080,150 attempted/acknowledged/consumed-unique | loss 0, duplicate 0, unknown 0; `recovered=true`; final in-flight 0, buffered 0; reconciliation qualified, digest `60bb98c0dd2522563bfd669022df84bb55a9d4004b6a7383ea342da2b0eb7782` |
| Plaintext soak | Kafka 4.3.1 plaintext, 12h, 12 records/s, 64-byte values | 43,208.209 s; 518,550 attempted/acknowledged/consumed-unique | loss 0, duplicate 0, unknown 0; `recovered=true`; final in-flight 0, buffered 0; reconciliation qualified, digest `a017569b0cd72aa692d16d0ff6a8942be35af4a28d2578ed8ed644d82149f545` |

The secure phase recorded 43,274 requests, 4 failed requests, and 7 retries.
The plaintext phase recorded 20,773 requests, 6 failed requests, and 9
retries. Both phases reported zero operation errors and one broker restart
with successful recovery.

The plaintext helper is a diagnostic workload. Its
`local-lifetime-descriptor.json` deliberately marks `qualified=false` and
records that it is not V1-21, V1-22, published-artifact, service-canary, or
release-authorization evidence.

## Resource traces

The sampler ran every 10 seconds. RSS is for the helper process tree. The
`OS threads` column is an operating-system thread count; it is not a Tokio
task count.

| Phase | Samples (UTC range) | Helper RSS (MiB) | OS threads | Open sockets | `/mnt/t` free minimum |
| --- | --- | ---: | ---: | ---: | ---: |
| Classic leave | 52 (`06:34:43`–`06:47:41`) | 8.61–16.23 | 19–20 | 1–16 | 736.54 GiB |
| Classic drop | 52 (`06:50:25`–`07:03:24`) | 8.81–15.29 | 19–20 | 1–16 | 736.54 GiB |
| KIP-848 leave | 29 (`07:05:36`–`07:12:42`) | 8.66–15.21 | 19–20 | 1–14 | 736.54 GiB |
| KIP-848 drop | 321 (`07:14:40`–`08:36:31`) | 8.59–15.25 | 19–20 | 1–14 | 736.54 GiB |
| Secure soak | 1,414 (`08:38:13`–`14:51:25`) | 7.66–9.53 | 18–19 | 1–8 | 736.54 GiB |
| Plaintext soak | 2,830 (`14:53:41`–`03:41:21`) | 6.35–6.88 | 18–19 | 1–8 | 736.51 GiB |

The plaintext Docker free-space minimum was 845.26 GiB; the final Windows
capacity check reported 736 GiB on `T:` and 847 GiB at `/var/lib/docker`.
Every phase stayed above the 100 GiB reserve and below the 20 GiB growth
guard. Recorded estimated growth was 2.00 GiB for each churn phase, 8.57 GiB
for secure soak, and 5.15 GiB for plaintext soak.

## Source and artifact provenance

The root `campaign-provenance.json` and the per-phase provenance files identify
source commit `de9ae74d928f8974dbcc9277d8f557c8cebe25ee`, source mode `local`,
and source root `/mnt/c/Users/user/Documents/New project 4`. They also record
`working_tree_dirty=true`, so this run is retained as evidence of the tested
working tree rather than as an immutable release artifact. The plaintext phase
has no separate per-phase provenance file; its
`local-lifetime-descriptor.json` carries the same source commit and is paired
with the root provenance file.

The Windows holder exited with code 0 at `2026-09-10T12:42:04+09:00`.
`campaign-state.json` is `status=completed`, and the run-scoped Docker
container query is empty after teardown.

## Qualification boundary

This is supplemental 0.5 profile evidence for a half-budget local envelope. It
does not satisfy V1-21 high-load evidence, V1-22 SLO evidence, the full fault
matrix, published-artifact evidence, service-canary evidence, or release
authorization. Those gates remain defined by the V1 milestone documents.
