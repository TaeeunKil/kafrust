# Bounded workstation follow-up campaign

These are local lifetime/recovery diagnostics, not V1-21 high-load or V1-22
SLO qualification. The long phases must finish before their results can be
claimed. No canary or complete live CI matrix result is implied.

## Inputs and checks

- Windows host with Ubuntu-T9 WSL and run-scoped Apache Kafka containers.
- Three brokers, each capped at one CPU and 2 GiB memory; rotating Docker logs.
- One phase at a time. Each active phase checks a 20 GiB filesystem-growth
  abort threshold and a 100 GiB free-space reserve every ten seconds.
- OS resource samples include helper process-tree RSS, OS threads, file
  descriptors, sockets, broker memory, and filesystem free space.
- Windows source validation passed all six required commands on Rust 1.81.0;
  workspace tests reported 892 passes, including documentation tests.
- Immutable Linux snapshot checks passed 37 Python tests, including real
  POSIX process-group cleanup with a TERM-ignoring descendant and fake Docker
  checks that reject global prune operations.

## First smoke and bounded failure

Snapshot `37bfd2255a955c416383ecda8095cc772357a25e` is retained under
`/home/taeeun/kafrust-campaigns/bounded-followup-20260908/source`.
Windows artifacts are `.artifacts/bounded-followup-20260908/`.

The SASL/TLS smoke passed classic leave, classic drop, and KIP-848 leave,
each with two cycles. KIP-848 drop then failed the exact all-partition recovery
assertion. The runner stopped, recorded failure, cleaned its resources, and
did not start either long soak.

The helper used 80 poll attempts as a departure-recovery bound. Kafka's
consumer protocol uses a broker-controlled session timeout, whose default is
45 seconds, rather than the classic client's six-second setting. The helper
now polls until a 60-second elapsed deadline while retaining the same exact
all-partition assertion. Broker defaults and the assertion were not relaxed.
See [Apache Kafka group configuration](https://kafka.apache.org/43/configuration/group-configs/).

## Second execution

Snapshot `2212fb0c79cbf0693de7f3dc607d09a21b854aad` includes the helper deadline
correction. It is retained under
`/home/taeeun/kafrust-campaigns/bounded-followup-20260908-attempt2/source`.
Windows artifacts are `.artifacts/bounded-followup-20260908-attempt2/`.

This execution first reruns KIP-848 drop twice, then runs SASL/TLS for 180
seconds at 100 records/s and plaintext for 180 seconds at 25 records/s. Both
soaks restart one broker and require recovery, exact record-ID reconciliation,
and zero final request/buffer gauges. Standalone helper tests follow.

Only if those checks pass does the runner start the full sequence:

1. Classic leave and drop, 100 cycles each on Kafka 3.7.2.
2. KIP-848 leave and drop, 100 cycles each on Kafka 4.3.1.
3. SASL/TLS for six hours at 100 records/s, 64-byte values, Kafka 4.3.1.
4. Plaintext for 24 hours at 25 records/s, 64-byte values, Kafka 4.3.1.

At this record's creation, the second execution is still in smoke validation.
Use its `smoke/campaign-state.json`, `long/campaign-state.json`, per-phase
results/provenance, resource JSONL, and Windows holder exit status to determine
the actual outcome. Planned durations and profile entries are not completion
evidence.
