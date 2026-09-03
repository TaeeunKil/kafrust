# V1-22 Performance And Operational SLOs

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-18, V1-20, V1-21

## User-Visible Objective

Publish reproducible throughput, latency, memory, allocation, retry, duplicate,
and backpressure budgets for representative supported workloads, and prevent
material regressions on the same controlled runners.

## Non-Goals

- No universal claim that Rust or kafrust is faster than rust-rdkafka.
- No benchmark result without exact hardware/runner, broker, configuration, and
  artifact identity.
- No optimization that weakens delivery, ordering, limits, or error semantics.
- No single-number “Kafka performance” score.

## Scope

- `crates/kafrust/src/{metrics,producer,consumer,group,share_consumer,
  telemetry}.rs`
- throughput benchmark and soak examples, benchmark/published comparison
  workflows, allocation/RSS sampling, result artifact format
- immediate and buffered producer; direct consumer; classic/KIP-848 groups;
  stable Share; transaction/read-committed; telemetry if stable
- payloads 100 B and 1 KiB; batch sizes 1, 100, and 200; concurrency 1 and 4;
  none/Gzip/Snappy/LZ4/Zstd where meaningful
- pinned-current single-node baseline and three-broker secured steady/fault
  profiles; rust-rdkafka comparison as contextual evidence only

## Work Packages

1. Pin runner class, Kafka image/config, topic partitions/replication, warmup,
   sample duration, dataset IDs, and result schema. Define each long
   steady-state profile as eight hours: two hours warmup and a six-hour
   measured window with process RSS and counters sampled every ten seconds.
2. Capture throughput, p50/p95/p99 operation latency, RSS, allocations where
   tooling permits, queue depth, retries, failures, duplicates/loss, and final
   resources.
3. Run at least five independent repetitions per required profile and report
   all samples plus median, not only the best run.
4. Lock one accepted baseline artifact/result and add regression comparison on
   equivalent runners.
5. Profile before optimizing allocations/clones/locks/tasks; retain before/after
   evidence for every performance change.
6. Publish operational tuning and backpressure guidance.

## Preparation Record (2026-08-22)

V1-22 remains `In progress` pending V1-20 and V1-21. `docs/performance.md` and the
manual benchmark workflow already record end-to-end Produce/Fetch throughput,
batch/request percentiles, retry counts, and peak/final resource gauges for
four diagnostic profiles. Those hosted-runner samples are not yet the required
five-repetition, eight-hour, ten-second-sampled SLO campaign. The preparation
manifest at
[`v1-22-performance-campaign-manifest.json`](../../evidence/v1-22-performance-campaign-manifest.json)
and `scripts/check_v1_performance_campaign_manifest.py` now enforce the timing,
profile, regression, retry, RSS, loss, duplicate, and final-gauge fields that
the later campaign must emit. No regression threshold or production claim is
being inferred from the short samples.

### Bounded current-source diagnostic benchmark (2026-08-22)

Run [32554051332](https://github.com/TaeeunKil/kafrust/actions/runs/32554051332)
used source `3fdfc778`, Kafka 4.3.1 KRaft, one partition, 2,000 records per
profile, batches of 100, and a single hosted-runner repetition. All four
profiles completed with zero retries, zero final in-flight/buffered records,
and no acknowledged loss or duplicates:

| Payload | Compression | Produce records/s | Fetch records/s | Request p50/p95/p99 |
| ---: | --- | ---: | ---: | --- |
| 100 B | none | 52,318.33 | 110,844.48 | 5/25/25 ms |
| 1 KiB | none | 48,824.97 | 162,383.04 | 5/5/5 ms |
| 10 KiB | none | 3,238.35 | 57,247.24 | 5/5/25 ms |
| 1 KiB | Zstd | 40,093.80 | 191,685.69 | 5/5/25 ms |

These values are retained as a reproducible short diagnostic artifact, not as
the five-repetition, eight-hour SLO campaign or a cross-client parity claim.

### Timed campaign harness (2026-08-22)

The `throughput_benchmark` example now supports timed campaigns through
`KAFRUST_BENCH_WARMUP_SECONDS`, `KAFRUST_BENCH_MEASURED_SECONDS`,
`KAFRUST_BENCH_SAMPLE_SECONDS`, and `KAFRUST_BENCH_WORKERS`. It synchronizes
workers at the measured-window boundary, emits JSONL samples with throughput,
request latency buckets, RSS, retries, and resource gauges, and requires the
final produced/consumed counts and gauges to reconcile. The manual
[`Kafka Benchmark Campaign Diagnostic`](../../.github/workflows/benchmark-campaign-diagnostic.yml)
workflow bounds this harness to a short Kafka 4.3.1 run (maximum ten minutes
measured) and archives the raw JSONL output. This is preparation evidence only;
the six-hour measured window, five repetitions, published artifact identity,
and regression/RSS adjudication remain open.

### Profile-path implementation (2026-08-23)

The timed harness now selects an explicit `KAFRUST_BENCH_MODE` for each
manifest profile and emits that mode in the final result. `immediate` exercises
`Producer::send_batch`, `buffered` exercises the bounded `BufferedProducer`
queue and waits on every delivery handle before consuming, and
`direct-consumer` assigns the partition and drains it through
`Consumer::poll`. The direct-consumer profile still produces its records with
the immediate producer so the measurement remains a round-trip workload; its
distinguishing path is the assigned consumer API. The result adjudicator now
requires the descriptor workload mode and final `campaign_mode` to match the
manifest profile. This removes a profile-labeling gap but does not qualify any
short run or the full matrix.

The published-artifact profile-mode diagnostic [32629657794](https://github.com/TaeeunKil/kafrust/actions/runs/32629657794)
then ran the exact `kafrust 0.3.6` dependency across Kafka 3.7.2/4.3.1 and
none/Zstd. All four buffered jobs passed with identity reconciliation and
drained gauges. Its retained descriptors remain `qualified: false`, so this
confirms published-mode compatibility only; the full V1-22 matrix and locked
baseline remain open. The evidence is recorded in
[`v1-22-performance-mode-diagnostic-2026-08-23.md`](../../evidence/v1-22-performance-mode-diagnostic-2026-08-23.md)
and ledger row `Q-PUBLISHED-V122-MODE-001`.

### Bounded timed-campaign diagnostic (2026-08-22)

Run [32558818231](https://github.com/TaeeunKil/kafrust/actions/runs/32558818231)
used source `69e4997`, Kafka 4.3.1 KRaft, two workers/two partitions, a
5-second warmup, a 20-second measured window, 5-second samples, 50-record
batches, and 1-KiB values. It roundtripped 1,546,200 records with zero failed
requests, zero retries, and zero final in-flight/buffered records. Sampled RSS
was 8,396,800–8,568,832 bytes, and the raw `benchmark-campaign.jsonl` artifact
was uploaded with 90-day retention. This confirms the current-source harness
and reconciliation path only; it is not a five-repetition, eight-hour SLO
campaign or a published-artifact result.

### Published timed-campaign diagnostic (2026-08-23)

The [`Published Performance Campaign Diagnostic`](../../.github/workflows/published-performance-campaign-diagnostic.yml)
workflow now copies the timed harness and its security helper into a fresh
external project, resolves the exact published `kafrust` version from
crates.io, and retains a descriptor with campaign identity, repetition,
lockfile hash, runner, broker image identity, workload parameters, and raw
JSONL. Run [32619372203](https://github.com/TaeeunKil/kafrust/actions/runs/32619372203)
completed a bounded 5-second warmup/20-second measurement across Kafka 3.7.2
and 4.3.1 with none/Zstd. All four jobs reconciled produced and consumed
records, had zero retries and failed requests, and drained final gauges. The
workflow explicitly marks these descriptors `qualified: false`; the full
eight-hour, five-repetition, six-profile, secured/topology matrix and baseline
comparison remain open. The complete result and failure history are recorded
in [`v1-22-performance-diagnostic-2026-08-23.md`](../../evidence/v1-22-performance-diagnostic-2026-08-23.md).

### Result-schema hardening (2026-08-23)

The timed harness now emits the manifest's aggregate result fields in its final
JSONL record: measured request latency p50/p95/p99, RSS baseline/terminal/
growth/least-squares slope with sample count, attempted/acknowledged/unknown
outcome counts, and qualified business-ID reconciliation with matching
expected/observed SHA-256 digests. The RSS windows use the first and final
thirty minutes of measured samples when available and fall back to the
available diagnostic window for short runs. Focused tests cover the identity
tracker, digest, median/window, and slope calculations. This closes a
result-shape preparation gap only; it does not qualify the long campaign or
infer thresholds from the short diagnostic.

### Result-bundle adjudicator (2026-08-23)

The preparation manifest now pins an immutable result bundle contract. Each
future qualified run contributes one `*descriptor.json` plus a relative JSONL
result file. The descriptor records the exact artifact digest, broker image,
runner, topology, security profile, workload, timing, profile ID, and
repetition. The adjudicator
[`check_v1_performance_results.py`](../../../scripts/check_v1_performance_results.py)
requires every `profile × topology × security × repetition` combination,
contiguous ten-second sample windows, one final record with zero loss/duplicate
and drained gauges, the RSS/retry thresholds, and one artifact digest across
the complete bundle. When a locked baseline is supplied, it compares the
median throughput and p99 latency for every matrix key against the manifest
budgets. A diagnostic descriptor marked `qualified: false`, an incomplete
matrix, or a missing baseline is therefore never promoted by accident.

The checker and its malformed-bundle/regression tests are exercised in normal
CI. No qualified bundle or locked baseline exists yet; this closes the
adjudication tooling gap but does not close V1-22. The current source branch is
now exercising bounded before/after diagnostics for buffered enqueue threshold
and delivery-deadline scans; those runs are retained as non-qualifying evidence
while the six-profile, five-repetition, locked-baseline campaign remains open.

### Full published campaign workflow (2026-08-23)

The manual [V1-22 Published Performance Campaign](../../.github/workflows/v1-22-performance-campaign.yml)
now materializes the complete manifest matrix: six profiles × single-node and
three-broker Kafka 4.3.1 × PLAINTEXT and SASL_SSL/SCRAM-SHA-256 × five
repetitions (120 jobs). Each job resolves a fresh external project against one
exact crates.io artifact digest, records broker/runner/lockfile identity, runs
the two-hour warmup plus six-hour measured window with ten-second JSONL samples,
and uploads a relative result/descriptor pair. The aggregate job runs the
existing adjudicator and can require a checked-in locked baseline through
require_baseline=true; without that baseline it deliberately reports
matrix-complete-baseline-pending. The workflow now defaults to the
`self-hosted` label and rejects GitHub-hosted labels, because the eight-hour
contract cannot fit the [hosted six-hour job limit](https://docs.github.com/en/actions/reference/limits).
The repository currently has
no self-hosted runner registered, so no campaign has been dispatched and this
remains an external-capacity gate.

This adds an executable qualification path but no campaign has been dispatched
yet, so V1-22 remains In progress and no SLO, baseline, competitor-parity, or
release claim follows from the workflow definition alone.

### Long-campaign capacity re-audit (2026-08-24)

The repository runner inventory still reports zero registered self-hosted
runners, and the workstation has no Docker executable for a local substitute.
Exact-head CI [32646817241](https://github.com/TaeeunKil/kafrust/actions/runs/32646817241)
passed the six-profile manifest, result adjudicator, and hosted-label guard.
The eight-hour matrix therefore remains an external-capacity gate rather than
a shortened or hosted-runner claim. The audit is retained in
[`v1-long-campaign-capacity-audit-2026-08-24.md`](../../evidence/v1-long-campaign-capacity-audit-2026-08-24.md).

### Company self-hosted capacity recovery (2026-09-03)

The company WSL2 runner was recovered without lowering the campaign contract.
After removal of the explicitly authorized stale backup, the unchanged guard
reported 736 GiB free on `/mnt/t` and 854 GiB free under `/var/lib/docker`.
The `wsl-ubuntu-t9` runner was online and idle, and a published `0.3.6`
three-broker short diagnostic passed in
[33716428169](https://github.com/TaeeunKil/kafrust/actions/runs/33716428169).
This clears the host preflight for a future campaign dispatch only. The full
120-job matrix still requires five repetitions, two-hour warmup, six-hour
measurement, ten-second samples, and a locked baseline; no V1-22 SLO result is
claimed from the short diagnostic.

### WSL2 capacity activation follow-up (2026-08-24)

The follow-up WSL2 preflight registered one `wsl-ubuntu-t9` runner with the
`self-hosted`, `Linux`, `X64`, `docker`, and `wsl2` labels. A 60-second
non-qualification diagnostic passed after the host's missing `python` and
`jq` utilities were installed. The first exact V1-21 campaign,
`pinned-secured-six-hour-1` ([run 32649020906](https://github.com/TaeeunKil/kafrust/actions/runs/32649020906)),
is now running on that runner from the published `0.3.6` artifact. This
registration clears the execution-path prerequisite but does not qualify
V1-21 or V1-22: the V1-21 descriptor and adjudicator remain pending, and the
single runner would serialize V1-22's 120-job matrix. Additional performance-
isolated runner capacity is therefore still required before dispatching the
full V1-22 campaign; no matrix, duration, or baseline requirement is reduced.

### Competitor review and re-plan (2026-08-23)

The companion published comparison
[32619987006](https://github.com/TaeeunKil/kafrust/actions/runs/32619987006)
measured `kafrust 0.3.6` at roughly 50.0% of rust-rdkafka Produce throughput
and 46.2% of Consume throughput for the recorded 20,000-record profile, while
business-record reconciliation remained exact. This is not a universal parity
threshold, but it is a material signal for the release decision. V1-22 is
therefore re-planned to profile batching, request concurrency, allocation and
queue behavior on the target workload, preserve before/after evidence for any
optimization, and rerun the six-profile/five-repetition SLO campaign before
V1-25 RC review. No `0.3.7` or `1.0.0` publication is justified by the
comparison alone.

The five-repetition follow-up
[32626740940](https://github.com/TaeeunKil/kafrust/actions/runs/32626740940)
confirmed the signal from the exact published pair: median kafrust throughput
was 77,157 Produce and 286,135 Consume records/s versus 171,065 and 501,450
for rust-rdkafka 0.39.0, or 45.1% and 57.1%. All ten rows reconciled 20,000
unique records with zero loss/duplicates and one shared payload digest. The
raw result and non-claims are in
[`v1-22-published-competitor-comparison-2026-08-23.md`](../../evidence/v1-22-published-competitor-comparison-2026-08-23.md).
This strengthens the comparison evidence but does not close V1-22's SLO,
baseline, or optimization gates.

### Published comparison refresh (2026-09-03)

The exact published `kafrust 0.3.6` versus `rust-rdkafka 0.39.0` comparison was
rerun in [33718060135](https://github.com/TaeeunKil/kafrust/actions/runs/33718060135)
with 20,000 unique 1-KiB records, batch size 200, and three repetitions per
implementation on Kafka 4.3.1. All six rows reconciled zero loss and
duplicates with one shared digest. Median kafrust throughput was 81,174.44
Produce and 316,018.06 Consume records/s versus 155,028.38 and 608,580.00 for
rust-rdkafka (52.36% and 51.93% for this workload). This confirms a material
workload-specific gap to drive profiling; it is not an SLO, locked baseline,
universal parity claim, or release authorization. Details are in
[`v1-23-published-competitor-comparison-2026-09-03.md`](../../evidence/v1-23-published-competitor-comparison-2026-09-03.md).

### Published performance smoke and fixture correction (2026-09-03)

The short published smoke was first run in
[33718369244](https://github.com/TaeeunKil/kafrust/actions/runs/33718369244).
All client roundtrips completed, but the external fixture failed its gauge
assertion because two JSON snapshot arguments were emitted under the wrong
field names. Commit `6d5d7ec` corrected that test-fixture-only mapping. The
four-profile rerun
[33718664874](https://github.com/TaeeunKil/kafrust/actions/runs/33718664874)
passed on Kafka 3.7.2/4.3.1 with none/Zstd: 10,000 records per profile, zero
retries, `in_flight_requests=0`, and `buffered_records=0`. Batch p99 ranged
from 18.782ms to 40.051ms. This is a corrected smoke diagnostic, not the
five-repetition eight-hour matrix or a locked SLO baseline. Full values and
the retained failure are in
[`v1-published-performance-smoke-2026-09-03.md`](../../evidence/v1-published-performance-smoke-2026-09-03.md).

### Profiling path (2026-08-23)

The manual [`Kafka Benchmark Profile Diagnostic`](../../.github/workflows/benchmark-profile-diagnostic.yml)
workflow now retains four comparable source profiles (immediate with one and
four workers, buffered with four workers, and direct-consumer with one worker)
with runner/kernel/CPU identity, `/usr/bin/time -v` counters, optional Linux
`perf stat` counters, and the reconciled timed JSONL result. Its descriptors
are explicitly `qualified: false`; the workflow establishes the before/after
evidence required by the batching, concurrency, allocation, and queue re-plan.
No optimization or locked baseline is inferred until it produces a retained
pair on equivalent runners.

The first retained before/after pair is documented in
[`v1-22-performance-profile-before-after-2026-08-23.md`](../../evidence/v1-22-performance-profile-before-after-2026-08-23.md).
Concurrent buffered delivery waits reduced the timed process's context
switches by approximately 73% while throughput remained approximately 13.5k
records/s and identity/final-gauge checks stayed green. This is a bounded
resource improvement only; the next profile slice must investigate repeated
buffered encoded-size and queue-batching work before another optimization.

The second source pair (before workflow
[32631000062](https://github.com/TaeeunKil/kafrust/actions/runs/32631000062),
after workflow
[32631701269](https://github.com/TaeeunKil/kafrust/actions/runs/32631701269))
removed `ProducerRecord` clones from the repeated buffered encoded-size check
while preserving its wire-length calculation. The buffered profile increased
from 13,480 to 18,283 records/s and p99 decreased from 25 ms to 10 ms in the
bounded 60-second pair; context switches increased in the after run. This is
promising source-profile evidence, not a locked baseline, SLO qualification,
competitor parity result, or publication authorization. The retained details
are in the same evidence record. A replication of the same source code in
[32632004251](https://github.com/TaeeunKil/kafrust/actions/runs/32632004251)
measured 16,217 buffered records/s with p99 10 ms but 1.83M context switches.
The two after runs remain above the predecessor's 13,480 records/s, while the
variation demonstrates that a controlled repetition set is required before
accepting a baseline or claiming a release-relevant percentage improvement.

The next source optimization (`07705c5`) narrows the buffered enqueue
threshold scan to the newest request's topic/partition group, preserving the
existing flush and encoded-byte semantics while avoiding repeated work for
unaffected groups. Same-source repetitions
[32634691272](https://github.com/TaeeunKil/kafrust/actions/runs/32634691272) and
[32634877270](https://github.com/TaeeunKil/kafrust/actions/runs/32634877270)
both reconciled all four profiles with zero retries, unknown outcomes,
loss/duplicates, and drained gauges. Buffered throughput was 15,403 and
16,203 records/s with p99 10 ms; context switches were 378,946 and 1,349,630.
The surrounding throughput/resource variation means this is retained as
semantics-preserving diagnostic evidence only. It does not lock a baseline,
qualify an SLO, or close the six-profile/five-repetition eight-hour campaign.

Commit `866509a` then reuses the maintained oldest pending enqueue timestamp for
the buffered delivery-timeout wake-up, avoiding a full queue minimum scan on
each select-loop iteration. Combined source profile
[32635871875](https://github.com/TaeeunKil/kafrust/actions/runs/32635871875)
reconciled all four profiles with zero retries, unknown outcomes, loss,
duplicates, and drained gauges. Buffered throughput was 15,977 records/s with
p99 10 ms and 39,124 KiB maximum RSS. Because this run includes both queue
changes and remains within hosted-runner variation, it is diagnostic evidence,
not a stable percentage, locked baseline, SLO qualification, or release gate.

## Failure And Lifecycle Contract

- Benchmark backpressure uses the same bounded production queues and reports
  wait/rejection behavior.
- Measurement cancellation/failure retains partial artifacts and does not count
  as a passing zero result.
- Planned outage windows are separated from steady-state retry/error ratios.
- No acknowledged loss is tolerated; duplicates follow the workload's stated
  non-idempotent/idempotent contract.
- Final queues/in-flight/tasks must drain after measurement.

## Verification

Minimum quantitative gates on equivalent runners:

- five repetitions for each required 100 B/1 KiB, batching, concurrency, codec,
  and client-family profile selected in the benchmark manifest;
- median throughput regression no worse than 20% and p99 latency regression no
  worse than 25% versus the locked accepted baseline, unless a reviewed
  correctness/security tradeoff updates the baseline before release;
- during the final six hours of each long steady-state run, RSS growth is no
  more than `max(64 MiB, 10% of steady-state RSS)`: baseline RSS is the median
  of the first measured 30 minutes, terminal RSS is the median of the final 30
  minutes, and both terminal-minus-baseline and least-squares slope extrapolated
  across six hours must stay within that bound;
- outside manifest-declared outage windows (injection until the recorded
  readiness predicate recovers), retry ratio is `retry attempts / all protocol
  attempts` and is at most 1%; warmup and fault-window samples remain archived
  but are excluded from that steady-state ratio. Unaccounted
  acknowledged loss is zero, idempotent/transaction duplicate count is zero,
  and final resource gauges are zero;
- rust-rdkafka comparison uses identical workload inputs and is labeled
  informational, with no pass/fail parity threshold.

## Exit Criteria

1. Benchmark manifest/result schema and runner identity are reproducible,
   including eight-hour timing, ten-second sampling, readiness/fault windows,
   RSS formulas, and retry numerator/denominator.
2. All required profiles have five samples and published distributions.
3. Regression, RSS, retry, loss/duplicate, and final-resource thresholds pass.
4. Every optimization has measurement evidence and preserves correctness tests.
5. `docs/performance.md`, operational guidance, compatibility non-claims, and
   ledger rows are updated.

## Migration And Rollback

Publish configuration mapping for batching, linger, delivery/request timeouts,
queue capacity, compression, fetch limits, and concurrency. Roll back a
performance change if it crosses the locked budget or correctness gates; retain
the data explaining the decision.

## Conventional Commit Plan

1. `test(perf): make workload results reproducible`
2. `perf(runtime): improve measured bottleneck`
3. `ci(perf): enforce operational regression budgets`
4. `docs(perf): publish v1 baselines and tuning`

## Evidence Record On Completion

Record artifact, commit, hardware/runner, broker/topology/security, full
configuration, repetitions, raw and aggregate throughput/latency/RSS/allocation/
retry/loss/duplicate/queue results, baseline comparison, and universal-parity
non-claim.
