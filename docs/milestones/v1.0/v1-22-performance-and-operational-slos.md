# V1-22 Performance And Operational SLOs

- Status: Planned
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

V1-22 remains `Planned` pending V1-20 and V1-21. `docs/performance.md` and the
manual benchmark workflow already record end-to-end Produce/Fetch throughput,
batch/request percentiles, retry counts, and peak/final resource gauges for
four diagnostic profiles. Those hosted-runner samples are not yet the required
five-repetition, eight-hour, ten-second-sampled SLO campaign; no regression
threshold or production claim is being inferred from them.

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
