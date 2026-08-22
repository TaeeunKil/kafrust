# V1-18 Resource Limits And Fuzzing

- Status: Planned
- Target evidence: CI
- Dependencies: V1-03, V1-15

## User-Visible Objective

Reject malicious or accidental oversized Kafka input before allocation and keep
all queues, decompression, sessions, tasks, and retries within documented limits
under deterministic faults and recurring fuzz campaigns.

## Non-Goals

- No claim that fuzzing proves absence of bugs.
- No removal of valid Kafka inputs merely to simplify limit handling.
- No arbitrary sleeps as concurrency proof.
- No performance optimization without V1-22 measurement.

## Scope

- `crates/kafrust-protocol/src/{codec,frame,record_batch,api}.rs`
- `crates/kafrust/src/{config,client,producer,consumer,group,share_consumer,
  streams,admin,telemetry,error}.rs`
- response frame, string/bytes/array/tagged-field, batch/record/header,
  decompressed data, request/response, metadata, partition, queue, in-flight,
  retry, cache, and task limits
- ten existing `fuzz/fuzz_targets`, corpora, crash/minimized artifacts, fuzz
  README and workflow
- scripted-broker malformed, slow, partial, and saturation cases

## Work Packages

1. Create an allocation-boundary ledger naming input, limit, validation point,
   error, and test for every externally controlled length/count.
2. Move validation before unbounded allocation and checked arithmetic before
   size sums. Where compressed input has no trustworthy decoded-size header,
   stream into a capped sink and stop immediately when decoded-byte or record
   limits are crossed.
3. Add decompression bombs, nested arrays/tags, corrupt/truncated batches,
   pathological metadata, queue saturation, retry storms, and cancellation
   leak regressions.
4. Route every stable decoder/failure boundary to an existing or new fuzz target;
   avoid target count as the success metric.
5. Track and minimize corpora; convert every crash/hang/OOM into a deterministic
   regression before closure.
6. Raise scheduled/release campaigns while preserving RSS and timeout limits.
   The current 30-second-per-target workflow is discovery smoke only: add
   sharding, job timeouts, and a versioned campaign manifest that can actually
   deliver the required 60 cumulative minutes per target before accepting its
   evidence.

## Failure And Lifecycle Contract

- Limits are checked before unbounded allocation and return typed errors.
  Decompression either validates a trustworthy declared bound or writes
  incrementally to a capped sink; it never allocates an unbounded decoded
  buffer first.
- Checked arithmetic rejects overflow and impossible negative/compact lengths.
- A malformed frame poisons the connection; one bad partition result does not
  erase safe partial results when Kafka framing remains valid.
- Queue saturation follows the owner's documented wait/fail/backpressure policy.
- Cancellation and shutdown release reserved capacity and tasks.
- Fuzz OOM, timeout, sanitizer, panic, and crash are failures with retained
  artifacts, not ignored noise.

## Verification

- Deterministic boundary tests at limit-1, limit, limit+1, maximum integer,
  overflow, negative/null, and nested/compressed cases for every ledger row.
- Codec tests include unknown/misleading decoded sizes and prove the capped sink
  stops at the configured byte/record boundary with bounded peak memory.
- All ten current fuzz targets plus any newly required target compile on the
  pinned nightly while ordinary workspace builds remain MSRV-compatible.
- Before milestone completion, each target runs at least 60 cumulative minutes
  with tracked corpora and resource bounds, with zero unresolved crash/hang/OOM;
  four consecutive weekly campaigns pass on the same protected branch line.
- The retained campaign manifest names target, corpus hash, shard, duration,
  workflow SHA, toolchain, and resource cap; configured job timeouts exceed the
  declared campaign duration.
- Fault tests verify queue/cache/task final gauges return to zero.

## Exit Criteria

1. 100% of external allocation boundaries have a pre-allocation limit and test.
2. Every stable decoder/codec/failure family is fuzz-routed.
3. Each target has 60 crash-free minutes and four consecutive weekly passes,
   with artifacts retained.
4. Every discovered issue has a minimized deterministic regression.
5. Limits, errors, config defaults, compatibility docs, and ledger records agree.

## Migration And Rollback

Limit tightening is observable and requires release/migration notes, including
how to raise a bound safely. Never roll back a limit merely to accept a
decompression bomb or overflow. Preserve minimized corpora and regressions.

## Conventional Commit Plan

1. `test(protocol): cover allocation and decompression boundaries`
2. `fix(protocol): reject oversized input before allocation`
3. `test(runtime): cover queue and cancellation saturation`
4. `ci(fuzz): extend bounded recurring campaigns`
5. `docs(limits): publish resource boundary ledger`

## Evidence Record On Completion

Record each limit/error, fuzz target/corpus hash/duration/executions/resource
bound, weekly run IDs, crash disposition, queue/cache peaks/final gauges, source
SHA, and no-proof-of-absence non-claim.
