# V1-18 Resource Limits And Fuzzing

- Status: In progress
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

## Current Execution Record (2026-08-22; refreshed 2026-08-23)

V1-18 is now `In progress`. The current decoder path rejects negative, overflow,
truncated, and over-limit frame/string/bytes/array/tagged-field lengths before
allocation. `DecodeLimits` bounds decoded arrays and decompressed record-batch
bytes, while the client checks the response-frame ceiling before allocating the
response buffer. Deterministic compression tests cover declared and observed
output limits for Snappy, Gzip, LZ4, and Zstd; configuration tests reject zero
resource limits before network access; and scripted fault tests cover malformed
data-plane responses and bounded queue saturation.

All ten fuzz targets compile and run from the standalone nightly workflow, but
the discovery workflow grants only a 30-second smoke per target. The
2026-08-22 discovery run [32555867720](https://github.com/TaeeunKil/kafrust/actions/runs/32555867720)
passed all ten targets and retained corpus/crash artifacts; it is recorded as
an in-progress discovery row in the
[qualification ledger](../../evidence/qualification-ledger.md). The versioned
campaign manifest at
[`v1-18-fuzz-campaign-manifest.json`](../../evidence/v1-18-fuzz-campaign-manifest.json)
and `scripts/check_v1_fuzz_campaign_manifest.py` now make the distinction
executable: discovery remains 30 seconds, while qualification requires 3,600
seconds per target, four shards, a 70-minute job timeout, and four weekly
campaigns. The dedicated
[`fuzz-qualification.yml`](../../../.github/workflows/fuzz-qualification.yml)
workflow now runs that matrix on manual dispatch and weekly schedule, records
per-target/per-shard hashes and final statistics, and uploads crash/OOM
artifacts. Each shard runs 900 seconds, giving each target 3,600 cumulative
seconds across four shards while avoiding an accidental four-hour-per-target
overrun. The first full qualification campaign
[32561454977](https://github.com/TaeeunKil/kafrust/actions/runs/32561454977)
from source `c2c75aa23dc962faad7f33f22cd15a96303d4b56` passed all ten targets
and four shards. All 40 qualification records declared 900 seconds, the
3,600-second cumulative target budget, nightly toolchain, 2,048 MB RSS cap,
and a passing result; their 40 corpus hashes and target/shard pairs were
verified, with no crash/OOM artifact files. This closes one qualifying
campaign set only. Three additional weekly campaign passes and retained
crash/OOM disposition remain open, so V1-18 stays `In progress`. Existing fuzz
success is still not treated as absence-of-bug evidence.

The second full qualification campaign
[32635558822](https://github.com/TaeeunKil/kafrust/actions/runs/32635558822)
from source `e90fc6c3d02c527bdabc350837ee53a6c95b20fc` also passed all ten
targets and four shards. The 40 downloaded qualification records again
declared 900 seconds per shard, 3,600 cumulative seconds per target, nightly
toolchain, 2,048 MB RSS cap, and a passing result; the checker verified every
target/shard pair and corpus SHA-256, with no crash/OOM artifact files. The
manifest now records two of the required four weekly campaign sets, leaving
two additional passes plus retained crash/OOM disposition and the other
resource-limit gates open. This is additional qualification evidence, not an
absence-of-bugs or V1-18 completion claim.

The checked-in
[`check_v1_fuzz_qualification_artifacts.py`](../../scripts/check_v1_fuzz_qualification_artifacts.py)
reproduces the artifact audit, including all 40 target/shard records, the
900-second shard and 3,600-second cumulative budgets, workflow SHA, RSS cap,
corpus hashes, and crash/OOM-file absence.

Two duplicate manual dispatches (`32645221503` and `32645222921`) remained
queued for more than nine hours and were cancelled without starting artifact
collection. They are not campaign sets: the remaining requirement is two
additional weekly scheduled passes with retained crash/OOM disposition.

### Checked allocation-boundary ledger (2026-09-03)

The reviewed allocation boundaries are now captured in
[`v1-18-allocation-boundary-ledger.json`](../../evidence/v1-18-allocation-boundary-ledger.json)
and checked by [`check_v1_allocation_boundary_ledger.py`](../../../scripts/check_v1_allocation_boundary_ledger.py).
The 14 entries cover response frames, collection/string/bytes/tag lengths,
varints, message sets, compressed batches, consumer budgets and queues,
producer batch/buffer limits, and telemetry payloads. Each entry names its
source file, limit, validation point, typed failure, allocation behavior, and
focused test. The checker and five tests pass. This closes only the reviewed
static ledger slice; full fuzz duration, recurring weekly passes, and any
unreviewed boundary remain open.

### Company local fuzz preflight (2026-09-03)

The company Windows host cannot link libFuzzer targets with its MSVC nightly
toolchain (`LNK1561`), and Ubuntu-T9 WSL2 has no Linux nightly installed. The
WSL toolchain refresh was blocked before compilation by temporary DNS failure
against `static.rust-lang.org`. No target or corpus was run or changed; this is
retained as a prerequisite blocker only in
[`v1-local-fuzz-preflight-2026-09-03.md`](../../evidence/v1-local-fuzz-preflight-2026-09-03.md).
The two required weekly campaign sets, crash/OOM disposition, and remaining
resource-limit gates are unaffected.

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
