# Release Preparation

kafrust publishes two crates:

- `kafrust-protocol`: Kafka wire-format primitives and request/response types
- `kafrust`: the user-facing async Kafka client

Publish `kafrust-protocol` before `kafrust` because the client crate depends on the protocol crate by version.

## Versioning

The public alpha line starts at `0.1.0`. Until the protocol and runtime behavior stabilize, keep public API additions small and document alpha limits in the affected API direction document.

Before publishing:

1. Update both crate versions together.
2. Update the `kafrust-protocol` dependency version in `crates/kafrust/Cargo.toml`.
3. Update roadmap status and any user-facing API direction document affected by the release.
4. Keep `Cargo.lock` out of the commit unless the repository policy changes.

Never reuse a version that already exists on crates.io. The client and protocol
crates must use the same new patch version, and the protocol crate must be
published first. A local workspace build can pass while an isolated client
package still resolves an older published protocol crate, so the ordered
registry checks below are part of the release gate.

## Release Notes

Every GitHub release should use a consistent structure so downstream users can
evaluate alpha risk without reading the full diff. Use `None` explicitly when a
section does not apply.

```md
## Summary

- What changed for users.

## Breaking changes

- Renamed, removed, or behavior-changing public APIs.
- Changed defaults, feature flags, environment variables, or broker assumptions.

## Migration notes

- Old API or behavior.
- Replacement API or behavior.
- Required caller changes.

## Compatibility evidence

- Broker versions and security profiles verified for this release.
- Published crate or fresh-project checks completed after release.

## Verification

- Local, CI, packaging, docs, and broker smoke checks used for the tag.

## Known limits

- Alpha limitations users should consider before adoption.
```

Patch releases should usually have `None` under `Breaking changes` and
`Migration notes`. Any `0.x` minor release with public API changes should call
out the affected types, methods, variants, or defaults and link to the relevant
API direction document or roadmap entry.

## Required Checks

Run the same checks used by CI:

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo check -p kafrust --examples
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc -p kafrust --no-deps
```

Run the protocol packaging check before publishing:

```sh
cargo package -p kafrust-protocol --allow-dirty
```

Use `--allow-dirty` only for local verification of intentional uncommitted version edits. Do not publish from a dirty worktree.

The `kafrust` package cannot be fully prepared until the matching `kafrust-protocol` version is available from crates.io. After `kafrust-protocol` is published, run:

```sh
cargo package -p kafrust
```

## Local Package Qualification

Before claiming a release candidate, verify both packages from their staged
package directories with all features enabled. This approximates the docs.rs
build without claiming that the external docs.rs service has completed:

```sh
cargo package -p kafrust-protocol --allow-dirty
cargo package -p kafrust --allow-dirty
cargo doc --manifest-path target/package/kafrust-protocol-<version>/Cargo.toml --all-features --no-deps
cargo doc --manifest-path target/package/kafrust-<version>/Cargo.toml --all-features --no-deps
```

The `0.2.18` packages passed Cargo publish verification and both staged
all-feature package-documentation builds on commit `373de00`. The matching
`kafrust-protocol` and `kafrust` packages were published in that order; both
docs.rs pages returned HTTP 200, and a fresh external project compiled the
published `kafrust 0.2.18` crate with `tls` enabled. The main CI run
[`31661719918`](https://github.com/TaeeunKil/kafrust/actions/runs/31661719918)
and complete 17-job Live Kafka Smoke run
[`31661883116`](https://github.com/TaeeunKil/kafrust/actions/runs/31661883116)
passed on the release preparation commit. Update this paragraph after each
release with the current package and external verification results. None of
these checks replace broader workload qualification.

The `0.2.19` packages passed Cargo publish verification on release commit
`6a0c34c`; `kafrust-protocol` was published before `kafrust`. Both docs.rs
pages returned HTTP 200, and a fresh external project compiled the published
`kafrust 0.2.19` crate with `tls` on Rust 1.81. The complete 17-job Live Kafka
Smoke matrix passed on commit `1e5d5c6` in
[`31663188419`](https://github.com/TaeeunKil/kafrust/actions/runs/31663188419).

The `0.2.20` packages passed Cargo publish verification on release preparation
commit `5d028f1`; `kafrust-protocol` was published before `kafrust`. Both
docs.rs pages returned HTTP 200, and a fresh external project compiled the
published `kafrust 0.2.20` crate with `tls` on Rust 1.81. The complete 17-job
Live Kafka Smoke matrix passed the Admin retry change on commit `ec293d1` in
[`31665016772`](https://github.com/TaeeunKil/kafrust/actions/runs/31665016772).

The `0.2.21` packages passed Cargo publish verification on release preparation
commit `e2859d7`; `kafrust-protocol` was published before `kafrust`. The
published package metadata resolved with HTTP 200 from crates.io, both docs.rs
pages returned HTTP 200, and a fresh external project compiled
`kafrust 0.2.21` with the `tls` feature. The complete 17-job Live Kafka Smoke
matrix, including the new Kafka 3.7.2 three-broker eager sticky group gate,
passed in [`31666975512`](https://github.com/TaeeunKil/kafrust/actions/runs/31666975512).
The external smoke project used the current stable toolchain; the repository's
Rust 1.81 compatibility remains covered by the required CI job.

The `0.2.22` packages passed Cargo publish verification on release preparation
commit `af52ab9`; `kafrust-protocol` was published before `kafrust`. The
release contains sticky duplicate-claim invalidation and Kafka-compatible
mixed-topic candidate ordering. The complete 17-job Live Kafka Smoke matrix,
including the Kafka 3.7.2 three-broker sticky group path, passed in
[`31668518895`](https://github.com/TaeeunKil/kafrust/actions/runs/31668518895).
Both crates.io package endpoints and both published docs.rs pages returned HTTP
200, and a fresh external project compiled `kafrust 0.2.22` with `tls`.

The `0.2.23` packages passed Cargo package and publish verification on release
preparation commit `ee49471`; `kafrust-protocol` was published before
`kafrust`. The release adds classic AlterConfigs v1 through a typed
`TopicConfigUpdate` API and updates the admin lifecycle example to exercise
classic replacement followed by incremental alteration. The complete 17-job
Live Kafka Smoke matrix passed on commit `1085880` in
[`31669906872`](https://github.com/TaeeunKil/kafrust/actions/runs/31669906872),
qualifying the plaintext broker profiles and Kafka 3.7.2 multi-broker path.
Both crates.io package endpoints and both docs.rs pages returned HTTP 200, and
a fresh external project compiled published `kafrust 0.2.23` with `tls`.

The `0.2.24` packages passed Cargo package and publish verification on release
preparation commit `f64df2e`; `kafrust-protocol` was published before
`kafrust`. The release adds broker-scoped fetch-session reuse for rack-aware
Fetch v11/v12 requests, with focused session and invalid-epoch retry coverage.
The complete 17-job Live Kafka Smoke matrix passed on code commit `8615833` in
[`31671783977`](https://github.com/TaeeunKil/kafrust/actions/runs/31671783977),
including the Kafka 3.7.2 three-broker rack-aware follow-up request. Both
crates.io package endpoints and docs.rs pages returned HTTP 200, and a fresh
external project compiled published `kafrust 0.2.24` with `tls`.

The `0.2.25` packages passed Cargo package and publish verification on release
preparation commit `f222d05`; `kafrust-protocol` was published before
`kafrust`. The release broadens Fetch v11/v12 negotiation and broker-scoped
fetch-session reuse to direct and group consumers without `client_rack`, while
retaining Fetch v4 fallback for older broker capability ranges. The complete
17-job Live Kafka Smoke matrix passed on the release commit in
[`31673377685`](https://github.com/TaeeunKil/kafrust/actions/runs/31673377685).
Both crates.io package endpoints and docs.rs pages returned HTTP 200, and a
fresh external project compiled published `kafrust 0.2.25` with `tls`.

The `0.2.26` packages passed Cargo package and publish verification on release
preparation commit `3f917c6`; `kafrust-protocol` was published before
`kafrust`. The release adds automatic direct-consumer leader-epoch truncation
recovery after a fenced or unknown leader epoch, with focused injected-broker
coverage. The complete 17-job Live
Kafka Smoke matrix passed on code commit `1694889` in
[`31677617186`](https://github.com/TaeeunKil/kafrust/actions/runs/31677617186).
Both crates.io package endpoints and both docs.rs pages returned HTTP 200. A
fresh external project compiled published `kafrust 0.2.26` with `tls` on Rust
1.81 MSVC.

The workflow-only follow-up gate in
[`31679167875`](https://github.com/TaeeunKil/kafrust/actions/runs/31679167875)
also passed. Its Kafka 3.7.2 three-broker profile stopped the second leader
after the initial assigned-consumer poll and verified automatic recovery after
the leader epoch changed from 1 to 2. This qualifies the live direct-consumer
leader-epoch failover path; group rebalance and data-loss/log-retention
scenarios remain outside the release claim.

The `0.2.27` packages passed protocol-first Cargo publish verification on
release preparation commit `d549a96`; `kafrust-protocol` was published before
`kafrust`. `cargo search` resolved both crates at `0.2.27`, both docs.rs pages
returned HTTP 200, and a fresh external project compiled published
`kafrust 0.2.27` with `tls`. The annotated `v0.2.27` tag points to the same
release preparation commit. A follow-up current-main `Live Kafka Smoke` run
[`31716400583`](https://github.com/TaeeunKil/kafrust/actions/runs/31716400583)
passed all 17 jobs, including classic Kafka 3.7.2 and Kafka 4.3.1 KIP-848
leader-epoch recovery over plaintext, SASL/PLAIN, and SASL_SSL/SCRAM. The
follow-up workflow and example fixes are on `main`; they do not change the
already-published `0.2.27` library artifacts.

The next current-main qualification run
[`31717934296`](https://github.com/TaeeunKil/kafrust/actions/runs/31717934296)
also passed all 17 jobs after adding the direct assigned-consumer retention
example. Its controlled `DeleteRecords` scenario moved the low watermark past
the consumer position and verified `OffsetResetPolicy::Earliest` recovery on
Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1. This is a main-branch 1.0 gate and does
not modify the already-published `0.2.27` artifacts.

The published runtime smoke
[`31719041843`](https://github.com/TaeeunKil/kafrust/actions/runs/31719041843)
then created a fresh project outside the repository, resolved `kafrust 0.2.27`
and its matching protocol crate from crates.io, and executed a producer to
direct-consumer roundtrip against Kafka 3.7.2. This verifies published runtime
linkage in addition to the earlier compile-only external smoke.

The follow-up published runtime smoke
[`31721075666`](https://github.com/TaeeunKil/kafrust/actions/runs/31721075666)
expanded that external project to call `AdminClient::describe_cluster`, an
idempotent producer, a direct consumer, and a classic consumer group against
Kafka 3.7.2. It resolved both `0.2.27` crates from crates.io and passed without
a workspace path dependency. This is runtime coverage for representative
published APIs, not a replacement for the multi-broker and security matrices.

The published runtime smoke was expanded again in
[`31729003352`](https://github.com/TaeeunKil/kafrust/actions/runs/31729003352)
with two matrix profiles. Fresh projects outside the repository resolved the
published `kafrust 0.2.27` and matching protocol crate from crates.io, then
completed the same Admin, idempotent-producer, direct-consumer, and group
poll/leave paths against Kafka 3.7.2 with the classic protocol and Kafka 4.3.1
with KIP-848. Both profiles passed without a workspace path dependency. This
extends published-artifact runtime evidence; it does not replace the broader
multi-broker, security, failure, and workload matrices.

The same published runtime smoke now also passes a Kafka 3.7.2
`SASL_SSL`/SCRAM-SHA-256 profile with the published `tls` feature in
[`31729868783`](https://github.com/TaeeunKil/kafrust/actions/runs/31729868783).
That profile generated a fresh broker CA, resolved the published crates from
crates.io in an external project, configured the public TLS/SCRAM builders, and
completed the Admin, idempotent-producer, direct-consumer, and classic group
paths. This is published security runtime evidence for the tested profile; it
does not claim all security providers, broker topologies, or failure modes.

The published transaction runtime gate passed in the same three-profile
workflow [`31730411006`](https://github.com/TaeeunKil/kafrust/actions/runs/31730411006).
Each fresh external project wrote an aborted transaction followed by a committed
transaction and verified that `ReadCommitted` exposed only the committed record.
The gate passed for Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, and Kafka 3.7.2
SASL_SSL/SCRAM with the published `tls` feature. This validates representative
published transaction semantics, not every transaction failure or throughput
workload.

The published compression gate passed in
[`31731421599`](https://github.com/TaeeunKil/kafrust/actions/runs/31731421599).
Fresh external projects resolved `kafrust 0.2.27` from crates.io and completed
the same direct, transactional, and `ReadCommitted` paths with Gzip, Snappy,
LZ4, and Zstd producer compression against Kafka 3.7.2. This confirms the
published codec configuration and fetch roundtrips; codec-specific throughput
and failure qualification remain separate.

The published Admin lifecycle gate also passed in
[`31731934027`](https://github.com/TaeeunKil/kafrust/actions/runs/31731934027).
Fresh external projects created a topic with `NewTopic`, verified it through
`list_topics` and `describe_topic_configs`, and deleted it through the public
`AdminClient` API. The lifecycle passed across the classic, KIP-848,
SASL_SSL/SCRAM, and four compression profiles; this is representative Admin
runtime evidence, not a claim that every Admin API and authorization policy is
interchangeable yet.

The `0.2.28` packages were then published protocol-first after the local
workspace, package, and registry verification gates passed. A fresh external
project matrix resolved `kafrust 0.2.28` and `kafrust-protocol 0.2.28` from
crates.io and passed all seven profiles in
[`Published Crate Smoke`, run `31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869): Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2 SASL_SSL/SCRAM, and Gzip, Snappy, LZ4, and Zstd. The published flow now also commits the first group record, leaves, rejoins with the same group ID, and consumes a post-commit record without replaying the committed record. This is representative published runtime evidence, not a full replacement, multi-broker, authorization, or workload claim.

The KIP-848 result also includes a current-main fix carried by `0.2.28`: when
Kafka returns an initial empty assignment while subscribed topic partitions
exist, `ConsumerGroup::join` waits for the non-empty broker assignment before
exposing the group handle. A focused regression test and all 321 workspace
tests pass; broader multi-member assignment and failure qualification remain
separate gates.

The published multi-broker follow-up
[`31735177161`](https://github.com/TaeeunKil/kafrust/actions/runs/31735177161)
also passed against the published `0.2.28` artifact. A fresh external project
observed a three-broker Kafka 3.7.2 cluster, committed a replicated-topic group
record, stopped its partition leader, waited for leader movement to a replica,
and then produced and consumed a post-failover record after rejoining with the
same group ID. This qualifies one published classic group and leader-failover
workload; it does not claim every multi-broker topology or failure mode.

The same published multi-broker workflow was then parameterized for Kafka 4.3.1
and the KIP-848 `consumer` group protocol. Run
[`31735762087`](https://github.com/TaeeunKil/kafrust/actions/runs/31735762087)
passed with `kafrust 0.2.28`: the external project committed before the broker
stop, followed the replacement partition leader, and consumed a post-failover
record after KIP-848 group rejoin. This is published-artifact evidence for one
KIP-848 failover workload, not complete multi-member or failure parity.

The published group-rebalance workflow then verified two-member ownership and
record delivery from fresh external projects. Kafka 3.7.2 classic passed in
[`31736939236`](https://github.com/TaeeunKil/kafrust/actions/runs/31736939236),
and Kafka 4.3.1 KIP-848 passed in
[`31736362411`](https://github.com/TaeeunKil/kafrust/actions/runs/31736362411).
Both runs observed disjoint ownership of all six topic partitions and consumed
the partition records through the published `0.2.28` artifact. These are
representative two-member workloads, not every assignor or failure mode.

The published secured group-rebalance workflow then passed the same two-member
ownership and record-delivery check with SCRAM-SHA-256 over SASL_SSL. Kafka
3.7.2 classic passed in
[`31740436499`](https://github.com/TaeeunKil/kafrust/actions/runs/31740436499),
and Kafka 4.3.1 KIP-848 passed in
[`31740567979`](https://github.com/TaeeunKil/kafrust/actions/runs/31740567979).
Each fresh external project resolved `kafrust 0.2.28` with `tls`, authenticated
both group members, and verified disjoint six-partition ownership plus record
delivery. This is representative published secured group evidence, not every
assignor, security mechanism, or member-failure workload.

The published `0.2.28` seven-profile smoke was rerun after adding active-group
Admin checks. [`31737581786`](https://github.com/TaeeunKil/kafrust/actions/runs/31737581786)
passed Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, SASL_SSL/SCRAM, and all four
compression profiles. Each external project listed and described its active
group and verified classic or KIP-848 committed offsets through the published
API. This is representative operational parity, not every Admin authorization
or failure workload.

The published transaction failover workflow
[`31738090052`](https://github.com/TaeeunKil/kafrust/actions/runs/31738090052)
also passed against Kafka 3.7.2 three-broker KRaft. The fresh external project
identified the transaction coordinator, the workflow stopped that broker while
the transaction was open, and the published `0.2.28` producer committed through
the replacement coordinator. A published `ReadCommitted` consumer observed the
committed record. This qualifies coordinator-stop recovery, not every
ambiguous-outcome, fencing, or throughput workload.

The published secured multi-broker workflow then passed both supported group
protocol profiles. Kafka 3.7.2 classic passed in
[`31738997447`](https://github.com/TaeeunKil/kafrust/actions/runs/31738997447),
and Kafka 4.3.1 KIP-848 passed in
[`31739154764`](https://github.com/TaeeunKil/kafrust/actions/runs/31739154764).
Each fresh external project resolved `kafrust 0.2.28` with the `tls` feature,
validated three SASL_SSL listeners, authenticated Admin/producer/group paths
with SCRAM-SHA-256, stopped broker 1's selected partition leader, and consumed
a post-failover record through the replacement leader. This is published
secured leader-failover evidence, not a claim for every security mechanism,
coordinator-plus-leader fault combination, or workload profile.

The same workflow then qualified the secured coordinator-plus-partition-leader
fault. Kafka 3.7.2 classic passed in
[`31739763944`](https://github.com/TaeeunKil/kafrust/actions/runs/31739763944),
and Kafka 4.3.1 KIP-848 passed in
[`31739927915`](https://github.com/TaeeunKil/kafrust/actions/runs/31739927915).
The fresh published projects listed the active group's coordinator, selected a
replicated partition led by that broker, stopped it, and verified authenticated
producer recovery plus same-group post-failover consumption. This qualifies a
representative combined fault, not repeated faults, every security mechanism,
or the complete 1.0 failure matrix.

The published secured repeated-leader workflow then passed two sequential
partition-leader failures. Kafka 3.7.2 classic passed in
[`31743322062`](https://github.com/TaeeunKil/kafrust/actions/runs/31743322062),
and Kafka 4.3.1 KIP-848 passed in
[`31743497415`](https://github.com/TaeeunKil/kafrust/actions/runs/31743497415).
Each fresh external project recovered after broker 1 stopped, restarted that
broker, then recovered again after a different partition leader stopped. This
qualifies repeated leader-failover behavior for the tested security and group
protocols, not unclean election, simultaneous broker loss, every security
mechanism, or the complete 1.0 fault matrix.

The published secured transaction workflow also passed coordinator-stop
recovery for Kafka 3.7.2 in
[`31741012713`](https://github.com/TaeeunKil/kafrust/actions/runs/31741012713)
and Kafka 4.3.1 in
[`31741137784`](https://github.com/TaeeunKil/kafrust/actions/runs/31741137784).
Each fresh external project opened a transaction over SASL_SSL/SCRAM-SHA-256,
stopped the discovered transaction coordinator, committed through the
replacement, and verified the result with an authenticated `ReadCommitted`
consumer. This qualifies secured coordinator-stop recovery, not every
ambiguous outcome, fencing, repeated-fault, or throughput workload.

The published restricted Admin authorization workflow passed for Kafka 3.7.2
in [`31741997691`](https://github.com/TaeeunKil/kafrust/actions/runs/31741997691)
and Kafka 4.3.1 in
[`31742115305`](https://github.com/TaeeunKil/kafrust/actions/runs/31742115305).
Fresh external projects resolved `kafrust 0.2.28` with `tls`, authenticated as a
non-superuser through SASL_SSL/SCRAM-SHA-256, and verified cluster describe,
allowed topic configuration, idempotent production, direct consumption, and
classic group consumption. The same user received typed denied results for
topic configuration on an unauthorized topic and unauthorized topic create and
delete operations. This qualifies one published StandardAuthorizer policy,
not every ACL pattern, Admin API, security provider, or mutation-failure mode.

The published restricted Admin mutation and offset-management workflow passed
for Kafka 3.7.2 in
[`31742788549`](https://github.com/TaeeunKil/kafrust/actions/runs/31742788549)
and Kafka 4.3.1 in
[`31742924984`](https://github.com/TaeeunKil/kafrust/actions/runs/31742924984).
Fresh external projects authenticated as the restricted user, successfully
altered the allowed topic configuration, preserved a denied config mutation,
committed a group offset, listed it through Admin OffsetFetch, reset it through
Admin OffsetCommit v2, and consumed from the reset position after rejoin. This
qualifies representative published mutation and offset policy behavior, not
every Admin mutation, ACL pattern, provider, or ambiguous failure mode.

The published performance workflow
[`31744206188`](https://github.com/TaeeunKil/kafrust/actions/runs/31744206188)
passed all four external `kafrust 0.2.28` profiles: Kafka 3.7.2 and 4.3.1
with no compression and Zstd. Each fresh project produced and consumed 10,000
1-KiB records in batches of 200. Producer throughput was 48,929 records/s and
45,507 records/s for Kafka 3.7.2 no-compression and Zstd, and 43,736 records/s
and 44,797 records/s for Kafka 4.3.1. Consumer throughput was 210,558,
257,703, 229,630, and 268,299 records/s in the same order. All profiles had
zero retries and zero final in-flight or buffered records. This is a published
baseline for repeatability, not a direct rust-rdkafka comparison or production
SLO.

The published direct comparison workflow
[`31753172293`](https://github.com/TaeeunKil/kafrust/actions/runs/31753172293)
passed a fresh external project using `kafrust 0.2.28` and `rust-rdkafka 0.39.0`
against Kafka 4.3.1. Both implementations used fresh one-partition topics,
2,000 1-KiB records, and batches of 100; the kafrust profile used
`Acks::Leader` and the rust-rdkafka profile used `acks=1`. Kafrust measured
51,834 producer records/s and 129,875 consumer records/s; rust-rdkafka measured
48,452 producer records/s and 252,306 consumer records/s. The comparison
builds `librdkafka` only in this external benchmark project and does not add a C
dependency to kafrust. This is one reproducible throughput baseline, not API or
feature parity, production SLO evidence, or a universal performance ranking.

The published soak workflow
[`31744827441`](https://github.com/TaeeunKil/kafrust/actions/runs/31744827441)
passed a fresh external `kafrust 0.2.28` project against Kafka 4.3.1. The
120-second, 1-KiB, batch-size-100 workload stopped the broker after one third of
the run, waited ten seconds, restarted it, and reconciled 7,229,000 produced
and consumed records. It observed 173 operation errors, 982 failed requests,
and 1,210 retries, then reported `recovered=true` with zero final in-flight or
buffered records. This is a published single-node recovery profile, not a
multi-broker soak or production SLO.

The published multi-broker soak workflow
[`31746182158`](https://github.com/TaeeunKil/kafrust/actions/runs/31746182158)
passed a fresh external `kafrust 0.2.28` project against Kafka 4.3.1. The
three-broker, three-replicated-partition, 120-second workload stopped broker 1
after one third of the run, waited ten seconds, restarted it, and reconciled
4,918,800 records across all partitions. It observed one operation error, seven
failed requests, and 1,006 retries, then reported `recovered=true` with zero
final in-flight or buffered records. This is a published plaintext
multi-broker recovery profile, not secured, simultaneous-loss, or production
SLO evidence.

The published secured multi-broker soak workflow
[`31747389166`](https://github.com/TaeeunKil/kafrust/actions/runs/31747389166)
passed a fresh external `kafrust 0.2.28` project with the `tls` feature against
Kafka 4.3.1 SASL_SSL/SCRAM-SHA-256. The three-broker, three-replicated-
partition, 120-second workload stopped broker 1 after one third of the run,
waited ten seconds, restarted it, and reconciled 2,288,700 records. It
observed one failed request and 1,001 retries, no high-level operation errors,
and ended with `recovered=true` plus zero final in-flight or buffered records.
This is a published secured recovery profile, not simultaneous-loss or
production SLO evidence.

The published simultaneous broker-loss workflow
[`31748293446`](https://github.com/TaeeunKil/kafrust/actions/runs/31748293446)
passed a fresh external `kafrust 0.2.28` project against Kafka 4.3.1. The
three-broker, three-replicated-partition, 120-second workload stopped brokers 1
and 2 simultaneously after one third of the run, waited ten seconds, restarted
both, and reconciled 4,423,200 records. It observed one failed request and 999
retries with no high-level operation errors, then reported `recovered=true` and
zero final in-flight or buffered records. This is a published plaintext
simultaneous-loss profile, not secured or production SLO evidence.

The same published simultaneous broker-loss workflow passed Kafka 3.7.2 in
[`31748860976`](https://github.com/TaeeunKil/kafrust/actions/runs/31748860976).
The fresh external `0.2.28` project processed 4,620,200 records across three
replicated partitions, observed one failed request and 1,008 retries, and ended
with `recovered=true` plus zero final in-flight or buffered records. Together
the Kafka 3.7.2 and 4.3.1 runs qualify the tested plaintext simultaneous-loss
behavior, not secured simultaneous loss or production SLOs.

The secured simultaneous broker-loss workflow
[`31750274774`](https://github.com/TaeeunKil/kafrust/actions/runs/31750274774)
then passed a fresh external `kafrust 0.2.28` project with the `tls` feature
against Kafka 4.3.1 SASL_SSL/SCRAM-SHA-256. The three-broker,
three-replicated-partition workload used `Acks::All` with
`min.insync.replicas=2`, stopped brokers 1 and 2 simultaneously for ten
seconds, and reconciled 2,704,200 successfully acknowledged records after
recovery. Kafka rejected 282 produce operations while two brokers were down;
the client recorded two failed requests and three retries, then reported
`recovered=true` with zero final in-flight or buffered records. This closes the
tested Kafka 4.3.1 secured simultaneous-loss durability/availability profile.
The same gate passed Kafka 3.7.2 in
[`31751812178`](https://github.com/TaeeunKil/kafrust/actions/runs/31751812178)
with a 60-second run that reconciled 686,700 successfully acknowledged records,
330 expected write rejections, two failed requests, three retries, and zero
final in-flight or buffered records. Together these qualify the tested secured
simultaneous-loss behavior across Kafka 3.7.2 and 4.3.1. Unclean-election data
loss, production SLOs, and service canary readiness remain separate claims.

The following current-main live run
[`31719615947`](https://github.com/TaeeunKil/kafrust/actions/runs/31719615947)
also passed the controlled classic consumer-group combined-fault gate in the
Kafka 3.7.2 three-broker profile. The target broker was both coordinator and
partition leader; after it was stopped, the replacement leader accepted a new
record and the group rejoined to consume it. This remains a main-branch gate
and does not modify the published `0.2.27` artifacts.

The subsequent complete current-main matrix
[`31723663771`](https://github.com/TaeeunKil/kafrust/actions/runs/31723663771)
passed all 17 jobs after adding the protocol-selectable combined-fault path.
It qualified the Kafka 4.3.1 plaintext KIP-848 case where the stopped broker
was both group coordinator and target partition leader, and verified rejoin
plus post-failover record consumption. The Kafka 3.7.2 classic group path also
passed its observable post-failover record check. Secured combined faults and
broader workload matrices remain outside the release claim.

The next complete current-main matrix
[`31725607371`](https://github.com/TaeeunKil/kafrust/actions/runs/31725607371)
passed all 17 jobs. It added the Kafka 3.7.2 `SASL_PLAINTEXT` classic combined
fault gate, selecting a broker that was both group coordinator and partition
leader, then verifying authenticated replacement-leader production and group
consumption after rejoin. It also removed an execution-order assumption from
the Kafka 4.3.1 SASL_SSL/SCRAM KIP-848 leader-epoch gate. These are current-main
qualification results and do not modify the published `0.2.27` artifacts;
the secured KIP-848 combined gate was qualified in the subsequent matrix below.

The following complete current-main matrix
[`31727573855`](https://github.com/TaeeunKil/kafrust/actions/runs/31727573855)
passed all 17 jobs and qualified the Kafka 4.3.1 KIP-848 combined coordinator
and partition-leader fault over `SASL_SSL` with SCRAM-SHA-256. It stopped the
selected broker, produced through the authenticated replacement leader, and
verified group rejoin plus post-failover consumption. This current-main gate
does not modify the published `0.2.27` artifacts; broader fault and transaction
matrices remain outside the release claim.

That same current-main matrix also qualified a Kafka 3.7.2 three-broker classic
consumer-group retained-log boundary: after a committed position was deleted
through Admin `DeleteRecords`, `OffsetResetPolicy::Earliest` recovered the group
and it consumed a post-delete record. This is a current-main compatibility gate,
not a change to the published `0.2.27` artifacts.

## Optional Broker Checks

The default test suite does not require a Kafka broker. Before an alpha tag, run the opt-in examples or tests against a local broker when practical:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 cargo test -p kafrust --test broker_roundtrip -- --nocapture
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example producer_send
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example consumer_fetch
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=kafrust-smoke KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example consumer_group_poll
```

## Publish Order

Dry-run first:

```sh
cargo publish -p kafrust-protocol --dry-run
cargo publish -p kafrust --dry-run # only after protocol is visible on crates.io
```

Publish after dry-runs pass:

```sh
cargo publish -p kafrust-protocol
cargo publish -p kafrust
```

After publishing `kafrust-protocol`, wait for the crates.io index to expose the
new version and confirm it resolves before publishing `kafrust`:

```sh
cargo info kafrust-protocol@<version>
cargo publish -p kafrust --dry-run
```

After publishing, tag the release with a Conventional Commit history summary and include known alpha limits from the roadmap.

## Post-publish Verification

After both crates are published:

1. Confirm crates.io resolves both packages:

   ```sh
   cargo search kafrust --limit 5
   ```

2. Confirm a fresh project can compile against the published client crate. Replace `<version>` with the version being verified:

   ```sh
   cargo new --bin /tmp/kafrust-published-smoke
   cargo add kafrust@<version> --manifest-path /tmp/kafrust-published-smoke/Cargo.toml
   cargo check --manifest-path /tmp/kafrust-published-smoke/Cargo.toml
   ```

3. Run the manually dispatched `Published Crate Smoke` workflow with the same
   version. It creates external projects under `$RUNNER_TEMP`, resolves the
   dependency from crates.io, and executes representative Admin, idempotent
  producer, direct-consumer, and consumer-group paths against Kafka 3.7.2
  classic, Kafka 4.3.1 KIP-848, and Kafka 3.7.2 SASL_SSL/SCRAM profiles. The
   last profile uses the published `tls` feature; additional matrix profiles
   exercise Gzip, Snappy, LZ4, and Zstd. This is stronger than a workspace
   compile because the smoke projects have no path dependency on the repository.
4. Confirm docs.rs builds the published documentation for both crates.
5. Push an annotated release tag and create a GitHub release.
6. Run the `Live Kafka Smoke` workflow from GitHub Actions against `main`.
