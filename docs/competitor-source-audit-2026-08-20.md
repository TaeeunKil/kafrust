# Competitor Source Audit

Date: 2026-08-20

This is a source-level comparison of the current pure-Rust Kafka client
alternatives. It is not a benchmark and it does not treat a README claim as
production evidence unless the corresponding source, tests, or published
artifact can be inspected.

## Method

The audit used shallow source snapshots for the following revisions:

| Project | Source snapshot | Release baseline |
| --- | --- | --- |
| [krafka](https://github.com/hupe1980/krafka) | [`e535c61`](https://github.com/hupe1980/krafka/tree/e535c616636a1862a9ab2654b1b2d8fc6902be63) | `0.20.0` on `main` |
| [kacrab](https://github.com/pirumu/kacrab) | `cf206a0` | published `0.4.0` |
| [rskafka](https://github.com/influxdata/rskafka) | [`9b7699d`](https://github.com/influxdata/rskafka/tree/9b7699d2ed818c19145a2728bfc7a9c456a04b66) | published `0.6.0` |
| [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) | [`3f54ff1`](https://github.com/fede1024/rust-rdkafka/tree/3f54ff1dabe7eece876b9635e22462b04478a445) | published `0.39.0` / `librdkafka 2.12.1` |
| [kafkit-client](https://docs.rs/kafkit-client/0.1.9/kafkit_client/) | crates.io `0.1.9` source archive | published `0.1.9` |
| kafrust | [`e799c96`](https://github.com/TaeeunKil/kafrust/tree/e799c96) | published `0.3.1` |

The `kafkit-client` Cargo metadata points at a GitHub repository that currently
returns 404, so the published crates.io archive was used instead of inventing
a current repository snapshot. The local competitor copies were kept outside
the repository under a temporary audit directory and are not dependencies of
kafrust.

The refreshed comparison also inspected the current source manifests and
module trees directly. Approximate Rust source size, including comments and
tests, was: kafrust application plus protocol crates 79.9k lines in 86 files;
krafka 144.8k lines in 133 files; rskafka 14.5k lines in 45 files; and the
rust-rdkafka wrapper 10.4k lines in 19 files, excluding its native librdkafka
tree. These counts indicate engineering surface, not quality or compatibility.

The current pure-Rust source snapshot shows 63 named kafrust protocol API
modules, 54 named krafka protocol message modules, and 12 named rskafka
message modules. File counts are not an API compatibility score: krafka folds
some APIs together, while kafrust keeps several versioned or broker-internal
surfaces separate.

### Refreshed source observations

- `rust-rdkafka` remains the mature adoption baseline, but its `rdkafka-sys`
  crate declares a native build script, bindgen-generated C bindings, and
  librdkafka 2.12.1. Its breadth comes from the native library, so it is not a
  pure-Rust implementation target for this project.
- `rskafka` is the official InfluxData repository at `0.6.0`. Its README
  explicitly excludes offset tracking, consumer groups, transactions, and
  built-in buffering. It has useful pure-Rust transport, compression, SASL/TLS,
  fuzzing, and benchmark patterns, but it is not a competing drop-in client
  for kafrust's target.
- `krafka` `0.20.0` is the closest current pure-Rust comparison. Its source
  contains producer batching/idempotence/transactions, classic and KIP-848
  consumer runtime code, ShareConsumer, broad Admin APIs, telemetry, several
  authentication providers, and a real in-process TCP fake broker. Its README
  explicitly says KIP-1071 Streams group runtime and KIP-1258 OAuth client
  assertions are not implemented. Default compression is pure Rust; its
  optional `compression-all` Zstd path uses `zstd-sys`, so the strict
  no-required-C posture still differentiates kafrust.
- `kafrust` `0.3.3` has broader explicitly named protocol coverage in the
  checked tree, including the alpha Streams group APIs and a large typed Admin
  surface. The source also contains all four record-batch codecs using
  Rust-native codec dependencies. A fresh external published `0.3.3` Share
  runtime and 64-record acknowledgement soak now pass on Kafka 4.3.1 in
  [`32384767744`](https://github.com/TaeeunKil/kafrust/actions/runs/32384767744)
  and [`32385522647`](https://github.com/TaeeunKil/kafrust/actions/runs/32385522647).
  A fresh external three-broker leader-failover path also passes in
  [`32386637555`](https://github.com/TaeeunKil/kafrust/actions/runs/32386637555).
  The published active-heartbeat path also passes three consecutive dynamic
  coordinator-loss cycles in
  [`32387564503`](https://github.com/TaeeunKil/kafrust/actions/runs/32387564503).
  The remaining gap is long-running and multi-member Share ownership/assignment
  qualification, Streams background heartbeat/assignment ownership, and
  broader published coverage.

## Findings By Project

### krafka

The source tree is a broad async-native client with hand-maintained protocol
modules under `src/protocol/messages`, high-level producer, consumer, Admin,
ShareConsumer, telemetry, and an optional in-process test broker. Its Cargo
manifest enforces `unsafe_code = "deny"`, missing documentation, and panic-like
Clippy lints. The README claims 2,350+ tests, six fuzz targets, transaction
fault injection, and a Kafka 3.9-to-4.3 integration matrix.

The important architectural lessons are:

- one shared transport configuration and connection/pool model across client
  types;
- explicit producer sequencing and transaction state machines;
- a fake broker that exercises real wire paths without Docker;
- protocol parity treated as a CI failure rather than a documentation task;
- strict compile-time enforcement of the advertised no-panic and no-unsafe
  posture.

Its main limitations relative to kafrust's target are the Kafka 3.9 broker
floor and the optional Zstd dependency path that uses `zstd-sys`. Kafrust can
differentiate on Kafka 3.7 coverage and a strict pure-Rust codec posture, but
only after equivalent modern-feature and live-evidence gates are closed.

### kacrab

The source tree is the strongest engineering reference in this comparison.
It is a workspace with separate runtime, protocol, code-generation, and macro
crates. Protocol types are generated from Kafka schemas, and the protocol test
suite includes generated round trips and a Java interoperability oracle. The
runtime has separate producer dispatcher/idempotence/transaction modules,
consumer membership and assignment modules, ShareConsumer code, and typed
Admin operations.

The repository also contains:

- real-broker producer, consumer, transaction, Share, Admin, TLS, SASL, and
  GSSAPI tests;
- Kafka 3.3.2, 3.6.2, 3.9.0, 4.0.0, and 4.3.0 fixture/matrix coverage;
- fuzz workflows and a coverage gate with an 80% line threshold for the
  selected source set;
- workspace-wide `unsafe_code = "forbid"` and strict Rust/Clippy lint policy;
- generated protocol source large enough to make schema drift a build concern
  rather than a manual checklist.

The published README claims 62 Admin operations, classic and KIP-848 groups,
Share groups, four codecs, transactions, and broad security support. These are
the feature and evidence gates kafrust must match before calling itself ahead
of the broad pure-Rust field.

### kafkit-client

The published `0.1.9` archive uses the `kafka-protocol` crate rather than
shipping its own generated protocol tree. It contains producer, consumer,
ShareConsumer, transaction, Admin, TLS/SASL, telemetry, Testcontainers, and
Turmoil integration code. Its documented target is Kafka 4.0+, KRaft, KIP-848,
transaction protocol v2, and Share groups.

This is a useful modern-broker comparison, but it intentionally excludes
classic consumer runtime protocols and older broker paths. Kafrust already has
the stronger intended compatibility floor; the remaining work is proving that
its modern paths are at least as usable and reliable from a published crate.

### rskafka

`rskafka` is a deliberately small distributed-write-ahead-log client. Its own
README explicitly excludes offset tracking, consumer groups, transactions,
and built-in buffering. It supports compression and transport features, but it
is not competing for the same drop-in client role. Kafrust has already exceeded
its scope; no roadmap time should be spent copying its narrower design.

## Kafrust Current Position

The current worktree has the following locally verified evidence:

- 738 workspace test and doctest cases pass with all features;
- 10 libFuzzer targets compile with tracked seed corpora;
- the protocol audit reports 63 source modules and 76 unique Kafka API keys;
- the Apache Kafka 4.3.1 schema audit and its regression tests pass;
- a reusable scripted TCP broker covers connection-aware drops and response
  injection for focused retry and ambiguity tests;
- the Admin member-aware OffsetFetch/OffsetCommit path resolves topic IDs via
  Metadata v12 and falls back safely to v9 when the capability is unavailable;
- the repository contains live and published workflows, but a workflow file is
  not itself live evidence. Each remaining workflow gate needs a passing run
  against the intended published artifact or source revision.

The local protocol breadth is therefore ahead of the current runtime maturity.
The most important evidence gap is not another isolated request encoder; it is
the absence of the broad, repeatable matrix and failure proof that the leading
competitors use to support their claims.

## Direct Gap Matrix

| Area | Source audit result | Required kafrust gate |
| --- | --- | --- |
| Protocol currency | krafka tracks Kafka 4.3 in CI; kacrab generates from schemas and runs Java oracle tests | make the Apache schema snapshot and API-version table a complete build/review gate, then qualify every advertised 4.x path on a broker |
| Admin breadth | kacrab claims 62 operations; kafkit covers common operations; kafrust has a substantial but incomplete typed surface | close the remaining public Admin operations, especially modern group/share/KRaft paths, with typed errors and live tests |
| Producer correctness | krafka and kacrab isolate idempotence, ordering, retry, and transaction state machines | finish multi-broker retry/reordering/epoch/fencing tests and prove ambiguous outcomes without unsafe replay |
| Consumer operations | krafka/kacrab cover classic plus KIP-848; kafkit focuses on modern groups | qualify classic and KIP-848 rejoin, static membership, cooperative assignment, read-committed, and coordinator recovery across the matrix |
| Share groups | all broad competitors expose Share functionality in some form | complete Share acknowledgement, renewal, recovery, and long-running poison/duplicate delivery evidence |
| Security | krafka/kacrab advertise broader TLS/SASL/OIDC/GSSAPI/IAM combinations than kafrust currently proves | add only the mechanisms in scope, then run a real secured matrix; do not count configuration-only tests as compatibility evidence |
| Fault and soak evidence | krafka has a fake broker; kacrab has real-broker and coverage gates | expand the scripted broker and run repeated multi-broker, secured, transaction, Share, and long-duration campaigns |
| Release credibility | competitor claims are backed by published artifacts and CI workflows | publish each milestone, build docs.rs, run fresh external examples, and record rollback/migration results |

## How Much Remains

These are engineering-effort estimates, not promises about elapsed calendar
time:

- `rskafka` scope: already exceeded.
- `kafkit-client` feature breadth: close in intended scope because kafrust
  preserves older/classic brokers, but not yet operationally surpassed until
  the modern published-artifact and failure gates pass.
- `krafka`/`kacrab` feature checklist: about 60-70% complete; roughly 12-18
  focused engineering months remain for modern protocol, Admin, security,
  Share, and configuration gaps plus evidence.
- Operationally surpassing the broad pure-Rust competitors: about 40-50%
  complete; roughly 18-30 focused engineering months remain. The exit bar is
  a repeatable Kafka 3.7-through-current matrix, secured and multi-broker fault
  injection, fuzz/coverage evidence, long soaks, and a published migration
  path.
- Broadly replacing `rust-rdkafka`: about 25-35% complete; roughly 24-48
  focused engineering months remain, or approximately 3-5 years for one
  person working part-time. This includes the compatibility long tail,
  configuration/callback migration surface, production SLOs, and ecosystem
  adoption; protocol feature count alone cannot establish replacement.

The next implementation priority is therefore: complete the public modern
Admin and Share surface, strengthen producer/consumer state-machine evidence,
run the secured and multi-broker matrix, then publish and test the migration
path. This sequence is more valuable than adding another advanced protocol
whose live behavior and public API are not yet qualified.
