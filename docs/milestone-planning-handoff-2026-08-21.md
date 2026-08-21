# kafrust Milestone Planning Handoff

Date: 2026-08-21
Audience: the next coding session / GPT-5.6 planning pass
Repository: kafrust

## 1. Mission

The long-term objective is to make kafrust a credible pure-Rust replacement
for Kafka client dependencies in Rust services, including workloads that would
otherwise use rust-rdkafka. The target is a client replacement, not a Kafka
broker replacement.

Hard constraints:

- pure Rust
- no librdkafka, C client bindings, or required C toolchain
- Kafka concepts and operational behavior remain visible in public APIs
- Tokio-based async behavior unless a deliberate runtime boundary is added
- compatibility claims require protocol tests, deterministic fault injection,
  live broker tests, published-crate tests, and documented limits
- do not describe kafrust as a Kafka broker, storage engine, controller, proxy,
  or complete Kafka Streams application engine

## 2. Source Of Truth

Read these files before proposing a milestone:

1. AGENTS.md - repository policy, validation, Rust and Kafka safety rules.
2. docs/roadmap.md - release qualification, milestone history, evidence, gaps.
3. docs/project-strategy.md - replacement target, competitors, completion tiers.
4. docs/compatibility.md - capability-by-capability status and evidence.
5. docs/competitor-source-audit-2026-08-20.md - source and published-artifact
   comparison; README claims are not automatically verified behavior.
6. docs/migration-from-rust-rdkafka.md - migration surface and known gaps.
7. docs/release.md - publish order and post-publish checks.
8. docs/agentic-development.md - development and evidence workflow.
9. This handoff - current session state and uncommitted work.

When documents disagree, prefer current source and fresh command output, then
update the relevant document instead of silently choosing a number.

## 3. Current Repository State

As of this handoff:

- Branch: main
- HEAD: b6602c9 docs(compat): record published 0.3.5 comparison
- origin/main: same commit as local HEAD
- Repository release line documented as published: 0.3.5
- Working tree: 44 tracked files modified and 2 untracked files
- Current changes are not committed or pushed
- gh auth status -h github.com reports the active TaeeunKil token is invalid
- No new commit, push, PR, merge, or release claim is valid for current changes

The dirty tree is a large accumulated change set spanning runtime, protocol,
Admin, CI/workflows, documentation, and release work. Do not blindly commit
every dirty path as one mega-commit. Inspect the diff and split coherent
Conventional Commits. Do not discard or reset existing changes.

The newest implementation slice in this session includes:

- ProducerConfig total delivery timeout with a 120-second default
- timeout validation, getter, Debug, and equality coverage
- total deadline enforcement for immediate and batch sends
- connection poisoning plus metadata/leader/topic-ID cache invalidation after
  a delivery deadline expires
- buffered-producer deadline accounting from accepted enqueue through Produce,
  including expiry without transmission
- deterministic producer timeout regression tests
- Delegation Token response-loss regression coverage returning
  AdminMutationOutcomeUnknown instead of replaying an ambiguous mutation
- roadmap and producer/migration documentation for these behaviors

These changes pass local validation but are not in a commit or published artifact.

## 4. Verified Local Baseline

The latest local validation passed after the current changes:

| Check | Result |
| --- | --- |
| cargo fmt --all -- --check | PASS |
| cargo check --workspace --all-targets | PASS |
| cargo test --workspace --all-features | PASS: 792 tests |
| cargo clippy --workspace --all-targets --all-features -- -D warnings | PASS |
| cargo doc --workspace --all-features --no-deps | PASS |
| cargo package --workspace --locked --allow-dirty --no-verify --offline | PASS |
| git diff --check | PASS |

Packaging produced kafrust 0.3.5 and kafrust-protocol 0.3.5.

Repository scripts also pass:

- check_protocol_api_surface.py: 63 modules, 76 unique Kafka API keys
- check_apache_schema_versions.py: Kafka 4.3.1 snapshot, 12 schemas, local
  implementations within official bounds

On this machine PowerShell has no normal python command. Use the bundled
runtime when necessary:

    & 'C:\Users\user\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' scripts/check_protocol_api_surface.py
    & 'C:\Users\user\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' scripts/check_apache_schema_versions.py

This is strong deterministic evidence, not a substitute for a fresh published
crate smoke, live Kafka matrix, or production SLO test.

## 5. Published And Live Evidence

The roadmap records these important 0.3.5 gates:

- Fresh external signed OAUTHBEARER on Kafka 3.7.2 passed in GitHub run
  32420723537, including RS256 OIDC/JWKS validation, authentication,
  produce/readback, and SASL re-authentication.
- Fresh seven-profile published-crate smoke passed in 32420987547, covering
  Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2 SASL_SSL/SCRAM,
  Gzip, Snappy, LZ4, and Zstd.
- Published secure multi-broker simultaneous-loss soak passed in 32440677496
  on Kafka 4.3.1 SASL_SSL/SCRAM, ending with zero in-flight and buffered
  records.
- Current-source Share acknowledgement response-loss gate passed in 32449038941
  through an injected proxy.
- docs.rs pages for kafrust 0.3.5 and kafrust-protocol 0.3.5 returned HTTP 200.

These are named evidence slices, not complete replacement qualification.
Use the full historical run list in docs/roadmap.md rather than duplicating it.

Evidence language must stay precise:

- Candidate: API and focused tests exist; live qualification may be absent.
- Implemented: source and examples exist; live evidence is incomplete/outside
  the default gate.
- Live verified: a specific broker/security/workload profile passed.
- Published: the exact crate artifact was resolved by a fresh external project
  and the stated workflow passed.
- Production-ready: do not use this label until broad matrix, failure,
  performance, release, and migration gates are complete.

## 6. Current Capability Shape

The source now covers much more than a producer-only alpha:

- 76 unique Kafka API keys, with Apache Kafka 4.3.1 schema metadata bounds
- immediate, batch, buffered, custom-partitioned, compressed, idempotent, and
  transactional producers
- direct consumers, classic groups, KIP-848 groups, assignment/rejoin state,
  offset management, leader epochs, rack-aware fetch, and read-committed
- ShareConsumer/KIP-932 runtime slices and acknowledgement ambiguity handling
- Streams group membership and heartbeat/session slices; not a Streams DSL
- typed Admin APIs for many topic, group, transaction, ACL, quota, SCRAM,
  delegation-token, reassignment, log-directory, feature, quorum, telemetry,
  and Share Group State operations
- plaintext, TLS, SASL/PLAIN, SASL/SCRAM, OAUTHBEARER, and mutual TLS config
- pure-Rust Gzip, Snappy, LZ4, and Zstd codec paths
- metrics, latency percentiles, request trace metadata, and telemetry
- optional blocking adapters with a dedicated Tokio runtime
- scripted-broker, fault-injection, and fuzzing infrastructure

Exact capability status is in docs/compatibility.md. API count and local test
count must never be presented as production parity.

## 7. Competitive Baseline

The comparison target is behavior plus evidence, not README feature count.

- krafka is the closest broad pure-Rust competitor. Published docs.rs is
  0.19.0; current main claims Kafka 4.3 parity, classic/KIP-848 groups,
  ShareConsumer, transactions, OAUTHBEARER, broad Admin, telemetry, metrics,
  a fake broker, 2,350+ tests, and six fuzz targets. Main-only claims are
  unreleased until a published artifact proves them. Its optional Zstd path
  has a C-toolchain concern.
- kacrab 0.4.0 claims Kafka 4.3.0 producer/consumer/share/Admin breadth,
  62 Admin operations, groups, four codecs, TLS/SASL, generated protocol,
  broker matrix, and fuzzing.
- kafkit-client 0.1.9 targets modern Kafka 4.0+/KRaft with KIP-848,
  ShareConsumer, transactions, Admin, security, compression, and metrics, but
  intentionally drops classic/older-broker compatibility.
- kafka_client 0.5.2 is a broad Tokio/TLS/SASL/Admin candidate with thinner
  public compatibility evidence.
- kafka-rust has established APIs but documented tested brokers end at 3.1.
- rskafka is a narrower WAL-oriented client and explicitly excludes groups,
  offset tracking, and transactions.
- rust-rdkafka remains the mature adoption baseline, but is not pure Rust
  because it wraps librdkafka. It is the final bar for broad service migration,
  operational semantics, and production evidence.

The strategy document's current planning estimates are:

- protocol/API breadth: 75-85%
- high-level runtime semantics: 45-55%
- production evidence/release maturity: 35-45%
- weighted replacement readiness: 50-60%
- pure-Rust breadth match: 60-70%; roughly 12-18 focused months remaining
- operationally surpassing broad pure-Rust competitors: 40-50%; roughly
  18-30 focused months remaining
- credible broad rust-rdkafka replacement: 25-35%; roughly 24-48 focused
  months, or 3-5 years for one person part-time

These are estimates, not facts or calendar promises. The next planner must
re-baseline them with an explicit weighting model.

## 8. Remaining Gates

Choose one coherent slice per milestone. Do not attempt to close this whole
list in one broad rewrite.

### P0: Repository and release hygiene

- split the current dirty tree into coherent Conventional Commits
- preserve existing work; never use destructive reset/checkout commands
- re-authenticate with gh auth login -h github.com
- inspect failed CI steps with gh run view <RUN_ID> --log-failed
- push only after local commit review and successful authentication
- run CI on the exact pushed commit
- decide whether repository policy requires a PR/merge instead of direct main push

### P1: Protocol and public API breadth

- close remaining Kafka 4.3/current API and schema gaps used by real services
- finish stable high-level ShareConsumer and consumer-group contracts
- qualify advanced Share Group State and KRaft paths separately
- stabilize public types, config names, error semantics, and semver notes
- preserve Kafka 3.7-era compatibility while adding Kafka 4.x features

### P1: Failure semantics and operational correctness

- broaden post-transmission ambiguity tests for every non-idempotent Admin
  mutation, including authorization and active-member cases
- complete delivery timeout behavior for immediate, batch, buffered, retry,
  backpressure, and shutdown paths
- qualify idempotent producer leader movement, duplicate, out-of-order,
  fencing, sequence exhaustion, and ambiguous responses
- qualify transaction coordinator/broker loss, timeout, fencing, commit/abort
  ambiguity, and read-committed consumption
- qualify classic/KIP-848 rebalances, member loss, coordinator movement,
  static membership, cooperative assignment, and offset restoration
- define unclean-election/data-loss semantics without implying retry can recover
  data already lost by the broker

### P1: Evidence and performance

- establish a reproducible broker matrix for declared 3.3/3.6/3.7/3.9/4.0/4.3
  compatibility where required
- extend multi-broker/security tests to repeated, long, realistic workloads
- add throughput, tail latency, memory, allocation, retry, duplicate, and
  backpressure evidence
- expand fault injection, fuzzing, malformed-frame, decompression-limit, and
  fake-broker coverage toward competitor-level infrastructure
- qualify published artifacts independently from workspace path dependencies

### P2: Adoption and tooling

- document rust-rdkafka migration for producer, consumer, groups, Admin,
  security, metrics, callbacks, and errors
- build an adapter/canary workflow so business logic is not rewritten during
  client migration
- add credential rotation, observability integration, and rollback guidance
- qualify blocking adapters and decide whether runtime abstraction belongs in 1.0
- define API freeze, deprecation policy, MSRV, feature flags, and release cadence

## 9. Milestone Format For GPT-5.6

The next milestone must contain:

1. One user-visible objective or evidence gate.
2. Explicit non-goals to prevent scope creep.
3. Exact source modules, protocol versions, public API changes, docs, tests,
   CI workflow, and release artifacts.
4. Failure model: pre-transmission failure, post-transmission ambiguity,
   retryability, cancellation, timeout, shutdown, and connection ownership.
5. Deterministic local tests plus required live broker/published-crate profile.
6. Measurable exit criteria with counts, broker versions, security mode, exact
   error types, and reconciliation behavior.
7. Rollback and migration notes for public API/protocol changes.
8. Conventional Commit plan and a clean push/CI checkpoint.

Recommended immediate order:

1. Cleanly commit the accumulated work and repair GitHub/CI visibility.
2. Re-baseline parity gaps using source plus published-artifact evidence.
3. Choose the highest-risk common client behavior, not an obscure API.
4. Close it with code, deterministic tests, live qualification, and docs.
5. Update docs/roadmap.md only after the exit evidence exists.

## 10. First Commands For The New Session

Run before changing code:

    git status --short --branch
    git diff --stat
    git log -12 --oneline --decorate
    gh auth status -h github.com
    gh run list --limit 20
    cargo metadata --no-deps --format-version 1

For a failed workflow:

    gh run view <RUN_ID> --log-failed

Then read the source-of-truth documents in Section 2 and compare actual source
against their claims. Do not start from the old percentage estimate alone.

## 11. Decision

kafrust is beyond a basic alpha client and has substantial pure-Rust Kafka
coverage, but it is not yet a complete rust-rdkafka replacement. The current
working changes are not published. The next session must turn the broad
ambition into smaller evidence-backed milestones while preserving the core
constraints: no C client dependency, no broker-replacement scope creep, no
unsupported compatibility claims, and no completion claim without commit, CI,
and required broker/published-artifact evidence.
