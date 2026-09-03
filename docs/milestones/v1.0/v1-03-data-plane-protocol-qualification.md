# V1-03 Data-Plane Protocol Qualification

- Status: In progress
- Target evidence: Live current-source
- Dependencies: V1-02

## User-Visible Objective

Make every wire version selected by the stable producer and direct consumer
byte-auditable, bounded, and verified against the accepted broker floor and
pinned-current broker.

## Non-Goals

- No producer retry/idempotence or high-level consumer lifecycle changes.
- No requirement to implement broker/controller-internal API keys.
- No claim that low-level Fetch v14-v18 are used by high-level consumers unless
  this milestone deliberately selects and qualifies them.
- No raw API-key-count parity claim.

## Scope

Source:

- `crates/kafrust-protocol/src/{codec,frame,header,record_batch}.rs`
- protocol APIs `api_versions.rs`, `metadata.rs`, `produce.rs`, `fetch.rs`,
  `list_offsets.rs`, and `offset_for_leader_epoch.rs`
- `crates/kafrust/src/{client,producer,consumer,error}.rs`
- protocol audit scripts and broker/scripted-broker fixtures

Required API inventory:

| API | Key | Current versions requiring decision |
| --- | ---: | --- |
| Produce | 0 | legacy low-level v2 classification; high-level v3, v7, v9, v11, v12, v13 |
| Fetch | 1 | high-level v4/v11/v12/v13; low-level v2/v14-v18 classification |
| ListOffsets | 2 | v1 |
| Metadata | 3 | v1 and topic-ID v12 |
| ApiVersions | 18 | v0/v3/v4; any local v5 type is future expert/excluded, not a Kafka 4.3.1 live selection |
| OffsetForLeaderEpoch | 23 | v3 |

Produce v12/v13 may be selected for non-transactional or idempotent traffic.
V1-06 exclusively owns transactional version selection: this milestone must
not let broker-advertised Produce maxima silently opt transactions into Kafka
transaction protocol V2.

## Work Packages

1. Generate the official schema/version manifest for every selected and
   fallback request/response, including request/response header versions.
2. Add golden bytes from an Apache/Java oracle or checked official fixture for
   every high-level selected version.
3. Cover nullable versus empty keys, values, headers, records, topic IDs,
   compact arrays, tagged fields, negative lengths, integer boundaries, and
   trailing bytes.
4. Audit allocation bounds before frame, array, record batch, and decompression
   allocation; V1-18 owns the cross-cutting completion campaign.
5. Verify capability negotiation selects the documented version and rejects a
   lossy downgrade. Keep transactional selection behind V1-06's
   `transaction.version` decision rather than sharing the ordinary Produce
   maximum blindly.
6. Run source live negotiation on accepted floor and pinned-current brokers.

## Failure And Lifecycle Contract

- Malformed or oversized broker data returns a typed protocol/limit error
  before unbounded allocation.
- `UNSUPPORTED_VERSION` may trigger only a documented, semantics-preserving
  fallback inside the same request budget.
- Unknown flexible tags are skipped/preserved according to the type contract;
  required fields are never silently defaulted.
- A connection that loses framing alignment is poisoned and not cached.
- Nullable record fields retain Kafka null/empty distinctions through a
  roundtrip.

## Verification

Deterministic:

- at least one exact request and response golden fixture for every selected
  high-level version and fallback;
- malformed length/tag/truncation fixtures for each wire family;
- cross-codec record-batch roundtrips for Gzip, Snappy, LZ4, and Zstd;
- scripted negotiation assertions for floor and pinned-current selections.

Live:

- accepted floor and pinned-current single-node plaintext brokers;
- Produce, Fetch, Metadata, ListOffsets, and OffsetForLeaderEpoch roundtrip with
  exact selected versions logged as test output;
- three-broker topic-ID/leader-movement negotiation on the pinned-current line.

Run both protocol audit scripts, their checker tests, and all required local
Rust validation.

## Current Execution Record (2026-08-22)

The reviewed selection boundary is recorded in
[`docs/evidence/data-plane-version-manifest.json`](../../evidence/data-plane-version-manifest.json).
It names six data-plane API families, selected and low-level versions, request
and response header versions, local protocol types, and the V1-06 transaction
ownership boundary. `scripts/check_data_plane_manifest.py` checks those entries
against the Kafka 4.3.1 schema metadata snapshot, local API keys/types, and
client response-header paths; for the four data-plane schemas not yet in the
offline twelve-schema audit, it records the pinned Apache 4.3.1 raw-schema
values and URL template explicitly. CI runs it alongside the existing protocol
and Apache schema gates.

The producer selection guard now mechanically caps transactional Produce at
v11 and ignores topic-ID v13 while TV2 is unqualified. A focused regression
test covers both immediate and batch selection with a broker advertising v13;
non-transactional selection retains the v13/v12/v11 ladder.

`crates/kafrust-protocol/tests/data_plane_malformed.rs` now rejects truncated
responses and invalid negative collection lengths across all six data-plane
families. The manifest checker requires both named regression tests to remain
present in CI. The malformed-fixture commit `c6fb619` passed the stable and
Rust 1.81.0 matrix in [CI run 32548081944](https://github.com/TaeeunKil/kafrust/actions/runs/32548081944).

The transactional Produce cap implementation in `405bbac` also passed the
same matrix in [CI run 32547821393](https://github.com/TaeeunKil/kafrust/actions/runs/32547821393).

This is the deterministic inventory gate only. Official byte fixtures,
malformed-boundary expansion, floor/pinned live version logs, and the final
transaction-selection regression remain open and keep the milestone `In
progress`.

### Request golden-fixture slice (2026-09-03)

The integration fixture
[`data_plane_golden.rs`](../../../crates/kafrust-protocol/tests/data_plane_golden.rs)
now records complete request bytes for Produce v2/v3/v7/v9/v11/v12/v13, Fetch
v4/v11/v12/v13, Metadata v1/v12, ListOffsets v1, ApiVersions v0/v3/v4, and
OffsetForLeaderEpoch v3. The empty/nullable shapes isolate header versions,
compact counts, and tagged-field terminators against the Apache 4.3.1 schema
manifest. Focused test execution passed all four tests, and the exact pushed
head passed stable/Rust 1.81 CI in
[run 33726318714](https://github.com/TaeeunKil/kafrust/actions/runs/33726318714);
details are in
[`v1-data-plane-golden-fixtures-2026-09-03.md`](../../evidence/v1-data-plane-golden-fixtures-2026-09-03.md).
This closes the deterministic request-shape and minimal empty-response slice;
the flexible tagged-field truncation cases are also covered by
[`data_plane_malformed.rs`](../../../crates/kafrust-protocol/tests/data_plane_malformed.rs).
The pushed head also passed the malformed fixture in both stable and Rust 1.81
CI jobs in
[run 33727384183](https://github.com/TaeeunKil/kafrust/actions/runs/33727384183).
Non-empty response oracles, the full malformed length/trailing-byte matrix,
transaction-selection proof, and live version logs remain required.

### Current-source company workstation smoke (2026-09-03)

The pushed `main` head `37c5baa` was exercised against an isolated Kafka 4.3.1
single-node KRaft broker from the company Windows x64 workstation's WSL2
Ubuntu-T9 environment. The broker roundtrip binary passed all 13 tests, and
immediate/idempotent producer, buffered/idempotent producer, classic and
KIP-848 group poll/commit/leave, and Admin topic lifecycle examples passed.
The exact diagnostic record is
[`v1-company-workstation-current-short-smoke-2026-09-03.md`](../../evidence/v1-company-workstation-current-short-smoke-2026-09-03.md).
Share-specific tests were left unconfigured and are explicitly not claimed.
This is a bounded local diagnostic; accepted-floor/pinned-current version
logs, three-broker topic-ID movement, and the published qualification gate
remain open.

The workstation also reran the deterministic protocol fixture suites at pushed
head `e51384d`: four selected-version golden tests and three malformed-boundary
tests passed, alongside the 19-test scripted fault suite. The exact local
record is
[`v1-company-short-fault-protocol-smoke-2026-09-03.md`](../../evidence/v1-company-short-fault-protocol-smoke-2026-09-03.md).
This remains local deterministic evidence; live floor/pinned-current version
negotiation and non-empty response oracles are still required.

The planned floor line was also exercised from WSL2 against Kafka 3.7.2 at
pushed head `4f81471`: the 13-test broker roundtrip, immediate/buffered
producer (normal and idempotent), classic group, and Admin lifecycle smoke
passed. The record is
[`v1-company-floor-short-smoke-2026-09-03.md`](../../evidence/v1-company-floor-short-smoke-2026-09-03.md).
This is a single-node diagnostic only; it does not satisfy the accepted-floor
security/workload matrix or V1-20 published gate.

## Exit Criteria

1. Every stable high-level selected/fallback version has official metadata,
   golden request/response bytes, and malformed-boundary coverage.
2. All low-level-only versions are explicitly expert or promoted with live
   proof; this includes Produce v2 (removed/rejected by Kafka 4.x), and no
   document calls Fetch v18 a gap and implemented path simultaneously.
3. Null/empty record fields and all four codecs roundtrip without ambiguity.
4. Floor and pinned-current live runs select the documented versions.
5. Tests prove transactional traffic cannot select Produce v12/v13 until V1-06
   enables the complete TV2 protocol as one coherent state machine.
6. The evidence ledger records exact versions and non-claims; CI is green.

## Migration And Rollback

Version promotion must retain a lossless older fallback or return a typed
unsupported-version/configuration error. Roll back selection separately from
wire types so expert users do not lose audited fixtures unnecessarily.

## Conventional Commit Plan

1. `test(protocol): add official data-plane wire fixtures`
2. `fix(protocol): enforce selected schema boundaries`
3. `test(client): verify data-plane version negotiation`
4. `ci(kafka): qualify data-plane protocol versions`
5. `docs(compat): record data-plane protocol evidence`

## Evidence Record On Completion

Record every selected API/header version, oracle source, broker versions,
record shapes, codecs, malformed cases, run IDs, and explicit low-level-only
versions.
