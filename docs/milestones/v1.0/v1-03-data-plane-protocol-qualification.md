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
The deterministic response-fixture slice below is now also recorded; the full
malformed length/trailing-byte matrix, transaction-selection proof, and live
version logs remain required.

### Non-empty response golden-fixture slice (2026-09-03)

The same integration fixture now contains fixed non-empty response bodies for
Produce v2/v7/v9/v11/v12/v13, Fetch v4/v11/v12/v13, Metadata v1/v12,
ListOffsets v1, OffsetForLeaderEpoch v3, and ApiVersions v0/v3/v4. Assertions
verify decoded topics, partitions, offsets, record errors, aborted transactions,
topic UUIDs, compact collections, and flexible tagged fields. The focused
golden suite passed all five tests, the malformed suite passed all three tests,
and the complete repository validation passed on source commit
`fd3718484ce84cc37d4b8ebf1b3267a4e404e1b5`. The pushed commit is covered by
stable and Rust 1.81.0 in
[CI run 33735496212](https://github.com/TaeeunKil/kafrust/actions/runs/33735496212).
The detailed record is
[`v1-data-plane-response-golden-2026-09-03.md`](../../evidence/v1-data-plane-response-golden-2026-09-03.md).

This is deterministic decoder evidence, not an official Apache oracle for every
response or a live broker qualification. Full malformed boundary expansion,
transaction-selection proof, accepted-floor/pinned-broker version logs,
three-broker movement, and the V1-03 exit criteria remain open.

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

### Floor/current version negotiation probe (2026-09-03)

The opt-in broker-roundtrip probe now records the negotiated data-plane
selection and exercises the selected paths on both local profiles. At source
commit `4110089`, Kafka 4.3.1 selected Produce 13 with topic IDs (12 without
them), Fetch 13, Metadata 12, ListOffsets 1, OffsetForLeaderEpoch 3, and
ApiVersions 3. Kafka 3.7.2 selected Produce 9, Fetch 13, Metadata 12,
ListOffsets 1, OffsetForLeaderEpoch 3, and ApiVersions 3. Each run created a
unique topic, waited for a ready leader, completed ListOffsets and
OffsetForLeaderEpoch partition roundtrips, produced one record, fetched it at
the returned offset, and cleaned up the topic. The exact logs, image digests,
commands, and non-claims are in
[`v1-company-data-plane-version-log-2026-09-03.md`](../../evidence/v1-company-data-plane-version-log-2026-09-03.md).

This closes the bounded single-node floor/current version-log slice only. It
does not close the full malformed length/trailing-byte matrix, transaction
selection proof, three-broker topic-ID/leader movement, accepted security or
published-artifact qualification, or the V1-03 exit criteria.

### Deterministic response truncation prefix matrix (2026-09-03)

At source commit `8842c654702010a0719049bb70f1458a66954c80`,
`data_plane_malformed.rs` now starts from the valid empty-body shape for each
selected/fallback response version and checks every shorter byte prefix. The
matrix covers Produce v2/v7/v9/v11/v12/v13, Fetch v4/v11/v12/v13, Metadata
v1/v12, ListOffsets v1, OffsetForLeaderEpoch v3, and ApiVersions v0/v3/v4.
All four focused malformed tests passed, and the manifest checker requires the
new matrix test. The detailed record is
[`v1-data-plane-malformed-prefix-matrix-2026-09-03.md`](../../evidence/v1-data-plane-malformed-prefix-matrix-2026-09-03.md).

This strengthens deterministic truncation evidence only; the earlier source
slice left complete malformed length/trailing-byte coverage, transaction
selection proof, three-broker topic-ID/leader movement, accepted security or
published-artifact qualification, and the V1-03 exit criteria open. The strict
boundary and selector follow-ups are recorded below.

### Deterministic transactional Produce selector guard (2026-09-03)

At source commit `fd9b93938f65f7d5944175dd52225bd93b3d2af3`, the transactional
selector matrix exercises brokers advertising Produce v11, v12, and v13 with
a topic ID present. Both immediate and prepared-batch traffic resolve to
Produce v11 and expose wire API version 11 in every case. The focused test
passed, with the detailed record in
[`v1-transactional-produce-version-cap-2026-09-03.md`](../../evidence/v1-transactional-produce-version-cap-2026-09-03.md).

This closes the deterministic selector guard for V1-03. The Kafka 4.3.1
transaction.version=2 coherent-state fixture owned by V1-06, live
transactional roundtrips, three-broker movement, and the remaining V1-03
qualification gates remain open.

### Selected response trailing-byte rejection (2026-09-03)

At source commit `f98275d35ebceba41dfeb77505fd08f857d48726`, the selected
data-plane response decoders finish with `Decoder::finish()`, which returns a
typed `TrailingBytes` error when a response body leaves input unconsumed. The
malformed matrix appends one sentinel byte to every selected/fallback response
shape across Produce, Fetch, Metadata, ListOffsets, OffsetForLeaderEpoch, and
ApiVersions; all five focused malformed tests passed. The detailed record is
[`v1-data-plane-malformed-trailing-2026-09-03.md`](../../evidence/v1-data-plane-malformed-trailing-2026-09-03.md).

This closes the selected deterministic malformed length/tag/truncation/trailing
boundary slice. Official Apache response oracles for every shape, live
three-broker movement, accepted security/published qualification, and the
remaining V1-03 exit criteria remain open.

The high-level client boundary was also exercised at source commit
`e1df007da77bd6ff6cc3031bc64de9c10b033da8`: an injected OffsetForLeaderEpoch
v3 response with one trailing sentinel byte returns the typed
`Error::Protocol(TrailingBytes { remaining: 1 })`. The focused regression and
full required validation passed; details are in
[`v1-data-plane-trailing-client-boundary-2026-09-03.md`](../../evidence/v1-data-plane-trailing-client-boundary-2026-09-03.md).
This confirms client observability of the deterministic boundary only; live
malformed-broker evidence and the remaining V1-03 gates stay open.

### Current-source broker recheck after strict response boundary (2026-09-03)

Source commit `1b4cb5f952261325dd0c20d6348829d3dd7a8e4f` was rechecked from the
company WSL2 workstation against an isolated Kafka 4.3.1 single-node broker.
The negotiated data-plane versions and valid ListOffsets, OffsetForLeaderEpoch,
Produce, and Fetch roundtrips all passed after the strict decoder change. The
retained diagnostic is
[`v1-company-data-plane-response-boundary-smoke-2026-09-03.md`](../../evidence/v1-company-data-plane-response-boundary-smoke-2026-09-03.md).
This is current-source local evidence only; multi-broker movement, security,
published, and release gates remain open.

### Complete broker-roundtrip recheck on the company workstation (2026-09-03)

The complete `broker_roundtrip` integration target was rerun at source commit
`3dc0d9ca2ed97359d4297267a117fd32d52da998` from WSL2 Ubuntu-T9 (`x86_64`, Rust
1.81.0) against an isolated Kafka 4.3.1 single-node KRaft broker. All 13 test
cases passed, including data-plane version logging and a one-record
Produce/Fetch roundtrip. Share-specific cases were intentionally skipped
because no Share topic or failover phase was configured. The detailed record
is [`v1-company-broker-roundtrip-2026-09-03.md`](../../evidence/v1-company-broker-roundtrip-2026-09-03.md).

This recheck confirms valid response compatibility after the strict decoder
boundary change; it does not replace official response oracles, accepted-floor
or published qualification, three-broker movement, long campaigns, or the
remaining V1-03 exit criteria.

The same complete target was rerun against the planned Kafka 3.7.2 floor line
with its actual ListGroups v4 advertisement. All 13 cases passed, including
Produce v9 and Fetch v13 negotiation plus the one-record roundtrip. The
floor-line record is [`v1-company-broker-roundtrip-floor-2026-09-03.md`](../../evidence/v1-company-broker-roundtrip-floor-2026-09-03.md).
This remains short single-node evidence and does not satisfy the accepted
floor security/workload or multi-broker exit gates.

### Current-source rerun after metrics changes (2026-09-04)

The current pushed source commit `0ce95cabb5add692ab9b7e1465dfb6555c54d7ae`
was rerun from the company Windows x64 workstation's WSL2 Ubuntu-T9 runtime
against an isolated Kafka 4.3.1 single-node KRaft broker. The serial
`broker_roundtrip` target passed all 13 cases, including the selected
data-plane version log and one-record Produce/Fetch roundtrip. The
`producer_send`, `producer_buffered`, `consumer_group_poll`, and
`admin_create_topic` examples also passed their short lifecycle checks. The
exact record is
[`v1-company-workstation-kafka-short-smoke-2026-09-04.md`](../../evidence/v1-company-workstation-kafka-short-smoke-2026-09-04.md).
This refresh is local deterministic evidence only; Share, security,
three-broker movement, accepted-floor, published, long-campaign, and release
gates remain open.

### Pushed-source company broker smoke after producer cancellation fence (2026-09-04)

Source `0674907` was rerun on the company Ubuntu-T9 WSL2 x86_64 workstation
against an isolated Kafka 4.3.1 broker. `broker_roundtrip` passed all 13 tests,
including the negotiated data-plane version log and one-record Produce/Fetch
roundtrip. Idempotent immediate and buffered producers passed, including
buffered fetch reconciliation, and the Admin topic lifecycle passed. The
record is [`v1-company-pushed-short-kafka-smoke-2026-09-04.md`](../../evidence/v1-company-pushed-short-kafka-smoke-2026-09-04.md).
This rerun is local deterministic evidence only; no group, Share, security,
three-broker, published, long-campaign, or release gate is promoted.

### Current-source hosted matrix recheck (2026-09-04)

After the producer deadline phase change, source `513dc7e` passed all 17 jobs
of the hosted [Live Kafka Smoke workflow](https://github.com/TaeeunKil/kafrust/actions/runs/33803553498), covering the
declared Kafka 3.7.2 through 4.3.1 plaintext, TLS, SASL, ACL, multi-broker,
and KIP-848/failover profiles. The grouped record is
[`v1-short-recheck-2026-09-04.md`](../../evidence/v1-short-recheck-2026-09-04.md).
This is a non-long current-source diagnostic; accepted published matrix,
official response oracles, and three-broker qualification gates remain open.

### Current-source hosted matrix rerun (2026-09-04)

The full 17-job `Live Kafka Smoke` workflow was rerun from source commit
`3c27e61820b7fb53450996d09a79c9278c8764e8` in
[run 33820740402](https://github.com/TaeeunKil/kafrust/actions/runs/33820740402).
All Kafka 3.7.2/3.8.1/3.9.1/4.3.1 plaintext, TLS, SASL, OAUTHBEARER, ACL,
KIP-848, and three-broker failover jobs passed. The retained record is
[`v1-live-kafka-smoke-rerun-2026-09-04.md`](../../evidence/v1-live-kafka-smoke-rerun-2026-09-04.md).
This refresh strengthens short live version/behavior evidence only; official
response oracles, complete published qualification, long campaigns, and the
remaining V1-03 exit criteria stay open.

### Hosted Apache schema audit on pushed source (2026-09-04)

The pushed source commit `a8199d66b75cae90db4de33b3f7db629a6b0eacc` passed
[Apache Schema Audit 33823046705](https://github.com/TaeeunKil/kafrust/actions/runs/33823046705).
The online Kafka 4.3.1 audit checked 152 request/response schemas across the
local protocol modules. Coverage notes for local versions that do not reach an
Apache flexible boundary, and the local ApiVersions v5 versus the pinned
Apache v4 ceiling, were emitted as explicit notes rather than failures. This
is a schema identity/version audit only; it does not replace golden-response
oracles, accepted-floor or three-broker qualification, published testing, or
the remaining V1-03 exit criteria.

### Published topic-ID continuity through leader movement (2026-09-04)

The published `kafrust 0.3.6` three-broker Kafka 4.3.1 KIP-848 smoke in
[run 33827383799](https://github.com/TaeeunKil/kafrust/actions/runs/33827383799)
passed after broker 1 was stopped and partition 0 moved to broker 2. Metadata
v12 returned the same topic UUID
`217dfd7fb4d9462d98c09fedc14b9b1d` before and after the movement; the published
client produced and consumed one record at offset 0 before the stop and one at
offset 1 afterward. The implementation also rejects a changed UUID. The exact
record is [`v1-published-topic-id-leader-movement-2026-09-04.md`](../../evidence/v1-published-topic-id-leader-movement-2026-09-04.md).

The same bounded probe also passed against the planned accepted-floor Kafka
3.7.2 classic line in
[run 33828967587](https://github.com/TaeeunKil/kafrust/actions/runs/33828967587):
partition 1 moved from broker 1 to broker 2, offsets advanced from 0 to 1, and
Metadata v12 preserved UUID `b258804505e74c4eb3186133ba66b260`.

This closes the bounded published topic-ID continuity probe only. Official
response oracles for every selected shape, accepted-floor security/workload
qualification, the full V1-20 matrix, long campaigns, and the remaining V1-03
exit criteria stay open.

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
