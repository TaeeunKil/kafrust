# Roadmap

kafrust milestones are ordered by implementation risk and user-visible value. The project should keep Kafka concepts familiar to existing Kafka users while building a native Rust implementation underneath.

See [Project Strategy](project-strategy.md) for the replacement target, non-goals, existing alternatives, completion tiers, and the rationale for building a pure Rust client instead of wrapping librdkafka.

For the M21/v1.0 program, work status and evidence level are separate:

- Work status: Planned, In progress, Blocked, Done, or Superseded.
- Evidence level: Design, Local deterministic, CI, Live current-source,
  Packaged candidate, Published artifact, or Service canary.

Historical M0-M20 sections retain older `Complete`, `Implemented`, and
`Published` wording. Those labels describe the narrower milestone scope at the
time and do not imply that the v1.0 program is complete. The detailed status,
dependency, evidence, and exit rules for M21 are in the
[v1.0 Milestone Program](milestones/v1.0/README.md).

## v1.0 Planning Baseline

The 2026-08-21 planning inspection uses source commit `9eba7e5`. At inspection,
`main` was clean and synchronized with `origin/main`, GitHub authentication was
valid, and exact-HEAD CI passed in
[`32468949663`](https://github.com/TaeeunKil/kafrust/actions/runs/32468949663).
This supersedes the transient dirty-tree/authentication facts in the earlier
planning handoff; it does not turn post-`0.3.5` source changes into published
`0.3.5` evidence.

The first v1 prerequisite is package integrity. Both workspace manifests still
identify changed source as `0.3.5`, which is already published. A real
`cargo package -p kafrust --all-features --locked` verification fails because
the packaged client resolves registry `kafrust-protocol 0.3.5`, which lacks the
new AddOffsetsToTxn v3, AddPartitionsToTxn v3, EndTxn v3, and InitProducerId v2
types used by current source. The existing CI `--no-verify` package-assembly
check cannot detect that registry compilation boundary. This is the blocking
gate in [V1-00](milestones/v1.0/v1-00-repository-and-package-baseline.md).
The complete dated evidence and contradiction audit is in the
[v1.0 planning baseline](milestones/v1.0/baseline.md).

## V1-00 Execution Update (2026-08-22)

V1-00 is `Done`. The working candidate has moved both crates and the
client protocol dependency to coordinated `0.3.6`. The package verifier
reproduced the immutable `0.3.5` protocol mismatch, then built and unpacked
matching `0.3.6` tarballs outside the workspace and compiled five feature
profiles on Rust 1.81.0. Implementation commit `3e12192` passed the exact
Rust 1.81.0/stable matrix in
[CI run 32545563612](https://github.com/TaeeunKil/kafrust/actions/runs/32545563612).
This is packaged-candidate evidence only; V1-01 has now completed its contract
gate and no crate has been published.

V1-01 owns the accepted support boundary and ledger. Its exact CI result is
recorded in [run 32546206600](https://github.com/TaeeunKil/kafrust/actions/runs/32546206600).
The compatibility table remains a qualification target for each later
milestone rather than a blanket claim for every broker/security/workload
combination.

## V1-02 Execution Update (2026-08-22)

V1-02 is `Done`. The all-features rustdoc inventory now records 2,366
public symbols across twelve modules and 286 root exports in
[`docs/evidence/public-api-snapshot.json`](evidence/public-api-snapshot.json).
Each symbol is classified as `stable`, `expert`, `experimental`, or
`excluded`, assigned an owning milestone, and protected by a CI root-surface
and public-declaration digest check. Commit `1ea206d` passed both toolchains in
[CI run 32546683281](https://github.com/TaeeunKil/kafrust/actions/runs/32546683281).
The current snapshot is locked by the V1-24 preparation manifest at 2,374
symbols, twelve modules, and 288 root exports. This remains a classification
baseline only; V1-24 still owns the final semver freeze.

## V1-03 Execution Update (2026-08-22)

V1-03 is `In progress`. The reviewed data-plane manifest now names selected,
fallback, low-level, and header versions for Produce, Fetch, ListOffsets,
Metadata, ApiVersions, and OffsetForLeaderEpoch. Its checker cross-validates
the local API types and keys against Kafka 4.3.1 metadata and keeps transactional
Produce selection owned by V1-06; the producer now enforces the V1-06 TV1 cap
at Produce v11 for transactional sends. Golden/malformed fixture expansion and
the remaining live multi-broker qualification remain open. The cap commit passed
CI run [32547821393](https://github.com/TaeeunKil/kafrust/actions/runs/32547821393),
and the malformed-boundary increment passed CI run
[32548081944](https://github.com/TaeeunKil/kafrust/actions/runs/32548081944).

The latest pushed `main` head `37c5baa` was exercised from the company Windows
x64 workstation's WSL2 Ubuntu-T9 environment against an isolated Kafka 4.3.1
single-node KRaft broker. The broker roundtrip binary passed all 13 tests, and
immediate/idempotent producer, buffered/idempotent producer, classic and
KIP-848 group poll/commit/leave, and Admin topic lifecycle examples passed.
The bounded diagnostic is recorded in
[`v1-company-workstation-current-short-smoke-2026-09-03.md`](evidence/v1-company-workstation-current-short-smoke-2026-09-03.md).
This is local diagnostic evidence only; three-broker topic-ID movement and
published qualification remain open.

The same pushed head `e51384d` also passed the 19-test scripted fault suite and
the selected-version golden/malformed protocol fixture suites on Windows Rust
1.81. The bounded record is
[`v1-company-short-fault-protocol-smoke-2026-09-03.md`](evidence/v1-company-short-fault-protocol-smoke-2026-09-03.md);
it strengthens local deterministic evidence without changing the live or
published exit gates.

The planned Kafka 3.7.2 floor line was also exercised at pushed head `4f81471`
from WSL2 with the same short producer, classic-group, Admin, and roundtrip
checks. The record is
[`v1-company-floor-short-smoke-2026-09-03.md`](evidence/v1-company-floor-short-smoke-2026-09-03.md);
it is a single-node diagnostic and does not replace the accepted-floor matrix.

The opt-in broker-roundtrip probe at pushed head `4110089` now records the
floor/current data-plane selections and runs the selected paths. Kafka 4.3.1
selected Produce 13 with topic IDs (12 without), Fetch 13, Metadata 12,
ListOffsets 1, OffsetForLeaderEpoch 3, and ApiVersions 3. Kafka 3.7.2
selected Produce 9, Fetch 13, Metadata 12, ListOffsets 1,
OffsetForLeaderEpoch 3, and ApiVersions 3. Both profiles completed a
leader-ready topic probe, ListOffsets/OffsetForLeaderEpoch roundtrip, one
Produce, and one Fetch with cleanup. Exact logs and image digests are recorded
in [`v1-company-data-plane-version-log-2026-09-03.md`](evidence/v1-company-data-plane-version-log-2026-09-03.md).
This closes only the bounded single-node version-log slice; three-broker
topic-ID/leader movement and the accepted/published qualification gates remain
open.

At pushed head `8842c65`, the deterministic malformed suite was expanded with
a prefix matrix that feeds every shorter prefix of valid empty-body responses
to Produce v2/v7/v9/v11/v12/v13, Fetch v4/v11/v12/v13, Metadata v1/v12,
ListOffsets v1, OffsetForLeaderEpoch v3, and ApiVersions v0/v3/v4. All four
focused malformed tests passed, with the detailed record in
[`v1-data-plane-malformed-prefix-matrix-2026-09-03.md`](evidence/v1-data-plane-malformed-prefix-matrix-2026-09-03.md).
This is deterministic truncation evidence only; complete malformed
length/trailing-byte coverage, transaction-selection proof, three-broker
movement, and accepted/published qualification remain open.

The transactional Produce selector was then exercised at pushed head `fd9b939`
against advertised maxima v11, v12, and v13, with topic IDs and both immediate
and prepared-batch paths. Every case selected Produce v11 and reported wire
API version 11. The focused test and full required workspace validation passed;
the detailed record is
[`v1-transactional-produce-version-cap-2026-09-03.md`](evidence/v1-transactional-produce-version-cap-2026-09-03.md).
This closes only the deterministic selector guard; V1-06's coherent TV2
fixture, live transactional roundtrips, three-broker movement, and accepted or
published qualification remain open.

The selected response decoders now also reject unconsumed trailing bytes at
pushed head `f98275d`. A sentinel-byte matrix covers Produce v2/v7/v9/v11/v12/v13,
Fetch v4/v11/v12/v13, Metadata v1/v12, ListOffsets v1, OffsetForLeaderEpoch v3,
and ApiVersions v0/v3/v4; all five focused malformed tests passed. The exact
record is [`v1-data-plane-malformed-trailing-2026-09-03.md`](evidence/v1-data-plane-malformed-trailing-2026-09-03.md),
and the source CI is [33743888446](https://github.com/TaeeunKil/kafrust/actions/runs/33743888446).
This closes the selected deterministic malformed boundary slice only; complete
official response oracles, live three-broker movement, and accepted/published
qualification remain open.

The same boundary is observable through `Client` at pushed head `e1df007`: an
injected OffsetForLeaderEpoch v3 response with one trailing sentinel byte is
returned as typed `Error::Protocol(TrailingBytes { remaining: 1 })`. The focused
regression passed with the required workspace validation; see
[`v1-data-plane-trailing-client-boundary-2026-09-03.md`](evidence/v1-data-plane-trailing-client-boundary-2026-09-03.md).
This remains deterministic injected-stream evidence and does not replace live
broker malformed-response or published qualification.

The current source was rechecked on the company WSL2 workstation at `1b4cb5f`
against an isolated Kafka 4.3.1 broker after the strict response-boundary
change. Version negotiation and valid ListOffsets, OffsetForLeaderEpoch,
Produce, and Fetch roundtrips passed; the named test container was removed
without touching pre-existing resources. Details are in
[`v1-company-data-plane-response-boundary-smoke-2026-09-03.md`](evidence/v1-company-data-plane-response-boundary-smoke-2026-09-03.md).
This remains a short local diagnostic and does not replace three-broker or
published qualification.

The protocol fixture increment at pushed head `fd37184` adds fixed non-empty
response bodies for the selected Produce, Fetch, Metadata, ListOffsets,
OffsetForLeaderEpoch, and ApiVersions versions. Five golden tests and three
malformed-boundary tests passed, together with the full workspace validation;
stable and Rust 1.81.0 are covered by
[CI run 33735496212](https://github.com/TaeeunKil/kafrust/actions/runs/33735496212).
The evidence record is
[`v1-data-plane-response-golden-2026-09-03.md`](evidence/v1-data-plane-response-golden-2026-09-03.md).
This strengthens deterministic V1-03 evidence only; live three-broker
topic-ID/leader movement, the complete malformed matrix, transaction-selection
proof, and the milestone exit criteria remain open.

The complete `broker_roundtrip` integration target was also rerun at pushed
head `3dc0d9c` from the company WSL2 Ubuntu-T9 workstation against an isolated
Kafka 4.3.1 single-node broker. All 13 cases passed, with the configured
single-node run intentionally skipping Share-specific phases. The container
was uniquely named and removed after the run; existing Docker resources were
left untouched. Details are in
[`v1-company-broker-roundtrip-2026-09-03.md`](evidence/v1-company-broker-roundtrip-2026-09-03.md).
This is short current-source diagnostic evidence only; accepted-floor,
published, three-broker, long-campaign, canary, and release gates remain open.

The same current-source 13-case target also passed against the planned Kafka
3.7.2 floor-line broker after setting its actual ListGroups v4 expectation.
The first invocation exposed only a harness mismatch (v5 is not advertised by
Kafka 3.7.2); the corrected rerun passed without product-code changes. Details
are in
[`v1-company-broker-roundtrip-floor-2026-09-03.md`](evidence/v1-company-broker-roundtrip-floor-2026-09-03.md).
This remains short single-node diagnostic evidence, not accepted-floor
security/workload qualification.

## V1-04 Execution Update (2026-08-22)

V1-04 is `In progress`. Producer delivery expiry now has a typed
`DeliveryDeadlineExceeded` error with a finite phase and explicit
`possibly_transmitted` flag, while request timeouts remain a separate transport
error. Queue expiry is replay-safe and sends zero Produce frames; immediate and
batch deadline expiry poisons the producer path. Focused local deadline,
poisoning, and retry-classification tests pass, and the public API snapshot was
regenerated for the new export. Published mixed-outcome profiles and the full
clock-controlled matrix remain open. The buffered budget helper now accepts a
fixed clock anchor, with oldest/newer/expired/empty queue tests passing on
source commit `b838fa3`; the exact deterministic record is in
[`v1-producer-delivery-budget-2026-09-03.md`](evidence/v1-producer-delivery-budget-2026-09-03.md).
The pushed candidate's stable and Rust 1.81.0 jobs are green in [CI run 32548809314](https://github.com/TaeeunKil/kafrust/actions/runs/32548809314).

At pushed head `fb2778b`, the real buffered worker was exercised with a 20 ms
delivery budget and 10 second linger. Queue expiry returned the typed
queue-phase deadline, drained the buffered gauge, and sent zero Produce
requests. The detailed record is in
[`v1-buffered-queue-expiry-2026-09-03.md`](evidence/v1-buffered-queue-expiry-2026-09-03.md).
Delayed metadata/capability, post-write deadlines, cancellation/shutdown
ambiguity, and published mixed-outcome reconciliation remain open.

At pushed head `40905f1`, `BufferedProducer::close()` was exercised with one
accepted record and a long linger. Close flushed Metadata, ApiVersions, and
Produce before joining the worker; the delivery resolved at offset 42 and the
buffered gauge drained. Details are in
[`v1-buffered-close-flush-2026-09-03.md`](evidence/v1-buffered-close-flush-2026-09-03.md).
Expired or in-flight close ambiguity, cancellation during transmission, and
published mixed-outcome reconciliation remain open.

At pushed head `ed71f6d`, the buffered worker was also held after transmitting
Produce until its 100 ms total delivery budget expired. Both `flush()` and the
delivery handle reported `DeliveryDeadlineExceeded` in the Produce phase with
`possibly_transmitted=true`; the buffered gauge drained and the worker joined
cleanly. Details are in
[`v1-buffered-inflight-deadline-2026-09-03.md`](evidence/v1-buffered-inflight-deadline-2026-09-03.md).
This closes the deterministic buffered post-write deadline slice only; close
while a request remains blocked, delayed metadata/capability, cancellation
during transmission, and published mixed-outcome reconciliation remain open.

The complete 26-test deterministic fault target was then re-run from company
Ubuntu-T9 WSL2 x86_64 at source `ed71f6d`; all cases passed, including the
buffered queue-expiry, close-flush, and post-write deadline paths. The short
record is in
[`v1-company-fault-matrix-short-smoke-2026-09-03.md`](evidence/v1-company-fault-matrix-short-smoke-2026-09-03.md).
It remains current-source scripted evidence, not published, accepted-floor,
three-broker, long-campaign, canary, or release evidence.

The company WSL2 short smoke was refreshed at source `c1dc209`; the complete
29-test `fault_injection` target passed, including delivery-receiver
cancellation and the buffered close edge cases. The latest record is
[`v1-company-fault-matrix-29test-smoke-2026-09-03.md`](evidence/v1-company-fault-matrix-29test-smoke-2026-09-03.md).

## V1-04/V1-05 Execution Update (2026-09-03)

The low-level client now poisons a connection when a caller cancels an
in-flight request after socket I/O begins. The regression
`does_not_reuse_connection_after_canceled_request` confirms that a subsequent
request receives `NotConnected` instead of consuming an uncertain response.
The focused test and the required workspace validation passed at source
`2c28eec77911420e8dbb2d5d94bb96400f0148b9`. The detailed evidence is
[`v1-client-cancellation-poisoning-2026-09-03.md`](evidence/v1-client-cancellation-poisoning-2026-09-03.md).

This closes only low-level connection reuse after caller cancellation. Partial
client request writes, published mixed-outcome reconciliation, long campaigns,
multi-broker security profiles, service canary qualification, and release
authorization remain open; no version or publication decision follows from
this slice.

The partial-write boundary is now covered as well at source
`c42826f36f8af9b2d368d4035a05dfb8eb189ab8`: an injected three-byte request
write followed by `BrokenPipe` poisons the connection, and the next request is
rejected with `NotConnected`. See
[`v1-client-partial-write-2026-09-03.md`](evidence/v1-client-partial-write-2026-09-03.md).
This remains low-level transport evidence and does not close producer retry,
published reconciliation, long-campaign, canary, or release gates.

Company Ubuntu-T9 WSL2 x86_64 reproduced the partial-write regression at source
`abaf82a`; the focused test and all 29 scripted fault-injection tests passed.
The environment record is
[`v1-company-partial-write-smoke-2026-09-03.md`](evidence/v1-company-partial-write-smoke-2026-09-03.md).
No Docker resources were touched, and this remains short deterministic evidence.

At pushed head `37c3a44bad1748f6f4a5b3b311db2357617b3b99`, the producer-level
partial Produce write boundary is covered: after an injected three-byte write
and `BrokenPipe`, the producer refreshes metadata, reconnects, and retries
exactly once with the same `producer_id=42`, `producer_epoch=3`, and
`base_sequence=0`. The focused regression passed on Windows and company
Ubuntu-T9 WSL2 (`x86_64`, Rust 1.81.0), alongside all 29 WSL2 fault-injection
tests. Details are in
[`v1-producer-partial-write-retry-2026-09-04.md`](evidence/v1-producer-partial-write-retry-2026-09-04.md).
This closes deterministic producer classification for this partial-write phase
only; remaining cancellation/shutdown phases, published reconciliation,
long-campaign, canary, and release gates remain open.

The no-response cancellation path is covered at source `d9f1309`; the focused
regression passed and rejects reuse after a blocked `acks=0` write. Details are
in [`v1-client-no-response-cancellation-2026-09-03.md`](evidence/v1-client-no-response-cancellation-2026-09-03.md).
This remains low-level evidence and does not alter producer retry, published,
long-campaign, canary, or release gates.

Company Ubuntu-T9 WSL2 x86_64 also reproduced the no-response cancellation
regression at source `040edc5`; the focused test and all 29 scripted fault
tests passed. See
[`v1-company-no-response-cancellation-smoke-2026-09-03.md`](evidence/v1-company-no-response-cancellation-smoke-2026-09-03.md).
No Docker resources were touched.

## V1-05 Execution Update (2026-08-22)

V1-05 is `In progress`. The deterministic idempotent slice now records exact
frame replay after a dropped Produce response, duplicate-sequence resolution,
terminal handling for out-of-order/invalid-epoch/fenced responses, partition-
scoped batch sequencing, and sequence-modulus rollover. The focused tests are
`idempotent_producer_retries_dropped_response_with_same_batch_sequence`,
`retries_ambiguous_idempotent_batch_with_the_same_sequence`,
`idempotent_producer_fatal_sequence_errors_are_terminal`,
`preserves_reserved_batch_sequences_across_retries_and_chunks`, and
`wraps_idempotent_sequence_after_i32_max`; they pass on candidate commit
`5571ca3` with the required local validation. Buffered-mode coverage, the full
fault-phase table, and the exact published ten-cycle/100,000-record
reconciliation gate remain open, so no published-artifact claim is made.

The buffered idempotent path now has the same deterministic response-loss
replay guard at pushed head `27a9f46`: the first Produce response is dropped,
the reconnect replay is byte-identical, and duplicate-sequence success resolves
the original delivery without a new sequence. The focused regression and full
required local Rust validation passed; details are in
[`v1-buffered-idempotent-retry-2026-09-03.md`](evidence/v1-buffered-idempotent-retry-2026-09-03.md).
This closes only the buffered deterministic slice; published fault cycles and
100,000-record reconciliation remain open.

At pushed head `ed7d5eb`, the scripted broker now also writes only a partial
response frame before closing the connection. Immediate and linger-buffered
idempotent sends both reconnect and replay a byte-identical Produce frame with
the original batch sequence; the duplicate-sequence response resolves each
delivery. The exact deterministic record is in
[`v1-idempotent-partial-response-retry-2026-09-03.md`](evidence/v1-idempotent-partial-response-retry-2026-09-03.md).
Partial client request writes, cancellation/shutdown faults, published cycles,
and 100,000-record reconciliation remain open.

The complete 26-test scripted fault target was also re-run from the company
Windows/Ubuntu-T9 WSL2 x86_64 environment at `ed71f6d`; all tests passed,
including immediate/buffered dropped- and partial-response cases, buffered
terminal sequence errors, queue expiry, close flush, and the post-write
deadline. The short reproduction is recorded in
[`v1-company-fault-matrix-short-smoke-2026-09-03.md`](evidence/v1-company-fault-matrix-short-smoke-2026-09-03.md)
and does not change the published or multi-broker gates. The prior 22-test
record remains historical evidence at `83ec120`.

The company WSL2 run was refreshed at source `c1dc209`; all 29 deterministic
fault tests passed, including buffered close and delivery-cancellation cases.
The latest short record is
[`v1-company-fault-matrix-29test-smoke-2026-09-03.md`](evidence/v1-company-fault-matrix-29test-smoke-2026-09-03.md).

At pushed head `b885647`, two additional close cases passed: owner close joins
cleanly after a buffered in-flight Produce deadline with
`possibly_transmitted=true`, and owner close flushes a record enqueued through
`BufferedProducerHandle`. Both delivery outcomes and the buffered gauge were
verified. Details are in
[`v1-buffered-close-edge-cases-2026-09-03.md`](evidence/v1-buffered-close-edge-cases-2026-09-03.md).
Cancellation during socket I/O, delayed metadata/capability, and published
mixed-outcome reconciliation remain open.

At pushed head `c1dc209`, dropping a buffered `ProducerDelivery` after enqueue
was also exercised. The worker still sent and acknowledged the accepted
record, `flush()` completed, and the buffered gauge drained before owner close.
Details are in
[`v1-buffered-delivery-cancellation-2026-09-03.md`](evidence/v1-buffered-delivery-cancellation-2026-09-03.md).
Cancellation while socket I/O is blocked and partial client request writes
remain open.

At pushed head `a46462f`, the buffered idempotent path now covers terminal
sequence errors 45, 47, and 90. Each first delivery returns the fatal broker
code; a second queued delivery emits no Produce frame and returns the same
terminal result. Details are in
[`v1-buffered-idempotent-terminal-sequence-2026-09-03.md`](evidence/v1-buffered-idempotent-terminal-sequence-2026-09-03.md).
Partial client request writes, cancellation/shutdown ambiguity, and published
fault-cycle/reconciliation gates remain open.

The buffered terminal regression was reproduced from company Ubuntu-T9 WSL2
x86_64 at `a46462f1f51c257b18d85c8fd265e00d1b63f8a3`; the targeted test passed
for all three fatal broker codes. The short record is in
[`v1-company-buffered-terminal-smoke-2026-09-03.md`](evidence/v1-company-buffered-terminal-smoke-2026-09-03.md)
and remains non-published diagnostic evidence.

At pushed head `d0e033f`, immediate and batch idempotent Produce paths now keep
an in-flight outcome marker while awaiting the broker response. If the caller
cancels after the frame is transmitted, the producer is fenced and the next
operation returns `Error::IdempotentProducerDefunct` before sending another
sequence. Normal responses and transport retries clear the marker. Windows
required validation and company WSL2 Ubuntu-T9/Rust 1.81 focused plus all 29
fault-injection tests passed. This closes deterministic immediate/batch
cancellation only; buffered worker cancellation, published ten-cycle and
100,000-record reconciliation, secure transport, and live gates remain open.
Evidence: [`v1-idempotent-send-cancellation-2026-09-04.md`](evidence/v1-idempotent-send-cancellation-2026-09-04.md).

The company Windows/Ubuntu-T9 WSL2 recheck at pushed documentation head
`d1fe161` repeated both cancellation tests and all 29 scripted fault-injection
tests successfully with Rust 1.81.0. A separate full-workspace attempt reached
the example-link stage but hit the WSL mounted-checkout linker memory limit;
the authoritative stable/Rust 1.81 CI matrix remains green. The recheck record
is [`v1-company-validation-recheck-2026-09-04.md`](evidence/v1-company-validation-recheck-2026-09-04.md).

## V1-06 Execution Update (2026-08-22)

V1-06 is `In progress`. The transaction path keeps one coherent legacy TV0/TV1
protocol decision and mechanically caps transactional Produce at v11 until a
complete TV2 implementation is qualified. Deterministic scripted-broker tests
now cover both lost commit and lost abort EndTxn responses: each returns
`TransactionOutcomeUnknown`, marks the producer Defunct, performs no retry, and
rejects a later transaction start. Coordinator rediscovery, flexible-v3
fallbacks, fatal EndTxn handling, and the transactional Produce cap are also
covered. Published floor/current reconciliation and the full mutation fault
matrix remain open.

### EndTxn cancellation boundary (2026-09-03)

Source commit `34bbd443b835cd80056d330b21aa44ddc06ff6e0` adds a deterministic
regression for cancellation after the coordinator has observed the EndTxn v3
frame. The producer is marked `Defunct` while the response is pending, so a
cancelled commit cannot be reused as if its outcome were known. The evidence is
[`v1-transaction-end-cancellation-2026-09-03.md`](evidence/v1-transaction-end-cancellation-2026-09-03.md).
This is a direct EndTxn boundary only; the other transaction mutations and
published/reconciliation gates remain open.

### Transaction mutation cancellation boundary (2026-09-03)

Source commit `391a0562af676abbf838eaa6435c85965322f6fc` adds one deterministic
post-transmission cancellation regression for each remaining direct mutation:
`AddPartitionsToTxn` v3, `AddOffsetsToTxn` v3, and `TxnOffsetCommit` v0. The
scripted coordinator observes each frame, withholds the response, and the
caller cancellation leaves the producer `Defunct` with no reusable transaction
state. All three focused tests passed on Windows and company Ubuntu-T9 WSL2;
the complete 29-test fault-injection target also passed there. The record is
[`v1-transaction-mutation-cancellation-2026-09-03.md`](evidence/v1-transaction-mutation-cancellation-2026-09-03.md).
This is bounded direct-mutation evidence, not published, multi-broker,
security, long-campaign, service-canary, or release evidence.

## V1-07 Execution Update (2026-08-22)

V1-07 is `In progress`. Direct-consumer deterministic coverage now records
Fetch-response reconnect, bounded partition-queue backpressure without skipped
records, out-of-range reset, leader-epoch lookup, preferred-replica handling,
and read-committed record filtering. The dispatched current-source Live Kafka
Smoke run is refreshing the broker/version slice; golden record-shape fixtures,
retention/preferred-replica faults, and the published 100,000-record
reconciliation gate remain open.

### Record shape and Fetch cancellation boundaries (2026-09-03)

Source commit `df43749cfd277509c8173ac5f68beb8bced866bd` adds a null-versus-empty
record mapping regression and a Fetch v12 post-transmission cancellation test.
The scripted broker observes the Fetch frame, withholds the response, and the
next fetch succeeds through a fresh connection with no cached session reuse.
Both focused cases and the 29-test fault-injection target passed on company
Ubuntu-T9 WSL2. The evidence is
[`v1-direct-consumer-integrity-2026-09-03.md`](evidence/v1-direct-consumer-integrity-2026-09-03.md).
Retention, leader movement, published reconciliation, and queue/resource exit
gates remain open.

## V1-08 Execution Update (2026-08-22)

V1-08 is `In progress`. Classic and KIP-848 OffsetCommit now expose a typed
`ConsumerGroupCommitOutcomeUnknown` carrying group/member/generation identity
and exact topic-partition next offsets when a transmitted request loses its
response. Direct and bounded background worker paths share the same
post-transmission classifier; ambiguous outcomes are not retried, and
pre-transmission failures remain ordinary errors. The scripted classic group
response-drop regression passed on source commit `8a29d1e`, together with the
full local Rust validation and the regenerated public API snapshot. Published
floor/current group profiles, churn, callback, heartbeat, and offset
restoration gates remain open.

### Long-processing and `max.poll.interval` policy (2026-09-03)

V1-08 now records an explicit boundary: the classic group API does not expose
or enforce a client-side `max.poll.interval.ms` timer. The broker setting is
authoritative, and callers must return to `poll_with_heartbeat` within that
interval even when background heartbeats run. Longer processing must be moved
outside the group-poll call or covered by an explicitly larger broker setting;
missed intervals follow the normal rejoin path and are not hidden. This closes
the policy decision only; published churn, callback, heartbeat, and exact
offset-restoration gates remain open.

## V1-09 Execution Update (2026-08-22)

V1-09 is `In progress`. The KIP-848 path now has deterministic evidence for
member/epoch rejoin, nullable and empty assignments, Metadata v12 UUID
resolution, v9 fallback, v10 UUID commit, repeated rebalance recovery, and
regex topic/UUID refresh. The rejoin fixture asserts the member ID remains
stable while assignment offsets are restored. Published pinned-current
plaintext/SASL churn, stale-task cancellation, delete/recreate fallback races,
and exact 40-cycle ownership/offset gates remain open.

### V1-09 deterministic session-identity fence (2026-09-04)

The KIP-848 heartbeat handle now has a direct regression test at source
`3cc672a063cc0b3f2d4d3c0119fa17284190fd3f`: the same consumer session is
`Current`, a replaced or missing session is `StaleGeneration`, and a wrong
group is `DifferentGroup`; the scripted task is stopped and joined. Windows
required validation and company WSL2 Ubuntu-T9/Rust 1.81 focused plus all 29
fault-injection tests passed. This closes only the deterministic helper fence;
stale-task broker churn, member-loss, delete/recreate races, exact restoration,
and published qualification remain open. Evidence:
[`v1-kip848-consumer-session-identity-2026-09-04.md`](evidence/v1-kip848-consumer-session-identity-2026-09-04.md).

## V1-10 Execution Update (2026-08-22)

V1-10 is `In progress`. ShareConsumer deterministic coverage now records v1/v2
negotiation, bounded record-limit behavior, lost Accept/Release acknowledgement
classification, session reset, conditional Release redelivery, unknown-outcome
close, acquisition filtering, and stable member identity across reconciliation.
Published secure two-member churn, delayed/applied acknowledgement branches,
and the 10,000-record gate remain open; no exactly-once claim is made.

The company workstation then passed the three focused Share tests against an
isolated Kafka 4.3.1 Share coordinator at pushed head `74ee4dc`. The record is
[`v1-company-share-short-smoke-2026-09-03.md`](evidence/v1-company-share-short-smoke-2026-09-03.md);
it is single-node diagnostic evidence and does not replace the secure
multi-member published gate.

The direct acknowledgement cancellation boundary is now covered at pushed head
`2f04e650d55ff172eb30f1a07b8652e15a55d2fd`. If a caller drops `commit` after a
ShareAcknowledge frame is observed but before its response, the affected
pending records become `acknowledgement_outcome_unknown`, the Share session is
discarded, and the broker connection is not cached; a later commit cannot
replay the acknowledgement. The focused regression passed on Windows and
company WSL2, and WSL2 also passed all 29 fault-injection tests. This is local
deterministic evidence only; published secure multi-member coverage, long
campaigns, and the 10,000-record gate remain open. See
[`v1-share-ack-cancellation-2026-09-04.md`](evidence/v1-share-ack-cancellation-2026-09-04.md).

## V1-11 Execution Update (2026-08-22)

V1-11 is `In progress`. Controller-routed Admin operations now share explicit
pre/post-transmission handling: discovery and capability failures may retry,
while possible mutation response loss returns typed
`AdminMutationOutcomeUnknown` with no replay. Deterministic routing and partial
result coverage spans topic, partition, election, reassignment, feature, voter,
and broker-unregister operations. The complete authorization/reconciliation
ledger and published controller failover profiles remain open.

## V1-12 Execution Update (2026-08-22)

V1-12 is `In progress`. Coordinator/leader/broker Admin paths retain route
ownership, active-member identity, partial results, and typed ambiguity through
owner movement. Read-only operations may retry; unsafe writes do not replay,
while fixed-target DeleteRecords remains separately state-idempotent. The owner
ledger, UUID race, three-broker leader failover, and published profiles remain
open.

## V1-13 Execution Update (2026-08-22)

V1-13 is `In progress`. Security Admin routes preserve typed mixed
allow/deny results, pre-send retry, post-send unknown mutation outcomes, and
secret redaction across configs, ACLs, quotas, SCRAM, and delegation tokens.
Restricted-principal live qualification and the zero-secret artifact scan
remain open.

The deterministic secret-artifact slice is now implemented by
`scripts/check_v1_secret_artifacts.py`: it scans retained evidence in bounded
chunks for seven seeded credential markers, detects chunk-boundary splits, and
never prints marker contents. The local 47-file scan and four checker tests
passed, and CI now runs both checks. This does not close the required
restricted-principal/delegation-token published security profiles.

On 2026-09-03, exact-head CI for `ddaedb2` passed both the stable and Rust
1.81.0 jobs in [run 33708343106](https://github.com/TaeeunKil/kafrust/actions/runs/33708343106),
including the seeded secret-artifact scan and its four tests. This records the
checker and workflow slice only; it does not close the live security profiles
or authorize a release.

## V1-14 Execution Update (2026-08-22)

V1-14 is `In progress`. Streams, Share Group State, dynamic quorum, low-level
protocol, and blocking surfaces are explicitly classified as expert,
experimental, or excluded from the stable core claim. Deterministic routing,
task/runtime, controller, and nested-runtime tests exist; final API-key
classification and required live retained-surface gates remain open.

The Apache Kafka 4.3.1 `ApiKeys` inventory is now explicit for every key
0-92 in [`v1-14-api-key-classification.json`](evidence/v1-14-api-key-classification.json).
The checker covers all 93 entries, identifies 16 broker-internal RPCs, and
keeps the unimplemented `UPDATE_RAFT_VOTER` key 82 explicitly excluded. The
classification checker and tests are wired into CI. This closes the V1-14
classification inventory slice; retained expert/experimental live gates, long
campaigns, migration canary, and release gates remain open.

## Company Workstation Non-Long Validation (2026-09-03)

On the company Windows x64 workstation, WSL2 Ubuntu-T9 (`x86_64`) and isolated
Docker brokers made the short deterministic surface executable again. Kafka
4.3.1 single-node checks passed producer delivery (including compression,
buffering, idempotence, and transactions), direct and group consumers (classic
and KIP-848), regex assignment, retention/offset recovery, Admin mutations,
Streams group membership, ShareConsumer/ShareGroup lifecycle, quorum/topic
description, and the telemetry plugin/OTLP push path. The same broker-roundtrip
suite also passed against Kafka 3.7.2. Detailed commands, image digests,
topology caveats, and cleanup evidence are in
[`v1-company-workstation-nonlong-2026-09-03.md`](evidence/v1-company-workstation-nonlong-2026-09-03.md).

The same workstation also completed a separate short Kafka 4.3.1 three-broker
Streams coordinator-failover diagnostic: node 1 was stopped during the
heartbeat pause, the member completed a post-failover heartbeat and clean
leave, and the broker passed readiness after restart. This is diagnostic
evidence only and does not satisfy the V1-21 fault-soak duration or ledger gate.

The self-hosted runner is now technically online, but the unchanged campaign
capacity guard refuses a long dispatch because T: has `629.32 GiB` free versus
the required `700 GiB`; Docker root has `855 GiB` free. The T: backup
`Ubuntu-full-2026-08-24.tar` is `107.46 GiB`, so relocating that verified copy
to independent storage would clear the host threshold. No backup move or
deletion was performed, and the generated WSL resolver was restored after a
temporary DNS connectivity probe. Details are in
[`v1-long-campaign-capacity-audit-2026-08-24.md`](evidence/v1-long-campaign-capacity-audit-2026-08-24.md).

The return-to-workstation preflight on source `7c578f0a` re-established the
runner as online/idle long enough to run isolated `broker_roundtrip` against
Kafka 4.3.1 and 3.7.2; both 13-test runs passed. The generated resolver was
restored after the temporary connectivity probe, and the 629 GiB T: free-space
guard remains unchanged. This refresh is recorded in
[`v1-company-workstation-return-2026-09-03.md`](evidence/v1-company-workstation-return-2026-09-03.md)
and does not promote any long-campaign or release gate.

These are local diagnostics only. They do not close V1-03 through V1-18 or
V1-20 through V1-23, do not substitute for the six-hour/24-hour campaigns or
named migration canary, and do not authorize a version/tag or crates.io
publication.

## V1-15 Execution Update (2026-08-22)

V1-15 is `In progress`. The current owner audit keeps stateless producer/Admin
cache reuse separate from direct Fetch, group, Share, Streams, and telemetry
sessions that carry identity leases. Deterministic cache eviction, poisoned-
connection non-reuse, heartbeat cancellation, graceful close, bounded enqueue,
and nested-runtime rejection tests pass on source `e6de5c5`. The ownership table,
100 construct/use/fault/close cycles, final gauges, and published secured churn
profile remain open.

The static ownership table is now checked by
[`v1-15-ownership-inventory.json`](evidence/v1-15-ownership-inventory.json)
and [`check_v1_ownership_inventory.py`](../scripts/check_v1_ownership_inventory.py).
The inventory covers ten stable connection/session/task owners with finite
capacities, identity leases, saturation, cancellation, join, fault, and
verification fields; its five focused tests and CI check pass. This closes the
static inventory slice only. The 100-cycle gauge audit and exact published
secured churn gate remain open.

### V1-15 buffered owner-drop fence (2026-09-04)

At source `0e6c2057fe669d1522910294ec55a518ac2fda20`, the owning
`BufferedProducer` now aborts its Tokio worker on `Drop` instead of detaching
the task. The deterministic `dropping_buffered_producer_aborts_worker` test
passed, and the owner-drop contract is documented in the producer guides and
[`v1-buffered-owner-drop-2026-09-04.md`](evidence/v1-buffered-owner-drop-2026-09-04.md).
This closes only the owner-drop lifecycle boundary; graceful `close()` flush,
socket-I/O cancellation, 100-cycle gauges, and published secured churn remain
open.

## V1-16 Execution Update (2026-08-22)

V1-16 is `In progress`. SCRAM, TLS/mTLS, OAUTHBEARER provider single-flight,
rotation-window, outage, timeout, handshake, and credential-redaction tests are
present. Authentication/provider failures keep the configured security protocol
and poison the affected connection. Current-source rotation, restricted-principal
profiles, zero-secret artifact scan, and exact-candidate floor/current security
gates remain open.

The supported security contract is now machine-checked by
[`v1-16-security-contract.json`](evidence/v1-16-security-contract.json) and
[`check_v1_security_contract.py`](../scripts/check_v1_security_contract.py).
Seven transport/mechanism entries cover the source enum variants and explicitly
record validation, failure, rotation, redaction, and focused-test references;
the checker and five tests pass. This closes the deterministic contract slice
only. Live rotation, restricted-principal, and published security gates remain
open.

## V1-17 Execution Update (2026-08-22)

V1-17 is `In progress`. The bounded `ClientMetricsSnapshot` and KIP-714 provider
cover filtered cumulative/delta metrics, subscription negotiation, codec and
payload-limit handling, and a single-connection push path. The 60-minute
published collection profiles, broker replacement with stable ClientInstanceId,
throttle/mutation checks, published terminating-push count, and secret scan
remain open.

The public snapshot contract is now frozen and checked by
[`v1-17-metrics-contract.json`](evidence/v1-17-metrics-contract.json) and
[`check_v1_metrics_contract.py`](../scripts/check_v1_metrics_contract.py).
All 19 snapshot fields retain explicit types, units, lifecycle/aggregation
semantics, and maximum cardinality one; five focused tests and the CI check
pass. This closes the deterministic metric-inventory slice only. The published
telemetry collection and broker-replacement gates remain open.

The PushTelemetry cancellation boundary is also covered at source commit
`528986f08c1fc8cee6ee37c57f2e1ae8e92608cb`: after a scripted broker observes a
push frame, dropping `push_once` leaves the connection unusable and the next
push returns `NotConnected` instead of reusing an uncertain response. The
focused regression passed on Windows and company Ubuntu-T9 WSL2, with all 29
WSL2 fault-injection tests passing as well. This is local deterministic
evidence only; replacement, throttling, published terminating-push count,
secure published, and long-duration gates remain open. See
[`v1-telemetry-push-cancellation-2026-09-04.md`](evidence/v1-telemetry-push-cancellation-2026-09-04.md).

The deterministic terminating-shutdown boundary is now covered at source
commit `75644e4b5a2ae85e8764eacc95f8bc95102bcdbc`: a scripted broker validates
the single PushTelemetry v0 request, including its terminating bit,
subscription ID, compression, compact payload, tagged fields, and response;
the broker task is joined. The focused test passed on Windows and company
Ubuntu-T9 WSL2 (Rust 1.81.0), and WSL2 passed all 29 fault-injection tests.
This closes local request encoding and shutdown behavior only; published
60-minute collection, broker replacement, mutation/throttle behavior, secure
transport, and final task/resource/secret checks remain open. See
[`v1-telemetry-terminating-push-2026-09-04.md`](evidence/v1-telemetry-terminating-push-2026-09-04.md).

At source commit `c526c412460af17cacd3816f6c04709b34ca31f9`, all
`ClientMetrics` counter and current-gauge updates use atomic saturating
arithmetic. The `metric_atomic_updates_saturate_at_u64_boundaries` regression
passed at both overflow and underflow boundaries, so metrics cannot wrap to
zero or underflow during cleanup. This closes the deterministic arithmetic
slice only; published telemetry, secure broker replacement, and long-duration
gates remain open. See
[`v1-metrics-saturating-arithmetic-2026-09-04.md`](evidence/v1-metrics-saturating-arithmetic-2026-09-04.md).

At source commit `91c5592c6599eeb16df661616efa3fe0d5c7e0b4`, a deterministic
four-thread shared-`ClientMetrics` regression performs 100 synchronized
updates per worker and verifies exact counters, latency-bucket conservation,
and final in-flight state. The focused test passes on the company Windows x64
checkout and Ubuntu-T9 WSL2 with Rust 1.81.0. This closes the in-process
atomic-update consistency slice only. Published collection, broker replacement,
throttling, secure transport, and long-duration qualification remain open.
See [`v1-metrics-concurrency-2026-09-04.md`](evidence/v1-metrics-concurrency-2026-09-04.md).

## V1-18 Execution Update (2026-08-22)

V1-18 is `In progress`. Frame, collection, compact/tagged-field, decompression,
response-buffer, queue, and configuration boundaries reject malformed or
oversized input before the relevant allocation. Ten fuzz targets compile and
run. Discovery run
[32555867720](https://github.com/TaeeunKil/kafrust/actions/runs/32555867720)
passed all ten targets; it remains a 30-second-per-target smoke. The
versioned manifest at
[`docs/evidence/v1-18-fuzz-campaign-manifest.json`](evidence/v1-18-fuzz-campaign-manifest.json)
and its checker now declare the required 3,600 cumulative seconds per target,
four 900-second shards, 70-minute job timeout, and four weekly passes. The dedicated
[`fuzz-qualification.yml`](../.github/workflows/fuzz-qualification.yml) workflow
implements that matrix with retained per-shard statistics and artifacts. The
first cumulative qualification run
[32561454977](https://github.com/TaeeunKil/kafrust/actions/runs/32561454977)
passed all 40 target/shard jobs. Each artifact reports 900 seconds per shard
and 3,600 cumulative seconds per target; all 40 corpus hashes were verified
with no crash/OOM artifact files. Three additional weekly campaign passes and
retained crash/OOM dispositions remain required. The superseded
3,600-second-per-shard run was cancelled before evidence collection to avoid
overstating or wasting the campaign budget.
The second cumulative qualification run
[32635558822](https://github.com/TaeeunKil/kafrust/actions/runs/32635558822)
passed all 40 target/shard jobs from source `e90fc6c`. Its downloaded records
and corpus hashes passed the checked-in artifact audit with no crash/OOM files;
the manifest now records two of four weekly campaign sets. Two more weekly
passes, retained crash/OOM disposition, and the remaining allocation-boundary
and resource-limit evidence are still required, so V1-18 remains in progress.
The checked-in
[`check_v1_fuzz_qualification_artifacts.py`](scripts/check_v1_fuzz_qualification_artifacts.py)
now reproduces the 40-record, corpus-hash, resource-cap, and crash/OOM audit.
Two duplicate manual dispatches, [32645221503](https://github.com/TaeeunKil/kafrust/actions/runs/32645221503)
and [32645222921](https://github.com/TaeeunKil/kafrust/actions/runs/32645222921),
were cancelled after remaining queued for more than nine hours; they are not
counted as weekly campaign sets.

The reviewed allocation-boundary ledger is now machine-checked by
[`v1-18-allocation-boundary-ledger.json`](evidence/v1-18-allocation-boundary-ledger.json)
and [`check_v1_allocation_boundary_ledger.py`](../scripts/check_v1_allocation_boundary_ledger.py).
Fourteen boundary families name their source, finite limit, pre/during-allocation
validation, typed failure, and focused test; the checker and five tests pass.
This closes the static ledger slice only. Two additional weekly fuzz passes,
crash/OOM disposition, and complete resource-limit coverage remain required.

The 2026-09-03 company preflight did not produce fuzz evidence: Windows MSVC
cannot link the libFuzzer executables (`LNK1561`), while WSL2's missing Linux
nightly could not be downloaded because of DNS failure. No target or corpus was
changed. The retained details are in
[`v1-local-fuzz-preflight-2026-09-03.md`](evidence/v1-local-fuzz-preflight-2026-09-03.md);
the two additional weekly campaign sets remain pending.

## V1-19 Execution Update (2026-08-22; completed 2026-08-23)

V1-19 is `Done` at packaged-candidate evidence level. The staged `0.3.6` package candidate passes five
feature profiles on Rust 1.81.0 and stable in
[CI run 32545563612](https://github.com/TaeeunKil/kafrust/actions/runs/32545563612).
The manifests contain no librdkafka/C client binding and the source crates
forbid unsafe code. Feature-specific dependency reports, optional-TLS native
tool detection, license/advisory/yank review, reviewed transitive-native
ownership, reproducible package-pair SBOMs, and drift gates remain open. The
exact-HEAD refresh in [CI run 32559004319](https://github.com/TaeeunKil/kafrust/actions/runs/32559004319)
regenerated archives with protocol SHA-256
`ee191756dddae5b5d591c935416a0d06720f5ea90a7bdab8233734b0bb893768` and client
SHA-256 `00f656d820b11df0d06d56c9bd6869810f28f7c14242d838a4b1bfed6c675325`;
all five external feature profiles passed on both toolchains. This is still
packaged-candidate evidence, not published-artifact or full dependency-audit
completion.

A local deterministic dependency-graph slice on exact HEAD `1f6c60d` recorded
56/65/56/72/81 unique normal-edge packages for default/tls/blocking/otlp/all
profiles and found no librdkafka, rdkafka-sys, kafka-sys, or rdkafka package.
The record is in
[`docs/evidence/v1-19-dependency-audit.md`](evidence/v1-19-dependency-audit.md);
optional TLS native-tooling, advisory/license/yank, transitive-native review,
SBOM, and drift gates remain explicitly open.

The dependency checker now also scans full-graph Cargo metadata (using the
ignored lockfile when present). On commit `ab87cd7` it found license metadata for all 71 resolved packages with
zero missing `license`/`license_file` fields; the slice is recorded in
[`docs/evidence/v1-19-license-metadata-audit.md`](evidence/v1-19-license-metadata-audit.md)
and ledger row `Q-LOCAL-V119-002`. This is metadata completeness evidence
only; compatibility, advisory/yank, native/unsafe ownership, SBOM, and drift
reviews remain open.

The fresh-checkout dependency checker fixes passed on both Rust 1.81.0 and
stable in [CI run 32561532044](https://github.com/TaeeunKil/kafrust/actions/runs/32561532044)
from `f499ee6`; this confirms the metadata-completeness slice while the
remaining V1-19 audit gates stay open.

  The reproducible SBOM slice is now checked in at commit
`1ec37af6ee47ebcf995b9927e008700a8ad584da` (the drift-policy follow-up
to `6e24a247c2aa4aa9c63086e68d753988adbfe3aa`). The new
[`check_v1_sbom.py`](scripts/check_v1_sbom.py) follows locked normal/build
edges for the explicit Linux release platform, excludes dev-only edges,
requires license metadata for every component, and verifies the checked-in
CycloneDX 1.5 document after staged package creation in CI. The artifact
contains 89 components and is recorded with its digest in
[`docs/evidence/v1-19-sbom.md`](evidence/v1-19-sbom.md). This is a completed
SBOM/drift-inventory slice, not completion of V1-19: advisory/yank review,
optional-TLS native tooling, transitive unsafe/native ownership, and the full
feature/platform package audit remain open.

The CI comparison allows only transitive version re-resolution caused by
platform or Cargo index state. Workspace versions, direct dependency
versions, package names, licenses, source kinds, and graph edges remain strict
drift gates.

The native-tooling slice is recorded at commit
`83864c1058347dd753608307bdd5ab1d7eb68be3`. Its checker covers the five
feature profiles on the same explicit Linux target and runs the default
package with nonexistent C/C++/archiver/pkg-config tools. The default,
`blocking`, and `otlp` profiles have no native candidates; `tls` and `all`
explicitly record `ring` as the custom-build candidate. This closes the
no-C-default and optional-TLS-posture slice, while advisory/yank and reviewed
transitive unsafe/native ownership remain open.

The license-expression slice is now recorded in
[`docs/evidence/v1-19-license-policy.md`](evidence/v1-19-license-policy.md).
The all-feature runtime/build closure contains 89 packages, all with SPDX
expressions drawn from the explicit permissive allowlist, and CI rejects
license-policy or package-identity drift. This closes license-expression
compatibility metadata only; advisory/yank review, packaged notice inspection,
and reviewed transitive unsafe/native ownership remain open.

The transitive unsafe/native inventory is recorded in
[`docs/evidence/v1-19-unsafe-native-inventory.md`](evidence/v1-19-unsafe-native-inventory.md).
It scans the 89-package closure, finds zero unsafe constructs in the two
workspace crates, and records 62 third-party review entries with owner and
rationale fields. The companion owner-review matrix covers all 62 entries and
six named native/platform boundaries with candidate-only dispositions. It does
not claim a source audit of every upstream unsafe block or final 1.0.0 risk
acceptance; package/target/advisory changes require rerunning the matrix.

The local registry provenance slice also records checksums for 87 registry
packages with zero missing or yanked entries. It is explicitly local-index
evidence; live advisory/current-index review remains open.

The dated advisory slice now records an OSV/RustSec query for all 89 resolved
packages in [`v1-19-advisories.md`](evidence/v1-19-advisories.md): zero advisory
matches and zero critical/high matches at the pinned 2026-08-23 review. The
offline CI gate enforces the exact inventory and a 30-day freshness window; it
does not claim future or undisclosed vulnerability coverage. Manual
unsafe/native ownership, multi-platform/package evidence, and published
artifact gates remain open.

All current V1-19 dependency-hardening checkers passed on both Rust 1.81.0 and
stable in [CI run 32610559177](https://github.com/TaeeunKil/kafrust/actions/runs/32610559177)
from exact source `af73b8d`. This is CI confirmation of the slices, not V1-19
completion or publication readiness.

The advisory follow-up and drift-tolerant gate passed on both toolchains from
exact source `68f7775` in [CI run 32611666435](https://github.com/TaeeunKil/kafrust/actions/runs/32611666435).
This confirms the new advisory snapshot wiring only; manual unsafe/native
ownership, multi-platform/package evidence, and published-artifact gates still
block V1-19 completion.

The OSV/RustSec snapshot was refreshed on 2026-09-03 at source
`34bbd443b835cd80056d330b21aa44ddc06ff6e0` after a resolved dependency
inventory drift. All 89 queried packages returned zero advisory matches and
zero critical/high matches; the offline freshness/inventory check now passes.
This refresh is dated evidence and does not close multi-platform, owner-review,
published-artifact, or release gates.

The final owner-review matrix and exact-head package validation then passed both
toolchains from `a3df635` in [CI run 32612304536](https://github.com/TaeeunKil/kafrust/actions/runs/32612304536).
This closes V1-19's packaged-candidate exit criteria. The next gate is V1-20's
fresh published `0.3.6` matrix. The dated competitive review and exact c0bb728
CI gate now authorize one protocol-first `0.3.6` pre-1.0 publication attempt;
no tag, GitHub release, or `1.0.0` claim is implied.

The dated version-readiness decision is archived in
[`docs/evidence/v1-release-competitive-readiness-2026-08-23.md`](evidence/v1-release-competitive-readiness-2026-08-23.md):
the next identity remains pre-1.0 `0.3.6`, while `1.0.0` is not justified yet.
The ordered protocol-first publication completed on 2026-08-23 after the
hardening and competitor gates passed. This is a published pre-1.0 boundary,
not a completed V1-20 matrix or a `1.0.0` authorization.

Release planning now also requires a dated competitor comparison and a
version-readiness decision before any RC/stable artifact is published. The
agent may make the publication decision autonomously only after those results,
the complete milestone gates, and exact artifact verification show no material
gap; otherwise the milestone graph and roadmap must be replanned around the
needed intermediate version.

The staged package boundary was refreshed on 2026-09-03 from source
`b4505903e9b15b3e7452b9e6f8e9cbf3f6ea679b`. The actual 0.3.6 crate pair passed
default, TLS, blocking, OTLP, and all-feature consumer fixtures with locked
dependency trees. Exact tarball hashes are retained in
[`v1-package-boundary-2026-09-03.md`](evidence/v1-package-boundary-2026-09-03.md).
This does not publish the crates or close live/release gates.

## V1-20 Execution Update (2026-08-22)

V1-20 is `In progress`. The draft machine-readable matrix at
[`docs/evidence/v1-20-compatibility-matrix.json`](evidence/v1-20-compatibility-matrix.json)
now preserves the accepted V1-01 broker order, pairwise topology/security
  profiles, feature/toolchain package rows, and the protocol-first exact-registry
  policy. `scripts/check_v1_compatibility_matrix.py` validates ten draft profiles
and is wired into CI. The initial draft had no passed published rows; the
published boundary and named rows are now recorded below, while fresh lockfiles
and the inherited V1-15~V1-19 artifact gates are still required before the
matrix can be frozen. The source-only 17-job Live Kafka Smoke matrix passed on
commit `e6de5c5` in
[run 32551145773](https://github.com/TaeeunKil/kafrust/actions/runs/32551145773),
including secured SCRAM transaction failover after the fencing-code assertion
was corrected to accept Kafka code 47 or 90. This does not qualify a published
artifact or close the long-duration and downstream milestone gates.

The 17-job source-only matrix was rerun from `4f6918b` in
[run 32555053351](https://github.com/TaeeunKil/kafrust/actions/runs/32555053351)
and all jobs passed across the four broker lines, security profiles, KIP-848,
Admin, response-loss, and multi-broker failover slices. It remains source-only:
the exact published `0.3.6` pair, fresh lockfiles, and V1-20 published exit
criteria are still pending. The ordered `0.3.6` publication and exact external
lockfile boundary now pass; the seven-profile published smoke run
[32613844625](https://github.com/TaeeunKil/kafrust/actions/runs/32613844625),
Kafka 3.7.2 classic failover [32613851826](https://github.com/TaeeunKil/kafrust/actions/runs/32613851826),
and Kafka 4.3.1 KIP-848 failover [32613855210](https://github.com/TaeeunKil/kafrust/actions/runs/32613855210)
also passed. Additional exact-published `0.3.6` rows then passed API 74
configuration, DescribeCluster, ConsumerGroupDescribe, KIP-848 churn and
regex assignment, member-aware offsets, Share/Streams runtime, Streams API
surface, Share state failover, and Share multi-member ownership in the retained
[published evidence](evidence/v1-20-published-smoke-2026-08-23.md) and ledger.
The first Share multi-member attempt exposed a workflow variable-scope defect,
which was corrected in `546a3a1` and passed on rerun
[32614372643](https://github.com/TaeeunKil/kafrust/actions/runs/32614372643).
The default-timing Share member-loss and eight-cycle repeated-loss rows also
passed ([32614875041](https://github.com/TaeeunKil/kafrust/actions/runs/32614875041),
[32615024395](https://github.com/TaeeunKil/kafrust/actions/runs/32615024395)),
and the pinned-current secure Kafka 4.3.1 KIP-848 leader-failover row passed in
[32615403411](https://github.com/TaeeunKil/kafrust/actions/runs/32615403411).

The current-source matrix was refreshed once more from `e90fc6c` in
[run 32635573529](https://github.com/TaeeunKil/kafrust/actions/runs/32635573529).
All 17 jobs passed across the four broker versions, plaintext/TLS/SASL
profiles, ACL, transactions, KIP-848, and three-broker failover slices. The
result is recorded as `Q-LIVE-MATRIX-003`; it remains source-only evidence and
does not close V1-20 or authorize a release.

The published crate smoke workflow now covers twelve draft continuity,
security, and codec profiles, including explicit PLAIN and SCRAM-SHA-512
authentication; Kafka 3.8.1, 3.9.1, and 4.0.0 classic rows passed alongside
the existing floor/pinned/codecs in
[32616834901](https://github.com/TaeeunKil/kafrust/actions/runs/32616834901).
The final run also records and resolves one PLAIN readiness-probe correction
and one transient 3.8.1 coordinator retry. The full accepted published matrix,
mechanism-specific source rows, long-duration campaigns, and downstream
release gates remain open.

The exact published-matrix documentation and continuity-workflow changes pass
both Rust 1.81.0 and stable in [CI run 32615676173](https://github.com/TaeeunKil/kafrust/actions/runs/32615676173). The subsequent workflow-only
security-profile corrections are recorded in the published evidence and are
validated again with the current documentation/ledger head `3fc4641` in
[CI run 33715029984](https://github.com/TaeeunKil/kafrust/actions/runs/33715029984)
on both Rust 1.81.0 and stable.

On 2026-09-03, the exact published `0.3.6` pair passed all twelve published
smoke profiles again in [run 33714006944](https://github.com/TaeeunKil/kafrust/actions/runs/33714006944)
from workflow head `bc0b40e`. The rerun covers Kafka 3.7.2/3.8.1/3.9.1/4.0.0/
4.3.1, classic and KIP-848, PLAIN, SCRAM-256/512, and all four codecs, with
fresh external lockfiles and retained outputs. The workflow now proves group
coordinator readiness with a real describe request; the two preceding startup
races remain diagnostics. This refresh strengthens named published rows only:
the full V1-20 matrix, long fault/SLO gates, migration canary, API freeze, and
release gates remain open. Details are in
[`v1-20-published-smoke-rerun-2026-09-03.md`](evidence/v1-20-published-smoke-rerun-2026-09-03.md).

The latest current-source live matrix was rerun from documentation head
`c504205` in [run 33714444474](https://github.com/TaeeunKil/kafrust/actions/runs/33714444474).
All 17 jobs passed across Kafka 3.7.2/3.8.1/3.9.1/4.3.1, plaintext/TLS/SASL,
classic/KIP-848, codecs, Admin/transaction, telemetry-adjacent, and
three-broker failover paths. This refresh is short current-source evidence;
it does not substitute for the exact published matrix, six-hour/24-hour
campaigns, V1-22 SLO repetitions, or V1-23 service canary. The immutable
record is [`v1-live-matrix-rerun-2026-09-03.md`](evidence/v1-live-matrix-rerun-2026-09-03.md).

## V1-21~V1-26 Release-Path Preparation (2026-08-22)

V1-21 and V1-22 are `In progress` while the exact published matrix and
long-duration fault/SLO campaigns are pending; V1-23 is `Blocked` until a
named migration service and approved canary environment are supplied.
Their preparation records explicitly preserve the six-hour/24-hour campaign
capacity, five-repetition performance requirements, and canary rollback
prerequisites. The V1-21 fault manifest/checker now names the four six-hour
campaigns, 100 member-loss cycles, and 100 outcomes per ambiguity family; the
V1-22 performance manifest/checker names six representative profiles, five
eight-hour repetitions, ten-second samples, and regression/RSS/retry limits.
The V1-23 migration manifest/checker and manual reference-canary workflow now
define isolated kafrust/rust-rdkafka smoke topics, a 1,000-record smoke gate,
and the later million-record forward/rollback exit gate. The current-source
baseline smoke passed in [run 32552631034](https://github.com/TaeeunKil/kafrust/actions/runs/32552631034)
after the comparison-only stable toolchain and libcurl runner fixes; the
artifact records 1,000 records per implementation and zero normalized
divergence. Historical smoke runs are not promoted into the long-duration or
service-canary gates.

The isolated migration smoke was rerun from `6bcf1ef` in
[run 32555867981](https://github.com/TaeeunKil/kafrust/actions/runs/32555867981).
Both implementations processed 1,000 unique 1-KiB records with batch size 100
and zero normalized divergence; this refreshes source evidence only and does
not close the named-service, fault, forward-cutover, rollback, or million-record
exit gate. The comparison fixture now embeds business IDs and compares
unique/loss/duplicate counts plus a SHA-256 payload digest. Exact-HEAD run
[32557407734](https://github.com/TaeeunKil/kafrust/actions/runs/32557407734)
passed the strengthened reconciliation with 1,000 unique records per
implementation, zero loss/duplicates, and matching digest; the stronger smoke
still does not close the named-service, fault, forward-cutover, rollback, or
million-record exit gate.

## V1-21 Execution Update (2026-08-23)

V1-21 is `Blocked` pending recovery of the registered WSL2 self-hosted
runner. The published multi-broker soak now accepts campaign
and segment identity, emits immutable artifact/workflow/broker descriptors, and
supports a six-hour-compatible job timeout. The exact published `0.3.6` pair
passed one bounded Kafka 4.3.1 three-broker restart segment in
[32618344222](https://github.com/TaeeunKil/kafrust/actions/runs/32618344222)
after two retained fixture diagnostics: `Acks::Leader` left 100 records
unreconciled, and a gauge formatter mislabeled the in-flight peak. The final
segment uses `Acks::All`, has zero final gauges, and the current fixture emits
qualified per-segment business-ID reconciliation with a canonical digest. Its
descriptor still marks cross-segment continuity as unqualified because each
runner starts a fresh broker. The six-hour campaigns, 100 churn cycles,
ambiguity families, controlled data-loss fixtures, and cross-segment
continuity remain open.

The secure simultaneous-loss fixture then exposed and corrected an
unknown-outcome handling defect. Runs
[32632600261](https://github.com/TaeeunKil/kafrust/actions/runs/32632600261)
and [32633284143](https://github.com/TaeeunKil/kafrust/actions/runs/32633284143)
discarded failed batches after `NOT_ENOUGH_REPLICAS` and therefore reported
identity gaps. Commit `15741d8` enables idempotence, retains failed batches for
replay, and fails explicitly when a pending outcome remains unresolved. The
corrected published `0.3.6` secure segment
[32633658046](https://github.com/TaeeunKil/kafrust/actions/runs/32633658046)
reconciled 6,089,400 unique records through a simultaneous two-broker outage
with zero loss/duplicates and drained gauges. It observed 300 operation
errors, 5 retries, and 30,000 transient unknown attempts that were recovered;
this is stronger per-segment evidence, not six-hour or cross-segment
qualification.

The hardened schedule was then exercised from `dd605ff` in published run
[32638787704](https://github.com/TaeeunKil/kafrust/actions/runs/32638787704):
one 120.005-second Kafka 4.3.1 three-broker segment ran leader, coordinator,
combined, and simultaneous fault events at 25/50/70/85% with ten-second
outages. It reconciled 3,553,800 unique records with six failed requests, 15
retries, zero unknown/loss/duplicate outcomes, drained gauges, and a qualified
single-segment continuity claim. This validates the schedule harness only; the
long campaign, churn, ambiguity-family, and controlled-data-loss gates remain
open.

The same four-event schedule passed the published SASL_SSL/SCRAM-SHA-256 path
in [run 32639181770](https://github.com/TaeeunKil/kafrust/actions/runs/32639181770):
120.002 seconds, 2,943,900 unique records, 24,532 records/s, four failed
requests, 11 retries (0.000374%), zero unknown/loss/duplicate outcomes, and
drained gauges. This strengthens secured per-segment evidence only; the
six-hour campaigns and downstream V1-21 gates remain open.

The published Share repeated-member-loss workflow now accepts a bounded
`cycles` input and dynamically checks per-member ownership records and the
final cycle, while retaining the historical eight-cycle default. A qualification
run may request exactly 100 cycles and receives a retained JSON summary marked
qualified only at that bound. The 100-cycle run
[32642115585](https://github.com/TaeeunKil/kafrust/actions/runs/32642115585)
became a stale hosted-run cancellation after more than ten hours without a
summary; it is infrastructure-only evidence and was not promoted.

The first parallel six-hour fault set also revealed hosted-runner disk
exhaustion caused by unbounded broker logs during failure diagnostics. The
plaintext and secured multi-broker workflows now use three 50-MiB JSON log
files per broker and retain only the last 200 lines when a run fails. The
affected runs are infrastructure failures requiring rerun, not client fault
qualification evidence.

The first rerun with bounded broker logs exposed a second log-volume source:
the published soak clients emitted every retryable Produce/Fetch error during
an outage. That run again exhausted the hosted runner before its descriptor
could be retained. Both published soak clients now rate-limit repeated error
diagnostics to one line per ten seconds while preserving counters and final
reconciliation; the failed run remains infrastructure-only evidence and must
be rerun from the bounded-error-log HEAD.

The published classic/KIP-848 group-rebalance fixture now accepts up to 100
cycles (previously 64), with a cycle-scaled client timeout and a 35-minute job
bound. This only enables the required executions; no 100-cycle published result
is counted before its run output is retained and checked. The exact published
classic run [32642112754](https://github.com/TaeeunKil/kafrust/actions/runs/32642112754)
now passes the 100-cycle gate and uploads an immutable JSON summary with the
published lockfile digest and broker image identity. This closes only the
classic group sub-gate. The KIP-848 consumer run
[32642114151](https://github.com/TaeeunKil/kafrust/actions/runs/32642114151)
also completed its 100-cycle gate on Kafka 4.3.1 with the same zero-loss,
zero-duplicate, drained-gauge result. The group-family 100-cycle sub-gates are
now both covered; Share, long fault, ambiguity, and controlled-data-loss gates
remain open.

The promoted classic summary records source `7cc0b55`, `cycle_count=100`, six
partitions, zero ownership loss/duplicates, and drained final gauges. It is
published-artifact evidence for that exact group protocol and broker profile,
not six-hour fault, Share, ambiguity-family, controlled-data-loss, SLO, or V1
completion evidence. The second row has the same exact published artifact,
lockfile, and result contract for KIP-848 consumer groups.

Share 100-cycle attempts with one- and ten-second member dwell both ended before
the surviving member reacquired its first post-loss assignment. These are
harness timing failures, not data-loss results; the qualification rerun uses
the existing 120-second dwell that the bounded workflow has already exercised.

The published multi-broker soak workflow now accepts and validates an ordered
fault schedule (`leader`, `coordinator`, `combined`, and `simultaneous` events
at increasing percentages of the segment). The parser has focused boundary
tests, and a one-segment descriptor can mark continuity qualified while
multi-segment runner-local descriptors remain unqualified. This hardens the
campaign harness, including the published SASL_SSL/SCRAM workflow's six-hour
timeout contract, but does not itself provide the required six-hour campaign
results or completion evidence.

After exact-head CI run
[32644139214](https://github.com/TaeeunKil/kafrust/actions/runs/32644139214)
passed, the four V1-21 six-hour published campaigns were relaunched from
commit 7c24f7399325b5d0ab6f91f6e2ecd4d5b49985ec: plaintext Kafka 3.7.2 floor
run [32644605379](https://github.com/TaeeunKil/kafrust/actions/runs/32644605379)
and secured Kafka 4.3.1 runs
[32644605637](https://github.com/TaeeunKil/kafrust/actions/runs/32644605637),
[32644605633](https://github.com/TaeeunKil/kafrust/actions/runs/32644605633),
and [32644605743](https://github.com/TaeeunKil/kafrust/actions/runs/32644605743).
They are recorded as In progress/not-run until immutable descriptors and the
fault-result adjudicator pass; no long-soak or V1-21 completion claim follows
from launch alone.

The same four runs later exceeded nine hours wall time without descriptors.
Runs [32644605379](https://github.com/TaeeunKil/kafrust/actions/runs/32644605379),
[32644605637](https://github.com/TaeeunKil/kafrust/actions/runs/32644605637),
and [32644605743](https://github.com/TaeeunKil/kafrust/actions/runs/32644605743)
were cancelled as stale infrastructure runs; [32644605633](https://github.com/TaeeunKil/kafrust/actions/runs/32644605633)
remained stuck in `in_progress` after cancellation was requested. No run
produced a result descriptor, so none is client or V1-21 evidence. The workflow
now requires a self-hosted-capable runner before any replacement dispatch.
The previously stuck secure run `32644605633` later completed as `cancelled`,
so all four long runs are terminal infrastructure non-results.

The V1-22 `throughput_benchmark` example now has a timed campaign mode with
barrier-synchronized warmup/measurement windows, worker-per-partition
concurrency, configurable JSONL samples (ten-second campaign target), RSS/retry/latency fields, and final
record/gauge reconciliation. It now also emits attempted/acknowledged/unknown
outcome counts and qualified business-ID expected/observed SHA-256 digests;
the adjudicator rejects missing or mismatched identity evidence. The bounded
manual diagnostic workflow archives
short current-source runs only; it does not promote them to the required
five-repetition, eight-hour published SLO campaign.

Exact-HEAD diagnostic run
[32558818231](https://github.com/TaeeunKil/kafrust/actions/runs/32558818231)
used source `69e4997`, two workers/two partitions, 5s warmup, 20s measured,
5s samples, and 50-record 1-KiB batches. It roundtripped 1,546,200 records
with zero failed requests/retries and zero final gauges; the raw JSONL artifact
is retained. This is harness evidence only, not V1-22 completion.

## V1-22 Execution Update (2026-08-23)

The published timed-campaign diagnostic now runs the same harness from a fresh
external project against exact crates.io `kafrust = "=0.3.6"`, with a retained
lockfile hash, broker image reference/ID, campaign/repetition identity, and raw
JSONL descriptor. Final run
[32619372203](https://github.com/TaeeunKil/kafrust/actions/runs/32619372203)
covered Kafka 3.7.2 and 4.3.1 with none/Zstd using two workers, a 5s warmup,
20s measurement, 5s samples, 50-record batches, and 1-KiB values. All four
jobs reconciled produced and consumed records, had zero retries/failed
requests, and drained final gauges. The first run exposed missing fixture
tracing dependencies and the second exposed a Docker image-identity assumption;
both failures and their fixes are retained in
[`v1-22-performance-diagnostic-2026-08-23.md`](evidence/v1-22-performance-diagnostic-2026-08-23.md).
The descriptors are explicitly `qualified=false`: five repetitions, six
profiles, secured/three-broker topology, two-hour warmup, six-hour measurement,
RSS/regression adjudication, and baseline locking remain open.

The identity-reconciled rerun
[32625017236](https://github.com/TaeeunKil/kafrust/actions/runs/32625017236)
from `99ef31f` passed all four bounded published combinations with attempted =
acknowledged = consumed, zero unknown/loss/duplicate outcomes, and matching
expected/observed SHA-256 business-ID digests. It remains diagnostic evidence:
the descriptors are still `qualified=false`, and the full six-profile,
five-repetition, eight-hour matrix and locked baseline are not closed.

The result-bundle adjudicator is now implemented in
[`scripts/check_v1_performance_results.py`](../scripts/check_v1_performance_results.py)
and covered by focused malformed-bundle, incomplete-matrix, and regression
tests. It requires the full profile/topology/security/repetition matrix,
contiguous ten-second windows, one artifact digest, drained resources, and a
locked baseline before it can return a passing result. This makes the eventual
campaign decision reproducible; no qualified bundle or locked baseline exists
yet, so V1-22 remains `In progress` while the bounded before/after diagnostics
and the full campaign preparation continue.

The manual V1-22 Published Performance Campaign workflow now exposes the
complete executable matrix: six named profiles, two Kafka 4.3.1 topologies,
PLAINTEXT and SASL_SSL/SCRAM-SHA-256, and five repetitions (120 jobs). Each
profile job resolves a fresh external project against one exact published
artifact digest, runs the two-hour warmup plus six-hour measured window, and
uploads the descriptor and relative JSONL result required by the adjudicator.
The aggregate job can require a checked-in locked baseline; without one it
reports matrix-complete-baseline-pending. The workflow now defaults to a
pinned `self-hosted` runner label and rejects hosted labels before the matrix
starts, because the eight-hour contract cannot fit the [hosted six-hour job
limit](https://docs.github.com/en/actions/reference/limits). No self-hosted
runner is currently registered, so this is an explicit external-capacity gate.
No campaign has been dispatched yet, so this closes the
execution-path preparation gap but does not qualify V1-22 or authorize a
release.

The benchmark profile paths are now explicit rather than label-only. The
`immediate` profile uses `Producer::send_batch`, `buffered` uses the bounded
`BufferedProducer` queue and waits for each delivery, and `direct-consumer`
assigns a partition and drains it through `Consumer::poll`. The final JSONL
record emits `campaign_mode`, while the adjudicator requires the descriptor and
final mode to match the manifest profile. This closes the harness identity gap;
it is not a qualification result and does not change the current no-publication
decision.

The exact published `kafrust 0.3.6` artifact also passed the new buffered mode
diagnostic in [32629657794](https://github.com/TaeeunKil/kafrust/actions/runs/32629657794)
across Kafka 3.7.2/4.3.1 and none/Zstd. All four jobs reconciled identities and
drained final gauges; the retained descriptors are explicitly
`qualified=false`. This is recorded in
[`v1-22-performance-mode-diagnostic-2026-08-23.md`](evidence/v1-22-performance-mode-diagnostic-2026-08-23.md)
and `Q-PUBLISHED-V122-MODE-001`, and leaves the full SLO matrix and locked
baseline open.

The manual [`Kafka Benchmark Profile Diagnostic`](.github/workflows/benchmark-profile-diagnostic.yml)
now captures runner/kernel/CPU identity, `/usr/bin/time -v` counters, optional
Linux `perf stat` data, and reconciled timed JSONL for immediate, buffered, and
direct-consumer paths. Its descriptors are deliberately `qualified=false`; it
is the before/after profiling input for the V1-22 batching/concurrency/queue
re-plan, not a locked baseline or release authorization.

The first source before/after profile pair is retained in
[`v1-22-performance-profile-before-after-2026-08-23.md`](evidence/v1-22-performance-profile-before-after-2026-08-23.md): concurrent buffered
delivery waits cut measured context switches by approximately 73% but did not
improve its roughly 13.5k records/s throughput. This keeps the optimization
work open and directs the next slice to buffered queue batching and repeated
encoded-size work; no SLO or publication decision follows from this pair.

The next retained pair compares source `fa419f2` (workflow
[32631000062](https://github.com/TaeeunKil/kafrust/actions/runs/32631000062))
with `e4f4f60` (workflow
[32631701269](https://github.com/TaeeunKil/kafrust/actions/runs/32631701269)).
The latter removes `ProducerRecord` clones from the repeated buffered
encoded-size check without changing the wire-length calculation. In the
bounded 1-KiB/60-second profile, buffered throughput increased from 13,480 to
18,283 records/s and p99 fell from 25 ms to 10 ms; context switches rose in
the after run. This is a promising diagnostic signal, not a locked baseline,
SLO qualification, or publication decision. The next slice repeats the result
and measures queue batching before another release decision. Replication
[32632004251](https://github.com/TaeeunKil/kafrust/actions/runs/32632004251)
measured 16,217 buffered records/s with the same 10 ms p99 but 1.83M context
switches. The two after results stay above the 13,480 records/s predecessor,
yet the surrounding hosted-runner variation means no stable percentage claim
or release decision is made; a controlled repetition set remains next.

The next source optimization (`07705c5`) narrows the buffered enqueue
threshold scan to the newest request's topic/partition group without changing
record-count, encoded-byte, flush, or delivery semantics. Same-source runs
[32634691272](https://github.com/TaeeunKil/kafrust/actions/runs/32634691272) and
[32634877270](https://github.com/TaeeunKil/kafrust/actions/runs/32634877270)
reconciled all four profiles with zero retries, unknown outcomes, loss,
duplicates, and drained gauges. Buffered throughput measured 15,403 and
16,203 records/s with p99 10 ms; context switches measured 378,946 and
1,349,630. This retains the change as a semantics-preserving diagnostic, but
the hosted-runner variation does not establish a stable percentage, lock a
baseline, qualify V1-22, or authorize a publication.

Commit `866509a` additionally reuses the maintained oldest pending enqueue time
for the buffered delivery-timeout wake-up instead of scanning the queue on
each select-loop iteration. Combined source profile
[32635871875](https://github.com/TaeeunKil/kafrust/actions/runs/32635871875)
reconciled all four paths with zero retries, unknown outcomes, loss,
duplicates, and drained gauges; buffered throughput was 15,977 records/s with
p99 10 ms and 39,124 KiB maximum RSS. The result remains within hosted-runner
variation and is recorded as diagnostic evidence only; V1-22's baseline and
SLO gates remain open.

V1-21 now has the corresponding cross-segment adjudicator in
[`scripts/check_v1_fault_results.py`](../scripts/check_v1_fault_results.py).
It rejects count-only or continuity-unqualified diagnostics and requires
contiguous segment identity, qualified unique-record reconciliation, one
published artifact digest, drained gauges, the six-hour totals, the
100-cycle/100-outcome family gates, and declared data-loss fixture matches.
Both plaintext and secure published soak fixtures now emit the per-segment
fields; no artifact currently satisfies the cross-segment qualified contract.

The WSL2 runner was subsequently registered and passed the non-qualification
diagnostic, but the first declared six-hour campaign
[`32649020906`](https://github.com/TaeeunKil/kafrust/actions/runs/32649020906)
lost its Ubuntu-T9 instance during the soak. WSL returned
`CreateInstance/E_FAIL` and `E_UNEXPECTED` on restart, the runner went
offline, and GitHub closed the run as `failure` without an artifact. This is an
infrastructure non-result, recorded in the
[`capacity audit`](evidence/v1-long-campaign-capacity-audit-2026-08-24.md);
the exact manifest campaign must be rerun after an elevated WSL/Hyper-V
recovery. Follow-up inspection found the `T:` host volume has only
`31,256,576` free bytes (`29.8 MiB`) while the registered `ext4.vhdx` is
approximately `773.8 GiB`; the near-full host volume is the primary recovery
hypothesis. No V1-21 ledger row, V1-22 campaign, RC, stable release, or
publication is authorized by this failed run.

## V1-21 Capacity Recovery Update (2026-08-24)

The export backup was moved to `C:\Users\user\Backups\Ubuntu-full.tar` and
the old `T:\Backups\Ubuntu-full.tar` copy was removed. `Ubuntu-T9` now starts
normally with `185 GiB` free on its Linux filesystem. Live Docker inspection
found the concrete VHDX consumers: the three exited
`kafrust-published-secure-multi-soak-{1,2,3}` containers created by failed run
`32649020906` each retain a `211 GB` writable layer (`631.7 GB` total), while
Docker build cache reports `85.89 GB` (`61.44 GB` reclaimable). Their Kafka
log/data trees are stale failure-run state, not qualification evidence.

The configured `wsl-ubuntu-t9` listener was restarted manually because its
runner service was not installed. GitHub now reports one idle online runner
with the required `self-hosted`, `Linux`, `X64`, `docker`, and `wsl2` labels.
This restores execution capacity but does not qualify V1-21 or V1-22. The
stale containers/cache must be explicitly cleaned before the exact V1-21
six-hour manifest is rerun; no RC, stable release, or registry publication is
authorized by this recovery state.

The three stale containers were removed with their anonymous data volumes and
the unused Docker build cache was pruned. WSL then reported `110 GiB` used and
`846 GiB` available. A fresh export completed with exit code `0` at
`T:\Backups\Ubuntu-full-2026-08-24.tar` (`115,386,419,200` bytes). The old C:
copy was deleted only after that success; the new export remains a
same-volume recovery copy, not independent disaster-recovery storage. The
runner is online and idle, but the exact V1-21 six-hour manifest still must be
rerun and adjudicated before any release decision.

The incident and prevention runbook is now recorded in
[`v1-wsl-capacity-incident-2026-08-24.md`](evidence/v1-wsl-capacity-incident-2026-08-24.md).
Long-campaign workflows check the Windows volume that owns the WSL VHDX before
dispatch, refuse insufficient host/Docker capacity, and always remove their
campaign-scoped containers, anonymous volumes, networks, and stale build
cache. The WSL runner is also installed as an enabled systemd service. These
are operational safety controls; they do not qualify the failed campaign or
authorize a release.

## Non-Long Validation Update (2026-09-03)

The company workstation is Windows x64, with an x86_64 WSL2 environment
available for diagnostics. Existing Docker resources were inspected without
prune or mutation. From source `924540c`, all required local Rust validation,
the V1 static/manifest gates, staged package profiles, and the 89-component
Linux-target SBOM passed. The exact-head UpdateFeatures transaction workflow
had failed twice because an idle Admin connection reused stale ApiVersions
feature metadata after a successful mutation. The cache invalidation fix and
scripted-broker regression are recorded in
[`v1-nonlong-validation-2026-09-03.md`](evidence/v1-nonlong-validation-2026-09-03.md).
The pushed CI run is
[33698482547](https://github.com/TaeeunKil/kafrust/actions/runs/33698482547);
the fixed Kafka 4.3.1 UpdateFeatures workflow then passed in
[33698683806](https://github.com/TaeeunKil/kafrust/actions/runs/33698683806),
covering the safe downgrade and upgrade lifecycle. This closes the named
diagnostic only; no long-campaign, service-canary, `0.3.7`, or `1.0.0` claim
is made.

## V1-23 Execution Update (2026-08-23)

The current published competitor check was run before making any release-path
decision. In [32619987006](https://github.com/TaeeunKil/kafrust/actions/runs/32619987006),
crates.io `kafrust 0.3.6` and `rust-rdkafka 0.39.0` processed isolated 20,000-
record, 1-KiB, batch-200 workloads on Kafka 4.3.1 for three repetitions each.
All rows reconciled identical business-ID digests with zero loss and
duplicates. Median kafrust throughput was 76,625 Produce and 272,659 Consume
records/s versus 153,297 and 590,467 for rust-rdkafka, approximately 50.0%
and 46.2% respectively. This is not a universal performance ranking, but it
is a material workload-specific gap; V1-22 profiling/SLO work and target-service
performance review remain required before a 1.0.0 decision. The comparison
and its non-claims are recorded in
[`v1-23-published-competitor-comparison-2026-08-23.md`](evidence/v1-23-published-competitor-comparison-2026-08-23.md).

The same comparison was repeated five times per implementation in
[32626740940](https://github.com/TaeeunKil/kafrust/actions/runs/32626740940).
The exact published `0.3.6` client reached median 77,157 Produce and 286,135
Consume records/s versus rust-rdkafka 0.39.0 at 171,065 and 501,450. All ten
rows reconciled the same digest with zero loss/duplicates. This confirms the
material workload-specific gap and keeps V1-22 profiling and optimization
requirements open; it does not change V1-23's separate service-canary block.

The external service-canary gate is now explicitly `Blocked`: no named
representative service, owner, deployment environment, or approved
production-like canary target is registered. The in-repository dual-client
fixture remains a smoke/reference comparator only and is not promoted as a
substitute. Forward cutover, fault observation, credential rotation, rollback
objective, and the million-record migration exit remain unexecuted; V1-24 and
V1-25 preparation may continue, but their completion gates cannot close until
the canary dependency is supplied.

Reference smoke evidence was refreshed from `429f19f` in
[32625629138](https://github.com/TaeeunKil/kafrust/actions/runs/32625629138):
both isolated clients processed 1,000 unique 1-KiB records with zero
loss/duplicates and matching digest. This is baseline evidence only and does
not change the blocked external-canary decision.

The reference fixture then completed a one-million-record comparison in
[32645204676](https://github.com/TaeeunKil/kafrust/actions/runs/32645204676)
from source `c56beaa`: both implementations reported 1,000,000 unique records,
zero loss/duplicates, and the same payload digest. This strengthens the
reference-comparison preparation rung only; it does not supply the named
service, transaction/Admin, fault, credential-rotation, forward-cutover, or
rollback evidence required to unblock V1-23. Detailed values are retained in
[`v1-23-migration-million-record-reference-2026-08-23.md`](evidence/v1-23-migration-million-record-reference-2026-08-23.md).

V1-24 through V1-26 also remain `Planned`: the API snapshot is only a freeze input,
and no RC or `1.0.0` publication/tag exists. Protocol-first publication is
allowed only when the named release milestone, exact artifact evidence, and
dated competitor review authorize it; the agent may make that decision
autonomously, as it did for the pre-1.0 `0.3.6` boundary.
The V1-25 RC and V1-26 stable-release manifests/checkers now enforce the exact
version identities, dependency/publication order, metadata-only RC-to-stable
diff, and post-publish canary/tag evidence requirements without performing any
registry or GitHub release action.

The bounded current-source diagnostics are now recorded without inflating those
claims. The first 60-second soak run
[32554050050](https://github.com/TaeeunKil/kafrust/actions/runs/32554050050)
failed only because the soak JSON formatter swapped two gauge arguments; the
client assertions had already reached zero final gauges. Commit `f7a5fcf`
corrected the formatter, and rerun
[32554367028](https://github.com/TaeeunKil/kafrust/actions/runs/32554367028)
passed with 3,481,600 records, 156 operation errors, 219 failed requests,
1,091 retries, recovery, and zero final in-flight/buffered records. The short
benchmark diagnostic
[32554051332](https://github.com/TaeeunKil/kafrust/actions/runs/32554051332)
also passed four 2,000-record profiles with zero retries and zero final gauges.
Both remain diagnostic evidence only; the six-hour/24-hour fault campaigns,
five-repetition eight-hour SLO campaign, and published-artifact gates remain
open.

## Current 0.3.6 Pre-1.0 Qualification (2026-08-23)

The exact `kafrust 0.3.6` and `kafrust-protocol 0.3.6` pair is now visible on
crates.io and resolves from fresh external projects on stable and Rust 1.81;
the ordered publication boundary, checksums, and docs.rs checks are recorded in
[`v1-20-published-0.3.6-boundary-2026-08-23.md`](evidence/v1-20-published-0.3.6-boundary-2026-08-23.md).
The published smoke evidence has expanded through the named API, group,
Share/Streams, security, failover, PLAIN, and SCRAM-SHA-512 rows above. This is the active pre-1.0
baseline for the remaining V1-20 matrix and V1-21~V1-26 gates. No tag, GitHub
release, or `1.0.0` publication exists.

Version cadence remains evidence-driven: a `0.0.1`-sized change is published
only when it forms a useful user-visible or independently consumable boundary
with fresh registry evidence. Internal or incomplete changes stay grouped in
the current candidate. Every external release decision—patch, minor, RC, or
stable—must include a dated competitor comparison and affected verification
rows; if those checks expose a material gap, re-plan the milestone instead of
publishing automatically. The next release is allowed only after the revised
exit criteria are complete and the resulting non-claims are recorded.
All 35 published smoke workflows now default to the current published `0.3.6`
client, while explicit version inputs remain available for historical reruns;
the machine-readable baseline is [published-baseline.json](evidence/published-baseline.json)
and CI enforces this default-version invariant. A future RC may change the
source Cargo.toml version without changing this published baseline until the RC
is actually visible in the registry.

The exact published pair was freshly rerun through all twelve
`published-crate-smoke` profiles in
[32626214201](https://github.com/TaeeunKil/kafrust/actions/runs/32626214201)
from pushed source `8331079`. The external projects resolved the registry pair
and passed the Kafka 3.7.2/3.8.1/3.9.1/4.0.0/4.3.1, security, group, and codec
rows listed in
[`v1-20-published-smoke-rerun-2026-08-23.md`](evidence/v1-20-published-smoke-rerun-2026-08-23.md).
This is a fresh published-row refresh, not full V1-20 completion or a release
decision.
The follow-up clean run
[32626589535](https://github.com/TaeeunKil/kafrust/actions/runs/32626589535)
also passed all twelve profiles and retained 12 external lockfiles plus 12
captured outputs as workflow artifacts. The intermediate 3.9.1 readiness flake
in run 32626452478 is retained as a failure diagnostic, not as passing product
evidence; the clean artifact details are recorded in
[`v1-20-published-smoke-artifact-rerun-2026-08-23.md`](evidence/v1-20-published-smoke-artifact-rerun-2026-08-23.md).
After the bounded metadata retry was added, run
[32627054021](https://github.com/TaeeunKil/kafrust/actions/runs/32627054021)
passed all twelve profiles, including Kafka 3.9.1, and retained the same 12
lockfiles plus 12 captured outputs. The retry remains a ten-second readiness
bound, not a suppression of real failures; the immutable record is in
[`v1-20-published-smoke-readiness-rerun-2026-08-23.md`](evidence/v1-20-published-smoke-readiness-rerun-2026-08-23.md).

The 0.3.6 competitor review on 2026-08-23 found a material workload-specific
throughput gap versus rust-rdkafka, so V1-22 has been re-planned for profiling,
optimization evidence, and the full SLO campaign before any 0.3.7 or 1.0.0
decision. No version bump is being made solely to reflect the comparison.
The five-repetition follow-up in
[32626740940](https://github.com/TaeeunKil/kafrust/actions/runs/32626740940)
confirmed the gap: median kafrust reached 45.1% of rust-rdkafka Produce and
57.1% of Consume throughput, with exact zero-loss/zero-duplicate reconciliation
across all ten rows. The raw result and non-claims are recorded in
[`v1-22-published-competitor-comparison-2026-08-23.md`](evidence/v1-22-published-competitor-comparison-2026-08-23.md).
This is a stronger re-plan input, not a release authorization.

The exact published `0.3.6` pair was refreshed from pushed source
`cf4429d7c643cbfe0046d5c3571a1a3b10f04573` in
[32646370582](https://github.com/TaeeunKil/kafrust/actions/runs/32646370582):
all twelve external smoke profiles passed with retained lockfiles and outputs.
Published mTLS on Kafka 3.7.2 and 4.3.1, OAUTHBEARER, and API 74 configuration
also passed in [32646371786](https://github.com/TaeeunKil/kafrust/actions/runs/32646371786),
[32646373388](https://github.com/TaeeunKil/kafrust/actions/runs/32646373388),
[32646374747](https://github.com/TaeeunKil/kafrust/actions/runs/32646374747),
and [32646376335](https://github.com/TaeeunKil/kafrust/actions/runs/32646376335).
The exact checksum, profile list, and non-claims are retained in
[`v1-20-published-refresh-2026-08-23.md`](evidence/v1-20-published-refresh-2026-08-23.md).
This strengthens published compatibility evidence only; V1-20, V1-21, V1-22,
V1-23, and the release gates remain open, and no `0.3.7` or `1.0.0` decision is
made from this refresh.

On 2026-08-24 the long-campaign capacity was re-audited after the pushed
refresh: the repository has zero registered self-hosted runners and the local
workstation has no Docker executable. Exact-head CI
[32646817241](https://github.com/TaeeunKil/kafrust/actions/runs/32646817241)
passed the runner guards and adjudication tooling, so the missing capacity is
an explicit external gate rather than a reason to weaken V1-21/V1-22 duration,
matrix, or evidence requirements. The immutable audit is
[`v1-long-campaign-capacity-audit-2026-08-24.md`](evidence/v1-long-campaign-capacity-audit-2026-08-24.md).
The operator procedure for clearing this external gate, supplying the V1-23
canary authority, and preserving the weekly fuzz requirement is documented in
[`external-gate-unblock-runbook.md`](milestones/v1.0/external-gate-unblock-runbook.md).

The WSL2 follow-up registered the `wsl-ubuntu-t9` self-hosted runner after a
60-second non-qualification diagnostic initially exposed missing `python` and
`jq` host utilities. The corrected diagnostic
[32648820867](https://github.com/TaeeunKil/kafrust/actions/runs/32648820867)
passed the published `0.3.6` path and artifact checks. The first exact V1-21
campaign, `pinned-secured-six-hour-1`, is running from `54c8e21` in
[32649020906](https://github.com/TaeeunKil/kafrust/actions/runs/32649020906);
this is active execution evidence only, not a qualification or release claim.

### External gate recovery (2026-09-03)

The stale 115,386,419,200-byte same-volume backup export was removed under
the previously authorized cleanup. The unchanged capacity guard now passes
with 736 GiB free on T: and 854 GiB free under `/var/lib/docker`. A temporary
WSL resolver override restored the installed `wsl-ubuntu-t9` service to
`online/idle`; no persistent resolver setting or existing Docker resource was
changed. The published `0.3.6` self-hosted short diagnostic
[33716428169](https://github.com/TaeeunKil/kafrust/actions/runs/33716428169)
then completed successfully: 120.006 seconds, 3,012,600 unique records,
zero loss/duplicates/unknown outcomes, and drained gauges across the four
fault events. This clears the runner/capacity preflight only. V1-21's four
six-hour campaigns and family gates, V1-22's 120-job SLO matrix and locked
baseline, and V1-23's named service canary remain open. Full details are in
[`v1-company-capacity-recovery-2026-09-03.md`](evidence/v1-company-capacity-recovery-2026-09-03.md)
and [`v1-company-selfhosted-short-fault-2026-09-03.md`](evidence/v1-company-selfhosted-short-fault-2026-09-03.md).

The current-source V1-23 reference smoke was refreshed in
[33717760410](https://github.com/TaeeunKil/kafrust/actions/runs/33717760410)
from `12fe52a`: Kafka 4.3.1, isolated topics, 1,000 unique 1-KiB records per
implementation, zero loss/duplicates, and matching business-payload digest.
This strengthens reproducible migration-fixture evidence only; a named service
canary, fault/cutover, credential rotation, rollback, and million-record exit
remain absent. See
[`v1-23-reference-smoke-2026-09-03.md`](evidence/v1-23-reference-smoke-2026-09-03.md).

The published `0.3.6` competitor comparison was also refreshed in
[33718060135](https://github.com/TaeeunKil/kafrust/actions/runs/33718060135):
three repetitions of 20,000 1-KiB records on Kafka 4.3.1 reconciled exactly,
while median kafrust Produce/Consume throughput was 52.36%/51.93% of
rust-rdkafka for that workload. This is a profiling signal, not a universal
performance or release claim; V1-22 still needs its controlled 120-job SLO
matrix and locked baseline. See
[`v1-23-published-competitor-comparison-2026-09-03.md`](evidence/v1-23-published-competitor-comparison-2026-09-03.md).

The short published performance smoke exposed and corrected a fixture-only
metrics-labeling defect: [33718369244](https://github.com/TaeeunKil/kafrust/actions/runs/33718369244)
failed because JSON gauge arguments were ordered incorrectly, while the
`6d5d7ec` rerun [33718664874](https://github.com/TaeeunKil/kafrust/actions/runs/33718664874)
passed all four Kafka 3.7.2/4.3.1 none/Zstd profiles with zero final gauges.
The failure and correction are retained in
[`v1-published-performance-smoke-2026-09-03.md`](evidence/v1-published-performance-smoke-2026-09-03.md).
This remains smoke evidence; V1-22's long matrix and locked baseline are open.

The published 0.3.6 single-node broker-restart smoke then passed in
[33719565892](https://github.com/TaeeunKil/kafrust/actions/runs/33719565892)
from 003561b: a ten-second outage in a 120.001-second run produced 139
recoverable operation errors, 6,375,800 produced/consumed records, 972
retries, recovered=true, and zero final in-flight/buffered gauges. The
preceding [33718942779](https://github.com/TaeeunKil/kafrust/actions/runs/33718942779)
failed only because its fixture JSON omitted the peak-gauge fields checked by
the workflow; the formatter correction is retained in the evidence document
and ledger. This closes one bounded published single-node recovery diagnostic
only. V1-21's six-hour campaigns, V1-22's controlled SLO matrix, V1-23's named
service canary, and all release gates remain open.

The bounded published performance campaign diagnostic
[33720136913](https://github.com/TaeeunKil/kafrust/actions/runs/33720136913)
also passed four 0.3.6 profiles (Kafka 3.7.2 and 4.3.1, none and Zstd) using
5-second warmup and 20-second measured windows. Each profile reconciled more
than one million produced/consumed records with zero retries, unknown
outcomes, loss, duplicates, and final queue gauges. The retained descriptor
marks these results diagnostic and qualified=false; they inform profiling only
and do not close V1-22's five-repetition eight-hour SLO or authorize a release.
See [v1-published-performance-campaign-diagnostic-2026-09-03.md](evidence/v1-published-performance-campaign-diagnostic-2026-09-03.md).

The same published artifact then passed buffered and direct-consumer
diagnostics in [33721044749](https://github.com/TaeeunKil/kafrust/actions/runs/33721044749)
and [33721215334](https://github.com/TaeeunKil/kafrust/actions/runs/33721215334).
All eight additional profiles reconciled exactly with zero retries, unknown
outcomes, loss, duplicates, and final queue gauges. These mode-specific
diagnostics extend profiling coverage only; their descriptors remain
qualified=false, and V1-22's five-repetition eight-hour SLO and locked
baseline remain open. See [v1-published-performance-mode-diagnostics-2026-09-03.md](evidence/v1-published-performance-mode-diagnostics-2026-09-03.md).

The remaining bounded published surface checks were then run from the same
workflow head `31b56aeb`. Eleven workflows passed: PLAINTEXT and SASL_SSL
multi-broker failover, secure group rebalance and transaction failover,
ShareGroupDescribe, Share multi-broker/member/state paths, Streams runtime,
64-cycle Share acknowledgement, and the supported 180-second Share
member-loss window. The run set and exact non-claims are recorded in
[`v1-published-short-surface-smoke-2026-09-03.md`](evidence/v1-published-short-surface-smoke-2026-09-03.md)
and ledger row `Q-PUBLISHED-SHORT-SURFACE-2026-09-03`.

An intentionally shortened 30-second member-loss input failed its six-
partition ownership assertion (`assignment=3`); it is retained as a failed
diagnostic in `Q-PUBLISHED-SHORT-MEMBER-LOSS-FAIL-2026-09-03`. Re-running with
the workflow's supported 180-second window passed, so no source change was
inferred. Four duplicate dispatches were cancelled before execution. These
results strengthen bounded V1-10/V1-14/V1-20 evidence but do not close the
accepted matrix, V1-21/V1-22 long gates, V1-23 service canary, or any release
milestone. The documentation-head CI [33721524284](https://github.com/TaeeunKil/kafrust/actions/runs/33721524284)
also passed.

V1-03's missing deterministic request-shape and minimal response-boundary
slice is now covered by complete golden bytes for the selected Produce, Fetch,
Metadata, ListOffsets, ApiVersions, and OffsetForLeaderEpoch versions. The
focused protocol test passed all four cases; the exact pushed head passed
stable/Rust 1.81 CI in
[run 33726318714](https://github.com/TaeeunKil/kafrust/actions/runs/33726318714).
The fixture and Apache-schema boundary are recorded in
[`v1-data-plane-golden-fixtures-2026-09-03.md`](evidence/v1-data-plane-golden-fixtures-2026-09-03.md).
The malformed tagged-field extension is also covered by the same stable/Rust
1.81 CI run [33727384183](https://github.com/TaeeunKil/kafrust/actions/runs/33727384183).
This is a current-source byte-audit increment only. Non-empty response oracles,
the full malformed length/trailing-byte matrix, transaction-selection proof,
and floor/pinned-current live version logs remain open, so V1-03 stays `In
progress`.

## Historical Release Qualification

`0.3.5` is now published on crates.io in protocol-first order. The fresh
external signed OAUTHBEARER gate passed on Kafka 3.7.2 in
[`32420723537`](https://github.com/TaeeunKil/kafrust/actions/runs/32420723537):
the published `kafrust 0.3.5` client completed RS256 OIDC/JWKS validation,
initial authentication, produce/readback, and SASL re-authentication on the
same connection after the broker session lifetime threshold. The prior
`0.3.4` failures used the already published artifact and are not evidence
against this fix. Both docs.rs pages now return HTTP 200:
[`kafrust 0.3.5`](https://docs.rs/kafrust/0.3.5/kafrust/) and
[`kafrust-protocol 0.3.5`](https://docs.rs/kafrust-protocol/0.3.5/kafrust_protocol/).
The main CI gate runs `cargo package -p kafrust --all-features --locked
--no-verify` after tests, Clippy, and documentation builds. This checks package
assembly only. As recorded in the v1.0 planning baseline above, it does not
compile the staged client against the matching registry protocol artifact and
must not be treated as the final package qualification gate.

The fresh external seven-profile `Published Crate Smoke` also passed with
`kafrust 0.3.5` in
[`32420987547`](https://github.com/TaeeunKil/kafrust/actions/runs/32420987547),
covering Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2
SASL_SSL/SCRAM, and Gzip, Snappy, LZ4, and Zstd. This refreshes the published
artifact baseline; it remains representative evidence rather than a complete
replacement, multi-broker, authorization, or workload claim.

The published secure multi-broker soak then passed in
[`32440677496`](https://github.com/TaeeunKil/kafrust/actions/runs/32440677496)
on Kafka 4.3.1 with SASL_SSL/SCRAM-SHA-256. The default 600-second fresh
external run stopped brokers 1 and 2 simultaneously for ten seconds, recovered,
and verified zero final in-flight and buffered records. This closes one
published secured simultaneous-loss soak slice; it does not close production
SLO, unclean-election, or the complete 1.0 fault matrix.

The current-source Share acknowledgement response-loss gate also passed in
[`32449038941`](https://github.com/TaeeunKil/kafrust/actions/runs/32449038941),
using an injected proxy to drop a ShareAcknowledge response and verify the
reconciliation path. This is a focused ambiguous-response qualification, not a
claim of complete Share failure or long-running production-SLO coverage.

`0.3.4` is now published on crates.io in protocol-first order. Fresh external
projects resolved both packages and passed the published `DescribeCluster` API
60 broker and controller endpoint gate on Kafka 3.7.2 and 4.3.1 in
[`32403253526`](https://github.com/TaeeunKil/kafrust/actions/runs/32403253526)
and [`32403253688`](https://github.com/TaeeunKil/kafrust/actions/runs/32403253688),
including lockfile verification, cluster identity, authorized operations,
broker metadata, and Metadata fallback. The explicit controller path requires
`ClientConfig::controller_bootstrap_servers`. crates.io publication succeeded;
both generated docs.rs pages now return HTTP 200:
[`kafrust 0.3.4`](https://docs.rs/kafrust/0.3.4/kafrust/) and
[`kafrust-protocol 0.3.4`](https://docs.rs/kafrust-protocol/0.3.4/kafrust_protocol/).
The same published workflow also verified `AdminClient::describe_features`
through ApiVersions v3 in [`32406914244`](https://github.com/TaeeunKil/kafrust/actions/runs/32406914244)
for Kafka 3.7.2 (`supported=1`, `finalized=1`, `epoch=68`) and
[`32406914237`](https://github.com/TaeeunKil/kafrust/actions/runs/32406914237)
for Kafka 4.3.1 (`supported=1`, `finalized=6`, `epoch=80`).

`0.3.3` is now published on crates.io in protocol-first order. Its package
verification passed after `kafrust-protocol 0.3.3` became available from the
registry, and a fresh external project compiled the published Streams public
surface, including task-runtime transitions, on stable Rust and Rust 1.81 in
[`32380345199`](https://github.com/TaeeunKil/kafrust/actions/runs/32380345199).
The previous `0.3.1` artifact remains
published on crates.io. The `0.3.0` release's complete seven-profile external
smoke passed in
[`31770895344`](https://github.com/TaeeunKil/kafrust/actions/runs/31770895344)
against Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2
SASL_SSL/SCRAM, and Gzip/Snappy/LZ4/Zstd paths. The `0.3.0` release also
included the typed Admin mutation ambiguity contract and its current-source
response-drop qualification. The published `0.3.1` artifact has separate
fresh-project evidence recorded below.

The current-source `Live Kafka Smoke` matrix passed on commit `1aa18d0` in
[`32382586220`](https://github.com/TaeeunKil/kafrust/actions/runs/32382586220).
This run covered Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 profiles, plaintext,
TLS, SASL/PLAIN, SASL_SSL/SCRAM, OAUTHBEARER, ACL authorization,
multi-broker failover, transaction reconciliation, KIP-848 paths, and the
Kafka 4.3.1 regex v1 initial and dynamic topic-assignment paths. The
plain and secured three-broker coordinator-stop gates now explicitly verify
that a transmitted classic `OffsetCommit` whose response is lost returns
`AdminMutationOutcomeUnknown` with no replay, matching the safety contract;
they no longer incorrectly require a retry of an ambiguous mutation.

The preceding `0.2.30` patch release included consumer-group assignment-state
preservation across classic and KIP-848 rejoin paths. Its full local Rust
validation passed, and the complete 17-job live matrix passed in
[`31761642197`](https://github.com/TaeeunKil/kafrust/actions/runs/31761642197)
against Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1, including plaintext, TLS,
SASL_PLAINTEXT, SASL_SSL/SCRAM, broker-stop recovery, and KIP-848 failover.

This completes the `0.3` publication gate, but not the `1.0` replacement
goal. Remaining gates include broader protocol and Admin coverage, longer
multi-broker and security soak workloads, public API stabilization, and
compatibility evidence against the remaining declared limits.

The current development line now consumes a complete Produce v9-v13
`CurrentLeader` plus `NodeEndpoints` hint after a retryable broker error. The
hint is bounded to the next producer attempt and is discarded when consumed;
incomplete or invalid endpoint data falls back to the existing metadata
refresh path. This closes a producer response-to-routing slice, but live
leader-movement qualification and broader routing/fault evidence remain open.

The published `0.3.0` artifact also passed a 600-second Kafka 4.3.1
three-broker soak with brokers 1 and 2 stopped simultaneously in
[`32230130048`](https://github.com/TaeeunKil/kafrust/actions/runs/32230130048).
The run processed 27,810,300 records with five failed requests and 2,475
retries, recovered successfully, and ended with zero in-flight requests and
zero buffered records. This closes the published simultaneous-loss soak
slice; repeated runs, secured variants, and longer-duration evidence remain
open.

The current published `0.3.3` artifact also passed a fresh external 120-second
Kafka 4.3.1 three-broker simultaneous-loss run in
[`32395288682`](https://github.com/TaeeunKil/kafrust/actions/runs/32395288682).
It reconciled 4,099,200 records after brokers 1 and 2 were stopped together,
recorded one operation error and 1,018 retries, and ended with zero in-flight
and buffered records. Longer current-version and secured production SLO gates
remain separate.

The published secure multi-broker soak workflow now defaults to the current
`0.3.5` artifact, Kafka 4.3.1, simultaneous broker loss, and a 600-second
campaign, and it runs on a weekly schedule as well as manually. The workflow
configuration is in place; recurring scheduled evidence remains required for
the longer secured-soak campaign.

The same secured simultaneous-loss campaign passed manually from a fresh
published `kafrust 0.3.1` project in
[`32345082487`](https://github.com/TaeeunKil/kafrust/actions/runs/32345082487).
The Kafka 4.3.1 SASL_SSL/SCRAM run lasted 600 seconds, processed 19,667,500
records across three replicated partitions, recorded 263 operation errors, 6
failed requests, and 9 retries, and ended with `recovered=true` plus zero final
in-flight or buffered records. This closes the current published secured
simultaneous-loss evidence slice; recurring scheduled evidence, unclean-election
data loss, production SLOs, and service-canary evidence remain separate gates.

The current published `0.3.3` secured simultaneous-loss campaign also passed in
[`32396241090`](https://github.com/TaeeunKil/kafrust/actions/runs/32396241090).
The fresh external project resolved `kafrust 0.3.3` from crates.io with TLS,
authenticated to Kafka 4.3.1 using SASL_SSL/SCRAM, ran for 600 seconds across
three replicated partitions, and survived simultaneous ten-second outages of
brokers 1 and 2. It reconciled 19,188,800 records, observed 283 operation
errors, 6 failed requests, and 9 retries, then reported `recovered=true` with
zero final in-flight and buffered records. This closes the current published
`0.3.3` secured simultaneous-loss evidence slice; repeated campaigns, unclean-
election data loss, production SLOs, and service-canary evidence remain
separate gates.

The published `0.3.5` ShareConsumer also passed a fresh external three-broker
leader-failover gate in
[`32423091397`](https://github.com/TaeeunKil/kafrust/actions/runs/32423091397).
The run used Kafka 4.3.1 with share groups enabled, produced and consumed a
record before stopping the selected partition leader, waited for a replacement
leader, and completed the post-failover produce/consume path through surviving
bootstrap servers. This closes the current published Share leader-failover
slice. It does not close long-running multi-broker ownership, multi-member
churn, secured Share, or production-SLO evidence.

The published `0.3.5` Share acknowledgement soak also passed in
[`32423629077`](https://github.com/TaeeunKil/kafrust/actions/runs/32423629077).
From a fresh external project on Kafka 4.3.1, 64 unique records were acquired,
acknowledged, and committed with `unique_offsets=64`. This closes the bounded
published acknowledgement/commit slice; multi-member churn, broker-failover
acknowledgement under load, secured Share, and production-SLO evidence remain
open.

The runtime connection-lifecycle slice now gives `AdminClient` its own idle
connection cache, shared only by clones of that AdminClient. Sequential Admin
metadata, `describe_features`, broker-endpoint `describe_cluster`,
broker-scoped `list_groups`, `list_transactions`, broker-local
`describe_log_dirs`, coordinator-routed `describe_transactions`, and
coordinator-routed `describe_consumer_groups`,
`describe_consumer_groups_modern`, `describe_share_groups`,
`describe_streams_groups`, and leader-routed `describe_producers` reads reuse
a healthy broker connection, and the capability-backed paths also reuse the
connection-local ApiVersions result without sending a duplicate handshake.
Controller-endpoint DescribeCluster and other coordinator/controller
operations remain operation-owned. Transport failures are not returned to the
cache. Producer cache sharing remains separate; direct consumer Fetch
sessions, ShareFetch sessions, and group heartbeat/membership connections
remain instance- or operation-owned until their session-aware lease contracts
are implemented. The focused Admin reuse regressions pass; broader Admin
broker-operation reuse and live connection-churn evidence remain open.

The session-boundary review confirms that group coordinator/heartbeat clients,
ShareFetch clients plus per-broker Share epochs, and Streams coordinator
clients plus member epochs remain owned by their membership objects. The three
public session types are intentionally non-`Clone`; focused heartbeat, Share
acknowledgement/reconciliation, and Streams lifecycle tests plus the recorded
multi-broker failover gates cover the current boundary. A generic shared cache
must not be extended to these paths without carrying the corresponding
membership lease and epoch state.

The verification-hardening slice now includes a standalone pure-Rust fuzz
workspace with ten libFuzzer targets covering primitive/flexible decoding,
framing, classic and modern group descriptions, share-group offsets, and all
five supported compression codecs. The targets compile with the repository
MSRV after pinning their fuzz-only build dependencies. Each target now has a
tracked seed corpus and the weekly/manual workflow runs a bounded corpus-backed
campaign with per-target RSS/timeout limits and uploaded crash/corpus artifacts;
recurring crash-free evidence, corpus reduction, and broad fault-injecting
broker coverage remain open. A reusable in-process scripted TCP broker now
provides connection-aware request observation, connection drops, and response
  injection for focused integration regressions. The first gates cover Admin
  metadata retry after a dropped response, idempotent Produce retry with the
  same batch sequence and duplicate-response handling. The idempotent producer
  gate also treats `OUT_OF_ORDER_SEQUENCE_NUMBER`, `INVALID_PRODUCER_EPOCH`, and
  `PRODUCER_FENCED` as terminal: none is retried, the first broker error is
  retained, and subsequent sends fail before transmission. The transactional
  EndTxn gate covers response loss with unknown-outcome and defunct-producer
  transitions. A direct
  consumer Fetch response-loss path now also covers metadata re-discovery,
  ApiVersions renegotiation, and one-record recovery. A classic consumer-group
  gate also covers transient `COORDINATOR_NOT_AVAILABLE` retry through
  JoinGroup, SyncGroup, and OffsetFetch assignment restoration. The harness
  keeps an idle post-join connection alive until the test finishes so the gate
  covers the same connection lifecycle as the public group API. A second group
  gate drops the active coordinator heartbeat, routes the rejoin to a replacement
  coordinator, and verifies the new generation through the public `poll()` path.
  A third group gate preserves a non-empty assignment through the same fault,
  restores the committed position with OffsetFetch, and completes a
  post-rejoin Fetch from the partition leader. A fourth gate covers the KIP-848
  path itself: repeated `REBALANCE_IN_PROGRESS` heartbeat responses trigger
  coordinator rediscovery and full modern-protocol rejoin. The replacement
  coordinator negotiates OffsetFetch v10 and OffsetCommit v10 when available,
  while mixed-version fixtures verify the v9 fallback for both operations,
  before the partition-leader Fetch. A fifth
  gate exercises the public Admin member-aware `OffsetCommit v9` path: after
  coordinator discovery, a dropped response is classified as
  `AdminMutationOutcomeUnknown` without replay, and the request is verified to
  use v9 rather than the classic v2 path.

The first KIP-714 broker-side qualification slice passed in
[`32229640441`](https://github.com/TaeeunKil/kafrust/actions/runs/32229640441)
through `live-telemetry.yml`: a Kafka 3.7.2 KRaft image builds the test-only
`KafrustTelemetryReporter`, creates a `client-metrics` subscription, and checks
that kafrust sends both ordinary and terminating payloads. The follow-up live
gate passed in
[`32236749392`](https://github.com/TaeeunKil/kafrust/actions/runs/32236749392)
after altering the active subscription: kafrust observed the stale subscription
ID, respected Kafka 3.7.2's quota cooldown, refreshed on the same connection,
and sent subsequent ordinary and terminating payloads with the new ID. This
closes the bounded subscription-mutation, throttling, and unknown-subscription
recovery slice. The advertised broker payload-limit gate also passed in
[`32237664774`](https://github.com/TaeeunKil/kafrust/actions/runs/32237664774):
with Kafka 3.7.2 advertising `telemetry.max.bytes=128`, kafrust rejected an
oversized OTLP payload before transmission with a typed limit error. Longer
collection and secured or multi-broker telemetry remain open follow-up gates.
The latest `main` run [`32422305042`](https://github.com/TaeeunKil/kafrust/actions/runs/32422305042)
passed the complete Kafka 3.7.2 plugin workflow, including broker plugin
loading, subscription mutation recovery, ordinary payload verification, and
the terminating push. This closes the current single-broker live telemetry
gate; secured, multi-broker, and long-running collection remain open.

The published group smoke now also verifies normal member departure recovery:
after two-member assignment and explicit position rejoin, the remaining
member must reacquire all six partitions and leave cleanly. The gate passed
with published `0.3.0` on Kafka 3.7.2 classic in
[`32231354623`](https://github.com/TaeeunKil/kafrust/actions/runs/32231354623)
and Kafka 4.3.1 KIP-848 in
[`32231357426`](https://github.com/TaeeunKil/kafrust/actions/runs/32231357426).
The same published artifact now also commits the recovered member's current
positions, leaves, and joins a fresh member. The fresh member restored all six
partitions at the committed position without replay on Kafka 3.7.2 classic for
normal departure in [`32233971623`](https://github.com/TaeeunKil/kafrust/actions/runs/32233971623)
and abrupt connection drop in
[`32233975110`](https://github.com/TaeeunKil/kafrust/actions/runs/32233975110),
and on Kafka 4.3.1 KIP-848 for normal departure in
[`32234514848`](https://github.com/TaeeunKil/kafrust/actions/runs/32234514848)
and abrupt connection drop in
[`32234518025`](https://github.com/TaeeunKil/kafrust/actions/runs/32234518025).
This closes the bounded committed-offset restoration slice. The published
group workflow now also supports a bounded repeated-churn campaign across
independent group IDs, with current defaults of ten cycles on Kafka 4.3.1
KIP-848 and abrupt member exit. The current published `0.3.1` artifact passed
the same ten-cycle campaign for Kafka 4.3.1 classic in
[`32369216807`](https://github.com/TaeeunKil/kafrust/actions/runs/32369216807)
and KIP-848 in
[`32369216929`](https://github.com/TaeeunKil/kafrust/actions/runs/32369216929).
This closes the current bounded published churn gate; recurring scheduled
evidence, longer-duration campaigns, retention/restart combinations, and
broader assignor matrices remain separate follow-up gates.

The same smoke was rerun with the second member's coordinator connection
dropped without `LeaveGroup`. The remaining member still reacquired all six
partitions on Kafka 3.7.2 classic in
[`32231944823`](https://github.com/TaeeunKil/kafrust/actions/runs/32231944823)
and Kafka 4.3.1 KIP-848 in
[`32232672745`](https://github.com/TaeeunKil/kafrust/actions/runs/32232672745).
The KIP-848 workflow explicitly bounds the broker's consumer session and
heartbeat settings for a deterministic failure-detection window; this is
test qualification, not a claim about Kafka's production defaults.

The published `0.2.30` artifacts were also exercised from fresh external
projects in the seven-profile `Published Crate Smoke` run
[`31762679537`](https://github.com/TaeeunKil/kafrust/actions/runs/31762679537).
Both published docs.rs pages returned HTTP 200 for
[`kafrust 0.2.30`](https://docs.rs/kafrust/0.2.30/kafrust/) and
[`kafrust-protocol 0.2.30`](https://docs.rs/kafrust-protocol/0.2.30/kafrust_protocol/).

## 0.3 Release Record

Status: Historical release line, published through `0.3.5` in protocol-first
order. The entries below retain their exact artifact and run history; relative
phrases such as “current” in an older entry refer to that entry's date, not the
v1.0 planning baseline. The authoritative latest-published summary is
[Current Release Qualification](#current-release-qualification).

`0.3.x` is a meaningful client milestone, not the complete Kafka replacement
claim. It is intended to move the current alpha from broad feature
coverage toward a release candidate that can be qualified in staging.

### Required slices

- Consumer-group lifecycle hardening across classic and KIP-848: initial and
  delayed assignment delivery, multi-member rebalance and member-loss paths,
  committed-offset restoration, leader-epoch recovery, and bounded heartbeat
  shutdown behavior.
- High-value protocol and Admin completion: close the next documented gaps,
  preserve typed broker and partition outcomes, and add live mutation and
  authorization evidence where transport ambiguity matters.
- Operational qualification: published-crate smoke, docs.rs, repeated
  multi-broker and secured soak runs, bounded retry/timeout evidence, and a
  repeatable comparison benchmark against `rust-rdkafka`.
- Public API and documentation pass: resolve the current alpha API audit,
  document intentional changes and defaults, compile published examples, and
  keep the migration guide aligned with the tested compatibility matrix.

### Progress Recorded

- The published `0.3.3` ShareConsumer now has a bounded multi-member
  ownership gate. Two fresh external members joined one Kafka 4.3.1 Share
  group, each accepted three records, and the six seeded partitions were
  observed exactly once across the members in
  [`32388813780`](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780).
  A 60-second extension then processed 384 records, with each member accepting
  192 and exact partition/offset reconciliation passing in
  [`32389641275`](https://github.com/TaeeunKil/kafrust/actions/runs/32389641275).
  This closes the basic published assignment and bounded soak slices; dynamic
  member-loss recovery also passed when the surviving member reacquired all six
  partitions in [`32390219711`](https://github.com/TaeeunKil/kafrust/actions/runs/32390219711).
  A two-cycle same-group churn profile also passed: the rejoined peer took
  over all six partitions after the second forced loss, with 12 unique offsets,
  in [`32391027028`](https://github.com/TaeeunKil/kafrust/actions/runs/32391027028).
   The same published group then passed three forced-loss cycles, moving
   ownership to member 1, member 2, and a rejoined member 1 again; 18 records
   with three per partition were unique, and the final survivor drained to
   `in_flight=0` in [`32392994232`](https://github.com/TaeeunKil/kafrust/actions/runs/32392994232).
   A fourth forced-loss cycle then moved ownership back to member 2; 24 records
   with four per partition were unique, and the final survivor drained to
   `in_flight=0` with no failed requests in
   [`32394453120`](https://github.com/TaeeunKil/kafrust/actions/runs/32394453120).
  The metrics-enabled 60-second published soak also matched each member's
  `consumed` count to its 192 accepted records and reached `in_flight=0` before
  close in [`32391918666`](https://github.com/TaeeunKil/kafrust/actions/runs/32391918666).
   Higher-cycle churn beyond four cycles, longer ownership, and backpressure
   qualification remain separate 1.0 gates.
- Stable `ShareGroupDescribe` v1 (API key 77) is now implemented through the
  typed protocol, low-level `Client`, and coordinator-aware `AdminClient`
  layers. The public result preserves share-group state and epochs, member
  details, subscribed topics, assignments, topic UUIDs, and authorization
  bits. Focused wire, injected-broker, and Admin routing tests pass; the Kafka
  4.3.1 live ShareConsumer smoke now also inspects its active group through
  this API in [`32223573332`](https://github.com/TaeeunKil/kafrust/actions/runs/32223573332).
  The published `kafrust 0.3.4` external gate then qualified the same public
  API 77 v1 path on Kafka 4.3.1 in
  [`32410690294`](https://github.com/TaeeunKil/kafrust/actions/runs/32410690294),
  observing `state=Stable`, group/assignment epochs `3/3`, `member_epoch=3`,
  subscription metadata, partition 0 assignment, and
  `authorized_operations=3400` from the crates.io artifact.
  Share Group offset mutation APIs 91 and 92 are also implemented with typed
  top-level and per-topic/per-partition results; the Kafka 4.3.1 set/delete
  gate passed in [`32224302754`](https://github.com/TaeeunKil/kafrust/actions/runs/32224302754).
   Share Group offset listing through API 90 v0/v1 and intent-specific group
   deletion through API 42 are now implemented with focused wire and
   coordinator-routing tests. The combined Kafka 4.3.1 live lifecycle gate
   passed in [`32225957928`](https://github.com/TaeeunKil/kafrust/actions/runs/32225957928);
   long-running operational evidence remains open.
- Kafka 4.x `StreamsGroupDescribe` v0 (API key 89) is now implemented through
  the typed protocol, low-level `Client`, and coordinator-aware `AdminClient`
  layers. The public result preserves topology, subtopology, managed-topic
  configuration, member endpoint and tag data, task offsets, assignments, and
  authorization bits. Focused wire and injected-broker routing tests pass.
  Live qualification against a real Kafka Streams application remains open,
  while the separate `StreamsGroupHeartbeat` v0 (API key 88) membership
  lifecycle is now live-qualified below. This does not claim Kafka Streams DSL
  or state-store compatibility.
- The Kafka 4.x `StreamsGroupHeartbeat` v0 wire shape (API key 88) is now
  implemented in the protocol crate and exposed through the low-level
  `Client` and the alpha `StreamsGroupSession`. The session covers initial
  topology publication, member and endpoint epochs, nullable task-state
  updates, bounded coordinator reconnect/rejoin, and graceful leave. Focused
  tests cover configuration validation, retry backoff, and the full injected
  wire lifecycle from topology initialization through task-state heartbeat and
  graceful shutdown. The session now preserves the latest successful broker
  assignment in a typed `StreamsGroupSessionAssignment` snapshot. A bounded
  `StreamsTaskRuntime` now applies Kafka's nullable role updates, canonicalizes
  task IDs, validates partition conflicts, and emits deterministic task
  lifecycle transitions. A bounded
  `StreamsGroupSessionHandle` now owns periodic
  heartbeat scheduling through a Tokio task, exposes a backpressured task-state
  command path, publishes latest assignments through a watch snapshot, and
  waits for graceful close. Focused wire coverage verifies the automatic
  heartbeat and member-epoch `-1` leave; transition application inside a
  complete Kafka Streams application remains open. The published `0.3.3` surface now also compiles the
  handle, assignment-watch, and task-runtime APIs from a fresh external
  project on stable and Rust 1.81 in
  [`32380345199`](https://github.com/TaeeunKil/kafrust/actions/runs/32380345199).
  The published single-broker broker-runtime gate passed in
  [`32381356444`](https://github.com/TaeeunKil/kafrust/actions/runs/32381356444);
  a complete Kafka Streams application remains open.
- `crates/kafrust/examples/streams_group_smoke.rs` and
  `.github/workflows/live-streams-group.yml` now provide a Kafka 4.3.1
  real-broker qualification entry point. The workflow enables the broker
  Streams protocol, creates a source topic, and exercises join, background
  task-state heartbeat, assignment notification, and graceful leave. The
  current-source live gate passed on commit `55e0d8b` in
  [`32373425539`](https://github.com/TaeeunKil/kafrust/actions/runs/32373425539),
  including the broker-required nullable task-offset path, two-member
  membership, member departure convergence, and clean leave. The run confirms
  the bounded background handle lifecycle and broker-side member observation
  on a single broker. Published broker-runtime qualification passed separately
  in [`32381356444`](https://github.com/TaeeunKil/kafrust/actions/runs/32381356444).
  A complete Kafka Streams application, transition application to real
  consumer/state-store processing, and
  coordinator-broker failure evidence remain outside the compatibility claim
  for this single-broker job. The separate three-broker coordinator-stop gate
  passed on commit `21ec3fd` in
  [`32374858753`](https://github.com/TaeeunKil/kafrust/actions/runs/32374858753),
  proving post-stop heartbeat recovery and clean leave through the replacement
  coordinator. Published API-surface, single-broker runtime, and
  coordinator-failover qualification are now complete; a complete Kafka
  Streams application and transition application to real consumer/state-store
  processing remain open.
- A standalone `fuzz/` workspace now provides ten libFuzzer targets, tracked
  seed corpora, and a manual/weekly corpus-backed campaign workflow. Each
  target has bounded RSS and input-time budgets, and the workflow uploads
  generated corpus and crash artifacts. The public bounded compression helpers
  have an all-codec roundtrip regression test, and the OffsetCommit v2/v7/v9/v10
  plus OffsetFetch v2/v9/v10 response decoders now have dedicated targets. This
  closes the initial fuzz
  campaign plumbing slice; recurring crash-free evidence, minimized crash
  regressions, and sustained campaign history remain part of the
  production-hardening gate.
- `scripts/check_protocol_api_surface.py` now runs in the main CI workflow and
  checks that every protocol source module is registered, every declared API
  key is unique, and the reviewed Kafka API-key manifest is unchanged. This is
  the first layer of the offline protocol-parity guard. The companion
  `scripts/check_apache_schema_versions.py` now checks a pinned Apache Kafka
  4.3.1 metadata snapshot for Produce, Fetch, OffsetCommit, OffsetFetch, and
  ConsumerGroupHeartbeat request/response schemas, including API identity,
  valid-version bounds, and flexible-version boundaries. It reports local lag
  without treating an intentionally older implementation as a failure. Full
  field-level parity for every implemented API and byte-level golden fixtures
  remain open. The scheduled/manual `Apache Schema Audit` workflow runs the
  same checker in `--online-all` mode across all 76 local request schemas and
  their 76 matching responses, so API identity and version drift is checked
  beyond the deterministic ten-schema snapshot gate. The latest online audit
  passed in [`32384257319`](https://github.com/TaeeunKil/kafrust/actions/runs/32384257319),
  checking 152 request/response schemas; the audit regression suite now also
  covers Apache singleton ranges such as `validVersions: "0"`.
- Kafka 4.x topic-UUID `OffsetCommit v10` and `OffsetFetch v10` are now
  implemented in the protocol crate and exposed through low-level `Client`
  methods. Focused request/response wire tests and the existing offset fuzz
  targets cover the flexible UUID shape. High-level consumer groups negotiate
  v10 from Metadata v12 topic IDs with v9 fallback. Member-aware Admin offset
  methods now resolve names through Metadata v12 for the same v10/v9
  negotiation; callers can attach complete topic UUIDs through the public
  offset/query builders to skip discovery. The Kafka 4.3.1 current-source live
  matrix passed the member-aware v10 Admin path in
  [`32339508792`](https://github.com/TaeeunKil/kafrust/actions/runs/32339508792),
  and the published `0.3.1` external-project gate passed both v10 operations
  with Kafka CLI offset verification in
  [`32341534974`](https://github.com/TaeeunKil/kafrust/actions/runs/32341534974).
- `ConsumerGroupHeartbeat` v1 is now implemented in the protocol crate and
  exposed through low-level `Client`, including the nullable
  `SubscribedTopicRegex` field added by Kafka 4.x/KIP-1082. The v1 response
  reuses the v0 wire shape with a typed alias. High-level regex subscriptions
  now select v1 for join, foreground/background heartbeat, and leave while
  explicit topic-name subscriptions retain v0. Regex v1 joins generate and
  retain a client-generated UUID-shaped member ID as required by Kafka 4.3.1;
  the high-level path resolves names locally for assignment and refreshes
  Metadata when a new assignment contains an unknown topic UUID. The complete
  live matrix passed the v1 initial join and dynamic post-join topic
  assignment/record path in [`32339508792`](https://github.com/TaeeunKil/kafrust/actions/runs/32339508792).
  The same regex assignment, dynamic-topic, commit, and rejoin path passed
  from published `0.3.1` in the fresh external project
  [`32341967051`](https://github.com/TaeeunKil/kafrust/actions/runs/32341967051),
  with Kafka CLI group-offset verification.
- Flexible topic-UUID `Fetch` v13 is now implemented in the protocol crate and
  exposed through low-level `Client`. Direct and group consumers select it when
  broker capabilities and Metadata v12 provide a stable topic ID, while the
  existing name-based v12/v11/v4 fallback remains available. The request
  covers the Kafka 4.x cluster-ID tag, fetch sessions, UUID topics, forgotten
  UUID topics, and rack selection; the response preserves session,
  transaction, and leader metadata from v12. Low-level `Client` now also
  exposes Fetch v14, whose request shape is wire-equivalent to v13 and whose
  response preserves the tiered-storage error code. Low-level `Client` also
  exposes Fetch v15's tagged replica-state request field. Low-level `Client`
  now also exposes Fetch v16-v18, including v16 node-endpoint decoding, v17
  follower directory IDs, and v18 follower high-watermarks. Live broker
  qualification and high-level follower selection remain open.
- Produce flexible responses v9-v13 now decode Kafka's tagged `NodeEndpoints`
  field into typed `ProduceNodeEndpointV10` values, preserving broker ID,
  host, port, and nullable rack data. A v13 tag-0 fixture covers the decode;
  partition current-leader decoding is covered by the same fixture. The
  high-level producer now consumes a complete `CurrentLeader` plus endpoint
  hint after retryable broker errors for the next attempt; live endpoint and
  leader-movement qualification remain open.
- Flexible `DescribeTopicPartitions` v0 is now implemented through the typed
  protocol and `AdminClient` layers, including topic UUIDs, partition leader/
  ISR state, nullable ELR fields, authorized operations, and paging cursors.
  The current-source compatibility gate passed the expected Kafka 3.7.2
  capability fallback and Kafka 4.3.1 full decode in
  [`31778114684`](https://github.com/TaeeunKil/kafrust/actions/runs/31778114684)
  and [`31778116310`](https://github.com/TaeeunKil/kafrust/actions/runs/31778116310).
- `DescribeQuorum` is now implemented through the typed protocol, low-level
  client, and controller-aware `AdminClient` layers. Its current-source live
  gate passed Kafka 3.7.2 with negotiated v0 and Kafka 4.3.1 with negotiated v2,
  including the explicit controller-listener workflow, in
  [`31781263986`](https://github.com/TaeeunKil/kafrust/actions/runs/31781263986)
  and [`31781264035`](https://github.com/TaeeunKil/kafrust/actions/runs/31781264035).
   Remaining modern gaps include broader Admin/controller protocol coverage and
   secured, multi-broker, and long-running telemetry qualification. The
   single-broker Share Group Admin and KIP-714 telemetry gates are now live-
   qualified.
- The first consumer-group lifecycle slice now has a focused regression test
  for retaining an explicit local position when a topic partition remains
  assigned across rejoin, plus a guard that does not copy position from a
  removed partition when it is later reassigned. The published group smoke
  verifies the same `seek`-then-`rejoin` behavior for Kafka 3.7.2 classic in
  [`31763950353`](https://github.com/TaeeunKil/kafrust/actions/runs/31763950353)
  and Kafka 4.3.1 KIP-848 in
  [`31763952591`](https://github.com/TaeeunKil/kafrust/actions/runs/31763952591).
  This closes the tested position-preservation sub-gate; delayed assignment,
  member-loss, committed-offset, leader-epoch, and shutdown cases remain part
  of the broader 0.3 lifecycle gate.
- The low-level broker connection now retires itself after a request timeout,
  transport failure, or invalid/oversized response frame. A focused regression
  test proves a later request cannot consume stale bytes from the failed
  stream, while high-level retry paths establish a replacement connection.
  The full local validation and complete 17-job `Live Kafka Smoke` matrix
  passed for commit `e0e7e03` in
  [`31765585666`](https://github.com/TaeeunKil/kafrust/actions/runs/31765585666),
  including Kafka 3.7.2 multi-broker failover, secured failover, and Kafka
  4.3.1 KIP-848 paths.
- Background classic and KIP-848 heartbeat tasks now cancel an in-flight
  heartbeat request when stopped instead of waiting for the broker request
  timeout. Focused duplex-broker regression tests cover both protocols, and
  the complete 17-job matrix passed on commit `9f96bf1` in
  [`31766439591`](https://github.com/TaeeunKil/kafrust/actions/runs/31766439591).
  This closes the bounded heartbeat-shutdown sub-gate; broader lifecycle,
  member-loss, committed-offset, and long-duration group qualification remain.
- The repeatable direct comparison gate was rerun against Kafka 4.3.1 using
  current-source commit `1528862`, 20,000 1-KiB records, and batch size 200 in
  [`31767095380`](https://github.com/TaeeunKil/kafrust/actions/runs/31767095380).
  Kafrust measured 49,161.76 producer and 226,166.96 consumer records/s;
  `rust-rdkafka 0.39.0` measured 84,235.49 producer and 220,147.27 consumer
  records/s. This closes the repeatability evidence slice, but does not close
  feature parity, production SLO, or replacement qualification.
- The same comparison passed from a fresh crates.io project resolving
  published `kafrust 0.2.30` in
  [`31768138519`](https://github.com/TaeeunKil/kafrust/actions/runs/31768138519).
  The published artifact measured 51,834.49 producer and 233,242 consumer
  records/s; `rust-rdkafka 0.39.0` measured 87,752.37 producer and 176,675.91
  consumer records/s. This closes the published-artifact comparison slice but
  remains one workload baseline, not feature parity or production SLO evidence.
- The published `0.3.1` comparison passed from a fresh external project in
  [`32355261735`](https://github.com/TaeeunKil/kafrust/actions/runs/32355261735)
  after the Fetch v13 flexible-tag schema-order fix. With the same 20,000
  record, 1-KiB, batch-200 Kafka 4.3.1 profile, kafrust measured 54,613.98
  producer and 256,404.94 consumer records/s; `rust-rdkafka 0.39.0` measured
  91,577.21 producer and 323,988.79 consumer records/s. This closes the
  published `0.3.1` comparison/reliability slice, not replacement, feature
  parity, or production SLO qualification.
- The comparison workflow now defaults to three independent repetitions per
  implementation, isolates each repetition on a fresh topic, and uploads
  repetition-labelled JSONL results. Its validation rejects incomplete or
  duplicated result sets. Historical red runs remain visible by design and
  must be interpreted against their recorded commit and published version;
  this workflow still qualifies only the documented produce/fetch profile.
- The repeated published `kafrust 0.3.1` profile passed in
  [`32368443357`](https://github.com/TaeeunKil/kafrust/actions/runs/32368443357).
  Across three repetitions, kafrust ranged from 45,767.55 to 61,927.71
  producer records/s and 217,064.20 to 292,880.32 consumer records/s;
  `rust-rdkafka 0.39.0` ranged from 86,085.93 to 165,229.42 producer and
  207,251.77 to 795,296.43 consumer records/s. The spread is recorded as
  workload evidence, not a universal performance ranking.
- The published `0.3.3` comparison passed after the workflow default moved to
  the current release in [`32381987301`](https://github.com/TaeeunKil/kafrust/actions/runs/32381987301).
  Across three repetitions, kafrust measured median 70,279.61 producer and
  388,288.51 consumer records/s; `rust-rdkafka 0.39.0` measured median
  161,271.11 producer and 795,363.67 consumer records/s. This closes the
  current published-artifact comparison run, not replacement, feature parity,
  failure compatibility, or production SLO qualification.
- The current published `0.3.5` comparison passed in
  [`32432679837`](https://github.com/TaeeunKil/kafrust/actions/runs/32432679837).
  Across three repetitions of the same Kafka 4.3.1, 20,000-record, 1-KiB,
  batch-200 profile, kafrust measured median 65,451.79 producer and
  351,376.54 consumer records/s; `rust-rdkafka 0.39.0` measured median
  162,827.84 producer and 615,755.30 consumer records/s. This is the current
  published workload baseline, not replacement, feature parity, failure
  compatibility, or production SLO qualification. Historical red runs using
  `0.3.0` remain visible and are not evidence against `0.3.5`.
- Published `0.2.30` then passed a 300-second single-node broker-restart soak
  in [`31768319413`](https://github.com/TaeeunKil/kafrust/actions/runs/31768319413),
  processing 21,597,600 records with 180 operation errors, 954 failed
  requests, and 1,243 retries before recovery completed with zero in-flight or
  buffered records.
- The same published artifact passed a 120-second three-broker plaintext soak
  in [`31768320764`](https://github.com/TaeeunKil/kafrust/actions/runs/31768320764),
  processing 4,404,900 records across three replicated partitions with 1
  operation error, 21 failed requests, and 1,021 retries before recovery
  completed with zero in-flight or buffered records. Secured soak, simultaneous
  loss, production SLO, and service-canary evidence remain separate gates.
- Delayed KIP-848 assignment expiry now returns the typed
  `Error::ConsumerGroupAssignmentTimeout { timeout_ms }` variant instead of
  an `Unsupported` string, allowing callers to distinguish a rebalance
  deadline from protocol or broker failures. The complete 17-job live matrix
  passed after this change on commit `b96f369` in
  [`31767641781`](https://github.com/TaeeunKil/kafrust/actions/runs/31767641781).
- Non-idempotent Admin mutations now classify a transport, timeout, response
  limit, or framing failure after transmission as the typed
  `Error::AdminMutationOutcomeUnknown { operation }` result instead of asking
  callers to infer ambiguity from a generic I/O error. Focused duplex-broker
  tests cover both post-transmission response loss and pre-transmission error
  preservation; the live authorization and broker-fault qualification for the
  remaining mutation families is still open. `DeleteRecords` remains the
  explicit idempotent exception with its existing leader-refresh retry path.
- The complete 17-job `Live Kafka Smoke` matrix passed for the ambiguity
  classification change at commit `bb9ad98` in
  [`31769663509`](https://github.com/TaeeunKil/kafrust/actions/runs/31769663509).
  Kafka 3.7.2 through 4.3.1 plaintext, TLS, SASL/PLAIN, SASL_SSL/SCRAM,
  OAUTHBEARER, ACL authorization, multi-broker failover, transaction
  reconciliation, and KIP-848 paths remained green. This confirms regression
  compatibility of the existing live workflows; it does not by itself qualify
  every post-transmission Admin mutation failure mode.
- The current-source Admin response-drop gate passed on Kafka 3.7.2 and 4.3.1
  in [`31770443512`](https://github.com/TaeeunKil/kafrust/actions/runs/31770443512)
  and [`31770443484`](https://github.com/TaeeunKil/kafrust/actions/runs/31770443484).
  It forwarded a real CreateTopics request to Kafka, dropped only its response,
  observed `Error::AdminMutationOutcomeUnknown { operation: "CreateTopics" }`,
  and reconciled the applied topic through ListTopics. This closes the
  current-source CreateTopics ambiguity sub-gate; other mutation families still
  require their own broker-fault evidence.
- The current-source `live-create-topics-authorization.yml` matrix passed on
  Kafka 3.7.2 and 4.3.1 in
  [`32364633106`](https://github.com/TaeeunKil/kafrust/actions/runs/32364633106).
  A restricted SASL/PLAIN principal with only cluster `Describe` received the
  per-topic `TopicAuthorizationFailed` result (29) and the topic remained
  absent; the administrator then completed create and cleanup. This closes the
  current-source CreateTopics authorization sub-gate only.
- The current-source `live-alter-configs-authorization.yml` matrix passed on
  Kafka 3.7.2 and 4.3.1 in
  [`32365666970`](https://github.com/TaeeunKil/kafrust/actions/runs/32365666970).
  A restricted SASL/PLAIN principal with cluster/topic discovery and
  `DescribeConfigs`, but without `AlterConfigs`, received
  `TopicAuthorizationFailed` (29) and the existing `retention.ms` value
  remained unchanged; the administrator then applied the replacement value and
  cleaned up the topic. This closes the current-source classic AlterConfigs
  authorization sub-gate only.
- The current-source `live-incremental-alter-configs-authorization.yml` matrix
  passed on Kafka 3.7.2 and 4.3.1 in
  [`32366418605`](https://github.com/TaeeunKil/kafrust/actions/runs/32366418605).
  The restricted SASL/PLAIN principal received `TopicAuthorizationFailed` (29)
  and the existing `retention.ms` value remained unchanged, while the
  administrator applied the incremental alteration. This closes the
  current-source IncrementalAlterConfigs authorization sub-gate only.
- The current-source `live-alter-client-quotas-authorization.yml` matrix passed
  on Kafka 3.7.2 and 4.3.1 in
  [`32367537887`](https://github.com/TaeeunKil/kafrust/actions/runs/32367537887).
  A restricted SASL/PLAIN principal with cluster discovery but without the
  quota mutation permission received `ClusterAuthorizationFailed` (31); a
  separate administrator readback confirmed no quota was applied before the
  administrator applied and removed it. This closes the current-source
  AlterClientQuotas authorization sub-gate only.
- The current-source `live-create-partitions-authorization.yml` matrix passed
  on Kafka 3.7.2 and 4.3.1 in
  [`32366048755`](https://github.com/TaeeunKil/kafrust/actions/runs/32366048755).
  A restricted SASL/PLAIN principal with cluster/topic discovery, but without
  the partition-change permission, received `TopicAuthorizationFailed` (29)
  and the one-partition topic remained unchanged; the administrator then
  expanded it to two partitions and cleaned it up. This closes the
  current-source CreatePartitions authorization sub-gate only.
- The current-source `live-delete-topics-authorization.yml` matrix passed on
  Kafka 3.7.2 and 4.3.1 in
  [`32365120994`](https://github.com/TaeeunKil/kafrust/actions/runs/32365120994).
  A restricted SASL/PLAIN principal with cluster and target-topic `Describe`,
  but without delete permission, received `TopicAuthorizationFailed` (29) and
  the topic remained present; the administrator then deleted it. This closes
  the current-source DeleteTopics authorization sub-gate only.
- The reusable current-source response-drop gate now also covers DeleteTopics.
  It created a topic, dropped the real DeleteTopics response, observed
  `Error::AdminMutationOutcomeUnknown { operation: "DeleteTopics" }`, and
  reconciled the deletion through ListTopics on Kafka 3.7.2 in
  [`31771419625`](https://github.com/TaeeunKil/kafrust/actions/runs/31771419625)
  and Kafka 4.3.1 in
  [`31771419124`](https://github.com/TaeeunKil/kafrust/actions/runs/31771419124).
  This closes the current-source DeleteTopics ambiguity sub-gate; ACL, quota,
  SCRAM, config, reassignment, offset, and other mutation families remain
  operation-specific gates.
- The same gate now covers CreatePartitions. It expanded a real topic from one
  to two partitions, dropped the response, observed
  `Error::AdminMutationOutcomeUnknown { operation: "CreatePartitions" }`, and
  reconciled the new partition count on Kafka 3.7.2 in
  [`31771635710`](https://github.com/TaeeunKil/kafrust/actions/runs/31771635710)
  and Kafka 4.3.1 in
  [`31771636082`](https://github.com/TaeeunKil/kafrust/actions/runs/31771636082).
  ACL, quota, SCRAM, config, reassignment, offset, and other mutation families
  remain operation-specific gates.
- IncrementalAlterConfigs is now qualified by the same current-source gate. It
  set `retention.ms`, dropped the response, observed
  `Error::AdminMutationOutcomeUnknown { operation: "IncrementalAlterConfigs" }`,
  and reconciled the value through DescribeConfigs on Kafka 3.7.2 in
  [`31771864914`](https://github.com/TaeeunKil/kafrust/actions/runs/31771864914)
  and Kafka 4.3.1 in
  [`31771865024`](https://github.com/TaeeunKil/kafrust/actions/runs/31771865024).
- Classic AlterConfigs is now qualified as well. It replaced `retention.ms`,
  dropped the response, observed
  `Error::AdminMutationOutcomeUnknown { operation: "AlterConfigs" }`, and
  reconciled the value through DescribeConfigs on Kafka 3.7.2 in
  [`31772009182`](https://github.com/TaeeunKil/kafrust/actions/runs/31772009182)
  and Kafka 4.3.1 in
  [`31772008771`](https://github.com/TaeeunKil/kafrust/actions/runs/31772008771).
- ACL mutation ambiguity is now qualified with Kafka's StandardAuthorizer and
  an explicit `User:ANONYMOUS` test superuser. CreateAcls response loss was
  reconciled through DescribeAcls on Kafka 3.7.2 in
  [`31772403290`](https://github.com/TaeeunKil/kafrust/actions/runs/31772403290)
  and Kafka 4.3.1 in
  [`31772403077`](https://github.com/TaeeunKil/kafrust/actions/runs/31772403077).
  DeleteAcls response loss was reconciled by confirming the binding was gone
  on Kafka 3.7.2 in
  [`31772470761`](https://github.com/TaeeunKil/kafrust/actions/runs/31772470761)
  and Kafka 4.3.1 in
  [`31772470590`](https://github.com/TaeeunKil/kafrust/actions/runs/31772470590).
  AlterClientQuotas is also qualified: it set `producer_byte_rate`, dropped
  the response, and reconciled the value through DescribeClientQuotas on Kafka
  3.7.2 in [`31772731756`](https://github.com/TaeeunKil/kafrust/actions/runs/31772731756)
  and Kafka 4.3.1 in
  [`31772731963`](https://github.com/TaeeunKil/kafrust/actions/runs/31772731963).
  SCRAM, reassignment, offset, and other mutation families
  remain separate operation-specific gates.
- AlterUserScramCredentials is now qualified with a deterministic SCRAM-SHA-256
  test credential. The response was dropped and the mechanism plus iteration
  count were reconciled through DescribeUserScramCredentials on Kafka 3.7.2 in
  [`31772992221`](https://github.com/TaeeunKil/kafrust/actions/runs/31772992221)
  and Kafka 4.3.1 in
  [`31772992381`](https://github.com/TaeeunKil/kafrust/actions/runs/31772992381).
  Reassignment, offset, and other mutation families remain
  separate operation-specific gates.
- Current-source `CreateDelegationToken` response-drop reconciliation is now
  qualified over authenticated SASL/PLAIN on Kafka 3.7.2 in
  [`31773884142`](https://github.com/TaeeunKil/kafrust/actions/runs/31773884142)
  and Kafka 4.3.1 in
  [`31773883953`](https://github.com/TaeeunKil/kafrust/actions/runs/31773883953).
  The gate confirms a new `User:admin` token through
  `DescribeDelegationTokens` without logging its HMAC; token policy, renewal,
  expiration, and other mutation families remain separate gates.
- Current-source administrative OffsetCommit v2 ambiguity is now qualified
  after coordinator readiness on Kafka 3.7.2 in
  [`31774729128`](https://github.com/TaeeunKil/kafrust/actions/runs/31774729128)
  and Kafka 4.3.1 in
  [`31774729263`](https://github.com/TaeeunKil/kafrust/actions/runs/31774729263).
  The response is dropped, `AdminMutationOutcomeUnknown` is returned without
  replay, and OffsetFetch reconciles the committed offset. DeleteGroups,
  member-aware failures, and target authorization remain open.
- Current-source OffsetDelete v0 ambiguity is now qualified after establishing
  an offset on Kafka 3.7.2 in
  [`31774990676`](https://github.com/TaeeunKil/kafrust/actions/runs/31774990676)
  and Kafka 4.3.1 in
  [`31774990554`](https://github.com/TaeeunKil/kafrust/actions/runs/31774990554).
  The response is dropped, the delete is not replayed, and OffsetFetch
  confirms removal. Member-aware failures and target authorization remain open.
- Current-source DeleteGroups v1 ambiguity is now qualified after making the
  group visible through ListGroups on Kafka 3.7.2 in
  [`31775333815`](https://github.com/TaeeunKil/kafrust/actions/runs/31775333815)
  and Kafka 4.3.1 in
  [`31775333736`](https://github.com/TaeeunKil/kafrust/actions/runs/31775333736).
  The response is dropped, the delete is not replayed, and ListGroups confirms
  the group disappears. Active-member behavior, member-aware failures, and
  target authorization remain open.
- Current-source `AlterPartitionReassignments` v0 ambiguity is now qualified
  on Kafka 3.7.2 in
  [`31776694068`](https://github.com/TaeeunKil/kafrust/actions/runs/31776694068)
  and Kafka 4.3.1 in
  [`31776695970`](https://github.com/TaeeunKil/kafrust/actions/runs/31776695970).
  The response is dropped, `AdminMutationOutcomeUnknown` is returned without
  replay, and `ListPartitionReassignments` plus final metadata reconcile the
  target replica order and ISR broker set. Authorization, cancellation,
  broker-loss, and data-movement qualification remain open.
- Current-source KIP-848 member-aware `OffsetCommit` v9 ambiguity is now
  qualified on Kafka 4.3.1 in
  [`31777089953`](https://github.com/TaeeunKil/kafrust/actions/runs/31777089953).
  A joined member's commit response is dropped, `AdminMutationOutcomeUnknown`
  is returned without replay, and member-aware OffsetFetch plus the Kafka CLI
  reconcile offset `42`. Active-member deletion, member-aware offset deletion,
  and target authorization remain open.
- Published `0.2.30` passed four multi-member group rebalance profiles:
  Kafka 3.7.2 classic in [`31770201899`](https://github.com/TaeeunKil/kafrust/actions/runs/31770201899),
  Kafka 4.3.1 KIP-848 in [`31770201823`](https://github.com/TaeeunKil/kafrust/actions/runs/31770201823),
  Kafka 3.7.2 SASL_SSL classic in [`31770202151`](https://github.com/TaeeunKil/kafrust/actions/runs/31770202151),
  and Kafka 4.3.1 SASL_SSL KIP-848 in [`31770201859`](https://github.com/TaeeunKil/kafrust/actions/runs/31770201859).
  This strengthens the group lifecycle gate across protocol and security
  modes; longer member-loss and service-canary behavior remain separate.
- Published `0.2.30` passed 120-second Kafka 4.3.1 three-broker SASL_SSL
  recovery soaks. Single-broker loss processed 3,512,100 records with zero
  operation errors and two retries in
  [`31770173454`](https://github.com/TaeeunKil/kafrust/actions/runs/31770173454);
  simultaneous loss of brokers 1 and 2 processed 2,445,000 records with 282
  operation errors, two failed requests, and seven retries in
  [`31770173559`](https://github.com/TaeeunKil/kafrust/actions/runs/31770173559).
  Both recovered with zero in-flight requests and buffered records.
- Both `0.3.0` crates were published in protocol-first order and resolved from
  crates.io. The fresh-project smoke in
  [`31770895344`](https://github.com/TaeeunKil/kafrust/actions/runs/31770895344)
  passed all seven profiles, including the published TLS and compression
  features.

### Exit criteria

- Both `0.3.0` crates publish in protocol-first order and resolve from a fresh
  external project. **Done.**
- docs.rs is green for both crates and the complete supported live matrix is
  green for the release commit. **Done.**
- The documented group, Admin, security, compression, idempotent, and
  transactional workflows pass representative multi-broker or secured gates;
  remaining unsupported behavior is explicit in the migration guide.
- No known release-blocking correctness issue remains in the tested paths, and
  local format, check, test, Clippy, docs, package, and diff gates pass.

`0.3.0` still does not claim complete `rust-rdkafka` parity or Kafka-broker
replacement. Those claims remain M21/`1.0` work and require broader failure,
authorization, performance, and production-canary evidence.

## M0 Foundation

Status: Done.

Goal: make the repository ready for steady development.

Scope:

- Cargo workspace
- initial crate or module layout
- license
- Rust toolchain or MSRV policy
- CI for `cargo fmt`, `cargo clippy`, and `cargo test`

Exit criteria:

- the workspace builds
- formatting, linting, and tests can run locally and in CI
- future work has a clear crate/module home

Evidence:

- Cargo workspace with `kafrust` and `kafrust-protocol` crates.
- CI runs format, build, clippy, and tests on Rust 1.81.0 and stable.
- The MSRV moved from Rust 1.75 to 1.81 when bidirectional pure-Rust Zstd
  support required language features stabilized in Rust 1.81.
- Main has stayed buildable through short-lived PRs.

## M1 Protocol Core

Status: Done for the currently implemented APIs; ongoing as new Kafka APIs are added.

Goal: encode and decode Kafka wire-format messages without needing a broker.

Scope:

- primitive wire types
- strings, nullable strings, bytes, and nullable bytes
- compact strings, compact bytes, compact arrays, and tagged fields
- request and response headers
- ApiVersions messages
- Metadata messages

Exit criteria:

- byte-level protocol tests cover the implemented primitives
- known request/response fixtures are checked where practical
- protocol code is separated from high-level client ergonomics

Evidence:

- Primitive codec, frame, request header, response header, ApiVersions, Metadata, Produce v2, and Fetch v2 live in `kafrust-protocol`.
- Protocol-focused unit tests cover byte-level encode/decode behavior.
- High-level client APIs depend on protocol types instead of mixing protocol parsing into user-facing builders.

## M2 Broker Roundtrip

Status: Implemented; live-broker verification is opt-in and scheduled.

Goal: prove kafrust can talk to a real Kafka broker.

Scope:

- TCP connection
- request and response framing
- correlation IDs
- client ID handling
- ApiVersions request/response
- Metadata request/response
- basic error decoding

Exit criteria:

- kafrust can connect to a local Kafka broker
- ApiVersions roundtrip succeeds
- Metadata roundtrip succeeds for at least one topic

Evidence:

- `Client` can connect over Tokio TCP, frame requests, increment correlation IDs, and decode response headers.
- `api_versions` and `metadata` roundtrip methods exist.
- `broker_roundtrip` example and opt-in integration test use `KAFRUST_BOOTSTRAP_SERVERS`.
- The `Live Kafka Smoke` workflow has passed the broker roundtrip test against Kafka 3.7.2.

Ongoing verification:

- Keep the scheduled/manual `Live Kafka Smoke` workflow passing before release tags.

## M3 Producer MVP

Status: Implemented; live produce verification is opt-in and scheduled.

Goal: provide a familiar minimal producer for Kafka users.

Scope:

- `Producer::builder()`
- `bootstrap_servers`
- `client_id`
- topic, key, and value records
- Produce request
- `acks=1`
- metadata-based leader routing
- basic retry behavior

Exit criteria:

- an example can produce a record to a real topic
- producer API exposes Kafka concepts directly
- basic metadata refresh and retry behavior are documented

Evidence:

- `ProducerConfig`, `ProducerRecord`, `Acks`, and `RecordMetadata` are public.
- `Producer::send` does metadata lookup, leader routing, Produce v2 encoding, ProduceResponse v2 decoding, and broker error surfacing.
- Producer retries stale-metadata-style produce errors once after refreshing metadata.
- `producer_send` example and `docs/producer-api.md` document the current path.
- The `Live Kafka Smoke` workflow has produced records to Kafka 3.7.2.

Known limits:

- Current high-level producer path negotiates Produce API support, uses v3 RecordBatch for headers, and falls back to v2 MessageSet only for records without headers.
- `acks=0` sends write and flush Produce requests without waiting for a broker
  response; returned offsets are `-1` and broker acceptance is not confirmed.
- Live produce validation runs through the scheduled/manual `Live Kafka Smoke` workflow.

## M4 Consumer MVP

Status: Implemented; live fetch verification is opt-in and scheduled.

Goal: provide a minimal consumer path before implementing full consumer groups.

Scope:

- Fetch request
- direct topic/partition assignment
- offset selection
- record batch decoding
- stream-like record consumption API

Exit criteria:

- an example can fetch records from a real topic partition
- offsets and partitions are visible to users
- record decoding is covered by focused tests

Evidence:

- Fetch v2 protocol request/response types exist.
- Legacy MessageSet and RecordBatch v2 records are decoded and covered by focused tests.
- `ConsumerConfig`, `Consumer`, and `ConsumerRecord` expose direct topic/partition/offset fetch.
- `Consumer::assign` and `Consumer::poll` provide a stream-like path with in-memory offset advancement.
- `consumer_fetch` example and `docs/consumer-api.md` document the current path.
- The `Live Kafka Smoke` workflow has fetched records from Kafka 3.7.2.

Next work:

- Extend live fetch checks across more record shapes and broker versions.

## M5 Consumer Group Alpha

Status: Implemented; live group verification is opt-in and scheduled.

Scope:

- FindCoordinator (implemented as protocol + client roundtrip)
- JoinGroup (implemented as protocol + client roundtrip)
- SyncGroup (implemented as protocol + client roundtrip)
- Heartbeat (implemented as protocol + client roundtrip)
- classic consumer protocol subscription/assignment v0 payloads
- internal range assignment for classic rebalance leaders
- OffsetFetch (implemented as protocol + client roundtrip)
- OffsetCommit (implemented as protocol + client roundtrip)
- ConsumerGroup alpha API with join, sync, heartbeat, background heartbeat, poll, rejoin, and commit
- client-side regex topic subscription through Metadata v1 resolution before
  each classic or KIP-848 join/rejoin
- explicit per-record offset commit queue with per-partition coalescing and
  current-generation flush
- bounded opt-in background commit worker with interval flush, retry, shutdown,
  and rejoin membership synchronization
- opt-in Kafka-style automatic commit mode that queues current assignment
  positions after successful polls and surfaces worker failure on a later poll
- rebalance handling (poll-triggered rejoin for coordinator, generation, member, and rebalance heartbeat errors)

Known limits:

- Rebalance handling is poll-triggered, not background-driven.
- Background heartbeats are opt-in and surface group errors through
  `ConsumerGroupHeartbeat::try_wait` or `ConsumerGroupHeartbeat::stop`;
  `poll_with_heartbeat` triggers poll-time rejoin and replaces completed or
  stale same-group heartbeat tasks for the current generation.
- Live group validation runs through the scheduled/manual `Live Kafka Smoke` workflow.
- Regex subscription has focused unit coverage and initial plus explicit rejoin
  two-topic assignment qualification across Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1, including the corrected KIP-848 path on 4.3.1 in [`Live Kafka Smoke`,
  run `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247);
  secured permission qualification remains.
- The regex record path also fetched a produced record, coalesced its next
  offset through `commit_record`, and flushed it with
  `commit_queued_offsets`. Classic Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plus
  KIP-848 Kafka 4.3.1 passed this live path in
  [`Live Kafka Smoke`, run `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247).
- `ConsumerGroup::spawn_commit_worker` now provides a bounded, interval-based
  queued-offset worker. It coalesces by partition, retries transport and
  coordinator-transition failures, synchronizes generation/member/assignment
  state across explicit rejoin, and waits for shutdown before LeaveGroup. The
  worker's focused unit coverage and live qualification passed for classic
  Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1, and KIP-848 Kafka 4.3.1 in
  [`Live Kafka Smoke`, run `31563953123`](https://github.com/TaeeunKil/kafrust/actions/runs/31563953123).
- `ConsumerGroupConfig::enable_auto_commit(true)` owns that worker for the
  lifetime of a joined group, queues current assignment positions after each
  successful classic or KIP-848 poll, preserves the worker through rejoin, and
  surfaces a terminal worker failure on a later poll. The default remains
  explicit commit mode for backward-compatible alpha behavior. The full
  classic and KIP-848 automatic-commit smoke paths passed in
  [`Live Kafka Smoke`, run `31593984640`](https://github.com/TaeeunKil/kafrust/actions/runs/31593984640).
- KIP-848 join completion now distinguishes a delivered empty assignment from
  a missing assignment. This matters when a second member joins a group with
  fewer partitions than members; the live Kafka 4.3.1 background-heartbeat
  rejoin path passed after this fix in
  [`Live Kafka Smoke`, run `31756119753`](https://github.com/TaeeunKil/kafrust/actions/runs/31756119753).
- `Consumer::split_partition_queue` and
  `ConsumerGroup::split_partition_queue` provide bounded per-partition
  delivery through `ConsumerPartitionQueue`. Focused tests cover independent
  routing, queue-full backpressure, and preservation of the first rejected
  offset; assignment replacement closes queues for partitions no longer owned.
  Direct and group live examples passed across Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 in [`Live Kafka Smoke`, run `31566523106`](https://github.com/TaeeunKil/kafrust/actions/runs/31566523106).
  The same matrix also passed the KIP-848 consumer-group queue path on Kafka
  4.3.1 in [`31566898432`](https://github.com/TaeeunKil/kafrust/actions/runs/31566898432).
  Queue-enabled group failover examples also passed the standard three-broker,
  SASL_PLAINTEXT, and KIP-848 coordinator-stop profiles in
  [`31567226615`](https://github.com/TaeeunKil/kafrust/actions/runs/31567226615).
- `ConsumerGroup::rejoin` is public and refreshes regex topic discovery before
  joining again. The classic matrix and Kafka 4.3.1 KIP-848 regex paths passed
  initial and explicit rejoin assignment checks in [`Live Kafka Smoke`, run
  `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247).
  A Kafka 3.7.2 StandardAuthorizer job then ran the same regex subscription
  over SASL_PLAINTEXT as a restricted user with one allowed and one denied
  topic. The initial assignment and explicit rejoin exposed only the allowed
  topic and fetched its record in
  [`31694784179`](https://github.com/TaeeunKil/kafrust/actions/runs/31694784179).

## M6 Production Behavior

Status: Implemented; deeper resilience behavior remains iterative.

Scope:

- request timeouts (implemented through `ClientConfig::request_timeout_ms`)
- producer retry policy (implemented through `ProducerConfig::max_retries`)
- producer metadata cache and refresh on retriable send failures
- producer reconnect on retriable send failures
- consumer fetch retry and reconnect on transient failures
- bootstrap failover (implemented by trying configured bootstrap servers in order)
- error classification (initial `BrokerErrorKind` mapping implemented)
- request and operation tracing (implemented with `tracing` events for request/response, producer, direct consumer, and group metadata)
- poll backpressure (implemented through `ConsumerConfig::max_poll_records`)

Known limits:

- Reconnects happen through operation retries, not long-lived connection recovery.
- Metadata caching currently exists on the producer and direct consumer paths.
- Tracing emits request lifecycle and high-level operation metadata through
  structured spans. `kafka.request` spans include API identity, correlation ID,
  request and response byte counts, terminal outcome, and elapsed time; dropped
  request futures are recorded as cancelled.
- Backpressure is limited to per-poll record count, not socket or memory pressure.

## M7 Public Alpha

Status: Published.

Scope:

- examples (implemented for broker roundtrip, producer send, direct consumer fetch, coordinator discovery, and group poll)
- API docs (implemented for the public `kafrust` API and enforced with `missing_docs`)
- integration tests (implemented as opt-in broker roundtrip tests)
- crates.io release preparation and publish flow

Evidence:

- `kafrust-protocol v0.1.0`, `kafrust v0.1.0`, `kafrust-protocol v0.2.0`,
  `kafrust v0.2.0`, `kafrust-protocol v0.2.1`, `kafrust v0.2.1`,
  `kafrust-protocol v0.2.2`, and `kafrust v0.2.2` are published on crates.io.
- GitHub releases `v0.1.0`, `v0.2.0`, `v0.2.1`, and `v0.2.2` are tagged and published.
- A fresh external project can add `kafrust = "0.2.2"` and compile from crates.io.
- docs.rs pages for both `0.2.2` crates build successfully; their latest build
  records report `All builds succeeded`.
- The `Live Kafka Smoke` workflow runs the broker roundtrip, producer, direct consumer, and consumer group examples against Kafka 3.7.2.

Known limits:

- Live broker checks are opt-in and scheduled, not part of default pull request CI.
- Published `0.x` APIs remain alpha APIs and may change while Kafka protocol coverage and runtime behavior stabilize.

## M8 Alpha Operations

Status: Done.

Goal: make the alpha reliable to operate during development and small experiments.

Scope:

- scheduled live Kafka smoke checks
- docs.rs and published-crate install smoke
- release checklist updates after each publish
- issue templates or labels for protocol bugs, runtime bugs, and API design questions
- documented compatibility notes for tested Kafka broker versions

Exit criteria:

- live smoke runs on a schedule and can be run manually before release tags
- release docs include post-publish verification, not only pre-publish commands
- known Kafka broker compatibility is visible in docs
- reported failures can be triaged into protocol, client runtime, or API surface areas

Evidence:

- `Live Kafka Smoke` exists and has passed manually against Kafka 3.7.2.
- `docs/broker-roundtrip.md` records the latest manual live smoke and the scheduled workflow.
- v0.1.0, v0.2.0, v0.2.1, and v0.2.2 were verified from fresh external projects.
- `docs/release.md` includes post-publish crates.io, docs.rs, release tag, and live smoke verification.
- `docs/compatibility.md` documents the current Kafka 3.7.2 compatibility claim and known non-claims.
- GitHub issue forms route reports into protocol bugs, client runtime bugs, or API design questions.
- The published `0.2.2` crates were re-verified from a fresh temporary project,
  and both docs.rs builds completed successfully.

Known limits:

- Compatibility has been verified against Kafka 3.7.2 only.
- Issue forms provide triage structure, but repository labels are not required yet.

## M9 Consumer Group Resilience

Status: Done.

Goal: make the consumer group alpha behavior safer under normal Kafka rebalances and coordinator changes.

Scope:

- background heartbeat error observation and recovery strategy
- automatic rejoin coordination between foreground poll and background heartbeat
- clearer member generation state transitions
- offset commit behavior during rejoin and stale generations
- focused tests for coordinator, generation, member, and rebalance error paths

Exit criteria:

- background heartbeat failures can trigger a controlled rejoin path or a clearly documented terminal state
- foreground `poll` and background heartbeat do not race over stale generation or member IDs
- offset commits fail predictably or recover after rejoin, with visible Kafka context
- docs describe when users should spawn background heartbeats and how failures are surfaced

Evidence:

- `ConsumerGroup::poll_with_heartbeat` observes background heartbeat task completion before polling and uses the existing rejoin path for rejoinable group errors.
- `poll_with_heartbeat` replaces completed and stale same-group heartbeat
  handles after background or foreground rejoin while preserving the configured
  heartbeat interval.
- Manual `Live Kafka Smoke` run `30067372344` passed a real two-member
  rebalance, automatic rejoin, and heartbeat handle replacement on Kafka
  3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext brokers.
- Focused unit tests cover running tasks, rejoinable background heartbeat errors, and non-rejoinable background heartbeat errors.
- `ConsumerGroupHeartbeat` records the group ID, member ID, and generation ID it was spawned for, and stale same-group handles are stopped before polling to avoid sending heartbeats for an older generation.
- `ConsumerGroup::commit_offsets` rejoins after rejoinable offset commit errors and returns the original commit error instead of retrying stale assignment offsets under a new generation.
- `docs/consumer-groups.md` describes when to spawn background heartbeats, how heartbeat failures are surfaced, and how offset commit rejoin behavior works.

Known limits:

- Background heartbeats can trigger a rejoin when users call
  `ConsumerGroup::poll_with_heartbeat`; the mutable handle is automatically
  replaced after background or foreground rejoin.
- Range assignment is the only high-level group assignment strategy.

## M10 Producer Throughput

Status: Done.

Goal: move from single-record send ergonomics toward practical producer throughput while keeping Kafka concepts visible.

Scope:

- multi-record produce requests
- per-topic and per-partition batching
- configurable linger and batch size
- retry behavior for partial partition failures
- clearer delivery metadata for batched sends

Exit criteria:

- users can send batches without manually building protocol structures
- batching preserves topic, partition, key, value, headers, acks, and offset metadata
- partial failures are surfaced per topic partition
- live smoke covers at least one multi-record produce and fetch roundtrip

Known limits:

- `acks=0` immediate and batch sends use the no-response Produce path and return
  unknown-offset metadata. Live workload-loss and broker-error semantics remain
  an operational qualification item.

Evidence:

- `Producer::send_batch` accepts multiple records, batches same topic-partition groups into one Produce request, and returns metadata in input order.
- `Producer::send_batch_report` surfaces per-record success and failure outcomes in input order, including broker Produce response errors for failed topic partitions.
- Batch retry recovery keeps successful records fixed and retries only input records whose topic partition returned a retryable Produce response error.
- `ProducerConfig::max_records_per_batch` splits large topic-partition groups across multiple Produce requests without changing input-order outcomes.
- `ProducerConfig::max_batch_bytes` splits large topic-partition groups by encoded Kafka record-set bytes without preventing an oversized single record from being sent.
- Focused unit tests cover batch Produce API version selection and batch metadata cache invalidation.
- The `Live Kafka Smoke` workflow runs the `producer_send_batch` and `producer_buffered` examples before direct fetch and group poll checks.
- Manual `Live Kafka Smoke` run `26989271377` passed on 2026-06-05 after the batch outcome, partial retry, and record-limit changes.
- Manual `Live Kafka Smoke` run `26999258762` passed on 2026-06-05 after the buffered producer flush trigger and smoke example changes.
- `docs/producer-buffering.md` defines the planned opt-in buffered producer path, linger flush triggers, delivery semantics, and implementation slices.
- `ProducerConfig::linger_ms` and `ProducerConfig::build_buffered` provide the first buffered producer lifecycle skeleton with `flush`, `close`, and `is_closed`.
- `BufferedProducer::send` queues records through a bounded channel and returns per-record `ProducerDelivery` handles; `flush` and `close` send pending records through `send_batch_report` and complete delivery handles from per-record outcomes.
- Automatic buffered flush triggers cover `linger_ms`, `max_records_per_batch`, and `max_batch_bytes`, with `linger_ms(0)` meaning no intentional wait before background flush.
- Focused unit tests cover buffered enqueue, cloneable non-transactional handle sharing, transactional handle rejection, delivery cancellation, pending delivery failure, per-record delivery completion, defensive handling for missing batch outcomes, and flush trigger selection.

## M11 Security And Connectivity

Status: Complete.

Goal: support common secured Kafka deployments without adding librdkafka or C bindings.

Scope:

- TLS transport using a Rust TLS stack
- SASL PLAIN secured client path
- client configuration for security protocol and authentication material
- secure error messages that do not leak secrets
- docs for local plaintext, TLS, and SASL broker profiles

Exit criteria:

- plaintext behavior remains the default and stays simple
- TLS connections can complete ApiVersions and Metadata roundtrips
- at least one SASL mechanism can authenticate against a broker in live smoke or documented manual checks
- credentials are kept out of tracing events and error displays

Known limits:

- Security protocol configuration exists and defaults to plaintext.
- TLS transport exists behind the non-default `tls` crate feature and has completed recorded broker roundtrip, producer, direct consumer, and consumer group smoke paths against a TLS broker.
- TLS workflows beyond the listed TLS smoke examples are not claimed yet.
- The current `tls` feature uses the `rustls` ring crypto provider, which can require native build tooling in some environments; the default kafrust build still has no required C toolchain.
- `SecurityProtocol::Tls` returns `Unsupported` when kafrust is built without the `tls` feature.
- SASL/PLAIN authentication is implemented and has completed recorded broker
  roundtrip, producer, direct consumer, and consumer group smoke paths against a
  SASL_PLAINTEXT broker.
- SASL_SSL with SCRAM-SHA-256 and SCRAM-SHA-512 is verified in the M13 live
  profile; SASL workflows beyond the listed smoke examples are not claimed yet.
- SCRAM live smoke and SASL_SSL are owned by M13 Secured Enterprise Connectivity.

Evidence:

- `SecurityProtocol` models Kafka `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` connection modes.
- `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig` expose `security_protocol` builders.
- `SaslMechanism::Plain` and `SaslCredentials::plain` model SASL/PLAIN authentication material separately from `SecurityProtocol`, and config debug output redacts passwords.
- `ClientConfig` performs `SaslHandshake v1` followed by `SaslAuthenticate v0` for configured SASL/PLAIN connections; mock broker tests verify request ordering, PLAIN auth bytes, missing-credential errors, and authentication error redaction.
- All current internal broker connection paths go through `ClientConfig`, so future TLS/SASL transport work has one configuration source.
- `Client` owns an internal broker stream abstraction instead of storing `TcpStream` directly, so the TLS stream reuses the same Kafka request framing, timeout, and tracing path.
- The non-default `tls` crate feature wires `SecurityProtocol::Tls` through `tokio-rustls`, `rustls`, and `rustls-platform-verifier` without pulling `aws-lc-rs`; plaintext remains the default build.
- Focused tests cover TLS bootstrap server-name extraction, invalid TLS server names, SASL missing-credential behavior, SASL/PLAIN handshake behavior, and TLS unsupported behavior when the feature is disabled.
- CI runs `check`, `clippy`, and `test` for both the default workspace path and the `kafrust --features tls` path.
- The broker roundtrip test and example accept `KAFRUST_SECURITY_PROTOCOL`, `KAFRUST_SASL_USERNAME`, and `KAFRUST_SASL_PASSWORD`, so plaintext, TLS, and SASL broker profiles can use the same smoke entry point.
- `kafrust-protocol` includes `SaslHandshake v1` and `SaslAuthenticate v0` request/response wire types with byte-level tests.
- Manual `Live Kafka Smoke` run `27326596181` passed on 2026-06-11 from `main`; the TLS job completed broker roundtrip test and example checks against Kafka 3.7.2 with `SecurityProtocol::Tls`.
- Manual `Live Kafka Smoke` run `27397850803` passed on 2026-06-12 from `main`; the SASL_PLAINTEXT job completed broker roundtrip test and example checks against Kafka 3.7.2 with `SecurityProtocol::SaslPlaintext`.
- Manual `Live Kafka Smoke` run `27399057735` passed on 2026-06-12 from `main`; the SASL_PLAINTEXT job completed broker roundtrip, producer, direct consumer, and consumer group checks against Kafka 3.7.2 with `SecurityProtocol::SaslPlaintext`.
- Manual `Live Kafka Smoke` run `27399394544` passed on 2026-06-12 from `main`; the TLS and SASL_PLAINTEXT jobs completed broker roundtrip, producer, direct consumer, and consumer group checks against Kafka 3.7.2.

Strategic role:

- This milestone established the baseline secured client path. TLS and SASL_PLAINTEXT producer, direct consumer, and consumer group smoke paths are now covered; M13 owns SASL_SSL, SCRAM, multi-broker secured profiles, and broader enterprise compatibility.

## M12 API Stabilization

Status: Complete.

Goal: prepare a stable pre-1.0 API shape with clear compatibility rules for downstream users.

Scope:

- audit public types for Kafka terminology, naming, and minimality
- decide which protocol types remain public re-exports
- builder validation and explicit error variants for common configuration failures
- docs examples that compile from published crates
- semver policy for `0.x` releases and migration notes

Exit criteria:

- public APIs have documented intended stability levels
- examples cover producer, direct consumer, and consumer group happy paths from published crates
- release notes call out breaking changes and migration steps
- downstream users can evaluate whether kafrust is suitable for experiments, staging, or production-like tests

Known limits:

- The project is still pre-1.0 and can make breaking changes between minor versions.
- Protocol coverage is intentionally incomplete and grows API by API.

Evidence:

- `docs/api-stability.md` documents the current pre-1.0 versioning policy,
  stability levels, change rules, and migration note expectations.
- `docs/public-api-audit.md` records the current root re-export surface,
  module visibility decision points, and the `kafrust::protocol` re-export
  policy.
- `cargo test -p kafrust --doc` compiles the crate README examples for
  producer, batch producer, buffered producer, direct consumer, and consumer
  group usage; CI runs this explicitly.
- `docs/release.md` defines a release note template with required breaking
  change, migration note, compatibility evidence, verification, and known-limit
  sections.

Strategic role:

- This milestone made the current alpha public surface explicit before adding
  more Kafka feature coverage. Future milestones can still change APIs before
  `1.0`, but those changes now have a documented stability policy, root export
  audit, compiled rustdoc examples, and release note migration format.

## M13 Secured Enterprise Connectivity

Status: Complete.

Goal: make kafrust usable against common secured Kafka deployments.

Scope:

- TLS transport with a pure Rust TLS stack
- configurable root certificates and server name validation
- SASL PLAIN
- SASL SCRAM-SHA-256 and SCRAM-SHA-512
- SASL OAUTHBEARER token authentication
- credential redaction in errors, debug output, logs, and tracing
- secured broker examples and manual smoke instructions

Exit criteria:

- `SecurityProtocol::Tls` can complete ApiVersions and Metadata roundtrips against a TLS broker
- `SecurityProtocol::SaslPlaintext` authenticates with at least SASL PLAIN
- `SecurityProtocol::SaslTls` authenticates with at least one SCRAM mechanism
- failed authentication errors do not expose passwords, tokens, salts, nonce material, or raw credentials
- compatibility docs list plaintext, TLS, SASL_PLAINTEXT, and SASL_SSL broker profiles with verification dates

Known limits:

- SASL/OAUTHBEARER is live-verified against Kafka 3.7.2's built-in unsecured
  validator in the dedicated OAuth-only smoke job `31478375106`, and against a
  signed local OIDC/JWKS fixture in the OIDC job
  [`31584760474`](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94078116567).
  The fixture covers signature, issuer, audience, Java client, static-token,
  and provider-backed paths. External provider compatibility and
  provider-specific failure behavior remain open. The public async
  token-provider callback is implemented and called for each new broker
  authentication.

Evidence:

- `ClientConfig::tls_server_name` and the matching producer, consumer, and
  consumer group builders allow TLS certificate validation to use an explicit
  server name when the bootstrap host differs from the broker certificate
  subject alternative name. Broker smoke examples accept
  `KAFRUST_TLS_SERVER_NAME`.
- `ClientConfig::tls_root_certificate_der` and the matching producer, consumer,
  and consumer group builders add DER-encoded root certificates while keeping
  platform roots enabled. Broker smoke examples accept
  `KAFRUST_TLS_ROOT_CERT_DER_PATH`.
- `SaslMechanism` models Kafka `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, and
  `OAUTHBEARER`; `SaslCredentials` has matching password and token
  constructors and the shared client, producer, consumer, and consumer group
  configs expose matching builder methods without changing
  `SecurityProtocol`.
- `ClientConfig` performs SCRAM client-first and client-final
  `SaslAuthenticate v1` exchanges after `SaslHandshake v1`, verifies the
  server-final signature, and reports invalid SCRAM responses without exposing
  passwords or raw credentials.
- Focused tests cover SCRAM-SHA-256 and SCRAM-SHA-512 proof generation,
  username escaping, nonce mismatch handling, server-final verification, mock
  broker SCRAM authentication ordering, OAUTHBEARER RFC 7628 initial response
  encoding, and secret-safe authentication errors.
- The broker roundtrip test and smoke examples accept
  `KAFRUST_SASL_MECHANISM` with `plain`, `scram-sha-256`, and
  `scram-sha-512`; they also accept `oauthbearer` with
  `KAFRUST_SASL_TOKEN` and an optional `KAFRUST_SASL_USERNAME`. The dedicated
  Kafka 3.7.2 SASL_SSL OAuth-only job exercises those entry points against the
  broker's built-in unsecured validator.
- `OAuthBearerTokenProvider` and the matching `*_provider` builders allow an
  application to retrieve a fresh token for each new broker connection without
  exposing it through `Debug` output. Provider calls are bounded by
  `ClientConfig::request_timeout_ms` and return the typed
  `Error::OAuthBearerTokenTimeout` when the callback exceeds that limit.
- `OAuthBearerToken`, `OAuthBearerTokenSource`, and
  `CachedOAuthBearerTokenProvider` now provide an opt-in expiry-aware policy
  without breaking the existing string-returning provider trait. The wrapper
  refreshes inside a caller-selected window, rotates when the source returns a
  new token, and falls back to a still-valid cached token during a temporary
  source outage. Once the cached token expires, the original source error is
  returned. HTTP discovery, JWKS retrieval, and provider-specific endpoint
  policy remain application-owned.
- Provider-backed OAUTHBEARER connections also refresh the token and send
  `SaslAuthenticate v1` again on the existing connection before requests after
  half of the broker-advertised session lifetime has elapsed; the focused
  client test and published Kafka 3.7.2 gate cover this lifecycle.
- If the token provider fails after the re-authentication handshake has begun,
  the client returns the provider error and poisons the connection rather than
  reusing a socket whose broker-side SASL exchange is partial. This is covered
  by a deterministic injected-stream regression test; external provider outage
  semantics remain a separate qualification gate.
- `SaslAuthenticate v1` responses remain decoded for PLAIN and SCRAM and
  provider-backed OAUTHBEARER re-authentication, while flexible `v2` is used
  for OAUTHBEARER initial authentication. `Client::sasl_session_lifetime_ms`
  exposes the broker's re-authentication window. Provider-backed OAUTHBEARER
  connections use that window to re-authenticate on the existing connection
  before requests after half the lifetime; detached refresh workers and
  production external-provider qualification remains open.
- The `Live Kafka Smoke` workflow includes a SASL_SSL SCRAM profile that
  creates separate Kafka SCRAM-SHA-256 and SCRAM-SHA-512 credentials, configures
  kafrust with `KAFRUST_SECURITY_PROTOCOL=sasl_tls`, the selected
  `KAFRUST_SASL_MECHANISM`, and a DER root certificate, then runs the shared
  broker roundtrip, producer, consumer, and group smoke paths for both
  mechanisms.
- Manual `Live Kafka Smoke` run `27531812308` passed on 2026-06-15 from
  `main`; the plaintext, TLS, SASL_PLAINTEXT, and SASL_SSL SCRAM jobs completed
  broker roundtrip, producer, direct consumer, and consumer group checks against
  Kafka 3.7.2.
- Manual `Live Kafka Smoke` run `31452872400` passed on 2026-08-11 from
  `main`; all eight profiles passed, including the SASL_SSL SCRAM-SHA-256 and
  SCRAM-SHA-512 subpaths against Kafka 3.7.2.
- Manual `Live Kafka Smoke` run `31478375106` passed on 2026-08-11 from
  `codex/live-oauth-smoke`; the dedicated Kafka 3.7.2 SASL_SSL OAUTHBEARER
  job passed with the built-in unsecured validator. This does not qualify a
  production OAuth/OIDC provider.
- Published `kafrust 0.3.4` passed the same basic SASL_SSL OAUTHBEARER path
  from a fresh external Cargo project in
  [`32411655133`](https://github.com/TaeeunKil/kafrust/actions/runs/32411655133):
  crates.io resolution, async token-provider authentication,
  `AdminClient::describe_cluster`, `acks=all` produce, and direct-consumer
  readback. This closes the published basic OAuth gate only; signed OIDC/JWKS,
  provider discovery, key rotation, and provider-specific outage behavior
  remain open.
- Published `kafrust 0.3.4` then passed the signed local OIDC/JWKS variant in
  [`32412721829`](https://github.com/TaeeunKil/kafrust/actions/runs/32412721829).
  Kafka 3.7.2 validated the RS256 signature, issuer, audience, and subject,
  and the fresh crates.io project completed the same Admin, `acks=all`
  producer, and direct-consumer checks through the async token provider. This
  closes the published signed local fixture gate; external provider
  discovery, token endpoints, key rotation, and outage semantics remain open.
- The signed local OIDC/JWKS fixture passed Kafka's validator, the Java Kafka
  client, and kafrust static and provider-backed paths in the OIDC job
  [`31584760474`](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94078116567).
  OAUTHBEARER initial authentication uses flexible `SaslAuthenticate v2`,
  while provider re-authentication uses `SaslAuthenticate v1`; PLAIN and SCRAM
  remain on `v1`. Detached refresh workers and external provider-specific
  OAuth/OIDC qualification remain open.

Strategic role:

- This is the first milestone where kafrust can plausibly be tested in typical company Kafka environments.

## M14 Multi-Broker And Failover Compatibility

Status: Complete.

Goal: handle normal multi-broker cluster behavior instead of only single-node broker checks.

Scope:

- metadata refresh across multiple brokers
- leader movement and partition leader failover
- bootstrap server failover beyond initial connect
- coordinator movement for consumer groups
- partition expansion handling
- broker disconnect and reconnect behavior under load
- live smoke workflows for at least one multi-broker Kafka profile

Exit criteria:

- producer sends recover after leader movement without user-visible duplicate success reports
- direct consumers recover after partition leader movement
- consumer groups recover after coordinator movement or a controlled rebalance
- compatibility docs distinguish single-node, multi-broker plaintext, and multi-broker secured claims
- tests cover stale metadata, unknown leader, coordinator movement, and reconnect paths

Strategic role:

- This milestone moves kafrust from local/simple broker evaluation toward production-like cluster evaluation.

Evidence:

- Producer and direct consumer retry classification treats missing partition
  leaders and missing broker metadata as stale metadata, invalidates the topic
  metadata cache, and refreshes metadata before retrying within the configured
  retry budget.
- Producer and direct consumer retry classification also treats unknown
  topic-partition entries from cached metadata as refreshable, which gives
  partition expansion and just-created topic metadata one retry budget to
  converge before surfacing the original Kafka concept to callers.
- Smoke examples and opt-in broker roundtrip tests accept comma-separated
  `KAFRUST_BOOTSTRAP_SERVERS` values, so multi-broker live checks can use the
  same environment format as Kafka's standard client configuration.
- The `Live Kafka Smoke` workflow includes a plaintext three-broker Kafka 3.7.2
  profile that creates a replicated topic and runs broker roundtrip, producer,
  direct consumer, and consumer group smoke paths against comma-separated
  bootstrap servers.
- Manual `Live Kafka Smoke` run `28009105074` passed on 2026-06-23; the
  multi-broker job completed broker roundtrip, producer, direct consumer, and
  consumer group checks against a three-broker Kafka 3.7.2 KRaft cluster,
  verified long-lived producer and direct consumer operations across a stopped
  partition leader, then reran batch producer, direct consumer, and consumer
  group checks through the remaining brokers.
- The batch producer smoke example accepts explicit partition lists so the
  multi-broker workflow can route one batch call across multiple partition
  leaders.
- The single-record producer smoke example accepts an explicit partition so the
  multi-broker workflow can cover both single-record and batch leader routing.
- The multi-broker smoke workflow stops the first configured bootstrap broker
  and reruns batch producer, direct consumer, and consumer group checks through
  the remaining brokers.
- The `producer_failover` smoke example sends twice through one producer
  instance, and the multi-broker workflow selects a partition led by the first
  broker, stops that broker during the configured pause, and then requires the
  second send to complete through refreshed metadata.
- The `consumer_failover` smoke example fetches twice through one direct
  consumer instance in the same broker-stop window, so stale direct-consumer
  metadata refresh is covered by the multi-broker workflow.
- Consumer group coordinator connection I/O failures and coordinator request
  timeouts are classified as rejoinable in group contexts, so poll,
  background-heartbeat observation, stale-heartbeat shutdown, and offset commit
  paths can rediscover the coordinator instead of treating only broker error
  codes as rejoin signals.
- Manual `Live Kafka Smoke` run `31465216280` passed all nine jobs on
  2026-08-11, including the three-broker failover profile, Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1 plaintext profiles, and the TLS,
  SASL_PLAINTEXT, SASL_SSL/SCRAM, and ACL-authorizer profiles. The
  three-broker job completed producer, direct-consumer, consumer-group,
  admin, reassignment, and broker-stop recovery paths.
- Manual `Live Kafka Smoke` run
  [`31502322974`](https://github.com/TaeeunKil/kafrust/actions/runs/31502322974)
  passed all 12 jobs on 2026-08-11. Its dedicated Kafka 3.7.2 three-broker
  `SASL_PLAINTEXT` job authenticated with SASL/PLAIN, stopped the broker that
  led the selected partition, and completed producer and direct-consumer
  operations before and after the stop through the remaining brokers. The
  workflow first builds both failover examples serially so the result is not
  contaminated by concurrent Rust toolchain initialization.
- Manual `Live Kafka Smoke` run
  [`31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594)
  passed all jobs on 2026-08-12. Its Kafka 3.7.2 three-broker
  `SASL_PLAINTEXT` job stopped the active transaction coordinator and group
  coordinator, then verified transactional commit/read-committed recovery,
  consumer-group recovery, and producer/direct-consumer recovery through the
  remaining authenticated brokers.
- Manual `Live Kafka Smoke` run
  [`31568412595`](https://github.com/TaeeunKil/kafrust/actions/runs/31568412595)
  passed all jobs on 2026-08-12. Its Kafka 3.7.2 three-broker `SASL_SSL`
  SCRAM job validated all three external TLS listeners, then verified
  consumer-group coordinator and partition-leader broker-stop recovery with
  the same authenticated bootstrap set.
- Manual `Live Kafka Smoke` run
  [`31725607371`](https://github.com/TaeeunKil/kafrust/actions/runs/31725607371)
  passed all 17 jobs after the secured combined-fault gate was added. The
  Kafka 3.7.2 three-broker `SASL_PLAINTEXT` classic group path selected a
  broker that was both coordinator and partition leader, stopped it, waited
  for replacement leadership, and consumed a post-failover record after
  rejoin. The workflow also made the existing Kafka 4.3.1 SASL_SSL/SCRAM
  KIP-848 leader-epoch check choose its partition leader dynamically.
- Manual `Live Kafka Smoke` run
  [`31727573855`](https://github.com/TaeeunKil/kafrust/actions/runs/31727573855)
  passed all 17 jobs after adding the Kafka 4.3.1 `SASL_SSL` SCRAM KIP-848
  combined-fault path and the replicated classic-group retention gate. The
  selected KIP-848 broker was both group coordinator and partition leader;
  after it was stopped, the authenticated replacement leader accepted a
  record and the KIP-848 group consumed it after rejoin. The same run verified
  committed classic-group offset recovery after `DeleteRecords`.
- The same complete matrix also ran the classic `consumer_group_offset_reset`
  example on a Kafka 3.7.2 three-broker replicated topic. It committed a group
  position, moved the low watermark past that position through Admin
  `DeleteRecords`, and verified `OffsetResetPolicy::Earliest` recovery at the
  retained boundary. Arbitrary retention timing and unclean-election data loss
  remain outside the claim.
- Manual `Live Kafka Smoke` run
  [`31572745537`](https://github.com/TaeeunKil/kafrust/actions/runs/31572745537)
  passed all 16 jobs on 2026-08-12. Its Kafka 3.7.2 three-broker `SASL_SSL`
  SCRAM job stopped the transaction coordinator, verified that the original
  producer terminates safely on `INVALID_PRODUCER_EPOCH`, restarted the broker,
  and verified that a new producer with the same transactional ID commits a
  recovery transaction visible to `read_committed`. This qualifies safe
  reinitialization, not transparent continuation or an assertion about the
  old transaction's outcome.
- Manual `Live Kafka Smoke` run
  [`31573662135`](https://github.com/TaeeunKil/kafrust/actions/runs/31573662135)
  passed all 16 jobs on 2026-08-12. The Kafka 3.7.2 plaintext three-broker
  profile repeated producer and direct-consumer leader failover after stopping
  broker 1, restoring it, then stopping a broker 2 leader partition. Both
  recovery windows completed without losing the client process.

## M15 Compression Compatibility

Status: Complete.

Goal: support common compressed Kafka record batches while preserving the no-required-C-toolchain policy.

Scope:

- gzip
- snappy
- lz4
- zstd evaluation under the project rule against required C toolchains
- compressed Produce request encoding
- compressed Fetch response decoding
- size and decompression safety limits

Exit criteria:

- producer can send compressed record batches with supported pure Rust codecs
- consumer can decode compressed batches for supported codecs
- unsupported or disabled codecs fail with typed, documented errors
- decompression limits prevent unbounded allocation or decompression bomb behavior
- live smoke or focused broker checks cover gzip, snappy, lz4, and zstd

Strategic role:

- Compression is required for realistic Kafka throughput and for compatibility with existing topics.

Evidence:

- Gzip compression is implemented with a Rust backend and no required C
  toolchain.
- Produce v3 RecordBatch encoding can write gzip-compressed record payloads.
- Fetch v4 RecordBatch decoding can read gzip-compressed record payloads.
- `ProducerConfig::compression(Compression::Gzip)` enables gzip for immediate,
  batch, and buffered producer paths when Produce API v3 is available.
- Manual `Live Kafka Smoke` run `28009105074` passed on 2026-06-23; the
  single-node and multi-broker plaintext jobs completed gzip batch producer
  checks against Kafka 3.7.2.
- Unsupported codecs currently return typed protocol errors instead of being
  decoded as uncompressed data.
- Gzip decompression is bounded to prevent unbounded decoded record payload
  growth.
- Snappy compression uses the pure-Rust `snap` backend with
  Kafka-compatible Xerial framing and no C toolchain dependency.
- Produce v3 RecordBatch encoding writes chunked Snappy frames, while Fetch v4
  RecordBatch decoding accepts both Xerial-framed and raw Snappy payloads.
- Snappy decoding validates each block's declared output length before
  allocation and enforces the record batch decompression limit.
- Focused tests cover multi-block Snappy roundtrips, raw-block compatibility,
  oversized declared output, malformed framing, and Produce-to-Fetch RecordBatch
  roundtrips.
- Manual `Live Kafka Smoke` run `29984929590` passed on 2026-07-23; the
  single-node and multi-broker plaintext jobs completed Snappy batch producer
  checks against Kafka 3.7.2.
- LZ4 compression uses the pure-Rust `lz-fear` backend with independent blocks
  and no C toolchain dependency.
- Produce v3 RecordBatch encoding writes standard LZ4 frames, and Fetch v4
  RecordBatch decoding reads those frames with a bounded output size.
- Focused tests cover the Kafka LZ4 frame magic, multi-block roundtrips,
  malformed frames, decompression limits, and Produce-to-Fetch RecordBatch
  roundtrips.
- Manual `Live Kafka Smoke` run `29986018854` passed on 2026-07-23; the
  single-node and multi-broker plaintext jobs completed LZ4 batch producer
  checks against Kafka 3.7.2.
- Zstd compression uses the pure-Rust `ruzstd` 0.8.1 backend with its optional
  checksum dependency disabled and no C toolchain dependency.
- Produce v7 RecordBatch encoding writes standard Zstd frames, while Fetch v4
  RecordBatch decoding validates declared content and window sizes before
  decoder allocation and bounds decoded output to 64 MiB.
- Focused tests cover the Zstd frame magic, multi-block roundtrips, malformed
  frames, declared window limits, decoded output limits, and Produce-to-Fetch
  RecordBatch roundtrips.
- Manual `Live Kafka Smoke` run
  [`29988390924`](https://github.com/TaeeunKil/kafrust/actions/runs/29988390924)
  passed on 2026-07-23; the
  single-node and multi-broker plaintext jobs completed Zstd Produce v7 batch
  checks against Kafka 3.7.2.

- All four decoders enforce the configurable
  `max_decompressed_record_bytes` limit inherited from `ClientConfig`,
  `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig`. Oversized
  output returns a typed `protocol::Error::LimitExceeded` failure.

## M16 Admin API MVP

Status: Complete.

Goal: provide the admin operations needed by common applications and test harnesses.

Scope:

- list topics and describe cluster metadata
- create topics
- delete topics
- describe topic configs
- alter basic topic configs
- describe consumer groups
- list and delete groups
- delete consumer group offsets evaluation
- list and alter committed consumer group offsets
- admin examples and typed request errors

Exit criteria:

- users can provision and inspect test topics without external Kafka CLI tools
- admin APIs expose Kafka concepts directly instead of generic resource abstractions
- live smoke covers create, describe, produce/fetch, and cleanup for a topic
- unsupported admin APIs are explicit and documented

Strategic role:

- Admin support reduces friction for integration tests, smoke workflows, and service bootstrap code.

Implemented evidence:

- `AdminClient::describe_cluster` exposes typed broker IDs, advertised
  endpoints, rack IDs, and the active controller. `AdminClient::list_topics`
  exposes names, internal-topic flags, partition counts, and topic-level Kafka
  error classifications. Both read-only metadata paths retry transport and
  timeout failures within the bounded AdminClient budget. `list_topics` also
  retries transient topic/partition metadata errors while preserving final
  topic-level partial errors.
- Injected broker tests distinguish Metadata v1's empty topic array for
  cluster-only inspection from its null array for all-topic listing and verify
  broker error metrics for partial metadata failures.
- DescribeConfigs v1 supports all or selected topic keys, optional synonyms,
  nullable and sensitive values, raw resource errors, typed config sources,
  broker throttle time, tracing, and shared broker-error metrics.
- IncrementalAlterConfigs v0 exposes Set, Delete, Append, and Subtract
  operations, validate-only mode, resource-level atomicity and partial
  outcomes, broker throttle time, tracing, and broker-error metrics.
- Classic AlterConfigs v1 exposes a typed `TopicConfigUpdate` builder for
  complete dynamic topic configuration maps, including null-valued deletion,
  validate-only mode, resource-level outcomes, broker throttle time, tracing,
  and broker-error metrics. Focused protocol and injected-broker tests pass;
  the admin lifecycle example exercises classic replacement followed by
  incremental alteration. The complete 17-job matrix qualified the plaintext
  lifecycle on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plus the Kafka 3.7.2
  three-broker profile in
  [`31669906872`](https://github.com/TaeeunKil/kafrust/actions/runs/31669906872).
  The same lifecycle also passed over Kafka 3.7.2 TLS, SASL/PLAIN, and
  SASL_SSL SCRAM-SHA-256 in the complete matrix
  [`31674680581`](https://github.com/TaeeunKil/kafrust/actions/runs/31674680581),
  qualifying those secured Admin AlterConfigs profiles. Post-transmission
  mutation recovery remains a separate gate.
- Delegation token lifecycle APIs cover CreateDelegationToken,
  DescribeDelegationToken, RenewDelegationToken, and ExpireDelegationToken.
  The high-level Admin API negotiates v1-v3 or v1-v2 ranges, routes through the
  active controller, retries only pre-transmission discovery, and redacts HMAC
  values from debug and tracing output. Focused protocol and
  injected-controller tests pass. The Kafka 4.3.1 three-broker
  SASL_PLAINTEXT lifecycle smoke passed in the complete 17-job matrix at
  commit `9d3916f` in
  [`31688516207`](https://github.com/TaeeunKil/kafrust/actions/runs/31688516207).
  Secured profiles beyond SASL_PLAINTEXT and post-transmission mutation
  recovery remain separate qualification gates. The same lifecycle also
  passed over TLS with SCRAM-SHA-256 on Kafka 3.7.2 in the complete matrix
  [`31689260396`](https://github.com/TaeeunKil/kafrust/actions/runs/31689260396),
  completing the documented single-broker SASL_SSL/SCRAM gate. The same
  create, describe, renew, and immediate-expire lifecycle also passed over
  three-broker SASL_SSL with SCRAM-SHA-256 in the complete matrix
  [`31691911558`](https://github.com/TaeeunKil/kafrust/actions/runs/31691911558)
  (job [`94420894174`](https://github.com/TaeeunKil/kafrust/actions/runs/31691911558/job/94420894174)).
  Token-authenticated data-plane failover and post-transmission mutation
  recovery remain separate claims.
- DescribeGroups v1 discovers each requested group's coordinator independently
  and preserves state, protocol, member identity, raw protocol metadata and
  assignments, per-group errors, throttle time, tracing, and metrics.
- ListGroups v1 queries every advertised broker and returns sorted,
  deduplicated listings with protocol type, coordinator ID, and throttle time.
- DeleteGroups v1 routes each group to its coordinator and preserves
  per-group results, including a typed `NonEmptyGroup` classification.
- OffsetDelete v0 routes to the group's coordinator and preserves its
  top-level group error plus every per-partition result. Typed classifications
  cover missing groups and active topic subscriptions.
- OffsetFetch v2 and OffsetCommit v2 expose typed classic consumer-group offset
  listing and administrative alteration. Queries can target selected
  topic-partitions or all topics, offset updates are grouped by topic, and
  top-level plus per-partition errors remain observable. Focused wire and
  coordinator-routing tests pass, and the Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 live smoke paths are qualified in
  [`31595485915`](https://github.com/TaeeunKil/kafrust/actions/runs/31595485915).
  Multi-broker, TLS, SASL_PLAINTEXT, and SASL_SSL/SCRAM routing are qualified
  in [`31597505667`](https://github.com/TaeeunKil/kafrust/actions/runs/31597505667);
  Admin coordinator discovery also retries transient coordinator errors and
  discovery transport failures with bounded exponential backoff; the focused mock-broker regression
  test covers `CoordinatorLoadInProgress`, bootstrap reconnect, and the
  follow-up OffsetFetch request. The read-only admin `OffsetFetch v2` path also
  reconnects and retries after a coordinator disconnect, request timeout, or
  transient coordinator response. The exact-offset administrative
  `OffsetCommit v2` path retries the same state-idempotent commit after the
  same transport or coordinator response failures. Other administrative write
  retry semantics remain deliberately conservative while ambiguous outcomes are
  not yet modeled. Read-only `DescribeGroups v1` now shares the coordinator
  reconnect path, with focused coverage for a dropped request and successful
  rediscovery. The default five-attempt budget is configurable through
  `AdminClient::max_retries`, including disabling retries with zero. The
  secured Kafka 3.7.2 three-broker SASL_SSL/SCRAM profile also holds the
  read-only DescribeGroups and OffsetFetch plus exact-offset OffsetCommit
  requests before transmission, stops their active group coordinator, and
  completes all three operations with `retries=1` after rediscovery in
  [`31698102459`](https://github.com/TaeeunKil/kafrust/actions/runs/31698102459)
  ([job](https://github.com/TaeeunKil/kafrust/actions/runs/31698102459/job/94440433930)).
  Other coordinator-routed mutations remain separate workload-specific gates
  because their post-transmission outcomes are not generally safe to replay.
  The read-only `DescribeProducers v0` path now retries transient leader movement,
  metadata convergence, transport, and timeout failures through fresh Metadata
  v1 routing; transient per-partition leader responses are also retried. The
  `DescribeTransactions v0` path retries coordinator rediscovery, transport,
  and transient per-ID coordinator responses. Focused mock-broker tests cover
  dropped requests and transient responses for both APIs. The latest
  three-broker live profile also gates DescribeGroups v1 and
  DescribeTransactions v0 before transmission, stops their current
  coordinators, and verifies `retries=1` after rediscovery in
  [`31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778).
  The same three-broker profile now also holds DescribeConfigs v1 before
  transmission, stops the bootstrap broker, and verifies `retries=1` after
  reconnecting through the bootstrap set in
  [`31613935963`](https://github.com/TaeeunKil/kafrust/actions/runs/31613935963).
  It now also queries ListGroups v1 across the advertised brokers, stops broker
  1 after the request gate opens, restarts it while the bounded reconnect loop
  is active, and records `retries=7` before completing the full group listing
  in [`31616181960`](https://github.com/TaeeunKil/kafrust/actions/runs/31616181960).
  The initial ListGroups Metadata discovery now shares the same bounded retry
  path as `describe_cluster` and `list_topics`, with focused coverage for a
  dropped bootstrap response before broker enumeration.
  The same three-broker profile now gates Metadata v1 before transmission,
  stops broker 1, and verifies `retries=1` for both `describe_cluster` and
  `list_topics` after bootstrap failover in
  [`31620595346`](https://github.com/TaeeunKil/kafrust/actions/runs/31620595346).
  The read-only DescribeAcls v1 path also retries transport, timeout, and
  retryable top-level broker failures; a focused mock-broker regression test
  verifies the dropped request and typed ACL response. Authorizer-specific
  broker-stop qualification remains a separate release gate.
  DescribeClientQuotas v0 now applies the same retry policy to its typed filter
  and top-level response, with focused coverage for a dropped request and
  successful quota result. StandardAuthorizer permission and broker-stop
  qualification remain separate release gates. The read-only
  DescribeUserScramCredentials v0 path now applies the same bounded retry
  policy to its nullable user filter and top-level response, with focused
  dropped-request coverage; live credential-policy and broker-stop
  qualification remain separate release gates.
- Controller-routed Admin writes now retry only pre-transmission controller
  discovery failures, including transient Metadata responses, with the bounded
  budget. CreateTopics has focused bootstrap-disconnect and retryable-metadata
  coverage; request transport failures after a mutation is sent remain
  single-attempt because the broker-side outcome is ambiguous.
- Non-controller Admin writes for ACLs, client quotas, and incremental topic
  configs now also retry bootstrap connection failures before their request is
  transmitted. The retry helper has deterministic coverage for the retry
  budget; transport failures after the mutation request remain single-attempt
  for the same ambiguous-outcome reason.
- KIP-848 member-aware administrative offsets negotiate OffsetFetch and
  OffsetCommit v10 when the coordinator advertises the API and Metadata v12
  resolves topic names. Complete topic UUIDs can be attached to the public
  query and offset builders to skip discovery. The APIs map UUID responses
  back to names and fall back to v9 for older coordinators or incomplete
  metadata. The APIs reuse the typed classic
  offset results while preserving throttle and group errors. Focused wire,
  mock, and fault-injection tests cover both paths. The existing Kafka 4.3.1
  single-node and multi-broker PLAINTEXT, SASL_PLAINTEXT, and SASL_SSL/SCRAM
  live qualification now covers the v10 member-aware calls in the complete
  Kafka 4.3.1 matrix (`32339508792`). Published `0.3.1` also passed the fresh
  external-project v10 Admin gate (`32341534974`), including Kafka CLI offset
  verification. Target authorization and broader member-failure workloads
  remain release gates. The live DeleteRecords, DescribeProducers, DescribeTransactions,
  DescribeGroups, OffsetFetch, exact-offset OffsetCommit, DescribeConfigs, and
  ListGroups
  broker-stop gates are covered by
  [`31616181960`](https://github.com/TaeeunKil/kafrust/actions/runs/31616181960);
  other coordinator-routed Admin writes remain separate workload-specific
  release gates.
- DescribeAcls v1, CreateAcls v1, and DeleteAcls v1 expose typed ACL bindings
  and filters through `AdminClient`, preserving top-level, per-entry,
  per-filter, and matching-ACL outcomes. Wire and mock-broker tests cover the
  protocol schemas and partial authorization failures. Manual `Live Kafka Smoke`
  run `31457478358` passed the focused ACL authorizer job against Kafka 3.7.2
  StandardAuthorizer using an explicitly configured `User:ANONYMOUS`
  superuser; target broker policy qualification remains required for production
  migrations.
- DescribeClientQuotas v0 and AlterClientQuotas v0 expose typed entity
  components, exact/default/any filter matching, floating-point values,
  validate-only mode, and per-entity outcomes. Wire and mock-broker coverage
  is complete. The ACL authorizer live profile passed set -> exact-filter
  describe -> remove against Kafka 3.7.2 StandardAuthorizer in run
  `31459874329` on 2026-08-11; the example uses bounded polling for KRaft
  metadata visibility.
- DescribeUserScramCredentials v0 and AlterUserScramCredentials v0 expose
  typed user, mechanism, iteration, and per-user outcome APIs. Flexible v0
  compact-field and tagged-field encoding is covered by wire and mock-broker
  tests. The SASL_SSL SCRAM live profile passed credential upsert -> describe
  -> delete against Kafka 3.7.2 in run `31461980967` on 2026-08-11. Upsertion
  derives the salted password locally and does not retain plaintext passwords
  or expose credential bytes in `Debug` output.
- AlterPartitionReassignments v0 and ListPartitionReassignments v0 expose
  typed replica targets, cancellation, ongoing replica sets, and controller
  routing. Focused wire tests and a controller-routing mock test cover the
  flexible schemas. The Kafka 3.7.2 three-broker profile passed reassignment
  submission and completion polling in live run `31462962605` on 2026-08-11.
  The read-only listing path now re-discovers the controller after transient
  transport, timeout, or retryable broker failures, with focused dropped-request
  coverage; live broker-stop recovery remains a separate release gate.
- ElectLeaders v0-v2 now exposes negotiated preferred and one-shot unclean
  leader elections through `AdminClient`. `None` preserves Kafka's all-eligible
  partition semantics, explicit `LeaderElection` filters preserve per-topic and
  per-partition results, and v0 rejects unclean requests instead of silently
  downgrading them. Focused wire and controller-routing tests pass. The
  multi-broker workflow runs the preferred-election example after reassignment;
  Kafka 3.7.2 returned partition success in
  [`31681439569`](https://github.com/TaeeunKil/kafrust/actions/runs/31681439569).
  The same preferred/no-op path over three-broker SASL_SSL with SCRAM-SHA-256
  passed in the complete matrix
  [`31691204180`](https://github.com/TaeeunKil/kafrust/actions/runs/31691204180).
  Unclean election is deliberately outside the default gate because it can
  lose records.
- DescribeLogDirs v1-v5 now exposes broker-selected log-directory results,
  replica size, offset lag, future-log state, v4+ volume capacity, and v5
  cordoned state. Focused wire and broker-routing tests pass, and the
  multi-broker workflow includes the filtered example. Kafka 3.7.2 returned
  successful filtered responses from all three brokers in
  [`31682889124`](https://github.com/TaeeunKil/kafrust/actions/runs/31682889124),
  including partition size and volume capacity. The same broker-1/2/3 query
  passed over three-broker SASL_SSL with SCRAM-SHA-256 in the complete matrix
  [`31691204180`](https://github.com/TaeeunKil/kafrust/actions/runs/31691204180).
- AlterReplicaLogDirs v1-v2 now exposes explicit broker-local replica movement
  through `AdminClient`, groups assignments by destination directory, preserves
  per-partition broker outcomes, and negotiates the Kafka 3.7 baseline (v1) or
  flexible schema (v2). Focused wire and injected-broker tests pass. The
  mutating path retries only connection and ApiVersions discovery before
  transmission and never replays an ambiguous send. The Kafka 3.7.2
  three-broker matrix moved a disposable replica to `/tmp/kafka-logs-2` and
  observed `future=false` completion in
  [`31688516207`](https://github.com/TaeeunKil/kafrust/actions/runs/31688516207),
  completing this configured-cluster gate. The same configured movement over
  three-broker SASL_SSL with SCRAM-SHA-256 passed in the complete matrix
  [`31691204180`](https://github.com/TaeeunKil/kafrust/actions/runs/31691204180).
- The `admin_describe_group` example runs after the consumer-group smoke path
  across plaintext, multi-broker, TLS, SASL_PLAINTEXT, and SASL_SSL profiles.
- The admin lifecycle example waits for asynchronous metadata propagation in
  multi-broker clusters and verifies `cleanup.policy` through
  `describe_topic_configs` before deleting the topic.
- CreateTopics v2 request encoding and response decoding preserve automatic
  and manual replica assignment, nullable topic configs, validate-only mode,
  broker timeout, throttle time, and topic-level partial failures.
- `AdminClient::create_topics` discovers the current controller through
  Metadata v1 and routes the request using the security, timeout, decode-limit,
  and metrics settings from `ClientConfig`.
- `NewTopic`, `CreateTopicsOptions`, `CreateTopicsResult`, and
  `CreateTopicResult` expose Kafka topic creation concepts without flattening
  partial responses into a single generic error.
- DeleteTopics v3 request encoding and response decoding preserve topic-level
  partial failures and broker throttle time. `AdminClient::delete_topics`
  shares the controller routing, security configuration, tracing, and metrics
  behavior of topic creation.
- Focused byte-level tests and an injected two-connection test cover protocol
  encoding, decoding, controller routing, topic error preservation, and broker
  error metrics.
- The `admin_create_topic` example creates a topic, verifies it through a
  subsequent metadata lookup, and deletes it. The live Kafka workflow runs it
  against the Kafka 3.7.2 and current stable single-node profiles and the
  Kafka 3.7.2 three-broker profile.
- `AdminClient::delete_records` implements DeleteRecords v1 with metadata-based
  partition-leader routing, groups requests per broker, and preserves each
  partition's low watermark and error code. Fixed-offset deletion is retried
  after retryable Metadata responses, transient transport, leader-movement, or
  retryable partition errors through fresh metadata within the Admin retry
  budget. Focused protocol and
  injected multi-broker routing tests cover partial success, broker error
  metrics, and a dropped leader request; live destructive-retention
  qualification remains a separate opt-in workflow. The live three-broker
  profile now gates the request before TCP transmission, stops its current
  leader, and verifies fresh-metadata recovery with `retries=1` in
  [`31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778).
- `AdminClient::describe_producers` implements DescribeProducers v0 with
  metadata-based partition-leader routing and preserves producer IDs, epochs,
  sequence state, transaction offsets, and per-partition errors. The paired
  `describe_transactions` API discovers transaction coordinators, groups IDs
  per coordinator, and preserves transaction state, producer identity, and
  topic membership. Focused wire and injected routing tests cover both paths.
  The complete 17-job `Live Kafka Smoke` run
  [`31589394777`](https://github.com/TaeeunKil/kafrust/actions/runs/31589394777)
  live-verified both examples on the supported single-node plaintext matrix,
  DescribeProducers on the Kafka 3.7.2 three-broker profile, and
  DescribeTransactions through the Kafka 3.7.2 three-broker SASL_SSL SCRAM
  failover profile. Both read-only paths now retry transient leader/coordinator
  movement, retryable Metadata responses, metadata convergence, transport
  disconnects, request timeouts, and transient routed response errors through
  fresh discovery within the configurable `AdminClient::max_retries` budget.
  Focused mock-broker tests
  cover dropped and transient responses, while the latest 17-job live matrix
  at commit `65b607e` passed the current single-node, secured, multi-broker,
  and KIP-848 examples in
  [`31601732149`](https://github.com/TaeeunKil/kafrust/actions/runs/31601732149).
  Target authorization policy and coordinator-routed Admin broker-stop
  injection remain workload-specific release gates. The same live profile now
  also gates DescribeProducers v0 before transmission, stops its current
  leader, and verifies `retries=1` after fresh-metadata recovery in
  [`31612533778`](https://github.com/TaeeunKil/kafrust/actions/runs/31612533778).
- Manual `Live Kafka Smoke` run `30059517473` passed CreateTopics v2 and its
  follow-up Metadata v1 description on 2026-07-24 against Kafka 3.7.2 and
  4.3.1 single-node brokers and the Kafka 3.7.2 three-broker cluster.
- Manual run `30060723690` passed cluster/topic inspection, bounded metadata
  propagation, CreateTopics v2, DescribeConfigs v1, and DeleteTopics v3 on
  Kafka 3.7.2 and 4.3.1 single-node brokers and the Kafka 3.7.2 three-broker
  cluster. The same three-broker job passed the subsequent broker-stop
  producer/consumer failover checks.
- Manual run `30061073263` passed IncrementalAlterConfigs v0 update and
  DescribeConfigs v1 readback on Kafka 3.7.2 and 4.3.1 single-node brokers and
  the Kafka 3.7.2 three-broker cluster, followed by the full existing smoke and
  failover sequence.
- Manual run `30061497355` passed DescribeGroups v1 on Kafka 3.7.2 and 4.3.1
  plaintext brokers plus TLS, SASL_PLAINTEXT, and SASL_SSL profiles. The
  three-broker job passed DescribeGroups and broker-stop failover before the
  run result was recorded.
- Manual run `30062203069` passed OffsetDelete v0 after broker-side group
  session expiry on all six live profiles, including Kafka 3.7.2 and 4.3.1,
  TLS, SASL_PLAINTEXT, SASL_SSL, and three brokers. The three-broker job also
  passed its subsequent broker-stop producer, consumer, and group checks.
- Manual run `30065771327` passed broker-wide ListGroups v1 and
  coordinator-routed DeleteGroups v1 across Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 plaintext brokers, TLS, SASL_PLAINTEXT, SASL_SSL, and the three-broker
  profile. The cleanup path accepted Kafka's expected `GroupIdNotFound` after
  OffsetDelete removed the empty group's final committed offset.

## M17 Idempotent Producer

Status: Complete.

Goal: support Kafka idempotent producer semantics for duplicate-safe retries within a producer session.

Scope:

- InitProducerId
- producer ID and epoch tracking
- per-topic-partition sequence numbers
- max in-flight request limits compatible with idempotence
- retry behavior that preserves Kafka ordering and sequence rules
- broker error handling for producer fencing, out-of-order sequence, and duplicate sequence cases

Exit criteria:

- idempotence can be enabled explicitly through producer configuration
- retries do not produce duplicate acknowledged records within the supported broker profile
- sequence state is scoped per topic partition and reset only under documented conditions
- focused tests cover sequence assignment, retry, fencing, and fatal idempotence errors
- live smoke verifies an idempotent send path against a real broker

Strategic role:

- This is a major requirement before kafrust can replace mature clients for many write-heavy services.

Evidence:

- `InitProducerId v2` request/response protocol types and the low-level client
  roundtrip are implemented with byte-level and injected-broker tests. The
  producer negotiates v2 when advertised and falls back to v0 for older brokers.
- RecordBatch v2 encoding accepts producer ID, producer epoch, and base
  sequence metadata while preserving the non-idempotent sentinel values.
- `ProducerConfig::enable_idempotence(true)` initializes a non-transactional
  producer ID, enforces `acks=all` with retries, and keeps acknowledged
  sequences scoped per topic partition for single-record, batch, and buffered
  sends.
- Batch sequence reservations are retained by input record across request and
  partial-record retries. Acknowledged state advances only after broker
  success, and later chunks are held back after a failed idempotent chunk to
  preserve partition ordering.
- `DUPLICATE_SEQUENCE_NUMBER` is accepted as an already delivered retry with
  unknown offset and timestamp metadata. `OUT_OF_ORDER_SEQUENCE_NUMBER`,
  `INVALID_PRODUCER_EPOCH`, and `PRODUCER_FENCED` are classified as fatal and
  leave the producer instance defunct for subsequent sends.
- A fatal idempotent error during an active transaction transitions the
  transaction state to terminal `Defunct`, clears registered partitions, and
  makes `in_transaction()` return false without claiming a commit or abort
  outcome. A focused injected `EndTxn` regression verifies
  `INVALID_PRODUCER_EPOCH` and repeated-command behavior after fencing; the
  application must discard that producer and determine any prior outcome
  separately.
- A deterministic injected-broker test drops the connection after receiving
  the first Produce request, verifies that the retry frame is byte-for-byte
  identical, returns `DUPLICATE_SEQUENCE_NUMBER`, and verifies one sequence
  advancement with unknown delivery metadata.
- Manual `Live Kafka Smoke` run `29991254722` passed the idempotent
  single-record, batch, and buffered producer paths against Kafka 3.7.2 and
  Kafka 4.3.1; all six plaintext, multi-broker, TLS, SASL_PLAINTEXT, and
  SASL_SSL jobs passed.
- Manual `Live Kafka Smoke` run
  [`31495298593`](https://github.com/TaeeunKil/kafrust/actions/runs/31495298593)
  passed idempotent producer recovery through the three-broker broker-stop
  window. The failover example keeps idempotence enabled for both sends and
  completed with all 11 plaintext, secured, multi-broker, ACL, and KIP-848
  jobs green.

## M18 Transactions And Read-Committed Consumers

Status: Complete.

Goal: support Kafka exactly-once workflows where applications need transactional produce and read-committed consumption.

Scope:

- transactional producer API
- begin, commit, and abort transaction flows
- AddPartitionsToTxn
- AddOffsetsToTxn
- TxnOffsetCommit
- EndTxn
- transactional error classification and producer fencing
- read-committed consumer behavior

Exit criteria:

- users can produce to multiple partitions in one transaction
- users can commit consumed offsets as part of a transaction where supported
- aborted transaction records are hidden from read-committed consumers
- transaction state transitions are explicit and documented
- live smoke verifies commit and abort paths against a real broker

Strategic role:

- This is required for broad replacement of clients used in exactly-once and stream-processing-style services.

Evidence:

- `EndTxn v3` request and response protocol types encode commit and abort
  results using Kafka API key 26 and decode coordinator throttle/error fields.
  The producer negotiates v3 when advertised and falls back to v0 for older
  brokers.
- `Client::end_txn_v3` and `Client::end_txn_v0` provide the low-level framed
  roundtrips, covered by byte-level commit/abort tests and an injected-broker
  response test.
- `FindCoordinator v1` now exposes transaction coordinator discovery using
  coordinator type 1, with protocol and injected-broker client coverage.
- `AddPartitionsToTxn v3` now carries the flexible compact/tagged request and
  response shape, with a negotiated `v0` fallback for older brokers. The
  low-level roundtrips and high-level producer selection are covered by
  byte-level and injected-broker tests.
- `AddOffsetsToTxn v3` now carries the flexible compact/tagged request and
  response shape, with a negotiated `v0` fallback for older brokers. The
  low-level roundtrips and high-level producer selection are covered by
  byte-level and injected-broker tests.
- `TxnOffsetCommit v0` encodes transactional topic-partition offsets and
  metadata, and preserves partition-scoped group errors through the low-level
  client roundtrip.
- `ProducerConfig::transactional_id` initializes a transactional producer ID
  and enforces idempotent producer settings. `Producer::begin_transaction`,
  `commit_transaction`, and `abort_transaction` expose explicit state
  transitions; sends outside an active transaction are rejected. A lost
  `EndTxn` response returns `Error::TransactionOutcomeUnknown`, transitions
  the producer to terminal `TransactionStatus::Defunct`, and rejects further
  transaction commands so callers cannot assume an abort or retry on the old
  producer.
- Transactional sends register each topic partition through negotiated
  `AddPartitionsToTxn v3` when the coordinator advertises it, fall back to
  `v0` for older brokers, pass the transactional ID to Produce v3/v7, and
  complete through negotiated `EndTxn v3`, falling back to v0. Transactional
  Produce requests set the
  RecordBatch transactional attribute as well as the request transactional ID.
- Transactional initialization discovers the transaction coordinator before
  `InitProducerId`. Partition registration rediscovers and retries transient
  coordinator errors, including `CONCURRENT_TRANSACTIONS`, using the configured
  retry limit.
- `IsolationLevel::ReadCommitted` is available on direct and group consumer
  configurations. Fetch v4 preserves producer and transactional/control batch
  metadata, hides control records, and filters aborted producer ranges while
  advancing poll offsets past hidden records.
- `Producer::send_group_offsets_to_transaction` binds current
  `ConsumerGroup::metadata` and assignments through negotiated
  `AddOffsetsToTxn v3` with a `v0` fallback and commits offsets through
  generation-fenced `TxnOffsetCommit v3` before EndTxn. Transaction
  initialization, partition registration, offset integration, and completion
  rediscover coordinators and retry transient coordinator errors within the
  configured retry limit.
- Manual `Live Kafka Smoke` run `29995762812` passed commit, abort,
  read-uncommitted versus read-committed isolation, and a consume-transform-
  produce transaction that committed group offsets against Kafka 3.7.2 and
  Kafka 4.3.1. All six plaintext, multi-broker, TLS, SASL_PLAINTEXT, and
  SASL_SSL jobs passed.
- Manual run `30063099869` passed the generation-fenced `TxnOffsetCommit v3`
  path on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext brokers plus the
  Kafka 3.7.2 TLS, SASL_PLAINTEXT, SASL_SSL, and three-broker profiles.
- `BufferedProducer` exposes serialized begin, group-offset attachment,
  commit, and abort commands. Commit drains accepted deliveries before
  `EndTxn`, blocks on delivery failure, and leaves the transaction active for
  an explicit abort. Active transactions cannot be closed accidentally.
- Manual run `30334327631` passed buffered commit and abort visibility,
  read-committed filtering, and generation-fenced group offset attachment on
  Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext brokers. The Kafka 3.7.2
  three-broker, TLS, SASL_PLAINTEXT, and SASL_SSL regression profiles also
  passed.
- Transaction coordinator discovery, connection, and request transport
  failures reconnect through the configured bootstrap set before retrying
  transactional initialization, partition registration, group offset
  attachment, offset commit, or transaction completion.
- Manual run `30335739033` stopped the active transaction coordinator in the
  Kafka 3.7.2 three-broker profile after a transactional Produce, then passed
  `EndTxn` commit and read-committed fetch-back through the remaining brokers.
  The stopped broker was restored before the existing broker-stop failover
  sequence, and all eight jobs passed.
- Manual `Live Kafka Smoke` run
  [`31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594)
  stopped the active Kafka 3.7.2 `SASL_PLAINTEXT` transaction coordinator,
  verified commit and `read_committed` recovery, then stopped the group
  coordinator and verified consumer-group recovery with SASL/PLAIN.
- A deterministic injected-broker test drops the connection after receiving
  `EndTxn`, verifies `TransactionOutcomeUnknown`, and verifies that the
  producer is terminally `Defunct` and cannot begin another transaction.
- The Kafka 3.7.2 three-broker live gate
  [`31708995196`](https://github.com/TaeeunKil/kafrust/actions/runs/31708995196/job/94476744970)
  drops the first `EndTxn` response, verifies that the producer reports the
  unknown outcome without replaying `EndTxn`, then verifies same-
  transactional-ID producer recovery and `read_committed` reconciliation.

Known limits:

- Transparent continuation after an unknown outcome is intentionally not
  provided; callers must discard the defunct producer and reinitialize it.
- Broader live transaction failure-injection beyond the verified coordinator
  broker-stop and response-drop paths, plus sustained transaction throughput,
  is not yet claimed.

## M19 Observability, Limits, And Performance

Status: Complete.

Goal: make kafrust measurable, tunable, and safe under sustained load.

Scope:

- metrics for requests, retries, errors, bytes, records, batches, queue depth, and latency
- structured tracing spans across complete producer, consumer, and group operations
- memory limits for producer buffers, fetch responses, decompression, and decode arrays
- producer and consumer throughput benchmarks
- latency benchmarks for common record sizes
- load, soak, and failure-injection test profiles

Exit criteria:

- users can observe throughput, latency, retries, and broker errors without inspecting payloads
- benchmark baselines are published for selected broker profiles
- configured memory limits produce typed errors instead of unbounded growth
- soak tests run long enough to catch connection, timer, and background task leaks
- docs explain operational tuning knobs and tradeoffs

Strategic role:

- Without observability and limits, kafrust cannot be responsibly adopted as a production client dependency.

Implemented evidence:

- `ClientMetrics` provides shared lock-free counters for started, successful,
  failed, timed-out, cancelled, current and peak in-flight request
  roundtrips, current and peak buffered producer records, request and
  response payload bytes, and total and maximum latency. Snapshots also expose
  a fixed upper-bound request latency histogram and approximate percentile
  queries for p50, p95, and p99 operational checks.
- `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and
  `ConsumerGroupConfig` accept a shared metrics handle. Every bootstrap,
  leader, coordinator, TLS, and SASL connection created from that
  configuration retains the same handle.
- Request start, response, and failure events now execute inside a
  `kafka.request` tracing span with API key, API version, correlation ID, and
  request byte count. Payload contents remain excluded.
- Focused tests cover shared success/failure accounting, timeout
  classification, byte counters, latency, cancellation cleanup, in-flight
  gauge cleanup, and percentile bucket selection.
- The shared metrics snapshot counts actual additional attempts for producer
  sends, partial batch retries, consumer fetches, metadata reconnects,
  idempotent initialization, transactional coordinator operations, and
  automatic consumer-group rejoins.
- Broker response frame allocation is bounded to 100 MiB by default and is
  configurable through all four client configuration builders. Oversized frame
  declarations return typed `Error::ResponseTooLarge { size, max }` failures
  before response payload allocation.
- Buffered producer command capacity is bounded to 1024 records by default and
  configurable through `ProducerConfig::buffer_capacity`. Full queues apply
  async backpressure, while shared metrics report current and maximum
  outstanding accepted records through lifecycle-safe gauges.
- Shared metrics count acknowledged produced records, successful
  topic-partition Produce chunks, and records returned after consumer
  isolation filtering and poll limits.
- Kafka response arrays, nested record counts, and record headers are checked
  before vector allocation. The default maximum is 1,000,000 elements and is
  configurable through all four client configuration builders.
- Fetched record batches are bounded to 64 MiB after decompression by default.
  The configurable limit is inherited by nested Fetch decoders and enforced by
  gzip, Snappy, LZ4, and Zstd, with typed
  `protocol::Error::LimitExceeded { kind, actual, max }` failures.
- Debug-level spans cover immediate and buffered producer operations,
  transaction completion and offset attachment, direct-consumer poll/fetch,
  and consumer-group join, poll, background/explicit heartbeat, and offset
  commit. Existing `kafka.request` spans nest under these operation spans, and
  all fields exclude record and protocol payload contents.
- The `throughput_benchmark` live example measures end-to-end batch Produce and
  offset-based Fetch throughput, Produce batch p50/p95/p99 latency, fixed-
  bucket Kafka request p50/p95/p99 upper-bound estimates, request counts,
  retries, and peak in-flight/buffered gauges. The manual `Kafka Benchmark`
  workflow runs selected payload and compression profiles against Kafka 4.3.1
  and uploads JSONL results for comparison. These peaks make queue growth
  visible even when the final gauges return to zero; threshold qualification
  remains a separate M21 workload/SLO gate.
- The published secure multi-broker soak now emits records/s, operation-error
  rate, retry ratio, and failed-request counters. Its workflow defaults to a
  10,000 records/s floor, 1,000 operation errors, 100 failed requests, and a
  1.0% retry-ratio ceiling while retaining recovery and final resource-drain
  checks. These are configurable qualification thresholds, not universal SLOs;
  target-service evidence remains required for the 1.0 replacement claim.
- Manual benchmark run `30057817575` published the first selected-profile
  baseline on 2026-07-24. The 1-KiB profiles reached 47,883 records/s
  uncompressed and 50,555 records/s with Zstd on a GitHub-hosted runner.
  Standard-check-vector table CRC and logarithmic exact-size batch selection
  improved those profiles by 37.6x and 29.1x over run `30057137300`.
- The `soak` live example continuously pairs acknowledged Produce batches with
  offset-based Fetch reads, verifies final record counts and zero in-flight and
  buffered gauges, and can require an observed error followed by recovery.
- The weekly `Kafka Soak` workflow runs the profile against Kafka 4.3.1,
  restarts the broker during active load, and uploads the final JSON result.
- Manual soak run `30058270907` passed on 2026-07-24: 1,038,200 records
  completed in 60 seconds across a ten-second broker outage, 145 high-level
  operation errors and 1,011 internal retries were observed, recovery
  completed, and both final resource gauges were zero.
- Merged `main` soak run
  [`31562320726`](https://github.com/TaeeunKil/kafrust/actions/runs/31562320726)
  passed a 120-second Kafka 4.3.1 broker restart profile with 6,019,400
  records, 135 operation errors, 678 failed requests, and 944 retries.
  Recovery completed and the final in-flight and buffered-record gauges were
  both zero.
- Scheduled `Kafka Soak` run
  [`31568595989`](https://github.com/TaeeunKil/kafrust/actions/runs/31568595989)
  passed a 300-second Kafka 4.3.1 broker restart profile with 17,019,900
  1-KiB records, 190 operation errors, 1,118 failed requests, and 1,329
  retries. Recovery completed and the final in-flight and buffered-record
  gauges were both zero; the result artifact reports approximately 56.7k
  records/s over the five-minute window.
- Latest `main` benchmark run
  [`31569180500`](https://github.com/TaeeunKil/kafrust/actions/runs/31569180500)
  published 20,000-record Kafka 4.3.1 baselines: 109,368 records/s for
  100-byte payloads, 58,135 records/s for 1-KiB payloads, 3,295 records/s for
  10-KiB payloads, and 55,226 records/s for 1-KiB Zstd payloads. All four
  profiles completed with zero retries.
- Merged `main` benchmark run
  [`31562321010`](https://github.com/TaeeunKil/kafrust/actions/runs/31562321010)
  published fresh Kafka 4.3.1 release-profile baselines: 104,277 records/s
  for 100-byte payloads, 54,649 records/s for 1-KiB payloads, 3,249 records/s
  for 10-KiB payloads, and 59,488 records/s for 1-KiB Zstd payloads. All four
  profiles completed with zero retries.
- Latest `main` benchmark run
  [`31574062876`](https://github.com/TaeeunKil/kafrust/actions/runs/31574062876)
  published 20,000-record Kafka 4.3.1 baselines: 115,388 records/s for
  100-byte payloads, 55,938 records/s for 1-KiB payloads, 3,292 records/s for
  10-KiB payloads, and 64,355 records/s for 1-KiB Zstd payloads. All four
  profiles completed with zero retries.
- Newer latest `main` benchmark run
  [`31621648602`](https://github.com/TaeeunKil/kafrust/actions/runs/31621648602)
  published 20,000-record Kafka 4.3.1 baselines: 142,018 records/s for
  100-byte payloads, 68,037 for 1-KiB, 3,773 for 10-KiB, and 68,922 for
  1-KiB Zstd. All four profiles completed with zero retries.
- Latest `main` benchmark run
  [`31757363941`](https://github.com/TaeeunKil/kafrust/actions/runs/31757363941)
  published the first JSONL baseline containing request p50/p95/p99
  upper-bound estimates from `ClientMetricsSnapshot`, alongside the existing
  high-level batch latency fields. Kafka 4.3.1 completed all four profiles with
  zero retries; the request values are approximate fixed-bucket measurements,
  not direct throughput or cross-client parity claims.
- Newer latest `main` five-minute soak run
[`31621654970`](https://github.com/TaeeunKil/kafrust/actions/runs/31621654970)
processed 16,773,500 1-KiB records across a ten-second broker outage, with
147 operation errors, 774 failed requests, and 1,028 retries. Recovery
completed and both final resource gauges were zero.
- Latest `main` five-minute soak run
  [`31631358207`](https://github.com/TaeeunKil/kafrust/actions/runs/31631358207)
  processed 16,847,700 1-KiB records across a ten-second broker outage, with
  148 operation errors, 782 failed requests, and 1,035 retries. Recovery
  completed and both final resource gauges were zero.
- Latest `main` benchmark run
  [`31631563194`](https://github.com/TaeeunKil/kafrust/actions/runs/31631563194)
  published 20,000-record Kafka 4.3.1 baselines: 118,556 records/s for
  100-byte payloads, 54,006 for 1-KiB, 3,030 for 10-KiB, and 60,486 for
  1-KiB Zstd. All four profiles completed with zero retries.
- Latest `main` soak run
  [`31574065286`](https://github.com/TaeeunKil/kafrust/actions/runs/31574065286)
  passed a 120-second Kafka 4.3.1 broker restart profile with 6,223,500
  records, 136 operation errors, 685 failed requests, and 950 retries.
  Recovery completed and the final in-flight and buffered-record gauges were
  both zero.
- Published `kafrust 0.2.28` performance run
  [`31744206188`](https://github.com/TaeeunKil/kafrust/actions/runs/31744206188)
  passed four fresh external projects against Kafka 3.7.2 and 4.3.1 with no
  compression and Zstd. The 10,000-record, 1-KiB, batch-size-200 workload
  measured 43.7k-48.9k producer records/s and 210.6k-268.3k consumer records/s,
  with p50/p95/p99 batch latency recorded, zero retries, and zero final queue
  gauges. This closes the published performance-baseline gate; it does not
  claim production SLO or long-running soak evidence.
- The published direct comparison run
  [`31753172293`](https://github.com/TaeeunKil/kafrust/actions/runs/31753172293)
  passed a fresh external `kafrust 0.2.28` versus `rust-rdkafka 0.39.0`
  profile against Kafka 4.3.1. Both used fresh one-partition topics, 2,000
  1-KiB records, and batches of 100. Kafrust measured 51,834 producer and
  129,875 consumer records/s; rust-rdkafka measured 48,452 producer and
  252,306 consumer records/s. This closes the direct benchmark evidence gap,
  but not API/feature parity, production SLO, or universal performance claims.
- Published `kafrust 0.2.28` soak run
  [`31744827441`](https://github.com/TaeeunKil/kafrust/actions/runs/31744827441)
  passed Kafka 4.3.1 after a broker stop at one third of a 120-second run and
  a ten-second outage. The fresh external project processed 7,229,000 records,
  observed 173 operation errors, 982 failed requests, and 1,210 retries, then
  recovered with `recovered=true` and zero final queue gauges. This closes the
  published single-node soak gate; multi-broker soak, production SLO, and
  canary evidence remain open.
- Published `kafrust 0.2.28` multi-broker soak run
  [`31746182158`](https://github.com/TaeeunKil/kafrust/actions/runs/31746182158)
  passed Kafka 4.3.1 three-broker KRaft with three replicated partitions. The
  fresh external project ran for 120 seconds through a ten-second broker 1
  outage, reconciled 4,918,800 records, observed one operation error, seven
  failed requests, and 1,006 retries, and ended with `recovered=true` plus zero
  final queue gauges. This closes the published plaintext multi-broker soak
  gate; secured multi-broker soak, simultaneous broker loss, production SLO,
  and canary evidence remain open.
- Published `kafrust 0.2.28` secured multi-broker soak run
  [`31747389166`](https://github.com/TaeeunKil/kafrust/actions/runs/31747389166)
  passed Kafka 4.3.1 three-broker KRaft with SASL_SSL/SCRAM-SHA-256 and three
  replicated partitions. The fresh external `tls` project ran for 120 seconds
  through a ten-second broker 1 outage, reconciled 2,288,700 records, observed
  one failed request and 1,001 retries with zero high-level operation errors,
  and ended with `recovered=true` plus zero final queue gauges. This closes the
  published secured multi-broker soak gate; simultaneous broker loss,
  production SLO, and canary evidence remain open.
- Published `kafrust 0.2.28` simultaneous broker-loss soak run
  [`31748293446`](https://github.com/TaeeunKil/kafrust/actions/runs/31748293446)
  passed Kafka 4.3.1 three-broker KRaft with three replicated partitions. The
  fresh external project stopped brokers 1 and 2 simultaneously for ten
  seconds during a 120-second run, reconciled 4,423,200 records, observed one
  failed request and 999 retries with zero high-level operation errors, and
  ended with `recovered=true` plus zero final queue gauges. This closes the
  published plaintext simultaneous-loss gate; secured simultaneous loss,
  production SLO, and canary evidence remain open.
- The same published simultaneous-loss gate passed Kafka 3.7.2 in
  [`31748860976`](https://github.com/TaeeunKil/kafrust/actions/runs/31748860976).
  The fresh external `0.2.28` project processed 4,620,200 records across three
  replicated partitions, observed one failed request and 1,008 retries, and
  ended with `recovered=true` plus zero final queue gauges. The paired Kafka
  3.7.2 and 4.3.1 runs qualify the tested plaintext simultaneous-loss behavior;
  secured simultaneous loss, production SLO, and canary evidence remain open.
- The published secured simultaneous-loss gate then passed in
  [`31750274774`](https://github.com/TaeeunKil/kafrust/actions/runs/31750274774).
  A fresh external `0.2.28` project with `tls` survived simultaneous
  ten-second outages of brokers 1 and 2 in Kafka 4.3.1 SASL_SSL/SCRAM, using
  `Acks::All` and `min.insync.replicas=2`. It reconciled 2,704,200 successfully
  acknowledged records, recorded the expected write rejections while the
  cluster had only one in-sync broker, then recovered with zero in-flight and
  buffered records. Unclean-election data loss, production SLOs, and canary
  evidence remain separate gates.
- The same published secured simultaneous-loss gate passed Kafka 3.7.2 in
  [`31751812178`](https://github.com/TaeeunKil/kafrust/actions/runs/31751812178).
  The 60-second fresh external `0.2.28` project reconciled 686,700
  successfully acknowledged records, recorded the expected write rejections
  while two brokers were unavailable, and ended with `recovered=true` plus
  zero final in-flight and buffered records. Together the Kafka 3.7.2 and
  4.3.1 runs close the tested secured simultaneous-loss durability/availability
  gate; unclean-election data loss, production SLOs, and canary evidence remain
  separate.
- Shared metrics count non-zero Kafka error codes handled by authentication,
  producer, transaction, consumer, and consumer-group operations, including
  retry attempts and partial batch failures. This separates protocol-level
  broker failures from transport request failures without inspecting payload
  contents.

## M20 Compatibility Matrix And Migration Guide

Status: Complete.

Goal: make replacement decisions concrete for teams comparing kafrust with existing Kafka clients.

Scope:

- broker version matrix across Kafka 3.7, 3.8, 3.9, and current stable Kafka
- plaintext, TLS, SASL_PLAINTEXT, and SASL_SSL profiles
- single-node and multi-broker profiles
- producer, consumer, group, admin, compression, idempotence, and transaction checklists
- migration guide from `rust-rdkafka`
- comparison notes for pure Rust alternatives
- release qualification checklist

Exit criteria:

- compatibility claims are backed by dated workflow runs or documented manual checks
- migration docs show how to map common producer, consumer, group, and admin usage
- unsupported features are listed with alternatives or planned milestones
- release qualification requires docs.rs success, fresh published-crate compile, CI, and relevant live smoke profiles

Strategic role:

- This milestone turns kafrust from a project into an evaluable replacement candidate.

Evidence:

- Manual `Live Kafka Smoke` run `29989550933` passed the single-node plaintext
  producer, all-codec compression, direct consumer, and consumer group paths
  against Kafka 3.7.2 and current stable Kafka 4.3.1.
- The Kafka 4.3.1 run exposed the removal of Fetch v2 support; the high-level
  consumer path now uses Fetch v4, which is supported by both verified broker
  versions.
- `docs/migration-from-rust-rdkafka.md` maps typed configuration, producer,
  direct consumer, classic consumer group, transactions, and admin workflows;
  it also identifies blocking feature gaps and requires staged dual-client,
  failure-injection, performance, and canary qualification.
- Manual run `30062587935` passed the complete single-node plaintext path on
  Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1. The same run passed the secured
  Kafka 3.7.2 profiles and the three-broker broker-stop failover profile.
- `docs/project-strategy.md` records a dated comparison with krafka, rskafka,
  and kafka-rust while distinguishing self-reported feature claims from
  kafrust's own verified evidence.
- `docs/release.md` requires CI-equivalent checks, package dry runs,
  docs.rs verification, a fresh published-crate compile, a GitHub release,
  and the relevant live broker workflow.
- On current main commit `4c03b90`, the working-tree `0.2.8` package contents
  passed `cargo package` verification and all-feature documentation builds from
  their staged package directories. This is local package evidence only;
  post-change external docs.rs and fresh published-crate verification remain
  release gates.

## M21 Broad Kafka Client Replacement

Status: In progress.

Goal: make kafrust a credible pure Rust replacement for Kafka client dependencies in a broad set of Rust services.

Scope:

- stable 1.0 candidate API surface
- producer, consumer, group, admin, security, compression, idempotence, and transaction workflows
- compatibility matrix maintained across supported Kafka versions
- documented operational limits and performance baselines
- migration guide and release notes with semver discipline
- security review of credential handling and unsafe-free dependency posture
- deprecation and compatibility policy for future Kafka protocol growth

Exit criteria:

- every V1-00 through V1-26 milestone is `Done`, or is `Superseded` by a linked
  completed replacement that maps and satisfies every inherited exit criterion
  and evidence gate
- one accepted support contract classifies every public capability and names
  exact broker, topology, security, runtime, and workload profiles
- every stable operation has tested retry, ambiguity, timeout, cancellation,
  shutdown, and reconciliation behavior
- the same release candidate passes package, live, published, fault, resource,
  performance, migration-canary, rollback, API, MSRV, and security gates
- protocol-first `1.0.0` publication, docs.rs, fresh external projects, the
  complete supported matrix, and the post-publish service canary all pass on
  the exact tagged source

Non-goal:

- This milestone does not replace Apache Kafka brokers, controllers, storage, replication, or server-side group coordination.

Strategic role:

- This is the documented-profile replacement target for selected Kafka client
  dependencies in Rust applications, not a universal drop-in replacement.

Detailed execution:

| Phase | Milestones | Outcome |
| --- | --- | --- |
| Package and scope | V1-00-V1-02 | repair package identity, freeze the support contract, and classify every public surface |
| Core data plane and groups | V1-03-V1-10 | qualify protocol, delivery, idempotence, transactions, direct consumption, classic/KIP-848 groups, and Share |
| Admin and advanced surfaces | V1-11-V1-14 | qualify routed mutation safety and isolate or qualify expert protocols |
| Cross-cutting hardening | V1-15-V1-19 | complete ownership/shutdown, credentials, observability, resource/fuzz, and pure-Rust dependency gates |
| Operations and adoption | V1-20-V1-23 | run one published matrix, long faults, SLOs, migration canary, and rollback |
| Freeze and release | V1-24-V1-26 | freeze API, qualify a published RC, and publish/post-verify `1.0.0` |

The individual plans, dependencies, quantitative gates, and program exit
definition are in the
[v1.0 Milestone Program](milestones/v1.0/README.md). Milestone IDs are not crate
versions, and no percentage estimate can substitute for these binary gates.

Current planning blockers:

- repair the same-version packaged-client/protocol mismatch in V1-00
- reconcile crates.io `0.3.5` with GitHub releases ending at `v0.3.3`, and
  decide the unprotected-`main` release policy without inventing retroactive tags
- reconcile the proposed 3.7-through-current support target with the separate
  3.3/3.6/3.7/3.9/4.0/4.3 matrix in V1-01
- reconcile the actual twelve public modules and protocol re-export with the
  incomplete public API audit in V1-02
- extend the 76-key identity/version audit beyond the current 12-entry
  identity/version/flexible-version metadata snapshot to field-level and
  byte-level evidence for every high-level negotiated/fallback version
- choose and prove one coherent transaction protocol in V1-06: cap
  transactional Produce at v11 for legacy TV0/TV1, or implement the complete
  KIP-890 TV2 feature/epoch/implicit-add/TxnOffsetCommit v5/EndTxn v5 flow
- split Share acknowledgement response-loss evidence into broker-unapplied
  redelivery, broker-applied no-redelivery, and persistently unknown outcomes
- obtain one named service-canary owner/environment before V1-25

Evidence history (mixed current-source and published tiers):

- Producer, direct consumer, classic/KIP-848 consumer-group, share-group,
  and Streams-group builders now accept a shared `ClientConfig` through
  `with_client_config(...)`. This keeps bootstrap rotation, controller
  bootstrap servers, security, decode limits, and metrics policy consistent
  across clients while retaining per-builder setters for local overrides.
  The same builders now expose `build_config()` for connection-free validation
  and return the validated configuration for later async construction;
  `AdminClient` exposes the same preflight after construction. The
  public-surface integration test locks these common client contracts.

- The optional `blocking` feature now provides synchronous
  `BlockingProducer`, `BlockingBufferedProducer`,
  `BlockingBufferedProducerHandle`, `BlockingConsumer`, `BlockingConsumerGroup`,
  `BlockingShareConsumer`, `BlockingStreamsGroupSession`, and
  `BlockingAdminClient` adapters for direct and bounded-buffered producer,
  manually assigned direct-consumer, joined group poll/heartbeat/commit, Share
  poll/acknowledge/commit, Streams heartbeat/task-state/close, cluster/topic,
  ACL/security, quotas, storage, reassignments, transaction diagnostics,
  group listing, group-offset, feature, topic-configuration, and topic
  lifecycle operations. Each owns a dedicated multi-thread Tokio runtime,
  preserves the async client's retry/compression/idempotence/transaction
  behavior where applicable, and rejects construction or use from inside
  another Tokio runtime instead of panicking. A general runtime-abstraction
  trait remains open.

- TLS mutual authentication is now available through DER-encoded client
  certificate-chain and private-key settings on the shared `ClientConfig`,
  with matching producer, direct consumer, classic group, ShareConsumer, and
  Streams-group forwarding methods. Configuration validation rejects an
  incomplete certificate/key pair, empty material, or plaintext use. The
  custom `ClientConfig` debug implementation reports counts and redacts the
  private key rather than formatting credential bytes. Focused all-feature
  tests pass. The `live-mtls.yml` workflow now generates a short-lived CA,
  requires client authentication on Kafka 3.7.2 or 4.3.1, verifies the
  handshake independently, and runs Admin, producer, direct-consumer,
  consumer-group, transactional/read-committed, low-level, and coordinator
  roundtrips. Kafka 3.7.2 passed in
  [`32343983601`](https://github.com/TaeeunKil/kafrust/actions/runs/32343983601)
  and Kafka 4.3.1 passed in
  [`32343983397`](https://github.com/TaeeunKil/kafrust/actions/runs/32343983397).
  Published `kafrust 0.3.1` then passed the same mTLS workflow from fresh
  external Cargo projects on Kafka 3.7.2 in
  [`32344673371`](https://github.com/TaeeunKil/kafrust/actions/runs/32344673371)
  and Kafka 4.3.1 in
  [`32344673373`](https://github.com/TaeeunKil/kafrust/actions/runs/32344673373),
  covering Admin, producer, direct consumer, consumer group, and
  transactional/read-committed paths. Certificate rotation behavior remains a
  separate security gate.

- The test suite now includes a reusable in-process scripted broker harness
  under `crates/kafrust/tests/support/`. It records API key, version,
  correlation ID, and request frames, and can deterministically drop or answer
  connections while keeping multi-request sessions open. Regression gates now
  verify Admin metadata reconnect, idempotent Produce retry, transactional
  EndTxn response loss, and direct-consumer Fetch recovery. The Produce path
  preserves the original batch frame and accepts Kafka's duplicate sequence
  response without replaying a new sequence; the transaction path returns an
  unknown outcome and defuncts the producer rather than claiming a commit; the
  consumer path recovers one record after metadata and Fetch re-discovery. This
  closes the absence of any reusable fault-injection baseline, but not the
  broader producer, consumer, group, transaction, security, and long-soak
  fault matrices required by M21.

- The local Admin mutation harness now also covers RenewDelegationToken and
  ExpireDelegationToken v2: it verifies the compact HMAC request reaches the
  controller, drops the response, returns the matching
  `AdminMutationOutcomeUnknown` without replaying either mutation. This is
  deterministic regression evidence only; authenticated Kafka 3.7.2
  and 4.3.1 live authorization, renewal, expiry, and reconciliation gates
  remain open.

- `ProducerConfig::delivery_timeout_ms` now provides a Kafka-style total
  deadline for immediate and batch delivery, covering metadata lookup,
  capability negotiation, Produce requests, retry attempts, and backoff. The
  default is 120 seconds. A deadline expiry returns the typed request-timeout
  error, poisons the producer's current metadata connection, clears stale
  routing hints, and never returns a possibly interrupted leader connection to
  the idle cache. Buffered records now carry their enqueue timestamp, expire
  before Produce when the deadline is reached, and use the remaining deadline
  during flush. The local delayed-response and queue-expiry regressions pass;
  `linger_ms` remains the independent batching-latency control.

- Flexible capability discovery now includes explicit `ApiVersions` v4 and v5
  request types. The opt-in `Client::api_versions_cached` helper prefers v4 and
  retries with v3 on Kafka's `UNSUPPORTED_VERSION` response; v5 encodes the
  optional cluster ID and node ID checks without changing the existing v3/v4
  response decoder. Protocol and injected-broker regression tests cover both
  request shapes and the fallback sequence. Live v4 qualification remains a
  release evidence follow-up before changing established higher-level paths.

- Modern `ListGroups` API versions 4 and 5 are now covered by typed flexible
  protocol messages and low-level `Client` methods. The new
  `AdminClient::list_groups_with_options` negotiates the highest supported
  version, sends state/type filters without loss, and preserves group state,
  group type, throttle time, coordinator ID, and selected API version in
  `GroupListing`. The low-level v1 method remains available for older callers.
  Protocol bytes and an injected Kafka-style v5 negotiation roundtrip pass.
  The live smoke gate now asserts v4 negotiation on Kafka 3.7.2 and v5
  state/type-filter negotiation on Kafka 4.3.1. Both assertions passed in the
  complete current-source matrix [`32382586220`](https://github.com/TaeeunKil/kafrust/actions/runs/32382586220),
  closing this live version-negotiation evidence slice; authorization and
  long-duration coordinator-churn behavior remain separate gates.

- Kafka API key 74 now preserves the Kafka version split: v0
  `ListClientMetricsResources` compatibility for Kafka 3.9-era brokers and v1
  `ListConfigResources` for Kafka 4.1+. The typed protocol, low-level `Client`,
  and `AdminClient::list_config_resources` select v0 only for an exact
  client-metrics filter and retain typed resource kinds for v1. Protocol,
  Client, and injected Admin fallback tests pass. The Kafka 4.3.1 live v1
  qualification passed in [`32342304005`](https://github.com/TaeeunKil/kafrust/actions/runs/32342304005),
  including the real Admin roundtrip and the opt-in DescribeConfigs v4 path.
  The published `0.3.3` artifact passed the Kafka 4.3.1 v1 branch in
  [`32382623298`](https://github.com/TaeeunKil/kafrust/actions/runs/32382623298),
  including fresh external-project resolution and lockfile verification. The
  Kafka 3.9.1 v0 branch remains supported by the earlier published `0.3.1`
  result. The manual
  `.github/workflows/live-list-config-resources.yml` workflow checks the v1
  capability and a real Admin roundtrip on Kafka 4.1.0, 4.2.0, or 4.3.1.
  The Kafka 3.9.1 v0 client-metrics filter path also passed in
  [`32342680037`](https://github.com/TaeeunKil/kafrust/actions/runs/32342680037);
  published `kafrust 0.3.1` external projects passed both branches in
  [`32343145837`](https://github.com/TaeeunKil/kafrust/actions/runs/32343145837)
  and [`32343030081`](https://github.com/TaeeunKil/kafrust/actions/runs/32343030081).

- Kafka `DescribeCluster` API 60 v0/v1 is now implemented through the typed
  protocol, low-level `Client`, and an opt-in `AdminClient` path. The result
  preserves cluster ID, endpoint type, broker rack, and cluster authorized
  operations, with Metadata fallback when API 60 is absent. Protocol,
  injected-client, and Admin capability-routing tests pass. The existing
  Metadata-based method remains unchanged for compatibility. The published
  `kafrust 0.3.3` external gate passed the broker-bootstrap path on Kafka 3.7.2
  and 4.3.1 in [`32400851719`](https://github.com/TaeeunKil/kafrust/actions/runs/32400851719)
  and [`32400851830`](https://github.com/TaeeunKil/kafrust/actions/runs/32400851830),
  including crates.io lockfile verification and API 60 cluster/authorized-
  operation checks. The published `kafrust 0.3.4` gate then qualified both the
  broker and explicit controller endpoint sets on Kafka 3.7.2 and 4.3.1 in
  [`32403253526`](https://github.com/TaeeunKil/kafrust/actions/runs/32403253526)
  and [`32403253688`](https://github.com/TaeeunKil/kafrust/actions/runs/32403253688).
  `DescribeClusterEndpointType::Controllers` requires
  `ClientConfig::controller_bootstrap_servers`; broader Admin version,
  security, and failure matrices remain separate 1.0 gates.

- KRaft `AddRaftVoter` API 80 v0/v1 and `RemoveRaftVoter` API 81 v0 are now
  implemented through typed flexible protocol messages, low-level Client
  methods, and controller-routed Admin methods. AddRaftVoter v1 exposes
  committed-acknowledgement semantics and rejects a v0 downgrade when that
  semantic is requested. Focused wire tests, injected Client tests, and
  injected controller-routing tests pass. The new
  `admin_dynamic_quorum` example and
  `.github/workflows/live-dynamic-quorum.yml` provision a Kafka 4.3.1
  standalone/dynamic controller pair and exercise Add/RemoveRaftVoter with
  DescribeQuorum convergence checks. The live qualification passed in
  [`32383742320`](https://github.com/TaeeunKil/kafrust/actions/runs/32383742320),
  recording `voters=1 observers=1` before the mutation, `voters=2
  observers=0` after AddRaftVoter, and `voters=1 observers=1` after
  RemoveRaftVoter. The follow-up
  `.github/workflows/live-dynamic-quorum-authorization.yml` gate passed in
  [`32364161150`](https://github.com/TaeeunKil/kafrust/actions/runs/32364161150):
  a SASL/PLAIN principal with only cluster `Describe` received
  `ClusterAuthorizationFailed` (31) and did not change quorum membership,
  while `User:admin` completed the Add/Remove lifecycle. Broader controller
  failure workloads remain a separate M21 gate.

- DescribeConfigs v4 is now available as an opt-in documentation-aware path.
  `DescribeConfigsOptions::include_documentation(true)` negotiates API 32 v4,
  preserves configuration type and documentation fields, and retains v1 as
  the default compatibility path. Protocol, capability, and Admin injected
  broker tests pass. The manual `live-list-config-resources.yml` workflow now
  creates a real topic and qualifies both ListConfigResources v1 and
  DescribeConfigs v4 on Kafka 4.1+. The Kafka 4.3.1 current-source gate passed
  in [`32342304005`](https://github.com/TaeeunKil/kafrust/actions/runs/32342304005);
  the published `0.3.1` external project preserved v4 configuration metadata in
  [`32343030081`](https://github.com/TaeeunKil/kafrust/actions/runs/32343030081).

- Stable KIP-932 v1 wire types now cover `ShareGroupHeartbeat` (API key 76),
  `ShareFetch` (API key 78), and `ShareAcknowledge` (API key 79), including
  flexible headers, share-session epochs, acknowledgement batches, acquired
  record ranges, current-leader metadata, node endpoints, and nullable record
  bytes. The low-level `Client` and the high-level `ShareConsumer` now expose
  the corresponding lifecycle: metadata discovery, leader-grouped fetch,
  bounded record-batch decoding, explicit or implicit acknowledgement state,
  grouped acknowledgement commits, session closure, and group leave. Focused
  protocol tests plus an injected-broker Metadata v12 -> ShareFetch v1 ->
  ShareAcknowledge v1 roundtrip pass, including UUID, offset, and
  acknowledgement-type assertions. An opt-in detached heartbeat task now owns
  a dedicated coordinator connection, supports bounded reconnect attempts, and
  cancels an in-flight request during shutdown in a focused test. Foreground
  heartbeat failures now rediscover the group coordinator instead of reconnecting
  only to a stale address. The public fault-injection harness now exercises the
  `ShareConsumerConfig::build() -> poll() -> acknowledge() -> commit()` path
  with a dropped ShareAcknowledge response, proving typed unknown-outcome
  classification, no automatic replay, redelivery after session reset, and a
  successful replacement acknowledgement.
  Lost ShareAcknowledge responses are classified as a typed unknown outcome and
  are never replayed automatically. The Kafka 4.3.1
  single-node live gate passed the complete poll/Renew/poll,
  acquisition-lock expiry/redelivery, Accept/commit, and close path in
  [`32213499877`](https://github.com/TaeeunKil/kafrust/actions/runs/32213499877).
  One three-broker leader-movement path, three independent active-heartbeat
  coordinator recovery attempts, and three consecutive in-process coordinator
  churn cycles are now live-qualified. Lost ShareAcknowledge responses are
   tracked as typed unknown outcomes and the public
   `reconcile_acknowledgement_outcomes` path discards the affected sessions
   without replay; a subsequent poll accepts only broker-redelivered records.
   The reconciliation path now permits that subsequent poll while continuing
   to block `commit()` until redelivery clears the unknown state. Focused
   regression coverage passes. The dedicated
   `.github/workflows/share-kafka-acknowledgement-ambiguity.yml` gate now drops
   the first `ShareAcknowledge` response for a `Release` and verifies
   redelivery plus replacement `Accept`; Kafka 4.3.1 passed this gate in
    [`32347035522`](https://github.com/TaeeunKil/kafrust/actions/runs/32347035522).
    The ordinary 64-record acknowledgement soak and the live response-loss
    reconciliation gate also passed in
    [`32355746726`](https://github.com/TaeeunKil/kafrust/actions/runs/32355746726)
    and [`32355746798`](https://github.com/TaeeunKil/kafrust/actions/runs/32355746798).
    Long-running ambiguous reconciliation, multi-broker ownership, and
    resource/backpressure measurements remain open.
  `close()` now skips unknown acknowledgements without replaying them, completes
  known shutdown releases, closes share sessions, leaves the group, and returns
  the unknown-outcome error only after cleanup. A focused regression test keeps
  this shutdown safety contract explicit; long-running ambiguous reconciliation
  remains open.
  KIP-1206 ShareFetch v2 is now negotiated when advertised: the high-level
  consumer exposes `BatchOptimized` (the backward-compatible default) and
  `RecordLimit`, which fails on brokers that cannot provide v2 rather than
  silently weakening the configured delivery limit. KIP-1222 `Renew` is now
  wired through ShareAcknowledge v2, retains renewed records for later
  completion, exposes the broker acquisition-lock timeout, and replaces a
  retained record when its acquisition lock expires and Kafka redelivers the
  same offset. The single-node Kafka 4.3.1 v2/renewal and expiry/redelivery
  path passed in the live run above; multi-broker and long-running
  reconciliation remain open.
  KIP-714 client telemetry now has low-level v0 request/response types plus a
  high-level `TelemetryClient` with an owned provider trait, capability
  negotiation, subscription state, payload ceilings, same-connection refresh
  and retry, broker-negotiated pure-Rust gzip/Snappy/LZ4/Zstd compression,
  jittered scheduling, and a terminating shutdown push. The optional `otlp`
  feature now provides `ClientMetricsTelemetryProvider`, mapping shared client
  counters and gauges to filtered cumulative or delta OTLP MetricsData bytes.
  The Kafka 3.7.2 KRaft broker plugin qualification passed in
  [`32229640441`](https://github.com/TaeeunKil/kafrust/actions/runs/32229640441),
  including ordinary and terminating payload delivery. The same workflow now
  passes active subscription mutation and recovery in
  [`32236749392`](https://github.com/TaeeunKil/kafrust/actions/runs/32236749392):
  the client honors response throttle windows and applies the existing push
  interval as a Kafka 3.7.2 compatibility cooldown when the broker returns a
  zero-throttle quota error during refresh. The dedicated payload-limit
  workflow passed in
  [`32237664774`](https://github.com/TaeeunKil/kafrust/actions/runs/32237664774),
  proving that an advertised 128-byte ceiling produces a typed pre-send
  rejection rather than a truncated or malformed OTLP payload. Longer
  collection and secured or multi-broker telemetry remain open hardening
  gates.
  Kafka 4.0 early-access v0 is intentionally excluded because the stable schemas
  removed it in Kafka 4.1.
- `.github/workflows/share-kafka-smoke.yml` now provides a dedicated Kafka 4.3.1
  live gate with share-state replication settings, renewal enabled, a produced
  smoke record, and the high-level poll/Renew/poll/expiry-redelivery/Accept/
  commit/close path.
  workflow run [`32213499877`](https://github.com/TaeeunKil/kafrust/actions/runs/32213499877)
  verifies the single-node ShareConsumer path.
- `.github/workflows/share-kafka-multi-broker-smoke.yml` now provides the
  three-broker Share gate: it selects a partition led by broker 1,
  consumes and accepts a pre-failover record, stops broker 1, waits for leader
  movement, and verifies a fresh ShareConsumer can consume and accept a
  post-failover record from the surviving brokers. Run
  [`32214201983`](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983)
  passed this path on Kafka 4.3.1; repeated failures and long-running soak
  remain open.
- `.github/workflows/share-kafka-heartbeat-failover.yml` now provides the
  active-heartbeat gate: it stops the discovered group coordinator while the
  detached heartbeat task is running, waits for partition leader movement, and
  verifies post-failover delivery, acknowledgement, and clean shutdown. Kafka
  4.3.1 passed the original path in
  [`32215845737`](https://github.com/TaeeunKil/kafrust/actions/runs/32215845737)
  and all three independent matrix attempts in
  [`32216383214`](https://github.com/TaeeunKil/kafrust/actions/runs/32216383214).
  The workflow now also passes three consecutive coordinator-loss/recovery
  cycles inside one ShareConsumer process in all three matrix attempts in
  [`32219147942`](https://github.com/TaeeunKil/kafrust/actions/runs/32219147942).
- The Share failover gates were revalidated on current source commit `35e7cec`:
  the three-broker leader-loss path passed in
  [`32356279940`](https://github.com/TaeeunKil/kafrust/actions/runs/32356279940),
  and all three active-heartbeat coordinator-loss attempts passed in
  [`32356280155`](https://github.com/TaeeunKil/kafrust/actions/runs/32356280155).
  This strengthens current-source evidence but does not close long-running
  multi-broker ownership, assignment/rebalance, or resource/backpressure gates.
- The current-source Share acknowledgement soak passed on Kafka 4.3.1 in
  [`32369562416`](https://github.com/TaeeunKil/kafrust/actions/runs/32369562416):
  64 independently seeded records were acquired one at a time, acknowledged
  and committed individually, checked for unique values and offsets, and the
  ShareConsumer closed cleanly. The published `kafrust 0.3.3` artifact then
  passed the same 64-record flow from a fresh external project, including
  heartbeat shutdown, close, and lockfile verification, in
  [`32385522647`](https://github.com/TaeeunKil/kafrust/actions/runs/32385522647).
  This closes bounded current-source and single-node published-artifact
  acknowledgement-progress gates. The published `0.3.3` three-broker
  leader-failover path also passed in
  [`32386637555`](https://github.com/TaeeunKil/kafrust/actions/runs/32386637555),
  covering pre-failover acceptance, broker 1 leader loss, replacement
  leadership, and post-failover acceptance from surviving bootstrap servers.
  The published active-heartbeat workflow then passed three consecutive
  coordinator-loss cycles, with dynamic coordinator stops and heartbeat-task
  liveness checks, in
  [`32387564503`](https://github.com/TaeeunKil/kafrust/actions/runs/32387564503).
  A bounded two-member published ownership/assignment gate then passed in
  [`32388813780`](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780).
  The published `kafrust 0.3.4` workflow then sustained two-member ownership
  for 300 seconds over six replicated partitions and 60 records in
  [`32404294014`](https://github.com/TaeeunKil/kafrust/actions/runs/32404294014).
  Each member retained three partitions, accepted and consumed 30 records,
  closed with `in_flight=0` and zero failed requests, and the workflow verified
  exact per-partition counts plus unique partition/offset pairs. This closes
  the bounded published long-running ownership slice; dynamic member-loss,
  resource/backpressure SLO, and production readiness remain open.
  The published `kafrust 0.3.4` repeated-loss workflow then completed four
  forced member-loss/rejoin cycles in
  [`32405501232`](https://github.com/TaeeunKil/kafrust/actions/runs/32405501232).
  Ownership alternated member 1 → member 2 → member 1 → member 2; all 24
  partition/offset pairs were unique with four records per partition, and the
  final survivor owned all six partitions with `accepted=6`, `consumed=6`,
  `in_flight=0`, and zero failed requests. Higher-cycle churn,
  resource/backpressure SLO, and production readiness remain open.
  The workflow is now configured for eight alternating member-loss/rejoin
  cycles and 48 unique partition/offset observations on the next run; this is
  configuration for future evidence, not a claim that the new run has passed.
  A separate published secure multi-member workflow is also configured for
  Kafka 4.3.1 SASL_SSL/SCRAM-SHA-256 with 64 records per partition by default,
  exact per-partition and duplicate checks, and the same resource-gauge
  checks; its first live run remains required before counting secure Share
  evidence.
  The published Share member example now also reports `buffered=0` plus peak
  in-flight and buffered gauges, and both the multi-member and repeated-loss
  workflows fail if those final resource gauges are non-zero. This makes the
  next resource/backpressure SLO gate observable without treating a single
  workflow's peak as a universal production limit.
- ShareFetch success responses preserve the broker that served the request,
  while `CurrentLeader` is used only for the leader-error responses where Kafka
  populates it. Retryable ShareFetch leader errors return the connection to the
  pool, refresh metadata, and retry with refreshed routing. Injected tests cover
  the response semantics; the three-broker leader movement workflow passed in
  run [`32214201983`](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983),
  while long-running acknowledgement reconciliation remains open. The
  `ShareConsumer::reconcile_acknowledgement_outcomes` API now discards the
  affected broker session after an ambiguous response and lets a later poll
  observe redelivery without replaying the original acknowledgement. The live
  runs exposed and fixed stale broker-connection reuse,
  partition fetches split across replacement leaders, and stale coordinator
  connections during group leave. Bootstrap reconnects now rotate across
  configured addresses when a dead broker resets requests after TCP connect.
- ConsumerGroupDescribe API key 69 is now implemented through flexible v0/v1
  protocol types, low-level Client methods, and the high-level
  `AdminClient::describe_consumer_groups_modern` path. It preserves group and
  assignment epochs, member type, topic UUID/name assignments, authorized
  operations, and broker error messages. An injected coordinator test covers
  ApiVersions negotiation and the v1 response mapping. The existing
  `admin_consumer_group_offsets_member` Kafka 4.3.1 KIP-848 workflow now calls
  this API while a real member is joined and verifies that the returned member
  set contains that member. The published `kafrust 0.3.4` gate now qualifies
  the same public read path from a fresh external project on Kafka 4.3.1 in
  [`32408765709`](https://github.com/TaeeunKil/kafrust/actions/runs/32408765709):
  it observed `state=Stable`, group/assignment epochs `2/2`, `member_type=1`,
  `member_epoch=2`, and current/target assignment of topic partition 0.
  Kafka 3.7.2 remains outside this gate because API 69 is advertised from Kafka
  3.8 onward; security and multi-member qualification remain open.
- ApiVersions v3 feature tags are now decoded into typed supported-feature and
  finalized-feature metadata while unknown tags remain preserved. The new
  `AdminClient::describe_features` read path exposes broker capability ranges,
  the finalized-feature epoch, and the ZooKeeper-migration-ready flag.
  Focused protocol and Admin conversion tests pass, and the existing broker
  roundtrip smoke now exercises both the low-level v3 response and high-level
  Admin method. The published `kafrust 0.3.4` external gate now qualifies this
  path on Kafka 3.7.2 and 4.3.1 in
  [`32406914244`](https://github.com/TaeeunKil/kafrust/actions/runs/32406914244)
  and [`32406914237`](https://github.com/TaeeunKil/kafrust/actions/runs/32406914237),
  including the observed supported/finalized counts and finalized epochs.
  Feature mutation, security, and broader version matrices remain open.
- KRaft `UpdateFeatures` v0/v1 is now implemented through a flexible typed wire
  path, low-level `Client::update_features_v0/v1`, and controller-aware
  `AdminClient::update_features`. The Admin path prefers v1 and falls back to
  v0 only when validation-only and unsafe-downgrade semantics are not needed.
  Top-level and per-feature outcomes are preserved, and transmitted-request
  failures use the existing `AdminMutationOutcomeUnknown` contract instead of
  replaying a feature change. Focused protocol and injected-client coverage is
  in place. The live authorization matrix
  [`32362301496`](https://github.com/TaeeunKil/kafrust/actions/runs/32362301496)
  now qualifies restricted-user rejection and administrator success on Kafka
  3.7.2 and 4.3.1. The three-broker validation failover matrix
  [`32363072430`](https://github.com/TaeeunKil/kafrust/actions/runs/32363072430)
  also passes after replacing the active controller; actual downgrade and
  state-changing mutation during controller failover remain open. Kafka 4.3.1
  `transaction.version` `2 -> 1 -> 2` state transitions are verified in
  [`32363428806`](https://github.com/TaeeunKil/kafrust/actions/runs/32363428806);
  metadata-version transitions across the declared broker matrix remain open.
- Kafka `UnregisterBroker` API 64 v0 is now implemented through a flexible
  typed protocol path, low-level `Client::unregister_broker_v0`, and the
  controller-routed `AdminClient::unregister_broker` method. The result keeps
  throttle and broker error metadata typed, and a transmitted-request failure
  uses `AdminMutationOutcomeUnknown` rather than replaying the unregister
  mutation. Protocol, injected Client, and injected controller-routing tests
  pass. The live multi-controller operational path is qualified for Kafka
  3.7.2, 3.8.1, 3.9.1, and 4.3.1 in the four-job matrix
  [`32359316032`](https://github.com/TaeeunKil/kafrust/actions/runs/32359316032):
  each three-node KRaft cluster stopped broker 1, unregistered it through the
  surviving controller quorum, restarted the same node, and verified
  re-registration plus quorum health.
- The reusable current-source Admin response-drop gate now includes API 64:
  the first `UnregisterBroker` response is dropped, the client returns
  `AdminMutationOutcomeUnknown` without replay, and `DescribeCluster` observes
  broker 1 absent. This closes transport-ambiguity evidence only; the
  multi-controller gate is recorded separately below. Kafka 3.7.2
  and 4.3.1 passed in [`32357381909`](https://github.com/TaeeunKil/kafrust/actions/runs/32357381909)
  and [`32357381879`](https://github.com/TaeeunKil/kafrust/actions/runs/32357381879).
- The `live-unregister-broker-rejoin.yml` gate now qualifies the remaining
  operational proof for Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 in
  [`32359316032`](https://github.com/TaeeunKil/kafrust/actions/runs/32359316032).
  The follow-up `live-unregister-broker-authorization.yml` gate passed Kafka
  3.7.2 and 4.3.1 in [`32360499520`](https://github.com/TaeeunKil/kafrust/actions/runs/32360499520):
  a SASL/PLAIN principal with only cluster discovery permission received
  `ClusterAuthorizationFailed` (error code 31), while the configured
  administrator principal was allowed to complete the mutation. This closes
  the operation-specific authorization proof; production ACL policy and
  workload-specific failure behavior remain separate gates.
- Kafka Share Group State APIs 83-87 are now implemented through flexible
  typed protocol requests and responses, low-level Client methods, and typed
  coordinator-routed Admin methods. Share-group membership/admin operations
  use the ordinary Group coordinator; durable state uses KIP-932
  FindCoordinator v6 with a `group:topic-id:partition` key whose topic-id
  segment uses Kafka's URL-safe Base64-without-padding UUID representation.
  WriteShareGroupState v1 and
  ReadShareGroupStateSummary v1 preserve `delivery_complete_count`; requests
  that need those fields reject a v0-only broker rather than silently losing
  data. Kafka currently marks these wire APIs unstable, so they are tracked as
  an advanced protocol qualification rather than a required public client
  surface. Initialize, Write, and Delete preserve the existing ambiguous-mutation
  contract. Protocol and local capability/routing coverage pass, and the live
  Share smoke workflow now covers metadata UUID discovery, initialize, v1
  write, full read, v1 summary, and delete. Multi-topic and multi-partition
  Admin requests now split by the per-resource v6 coordinator and merge
  partition-level results; injected tests cover two coordinators across all
  five state APIs. The current replicated gate passed in
  [`32398034582`](https://github.com/TaeeunKil/kafrust/actions/runs/32398034582):
  it verified the replicated `__share_group_state` topic, discovered the
  written Share coordinator, stopped it, observed reassignment, and completed
  post-failover read, summary, and delete. The new
  `.github/workflows/share-kafka-state-failover.yml` workflow adds the
  replicated-state gate: it verifies the internal `__share_group_state` topic
  is fully replicated, discovers the written share group's coordinator,
  stops that coordinator, waits for coordinator reassignment, then reads the
  full state and summary and deletes it through the surviving brokers. The
  first post-routing dispatch exposed the earlier v1/type-only lookup as
  Kafka `INVALID_REQUEST` (run
  [`32348148841`](https://github.com/TaeeunKil/kafrust/actions/runs/32348148841));
  the v6 per-partition routing fix and the follow-up workflow corrections are
  recorded in the subsequent commits and successful run above. This gate does
   not claim general ShareConsumer or `rust-rdkafka` replacement compatibility.
   The published `kafrust 0.3.3` version of this gate also passed in
   [`32399284180`](https://github.com/TaeeunKil/kafrust/actions/runs/32399284180):
   a fresh external Cargo project resolved both published packages, verified
   the replicated state topic, survived Share coordinator loss, completed
   post-failover read/summary/delete, and checked the generated lockfile.
   This closes the published-artifact evidence for the tested unstable state
   path; it does not close general ShareConsumer replacement, long-running SLO,
   or broader Kafka-version/security evidence.
- `.github/workflows/live-update-features.yml` now provides a broker matrix
  gate for the negotiated path. The earlier empty v1 `validate_only` requests
  passed on Kafka 3.7.2 and 4.3.1 in
  [`32346412517`](https://github.com/TaeeunKil/kafrust/actions/runs/32346412517)
  and [`32346412771`](https://github.com/TaeeunKil/kafrust/actions/runs/32346412771).
  The workflow now also sends a non-empty `metadata.version` v1
  `validate_only` request, derives the current finalized level from
  `DescribeFeatures`, and checks the per-feature result. Kafka 3.7.2 passed in
  [`32360936437`](https://github.com/TaeeunKil/kafrust/actions/runs/32360936437)
  and Kafka 4.3.1 passed in
  [`32361035007`](https://github.com/TaeeunKil/kafrust/actions/runs/32361035007).
  The workflow remains non-mutating at the feature level; v0 fallback remains
  covered by typed and injected-client tests. The separate authorizer matrix
  [`32362301496`](https://github.com/TaeeunKil/kafrust/actions/runs/32362301496)
  also passes for Kafka 3.7.2 and 4.3.1, with restricted-user rejection and
  administrator success. The three-broker controller-failover matrix
  [`32363072430`](https://github.com/TaeeunKil/kafrust/actions/runs/32363072430)
  passes the same v1 validation after leader replacement. Actual
  metadata-version upgrade/downgrade and state-changing mutation during
  controller failover remain separate gates. Kafka 4.3.1 transaction feature
  transitions are covered separately by
  [`32363428806`](https://github.com/TaeeunKil/kafrust/actions/runs/32363428806).
- The 2026-08-20 competitor recheck adds `kacrab` to the comparison set. Its
  published `0.4.0` docs claim Kafka 4.3 producer, consumer, share-consumer,
  and 62-operation Admin parity with a broker-matrix and fuzzing posture;
  published `krafka` docs.rs currently resolve to `0.19.0`, while its current
  source tree claims Kafka 4.3 parity, 2,350+ tests, six fuzz targets, and an
  in-process fault-injecting broker; `krafka` remains ahead in modern protocol
  breadth and test infrastructure.
  Kafrust's differentiator remains a Kafka 3.7-to-current compatibility target
  with a pure-Rust default codec and no librdkafka dependency, but that is not
  a substitute for the competitors' missing live and long-duration evidence.
  The source-level inspection and remaining-gate matrix are recorded in
  [`docs/competitor-source-audit-2026-08-20.md`](competitor-source-audit-2026-08-20.md).
- Flexible `ApiVersions v3` request and response types report broker API
  version ranges, preserve unknown top-level tagged fields, and share a common
  capability lookup with the legacy v0 response. The high-level producer now
  uses this negotiation path while retaining the v0 low-level method for
  compatibility. Live Kafka Smoke run
  [`31494820868`](https://github.com/TaeeunKil/kafrust/actions/runs/31494820868)
  passed all 11 plaintext, secured, multi-broker, ACL, and KIP-848 jobs on
  2026-08-11.
- Producer leader sends reuse an authenticated broker `Client` and its cached
  ApiVersions v3 response for sequential sends to the same broker address.
  A focused injected-broker test proves one capability handshake followed by
  two Produce requests on one socket; the existing ambiguous transport test
  proves failed connections are discarded before retry. Full live smoke rerun
  [`31496965137`](https://github.com/TaeeunKil/kafrust/actions/runs/31496965137)
  passed all 11 broker, security, ACL, KIP-848, and multi-broker failover jobs
  after this change.
- Direct consumer fetch and watermark paths reuse a successful partition-leader
  `Client` by broker address and evict it on request failure. A focused
  injected-broker test verifies two sequential Fetch requests on one socket.
- Producer and direct-consumer idle broker connections use bounded FIFO caches
  controlled by `ClientConfig::max_idle_broker_connections` (default 64).
  Requests remove a connection while it is in flight and return it only after
  success; the oldest idle entry is evicted when the bound is reached. This
  prevents unbounded broker-address growth while preserving the existing
  poisoned-connection rule. Producer instances built from cloned
  `ClientConfig` values share the producer cache. Direct consumers retain an
  instance-local cache because Kafka Fetch session state belongs to one
  consumer instance. Admin, group, and Share clients still have separate
  connection-lifecycle paths and remain a 1.0 gap.
- Producer capability negotiation now prefers topic-ID Produce v13 when the
  broker advertises it and Metadata v12 returns a topic UUID, then falls back
  to name-based flexible Produce v12, v11, and v9 for RecordBatch sends,
  including transactional and no-ack paths. Focused request/response fixtures
  and producer selection tests cover the topic-ID path and UUID-unavailable
  fallback. The live matrix now requires v13 on Kafka 4.3.1 and retains the
  v12/v11/v9 compatibility gate on older brokers. The complete 17-job live
  matrix [`31648660947`](https://github.com/TaeeunKil/kafrust/actions/runs/31648660947)
  passed at commit `1a844d8`: Kafka 4.3.1 selected v13, Kafka 3.8.1 and 3.9.1
  selected v11, and Kafka 3.7.2 selected v9.
- `AdminClient::list_transactions` now queries every metadata broker, uses
  ListTransactions v1 when advertised, falls back to v0, and aggregates
  broker-local transaction-state shards. Focused protocol and injected-broker
  tests pass, and the complete 17-job live matrix passed the listing example
  in [`31648660947`](https://github.com/TaeeunKil/kafrust/actions/runs/31648660947).
- Request-level observability records structured terminal fields for successful,
  failed, fire-and-forget, and cancelled broker requests without recording
  request payloads or credential material. The span-lifecycle guard is covered
  by the full workspace validation.
- Rack-aware direct and group fetches now expose `client_rack` through their
  builders. Connections prefer flexible Fetch v12 through ApiVersions, encode
  the compact/tagged rack-aware request, decode `preferred_read_replica`, and
  route the next partition fetch to the selected broker. Fetch v11 and Fetch v4
  remain compatibility fallbacks. Focused protocol fixtures cover both Fetch
  v11 and v12 wire fields, and an injected two-broker test verifies
  leader-to-preferred-replica routing plus fallback when the preference clears.
  The Kafka 3.7.2 three-broker `broker.rack` plus `RackAwareReplicaSelector`
  profile passed live qualification in
  [`31640494509`](https://github.com/TaeeunKil/kafrust/actions/runs/31640494509),
  including live Fetch v12 requests and preferred-replica routing.
- Direct and group Fetch v11/v12 now track the broker-scoped fetch session ID
  and epoch across sequential polls. Focused tests cover session creation,
  epoch advancement, retry classification for `INVALID_FETCH_SESSION_EPOCH`,
  and v4 fallback when a broker advertises neither session-capable version.
  Session state is explicitly discarded on assignment or position changes,
  reconnects, and fetch errors; the v4 fallback remains outside this claim.
  The complete 17-job matrix, including the Kafka 3.7.2 three-broker
  rack-aware follow-up request, passed in
  [`31671783977`](https://github.com/TaeeunKil/kafrust/actions/runs/31671783977).
- Classic consumer-group JoinGroup retries transient coordinator and membership
  errors; an `UNKNOWN_MEMBER_ID` response clears the stale member id before the
  next attempt. Live smoke run
  [`31500606310`](https://github.com/TaeeunKil/kafrust/actions/runs/31500606310)
  passed all 11 broker, security, ACL, KIP-848, and multi-broker failover jobs
  after these runtime changes on the merged `main` branch.
- Producer records without an explicit partition use Kafka-compatible Murmur2
  routing when a key is present, preserving standard-client key affinity.
- Keyless producer records use per-topic batch-sticky round-robin routing.
  Single sends rotate after completion, records in the same batch or buffered
  flush stay together, and retries keep the original sticky partition.
- `ProducerConfig::partitioner` accepts a thread-safe custom callback for
  records without explicit partitions. Immediate, batch, and buffered sends
  share the callback, explicit partitions bypass it, and metadata validation
  rejects a callback result that is not a current partition. Focused tests cover
  callback context, explicit-partition precedence, and invalid results.
- Manual `Live Kafka Smoke` run `30066831820` passed the exact
  `0,1,2,3,4,5,0` keyless rotation sequence against a six-partition,
  three-broker Kafka 3.7.2 topic while all seven regression profiles remained
  green.
- Manual `Live Kafka Smoke` run `30066328105` passed key-derived producer
  routing and buffered fetch-back across every selected partition on the
  three-broker Kafka 3.7.2 profile. The same run passed Kafka 3.7.2, 3.8.1,
  3.9.1, and 4.3.1 single-node plaintext plus TLS, SASL_PLAINTEXT, and
  SASL_SSL/SCRAM-SHA-256 profiles.
- Immediate and batch `acks=0` sends write and flush Produce requests without
  waiting for responses and return offset `-1`. Manual `Live Kafka Smoke` run
  `31464933145` passed these paths against Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 single-node plaintext brokers; durable-delivery and broker-error
  semantics remain explicitly outside the no-ack guarantee.
- Static classic-group membership carries a configured stable instance ID
  through JoinGroup v5, SyncGroup v3, Heartbeat v3, generation-fenced
  TxnOffsetCommit v3, and OffsetCommit v7. Duplicate instance fencing is
  classified separately from rejoinable group errors.
- Classic groups can advertise and execute either Kafka's `range` or
  `roundrobin` assignor, including mixed topic subscriptions.
- SASL/OAUTHBEARER uses the RFC 7628 GS2 initial response with either an empty
  authorization identity (`n,,`) or an explicit identity (`n,a=<id>,`), keeps
  the bearer token out of `Debug` output, and is exposed through all high-level
  connection builders. OAUTHBEARER uses flexible `SaslAuthenticate v2` for
  initial authentication and `SaslAuthenticate v1` for provider
  re-authentication, and sends Kafka's
  control-A acknowledgement after an error challenge. Injected broker tests
  cover handshake ordering, exact authentication bytes, and error challenge
  acknowledgement; the signed OIDC live job above adds Kafka 3.7.2 coverage.
  Async token providers are covered by injected connection tests and the
  published `0.3.4` gate [`32411655133`](https://github.com/TaeeunKil/kafrust/actions/runs/32411655133).
  The published gate uses Kafka's built-in unsecured validator; external
  provider-specific OAuth/OIDC verification remains open.
- Cooperative-sticky group membership encodes Subscription v1 owned
  partitions and performs staged ownership transfers with focused local tests.
  Manual `Live Kafka Smoke` run `31464021305` passed the Kafka 3.7.2
  three-broker cooperative group example. Live Kafka Smoke run
  [`31474626799`](https://github.com/TaeeunKil/kafrust/actions/runs/31474626799)
  additionally passed multi-member ownership transfer, transient-member
  rollback, and member-loss recovery in the three-broker profile.
- Consumer-group rejoin preserves the broker-assigned dynamic member ID in
  JoinGroup requests, preventing a rejoining member from being treated as a
  new member during cooperative or classic rebalances. Focused tests cover
  staged non-leader rejoin decisions and member-loss assignment recovery.
- The explicit per-record commit queue coalesces offsets by topic-partition and
  flushes them under the current generation. Its record-fetch plus OffsetCommit
  behavior passed the classic Kafka 3.7.2 through 4.3.1 matrix and KIP-848 on
  Kafka 4.3.1 in [`Live Kafka Smoke`, run `31560143467`](https://github.com/TaeeunKil/kafrust/actions/runs/31560143467).
- The bounded `ConsumerGroup::spawn_commit_worker` passed interval flush,
  explicit flush, classic and KIP-848 rejoin synchronization, and graceful
  shutdown across the current live matrix in
  [`Live Kafka Smoke`, run `31563953123`](https://github.com/TaeeunKil/kafrust/actions/runs/31563953123).
- `RebalanceListener` exposes synchronous assignment snapshots for initial join,
  classic and KIP-848 rejoin, and broker-assigned KIP-848 assignment changes
  from foreground or background heartbeats. Callback lifecycle behavior is
  covered by focused API tests and the three-broker cooperative multi-member
  live example, which asserts Before/After callbacks in
  [`Live Kafka Smoke` run `31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371);
  target-workload timing and cancellation qualification remain open.
- KIP-848 `ConsumerGroupHeartbeat v0` protocol types for explicit topic names
  and v1 for regex subscriptions, Metadata v12 UUID mappings, and a selectable
  high-level foreground group path are implemented with assignment
  application, member-epoch heartbeats/rejoin, negotiated OffsetFetch and
  OffsetCommit v10 with v9 fallback, explicit leave, and injected low-level
  roundtrip coverage. The current source path passed the fresh live negotiation
  matrix in [`32339508792`](https://github.com/TaeeunKil/kafrust/actions/runs/32339508792),
  including the Kafka 4.3.1 v10 Admin and regex v1 paths.
- KIP-848 background heartbeats share member epoch and broker assignment state
  with the owning group handle. Assignment responses are applied once per
  response, nullable assignments preserve existing ownership, and a rejoin
  session token stops stale tasks from sending requests for a new member epoch.
  Focused tests cover state updates and nullable assignment preservation.
- Kafka 4.3.1 KIP-848 live qualification passed in
  [`Live Kafka Smoke` run `31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371),
  including foreground and background heartbeat, concurrent-member rejoin,
  OffsetFetch v9, OffsetCommit v9, transient coordinator retry, and explicit
  leave. The same run also passed a three-broker Kafka 4.3.1 coordinator
  broker-stop recovery path for the foreground group poll process.
- The Kafka 3.7.2 three-broker `SASL_PLAINTEXT` profile stopped the active
  group coordinator and recovered a classic consumer group through the
  remaining authenticated brokers in
  [`Live Kafka Smoke` run `31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594).
- Kafka 4.3.1 KIP-848 coordinator recovery over `SASL_PLAINTEXT` is now
  qualified in a three-broker KRaft profile. The active coordinator is stopped
  after the first poll, the consumer-protocol group completes through the
  remaining authenticated brokers, and the stopped broker is restarted in
  [`Live Kafka Smoke` run `31569709189`](https://github.com/TaeeunKil/kafrust/actions/runs/31569709189).
- Kafka 4.3.1 KIP-848 coordinator recovery over `SASL_SSL` with SCRAM-SHA-256
  is also qualified in a three-broker KRaft profile. All three external TLS
  listeners are verified before the active coordinator is stopped, and the
  group completes through the remaining authenticated brokers in
  [`Live Kafka Smoke` run `31570924845`](https://github.com/TaeeunKil/kafrust/actions/runs/31570924845).
- The same Kafka 4.3.1 three-broker SASL_SSL/SCRAM KIP-848 profile then ran a
  second group through another coordinator broker-stop after the first broker
  had recovered. Both groups completed their poll and leave paths in
  [`Live Kafka Smoke` run `31695433295`](https://github.com/TaeeunKil/kafrust/actions/runs/31695433295),
  extending the secured evidence beyond a single coordinator failure.
- Partition-leader faults and broader KIP-848 failure combinations remain open
  beyond this repeated coordinator gate.
- Dynamic and static members can explicitly leave through LeaveGroup v3,
  avoiding session-timeout cleanup after graceful shutdown.
- Manual `Live Kafka Smoke` run `30065025169` passed graceful LeaveGroup v3 on
  Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext brokers plus TLS,
  SASL_PLAINTEXT, SASL_SSL, and the three-broker regression profile.
- Consumer group assignments without committed offsets support typed
  `Earliest`, `Latest`, and explicit absolute offset reset policies.
  Leader-routed `ListOffsets v1` resolution and the earliest/latest behavioral
  example passed Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 in manual `Live Kafka
  Smoke` run `30229718813`; all multi-broker, TLS, SASL_PLAINTEXT, and
  SASL_SSL regression profiles also passed.
- Direct and group consumers expose assignment-scoped `position`, `seek`,
  `pause`, and `resume` controls. Manual `Live Kafka Smoke` run `30230885629`
  verified paused polls, explicit seek and resume, and subsequent position
  advancement on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1; all multi-broker and
  secured regression profiles also passed.
- Direct and group consumers expose `PartitionWatermarks` through
  leader-routed Metadata v1 and ListOffsets v1 requests without requiring an
  assignment. Manual `Live Kafka Smoke` run `30333202216` passed the direct
  path on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plus every multi-broker and
  secured profile, and passed the group delegate on all four plaintext broker
  versions.
- `AdminClient::create_partitions` routes CreatePartitions v0 to the active
  controller, supports automatic or explicit replica assignment, and preserves
  per-topic errors. Manual `Live Kafka Smoke` run `30230301762` expanded a
  topic and verified its exact Metadata v1 partition count on Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1 plus the three-broker Kafka 3.7.2 profile; every
  secured regression profile also passed.
- Transaction coordinator transport recovery reconnects through the bootstrap
  set and rediscovers coordinators for all implemented transaction requests.
  Manual run `30335739033` stopped the active transaction coordinator after
  Produce and passed commit plus read-committed fetch-back on the Kafka 3.7.2
  three-broker profile; all seven other profiles remained green.
- Manual `Live Kafka Smoke` run `30064594451` passed the round-robin
  static-member path on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext
  brokers; all secured and multi-broker regression jobs also passed.
- Manual `Live Kafka Smoke` run `30064182907` passed static join, poll,
  heartbeat, and OffsetCommit v7 on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1
  plaintext brokers while every existing secured and multi-broker regression
  job remained green.
- Release `v0.2.2` post-publish verification run
  [`31471610040`](https://github.com/TaeeunKil/kafrust/actions/runs/31471610040)
  passed all nine Live Kafka Smoke jobs on 2026-08-11. This included the
  three-broker coordinator and broker-stop recovery paths, all four plaintext
  broker versions, TLS, SASL_PLAINTEXT, SASL_SSL/SCRAM, ACL authorizer,
  compression, idempotent, transactional, `read_committed`, admin,
  consumer-group, and `acks=0` smoke paths.
- Release `v0.2.3` published `kafrust-protocol` before `kafrust` so the
  packaged client resolves the matching protocol crate. Both package
  verification steps passed, and a fresh external project compiled the
  published client with its default and `tls` features while exercising the
  public configuration and producer-record builders.
- Release `v0.2.4` published `kafrust-protocol` before `kafrust`; both package
  dry-runs and uploads passed. The exact docs.rs pages for both crates return
  HTTP 200 and a fresh external project compiled the published client with
  default and `tls` features, plus `RUSTDOCFLAGS=-D warnings`, on the project
  MSRV Rust 1.81 toolchain. The current live qualification is
  [`31500606310`](https://github.com/TaeeunKil/kafrust/actions/runs/31500606310)
  on the merged `main` branch.
- Release `v0.2.5` published `kafrust-protocol` before `kafrust`; package
  dry-runs, uploads, docs.rs HTTP 200 checks, and fresh default/tls projects on
  Rust 1.81 all passed. The release includes the custom partitioner and
  synchronous rebalance listener surfaces, with live qualification in
  [`31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371).
- Release `v0.2.6` published both crates after protocol-first verification.
  Fresh external default and dependency-level TLS projects compiled against
  the published client, the exact [`kafrust 0.2.6 docs.rs`](https://docs.rs/kafrust/0.2.6/kafrust/)
  and [`kafrust-protocol 0.2.6 docs.rs`](https://docs.rs/kafrust-protocol/0.2.6/kafrust_protocol/)
  pages returned HTTP 200, release CI passed on stable and Rust 1.81 in
  [`31566231208`](https://github.com/TaeeunKil/kafrust/actions/runs/31566231208),
  and the post-release live matrix passed in
  [`31565059236`](https://github.com/TaeeunKil/kafrust/actions/runs/31565059236).
- Release `v0.2.7` published `kafrust-protocol` before `kafrust` after both
  package dry-runs passed. Both crates were published to crates.io, a fresh
  external project compiled against `kafrust 0.2.7`, and the exact
  [`kafrust 0.2.7 docs.rs`](https://docs.rs/kafrust/0.2.7/kafrust/) and
  [`kafrust-protocol 0.2.7 docs.rs`](https://docs.rs/kafrust-protocol/0.2.7/kafrust_protocol/)
  pages returned HTTP 200. The post-change live matrix passed in
  [`31585451218`](https://github.com/TaeeunKil/kafrust/actions/runs/31585451218),
  including signed OIDC/JWKS Kafka 3.7.2 coverage in job
  [`94078116567`](https://github.com/TaeeunKil/kafrust/actions/runs/31585451218/job/94078116567).
- Release `v0.2.8` published `kafrust-protocol` before `kafrust` after package
  dry-runs passed. Both crates were published to crates.io, fresh external
  default and `tls` projects compiled against the published client, the exact
  [`kafrust 0.2.8 docs.rs`](https://docs.rs/kafrust/0.2.8/kafrust/) and
  [`kafrust-protocol 0.2.8 docs.rs`](https://docs.rs/kafrust-protocol/0.2.8/kafrust_protocol/)
  pages returned HTTP 200, and GitHub release
  [`v0.2.8`](https://github.com/TaeeunKil/kafrust/releases/tag/v0.2.8) was
  published against the verified release commit. The admin offset live matrix
  is qualified in [`31595485915`](https://github.com/TaeeunKil/kafrust/actions/runs/31595485915)
  and [`31597505667`](https://github.com/TaeeunKil/kafrust/actions/runs/31597505667).
- Secured multi-broker failure injection is now qualified for the tested
  `SASL_PLAINTEXT` and `SASL_SSL` paths. The three-broker `SASL_PLAINTEXT`
  profile in [`31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594)
  verified transaction coordinator, consumer-group coordinator, producer,
  and direct-consumer recovery after broker stops. The three-broker
  `SASL_SSL` SCRAM profile in
  [`31568412595`](https://github.com/TaeeunKil/kafrust/actions/runs/31568412595)
  verified all external TLS listeners plus consumer-group coordinator,
  partition-leader recovery, and safe transactional producer
  reinitialization after coordinator failure. Production OAuth/OIDC provider
  compatibility, broader KIP-848 and transaction fault matrices, and
  workload-specific canary evidence remain before a 1.0 replacement claim.
  The plaintext three-broker profile now also qualifies a repeated
  partition-leader fault sequence in
  [`31573662135`](https://github.com/TaeeunKil/kafrust/actions/runs/31573662135);
  broader secured and KIP-848 repeated-fault matrices remain open.
  Kafka 4.3.1 KIP-848 coordinator broker-stop
  recovery is qualified over PLAINTEXT, SASL_PLAINTEXT, and SASL_SSL/SCRAM in
  the three-broker profiles by
  [`31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371),
  [`31569709189`](https://github.com/TaeeunKil/kafrust/actions/runs/31569709189),
  [`31570924845`](https://github.com/TaeeunKil/kafrust/actions/runs/31570924845),
  while the broader KIP-848 fault matrix remains open.
- The complete 17-job `Live Kafka Smoke` matrix passed on `main` after the
  transaction outcome-safety change in
  [`31576212276`](https://github.com/TaeeunKil/kafrust/actions/runs/31576212276).
  This rerun covered the supported Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1
  profiles, TLS, SASL/PLAIN, SASL/SCRAM, the test-only OAUTHBEARER validator,
  ACL administration, secured multi-broker failover, and KIP-848 recovery.
- The latest complete 17-job matrix passed at commit `256847f` in
  [`31624278107`](https://github.com/TaeeunKil/kafrust/actions/runs/31624278107)
  after adding bounded pre-transmission controller discovery retries for
  controller-routed Admin writes. Existing topic, partition, SCRAM, and
  reassignment workflows remained green across all supported profiles.
- The follow-up complete 17-job matrix passed at commit `25d614a` in
  [`31627790408`](https://github.com/TaeeunKil/kafrust/actions/runs/31627790408)
  after the ACL authorizer example added bounded polling for asynchronous
  post-create visibility. All supported broker, security, failover, ACL, and
  KIP-848 profiles remained green.
- The latest complete 17-job matrix passed at commit `43969e0` in
  [`31630339333`](https://github.com/TaeeunKil/kafrust/actions/runs/31630339333).
  It included the Kafka 3.7.2 multi-broker DeleteRecords and DescribeProducers
  leader-stop recovery gates, alongside the supported version, security, ACL,
  failover, and KIP-848 profiles.
- The latest complete 17-job matrix passed at commit `be78225` in
  [`31640494509`](https://github.com/TaeeunKil/kafrust/actions/runs/31640494509)
  after adding the flexible Produce v9 negotiation gate while retaining the
  rack-aware Fetch v12 and multi-broker recovery gates.
- A complete 17-job matrix passed at commit `9149d8f` in
  [`31643246432`](https://github.com/TaeeunKil/kafrust/actions/runs/31643246432)
  after adding flexible Produce v11 preference with v9 fallback. Kafka 4.3.1
  selected v11, while Kafka 3.7.2, 3.8.1, and 3.9.1 selected v9.
- A complete 17-job matrix passed at commit `4ab3226` in
  [`31644710449`](https://github.com/TaeeunKil/kafrust/actions/runs/31644710449)
  after adding ListTransactions v0/v1 protocol support, broker-shard
  aggregation, and a live admin example.
- The latest complete 17-job matrix passed at commit `3536376` in
  [`31645842282`](https://github.com/TaeeunKil/kafrust/actions/runs/31645842282)
  after adding flexible Produce v12 preference. Kafka 4.3.1 selected v12,
  Kafka 3.8.1 and 3.9.1 selected v11, and Kafka 3.7.2 selected v9; all
  existing security, failover, Admin, compression, transaction, and KIP-848
  gates remained green.
- High-level client builders now validate startup configuration before network
  access. `Error::InvalidConfiguration { field, reason }` covers blank
  bootstrap entries, zero request or decode limits, invalid direct-consumer
  fetch bounds, empty group subscriptions and IDs, zero commit or heartbeat
  intervals, and invalid transaction settings. Focused boundary tests prove
  these failures do not open a broker connection; `MissingBootstrapServer`
  remains the dedicated empty-list error.
- Release `v0.2.9` published `kafrust-protocol` before `kafrust` after both
  package dry-runs and staged all-feature docs builds passed. crates.io resolves
  both packages at `0.2.9`, both docs.rs pages return HTTP 200, and a fresh
  project outside this repository compiles the published client with all
  features. The release still requires the supported live Kafka matrix before
  any broader replacement claim.
- The same typed checks are exposed as connection-free `validate()` preflight
  methods on `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and
  `ConsumerGroupConfig`, so applications can fail startup configuration before
  beginning their broker connection lifecycle.
- Release `v0.2.10` published `kafrust-protocol` before `kafrust` after package
  dry-runs, staged all-feature docs builds, and an external published-crate
  smoke project passed. crates.io resolves both packages at `0.2.10`, both
  docs.rs pages return HTTP 200, and the Git tag and GitHub release are
  published from the verified release commit.
- Fetch RecordBatch decoding now preserves record headers through the public
  `ConsumerRecord::headers()` API. `ConsumerRecordHeader::value()` retains
  Kafka's nullable header-value semantics, while legacy MessageSet records
  continue to expose an empty header list. Focused protocol and high-level
  mapping tests cover ordered and null-valued headers.
- Release `v0.2.11` published the consumer-header implementation after
  protocol-first package verification, staged all-feature docs builds, a fresh
  external TLS project compile, crates.io resolution, and HTTP 200 responses
  from both docs.rs pages. The complete 18-job Kafka 3.7.2 through 4.3.1
  plaintext, TLS, SASL, OAUTHBEARER, ACL, multi-broker failover, and KIP-848
  matrix passed in [`31653113614`](https://github.com/TaeeunKil/kafrust/actions/runs/31653113614).
- Public `ConsumerGroupConfig::validate()` now validates the nested client
  configuration and enabled automatic-commit interval without opening a
  connection. Regression tests cover missing bootstrap servers and a zero
  commit interval; `join()` uses the same single preflight path.
- Release `v0.2.12` published that preflight correction after protocol-first
  packaging, staged all-feature docs, a fresh external TLS project compile,
  crates.io resolution, and HTTP 200 responses from both docs.rs pages. The
  full 18-job live matrix passed in
  [`31654276817`](https://github.com/TaeeunKil/kafrust/actions/runs/31654276817).
- Producer idempotence preflight now reports typed `InvalidConfiguration`
  errors when callers override the required `acks=all` or retry settings after
  enabling idempotence. Focused tests cover both invalid overrides without
  opening a broker connection.
- Release `v0.2.13` published that typed validation correction after
  protocol-first packaging, staged all-feature docs, a fresh external project
  compile with `tls` and all features, crates.io resolution, and HTTP 200
  responses from both docs.rs pages. The complete 18-job live matrix passed on
  the release commit in
  [`31655154051`](https://github.com/TaeeunKil/kafrust/actions/runs/31655154051).
- Shared connection preflight now validates required SASL credentials and
  explicit TLS server-name overrides before network access. `AdminClient::validate()`
  exposes the same connection-free check for administrative workflows, with
  focused tests for missing bootstrap servers, SASL credentials, and TLS names.
- Release `v0.2.14` published the admin preflight API after protocol-first
  packaging, staged all-feature docs, a fresh external project compile with
  `tls` and all features, crates.io resolution, and HTTP 200 responses from
  both docs.rs pages. The complete 17-job live matrix passed on the release
  commit in
  [`31656232857`](https://github.com/TaeeunKil/kafrust/actions/runs/31656232857).
- Fetch RecordBatch decoding now preserves the partition leader epoch through
  `ConsumerRecord::leader_epoch()`. Legacy MessageSet records explicitly use
  `-1`; focused protocol and high-level mapping tests cover both shapes. This
  preserves the broker state needed for future leader-epoch offset recovery.
- Release `v0.2.15` published the leader-epoch compatibility slice after
  protocol-first packaging, staged all-feature docs, a fresh external project
  compile with `tls` and all features, crates.io resolution, and HTTP 200
  responses from both docs.rs pages. The complete 17-job live matrix passed on
  the release commit in
  [`31657464035`](https://github.com/TaeeunKil/kafrust/actions/runs/31657464035).
- OffsetForLeaderEpoch v3 is now available as a pure-Rust protocol primitive,
  low-level `Client` call, and high-level `Consumer::offset_for_leader_epoch`
  method. The path preserves current and target epochs, broker error codes,
  returned leader epochs, and end offsets with focused byte-level and
  injected-broker coverage. It is an explicit recovery primitive; automatic
  fetch truncation correction, group rebalance integration, and live
  failure-injection qualification remain future work.
- Release `v0.2.16` published the OffsetForLeaderEpoch recovery primitive after
  protocol-first package verification, staged all-feature documentation builds,
  a fresh external project compile with `tls` and all features, crates.io
  resolution, and HTTP 200 responses from both docs.rs pages. The complete
  17-job live matrix passed on the release preparation commit in
  [`31658987651`](https://github.com/TaeeunKil/kafrust/actions/runs/31658987651).
- Consumer assignments now retain the latest RecordBatch leader epoch and send
  it in Fetch v11/v12 requests. Fenced and unknown leader-epoch broker errors
  refresh metadata under the bounded fetch retry policy. Automatic direct
  consumer truncation recovery now refreshes Metadata v12, resolves the prior
  epoch boundary through OffsetForLeaderEpoch v3, clamps the retry offset, and
  resends Fetch with the new epoch. An injected-broker regression covers the
  complete path; live broker qualification and group-level recovery
  orchestration remain separate release gates.
- Release `v0.2.17` published leader-epoch propagation through consumer fetch
  state after protocol-first package verification, staged all-feature
  documentation builds, a fresh external project compile with `tls` and all
  features, crates.io resolution, and HTTP 200 responses from both docs.rs
  pages. The complete 17-job live matrix passed on the release preparation
  commit in
  [`31660184647`](https://github.com/TaeeunKil/kafrust/actions/runs/31660184647).
- Assigned direct consumers can opt into bounded `OffsetResetPolicy::Earliest`
  or `Latest` recovery when Kafka returns `OFFSET_OUT_OF_RANGE`. The client
  resolves the retained low watermark or current log end through the partition
  leader and retries the assigned poll once; explicit `Consumer::fetch` offsets
  remain unchanged. `OffsetResetPolicy` is now shared by direct and group
  consumer configuration, and the typed `BrokerErrorKind::OffsetOutOfRange`
  classification is covered by injected-broker regression tests.
- Release `v0.2.18` published the bounded out-of-range consumer recovery slice
  after protocol-first package verification, staged all-feature documentation
  builds, a fresh external project compile with `tls`, crates.io resolution,
  and HTTP 200 responses from both docs.rs pages. Main CI passed in
  [`31661719918`](https://github.com/TaeeunKil/kafrust/actions/runs/31661719918)
  and the complete 17-job live matrix passed in
  [`31661883116`](https://github.com/TaeeunKil/kafrust/actions/runs/31661883116).
- Fetch v12 now forwards the assignment's last fetched leader epoch, and group
  offset-reset qualification covers initial Earliest/Latest behavior plus
  committed offsets recovered after the retained log moves past them. The
  complete 17-job live matrix passed on the follow-up commit in
  [`31663188419`](https://github.com/TaeeunKil/kafrust/actions/runs/31663188419).
- DeleteGroups v1 and OffsetDelete v0 now retry retryable coordinator responses
  through fresh coordinator discovery within the bounded Admin retry budget.
  Focused mock-broker regressions cover transient `NotCoordinator` responses
  and preserve the existing group and partition-level outcomes. Mutation
  transport failures after transmission remain single-attempt because the
  broker-side result is ambiguous. The complete 17-job matrix passed this
  change at commit `ec293d1` in
  [`31665016772`](https://github.com/TaeeunKil/kafrust/actions/runs/31665016772);
  transparent replay after a mutation transport failure remains explicitly
  outside the compatibility claim.
- Release `v0.2.20` published the coordinator-response retry slice after
  protocol-first package verification, staged all-feature documentation builds,
  a fresh external Rust 1.81 project compile with `tls`, crates.io resolution,
  and HTTP 200 responses from both docs.rs pages. The complete 17-job live
  matrix remained green in
  [`31665016772`](https://github.com/TaeeunKil/kafrust/actions/runs/31665016772).
- Release `v0.2.21` published the classic eager StickyAssignor slice after
  protocol-first package verification, staged all-feature documentation builds,
  a fresh external project compile with `tls`, crates.io resolution, and HTTP
  200 responses from both docs.rs pages. The complete 17-job live matrix,
  including Kafka 3.7.2 three-broker sticky transfer and recovery, passed in
  [`31666975512`](https://github.com/TaeeunKil/kafrust/actions/runs/31666975512).
- Release `v0.2.22` publishes the sticky compatibility correction after adding
  same-generation duplicate-claim invalidation and Kafka-compatible mixed-topic
  candidate ordering. The complete 17-job live matrix passed in
  [`31668518895`](https://github.com/TaeeunKil/kafrust/actions/runs/31668518895).
- Release `v0.2.23` publishes classic AlterConfigs v1 through the typed
  `TopicConfigUpdate` API. Package, docs.rs, crates.io, and fresh external
  `tls` compile verification passed after the complete 17-job matrix qualified
  the plaintext admin lifecycle and Kafka 3.7.2 three-broker path in
  [`31669906872`](https://github.com/TaeeunKil/kafrust/actions/runs/31669906872).
- Release `v0.2.24` publishes broker-scoped fetch-session reuse for rack-aware
  Fetch v11/v12. Package, docs.rs, crates.io, and fresh external `tls` compile
  verification passed after the complete 17-job matrix qualified the Kafka
  3.7.2 three-broker follow-up path in
  [`31671783977`](https://github.com/TaeeunKil/kafrust/actions/runs/31671783977).
- Release `v0.2.25` broadens Fetch v11/v12 negotiation and broker-scoped session
  reuse to direct and group consumers without `client_rack`, while retaining
  v4 fallback for older capability ranges. The complete 17-job matrix passed
  on commit `f222d05` in
  [`31673377685`](https://github.com/TaeeunKil/kafrust/actions/runs/31673377685).
- Classic eager `StickyAssignor` support now has a public
  `ConsumerGroupAssignmentStrategy::Sticky` variant. JoinGroup uses
  Subscription v0 `user_data` with Kafka's previous-assignment schema,
  decodes both legacy v0 and generation-carrying v1 data, preserves valid
  ownership, and applies transfers eagerly in the current SyncGroup result.
  Leader-side parsing also accepts the append-only classic subscription
  envelope through v3. Focused tests cover wire bytes, generation metadata,
  versioned envelopes, balancing, and member transfer. The Kafka 3.7.2
  three-broker multi-member sticky matrix passed transfer, transient-member
  rollback, and member-loss recovery in
  [`31666975512`](https://github.com/TaeeunKil/kafrust/actions/runs/31666975512),
  completing this release gate. Exact parity for every Kafka assignor edge
  case and arbitrary mixed-subscription workload remains future work.
- The classic Admin topic lifecycle is now live-qualified over authenticated
  Kafka connections. TLS, SASL/PLAIN, and SASL_SSL SCRAM-SHA-256 profiles all
  passed CreateTopics, DescribeConfigs, classic AlterConfigs,
  IncrementalAlterConfigs, and DeleteTopics in the complete matrix
  [`31674680581`](https://github.com/TaeeunKil/kafrust/actions/runs/31674680581).
- Release `v0.2.26` publishes automatic direct-consumer leader-epoch
  truncation recovery. After a fenced or unknown leader-epoch Fetch error, the
  client negotiates Metadata v12, resolves the previous epoch boundary through
  OffsetForLeaderEpoch v3, clamps the retry offset, and resends Fetch with the
  current epoch. The complete 17-job matrix passed on code commit `1694889` in
  [`31677617186`](https://github.com/TaeeunKil/kafrust/actions/runs/31677617186).
  Package, docs.rs, crates.io, and fresh external Rust 1.81 `tls` compile
  checks passed for `0.2.26`. The follow-up workflow-only live gate passed in
  [`31679167875`](https://github.com/TaeeunKil/kafrust/actions/runs/31679167875):
  Kafka 3.7.2 three-broker repeated leader failover moved the observed epoch
  from 1 to 2 and the assigned direct consumer recovered automatically through
  the OffsetForLeaderEpoch path. Group rebalance recovery and data-loss/log-
  retention fault scenarios remain separate gates.
- The current development line adds controller-routed ElectLeaders v0-v2
  negotiation and typed preferred/unclean outcomes. Plaintext multi-broker
  preferred-election verification is complete in
  [`31681439569`](https://github.com/TaeeunKil/kafrust/actions/runs/31681439569);
  the same preferred/no-op path over three-broker SASL_SSL with SCRAM-SHA-256
  passed in the complete matrix
  [`31691204180`](https://github.com/TaeeunKil/kafrust/actions/runs/31691204180).
  This does not make unclean election a default-safe operation.
- The current development line also adds broker-local DescribeLogDirs v1-v5
  negotiation. The plaintext multi-broker filtered query with capacity and
  replica-lag decoding passed in
  [`31682889124`](https://github.com/TaeeunKil/kafrust/actions/runs/31682889124);
  the same broker-1/2/3 query passed over three-broker SASL_SSL with
  SCRAM-SHA-256 in the complete matrix
  [`31691204180`](https://github.com/TaeeunKil/kafrust/actions/runs/31691204180).
- Release `v0.2.27` publishes the coordinated protocol and client crates after
  protocol-first package verification, docs.rs HTTP 200 checks, and a fresh
  external `kafrust 0.2.27` project with `tls`. The follow-up current-main
  `Live Kafka Smoke` matrix passed all 17 jobs in
  [`31716400583`](https://github.com/TaeeunKil/kafrust/actions/runs/31716400583),
  including heartbeat-preserved classic Kafka 3.7.2 and KIP-848 Kafka 4.3.1
  leader-epoch recovery over plaintext, SASL/PLAIN, and SASL_SSL/SCRAM.
- The current main line adds the `consumer_retention_recovery` example and a
  direct assigned-consumer `OffsetOutOfRange` gate. It produces a known
  position, moves the retained low watermark past that position with Admin
  `DeleteRecords`, then verifies `OffsetResetPolicy::Earliest` resumes from
  the new boundary and reaches a post-delete record. All four single-node
  Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 profiles passed this gate in the
  complete 17-job run [`31717934296`](https://github.com/TaeeunKil/kafrust/actions/runs/31717934296).
  This is a controlled retention boundary claim; arbitrary retention timing,
  unclean-election data loss, and combined fault scenarios remain unclaimed.
- A fresh project outside the repository resolved published `kafrust 0.2.27`
  and `kafrust-protocol 0.2.27` from crates.io, then executed a producer to
  direct-consumer roundtrip against Kafka 3.7.2 in
  [`Published Crate Smoke`, run `31719041843`](https://github.com/TaeeunKil/kafrust/actions/runs/31719041843).
  The lockfile was checked for the requested published client version, so this
  gate does not rely on the workspace path dependency.
- The published-crate runtime gate was expanded in
  [`Published Crate Smoke`, run `31721075666`](https://github.com/TaeeunKil/kafrust/actions/runs/31721075666).
  A fresh external project resolved `kafrust 0.2.27` and its matching protocol
  crate from crates.io, then executed `AdminClient::describe_cluster`, an
  idempotent producer, a direct consumer, and a classic consumer group against
  Kafka 3.7.2. This qualifies the published public entry points at runtime;
  it does not replace the broader multi-broker, security, and workload gates.
- The published-crate runtime gate was expanded to a two-profile matrix in
  [`Published Crate Smoke`, run `31729003352`](https://github.com/TaeeunKil/kafrust/actions/runs/31729003352).
  Fresh external projects resolved `kafrust 0.2.27` and its matching protocol
  crate from crates.io, then completed the Admin, idempotent-producer,
  direct-consumer, and group poll/leave paths against Kafka 3.7.2 classic and
  Kafka 4.3.1 KIP-848. Both profiles passed without a workspace path
  dependency. This strengthens the published-artifact gate but does not
  replace the broader multi-broker, security, failure, and workload gates.
- The published-crate matrix then added Kafka 3.7.2 `SASL_SSL` with
  SCRAM-SHA-256 and the published `tls` feature in
  [`Published Crate Smoke`, run `31729868783`](https://github.com/TaeeunKil/kafrust/actions/runs/31729868783).
  A fresh external project resolved both published crates from crates.io,
  configured the public TLS/SCRAM builders, and completed Admin,
  idempotent-producer, direct-consumer, and classic group paths. This qualifies
  the tested published security profile, not every security provider,
  topology, or failure mode.
- The same three-profile published workflow
  [`31730411006`](https://github.com/TaeeunKil/kafrust/actions/runs/31730411006)
  added a transaction boundary check. Each fresh external project wrote an
  aborted transaction followed by a committed transaction and verified that
  `ReadCommitted` exposed only the committed record on Kafka 3.7.2 classic,
  Kafka 4.3.1 KIP-848, and Kafka 3.7.2 SASL_SSL/SCRAM with the published `tls`
  feature. This qualifies representative published transaction semantics, not
  every transaction failure or throughput workload.
- The published compression matrix passed in
  [`31731421599`](https://github.com/TaeeunKil/kafrust/actions/runs/31731421599).
  Fresh external projects resolved `kafrust 0.2.27` from crates.io and
  completed direct, transactional, and `ReadCommitted` paths with Gzip,
  Snappy, LZ4, and Zstd producer compression against Kafka 3.7.2. This
  qualifies published codec configuration and fetch roundtrips; codec-specific
  throughput and failure qualification remain separate.
- The published Admin lifecycle gate passed in
  [`31731934027`](https://github.com/TaeeunKil/kafrust/actions/runs/31731934027).
  Fresh external projects created a topic with `NewTopic`, verified it through
  `list_topics` and `describe_topic_configs`, and deleted it through the public
  `AdminClient` API across the classic, KIP-848, SASL_SSL/SCRAM, and four
  compression profiles. This is representative Admin runtime evidence, not
  every Admin API or authorization policy.
- Release `v0.2.28` publishes the KIP-848 empty-assignment join fix together
  with the coordinated protocol and client crates. The seven-profile external
  published smoke [`31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869)
  resolved both `0.2.28` crates from crates.io and passed classic, KIP-848,
  SASL_SSL/SCRAM, and Gzip/Snappy/LZ4/Zstd profiles. It also verified
  `commit_record` plus `commit_queued_offsets`, same-group leave/rejoin, and
  resume at the committed offset without replay. This is published-artifact
  evidence for representative paths, not the full replacement gate.
- The published `0.2.28` multi-broker follow-up
  [`31735177161`](https://github.com/TaeeunKil/kafrust/actions/runs/31735177161)
  passed against a fresh three-broker Kafka 3.7.2 KRaft cluster. It committed
  a replicated-topic group record, stopped the selected partition leader,
  waited for replacement-leader metadata, and verified a same-group rejoin
  consumed a post-failover record. This is one published classic failover
  workload, not complete multi-broker or failure parity.
- The same published multi-broker fixture was parameterized for Kafka 4.3.1
  and the KIP-848 `consumer` group protocol. Run
  [`31735762087`](https://github.com/TaeeunKil/kafrust/actions/runs/31735762087)
  resolved `kafrust 0.2.28` from crates.io, committed before the broker stop,
  followed replacement leadership, and consumed a post-failover record after
  KIP-848 rejoin. Published multi-member, secured, and broader fault workloads
  remain separate 1.0 gates.
- The published group-rebalance fixture then qualified two-member partition
  ownership and record delivery from crates.io. Kafka 3.7.2 classic passed in
  [`31736939236`](https://github.com/TaeeunKil/kafrust/actions/runs/31736939236),
  and Kafka 4.3.1 KIP-848 passed in
  [`31736362411`](https://github.com/TaeeunKil/kafrust/actions/runs/31736362411).
  Both runs covered disjoint ownership of all six partitions and consumption
  through the published `0.2.28` artifact; broader assignor and failure
  matrices remain separate 1.0 gates.
- The published secured group-rebalance workflow then passed the same two-member
  ownership and record-delivery gate with SCRAM-SHA-256 over SASL_SSL. Kafka
  3.7.2 classic passed in
  [`31740436499`](https://github.com/TaeeunKil/kafrust/actions/runs/31740436499),
  and Kafka 4.3.1 KIP-848 passed in
  [`31740567979`](https://github.com/TaeeunKil/kafrust/actions/runs/31740567979).
  This closes the published secured multi-member gate; every assignor,
  security mechanism, and member-failure workload remains outside the claim.
- The published `0.2.28` seven-profile smoke then added active-group Admin
  inspection and committed-offset reads. Run
  [`31737581786`](https://github.com/TaeeunKil/kafrust/actions/runs/31737581786)
  passed Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, SASL_SSL/SCRAM, and all
  four compression profiles from fresh external projects. Broader Admin
  authorization and mutation-failure matrices remain separate 1.0 gates.
- The published `0.2.28` transaction failover workflow passed in
  [`31738090052`](https://github.com/TaeeunKil/kafrust/actions/runs/31738090052).
  A fresh external project identified its transaction coordinator, recovered
  after that broker was stopped during an open transaction, and verified the
  committed record through `ReadCommitted`. Ambiguous outcomes, fencing, and
  throughput workloads remain separate 1.0 gates.
- The published secured transaction workflow passed transaction coordinator
  failover with `ReadCommitted` verification for Kafka 3.7.2 in
  [`31741012713`](https://github.com/TaeeunKil/kafrust/actions/runs/31741012713)
  and Kafka 4.3.1 in
  [`31741137784`](https://github.com/TaeeunKil/kafrust/actions/runs/31741137784).
  Both fresh external projects opened and committed through SASL_SSL/SCRAM-
  SHA-256 after stopping the discovered coordinator. Ambiguous outcomes,
  fencing, repeated faults, and throughput remain separate 1.0 gates.
- The published restricted Admin authorization workflow passed for Kafka 3.7.2
  in [`31741997691`](https://github.com/TaeeunKil/kafrust/actions/runs/31741997691)
  and Kafka 4.3.1 in
  [`31742115305`](https://github.com/TaeeunKil/kafrust/actions/runs/31742115305).
  Fresh external `0.2.28` projects authenticated as a non-superuser over
  SASL_SSL/SCRAM-SHA-256, completed allowed cluster/topic/producer/consumer/group
  operations, and preserved denied topic-config, create-topic, and delete-topic
  outcomes. This closes the representative published StandardAuthorizer
  permission gate; every ACL pattern, Admin API, provider, and mutation-failure
  workload remains outside the 1.0 claim.
- The published restricted Admin mutation and offset-management workflow passed
  for Kafka 3.7.2 in
  [`31742788549`](https://github.com/TaeeunKil/kafrust/actions/runs/31742788549)
  and Kafka 4.3.1 in
  [`31742924984`](https://github.com/TaeeunKil/kafrust/actions/runs/31742924984).
  External `0.2.28` projects authenticated the restricted user, altered an
  allowed topic config, preserved a denied config mutation, committed and
  listed a group offset, reset it through Admin OffsetCommit v2, and consumed
  from the reset position after rejoin. This closes the representative
  published mutation/offset gate; every Admin mutation, ACL pattern, provider,
  and ambiguous failure workload remains outside the 1.0 claim.
- The published performance qualification then passed all four matrix profiles
  in [`31744206188`](https://github.com/TaeeunKil/kafrust/actions/runs/31744206188):
  Kafka 3.7.2 and 4.3.1 with no compression and Zstd. Fresh external `0.2.28`
  projects produced and consumed 10,000 1-KiB records in batches of 200,
  measured batch p50/p95/p99 latency, and ended with zero retries and zero
  in-flight or buffered records. Producer throughput ranged from 43.7k to
  48.9k records/s and consumer throughput from 210.6k to 268.3k records/s.
  This is a published baseline for repeatability, not production SLO or
  long-running soak evidence.
- The published direct comparison workflow
  [`31753172293`](https://github.com/TaeeunKil/kafrust/actions/runs/31753172293)
  passed a fresh external `kafrust 0.2.28` versus `rust-rdkafka 0.39.0` project
  against Kafka 4.3.1. Both used fresh one-partition topics, 2,000 1-KiB
  records, and batches of 100. Kafrust measured 51,834 producer and 129,875
  consumer records/s; rust-rdkafka measured 48,452 producer and 252,306
  consumer records/s. This closes the direct benchmark evidence gap, but not
  API/feature parity, production SLO, or universal performance claims.
- The current published `kafrust 0.3.4` comparison passed in
  [`32407748417`](https://github.com/TaeeunKil/kafrust/actions/runs/32407748417).
  Across three repetitions of the 20,000-record, 1-KiB, batch-size-200
  Kafka 4.3.1 profile, kafrust reached median throughput of 62,392.59 producer
  and 330,812.61 consumer records/s, while `rust-rdkafka 0.39.0` reached
  149,516.77 producer and 580,226.56 consumer records/s. This is the current
  published workload baseline; feature parity, failure compatibility, and
  production SLO gates remain open.
- The published single-node soak gate then passed in
  [`31744827441`](https://github.com/TaeeunKil/kafrust/actions/runs/31744827441).
  A fresh external `0.2.28` project ran for 120 seconds against Kafka 4.3.1,
  survived a ten-second broker outage, reconciled 7,229,000 records, and ended
  with `recovered=true` plus zero in-flight and buffered records. The remaining
  claim is deliberately narrow: this does not establish multi-broker soak,
  production SLOs, or service canary readiness.
- The published simultaneous-loss gate then passed in
  [`31748293446`](https://github.com/TaeeunKil/kafrust/actions/runs/31748293446).
  A fresh external `0.2.28` project survived simultaneous ten-second outages
  of brokers 1 and 2 in a three-broker Kafka 4.3.1 cluster, reconciled
  4,423,200 records across three replicated partitions, and ended with zero
  in-flight and buffered records. Secured simultaneous loss, production SLOs,
  and service canary readiness remain open.
- The published secured simultaneous-loss gate then passed in
  [`31750274774`](https://github.com/TaeeunKil/kafrust/actions/runs/31750274774).
  A fresh external `0.2.28` project with `tls` survived simultaneous
  ten-second outages of brokers 1 and 2 in Kafka 4.3.1 SASL_SSL/SCRAM, using
  `Acks::All` and `min.insync.replicas=2`. It reconciled 2,704,200 successfully
  acknowledged records, recorded the expected write rejections while the
  cluster had only one in-sync broker, then recovered with zero in-flight and
  buffered records. Unclean-election data loss, production SLOs, and service
  canary readiness remain separate gates.
- The published multi-broker soak gate then passed in
  [`31746182158`](https://github.com/TaeeunKil/kafrust/actions/runs/31746182158).
  A fresh external `0.2.28` project survived a ten-second broker outage in a
  three-broker Kafka 4.3.1 cluster, reconciled 4,918,800 records across three
  replicated partitions, and ended with zero in-flight and buffered records.
  The remaining 1.0 evidence still includes secured multi-broker soak,
  simultaneous broker loss, production SLOs, and service canary readiness.
- The published secured multi-broker soak gate then passed in
  [`31747389166`](https://github.com/TaeeunKil/kafrust/actions/runs/31747389166).
  A fresh external `0.2.28` project with `tls` survived a ten-second broker
  outage in a three-broker Kafka 4.3.1 SASL_SSL/SCRAM cluster, reconciled
  2,288,700 records across three replicated partitions, and ended with zero
  in-flight and buffered records. Simultaneous broker loss, direct
  rust-rdkafka comparison, production SLOs, and service canary readiness remain
  open.
- The published secured multi-broker workflow passed both representative
  security and group-protocol combinations. Kafka 3.7.2 classic passed in
  [`31738997447`](https://github.com/TaeeunKil/kafrust/actions/runs/31738997447),
  and Kafka 4.3.1 KIP-848 passed in
  [`31739154764`](https://github.com/TaeeunKil/kafrust/actions/runs/31739154764).
  Fresh external projects resolved `kafrust 0.2.28` with `tls`, validated all
  three SASL_SSL listeners, authenticated Admin/producer/group operations with
  SCRAM-SHA-256, and recovered after the selected partition leader stopped.
  This closes one published secured leader-failover gate; coordinator-plus-
  leader colocation, broader security mechanisms, and workload/fault matrices
  remain required before the M21 1.0 replacement claim.
- The same published workflow then passed the secured coordinator-plus-
  partition-leader combined fault for Kafka 3.7.2 classic in
  [`31739763944`](https://github.com/TaeeunKil/kafrust/actions/runs/31739763944)
  and Kafka 4.3.1 KIP-848 in
  [`31739927915`](https://github.com/TaeeunKil/kafrust/actions/runs/31739927915).
  Each run listed the active group's coordinator, selected a partition led by
  that broker, stopped it, and verified authenticated producer recovery plus
  same-group post-failover consumption. Repeated faults, broader security
  mechanisms, and the complete 1.0 failure matrix remain open.
- The published secured repeated-leader workflow passed two sequential
  partition-leader failures for Kafka 3.7.2 classic in
  [`31743322062`](https://github.com/TaeeunKil/kafrust/actions/runs/31743322062)
  and Kafka 4.3.1 KIP-848 in
  [`31743497415`](https://github.com/TaeeunKil/kafrust/actions/runs/31743497415).
  Each external project recovered after broker 1 stopped, restarted it, then
  recovered again after a different partition leader stopped. This closes the
  published secured repeated-leader gate; unclean election, simultaneous loss,
  every security mechanism, and the complete 1.0 fault matrix remain open.
- The complete 17-job run [`31719615947`](https://github.com/TaeeunKil/kafrust/actions/runs/31719615947)
  adds a controlled combined-fault gate in the Kafka 3.7.2 three-broker
  plaintext profile. It deliberately colocates the classic group coordinator
  and target partition leader, stops that broker, writes a post-failover
  record through the replacement leader, and verifies group rejoin plus
  consumption of that record. Broader combined-fault combinations remain
  separate gates; the KIP-848 plaintext and secured paths are recorded in the
  subsequent current-main qualification entries above.
- The complete 17-job run
  [`31723663771`](https://github.com/TaeeunKil/kafrust/actions/runs/31723663771)
  extends the combined-fault gate to Kafka 4.3.1 plaintext KIP-848. The
  protocol-selectable combined example colocates the KIP-848 group coordinator
  and target partition leader, stops that broker, produces through the
  replacement leader, and verifies group rejoin plus post-failover record
  consumption. The same run also keeps the Kafka 3.7.2 classic group gate
  green after its check was narrowed to the observable post-failover record;
  direct assigned-consumer leader-epoch marker coverage remains a separate
  gate. Secured combined faults are covered by the subsequent current-main
  qualification entry; broader fault matrices remain unclaimed.
- Classic and KIP-848 consumer-group polling now have live leader-epoch
  recovery gates. The complete matrix in
  [`31702236760`](https://github.com/TaeeunKil/kafrust/actions/runs/31702236760)
  kept the Kafka 3.7.2 classic group session alive through a broker stop and
  verified assigned-consumer OffsetForLeaderEpoch recovery in job
  [`94453938654`](https://github.com/TaeeunKil/kafrust/actions/runs/31702236760/job/94453938654)
  with an epoch transition from 3 to 4. The Kafka 4.3.1 three-broker KIP-848
  job also passed the corresponding gate in
  [`94453938633`](https://github.com/TaeeunKil/kafrust/actions/runs/31702236760/job/94453938633)
  with an epoch transition from 0 to 1. The same gate also passed over Kafka
  4.3.1 `SASL_PLAINTEXT` in job
  [`94459402338`](https://github.com/TaeeunKil/kafrust/actions/runs/31703868759/job/94459402338)
  with epoch 2 to 3 and over `SASL_SSL` with SCRAM-SHA-256 in job
  [`94459402266`](https://github.com/TaeeunKil/kafrust/actions/runs/31703868759/job/94459402266)
  with epoch 1 to 2. The follow-up complete 17-job matrix passed at commit
  `9e53941` in
  [`31703868759`](https://github.com/TaeeunKil/kafrust/actions/runs/31703868759).
  Broader fault combinations, arbitrary retention timing, and unclean-election
  data-loss scenarios remain separate 1.0 gates.
