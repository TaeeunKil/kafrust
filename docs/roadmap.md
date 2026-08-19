# Roadmap

kafrust milestones are ordered by implementation risk and user-visible value. The project should keep Kafka concepts familiar to existing Kafka users while building a native Rust implementation underneath.

See [Project Strategy](project-strategy.md) for the replacement target, non-goals, existing alternatives, completion tiers, and the rationale for building a pure Rust client instead of wrapping librdkafka.

Status legend:

- Done: implemented and covered by CI.
- Implemented: code and examples exist, with live-broker verification outside default PR CI.
- Published: released on crates.io with release artifacts and post-release checks.
- In progress: useful slices exist, but exit criteria are not fully met.
- Planned: not started.

## Current Release Qualification

`0.3.0` is published on crates.io in protocol-first order. The complete
post-publish seven-profile external smoke passed in
[`31770895344`](https://github.com/TaeeunKil/kafrust/actions/runs/31770895344)
against Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2
SASL_SSL/SCRAM, and Gzip/Snappy/LZ4/Zstd paths. The release also includes the
typed Admin mutation ambiguity contract and its current-source response-drop
qualification.

The current-source `Live Kafka Smoke` matrix passed on commit `ef766cd` in
[`32221883090`](https://github.com/TaeeunKil/kafrust/actions/runs/32221883090).
This run covered Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 profiles, plaintext,
TLS, SASL/PLAIN, SASL_SSL/SCRAM, OAUTHBEARER, ACL authorization,
multi-broker failover, transaction reconciliation, and KIP-848 paths. The
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

The published `0.3.0` artifact also passed a 600-second Kafka 4.3.1
three-broker soak with brokers 1 and 2 stopped simultaneously in
[`32230130048`](https://github.com/TaeeunKil/kafrust/actions/runs/32230130048).
The run processed 27,810,300 records with five failed requests and 2,475
retries, recovered successfully, and ended with zero in-flight requests and
zero buffered records. This closes the published simultaneous-loss soak
slice; repeated runs, secured variants, and longer-duration evidence remain
open.

The verification-hardening slice now includes a standalone pure-Rust fuzz
workspace with six libFuzzer targets covering primitive/flexible decoding,
framing, classic and modern group descriptions, share-group offsets, and all
five supported compression codecs. The targets compile with the repository
MSRV after pinning their fuzz-only build dependencies; crash corpus reduction,
scheduled campaigns, and fault-injecting broker coverage remain open.

The first KIP-714 broker-side qualification slice passed in
[`32229640441`](https://github.com/TaeeunKil/kafrust/actions/runs/32229640441)
through `live-telemetry.yml`: a Kafka 3.7.2 KRaft image builds the test-only
`KafrustTelemetryReporter`, creates a `client-metrics` subscription, and checks
that kafrust sends both ordinary and terminating payloads. Subscription
mutation, throttling, unknown-subscription recovery, and broker payload-limit
qualification remain open follow-up gates.

The published group smoke now also verifies normal member departure recovery:
after two-member assignment and explicit position rejoin, the remaining
member must reacquire all six partitions and leave cleanly. The gate passed
with published `0.3.0` on Kafka 3.7.2 classic in
[`32231354623`](https://github.com/TaeeunKil/kafrust/actions/runs/32231354623)
and Kafka 4.3.1 KIP-848 in
[`32231357426`](https://github.com/TaeeunKil/kafrust/actions/runs/32231357426).
Abrupt member-loss expiry, committed-offset restoration after loss, and
long-duration group churn remain open.

The published `0.2.30` artifacts were also exercised from fresh external
projects in the seven-profile `Published Crate Smoke` run
[`31762679537`](https://github.com/TaeeunKil/kafrust/actions/runs/31762679537).
Both published docs.rs pages returned HTTP 200 for
[`kafrust 0.2.30`](https://docs.rs/kafrust/0.2.30/kafrust/) and
[`kafrust-protocol 0.2.30`](https://docs.rs/kafrust-protocol/0.2.30/kafrust_protocol/).

## 0.3 Release Target

Status: Published as `0.3.0`; post-publish external smoke passed and both
docs.rs pages are green.

`0.3.0` is the next meaningful client milestone, not the complete Kafka
replacement claim. It is intended to move the current alpha from broad feature
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

- Stable `ShareGroupDescribe` v1 (API key 77) is now implemented through the
  typed protocol, low-level `Client`, and coordinator-aware `AdminClient`
  layers. The public result preserves share-group state and epochs, member
  details, subscribed topics, assignments, topic UUIDs, and authorization
  bits. Focused wire, injected-broker, and Admin routing tests pass; the Kafka
  4.3.1 live ShareConsumer smoke now also inspects its active group through
  this API in [`32223573332`](https://github.com/TaeeunKil/kafrust/actions/runs/32223573332).
  Share Group offset mutation APIs 91 and 92 are also implemented with typed
  top-level and per-topic/per-partition results; the Kafka 4.3.1 set/delete
  gate passed in [`32224302754`](https://github.com/TaeeunKil/kafrust/actions/runs/32224302754).
   Share Group offset listing through API 90 v0/v1 and intent-specific group
   deletion through API 42 are now implemented with focused wire and
   coordinator-routing tests. The combined Kafka 4.3.1 live lifecycle gate
   passed in [`32225957928`](https://github.com/TaeeunKil/kafrust/actions/runs/32225957928);
   long-running operational evidence remains open.
- A standalone `fuzz/` workspace now provides six libFuzzer targets and a
  manual/weekly compile-and-smoke workflow. The public bounded compression helpers have
  an all-codec roundtrip regression test. This closes the initial fuzz-harness
  scaffolding slice; corpus growth, minimized crash regressions, and sustained
  campaigns remain part of the production-hardening gate.
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
   Remaining modern gaps include live qualification of the Share Group Admin
   lifecycle, client telemetry/KIP-714, and broader Admin/controller protocol
   coverage.
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
- Focused unit tests cover buffered enqueue, delivery cancellation, pending delivery failure, per-record delivery completion, defensive handling for missing batch outcomes, and flush trigger selection.

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
- Provider-backed OAUTHBEARER connections also refresh the token and send
  flexible `SaslAuthenticate v2` again on the existing connection before
  requests after half of the broker-advertised session lifetime has elapsed;
  the focused client test covers this lifecycle.
- `SaslAuthenticate v1` responses remain decoded for PLAIN and SCRAM, while
  flexible `v2` responses are used for OAUTHBEARER. `Client::sasl_session_lifetime_ms`
  exposes the broker's re-authentication window. Provider-backed OAUTHBEARER
  connections use that window to re-authenticate on the existing connection
  before requests after half the lifetime; detached refresh workers and
  production provider policy remain open.
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
- The signed local OIDC/JWKS fixture passed Kafka's validator, the Java Kafka
  client, and kafrust static and provider-backed paths in the OIDC job
  [`31584760474`](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94078116567).
  OAUTHBEARER initial authentication and provider re-authentication use
  flexible `SaslAuthenticate v2`; PLAIN and SCRAM remain on `v1`. Detached
  refresh workers and external provider-specific OAuth/OIDC qualification
  remain open.

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
- KIP-848 member-aware administrative offsets now use OffsetFetch v9 and
  OffsetCommit v9 with the joined member ID, member epoch, optional static
  instance ID, `require_stable`, and committed leader epoch. The APIs reuse
  the typed classic offset results while preserving v9 throttle and group
  errors. Focused wire/mock tests and the
  `admin_consumer_group_offsets_member` example cover the active-member path;
  Kafka 4.3.1 single-node and multi-broker PLAINTEXT, SASL_PLAINTEXT, and
  SASL_SSL/SCRAM live qualification passed in
  [`31607006237`](https://github.com/TaeeunKil/kafrust/actions/runs/31607006237).
  Target authorization and broader member-failure workloads remain release
  gates. The live DeleteRecords, DescribeProducers, DescribeTransactions,
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

- `InitProducerId v0` request/response protocol types and the low-level client
  roundtrip are implemented with byte-level and injected-broker tests.
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

- `EndTxn v0` request and response protocol types encode commit and abort
  results using Kafka API key 26 and decode coordinator throttle/error fields.
- `Client::end_txn_v0` provides the low-level framed roundtrip, covered by
  byte-level commit/abort tests and an injected-broker response test.
- `FindCoordinator v1` now exposes transaction coordinator discovery using
  coordinator type 1, with protocol and injected-broker client coverage.
- `AddPartitionsToTxn v0` request/response types preserve topic-partition
  registration and partition-scoped broker errors. The low-level client
  roundtrip is covered by byte-level and injected-broker tests.
- `AddOffsetsToTxn v0` encodes the transactional producer identity and target
  consumer group, with low-level client and injected-broker coverage for
  coordinator errors.
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
- Transactional sends register each topic partition through
  `AddPartitionsToTxn v0`, pass the transactional ID to Produce v3/v7, and
  complete through `EndTxn v0`. Transactional Produce requests set the
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
  `ConsumerGroup::metadata` and assignments through `AddOffsetsToTxn v0` and
  commits offsets through generation-fenced `TxnOffsetCommit v3` before
  EndTxn. Transaction
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
  failed, timed-out, cancelled, and in-flight request roundtrips, request and
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
  bucket Kafka request p50/p95/p99 upper-bound estimates, request counts, and
  retries. The manual `Kafka Benchmark` workflow runs selected payload and
  compression profiles against Kafka 4.3.1 and uploads JSONL results for
  comparison.
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

- kafrust can replace an existing Kafka client dependency for representative producer-only, consumer-only, consumer group, admin, secured, compressed, idempotent, and transactional workloads
- default docs direct production users to supported broker profiles instead of broad unsupported claims
- `docs.rs` builds are green for every release candidate
- fresh external projects compile and run documented examples from published crates
- live compatibility workflows pass for every supported broker/security profile before release
- public APIs have clear stability guarantees and migration notes

Non-goal:

- This milestone does not replace Apache Kafka brokers, controllers, storage, replication, or server-side group coordination.

Strategic role:

- This is the "complete replacement" target for Kafka client dependencies in Rust applications.

Implemented evidence:

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
  only to a stale address. Lost ShareAcknowledge responses are classified as a
  typed unknown outcome and are never replayed automatically. The Kafka 4.3.1
  single-node live gate passed the complete poll/Renew/poll,
  acquisition-lock expiry/redelivery, Accept/commit, and close path in
  [`32213499877`](https://github.com/TaeeunKil/kafrust/actions/runs/32213499877).
  One three-broker leader-movement path, three independent active-heartbeat
  coordinator recovery attempts, and three consecutive in-process coordinator
  churn cycles are now live-qualified; ambiguous acknowledgement
  reconciliation remains open.
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
  including ordinary and terminating payload delivery. Subscription
  mutation, throttling, unknown-subscription recovery, broker payload limits,
  and longer telemetry collection remain open hardening gates.
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
- ShareFetch success responses preserve the broker that served the request,
  while `CurrentLeader` is used only for the leader-error responses where Kafka
  populates it. Retryable ShareFetch leader errors return the connection to the
  pool, refresh metadata, and retry with refreshed routing. Injected tests cover
  the response semantics; the three-broker leader movement workflow passed in
  run [`32214201983`](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983),
  while acknowledgement reconciliation and soak remain open. The live runs
  exposed and fixed stale broker-connection reuse,
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
  set contains that member; the resulting workflow run remains the live
  qualification gate.
- The 2026-08-19 competitor recheck adds `kacrab` to the comparison set. Its
  published `0.4.0` docs claim Kafka 4.3 producer, consumer, share-consumer,
  and 62-operation Admin parity with a broker-matrix and fuzzing posture;
  `krafka` remains ahead in modern protocol breadth and test infrastructure.
  Kafrust's differentiator remains a Kafka 3.7-to-current compatibility target
  with a pure-Rust default codec and no librdkafka dependency, but that is not
  a substitute for the competitors' missing live and long-duration evidence.
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
  initial authentication and provider re-authentication, and sends Kafka's
  control-A acknowledgement after an error challenge. Injected broker tests
  cover handshake ordering, exact authentication bytes, and error challenge
  acknowledgement; the signed OIDC live job above adds Kafka 3.7.2 coverage.
  Async token providers are covered by injected connection tests; external
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
- KIP-848 `ConsumerGroupHeartbeat v0` protocol types, Metadata v12 UUID
  mappings, and a selectable high-level foreground group path are implemented
  with assignment application, member-epoch heartbeats/rejoin, OffsetFetch v9,
  OffsetCommit v9, explicit leave, and injected low-level roundtrip coverage.
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
