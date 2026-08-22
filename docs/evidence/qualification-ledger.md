# Qualification Ledger

This ledger is the immutable evidence index for the v1 program. Each `Q-*`
section is one result, not a claim that all neighboring broker, security, or
workload combinations pass. Historical prose remains in
[Compatibility](../compatibility.md); rows are added here when a result is
imported or produced by a v1 gate.

The checker requires every row to carry the following fields:

`date_utc`, `source_commit`, `client_version`, `protocol_version`,
`work_status`, `evidence_level`, `kafka_version`, `kafka_image`, `mode`,
`topology`, `security`, `group_protocol`, `workload`, `workflow`, `fault`,
`duration`, `record_count`, `member_count`, `repetition_count`,
`expected_errors`, `observed_errors`, `retry_count`, `duplicate_count`,
`loss_count`, `latency`, `memory`, `final_resource_gauges`, `result`,
`artifact`, and `non_claims`.

The accepted status and evidence vocabularies are defined in
[`check_qualification_ledger.py`](../../scripts/check_qualification_ledger.py)
and are checked in CI. Values such as `not-applicable` and `not-recorded` are
deliberate classifications, not missing data. A row must never use an
unqualified relative artifact label.

## Q-CI-V118-001

- date_utc: 2026-08-22
- source_commit: 6bcf1efd852977be20f761e5dbddc5ca4bea4fab
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: In progress
- evidence_level: CI
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: not-applicable
- security: not-applicable
- group_protocol: not-applicable
- workload: ten-target nightly libFuzzer discovery smoke
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32555867720
- fault: malformed and bounded corpus inputs; no broker fault injection
- duration: 30s per target; workflow duration not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: no crash, timeout, OOM, or sanitizer failure
- observed_errors: none
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: RSS cap 2048 MB per target
- final_resource_gauges: libFuzzer final statistics retained in artifact
- result: passed
- artifact: GitHub Actions run 32555867720; kafrust-fuzz-32555867720 artifact
- non_claims: not 3600-second qualification per target, not four-shard qualification, not four weekly campaigns, not absence-of-bugs evidence

## Q-V100-001

- date_utc: 2026-08-22
- source_commit: 3e12192bbef3a01b2a1310979131115c9c7ecd69
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Done
- evidence_level: Packaged candidate
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: not-applicable
- security: not-applicable
- group_protocol: not-applicable
- workload: package assembly, archive inspection, and five external feature profiles
- workflow: scripts/verify_package_boundary.py --staged
- fault: package-only fixture has no workspace source dependency
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: none
- observed_errors: none
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: not-applicable
- result: passed
- artifact: kafrust-protocol-0.3.6.crate sha256 f12e95a30ce46fd7ffc097a97a31b0a918bcee9f83cefb72fe2484cfe9c255cc; kafrust-0.3.6.crate sha256 2ae1a135d3de7f00fb25455809ab9fc201ea41c398aa62ac14f34c2a2758fca9
- non_claims: not crates.io publication, not broker compatibility, not service canary

## Q-V100-REG-001

- date_utc: 2026-08-22
- source_commit: 3e12192bbef3a01b2a1310979131115c9c7ecd69
- client_version: 0.3.6
- protocol_version: 0.3.5
- work_status: Done
- evidence_level: CI
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: not-applicable
- security: not-applicable
- group_protocol: not-applicable
- workload: external compiler import regression for four transaction type families
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32545563612
- fault: registry protocol 0.3.5 lacks eight source types required by the client
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: eight unresolved imports across four transaction families
- observed_errors: eight unresolved imports across four transaction families
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: not-applicable
- result: passed
- artifact: crates.io kafrust-protocol 0.3.5
- non_claims: not a runtime behavior result, not a publication authorization

## Q-CI-1242-001

- date_utc: 2026-08-22
- source_commit: 3e12192bbef3a01b2a1310979131115c9c7ecd69
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Done
- evidence_level: CI
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: not-applicable
- security: not-applicable
- group_protocol: not-applicable
- workload: repository validation plus package boundary on Rust 1.81.0 and stable
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32545563612
- fault: none
- duration: 2 matrix jobs completed
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 2
- expected_errors: none
- observed_errors: none
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: package profiles passed
- result: passed
- artifact: CI logs retain package hashes; no release artifact retained
- non_claims: not crates.io publication, not live broker qualification, not service canary

## Q-PUB-OAUTH-001

- date_utc: 2026-08-20
- source_commit: e1a306880d967373e001a69e48d774d57dde16ed
- client_version: 0.3.5
- protocol_version: 0.3.5
- work_status: Done
- evidence_level: Published artifact
- kafka_version: 3.7.2
- kafka_image: apache/kafka:3.7.2
- mode: KRaft
- topology: single-node
- security: SASL_SSL with signed OAUTHBEARER and local OIDC/JWKS validator
- group_protocol: not-applicable
- workload: authentication, produce/readback, and same-connection SASL re-authentication
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32420723537
- fault: broker session lifetime threshold triggered re-authentication
- duration: 101s
- record_count: not-recorded
- member_count: not-applicable
- repetition_count: 1
- expected_errors: none
- observed_errors: none
- retry_count: not-recorded
- duplicate_count: none observed
- loss_count: none observed
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: not-recorded
- result: passed
- artifact: crates.io kafrust 0.3.5 and kafrust-protocol 0.3.5
- non_claims: not external identity-provider discovery, not key rotation, not provider outage policy

## Q-PUB-SMOKE-001

- date_utc: 2026-08-20
- source_commit: ba09cecbbcc16bbae74b6404b91d533715467776
- client_version: 0.3.5
- protocol_version: 0.3.5
- work_status: Done
- evidence_level: Published artifact
- kafka_version: 3.7.2 and 4.3.1
- kafka_image: apache/kafka:3.7.2 and apache/kafka:4.3.1
- mode: KRaft
- topology: single-node
- security: PLAINTEXT and SASL_SSL with SCRAM-SHA-256
- group_protocol: classic and KIP-848 consumer
- workload: producer, direct consumer, group, transactions/read-committed, TLS/SCRAM, and four codecs
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32420987547
- fault: none
- duration: 81s
- record_count: not-recorded
- member_count: not-recorded
- repetition_count: 7 profiles
- expected_errors: none
- observed_errors: none
- retry_count: not-recorded
- duplicate_count: not-recorded
- loss_count: not-recorded
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: lockfile versions verified
- result: passed
- artifact: crates.io kafrust 0.3.5 and kafrust-protocol 0.3.5
- non_claims: not complete API parity, not full fault matrix, not production SLO

## Q-PUB-SOAK-001

- date_utc: 2026-08-21
- source_commit: b6602c9beff3a82aa6c2f0ae8b80f31b39772320
- client_version: 0.3.5
- protocol_version: 0.3.5
- work_status: Done
- evidence_level: Published artifact
- kafka_version: 4.3.1
- kafka_image: apache/kafka:4.3.1
- mode: KRaft
- topology: three-broker
- security: SASL_SSL with SCRAM-SHA-256
- group_protocol: not-applicable
- workload: secured multi-broker recovery soak
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32440677496
- fault: simultaneous ten-second stop of brokers 1 and 2
- duration: 600s
- record_count: not-recorded
- member_count: not-applicable
- repetition_count: 1
- expected_errors: broker-loss operation errors allowed by the fixture
- observed_errors: recovery completed with zero final in-flight and buffered records
- retry_count: not-recorded
- duplicate_count: not-recorded
- loss_count: no unaccounted acknowledged loss observed
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: in_flight_requests=0; buffered_records=0
- result: passed
- artifact: crates.io kafrust 0.3.5 and kafrust-protocol 0.3.5
- non_claims: not unclean-election data-loss qualification, not production SLO, not service canary

## Q-LIVE-MATRIX-001

- date_utc: 2026-08-20
- source_commit: cb4875f344f34f97d2806ee8fbed25bc10149d16
- client_version: 0.3.5
- protocol_version: 0.3.5
- work_status: Done
- evidence_level: Live current-source
- kafka_version: 3.7.2, 3.8.1, 3.9.1, and 4.3.1
- kafka_image: apache/kafka:3.7.2, apache/kafka:3.8.1, apache/kafka:3.9.1, and apache/kafka:4.3.1
- mode: KRaft
- topology: single-node and three-broker
- security: PLAINTEXT, TLS, SASL/PLAIN, SASL_SSL with SCRAM-SHA-256, and OAUTHBEARER
- group_protocol: classic and KIP-848 consumer
- workload: live compatibility matrix covering data-plane, groups, Admin, security, and failover slices
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32382586220
- fault: selected broker and coordinator loss in named profiles
- duration: not-recorded
- record_count: not-recorded
- member_count: not-recorded
- repetition_count: not-recorded
- expected_errors: fixture-specific transient broker errors
- observed_errors: named live gates passed
- retry_count: not-recorded
- duplicate_count: not-recorded
- loss_count: not-recorded
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: not-recorded
- result: passed
- artifact: workspace source at the recorded commit
- non_claims: not published artifact evidence, not universal Kafka compatibility, not service canary

## Q-CI-1249-001

- date_utc: 2026-08-22
- source_commit: 5571ca36558c4757b811171d7a5d0d0a487333ae
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: CI
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: not-applicable
- security: not-applicable
- group_protocol: not-applicable
- workload: Rust formatting, package boundary, protocol audits, full tests, documentation, and clippy on stable and Rust 1.81.0
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32548809314
- fault: none
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 2
- expected_errors: none
- observed_errors: none
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: package boundary and documentation checks passed
- result: passed
- artifact: pushed workspace commit 5571ca36558c4757b811171d7a5d0d0a487333ae
- non_claims: not published crates, not live broker qualification, not service canary, not completion of V1-03 through V1-05

## Q-LOCAL-V108-001

- date_utc: 2026-08-22
- source_commit: 8a29d1eff1bd6d9fa80526be79f7e1ec99430075
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: scripted broker fixtures
- kafka_image: not-applicable
- mode: not-applicable
- topology: single scripted coordinator
- security: not-applicable
- group_protocol: classic
- workload: direct OffsetCommit response-loss classification with exact offset identity
- workflow: scripts/check_qualification_ledger.py
- fault: coordinator drops the transmitted OffsetCommit v2 response
- duration: 0.01s
- record_count: not-applicable
- member_count: 1
- repetition_count: 1
- expected_errors: ConsumerGroupCommitOutcomeUnknown; zero retries
- observed_errors: typed group/member/generation plus exact orders-0 next offset; zero retries
- retry_count: 0
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: scripted broker completed; no replay frame observed
- result: passed
- artifact: workspace source commit 8a29d1eff1bd6d9fa80526be79f7e1ec99430075
- non_claims: not CI, not live broker compatibility, not published artifact, not churn or data-loss qualification

## Q-LOCAL-V109-001

- date_utc: 2026-08-22
- source_commit: b54969655af6f309a457e3dc547bd47c6a0c4cdd
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: scripted broker fixtures
- kafka_image: not-applicable
- mode: not-applicable
- topology: single scripted coordinator plus partition leader
- security: not-applicable
- group_protocol: KIP-848 consumer
- workload: epoch rejoin, UUID offset restoration, and member identity stability
- workflow: scripts/check_qualification_ledger.py
- fault: coordinator heartbeat returns rebalance errors and reconnects before assignment restoration
- duration: 0.14s
- record_count: 1
- member_count: 1
- repetition_count: 1
- expected_errors: transient rebalance response; no terminal error
- observed_errors: member ID stable across rejoin; generation 2 and record position restored
- retry_count: 1
- duplicate_count: 0
- loss_count: 0
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: scripted broker completed; no stale assignment observed
- result: passed
- artifact: workspace source commit b54969655af6f309a457e3dc547bd47c6a0c4cdd
- non_claims: not CI, not live broker compatibility, not published artifact, not 40-cycle churn or data-loss qualification

## Q-LOCAL-V110-001

- date_utc: 2026-08-22
- source_commit: 55369e4abbda4ee5dfe3aed9774434b3799c8065
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: scripted broker fixtures
- kafka_image: not-applicable
- mode: not-applicable
- topology: scripted Share coordinator and leader
- security: not-applicable
- group_protocol: Share v1
- workload: lost Release acknowledgement, session reconciliation, redelivery, and member identity stability
- workflow: scripts/check_qualification_ledger.py
- fault: ShareAcknowledge response is dropped after transmission
- duration: 0.01s
- record_count: 1
- member_count: 1
- repetition_count: 1
- expected_errors: ShareAcknowledgementOutcomeUnknown before reconciliation; redelivery after session reset
- observed_errors: typed unknown state retained, one redelivered record, stable member ID, no replayed acknowledgement
- retry_count: 0
- duplicate_count: 0
- loss_count: 0
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: affected Share session discarded; pending unknown count cleared after redelivery
- result: passed
- artifact: workspace source commit 55369e4abbda4ee5dfe3aed9774434b3799c8065
- non_claims: not CI, not live broker compatibility, not published artifact, not exactly-once or 10,000-record qualification

## Q-LOCAL-V111-001

- date_utc: 2026-08-22
- source_commit: 413f0ffa568349fd468ed088fcddac0c2a80a139
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: injected broker streams
- kafka_image: not-applicable
- mode: not-applicable
- topology: controller-routed single listener
- security: not-applicable
- group_protocol: not-applicable
- workload: controller routing, partial results, pre-transmission retry, and mutation ambiguity classification
- workflow: scripts/check_qualification_ledger.py
- fault: controller discovery disconnect and post-transmission response loss
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: AdminMutationOutcomeUnknown only after possible transmission; no mutation replay
- observed_errors: typed unknown classification and retained per-resource errors in existing Admin fixtures
- retry_count: bounded pre-transmission only
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: controller request ownership and poisoned-connection disposal covered by tests
- result: passed
- artifact: workspace source commit 413f0ffa568349fd468ed088fcddac0c2a80a139
- non_claims: not CI, not live authorization/failover, not published artifact, not complete operation ledger

## Q-LOCAL-V112-001

- date_utc: 2026-08-22
- source_commit: 413f0ffa568349fd468ed088fcddac0c2a80a139
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: injected broker streams
- kafka_image: not-applicable
- mode: not-applicable
- topology: coordinator, leader, and broker-routed fixtures
- security: not-applicable
- group_protocol: classic, KIP-848, and Share member-aware paths
- workload: owner routing, v10/v9 identity fallback, partial results, and response-loss classes
- workflow: scripts/check_qualification_ledger.py
- fault: owner disconnect, coordinator movement, and resource-level broker errors
- duration: not-recorded
- record_count: not-applicable
- member_count: not-recorded
- repetition_count: 1
- expected_errors: typed owner/resource errors; unsafe writes not replayed
- observed_errors: route-specific responses and partial errors retained by existing Admin tests
- retry_count: bounded read/pre-send only
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: no shared session-bound member connection introduced
- result: passed
- artifact: workspace source commit 413f0ffa568349fd468ed088fcddac0c2a80a139
- non_claims: not CI, not live failover, not published artifact, not data-recovery qualification

## Q-LOCAL-V113-001

- date_utc: 2026-08-22
- source_commit: 413f0ffa568349fd468ed088fcddac0c2a80a139
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: injected broker streams
- kafka_image: not-applicable
- mode: not-applicable
- topology: controller/bootstrap security fixtures
- security: TLS, mTLS, SASL/PLAIN, SCRAM, and OAUTHBEARER validation paths
- group_protocol: not-applicable
- workload: security Admin routing, mixed results, redaction, and ambiguity classification
- workflow: scripts/check_qualification_ledger.py
- fault: authentication/configuration failure and possible post-send mutation loss
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: typed auth/authorization/unknown outcomes; no secret leakage
- observed_errors: redaction and partial-result tests passed; no credentials in diagnostics
- retry_count: bounded pre-send only
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: no secret material emitted by tested paths
- result: passed
- artifact: workspace source commit 413f0ffa568349fd468ed088fcddac0c2a80a139
- non_claims: not CI, not restricted-principal live qualification, not published artifact

## Q-LOCAL-V114-001

- date_utc: 2026-08-22
- source_commit: 413f0ffa568349fd468ed088fcddac0c2a80a139
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: protocol fixtures and injected broker streams
- kafka_image: not-applicable
- mode: not-applicable
- topology: controller/coordinator/worker-local fixtures
- security: not-applicable
- group_protocol: Streams, Share, and blocking adapter surfaces
- workload: advanced API classification, protocol bytes, task lifecycle, quorum routing, and runtime cleanup
- workflow: scripts/check_qualification_ledger.py
- fault: invalid assignment, nested runtime, state-session, and response-loss boundaries
- duration: not-recorded
- record_count: not-applicable
- member_count: not-recorded
- repetition_count: 1
- expected_errors: explicit experimental/unsupported/typed outcomes
- observed_errors: deterministic advanced-surface tests passed
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: blocking tasks joined; no detached runtime claim
- result: passed
- artifact: workspace source commit 413f0ffa568349fd468ed088fcddac0c2a80a139
- non_claims: not CI, not live retained-surface qualification, not stable core compatibility

## Q-LOCAL-V115-001

- date_utc: 2026-08-22
- source_commit: e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: scripted broker fixtures
- kafka_image: not-applicable
- mode: not-applicable
- topology: cache, direct-fetch, membership, telemetry, and adapter owners
- security: not-applicable
- group_protocol: classic, KIP-848, Share, and Streams lifecycle paths
- workload: session ownership, cache boundaries, task cancellation, and shutdown
- workflow: scripts/check_qualification_ledger.py
- fault: request timeout, heartbeat cancellation, unknown acknowledgement close, and nested runtime
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: poisoned connections and cancelled tasks do not resume under a stale owner
- observed_errors: deterministic owner-boundary tests passed; no replay frame observed
- retry_count: bounded by each fixture
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: not-recorded
- result: passed
- artifact: workspace source commit e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- non_claims: not 100-cycle gauge qualification, not published artifact, not secured churn

## Q-LOCAL-V116-001

- date_utc: 2026-08-22
- source_commit: e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: scripted broker and signed OIDC fixtures
- kafka_image: not-applicable
- mode: not-applicable
- topology: single authenticated connection
- security: TLS, mTLS, SASL/PLAIN, SCRAM-256/512, and OAUTHBEARER paths
- group_protocol: not-applicable
- workload: handshake, provider refresh, rotation window, outage, timeout, and redaction
- workflow: scripts/check_qualification_ledger.py
- fault: provider failure, expired token, invalid server-final, and incomplete TLS material
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: typed authentication/provider errors without credential material
- observed_errors: redaction, single-flight refresh, and connection-discard tests passed
- retry_count: bounded provider refresh only
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: not-recorded
- result: passed
- artifact: workspace source commit e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- non_claims: not seeded artifact scan, not rotation live gate, not published security matrix

## Q-LOCAL-V117-001

- date_utc: 2026-08-22
- source_commit: e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: injected telemetry broker
- kafka_image: not-applicable
- mode: not-applicable
- topology: one telemetry connection
- security: not-applicable
- group_protocol: not-applicable
- workload: bounded metric snapshot, OTLP delta/cumulative serialization, subscription, codec, and payload limits
- workflow: scripts/check_qualification_ledger.py
- fault: oversized payload, unsupported codec, subscription refresh, and provider boundary
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: TelemetryPayloadTooLarge before transmission
- observed_errors: filtered metrics and single-connection push tests passed
- retry_count: bounded subscription retry
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: not-recorded
- result: passed
- artifact: workspace source commit e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- non_claims: not 60-minute collection, not broker-replacement identity gate, not published telemetry support

## Q-LOCAL-V118-001

- date_utc: 2026-08-22
- source_commit: e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: protocol fixtures
- kafka_image: not-applicable
- mode: not-applicable
- topology: decoder and scripted broker boundaries
- security: not-applicable
- group_protocol: not-applicable
- workload: frame, collection, tagged-field, decompression, response-buffer, and queue limits
- workflow: scripts/check_qualification_ledger.py
- fault: malformed length, truncation, decompression expansion, and queue saturation
- duration: not-recorded
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: typed protocol limit/length errors before unbounded allocation
- observed_errors: boundary and compression-limit tests passed; ten fuzz targets compile
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-recorded
- memory: bounded by configured decoder/response limits in deterministic tests
- final_resource_gauges: not-recorded
- result: passed
- artifact: workspace source commit e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- non_claims: not 60-minute fuzz campaigns, not four weekly passes, not absence-of-bugs proof

## Q-PACKAGE-V119-001

- date_utc: 2026-08-22
- source_commit: 3e12192bbef3a01b2a1310979131115c9c7ecd69
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Packaged candidate
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: staged package and five external feature profiles
- security: not-applicable
- group_protocol: not-applicable
- workload: package boundary, MSRV/stable build, pure-Rust manifest, and feature isolation
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32545563612
- fault: workspace path dependency intentionally removed from external package checks
- duration: 2 matrix jobs completed
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 2
- expected_errors: no librdkafka/C client requirement in the default package
- observed_errors: package boundary passed on Rust 1.81.0 and stable
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: package profiles passed
- final_resource_gauges: package hashes retained in Q-V100-001
- result: passed
- artifact: kafrust-protocol-0.3.6.crate sha256 f12e95a30ce46fd7ffc097a97a31b0a918bcee9f83cefb72fe2484cfe9c255cc; kafrust-0.3.6.crate sha256 2ae1a135d3de7f00fb25455809ab9fc201ea41c398aa62ac14f34c2a2758fca9
- non_claims: not crates.io publication, not complete advisory/license/SBOM audit, not optional-TLS native-tool clearance

## Q-LOCAL-V120-001

- date_utc: 2026-08-22
- source_commit: e1f28c5dbb08e5d7ae499371b520873f56f31b8c
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: V1-01 accepted lines 3.7.2, 3.8.1, 3.9.1, 4.0.0, and 4.3.1
- kafka_image: manifest rows require pinned image evidence before freeze
- mode: KRaft
- topology: pairwise single-node, three-broker, and controller-listener profiles
- security: PLAINTEXT, TLS, mTLS, SASL/PLAIN, SCRAM, and OAUTHBEARER dimensions
- group_protocol: classic, KIP-848, Share, and not-applicable Admin/package rows
- workload: machine-readable compatibility matrix schema and profile checker
- workflow: scripts/check_v1_compatibility_matrix.py
- fault: malformed profile, duplicate ID, unsupported broker/topology, and policy drift
- duration: not-applicable
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: invalid matrix rows rejected before broker execution
- observed_errors: ten draft profiles and required floor/pinned/package IDs accepted
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: not-applicable
- result: passed
- artifact: docs/evidence/v1-20-compatibility-matrix.json
- non_claims: not published artifact, not fresh registry lockfile evidence, not matrix completion or publication authorization

## Q-LIVE-V120-001

- date_utc: 2026-08-22
- source_commit: e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Live current-source
- kafka_version: 3.7.2, 3.8.1, 3.9.1, and 4.3.1
- kafka_image: apache/kafka:3.7.2, apache/kafka:3.8.1, apache/kafka:3.9.1, and apache/kafka:4.3.1
- mode: KRaft
- topology: single-node and three-broker
- security: PLAINTEXT, TLS, SASL/PLAIN, SASL_SSL/SCRAM, OAUTHBEARER, and ACL authorization
- group_protocol: classic and KIP-848
- workload: Live Kafka Smoke source matrix with data-plane, groups, transactions, Admin, security, and failover jobs
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32551145773
- fault: selected leader, coordinator, and transaction-coordinator movement with reconnection
- duration: workflow-specific bounded smoke profiles
- record_count: workflow-specific
- member_count: workflow-specific
- repetition_count: 17 jobs
- expected_errors: fixture-specific transient broker and fencing outcomes
- observed_errors: all 17 jobs passed, including SCRAM transaction failover with code 47 or 90
- retry_count: workflow-specific
- duplicate_count: not-recorded
- loss_count: no unaccounted loss in the named smoke profiles
- latency: not-recorded
- memory: not-recorded
- final_resource_gauges: workflow-specific
- result: passed
- artifact: workspace source commit e6de5c5201f8c688bf5e3ca148d3c997cd8918f6
- non_claims: not published artifact, not full V1-20 matrix, not 100-cycle/long-soak/SLO evidence, not service canary

## Q-LOCAL-V118-002

- date_utc: 2026-08-22
- source_commit: d013d92107ce5b6c340ef0a1049a24f439fb0218
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: In progress
- evidence_level: Local deterministic
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: standalone nightly fuzz workspace
- security: not-applicable
- group_protocol: not-applicable
- workload: versioned discovery and qualification campaign manifest validation
- workflow: scripts/check_v1_fuzz_campaign_manifest.py
- fault: target-list drift, insufficient duration, insufficient timeout, or missing loop
- duration: not-applicable
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: manifest rejects a campaign incapable of delivering 60 minutes per target
- observed_errors: ten targets, 3,600-second qualification duration, four shards, and 70-minute timeout accepted
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-applicable
- memory: RSS cap declared at 2048 MiB
- final_resource_gauges: not-applicable
- result: passed
- artifact: docs/evidence/v1-18-fuzz-campaign-manifest.json
- non_claims: not actual 60-minute campaign, not four weekly passes, not crash/OOM absence proof

## Q-LOCAL-V121-001

- date_utc: 2026-08-22
- source_commit: 83a5c0d848dc52ba5d465011cbb5ea70557064f3
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Planned
- evidence_level: Local deterministic
- kafka_version: 3.7.2 and 4.3.1 campaign requirements
- kafka_image: image digests required at campaign execution
- mode: KRaft
- topology: three-broker
- security: PLAINTEXT and SASL_SSL/SCRAM-SHA-256
- group_protocol: classic, KIP-848, and Share
- workload: versioned fault/soak campaign manifest validation
- workflow: scripts/check_v1_fault_campaign_manifest.py
- fault: campaign identity, duration, cycle, ambiguity-family, and result-field drift
- duration: not-applicable
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: invalid campaign thresholds rejected before execution
- observed_errors: seven campaigns, four six-hour gates, 100-cycle, and 100-outcome requirements accepted
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: manifest requires final gauges in retained results
- result: passed
- artifact: docs/evidence/v1-21-fault-campaign-manifest.json
- non_claims: not an executed soak, not published artifact evidence, not data-loss or SLO completion

## Q-LOCAL-V122-001

- date_utc: 2026-08-22
- source_commit: 83a5c0d848dc52ba5d465011cbb5ea70557064f3
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Planned
- evidence_level: Local deterministic
- kafka_version: 4.3.1 campaign requirement
- kafka_image: runner and broker identity required at campaign execution
- mode: KRaft
- topology: single-node and three-broker
- security: PLAINTEXT and SASL_SSL/SCRAM-SHA-256
- group_protocol: not-applicable
- workload: versioned performance/SLO campaign manifest validation
- workflow: scripts/check_v1_performance_campaign_manifest.py
- fault: profile, timing, repetition, threshold, and measurement-field drift
- duration: not-applicable
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: invalid eight-hour campaign requirements rejected before execution
- observed_errors: six profiles, five repetitions, ten-second samples, and regression/RSS/retry limits accepted
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: not-applicable
- latency: manifest requires p50/p95/p99 fields
- memory: manifest requires RSS baseline/terminal/slope fields
- final_resource_gauges: manifest requires zero final gauges
- result: passed
- artifact: docs/evidence/v1-22-performance-campaign-manifest.json
- non_claims: not an executed benchmark, not production SLO evidence, not universal performance or parity claim

## Q-LOCAL-V123-001

- date_utc: 2026-08-22
- source_commit: 8fb451d07c1d4e87b8138b781fd52eb11cd68520
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Planned
- evidence_level: Local deterministic
- kafka_version: 4.3.1 reference-canary requirement
- kafka_image: apache/kafka:4.3.1 in manual workflow
- mode: KRaft
- topology: isolated single-node smoke; later named service topology required
- security: PLAINTEXT smoke; target security profile remains open
- group_protocol: not-applicable in smoke comparator
- workload: migration manifest, fixture-boundary checker, and forward/rollback workflow validation
- workflow: scripts/check_v1_migration_canary_manifest.py
- fault: fixture drift, missing stage, non-isolated topic/group, or missing rollback evidence field
- duration: not-applicable
- record_count: smoke minimum 1000; exit minimum 1000000
- member_count: not-applicable
- repetition_count: 1
- expected_errors: invalid migration gate rejected before canary execution
- observed_errors: reference fixture, five lifecycle stages, isolation, and reconciliation fields accepted
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: required in exit result fields
- result: passed
- artifact: docs/evidence/v1-23-migration-canary-manifest.json
- non_claims: not executed service canary, not million-record comparison, not source-compatible facade, not production migration approval

## Q-LOCAL-V124-001

- date_utc: 2026-08-22
- source_commit: 4a06eefbe0faacd57ec4ac33b5630b4293cbeae6
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Planned
- evidence_level: Local deterministic
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: external API snapshot and feature-profile preparation
- security: not-applicable
- group_protocol: not-applicable
- workload: public API snapshot, feature/toolchain policy, protocol RC pin policy, and freeze-gate manifest
- workflow: scripts/check_v1_api_freeze_manifest.py
- fault: snapshot drift, missing feature profile, changed MSRV, or relaxed publication/dependency boundary
- duration: not-applicable
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: changed public declarations rejected until snapshot and migration review are regenerated
- observed_errors: snapshot digest, counts, five feature profiles, Rust 1.81/stable policy, and exact RC protocol pin accepted
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: not-applicable
- result: passed
- artifact: docs/evidence/v1-24-api-freeze-manifest.json; docs/evidence/public-api-snapshot.json
- non_claims: not a semver freeze, not RC or stable publication evidence, not proof that V1-20 through V1-23 exit gates passed

## Q-LIVE-V123-001

- date_utc: 2026-08-22
- source_commit: 7a686b087a619928f7ab5d47b185b19074dc6195
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: Planned
- evidence_level: Live current-source
- kafka_version: 4.3.1
- kafka_image: apache/kafka:4.3.1
- mode: KRaft
- topology: isolated single-node reference smoke
- security: PLAINTEXT
- group_protocol: not-applicable
- workload: dual-client baseline comparison through the migration reference fixture
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32552631034
- fault: none injected in baseline smoke; forward/fault-observe/rollback stages remain open
- duration: workflow 85 seconds; kafrust produce 0.044929s consume 0.020054s; rust-rdkafka produce 0.032994s consume 0.010600s
- record_count: 1000 unique records per implementation
- member_count: not-applicable
- repetition_count: 1
- expected_errors: zero comparison divergence and positive produce/consume durations
- observed_errors: none; normalized comparison passed for both implementations
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: 0
- latency: not an SLO measurement; stage durations retained in artifact
- memory: not measured
- final_resource_gauges: not measured
- result: passed
- artifact: GitHub Actions run 32552631034; kafrust-migration-canary-32552631034 artifact; migration manifest
- non_claims: not a named service canary, not a forward/rollback run, not a million-record comparison, not published-artifact evidence, not production migration approval

## Q-CI-V124-001

- date_utc: 2026-08-22
- source_commit: fba572767e1d55668f711d53dd7665a4ca1bb63e
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Planned
- evidence_level: CI
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: stable and Rust 1.81 dual-toolchain CI
- security: not-applicable
- group_protocol: not-applicable
- workload: format, protocol/schema audits, v1 manifests, ledger/API checks, feature profiles, build, clippy, tests, docs, and package boundary
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32552742656
- fault: none
- duration: 8 minutes
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 2 toolchains
- expected_errors: none
- observed_errors: none; stable and Rust 1.81 jobs passed
- retry_count: 0
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: not-applicable
- result: passed
- artifact: CI run 32552742656 at source commit fba572767e1d55668f711d53dd7665a4ca1bb63e
- non_claims: not V1-24 semver freeze, not RC or stable publication, not long-duration/live broker/SLO evidence

## Q-LOCAL-V125-001

- date_utc: 2026-08-22
- source_commit: 5f165f49c6ef949c115b044bec927b5ab5290d6c
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Planned
- evidence_level: Local deterministic
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: release-candidate preparation boundary
- security: not-applicable
- group_protocol: not-applicable
- workload: coordinated RC identity, exact protocol pin, publication sequence, and campaign requirements
- workflow: scripts/check_v1_rc_manifest.py
- fault: changed RC identity, non-exact protocol dependency, reordered publication, or missing campaign gate
- duration: not-applicable
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: invalid RC preparation rejected before registry interaction
- observed_errors: 1.0.0-rc.1 pair, protocol-first sequence, 24-hour fault, 60-minute fuzz, and explicit authorization accepted
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: not-applicable
- result: passed
- artifact: docs/evidence/v1-25-release-candidate-manifest.json
- non_claims: not RC publication, not registry resolution, not RC service-canary evidence

## Q-LOCAL-V126-001

- date_utc: 2026-08-22
- source_commit: 5f165f49c6ef949c115b044bec927b5ab5290d6c
- client_version: 0.3.6
- protocol_version: 0.3.6
- work_status: Planned
- evidence_level: Local deterministic
- kafka_version: not-applicable
- kafka_image: not-applicable
- mode: not-applicable
- topology: stable-release preparation boundary
- security: not-applicable
- group_protocol: not-applicable
- workload: RC-to-stable diff policy, protocol-first stable sequence, artifact verification, tag/release, and post-publish canary requirements
- workflow: scripts/check_v1_release_manifest.py
- fault: behavior change after RC, non-protocol-first publication, premature tag/release, or missing rollback evidence
- duration: not-applicable
- record_count: not-applicable
- member_count: not-applicable
- repetition_count: 1
- expected_errors: invalid stable preparation rejected before registry or tag interaction
- observed_errors: metadata-only diff, explicit authorization, artifact-before-tag, and post-publish canary gates accepted
- retry_count: not-applicable
- duplicate_count: not-applicable
- loss_count: not-applicable
- latency: not-applicable
- memory: not-applicable
- final_resource_gauges: not-applicable
- result: passed
- artifact: docs/evidence/v1-26-release-manifest.json
- non_claims: not stable publication, not docs.rs or crates.io verification, not tagged release, not post-publish canary evidence

## Q-LIVE-V121-DIAG-001

- date_utc: 2026-08-22
- source_commit: 3fdfc77842c8c2a2a3be2c2f9d8f1f3844df65cc
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: In progress
- evidence_level: Live current-source
- kafka_version: 4.3.1
- kafka_image: apache/kafka:4.3.1
- mode: KRaft
- topology: isolated single-node broker-restart smoke
- security: PLAINTEXT
- group_protocol: not-applicable
- workload: 60-second bounded producer/fetch soak with a ten-second broker stop
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32554050050
- fault: broker stopped one third into the run and restarted after ten seconds
- duration: 60.000s workload; 10s outage
- record_count: 2886500
- member_count: not-applicable
- repetition_count: 1
- expected_errors: transient broker-stop failures followed by recovery and zero final gauges
- observed_errors: client assertions reached recovery and zero final gauges; workflow jq assertion failed because two JSON gauge fields were mislabeled
- retry_count: 1027
- duplicate_count: 0
- loss_count: 0
- latency: not measured
- memory: not measured
- final_resource_gauges: client assertion zero; emitted JSON field labels invalid
- result: failed
- artifact: GitHub Actions run 32554050050; retained soak-result.json
- non_claims: not a passing V1-21 campaign, not six-hour evidence, not secured three-broker evidence, not unclean-election evidence, not published artifact evidence

## Q-LIVE-V121-001

- date_utc: 2026-08-22
- source_commit: f7a5fcff1d15b48959762723371699e0194d7501
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: In progress
- evidence_level: Live current-source
- kafka_version: 4.3.1
- kafka_image: apache/kafka:4.3.1
- mode: KRaft
- topology: isolated single-node broker-restart smoke
- security: PLAINTEXT
- group_protocol: not-applicable
- workload: 60-second bounded producer/fetch soak with a ten-second broker stop
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32554367028
- fault: broker stopped one third into the run and restarted after ten seconds
- duration: 60.000s workload; 10s outage
- record_count: 3481600
- member_count: not-applicable
- repetition_count: 1
- expected_errors: transient broker-stop failures followed by recovery and zero final gauges
- observed_errors: 156 high-level operation errors; 219 failed requests; recovery completed
- retry_count: 1091
- duplicate_count: 0
- loss_count: 0
- latency: not measured
- memory: not measured
- final_resource_gauges: in_flight_requests=0; buffered_records=0; max_in_flight_requests=1; max_buffered_records=0
- result: passed
- artifact: GitHub Actions run 32554367028; retained soak-result.json
- non_claims: not six-hour evidence, not secured three-broker evidence, not unclean-election evidence, not published artifact evidence, not production SLO evidence

## Q-LIVE-V122-DIAG-001

- date_utc: 2026-08-22
- source_commit: 3fdfc77842c8c2a2a3be2c2f9d8f1f3844df65cc
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: In progress
- evidence_level: Live current-source
- kafka_version: 4.3.1
- kafka_image: apache/kafka:4.3.1
- mode: KRaft
- topology: isolated single-node benchmark broker
- security: PLAINTEXT
- group_protocol: not-applicable
- workload: four Produce/Fetch benchmark profiles with 2,000 records and batches of 100
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32554051332
- fault: none injected
- duration: 93s workflow
- record_count: 2000 per profile; 8000 total records
- member_count: not-applicable
- repetition_count: 1
- expected_errors: none
- observed_errors: none; all four profiles completed with zero retries and no acknowledged loss or duplicates
- retry_count: 0
- duplicate_count: 0
- loss_count: 0
- latency: request p50/p95/p99 5/25/25ms, 5/5/5ms, 5/5/25ms, and 5/5/25ms by profile
- memory: not measured
- final_resource_gauges: all profiles in_flight_requests=0; buffered_records=0
- result: passed
- artifact: GitHub Actions run 32554051332; benchmark-results.jsonl
- non_claims: not five repetitions, not eight-hour SLO evidence, not published artifact evidence, not cross-client parity, not production claim

## Q-LIVE-MATRIX-002

- date_utc: 2026-08-22
- source_commit: 4f6918b99666912a1e3bfca799664fc760bd1cc9
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: In progress
- evidence_level: Live current-source
- kafka_version: 3.7.2, 3.8.1, 3.9.1, and 4.3.1
- kafka_image: apache/kafka:3.7.2, apache/kafka:3.8.1, apache/kafka:3.9.1, and apache/kafka:4.3.1
- mode: KRaft
- topology: single-node and three-broker failover profiles
- security: PLAINTEXT, TLS, SASL_PLAINTEXT, SASL_SSL with SCRAM-SHA-256/512, SASL_SSL with signed OAUTHBEARER, and ACL authorization
- group_protocol: classic and KIP-848 consumer
- workload: 17-job source-only compatibility smoke covering data-plane, producer, consumer, groups, retained advanced surfaces, Admin, security, transaction ambiguity, and failover
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32555053351
- fault: named broker, leader, coordinator, transaction, and response-loss fixtures in selected jobs
- duration: 7m47s workflow window
- record_count: not-recorded
- member_count: not-recorded
- repetition_count: 17 jobs
- expected_errors: fixture-specific transient faults and typed ambiguous outcomes only
- observed_errors: all 17 jobs passed
- retry_count: not-recorded
- duplicate_count: not-recorded
- loss_count: not-recorded
- latency: not measured
- memory: not measured
- final_resource_gauges: workflow-specific final-resource assertions passed
- result: passed
- artifact: GitHub Actions run 32555053351; source-only job logs
- non_claims: not exact published 0.3.6 lockfile evidence, not complete V1-20 exit evidence, not long-duration SLO evidence, not service canary, not universal Kafka compatibility

## Q-LIVE-V123-002

- date_utc: 2026-08-22
- source_commit: 6bcf1efd852977be20f761e5dbddc5ca4bea4fab
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: Planned
- evidence_level: Live current-source
- kafka_version: 4.3.1
- kafka_image: apache/kafka:4.3.1
- mode: KRaft
- topology: isolated single-node reference smoke
- security: PLAINTEXT
- group_protocol: not-applicable
- workload: dual-client migration baseline with isolated kafrust and rust-rdkafka topics
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32555867981
- fault: none injected in baseline smoke; forward/fault-observe/rollback stages remain open
- duration: 99s workflow
- record_count: 1000 unique records per implementation
- member_count: not-applicable
- repetition_count: 1
- expected_errors: zero normalized comparison divergence and positive stage durations
- observed_errors: none; kafrust produce 0.034558s consume 0.018552s; rust-rdkafka produce 0.020658s consume 0.006937s
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: 0
- latency: not an SLO measurement; stage durations retained in artifact
- memory: not measured
- final_resource_gauges: not measured
- result: passed
- artifact: GitHub Actions run 32555867981; kafrust-migration-canary-32555867981 artifact; migration manifest
- non_claims: not a named service canary, not a forward/rollback run, not a million-record comparison, not published-artifact evidence, not production migration approval

## Q-LIVE-V123-003

- date_utc: 2026-08-22
- source_commit: ec0f86b2fbbec69057b11a48c4acf6ba1c96ae68
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: In progress
- evidence_level: Live current-source
- kafka_version: 4.3.1
- kafka_image: apache/kafka:4.3.1
- mode: KRaft
- topology: isolated single-node reference smoke
- security: PLAINTEXT
- group_protocol: not-applicable
- workload: dual-client migration baseline with embedded business IDs and SHA-256 reconciliation
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32557407734
- fault: none injected in baseline smoke; forward/fault-observe/rollback stages remain open
- duration: 86s workflow
- record_count: 1000 unique records per implementation
- member_count: not-applicable
- repetition_count: 1
- expected_errors: zero loss, zero duplicates, equal business-record digest, and positive stage durations
- observed_errors: none; both implementations unique_records=1000, loss_count=0, duplicate_count=0, matching payload digest
- retry_count: not-applicable
- duplicate_count: 0
- loss_count: 0
- latency: not an SLO measurement; stage durations retained in artifact
- memory: not measured
- final_resource_gauges: not measured
- result: passed
- artifact: GitHub Actions run 32557407734; kafrust-migration-canary-32557407734 artifact; migration manifest
- non_claims: not a named service canary, not a forward/rollback run, not a million-record comparison, not published-artifact evidence, not production migration approval

## Q-LIVE-V122-CAMPAIGN-001

- date_utc: 2026-08-22
- source_commit: 69e499768f4a4f14482b1c3b707a87b2e38620e8
- client_version: 0.3.6 source checkout
- protocol_version: 0.3.6 source checkout
- work_status: In progress
- evidence_level: Live current-source
- kafka_version: 4.3.1
- kafka_image: apache/kafka:4.3.1
- mode: KRaft
- topology: isolated single-node benchmark broker; two partitions
- security: PLAINTEXT
- group_protocol: not-applicable
- workload: timed Produce/Fetch campaign with two workers, 5s warmup, 20s measured window, 5s samples, 50-record batches, and 1-KiB values
- workflow: https://github.com/TaeeunKil/kafrust/actions/runs/32558818231
- fault: none injected
- duration: 25s workload; 121s workflow
- record_count: 1,546,200 produced and consumed
- member_count: not-applicable
- repetition_count: 1
- expected_errors: zero failed requests, zero retries, zero acknowledged loss/duplicates, and zero final resource gauges
- observed_errors: none; produced_records=consumed_records=1,546,200; requests_failed=0; retries=0
- retry_count: 0
- duplicate_count: 0
- loss_count: 0
- latency: request p50/p95/p99 mostly 1/1/1ms with 5ms upper buckets in early samples
- memory: RSS 8,396,800–8,568,832 bytes across measured samples
- final_resource_gauges: in_flight_requests=0; buffered_records=0
- result: passed
- artifact: GitHub Actions run 32558818231; kafrust-benchmark-campaign-32558818231/benchmark-campaign.jsonl; 90-day retention
- non_claims: not five repetitions, not eight-hour V1-22 SLO evidence, not published-artifact evidence, not cross-client parity, not production claim
