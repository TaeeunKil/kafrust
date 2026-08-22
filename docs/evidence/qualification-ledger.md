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
