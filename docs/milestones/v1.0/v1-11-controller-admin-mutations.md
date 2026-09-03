# V1-11 Controller-Routed Admin Mutations

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-02

## User-Visible Objective

Give every controller-routed public Admin mutation a tested routing,
authorization, partial-result, response-loss, and reconciliation contract that
never replays an ambiguous write blindly.

## Non-Goals

- No generic Admin operation abstraction that hides Kafka controller concepts.
- No automatic rollback of a broker-side mutation.
- No stabilization of Kafka-unstable quorum/state APIs; V1-14 owns their
  classification.
- No claim that every Admin operation is supported by every broker line.

## Scope

- `crates/kafrust/src/{admin,client,config,error,broker_client_cache}.rs`
- controller discovery through Metadata or explicit controller bootstrap
- current controller-routed mutations including CreateTopics 19, DeleteTopics
  20, CreatePartitions 37, ElectLeaders 43, AlterPartitionReassignments 45,
  UpdateFeatures 57, and UnregisterBroker 64
- matching read/reconciliation paths such as Metadata, DescribeConfigs,
  ListPartitionReassignments, DescribeQuorum, DescribeFeatures, DescribeCluster,
  and broker re-registration observation
- Admin ambiguity proxies/tests, authorization examples, controller workflows,
  release/compatibility/admin docs

Security-sensitive SCRAM and delegation-token operations have one owner in
V1-13 even though they use controller routing. Dynamic quorum APIs 80-82 have
one owner in V1-14. V1-11 supplies shared controller-routing mechanics but does
not duplicate those operation gates.

V1-01's operation ledger is authoritative. If an operation is routed
differently in source, move it to V1-12 or V1-13 rather than forcing this list.

## Work Packages

1. Generate a table for every public Admin method: API/version, route owner,
   read/idempotent/non-idempotent class, authorization resource, and
   reconciliation read.
2. For each controller mutation, test retryable discovery/connect failure
   before transmission and a dropped response after transmission.
3. Preserve top-level and per-resource errors/partial success without flattening.
4. Cover active controller replacement, stale controller, authorization denial,
   request timeout, cancellation, and shutdown.
5. Reject downgrades that lose requested semantics such as validation-only,
   unsafe downgrade, or committed acknowledgement.
6. Qualify only the common controller operations allocated to this milestone;
   V1-13 and V1-14 qualify their security and dynamic-quorum operations.

## Current Execution Record (2026-08-22)

V1-11 is now `In progress`. Common controller routing is operation-owned and
supports explicit controller bootstrap or Metadata-discovered controller
endpoints. The shared mutation classifier retries discovery/connect and
ApiVersions only before a controller request is transmitted; a possible
post-transmission I/O, timeout, oversized-response, or protocol failure returns
`AdminMutationOutcomeUnknown` without replay. Confirmed Kafka and per-resource
errors remain typed, and partial results are preserved.

The deterministic Admin suite already covers controller routing and result
retention for CreateTopics, CreatePartitions, DeleteTopics, ElectLeaders,
AlterPartitionReassignments, UpdateFeatures, Add/RemoveRaftVoter, and
UnregisterBroker, plus response-loss classification and pre-transmission
discovery retry. Existing CreateTopics/DeleteTopics response-drop fixtures
assert one mutation request and a follow-up reconciliation read. The complete
operation-by-operation authorization/failover table, active-controller
replacement, and published floor/current profiles remain open; no complete
Admin compatibility claim is made.

### UpdateFeatures cache regression (2026-09-03)

The exact-head live UpdateFeatures transaction workflow reproduced a stale
finalized-feature read twice: the broker accepted the mutation while a later
DescribeFeatures call reused an idle connection's cached ApiVersions response.
`AdminClient::update_features` now invalidates ApiVersions caches across the
shared broker-client cache after every attempted update, preserving the
post-transmission unknown-outcome classifier. A scripted-broker regression
test proves that the subsequent read renegotiates and observes the new feature
level. The deterministic and CI record is
[`v1-nonlong-validation-2026-09-03.md`](../../evidence/v1-nonlong-validation-2026-09-03.md);
the fixed source passed the Kafka 4.3.1 live workflow in
[33698683806](https://github.com/TaeeunKil/kafrust/actions/runs/33698683806).
This closes the named UpdateFeatures diagnostic only; the complete operation
matrix and published floor/current profiles remain open.

## Failure And Lifecycle Contract

- Pre-transmission discovery/connect errors may retry within the Admin budget.
- Once a non-idempotent request may have reached the controller, transport loss
  returns `Error::AdminMutationOutcomeUnknown { operation }` and sends no replay.
- Authorization and Kafka validation errors are confirmed broker outcomes, not
  transport ambiguity.
- Partial results retain every resource entry and exact error code/message.
- Controller connections are operation-owned unless a proven cache lease keeps
  endpoint/capability identity; poisoned connections are discarded.
- Cancellation after possible write follows the same unknown-outcome boundary.

## Verification

- Deterministic before/after-transmission and authorization fixtures for 100%
  of controller mutation methods in the generated ledger.
- Assertions include request count `1` after response loss and an exact
  reconciliation read for each operation.
- Published accepted-floor and pinned-current controller-capable profiles run
  common topic/partition/feature/unregister operations. Security and dynamic
  quorum results are cited from V1-13/V1-14 rather than counted here.
- Three-controller/broker tests replace the active controller during discovery
  and during a validation-only request.

## Exit Criteria

1. Every public controller mutation has complete classification and local
   pre/post-transmission coverage.
2. Every ambiguous case returns the exact typed unknown error with one request.
3. Every operation has a documented reconciliation read or an explicit manual
   operator procedure.
4. Required published floor/current authorization and failover profiles pass.
5. Admin, compatibility, migration, and evidence documents agree.

## Migration And Rollback

Map rust-rdkafka Admin options, per-resource results, timeouts, and opaque
operation handles to kafrust's typed outcomes. Rollback never issues the inverse
mutation automatically; reconcile cluster state first, then let an operator
choose a compensating action.

## Conventional Commit Plan

1. `test(admin): cover controller mutation ambiguity`
2. `fix(admin): preserve controller outcomes and routing`
3. `ci(admin): qualify controller authorization and failover`
4. `docs(admin): add reconciliation contracts`

## Evidence Record On Completion

Record every operation/API/version, route, request count, auth principal/error,
fault point, unknown outcome, reconciliation result, controller topology,
artifact, and unstable-operation non-claim.
