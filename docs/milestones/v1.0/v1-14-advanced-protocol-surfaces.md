# V1-14 Advanced Protocol Surfaces

- Status: In progress
- Target evidence: CI
- Dependencies: V1-02, V1-11
- Conditional evidence: every retained surface requires Live current-source;
  an all-excluded decision closes at CI.

## User-Visible Objective

Give advanced and broker-internal-adjacent surfaces an honest v1 boundary:
each is either qualified for a named stable/expert contract or mechanically
excluded from the stable `kafrust 1.x` promise.

## Non-Goals

- No Kafka Streams DSL, topology execution engine, state store, processor,
  scheduler, broker, controller, or storage implementation.
- No requirement to stabilize Kafka APIs that Kafka itself marks unstable.
- No API-key checklist work without a client workload.
- No use of advanced surface evidence as a substitute for core producer/group
  qualification.

## Scope

Surfaces requiring explicit decisions:

| Surface | APIs/source | v1 decision required |
| --- | --- | --- |
| Streams membership | StreamsGroupHeartbeat 88, StreamsGroupDescribe 89; `streams.rs` | Stable expert membership contract or experimental feature. |
| Share Group State | APIs 83-87; `admin.rs`, `share_group_state.rs` | Experimental/broker-internal by default unless Kafka stability changes. |
| Dynamic KRaft quorum | AddRaftVoter 80, RemoveRaftVoter 81, missing UpdateRaftVoter 82 | Qualify operator API or explicitly exclude key 82 and unstable mutations. |
| Modern low-level APIs | expert `Client` methods and protocol types | Stable expert, experimental, or protocol-crate-only. |
| Blocking adapters | `blocking.rs` | Stable wrappers or experimental feature; no implied alternate-runtime abstraction. |

Telemetry is classified here at the surface level; V1-17 owns its runtime
contract if retained.

## Work Packages

1. Record Kafka stability, broker-version availability, client use case, and
   public symbol class for each surface.
2. Decide API key 82 and classify every missing key 0-92 as client-relevant,
   broker/controller-internal, removed, or out of v1 scope.
3. For every retained expert surface, add protocol bytes, deterministic
   lifecycle/routing, authorization/failure, and at least one exact live profile.
4. Feature-gate, hide, or clearly namespace experimental surfaces so ordinary
   `1.x` users cannot mistake them for the core support claim.
5. Update README, API audit, compatibility, and rustdoc with explicit non-goals.

## Current Execution Record (2026-08-22)

V1-14 is now `In progress`. The current surface classification treats Streams
membership, Share Group State, dynamic quorum mutations, low-level protocol
methods, and blocking adapters as expert or experimental rather than silently
expanding the core Kafka replacement claim. The generated all-features API
snapshot assigns each retained symbol to an owner/classification, while the
protocol/data-plane manifest and milestone docs preserve explicit version and
stability boundaries.

Deterministic coverage includes Streams heartbeat/task assignment validation,
Share Group State request/response routing, add/remove voter controller paths,
blocking nested-runtime rejection, and low-level protocol bytes. Live dynamic
quorum convergence, unstable Share state replication, retained Streams churn,
and the final API-key 0-92 decision table remain open. Experimental and
broker-internal paths are not counted as stable core compatibility.

### API-key classification gate (2026-09-03)

The Apache Kafka 4.3.1 `ApiKeys` inventory is now captured in
[`v1-14-api-key-classification.json`](../../evidence/v1-14-api-key-classification.json)
and checked by `scripts/check_v1_api_key_classification.py`. All 93 keys from
0 through 92 have an explicit class, owner, and rationale: 16 are
broker-internal, key 82 (`UPDATE_RAFT_VOTER`) is explicitly excluded because
the client does not implement it, and every implemented key is classified as
stable-core, expert, or experimental. The checker and regression tests run in
CI. This closes the classification inventory only; retained expert/experimental
live gates and the broader V1-14 exit criteria remain open.

## Failure And Lifecycle Contract

- Streams membership owns member/endpoint epochs and task assignments; it does
  not own application processing or state restoration.
- Share state mutations retain `AdminMutationOutcomeUnknown` and reject lossy
  v1-to-v0 downgrades.
- Dynamic quorum mutations are operator actions with controller routing,
  authorization, convergence observation, and no automatic inverse write.
- Blocking adapters own and join one dedicated Tokio runtime and reject nested
  runtime use without panicking.
- Experimental APIs carry no accidental core compatibility claim.

## Verification

- 100% classification for API keys 0-92 and all advanced public exports.
- Retained Streams membership: pinned-current two-member churn, coordinator
  stop, task transition, and graceful close.
- Retained Share state: pinned-current replicated coordinator failover and
  post-fault read/summary/delete, labeled unstable.
- Retained quorum operations: pinned-current dynamic controller add/remove,
  authorization denial, convergence, and response-loss reconciliation.
- Retained blocking adapters: default and all-feature compile/runtime smoke on
  Rust 1.81 and stable, including nested-runtime rejection and task cleanup.

## Exit Criteria

1. Every advanced public export and missing API key has one explicit class.
2. Every retained surface has deterministic coverage and the named live gate.
3. Experimental/excluded surfaces are mechanically and visibly separated from
   the stable core contract.
4. Streams documentation cannot be read as a Streams application-engine claim.
5. API audit, compatibility, migration, and evidence rows match the decision.

## Migration And Rollback

Moving an alpha export behind a feature/module boundary requires a breaking
migration note. Rollback may restore an expert path before v1, but must not
upgrade its evidence label. Operator mutations require state observation before
any compensating action.

## Conventional Commit Plan

1. `docs(api): classify advanced protocol surfaces`
2. `refactor(api)!: isolate experimental client APIs`
3. `test(runtime): qualify retained expert lifecycles`
4. `ci(kafka): add advanced surface evidence gates`

## Evidence Record On Completion

Record each surface/API key, Kafka stability, symbol class, protocol/live
evidence, broker profile, failure/reconciliation result, and explicit core
replacement non-claim.
