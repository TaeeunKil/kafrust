# v1.0 Planning Baseline

Date: 2026-08-21
Inspected source: `9eba7e5` (`main`, synchronized with `origin/main`)

This document corrects the transient repository facts in
[the planning handoff](../../milestone-planning-handoff-2026-08-21.md) and
records the inputs used to create the v1.0 program. It is a dated snapshot,
not a rolling compatibility claim.

## Verified Repository State

| Check | Result |
| --- | --- |
| `git status --short --branch` | clean `main...origin/main` |
| GitHub authentication | active for `TaeeunKil` with repository/workflow access |
| Exact HEAD CI | passing run `32468949663` |
| Workspace crate versions | `kafrust 0.3.5`, `kafrust-protocol 0.3.5` |
| Latest published line | `0.3.5` |
| Latest GitHub release | `v0.3.3`; no `v0.3.4` or `v0.3.5` GitHub release was present |
| `main` branch protection | absent at inspection |
| Protocol surface script | 63 modules, 76 unique Kafka API keys |
| Apache schema script | Kafka 4.3.1 snapshot, 12 schemas within official version bounds |

The handoff's dirty-tree and invalid-authentication statements were true for
its earlier snapshot. The changes were subsequently split into Conventional
Commits and pushed. They now have current-source CI evidence, but changes made
after the `0.3.5` publication do not have `0.3.5` published-artifact evidence.

## Immediate Package Blocker

The following command fails on the inspected source:

```text
cargo package -p kafrust --all-features --locked
```

The packaged client resolves the already-published
`kafrust-protocol 0.3.5`, which does not contain source-tree additions used by
the client:

- `AddOffsetsToTxnRequestV3` and `AddOffsetsToTxnResponseV3`
- `AddPartitionsToTxnRequestV3` and `AddPartitionsToTxnResponseV3`
- `EndTxnRequestV3` and `EndTxnResponseV3`
- `InitProducerIdRequestV2` and `InitProducerIdResponseV2`

Workspace builds use the local path dependency and therefore pass. The current
CI package command uses `--no-verify`, so it checks package assembly but not
this registry dependency compilation boundary. Reusing a published version
for changed package contents is invalid. V1-00 must establish new coordinated
crate versions and a two-stage package verification path before publication.

## Evidence Strengths

The strongest existing evidence inputs are:

- published `0.3.5` signed OAUTHBEARER/OIDC authentication and re-authentication
  on Kafka 3.7.2;
- published `0.3.5` seven-profile smoke for classic, KIP-848, SCRAM, and all
  four supported compression codecs;
- published `0.3.5` 600-second secured simultaneous two-broker-loss soak on
  Kafka 4.3.1;
- current-source Share acknowledgement response-loss reconciliation on Kafka
  4.3.1;
- deterministic producer, transaction, consumer, group, Share, and Admin
  response-loss slices in the scripted-broker harness;
- ten fuzz targets with tracked seed corpora and bounded scheduled/manual CI;
- an exact current HEAD CI pass.

Each item is evidence only for its named artifact, broker, topology, security
mode, workload, and duration. None is a complete v1.0 or production-SLO claim.

## Planning Gaps

The v1.0 plan exists to close these concrete gaps:

1. Package versions and registry dependency verification are inconsistent.
2. crates.io publication and GitHub release/tag history are not synchronized
   for `0.3.4`/`0.3.5`, and `main` is not protected; provenance must be audited
   before adding or repairing release metadata.
3. The supported broker floor is contradictory: current evidence centers on
   3.7.2/3.8.1/3.9.1/4.3.1, while one strategy target lists
   3.3/3.6/3.7/3.9/4.0/4.3.
4. `crates/kafrust/src/lib.rs` exposes twelve public modules, while
   `docs/public-api-audit.md` lists only seven and omits separate stability
   treatment for blocking, Share, metrics, and telemetry exports.
5. The alpha `kafrust-protocol` crate is re-exported as `kafrust::protocol`,
   coupling its public wire structs to the `kafrust 1.0` semver boundary unless
   the relationship is changed.
6. Streams membership, unstable Share Group State, dynamic KRaft quorum,
   telemetry, blocking adapters, and low-level protocol APIs lack one explicit
   stable/experimental/excluded decision.
7. The API/version audit covers identity and bounds for 76 keys, while the
   offline Kafka 4.3.1 snapshot covers only 12 high-risk schema identity,
   version-bound, and flexible-version metadata entries; it is not a
   field-level schema audit.
8. Runtime failure semantics and live evidence are strong in selected slices
   but incomplete across producer, transaction, group, Share, and Admin
   operation families.
9. Transactional Produce v12/v13 must not be combined with the current legacy
   AddPartitions/AddOffsets/EndTxn v3 flow merely because Kafka 4.3.1 advertises
   the higher Produce version; V1-06 must choose legacy TV0/TV1 or implement the
   complete KIP-890 transaction protocol V2 boundary.
10. A lost Share acknowledgement response does not imply redelivery: an applied
   Accept moves the record to `Acknowledged`. V1-10 must distinguish applied,
   unapplied/redelivered, and persistently unknown outcomes.
11. Long-duration security, credential rotation, resource/backpressure, fuzz,
   data-loss, performance, service-canary, and rollback gates remain open.
12. Roadmap, strategy, compatibility, migration, and API audit documents contain
   stale counts or relative “current/latest” wording that can be misread.

## Known Documentation Contradictions

V1-01 and V1-02 must resolve, rather than silently choose among, these items:

- `0.3.0`, `0.3.3`, `0.3.4`, and `0.3.5` appear as “current” in different
  historical sections; `0.3.5` is the dated published baseline.
- the handoff records 792 local tests while strategy text records 788; future
  counts must be generated, dated, and tied to a command rather than copied;
- migration text says an mTLS live gate is open while compatibility evidence
  records current-source and published 3.7.2/4.3.1 passes;
- an early compatibility example describes Fetch v18 as a lag even though
  later text and source contain low-level v18 coverage;
- historical milestones use an undefined `Complete` status and sometimes mix
  implementation state with evidence state;
- readiness percentages use different denominators and must not be compared
  without the weighting model in the program index.

## Evidence That Must Be Refreshed

The following are deliberately not treated as complete by this baseline:

- a full local Rust validation after future Rust changes;
- a verified client package against the matching source protocol package;
- a fresh live matrix on one exact v1 candidate commit;
- a fresh published/pre-release external project with no path dependency;
- a service canary and rollback rehearsal;
- a production-style long soak or universal throughput claim.
