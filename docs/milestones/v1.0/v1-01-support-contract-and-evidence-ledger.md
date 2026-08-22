# V1-01 Support Contract And Evidence Ledger

- Status: Planned
- Target evidence: CI
- Dependencies: V1-00

## User-Visible Objective

Publish one unambiguous v1 support contract and one immutable qualification
ledger so users can tell exactly which broker, topology, security, workload,
and artifact combinations are supported or still unclaimed.

## Non-Goals

- No Kafka protocol or runtime implementation.
- No claim that every accepted broker line supports every modern Kafka API.
- No full Cartesian product of broker, security, topology, and workload.
- No deletion or rewriting of historical evidence.

## Scope

Documents and automation:

- `docs/compatibility.md`
- `docs/project-strategy.md`
- `docs/roadmap.md`
- `docs/release.md`
- new `docs/evidence/qualification-ledger.md` and its schema/checker
- README compatibility tables after the contract is accepted

Required decisions:

- broker floor: accept 3.3.2/3.6.2 or explicitly set 3.7.2;
- continuity profiles: 3.8.1, 3.9.1, 4.0.0, and pinned 4.3.1;
- KRaft-only versus any ZooKeeper claim;
- single-node, three-broker, controller-listener, and managed-service scope;
- plaintext, TLS, mTLS, PLAIN, SCRAM-256/512, and OAUTHBEARER matrix;
- async Tokio, optional blocking, and alternate-runtime scope;
- core workloads required for v1 and expert/experimental exclusions.

## Work Packages

1. Generate a capability/gap inventory from current source and public exports.
2. Reconcile every contradictory broker/security statement listed in
   `baseline.md`; never select a value silently.
3. Define the pairwise target matrix with exact versions and topology.
4. Create a ledger row schema containing the fields in `execution-rules.md`.
5. Migrate the strongest named evidence first and link, rather than duplicate,
   historical prose.
6. Add a CI checker that rejects missing fields, relative “latest/current”
   artifact labels, duplicate evidence IDs, and unsupported status values.

## Failure And Lifecycle Contract

- “Implemented” and “verified” remain separate facts.
- A failed or cancelled workflow is evidence of failure, not absence of data.
- Source, packaged, published, and service-canary results may not be merged into
  one row.
- A broker profile removed from the contract requires migration and release
  notes; a new profile starts unclaimed until its required evidence passes.

## Verification

- The ledger checker parses every row deterministically.
- Every current user-facing compatibility claim links to at least one exact
  row or is labeled candidate/unclaimed.
- All proposed broker lines are either accepted with required gates or rejected
  with a dated rationale.
- README, strategy, roadmap, compatibility, migration, and release documents
  agree on the latest published version and support terminology.
- `git diff --check` and exact pushed-commit CI pass.

## Exit Criteria

1. The accepted support table names exact broker versions, topology, security,
   group protocol, artifact level, and required workloads.
2. All capability families are `required`, `experimental`, or `excluded` at a
   high level; V1-02 refines the public symbol boundary.
3. Every imported evidence row has commit, versions, profile, result, URL, and
   explicit non-claim.
4. No active document calls a historical artifact “current” without a date and
   exact version.
5. CI validates the ledger and exact pushed-commit CI is green.

## Migration And Rollback

The contract may narrow claims without changing code. If an accepted profile
is later removed, restore the previous document only if evidence still supports
it; otherwise publish a superseding decision and migration note. Never delete
an unfavorable historical row.

## Conventional Commit Plan

1. `docs(compat): define v1 support contract`
2. `docs(evidence): add immutable qualification ledger`
3. `ci(evidence): validate qualification records`
4. `docs: reconcile current support claims`

## Evidence Record On Completion

Record the decision commit and classify this as a design/CI gate. Do not label
the matrix live verified merely because older subset runs were imported.
