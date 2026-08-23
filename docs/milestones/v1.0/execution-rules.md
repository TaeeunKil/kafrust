# v1.0 Milestone Execution Rules

These rules apply to every milestone in this directory. A milestone document
may make a rule stricter, but may not weaken repository policy in `AGENTS.md`.

## One Milestone, One Outcome

Each milestone must have one user-visible objective or one evidence gate. Work
packages may be split across commits, but unrelated capability additions do not
belong in the same milestone. If investigation reveals a materially different
scope, update the dependency map before implementing it.

## Release Authorization And Competitive Review

No agent may publish to crates.io, create a release tag, or create a GitHub
release from milestone progress alone. Those are irreversible or externally
visible actions and are allowed only after the release gate below is satisfied;
the agent may make that release decision autonomously when the evidence is
complete. Dry-runs, package verification, and evidence collection do not by
themselves justify publication.

Before proposing an RC or stable version, record a dated comparison against
the relevant competing clients (at minimum rust-rdkafka/librdkafka and the
closest pure-Rust alternatives). The comparison must identify where kafrust is
better, where it is weaker, and which documented support/operational tradeoffs
remain. If the comparison or qualification results show that the planned
version is premature, stop publication planning, update the milestone
dependency graph and roadmap, choose the required intermediate version(s), and
restart the affected gates from the new plan. Never silently turn an
intermediate release into `1.0.0`.

## Entry Procedure

Before changing source:

```text
git status --short --branch
git diff --stat
git log -12 --oneline --decorate
gh auth status -h github.com
gh run list --limit 20
cargo metadata --no-deps --format-version 1
```

Then:

1. Read `AGENTS.md`, this file, the selected milestone, nearby source, and the
   affected direction/compatibility documents.
2. Confirm each prerequisite's contract and deterministic/CI implementation
   gate is complete. Record terminal live/package/published evidence that is
   deliberately deferred to a shared V1-20 candidate; a dependency edge does
   not require a separate publication before downstream implementation starts.
   A `Superseded` prerequisite is valid only when its linked completed
   replacement maps every inherited contract, exit criterion, and evidence gate.
3. Record the exact baseline commit and existing uncommitted paths. Preserve
   user changes and never reset or discard them to simplify the milestone.
4. Restate the ownership, protocol version, and failure flow before editing.
5. Keep a public API change out of scope unless the milestone explicitly names
   it.

## Required Failure Model

Every public operation changed by a milestone must classify:

| Phase | Required decision |
| --- | --- |
| Before transmission | Whether discovery/connect/authentication failures are retryable and which total budget bounds them. |
| During transmission | When the request becomes possibly observable by the broker. |
| After transmission | Whether replay is safe, duplicate-prone, idempotent, fenced, or an unknown outcome. |
| Broker response | Retryable, terminal, fencing, duplicate, partial, authorization, and data-loss classes. |
| Cancellation | Which owner cancels in-flight I/O and whether state remains reusable. |
| Timeout | Distinguish request timeout, delivery deadline, transaction timeout, session timeout, and shutdown timeout. |
| Shutdown | Define queue drain/release, task join, connection discard, and returned error ordering. |
| Reconciliation | Name the read/redelivery/business-observation path, or state explicitly that Kafka cannot determine the outcome and preserve a typed terminal unknown with all required identity. |

Connection ownership must remain explicit. Producer and Admin idle caches may
reuse stateless authenticated connections according to their documented
boundaries. Direct Fetch sessions, consumer-group membership/heartbeat,
ShareFetch epochs, and Streams membership epochs remain session-owned unless a
milestone proves and documents a lease carrying the complete session identity.

## Verification Ladder

Use the narrowest useful check while iterating, then climb the applicable
ladder. A higher rung does not replace a lower one.

1. **Wire fixtures:** byte-auditable request/response fixtures for every newly
   selected API version, header version, flexible tag, nullable value, and
   malformed boundary.
2. **Deterministic runtime:** scripted-broker tests for disconnects, delayed or
   dropped responses, retries, fencing, cancellation, and shutdown.
3. **Required local Rust validation:** after Rust changes, run every command in
   the repository `AGENTS.md`, including formatting, check, tests, Clippy,
   docs, and `git diff --check`.
4. **Protocol audits:** run `scripts/check_protocol_api_surface.py` and the
   pinned Apache schema audit when protocol code or claims change.
5. **Live current-source:** run the exact broker/topology/security profile in
   the milestone and record commit, workflow, duration/count, result, and
   explicit non-claim.
6. **Package verification:** build both crate tarballs, inspect their contents,
   and compile the packaged client against the matching packaged protocol
   dependency without a workspace path override. `--no-verify` alone is not a
   package compilation gate.
7. **Published/pre-release artifact:** use a fresh external Cargo project,
   verify the lockfile resolves the exact requested versions, and run the named
   profiles on stable and the MSRV where required.
8. **Service canary:** deploy the exact candidate through the migration
   adapter, observe the named workload and faults, and execute rollback.

## Evidence Record

Every completed gate must add one immutable row to the evidence ledger created
by V1-01 with these fields:

- date in UTC;
- source commit;
- client and protocol artifact versions;
- work status and evidence level;
- Kafka version and image/digest when available;
- KRaft or ZooKeeper mode and broker/controller topology;
- security protocol and mechanism;
- client workflow and injected fault;
- duration, record count, member count, and repetition count as applicable;
- expected and observed error types;
- retry, duplicate, loss, latency, memory, and final resource gauges as
  applicable;
- workflow/run URL and retained artifact;
- explicit non-claims.

Do not use “latest,” “current,” or “production-ready” in an evidence row.

## Exit Discipline

A milestone is done only when:

- every numbered exit criterion passes;
- all required local commands pass after the final source change;
- the required live/package/published level is recorded;
- affected public documentation, compatibility limits, and migration notes are
  updated;
- no P0 or P1 defect remains in the milestone scope;
- `git diff --check` passes and the tree contains only reviewed intended work;
- commits are split coherently and the exact pushed commit has green CI.

A milestone may remain `In progress` while V1-20 supplies shared terminal
artifact evidence. Downstream implementation may start after the prerequisite
contract and deterministic/CI gate, but neither milestone is marked `Done`
until its own terminal evidence and exit criteria pass.

A `Superseded` milestone counts toward program closure only through its linked
completed replacement and an explicit old-to-new exit-criterion mapping.

A failing required command must remain visible. It cannot be waived by changing
the roadmap status or weakening a test.

## Commit And Review Checkpoint

Prefer this sequence, omitting empty slices:

1. `test(<scope>): add <failure or wire> regression`
2. `feat(<scope>): implement <observable behavior>` or
   `fix(<scope>): preserve <correctness property>`
3. `ci(<scope>): add <live or package> qualification`
4. `docs(<scope>): record <contract and evidence>`

Public breaking changes use `!` and include migration notes in the same review
series. Do not combine an entire milestone program into one commit. Review the
staged diff and package contents before push.

## Rollback Rules

- Source-only behavior changes: revert the smallest coherent commit and keep
  any regression test that still describes the unsafe behavior.
- Public API changes: provide the old-to-new mapping; if rollback restores the
  old surface, restore its documentation and compile tests together.
- Protocol version selection: keep a tested fallback where Kafka semantics are
  lossless; reject a lossy downgrade explicitly.
- Non-idempotent mutations: never “rollback” by replaying an unknown write.
  Reconcile through a read path or operator action.
- Published artifacts: crates.io artifacts are immutable. Yank only under the
  documented release policy, publish a corrected version, and preserve an
  advisory explaining affected profiles.
- Service canary: rollback is a deployment/configuration action. Preserve
  committed offsets, transactional identity, and duplicate-risk notes.

## Plan Change Control

Update the program index and every dependent milestone when changing:

- the supported broker floor or security matrix;
- stable versus experimental public surface;
- MSRV or feature policy;
- error semantics or a default;
- release artifact ordering;
- a numeric exit threshold.

Historical evidence is immutable. Add a superseding row or note rather than
rewriting the conditions of an older run.
