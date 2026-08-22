# V1-24 Public API Freeze

- Status: Planned
- Target evidence: Packaged candidate
- Dependencies: V1-20-V1-23

## User-Visible Objective

Freeze the supported `kafrust 1.x` API, defaults, errors, features, MSRV,
runtime boundary, protocol relationship, and deprecation policy after behavior
and migration have been proven by the same candidate line.

## Non-Goals

- No new feature added solely for parity after freeze begins.
- No stabilization of experimental/unsupported surfaces by omission.
- No public API change to avoid an internal ownership or compiler problem.
- No guarantee for alpha protocol structs unless the V1-02 decision includes
  them in the stable semver contract.

## Scope

- every public module/root export/feature and rustdoc item from V1-02's generated
  snapshot
- configs, builder methods/defaults/validation, errors and broker error mapping,
  result/partial-result shapes, traits/callbacks, async/blocking behavior,
  clone/send/sync/drop semantics, and non-exhaustive policy
- `kafrust::protocol` and `kafrust-protocol` version/compatibility policy
- Rust 1.81 MSRV, stable support, feature additive behavior, release cadence,
  deprecation and minimum migration window
- public compile tests, semver/API-diff tooling, README/API stability/public API
  audit/migration/release docs

## Work Packages

1. Regenerate and review the complete public API snapshot for every feature set.
2. Resolve all V1-02 provisional classifications and every public
   stringly/ambiguous error or default, including the role of
   `Error::Unsupported` and exhaustive enums.
3. Run downstream reference and canary builds, collect ergonomic issues, and
   accept only behavior-backed changes.
4. Publish old-to-new migration notes for every breaking change since `0.3.5`.
5. Add CI API/semver diff gates against the accepted snapshot and MSRV feature
   matrix.
6. Require every RC client to depend on the exact matching protocol prerelease
   (`=1.0.0-rc.N`) so a later protocol RC cannot change an existing client RC's
   resolved pair. Decide and test the separate stable `1.x` dependency policy.
7. Build final package candidates and review generated docs.

## Preparation Record (2026-08-22)

V1-24 remains `Planned`. The generated all-features snapshot currently records
2,374 symbols, twelve public modules, and 288 root exports after the typed
group-commit ambiguity addition; CI protects that snapshot and its digest.
The preparation lock is machine-checked by
[`v1-24-api-freeze-manifest.json`](../../evidence/v1-24-api-freeze-manifest.json)
and `scripts/check_v1_api_freeze_manifest.py`. This is a review baseline, not
a semver freeze: V1-20 through V1-23 must first close their behavior, artifact,
SLO, and migration gates, after which the snapshot will be regenerated and
locked as `1.0.0-rc.1` input.

The exact current-source preparation checks passed on both Rust 1.81 and stable
in [CI run 32552742656](https://github.com/TaeeunKil/kafrust/actions/runs/32552742656),
including the snapshot lock, all five feature profiles, package boundary, and
required repository validation. This qualifies the preparation input only; it
does not close V1-24's semver, migration, or release exit criteria.

## Failure And Lifecycle Contract

- Adding a new variant/method after freeze follows the documented exhaustive or
  non-exhaustive semver policy.
- Default changes are behavioral API changes and require migration/SLO evidence.
- Feature combinations are additive and do not silently alter unrelated
  semantics.
- Errors preserve Kafka codes, retry/unknown/fencing/data-loss classes, and
  reconciliation context without credentials.
- An RC-critical breaking fix resets the snapshot, increments the RC, and
  reruns dependent qualification.

## Verification

- Generated API snapshot accounts for 100% of default/feature exports.
- Semver/API diff is reviewed against `0.3.5` migration notes and then locked as
  the `1.0.0-rc.1` baseline.
- Public surface/reference service compiles on Rust 1.81 and stable for default,
  `tls`, `blocking`, `otlp`, and all features from staged packages.
- An external lockfile proves that each client RC resolves only its exact
  protocol RC; stable dependency-range behavior matches the separately accepted
  V1-24 policy.
- Rustdoc has no warnings or undocumented stable item; experimental boundaries
  are visible and mechanically enforced.
- No unresolved API-blocking issue remains.

## Exit Criteria

1. Every public symbol/default/error/feature has an accepted stability contract.
2. Protocol-crate/re-export semver policy is executable, RC dependencies use an
   exact matching prerelease pin, and stable package versions/ranges reflect the
   accepted separate policy.
3. Complete migration notes cover every intentional break from `0.3.5`.
4. API/semver/MSRV/package CI gates pass and protect the snapshot.
5. No open P0/P1 API or migration issue remains; exact candidate packages pass.

## Migration And Rollback

Before RC publication, rollback may restore the previous snapshot with matching
docs/tests. After RC publication, any breaking correction uses a new RC and a
superseding migration note. After `1.0.0`, normal semver applies; do not rewrite
the frozen contract retroactively.

## Conventional Commit Plan

1. `test(api): lock complete v1 surface snapshots`
2. `refactor(api)!: finalize stable client contracts`
3. `ci(api): enforce semver and MSRV gates`
4. `docs(api): publish v1 stability and migration policy`

## Evidence Record On Completion

Record snapshot/hash, symbol counts/classes, feature/toolchain matrix, protocol
relationship, defaults/error decisions, breaking migration list, package
hashes, canary compile results, and experimental non-claims.
