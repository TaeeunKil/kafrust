# V1-02 Public Surface Classification

- Status: Done
- Target evidence: CI
- Dependencies: V1-01

## User-Visible Objective

Classify every exported v1 symbol and module as stable, expert/experimental, or
excluded so downstream users know which API receives `kafrust 1.x` semver
guarantees.

## Non-Goals

- No final API freeze; V1-24 performs the last semver review after runtime
  behavior is qualified.
- No feature addition to make a checklist look complete.
- No Kafka Streams DSL, processor, state store, or task scheduler.
- No automatic stabilization of broker-internal or Kafka-unstable protocols.

## Scope

Audit the actual exports in `crates/kafrust/src/lib.rs`:

- twelve public modules: `admin`, feature-gated `blocking`, `client`, `config`,
  `consumer`, `error`, `group`, `metrics`, `producer`, `share_consumer`,
  `streams`, and `telemetry`;
- every root re-export, feature-gated export, public error variant, trait, and
  builder default;
- `kafrust::protocol` and the separate alpha `kafrust-protocol` crate;
- module-path imports versus root imports;
- low-level client methods versus high-level stable workflows.

Update `docs/public-api-audit.md`, `docs/api-stability.md`, public-surface compile
tests, crate documentation, and migration notes.

## Current Execution Record (2026-08-22)

The all-features rustdoc inventory is stored in
[`docs/evidence/public-api-snapshot.json`](../../evidence/public-api-snapshot.json).
It records 2,366 public symbols, twelve public modules, and 286 root exports;
each symbol has a `stable`, `expert`, `experimental`, or `excluded` class and
an owning milestone. `scripts/public_api_snapshot.py` verifies the committed
snapshot without requiring nightly in CI by checking the root surface and the
public-declaration digest. The existing
[`public_surface.rs`](../../../crates/kafrust/tests/public_surface.rs) test is
compiled in CI under default, `tls`, `blocking`, `otlp`, and all-feature
profiles.

This record is a classification baseline only. It does not freeze the API,
claim the experimental surfaces, or authorize removal of any public path;
V1-24 owns the final semver review.

## Completion Record (2026-08-22)

Commit `1ea206d` passed the exact Rust 1.81.0 and stable CI matrix in
[run 32546683281](https://github.com/TaeeunKil/kafrust/actions/runs/32546683281).
The snapshot checker and all five public-surface feature profiles passed on
both toolchains. V1-02 is complete as a classification gate; behavioral
qualification and the final semver freeze remain owned by later milestones.

## Work Packages

1. Generate a symbol inventory from rustdoc JSON or an equivalent reproducible
   API snapshot instead of maintaining counts manually.
2. Assign every symbol a stability class and owning milestone.
3. Decide the semver relationship between `kafrust 1.x` and
   `kafrust-protocol`; choose among freezing the protocol surface, removing or
   feature-gating the re-export, or keeping the protocol crate explicitly 0.x
   outside the stable client contract.
4. Decide whether blocking adapters, Streams membership, Share Group State,
   dynamic quorum, telemetry, and low-level client APIs are stable or expert.
5. Hide or deprecate accidental public helpers with migration notes and compile
   tests.

## Failure And Lifecycle Contract

- Removing or hiding an alpha public path is a breaking pre-1.0 API change and
  requires a `!` commit plus migration mapping.
- Experimental symbols must be mechanically distinguishable in docs/features;
  prose alone cannot imply they are outside semver if the crate exposes them as
  ordinary stable Rust API.
- Error enums must decide exhaustive versus `#[non_exhaustive]` behavior before
  V1-24.

## Verification

- A generated snapshot accounts for 100% of public modules and root exports.
- `crates/kafrust/tests/public_surface.rs` compiles supported root and module
  import paths for default, `tls`, `blocking`, `otlp`, and all features.
- An expert-only symbol cannot appear in the documented common API list.
- Removed/renamed symbols have compile-fail or replacement compile tests and a
  migration entry.
- Required local Rust validation and exact pushed-commit CI pass.

## Exit Criteria

1. Every public symbol has one stability class and owning milestone.
2. `docs/public-api-audit.md` matches all twelve actual public modules.
3. The `kafrust::protocol`/protocol-crate semver decision is explicit and
   executable.
4. Streams, Share state, KRaft quorum, blocking, telemetry, and low-level APIs
   have stable/experimental/excluded decisions.
5. No unowned stable export remains; CI protects the generated snapshot.

## Migration And Rollback

Keep root aliases temporarily only when they have a stated deprecation path.
Rollback restores both symbol and documentation/compile tests. Do not change a
Kafka-named concept merely to avoid an implementation difficulty.

## Conventional Commit Plan

1. `test(api): snapshot the complete public surface`
2. `refactor(api)!: classify stable and expert exports`
3. `docs(api): reconcile stability and migration records`
4. `ci(api): detect unreviewed public surface changes`

## Evidence Record On Completion

Record symbol totals by class, snapshot hash, features, toolchains, breaking
changes, and the explicit non-claim that classification is not final freeze.
