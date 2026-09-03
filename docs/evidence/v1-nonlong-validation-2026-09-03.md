# V1 Non-Long Validation Record

- date_utc: 2026-09-03
- source_commit: `924540cde617ebac5a01fdb16ee59766902707a0`
- host: company Windows x64 workstation
- local Linux diagnostic environment: WSL Ubuntu-T9, `x86_64`, Docker root `/var/lib/docker`
- client/protocol version: `0.3.6`
- evidence level: Local deterministic and CI (after remote rerun)

This record covers the work that can be completed without claiming the V1-21
fault-soak, V1-22 SLO campaign, or V1-23 service canary. Existing Docker
containers, networks, volumes, images, and build cache were inspected only;
none were pruned or changed during this validation.

## Defect found and corrected

The exact-head `Live UpdateFeatures Transaction Lifecycle` workflow failed in
runs [33095319319](https://github.com/TaeeunKil/kafrust/actions/runs/33095319319)
and [33697208127](https://github.com/TaeeunKil/kafrust/actions/runs/33697208127).
The broker accepted `UpdateFeatures`, but an immediate `DescribeFeatures`
read returned the previous finalized `transaction.version` level. The cause
was a stale `ApiVersions` response retained by an idle Admin connection after
the mutation.

`AdminClient::update_features` now invalidates ApiVersions capability caches
across the shared broker-client cache after every attempted update, including
an ambiguous transport result, before mapping the mutation outcome. The
existing no-replay ambiguity classifier is preserved. A scripted-broker
regression test proves that a subsequent feature read renegotiates and
observes the new finalized level.

## Validation performed

| Check | Result |
| --- | --- |
| `cargo fmt --all` | passed |
| `cargo check --workspace --all-targets` | passed |
| `cargo test --workspace --all-features` | passed: 467 client, 13 broker-roundtrip, 19 fault-injection, 5 public-surface, 284 protocol, 2 malformed, 10 doctests |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | passed |
| `cargo doc --workspace --all-features --no-deps` | passed |
| `git diff --check` | passed |
| V1 API-key classification | passed: all 93 Kafka 4.3.1 keys (0-92), 16 broker-internal, key 82 excluded |
| protocol/schema/data-plane/matrix/dependency/native/license/registry/advisory/unsafe checks | passed |
| fuzz/fault/performance/migration/freeze/RC/release manifest checks | passed |
| qualification ledger check | passed: 126 immutable rows |
| staged package boundary | passed for default, `tls`, `blocking`, `otlp`, and `all` profiles |
| deterministic SBOM | passed: 89 components, platform `x86_64-unknown-linux-gnu` |

The company-workstation rerun of the executable short-broker surface is
retained separately in
[`v1-company-workstation-nonlong-2026-09-03.md`](v1-company-workstation-nonlong-2026-09-03.md).
It uses isolated Kafka 4.3.1/3.7.2 containers and remains diagnostic evidence;
it does not add a qualification-ledger row or promote any long-duration gate.

The staged package hashes used by the SBOM check are:

- `kafrust-protocol-0.3.6.crate`: `5106ed2d161b01d19e639fa807781138ffe97be0f3ee8b140d7ac5f3dd879144`
- `kafrust-0.3.6.crate`: `a22fb6a65e402ab4f8949f2dfcabf0ac3d7538bdc7b438c1045ba47e0f35f36b`

The source commit was pushed to `main`. The first exact-head CI run
[33698482547](https://github.com/TaeeunKil/kafrust/actions/runs/33698482547)
reached all Rust checks but failed its final SBOM step because the committed
SBOM still listed the older direct `rand` resolution; the resolved-index
refresh in this record updates that inventory. After the fix, the Kafka 4.3.1
UpdateFeatures live workflow passed from the same commit in
[33698683806](https://github.com/TaeeunKil/kafrust/actions/runs/33698683806),
including the level-2 to level-1 downgrade and level-1 to level-2 upgrade.
This is a named live diagnostic row, not the complete V1-11 operation matrix.

The documentation/SBOM refresh commit `3fb31a3691a562d7315cb6dd3d6994f56d7df03c`
then passed the full exact-head CI matrix in
[33699218337](https://github.com/TaeeunKil/kafrust/actions/runs/33699218337)
on Rust 1.81.0 and stable, including package-boundary and deterministic SBOM
verification.

## Remaining gates and non-claims

- No V1-21 six-hour fault campaign was started from this workstation.
- No V1-22 eight-hour performance/SLO campaign was started.
- No V1-23 named service canary or rollback evidence exists.
- These deterministic and CI results do not close V1-11, V1-20, V1-21,
  V1-22, or V1-23, and do not authorize `0.3.7`, `1.0.0`, or any crates.io
  publication.
- No qualification-ledger row was added: this is a defect-fix and validation
  record, not a completed broker qualification gate.
