# V1-19 SBOM Evidence

- date_utc: 2026-09-03
- source_commit: `924540cde617ebac5a01fdb16ee59766902707a0`
- generator: `scripts/check_v1_sbom.py`
- format: CycloneDX 1.5 JSON
- platform: `x86_64-unknown-linux-gnu`
- feature_set: `all-features`
- dependency_scope: runtime and build dependencies; dev-only edges excluded
- component_count: 89
- sbom_sha256: `d9a671e8fcf741a00719b0406e667532e7398da494747ed0b7280d014214d8a9`

## Reproduction

The checked-in artifact is generated from Cargo's locked, filtered resolve
graph. The command is deterministic for the selected platform and feature
set:

```text
python scripts/check_v1_sbom.py --check --require-artifacts --allow-resolved-version-drift
```

The checker validates CycloneDX structure, unique package URLs, license
metadata for every component, complete dependency references, the generator
property, and package archive presence. CI permits only transitive version
re-resolution caused by platform/index state; workspace versions, direct
dependency versions, package names, licenses, source kinds, and graph edges
must remain identical. It also reports the archive digests:

- `kafrust-protocol-0.3.6.crate`: `5106ed2d161b01d19e639fa807781138ffe97be0f3ee8b140d7ac5f3dd879144`
- `kafrust-0.3.6.crate`: `a22fb6a65e402ab4f8949f2dfcabf0ac3d7538bdc7b438c1045ba47e0f35f36b`

The SBOM is a dependency inventory and drift gate. It does not close the
separate V1-19 advisory, yank, native-toolchain, or transitive unsafe review;
those remain explicit non-claims until their evidence is archived.
