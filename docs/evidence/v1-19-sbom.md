# V1-19 SBOM Evidence

- date_utc: 2026-09-10
- source_commit: `9f12c37ac11feceb18366ab902564378c79b181a`
- generator: `scripts/check_v1_sbom.py`
- format: CycloneDX 1.5 JSON
- platform: `x86_64-unknown-linux-gnu`
- feature_set: `all-features`
- dependency_scope: runtime and build dependencies; dev-only edges excluded
- component_count: 89
- sbom_sha256: `09e2edcb03f165b81a755470bf6b29ee42157dc82cf66a01efa89cca74c18ce1`

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

- `kafrust-protocol-0.4.0.crate`: `ef8f83b5d389512376d25e9d5346c4c1a78fd589e02f3c73c502c07742bb8010`
- `kafrust-0.4.0.crate`: `8f937f3c1074f94eb955e68ae592a90d53a8684afb960a3ba45e5c4d2d566cab`

The SBOM is a dependency inventory and drift gate. It does not close the
separate V1-19 advisory, yank, native-toolchain, or transitive unsafe review;
those remain explicit non-claims until their evidence is archived.
