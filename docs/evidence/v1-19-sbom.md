# V1-19 SBOM Evidence

- date_utc: 2026-08-23
- source_commit: `368a2080092cd9ba42afeb73ae23047251432803`
- generator: `scripts/check_v1_sbom.py`
- format: CycloneDX 1.5 JSON
- platform: `x86_64-unknown-linux-gnu`
- feature_set: `all-features`
- dependency_scope: runtime and build dependencies; dev-only edges excluded
- component_count: 89
- sbom_sha256: `01c14214426c4846533dae6d626283c5f4107e25b2d9f07d380042281e7f495a`

## Reproduction

The checked-in artifact is generated from Cargo's locked, filtered resolve
graph. The command is deterministic for the selected platform and feature
set:

```text
python scripts/check_v1_sbom.py --check --require-artifacts
```

The checker validates CycloneDX structure, unique package URLs, license
metadata for every component, complete dependency references, the generator
property, and package archive presence. It also reports the archive digests:

- `kafrust-protocol-0.3.6.crate`: `f12e95a30ce46fd7ffc097a97a31b0a918bcee9f83cefb72fe2484cfe9c255cc`
- `kafrust-0.3.6.crate`: `2ae1a135d3de7f00fb25455809ab9fc201ea41c398aa62ac14f34c2a2758fca9`

The SBOM is a dependency inventory and drift gate. It does not close the
separate V1-19 advisory, yank, native-toolchain, or transitive unsafe review;
those remain explicit non-claims until their evidence is archived.
