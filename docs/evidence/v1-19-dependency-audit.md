# V1-19 Dependency Audit Slice

- date_utc: 2026-08-22
- source_commit: `1f6c60d200c58301964c858053a4dd296c48746a`
- toolchain: stable Cargo on the local Windows workspace
- scope: direct metadata plus normal runtime dependency trees for `kafrust`
  default, `tls`, `blocking`, `otlp`, and all-feature profiles

## Reproduction

The graph check used Cargo's normal dependency edges (dev dependencies
excluded) and normalized the tree's indentation before counting unique package
names:

```text
cargo tree -p kafrust --edges normal --format "{p}"
cargo tree -p kafrust --edges normal --features tls --format "{p}"
cargo tree -p kafrust --edges normal --features blocking --format "{p}"
cargo tree -p kafrust --edges normal --features otlp --format "{p}"
cargo tree -p kafrust --edges normal --all-features --format "{p}"
cargo metadata --format-version 1 --no-deps
```

The checked-in reproduction is `python scripts/check_v1_dependency_graph.py`.

The five profiles contained 56, 65, 56, 72, and 81 unique normal-edge
packages respectively. No graph contained `librdkafka`, `rdkafka-sys`,
`kafka-sys`, or `rdkafka`. Metadata reported `kafrust 0.3.6` with 18 direct
dependencies and features `blocking`, `default`, `otlp`, and `tls`; the
protocol package reported four direct dependencies and no feature flags.

The `tls` and all-feature graphs explicitly include `ring`/`rustls`; the
optional native-tool posture is intentionally not inferred from this graph.
License/advisory/yank review, transitive unsafe/native ownership, SBOM
generation, and package-drift enforcement remain open V1-19 work packages.
