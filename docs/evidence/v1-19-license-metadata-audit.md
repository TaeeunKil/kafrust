# V1-19 License Metadata Audit

- date_utc: 2026-08-22
- source_commit: recorded in the corresponding immutable ledger row
- toolchain: stable Cargo on the local Windows workspace
- command: `cargo metadata --format-version 1` (with `--locked` when the
  ignored root lockfile is present)
- resolved_package_count: 71
- missing_license_or_license_file: 0

The dependency graph checker resolves workspace metadata (using the ignored
root lockfile when present) and rejects any package that has neither Cargo
`license` nor `license_file` metadata. The check covers the complete resolved
graph rather than only the two workspace crates and runs alongside the
forbidden-package and version checks in `scripts/check_v1_dependency_graph.py`.

This is a metadata-completeness slice only. It does not approve license
compatibility, advisories, yanks, transitive native/unsafe ownership, SBOM
contents, optional TLS system tooling, or registry publication. Those remain
separate V1-19 gates and must be reviewed against the exact release tarballs.
