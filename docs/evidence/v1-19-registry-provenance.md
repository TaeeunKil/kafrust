# V1-19 Registry Provenance Evidence

- date_utc: 2026-08-23
- scope: all-feature runtime and build dependency closure
- platform: `x86_64-unknown-linux-gnu`
- checker: `scripts/check_v1_registry_provenance.py`
- report: [`v1-19-registry-provenance.json`](v1-19-registry-provenance.json)
- report_sha256: `f243c0806e9b2a7a45acc454d6bf617772cdf79236a0220377eacd511b600790`

The checker decoded the local crates.io sparse-index cache for all 87 registry
packages in the resolved closure. Every entry had a 64-character crate
checksum, no index entry was missing, and no resolved version was marked
yanked. The report keeps the resolved version, checksum, yank bit, and
publication timestamp for each package.

This is local registry-index evidence, not a live crates.io query. It is not an
advisory/vulnerability scan, provenance/maintainer-trust guarantee, or a
replacement for the exact package hashes recorded by the SBOM gate.
