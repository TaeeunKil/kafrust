# V1-19 License Policy Evidence

- date_utc: 2026-08-23
- scope: all-feature runtime and build dependency closure
- platform: `x86_64-unknown-linux-gnu`
- checker: `scripts/check_v1_license_policy.py`
- report: [`v1-19-license-policy.json`](v1-19-license-policy.json)
- report_sha256: `b96da01852f88a3fb2510d8f4558900186d3527623aca9cb8d3f3fb491011eb8`

The checker consumes Cargo's locked metadata and follows the same normal/build
closure as the V1-19 CycloneDX SBOM. Every one of the 89 resolved packages has
an SPDX license expression. Every identifier is in the explicit permissive
allowlist: `0BSD`, `Apache-2.0`, `BSD-2-Clause`, `BSD-3-Clause`, `ISC`, `MIT`,
`Unicode-3.0`, `Unlicense`, or `Zlib`. The workspace distribution remains
`MIT OR Apache-2.0`.

CI compares package names, source kinds, license expressions, and policy while
allowing only transitive version re-resolution already permitted by the SBOM
drift rule. A newly introduced license or a package without an SPDX expression
fails the gate and requires an explicit policy review.

This is a license-expression compatibility and metadata slice. It does not
claim that advisories or yanks were reviewed, that packaged license notices are
complete, or that transitive unsafe/native code has been reviewed.
