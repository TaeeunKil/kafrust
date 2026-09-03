# V1-19 Advisory Snapshot

- date_utc: 2026-09-03
- scope: all-feature runtime and build dependency closure
- platform: `x86_64-unknown-linux-gnu`
- checker: [`scripts/check_v1_advisories.py`](../../scripts/check_v1_advisories.py)
- report: [`v1-19-advisories.json`](v1-19-advisories.json)
- report_sha256: `48577a2fe010229adf9434c8ff5051a0bb2a29c7f325f5732416889419c2958e`
- source: [OSV querybatch](https://osv.dev/docs/), RustSec advisory export
- rustsec_repository: <https://github.com/RustSec/advisory-db>
- rustsec_revision: `bf5c0d245a92671908518d7e765914d437954ed6`

The refresh run queried all 89 resolved workspace and registry packages in one
OSV batch request. It recorded zero advisory matches, including zero
critical/high matches. The snapshot is intentionally time-bounded to 30 days;
CI verifies the package identity inventory (allowing only transitive registry
version re-resolution already covered by the SBOM gate) and fails after the
review window so a new live query is required. The committed CI check is
offline and does not claim current future OSV, crates.io, or
undisclosed-vulnerability state.

This closes the dated advisory snapshot slice only. Manual owner/rationale
review for the 62 unsafe/native entries, multi-platform package evidence, and
the later published-artifact gates remain open for V1-19 and downstream
milestones.
