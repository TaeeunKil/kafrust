# V1-19 Direct-Dependency Drift Remediation — 2026-09-07

## Failure

CI run [34070902219](https://github.com/TaeeunKil/kafrust/actions/runs/34070902219)
from source `6ffd757ef6d36fdd5ddb66b03b62dc7ff2857536` failed only in the
stable job's `Verify deterministic V1-19 SBOM and package artifacts` step. The
checker reported that direct dependency versions had drifted from the checked-
in SBOM. The ignored root `Cargo.lock` is regenerated on each fresh checkout;
the caret requirement `tokio-rustls = "0.26.4"` therefore admitted the newly
available `0.26.5` release on the hosted Linux registry cache. The SBOM policy
allows transitive refreshes but intentionally rejects direct dependency drift.

## Remediation and recheck

Commit `af5f6982df8c3fea6dd4e96db32303415e66c46f` changes the direct
requirement to `tokio-rustls = "=0.26.4"`. This keeps the root lockfile
ignored, preserves the reviewed SBOM direct edge, and prevents the same index
refresh from changing the shipped dependency graph. Local fmt/check/test/
clippy/doc, SBOM, ledger, and diff checks passed.

The follow-up [CI run 34071606979](https://github.com/TaeeunKil/kafrust/actions/runs/34071606979)
passed both Rust stable and Rust 1.81.0 jobs, including the deterministic SBOM
and package-artifact step. No package was published and no version/tag was
advanced by this remediation.

## Boundary

This closes one reproducibility failure mode in the V1-19 SBOM gate. It does
not complete the advisory/yank review, optional-TLS native-tooling review,
transitive unsafe/native ownership review, published-artifact matrix, long
campaigns, service canary, or release authorization.
