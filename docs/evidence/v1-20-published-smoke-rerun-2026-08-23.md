# V1-20 Published Smoke Rerun (2026-08-23)

This record captures the fresh exact-registry rerun from GitHub Actions run
[32626214201](https://github.com/TaeeunKil/kafrust/actions/runs/32626214201).
The workflow checked out pushed source `8331079b18d00cdf8c9de1d3b3b05c0fa1d61094`,
then each job created an external Cargo project outside the repository and
resolved `kafrust 0.3.6` and `kafrust-protocol 0.3.6` from crates.io without a
workspace path or patch override.

## Result

All twelve matrix jobs passed. Each job compiled and ran the published smoke
fixture against its named Apache Kafka image and completed the profile's
produce/fetch or group roundtrip assertions:

| Profile | Kafka | Security / protocol | Compression | Result |
| --- | --- | --- | --- | --- |
| `kafka-3.7.2-classic` | 3.7.2 | PLAINTEXT / classic | none | passed |
| `kafka-3.8.1-classic` | 3.8.1 | PLAINTEXT / classic | none | passed |
| `kafka-3.9.1-classic` | 3.9.1 | PLAINTEXT / classic | none | passed |
| `kafka-4.0.0-classic` | 4.0.0 | PLAINTEXT / classic | none | passed |
| `kafka-4.3.1-kip848` | 4.3.1 | PLAINTEXT / KIP-848 consumer | none | passed |
| `kafka-3.7.2-sasl-ssl-scram` | 3.7.2 | SASL_SSL / SCRAM-SHA-256 | none | passed |
| `kafka-3.7.2-sasl-ssl-scram512` | 3.7.2 | SASL_SSL / SCRAM-SHA-512 | none | passed |
| `kafka-3.7.2-sasl-plain` | 3.7.2 | SASL_PLAINTEXT / PLAIN | none | passed |
| `kafka-3.7.2-gzip` | 3.7.2 | PLAINTEXT / classic | gzip | passed |
| `kafka-3.7.2-snappy` | 3.7.2 | PLAINTEXT / classic | snappy | passed |
| `kafka-3.7.2-lz4` | 3.7.2 | PLAINTEXT / classic | lz4 | passed |
| `kafka-3.7.2-zstd` | 3.7.2 | PLAINTEXT / classic | zstd | passed |

The workflow's external-project lockfile and compile/run logs are retained in
the per-job GitHub Actions logs. No job reported a profile, dependency, or
roundtrip failure.

## Qualification boundary

This is fresh published-artifact smoke evidence for the listed rows. It does
not close V1-20: the accepted matrix still requires every row's complete
source/published evidence and explicit non-claims, while V1-21 fault duration,
V1-22 SLO qualification, V1-23 service canary, and later release gates remain
open. It also records no latency, RSS, retry-ratio, or final-resource SLO
measurements. The run therefore does not authorize `0.3.7`, an RC, or `1.0.0`.
