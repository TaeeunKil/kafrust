# V1-20 Published Smoke Artifact Rerun (2026-08-23)

This is the clean validation of the published-smoke evidence-retention change
from source `6b21be7f48f730e27619c0890c881feced786385`. Run
[32626589535](https://github.com/TaeeunKil/kafrust/actions/runs/32626589535)
executed the exact `kafrust 0.3.6` / `kafrust-protocol 0.3.6` pair in twelve
fresh external Cargo projects against the accepted single-node Kafka matrix.

## Result and retained artifacts

All twelve jobs passed. The workflow uploaded 24 files with 90-day retention:
one external `Cargo.lock` and one captured `published-smoke-output.txt` per
profile. The downloaded run contained 12 lockfiles and 12 output files, with
286,576 total bytes. Each lockfile resolves the exact requested `0.3.6`
client and protocol pair without a workspace path or patch source.

The profiles were:

- Kafka 3.7.2 classic PLAINTEXT;
- Kafka 3.8.1, 3.9.1, and 4.0.0 classic PLAINTEXT;
- Kafka 4.3.1 KIP-848 consumer PLAINTEXT;
- Kafka 3.7.2 SASL_PLAINTEXT/PLAIN;
- Kafka 3.7.2 SASL_SSL/SCRAM-SHA-256 and SCRAM-SHA-512; and
- Kafka 3.7.2 gzip, snappy, lz4, and zstd codec profiles.

An earlier validation run, [32626452478](https://github.com/TaeeunKil/kafrust/actions/runs/32626452478),
had 11 successful jobs and one Kafka 3.9.1 fixture readiness failure
(`published admin list did not return the created topic`). It is retained as a
failure diagnostic and is not included in the passing evidence claim; the
clean run above supersedes it for this workflow change.

## Qualification boundary

This proves reproducible retained published-row evidence for the listed smoke
profiles. It does not close the complete V1-20 matrix, long fault/SLO gates,
V1-23 service canary, API freeze, RC, or stable release. It records no
latency/RSS/retry SLO result and does not authorize a version bump.
