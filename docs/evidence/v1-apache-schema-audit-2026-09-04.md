# Apache Kafka schema audit (2026-09-04)

- source_commit: `a8199d66b75cae90db4de33b3f7db629a6b0eacc`
- workflow: [Apache Schema Audit run 33823046705](https://github.com/TaeeunKil/kafrust/actions/runs/33823046705)
- tool: `scripts/check_apache_schema_versions.py --online-all`
- broker schema baseline: Kafka 4.3.1
- result: passed

The hosted Ubuntu job checked 152 request/response schemas across the local
protocol modules against the online Apache Kafka 4.3.1 schema metadata. The
audit completed in 28 seconds. It emitted explicit coverage notes for local
versions below an Apache flexible boundary and for local ApiVersions v5 being
newer than the pinned Apache v4 ceiling; those notes are intentional coverage
classification, not failures.

This confirms schema identity/version metadata only. It does not prove every
response oracle, accepted-floor or three-broker behavior, published-artifact
compatibility, or release readiness.
