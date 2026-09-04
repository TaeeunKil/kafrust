# V1-20 Published Smoke Refresh (2026-09-04)

Run [33844458915](https://github.com/TaeeunKil/kafrust/actions/runs/33844458915)
executed the immutable registry pair `kafrust 0.3.6` /
`kafrust-protocol 0.3.6` from fresh external Cargo projects. The workflow ran
from pushed source `b990f6df20344dacd4c8773b4276e148fd255e3a` (`b990f6d`);
the published package bytes remain the pair recorded in
[`v1-20-published-0.3.6-boundary-2026-08-23.md`](v1-20-published-0.3.6-boundary-2026-08-23.md).

All twelve matrix jobs passed and retained one external `Cargo.lock` plus one
captured fixture output per profile (12 artifacts, 90-day retention). The
downloaded artifact audit found 12 exact `0.3.6` client/protocol lockfile pairs
and 12 successful `published kafrust verified` output markers.

| Profile family | Profiles |
| --- | --- |
| Broker/group | Kafka 3.7.2 classic; 3.8.1 classic; 3.9.1 classic; 4.0.0 classic; 4.3.1 KIP-848 consumer |
| Security | Kafka 3.7.2 SASL_PLAINTEXT/PLAIN; SASL_SSL/SCRAM-SHA-256; SASL_SSL/SCRAM-SHA-512 |
| Codec | Kafka 3.7.2 gzip; snappy; lz4; zstd |

Each profile exercised the published admin/topic lifecycle, idempotent
produce, transaction commit/abort, read-committed consumption, direct consumer,
group read, group commit/restore, and the selected codec or security path.

This is a published-artifact smoke refresh only. It does not close the complete
V1-20 accepted matrix, mechanism-specific security rows, repeated fault
campaigns, V1-21/V1-22 long-duration or SLO gates, the V1-23 service canary,
API freeze, RC, or `1.0.0` release readiness.
