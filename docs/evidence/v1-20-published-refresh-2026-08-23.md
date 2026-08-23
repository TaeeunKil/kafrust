# V1-20 Published 0.3.6 Compatibility Refresh (2026-08-23)

This refresh records the exact published pair from source
`cf4429d7c643cbfe0046d5c3571a1a3b10f04573` without changing the release
version. Every external fixture resolved `kafrust 0.3.6` and
`kafrust-protocol 0.3.6` from crates.io.

## Published crate smoke

Run [32646370582](https://github.com/TaeeunKil/kafrust/actions/runs/32646370582)
passed all 12 profiles:

- Kafka 3.7.2 classic PLAINTEXT;
- Kafka 3.8.1, 3.9.1, and 4.0.0 classic PLAINTEXT;
- Kafka 4.3.1 KIP-848 PLAINTEXT;
- Kafka 3.7.2 SASL_PLAINTEXT/PLAIN;
- Kafka 3.7.2 SASL_SSL/SCRAM-SHA-256 and SCRAM-SHA-512; and
- Kafka 3.7.2 gzip, snappy, lz4, and zstd.

The run retained 12 external `Cargo.lock` files and 12 captured outputs. The
lockfiles resolve the registry package with `kafrust` checksum
`4fe2758d0093ef4b2a236090cca4dc7511b9e865f5e18ce42823e428a6be71d2`. Each
profile printed the published admin/topic lifecycle, idempotent producer,
transaction commit/abort, `read_committed`, direct consumer, group read, and
group commit/restore checks as passed.

## Published security and API refresh

- [Kafka 3.7.2 mutual TLS run 32646371786](https://github.com/TaeeunKil/kafrust/actions/runs/32646371786)
  passed the external `0.3.6` admin, producer, direct consumer,
  transaction/`read_committed`, and group/offset smoke.
- [Kafka 4.3.1 mutual TLS run 32646373388](https://github.com/TaeeunKil/kafrust/actions/runs/32646373388)
  passed the same published client checks.
- [OAUTHBEARER run 32646374747](https://github.com/TaeeunKil/kafrust/actions/runs/32646374747)
  printed `published oauthbearer ok brokers=1 produced_partition=0
  produced_offset=0 consumed=true token_provider=true`; the workflow also
  verified the external lockfile dependency version. Token material was not
  retained in logs or this record.
- [Kafka 4.3.1 API 74 run 32646376335](https://github.com/TaeeunKil/kafrust/actions/runs/32646376335)
  passed with `api74 list_version=1 resource_type=2 resources=1` and
  `describe_documentation=true entries=33`.

## Qualification boundary

These are short published-artifact smoke rows and a registry-resolution
refresh. They do not close the complete V1-20 compatibility matrix, the
six-hour fault campaigns, the six-profile performance SLO campaign, fuzz
weekly gates, the named V1-23 service canary, API freeze, release candidate,
or `1.0.0` release. They contain no long-duration latency, RSS, retry-budget,
rotation, or production-migration claim. The latest published version remains
`0.3.6`; no version bump is authorized by this refresh.
