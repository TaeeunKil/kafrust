# Current-source Live Kafka Smoke (2026-09-04)

- source_commit: `ce4719b17dc1f62cc8d5ee46a56a1d7b61493e6f`
- workflow: [Live Kafka Smoke run 33799054637](https://github.com/TaeeunKil/kafrust/actions/runs/33799054637)
- evidence level: Live current-source
- toolchains: workflow Rust 1.81.0 source checkout
- broker images: Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1
- topology: single-node and three-broker KRaft profiles as named by each job
- security: PLAINTEXT, TLS, SASL_PLAINTEXT, SASL_SSL SCRAM, OAUTHBEARER,
  signed OAUTHBEARER, and ACL authorization profiles

## Result

All 17 jobs completed successfully:

- Kafka 3.7.2: plaintext, TLS, SASL_PLAINTEXT, SASL_SSL SCRAM,
  SASL_SSL OAUTHBEARER, signed OAUTHBEARER, ACL authorizer, three-broker,
  three-broker SASL_PLAINTEXT failover, and three-broker SASL_SSL SCRAM
  failover.
- Kafka 3.8.1 and 3.9.1: plaintext matrix jobs.
- Kafka 4.3.1: plaintext, KIP-848, three-broker KIP-848 failover, and
  three-broker SASL_PLAINTEXT/SASL_SSL SCRAM KIP-848 failover.

The workflow's configured checks covered broker roundtrips, producer and
buffered producer paths, direct and classic/KIP-848 consumer lifecycles,
Admin operations, codecs, authentication, transaction response-loss
reconciliation, and multi-broker leader/coordinator movement. No job failed
and no local workstation resource was involved.

## Boundary

This is a source-only short matrix refresh. It does not replace exact
published-artifact lockfile rows, the full V1-20 matrix, six-hour/24-hour
fault campaigns, five-repetition SLO campaigns, service-canary evidence, or
release authorization.
