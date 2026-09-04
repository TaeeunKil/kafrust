# Current-source Live Kafka Smoke rerun (2026-09-04)

- source_commit: `3c27e61820b7fb53450996d09a79c9278c8764e8`
- workflow: [Live Kafka Smoke run 33820740402](https://github.com/TaeeunKil/kafrust/actions/runs/33820740402)
- evidence level: Live current-source
- toolchains: workflow Rust 1.81.0 and stable matrix jobs
- broker images: Apache Kafka tags 3.7.2, 3.8.1, 3.9.1, and 4.3.1
- topology: single-node and three-broker KRaft profiles as named by each job
- security: PLAINTEXT, TLS, SASL_PLAINTEXT, SASL_SSL SCRAM, OAUTHBEARER,
  signed OAUTHBEARER, and ACL authorization profiles
- duration: 7 minutes 31 seconds from workflow creation to completion

## Result

All 17 jobs completed successfully with zero failures:

- Kafka 3.7.2: plaintext, TLS, SASL_PLAINTEXT, SASL_SSL SCRAM,
  SASL_SSL OAUTHBEARER, signed OAUTHBEARER, ACL authorizer, three-broker,
  three-broker SASL_PLAINTEXT failover, and three-broker SASL_SSL SCRAM
  failover.
- Kafka 3.8.1 and 3.9.1: plaintext matrix jobs.
- Kafka 4.3.1: plaintext, KIP-848, three-broker KIP-848 failover, and
  three-broker SASL_PLAINTEXT/SASL_SSL SCRAM KIP-848 failover.

The workflow exercised broker roundtrips, producer and buffered producer
paths, direct and classic/KIP-848 consumer lifecycles, Admin operations,
codecs, authentication, transaction response-loss reconciliation, and
multi-broker leader/coordinator movement. The run used hosted GitHub runners;
it did not consume the company WSL Docker environment.

## Boundary

This is a current-source short matrix refresh. It does not replace exact
published-artifact lockfile rows, the full V1-20 matrix, six-hour/24-hour
fault campaigns, five-repetition SLO campaigns, service-canary evidence, or
release authorization.
