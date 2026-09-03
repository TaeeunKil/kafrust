# Current-Source Live Matrix Rerun (2026-09-03)

Run [33714444474](https://github.com/TaeeunKil/kafrust/actions/runs/33714444474)
passed all 17 current-source jobs from head
`c50420528f6e9aa11d938efc8bdc12e7efd2dc36` (`c504205`). The 7m27s hosted
workflow exercised the short live matrix across Kafka 3.7.2, 3.8.1, 3.9.1,
and 4.3.1, with single-node and three-broker KRaft fixtures.

Passing jobs covered:

- single-node plaintext smoke on all four broker lines;
- Kafka 3.7.2 TLS, SASL_PLAINTEXT, SASL_SSL/SCRAM, OAUTHBEARER,
  signed OAUTHBEARER, and ACL-authorizer paths;
- Kafka 4.3.1 KIP-848 consumer paths;
- Kafka 4.3.1 three-broker KIP-848 failover with SASL_PLAINTEXT and
  SASL_SSL/SCRAM;
- Kafka 3.7.2 three-broker plaintext, SASL_PLAINTEXT, and SASL_SSL/SCRAM
  failover paths.

The matrix executed broker roundtrips, producer/batch/buffered and
transactional paths, all four pure-Rust codecs, direct and group consumers,
regex and KIP-848 assignment, Admin operations, transaction ambiguity,
controller/coordinator/leader failover, delegation-token, ACL, and telemetry-
adjacent fixtures included by the workflow. Every job completed successfully;
no hosted runner, Docker, or repository resource was mutated by this record.

This is current-source short live evidence only. It is not the exact published
artifact matrix, a six-hour/24-hour fault campaign, an eight-hour/five-
repetition SLO campaign, a named V1-23 service canary, API-freeze/RC evidence,
or `1.0.0` release authorization.
