# V1 live Share acknowledgement ambiguity — 2026-09-04

- Source commit: `86f349bd5ce9d475fe5e1df1cfe8a238953eebf3`
- Workflow: [Live Kafka Share Acknowledgement Ambiguity](https://github.com/TaeeunKil/kafrust/actions/runs/33855859702)
- Broker: Apache Kafka 4.3.1, single-node KRaft with Share groups enabled
- Security: PLAINTEXT
- Client: current source checkout, Rust 1.81.0
- Test: `share_consumer_reconciles_lost_release_response_when_broker_is_configured`

The workflow seeded one Share record, routed the client through a response-drop
proxy for ShareAcknowledge API key 79, and verified the response-loss
reconciliation test. The broker-roundtrip test passed (`1 passed; 0 failed`)
and the drop marker was present, proving that the intended acknowledgement
response was actually discarded. The workflow completed successfully in about
one minute and retained no credential or data artifact.

This is a bounded current-source diagnostic for the Release/unknown-outcome
boundary. It is not the published artifact, the secure two-member/three-broker
matrix, the 10,000-record/20-cycle gate, a long campaign, a service canary, or
release evidence.
