# V1-22 Published Profile-Mode Diagnostic — 2026-08-23

## Run identity

- Workflow: [32629657794](https://github.com/TaeeunKil/kafrust/actions/runs/32629657794)
- Source commit: `18d34e974b6935ed9800c829d55c84c4f0615239`
- Published client: `kafrust = "=0.3.6"` with the matching `kafrust-protocol`
- Runner: GitHub-hosted `ubuntu-latest`
- Broker matrix: Apache Kafka 3.7.2 and 4.3.1, single-node KRaft
- Security: PLAINTEXT
- Campaign mode: `buffered`
- Workload: two workers, 1-KiB values, batch size 50, none/Zstd matrix
- Timing: 5-second warmup, 20-second measured window, 5-second samples

## Outcome

All four matrix jobs passed. The fresh external project compiled against the
exact published `0.3.6` artifact, the bounded `BufferedProducer` path completed
its delivery handles, business-record identity reconciliation reported zero
loss/duplicates/unknown outcomes, and final in-flight/buffered gauges drained to
zero. The workflow retained one JSONL result and one descriptor per matrix job;
the descriptors remain explicitly `qualified: false`.

This is a published-artifact profile-path diagnostic. It does not qualify the
V1-22 six-profile × two-topology × two-security × five-repetition matrix, does
not establish a locked baseline or regression budget, and does not authorize a
`0.3.7`, release candidate, or `1.0.0` publication.
