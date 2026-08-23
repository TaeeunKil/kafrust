# V1-21 Published Fault Diagnostic Evidence

This document records the first published-artifact execution of the V1-21
segmented fault-campaign scaffold. It is diagnostic evidence only; it does not
close the six-hour, 100-cycle, ambiguity-family, data-loss, or continuity
requirements in the V1-21 milestone.

## Final bounded segment

- artifact: `kafrust 0.3.6` and `kafrust-protocol 0.3.6`
- source_commit: `f3a76745a4ff7ad891ab7e8d479b3da823451ab7`
- workflow: [32618344222](https://github.com/TaeeunKil/kafrust/actions/runs/32618344222)
- uploaded descriptor: [artifact 9487644196](https://github.com/TaeeunKil/kafrust/actions/runs/32618344222/artifacts/9487644196)
- campaign_id: `member-loss-rejoin-cycles` (identity scaffold exercise only)
- segment: `0/1`
- broker: Kafka 4.3.1, three-broker KRaft, PLAINTEXT
- broker image: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- fault: stop broker 1 after one-third of a 60-second run; restart after 10 seconds
- result: passed bounded produce/fetch recovery smoke
- records: 2,468,400
- operation errors: 0
- retries: 2
- final gauges: `in_flight_requests=0`, `buffered_records=0`
- descriptor artifact digest: `37c7c0c76b31e34af838650acfb4fc351f1e63e4ed6f24dcefb56d2a39a745c7`
- record reconciliation: this historical descriptor predates the current
  per-segment business-ID fields; the current fixture emits qualified
  per-segment reconciliation, while every runner-local descriptor still uses
  `continuity_claim="not-qualified; runner-local broker segment"` because it
  does not prove cross-segment continuity.

The workflow now accepts `campaign_id`, `segment_index`, and `segment_count`,
raises the job timeout ceiling for future six-hour segments, records exact
artifact/workflow/broker identities, and rejects invalid segment ranges. The
fixture uses `Acks::All` for replicated-topic diagnostics and emits correctly
ordered resource gauges.

## Retained failed diagnostics

- [32617622923](https://github.com/TaeeunKil/kafrust/actions/runs/32617622923)
  used `Acks::Leader` and stopped with 616,300 produced versus 616,200
  consumed after the broker restart; the last 100 records were not reconciled
  before the 300-second drain deadline. This exposed why replicated fault
  qualification must declare `acks` and `min.insync.replicas`.
- [32618011465](https://github.com/TaeeunKil/kafrust/actions/runs/32618011465)
  used `Acks::All` and completed the data roundtrip, but failed the workflow
  gauge assertion because the fixture serialized `max_in_flight_requests` as
  `buffered_records`. The formatter was corrected before the passing run.

These failures remain immutable and are not averaged away. The final bounded
run is not evidence of continuous cross-run record identity because every
segment starts a fresh runner-local broker. The current descriptor contract
separates qualified per-segment identity from the still-unqualified
cross-segment continuity claim.

## Secure simultaneous-loss segment after retry fix

The secure fixture initially exposed a harness contract bug rather than a
client data-loss result. Runs [32632600261](https://github.com/TaeeunKil/kafrust/actions/runs/32632600261)
and [32633284143](https://github.com/TaeeunKil/kafrust/actions/runs/32633284143)
stopped two brokers simultaneously and reported identity gaps after repeated
`NOT_ENOUGH_REPLICAS` responses. The fixture had advanced its next sequence
before a failed batch and discarded the records, so it converted unresolved
outcomes into apparent loss. Commit `15741d8` enables idempotence for both
published multi-soak fixtures, retains failed batches for replay, advances the
sequence only after a successful metadata result, and fails explicitly if a
pending unknown outcome remains at the hard deadline.

The corrected secure segment passed in
[32633658046](https://github.com/TaeeunKil/kafrust/actions/runs/32633658046)
using the exact published `kafrust 0.3.6` / `kafrust-protocol 0.3.6` pair:

- Kafka 4.3.1, three-broker KRaft, SASL_SSL/SCRAM-SHA-256
- simultaneous stop of brokers 1 and 2 after one-third of a 180-second segment;
  10-second outage
- 6,089,400 attempted, acknowledged, and consumed unique 1-KiB records
- 300 operation errors, 2 failed requests, 5 client retries, and 30,000
  transient unknown attempts, all recovered by replaying retained batches
- zero loss, zero duplicates, matching identity digest, `recovered=true`, and
  final `in_flight_requests=0` / `buffered_records=0`
- published artifact digest and broker image digest retained in the uploaded
  immutable descriptor

This is stronger secure per-segment recovery evidence and validates the
unknown-outcome handling in the fixture. It is still diagnostic: the runner is
local to one segment, `continuity_claim` remains unqualified, and the six-hour,
100-cycle, ambiguity-family, controlled-data-loss, and repeated-campaign gates
remain open.
