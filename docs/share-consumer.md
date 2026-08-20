# Share Consumer

`ShareConsumer` is kafrust's alpha high-level runtime for Kafka Share Groups
from [KIP-932](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/255070434/KIP-932%2BQueues%2Bfor%2BKafka).
It is a separate API from `Consumer` and `ConsumerGroup`: a share group acquires
individual records from a broker-held work queue instead of advancing a
partition offset.

## Status

The current development branch implements the stable KIP-932 v1 wire shape for
`ShareGroupHeartbeat`, `ShareFetch`, and `ShareAcknowledge`, plus the optional
KIP-1206 ShareFetch v2 acquisition mode and KIP-1222 ShareAcknowledge v2
renewal, and provides a high-level `ShareConsumer` runtime. The API is pre-1.0.
The published `0.3.3` crate now has a fresh external single-node runtime and
acknowledgement-soak qualification, but this does not make the API stable or
complete.

Current evidence:

- protocol encode/decode tests for flexible headers, session epochs, nullable
  records, acquired ranges, acknowledgement batches, and node endpoints;
- a duplex-broker test covering Metadata v12, ShareFetch v1, record decoding,
  and ShareAcknowledge v1 on the actual `Client` request path;
- local acknowledgement-state tests for explicit and grouped acknowledgement
  behavior.
- KIP-1222 v2 request/response fixtures for renewal acknowledgement and the
  acquisition-lock timeout, plus local renewal state retention tests.
- an opt-in cancellable background heartbeat task using a dedicated coordinator
  connection, including a test that cancels an in-flight heartbeat request;
- a reusable in-process fault-injection gate using the public
  `ShareConsumerConfig::build()`, `poll()`, `acknowledge()`, and `commit()` path:
  a dropped `ShareAcknowledge` response is classified as
  `ShareAcknowledgementOutcomeUnknown`, is not retried, and leaves exactly one
  record pending reconciliation; the public reconciliation path then observes
  broker redelivery and completes a replacement `Accept`.
- a Kafka 4.3.1 single-node live gate passing the complete poll/Renew/poll,
  acquisition-lock expiry/redelivery, Accept/commit, and close workflow in
  [run 32213499877](https://github.com/TaeeunKil/kafrust/actions/runs/32213499877).
- a Kafka 4.3.1 three-broker live gate passing pre-failover acceptance, broker 1
  leader loss, replacement leader election, and post-failover acceptance by a
  fresh ShareConsumer in
  [run 32214201983](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983).
- a Kafka 4.3.1 three-broker live gate passing coordinator loss while an active
  background heartbeat task was running. The task recovered after broker 1
  stopped, the selected partition moved to broker 2, the post-failover record
  was received and accepted, and shutdown completed cleanly in
  [run 32215845737](https://github.com/TaeeunKil/kafrust/actions/runs/32215845737).
- three independent Kafka 4.3.1 active-heartbeat failover attempts passing in
  the matrix run
  [32216383214](https://github.com/TaeeunKil/kafrust/actions/runs/32216383214),
  each receiving, accepting, and cleanly closing after coordinator loss.
- a repeated Kafka 4.3.1 coordinator-churn gate passing three consecutive
  coordinator-loss/recovery cycles inside each long-running ShareConsumer
  process; all three matrix attempts passed in
  [run 32219147942](https://github.com/TaeeunKil/kafrust/actions/runs/32219147942).
- current-source revalidation on commit `35e7cec` passing the three-broker
  leader-failover path in
  [run 32356279940](https://github.com/TaeeunKil/kafrust/actions/runs/32356279940)
  and all three active-heartbeat coordinator-loss attempts in
  [run 32356280155](https://github.com/TaeeunKil/kafrust/actions/runs/32356280155).
- current-source bounded acknowledgement soak on Kafka 4.3.1 passing 64
  independently seeded records with `max_records(1)`, one acknowledgement and
  commit per record, unique accepted values and offsets, and clean close in
  [run 32369562416](https://github.com/TaeeunKil/kafrust/actions/runs/32369562416).
- published `kafrust 0.3.3` passing a fresh external Kafka 4.3.1 runtime that
  received the seeded record, validated its value and offset, accepted and
  committed it, stopped the heartbeat task, and closed cleanly in
  [run 32384767744](https://github.com/TaeeunKil/kafrust/actions/runs/32384767744).
- published `kafrust 0.3.3` passing a fresh external Kafka 4.3.1
  acknowledgement soak with 64 independently seeded records, one-at-a-time
  `Accept` and `commit` operations, unique value/offset reconciliation, clean
  heartbeat shutdown, clean close, and lockfile verification in
  [run 32385522647](https://github.com/TaeeunKil/kafrust/actions/runs/32385522647).
- published `kafrust 0.3.3` passing a fresh external three-broker Kafka 4.3.1
  leader-failover path: a pre-failover record was accepted, broker 1 was
  stopped, replacement leadership was observed, and a post-failover record was
  produced and accepted through surviving bootstrap servers in
  [run 32386637555](https://github.com/TaeeunKil/kafrust/actions/runs/32386637555).
- published `kafrust 0.3.3` passing three consecutive external coordinator-loss
  cycles on a three-broker Kafka 4.3.1 cluster. The workflow dynamically stopped
  coordinators 1, 3, and 1, produced through surviving bootstrap servers, and
  verified the heartbeat task remained alive through each recovery in
  [run 32387564503](https://github.com/TaeeunKil/kafrust/actions/runs/32387564503).
- published `kafrust 0.3.3` passing a bounded two-member ownership gate on a
  fresh external three-broker Kafka 4.3.1 cluster. Two members joined the same
  Share group, each accepted three records, and the six seeded partitions were
  observed exactly once across the members in
  [run 32388813780](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780).
- The same published two-member gate also passed a 60-second run with 64
  records seeded into each of six partitions. Each member accepted 192
  records; the workflow verified all 384 records, exact per-partition counts,
  and global `(partition, offset)` uniqueness in
  [run 32389641275](https://github.com/TaeeunKil/kafrust/actions/runs/32389641275).
- The metrics-enabled version of that published 60-second gate passed with
  the same 384-record reconciliation. Each member reported `consumed=192`,
  `in_flight=0`, and no failed requests; the observed retry counters were 5
  and 4 during normal multi-member coordination in
  [run 32391918666](https://github.com/TaeeunKil/kafrust/actions/runs/32391918666).
- A published member-loss gate then force-terminated member 2 after both
  heartbeat tasks were active. The surviving member rebalanced to all six
  partitions and accepted one record from each while member 2 produced no
  output, in [run 32390219711](https://github.com/TaeeunKil/kafrust/actions/runs/32390219711).
- A repeated published churn gate completed two forced-loss cycles in one
  three-broker cluster: member 1 took over all six partitions after member 2
  stopped, member 2 rejoined, and then member 2 took over all six after member
  1 stopped. Twelve records were reconciled without duplicate offsets in
  [run 32391027028](https://github.com/TaeeunKil/kafrust/actions/runs/32391027028).
- The same published group then passed three forced-loss cycles in one
  three-broker cluster: ownership moved to member 1, then member 2, then a
  rejoined member 1 again. Eighteen records were reconciled with three records
  per partition and unique offsets; the final survivor reported
  `consumed=6`, `in_flight=0`, and no failed requests in
  [run 32392994232](https://github.com/TaeeunKil/kafrust/actions/runs/32392994232).

The live gate is wired in
`.github/workflows/share-kafka-smoke.yml`. It starts Kafka 4.3.1 with the
single-node share-state settings, produces a record, and runs the configured
ShareConsumer poll/Renew/poll/expiry-redelivery/Accept/commit/close path. The
passing runs establish the single-node lifecycle and one replicated
leader-failover path on Kafka 4.3.1. They do not establish repeated failure
recovery, long-running multi-broker ownership, or production readiness.

The multi-broker gate is wired in
`.github/workflows/share-kafka-multi-broker-smoke.yml`. It starts a replicated
Kafka 4.3.1 KRaft cluster, selects a partition led by broker 1, consumes and
accepts a pre-failover record, stops broker 1, waits for a replacement leader,
and then verifies that a fresh ShareConsumer using the same group reads and
accepts a post-failover record from the surviving brokers. This workflow is a
manual and scheduled qualification gate. The original successful run is
recorded in [run 32214201983](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983);
the current-source revalidation passed in
[run 32356279940](https://github.com/TaeeunKil/kafrust/actions/runs/32356279940).

The active-heartbeat gate is wired in
`.github/workflows/share-kafka-heartbeat-failover.yml`. It joins a three-broker
cluster, starts the detached heartbeat task, discovers the actual group
coordinator, stops that broker, and verifies post-failover delivery and
acknowledgement while the heartbeat task remains active. Kafka 4.3.1 passed
the path in [run
32215845737](https://github.com/TaeeunKil/kafrust/actions/runs/32215845737),
and three independent matrix attempts passed in [run
32216383214](https://github.com/TaeeunKil/kafrust/actions/runs/32216383214).
The same three-attempt matrix was revalidated on the current source in [run
32356280155](https://github.com/TaeeunKil/kafrust/actions/runs/32356280155).

This evidence proves the client-side wire and state-machine slice, the
deterministic response-loss safety boundary, the single-node Kafka 4.3.1
lifecycle, three-broker leader movement, repeated coordinator churn within one
long-running process, bounded acknowledgement progress, bounded two-member
partition ownership, published-artifact runtime/soak behavior on one
single-node profile, and live ambiguous acknowledgement reconciliation. It
also proves that the bounded published multi-member run's consumed-record
counter matched accepted records and that all request connections drained
before close. It does not prove long-running multi-broker ownership,
 higher-cycle dynamic assignment/member-loss/rebalance behavior beyond this
 three-cycle profile, broad published-artifact coverage, or production
 readiness.

## Basic Usage

```rust
use kafrust::{ShareAcknowledgementType, ShareConsumerConfig};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut consumer = ShareConsumerConfig::new(
        ["localhost:9092", "localhost:9093"],
        "orders-workers",
    )
    .subscribe("orders")
    .build()
    .await?;

    loop {
        let records = consumer.poll().await?;
        for record in &records {
            if process(record.value()).is_ok() {
                consumer.acknowledge(record, ShareAcknowledgementType::Accept)?;
            } else {
                consumer.acknowledge(record, ShareAcknowledgementType::Release)?;
            }
        }
        consumer.commit().await?;
    }
}
```

`ShareConsumerConfig::build` validates the configuration, negotiates the three
Share APIs and the highest compatible ShareFetch/ShareAcknowledge versions,
discovers the group coordinator, joins the share group, and refreshes metadata.
The builder accepts the same bootstrap, TLS, SASL, timeout, response limit,
decode-limit, and metrics settings as the other high-level clients.

## Acknowledgement Contract

The default mode is `ShareAcknowledgementMode::Explicit`:

1. `poll()` returns acquired records and records them as pending locally.
2. The application calls `acknowledge` once for every returned record.
3. `commit()` sends typed acknowledgement batches to each partition leader.
4. The pending entries are removed only after a successful broker response.

The acknowledgement types map directly to Kafka's KIP-932 values:

| Rust value | Kafka value | Meaning |
| --- | ---: | --- |
| `Gap` | `0` | Leave the record eligible for another delivery. |
| `Accept` | `1` | Mark the record successfully processed. |
| `Release` | `2` | Return the record for a later delivery attempt. |
| `Reject` | `3` | Reject the record permanently. |
| `Renew` | `4` | Extend the acquisition lock without completing the record. Requires ShareAcknowledge v2. |

`Renew` is available only in explicit acknowledgement mode. A successful renew
keeps the record pending and makes it eligible to be returned by a later
`poll()` until the application completes it with `Accept`, `Release`, or
`Reject`. The most recent broker-provided acquisition lock timeout is available
through `ShareConsumer::acquisition_lock_timeout_ms()`. When a pending Renew is
flushed by `poll()`, kafrust uses ShareFetch v2 with `IsRenewAck=true` and zero
fetch limits, so the renewal request does not acquire unrelated records. When
the application calls `commit()` directly, kafrust uses ShareAcknowledge v2.

If the acquisition lock expires before completion, a broker redelivery of the
same topic/partition/offset replaces the retained Renew record and updates its
delivery count. An ordinary duplicate while no Renew state exists remains an
error instead of being silently delivered twice. Live expiry and redelivery
qualification passed on the single-node Kafka 4.3.1 gate; multi-broker and
long-running qualification are still required before this behavior is a
production claim.

`ShareAcknowledgementMode::Implicit` accepts unacknowledged records from the
previous poll before the next poll or commit. In implicit mode, calling
`acknowledge` is an error. Choose explicit mode when processing failures,
poison-message handling, or retry policy must be visible to the application.

## Acquisition Mode

`ShareAcquireMode::BatchOptimized` is the default and preserves KIP-932's
batch-oriented behavior. The broker may acquire more records than `max_records`
when it completes a record batch. `ShareAcquireMode::RecordLimit` uses
KIP-1206's ShareFetch v2 field so the broker does not acquire more than the
configured limit. It fails during `build()` when the broker advertises only
ShareFetch v1 instead of silently changing the requested processing semantics.

Kafka may return the complete record batch even when only some offsets in that
batch are acquired by this consumer. `AcquiredRecords` is therefore the
authoritative delivery subset: kafrust decodes the batch for framing and
limits, but returns only messages whose offsets fall inside an acquired range.
Messages outside those ranges are not errors and must not be acknowledged by
the application.

```rust
use kafrust::{ShareAcquireMode, ShareConsumerConfig};

let config = ShareConsumerConfig::new(["localhost:9092"], "orders-workers")
    .subscribe("orders")
    .acquire_mode(ShareAcquireMode::RecordLimit);
```

An acknowledgement response is not replayed automatically after a transport
failure. If the request may have been transmitted, `commit()` returns
`ShareAcknowledgementOutcomeUnknown` and leaves the records pending. The broker
may have applied the acknowledgement before the response was lost, so callers
must reconcile the broker-side state before choosing whether to replay it. This
is an intentional safety boundary and a 1.0 exit criterion. After
`reconcile_acknowledgement_outcomes()`, the next `poll()` is allowed to fetch
the broker's authoritative redelivery; `commit()` remains blocked until that
redelivery clears the unknown state. Kafrust never replays the original
acknowledgement automatically.

## Lifecycle and Limits

`poll()` currently performs a metadata refresh and uses one ShareFetch session
per partition leader. Share session epochs are advanced only after a successful
response; a final epoch of `-1` is used when closing a session. `close()` first
releases unacknowledged records, closes share sessions, and leaves the group.
If a prior `ShareAcknowledge` response was lost, shutdown skips that unknown
acknowledgement rather than replaying it, still performs the session/group
cleanup, and returns `ShareAcknowledgementOutcomeUnknown` after cleanup. Call
`reconcile_acknowledgement_outcomes()` before shutdown when the application
needs to observe broker redelivery and finish that record explicitly.

The runtime has an opt-in detached background heartbeat task. An application may
call `spawn_heartbeat_task(interval)` when record processing can exceed the
broker-provided heartbeat interval. The task can be checked with
`try_wait_heartbeat_task()` and stopped with `stop_heartbeat_task()` before
shutdown. It uses a dedicated coordinator connection and bounded retry, and
foreground heartbeat failures now rediscover the group coordinator instead of
reconnecting only to a stale address. Bootstrap reconnect attempts rotate
through configured broker addresses so a broker that accepts TCP but resets
Kafka requests does not consume the entire retry budget. The multi-broker
workflow now exercises three consecutive coordinator-loss/recovery cycles in
one process. The deterministic public response-loss gate and live Kafka 4.3.1
response-loss gate now cover unknown-outcome classification and redelivery
recovery. The 64-record live acknowledgement soak also passes;
multi-broker long-running ownership, dynamic assignment/rebalance behavior,
and resource/backpressure measurements remain open hardening work. A bounded
published two-member ownership gate passed separately in
[run 32388813780](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780).

The implementation reuses kafrust's bounded fetch decoder, including record
batch decompression, header decoding, and configured response/decompression
limits. A ShareFetch response is still untrusted broker input and is subject to
the same array, frame, and record-size limits as the direct consumer path.

The dedicated
`.github/workflows/share-kafka-acknowledgement-soak.yml` workflow seeds 64
records into Kafka 4.3.1 and repeatedly polls, acknowledges, and commits them
with `RecordLimit`. It is a long-running operational gate for complete-batch
filtering and ordinary acknowledgement progress; ambiguous response-loss
reconciliation is exercised by the separate
`.github/workflows/share-kafka-acknowledgement-ambiguity.yml` gate. That gate
drops the first `ShareAcknowledge` response for a `Release`, requires the client
to classify the outcome as unknown without replaying it, then verifies broker
redelivery, replacement `Accept`, and successful completion.
The current-source ordinary acknowledgement soak passed in
[`32346739498`](https://github.com/TaeeunKil/kafrust/actions/runs/32346739498),
and again from the current `main` commit in
[`32355746726`](https://github.com/TaeeunKil/kafrust/actions/runs/32355746726).
The published-artifact acknowledgement soak passed for `kafrust 0.3.3` in
[`32385522647`](https://github.com/TaeeunKil/kafrust/actions/runs/32385522647).
The live response-loss reconciliation gate passed in
[`32347035522`](https://github.com/TaeeunKil/kafrust/actions/runs/32347035522),
and again from the current `main` commit in
[`32355746798`](https://github.com/TaeeunKil/kafrust/actions/runs/32355746798).

## Release Gate

Before this API can be advertised as a production replacement, the project must
complete all of the following:

- repeated multi-broker live Kafka qualification with leader movement and
  coordinator recovery (a fresh-consumer path is verified in
  [run 32214201983](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983)
  and three independent active-heartbeat attempts in
  [run 32216383214](https://github.com/TaeeunKil/kafrust/actions/runs/32216383214),
  including three consecutive in-process churn cycles in
  [run 32219147942](https://github.com/TaeeunKil/kafrust/actions/runs/32219147942));
- live background-heartbeat ownership, cancellation, shutdown, and coordinator
  recovery behavior (three independent paths and three consecutive in-process
  coordinator-loss cycles are live-qualified, while multi-broker long-running
  ownership remains open);
- duplicate, delayed, and response-loss acknowledgement reconciliation (the
  response-loss path is live-qualified; delayed and duplicate delivery
  matrices remain open);
- the deterministic public response-loss gate in
  `crates/kafrust/tests/fault_injection.rs` must remain green as the live
  ambiguity workflow evolves;
- live renewal expiry, redelivery, and acquisition-lock timeout behavior across
  multiple brokers and repeated runs;
- response-loss fault injection through
  `.github/workflows/share-kafka-acknowledgement-ambiguity.yml` (passed on
  Kafka 4.3.1 in
  [run 32355746798](https://github.com/TaeeunKil/kafrust/actions/runs/32355746798));
- long-running multi-broker share-group soak and resource/backpressure
  measurements (the single-node 64-record soak passed in
  [run 32355746726](https://github.com/TaeeunKil/kafrust/actions/runs/32355746726));
- long-running published-artifact multi-broker Share ownership and
  resource/backpressure qualification beyond the tested `0.3.3` paths. A
  bounded two-member ownership/assignment run passed in
  [run 32388813780](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780),
  and the 384-record/60-second extension passed in
  [run 32389641275](https://github.com/TaeeunKil/kafrust/actions/runs/32389641275),
  and forced member-loss recovery passed in
  [run 32390219711](https://github.com/TaeeunKil/kafrust/actions/runs/32390219711),
  but longer backpressure and higher-cycle rebalance coverage remain open;
- stable public API review and a `rust-rdkafka` migration example for queue
  workloads.

Until those gates pass, use the low-level Share API for protocol evaluation or
the high-level runtime only in controlled tests and internal experiments.
