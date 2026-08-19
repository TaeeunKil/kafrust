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
renewal, and provides a high-level `ShareConsumer` runtime. The API is pre-1.0
and is not included in
the published `0.3.0` crate until a release gate is completed.

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
- a Kafka 4.3.1 single-node live gate passing the complete poll/Renew/poll,
  acquisition-lock expiry/redelivery, Accept/commit, and close workflow in
  [run 32213499877](https://github.com/TaeeunKil/kafrust/actions/runs/32213499877).

The live gate is wired in
`.github/workflows/share-kafka-smoke.yml`. It starts Kafka 4.3.1 with the
single-node share-state settings, produces a record, and runs the configured
ShareConsumer poll/Renew/poll/expiry-redelivery/Accept/commit/close path. The
passing run above establishes single-node Kafka 4.3.1 evidence only.

This evidence proves the client-side wire and state-machine slice plus the
single-node Kafka 4.3.1 path. It does not prove multi-broker leader movement,
coordinator recovery, or production readiness.

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
is an intentional safety boundary and a 1.0 exit criterion.

## Lifecycle and Limits

`poll()` currently performs a metadata refresh and uses one ShareFetch session
per partition leader. Share session epochs are advanced only after a successful
response; a final epoch of `-1` is used when closing a session. `close()` first
releases unacknowledged records, closes share sessions, and leaves the group.

The runtime has an opt-in detached background heartbeat task. An application may
call `spawn_heartbeat_task(interval)` when record processing can exceed the
broker-provided heartbeat interval. The task can be checked with
`try_wait_heartbeat_task()` and stopped with `stop_heartbeat_task()` before
shutdown. It uses a dedicated coordinator connection and bounded retry, and
foreground heartbeat failures now rediscover the group coordinator instead of
reconnecting only to a stale address. Live coordinator movement, connection
replacement after every ambiguous share operation, and long-running
assignment/rebalance tests remain open hardening work.

The implementation reuses kafrust's bounded fetch decoder, including record
batch decompression, header decoding, and configured response/decompression
limits. A ShareFetch response is still untrusted broker input and is subject to
the same array, frame, and record-size limits as the direct consumer path.

## Release Gate

Before this API can be advertised as a production replacement, the project must
complete all of the following:

- multi-broker live Kafka qualification with leader movement and coordinator
  recovery (single-node Kafka 4.3.1 is verified in the run above);
- live background-heartbeat ownership, cancellation, shutdown, and coordinator
  recovery behavior (the local in-flight cancellation path is covered);
- duplicate, delayed, and response-loss acknowledgement reconciliation;
- live renewal expiry, redelivery, and acquisition-lock timeout behavior across
  multiple brokers and repeated runs;
- long-running share-group soak and resource/backpressure measurements;
- stable public API review and a `rust-rdkafka` migration example for queue
  workloads.

Until those gates pass, use the low-level Share API for protocol evaluation or
the high-level runtime only in controlled tests and internal experiments.
