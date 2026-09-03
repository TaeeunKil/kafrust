# Producer Buffering And Linger Design

This note defines the intended M10 direction for buffered producer sends. It is a design document, not a public API reference. The implemented public API remains documented in [Producer API Direction](producer-api.md).

## Goal

Buffered sends should let callers enqueue single records while kafrust batches records by Kafka topic and partition before producing them. The design should keep Kafka behavior visible: linger controls how long records may wait for a fuller batch, batch limits control when a Produce request is cut, and each input record receives a delivery result.

## Non-Goals

- Do not change `Producer::send`, `Producer::send_batch`, or `Producer::send_batch_report` into background-buffered APIs.
- Do not hide Kafka topic, partition, acknowledgements, offsets, or broker errors behind generic queue terminology.
- Do not add serialization adapters, compression, transactions, idempotence, or `acks=0` behavior as part of the first buffered producer slice.
- Do not introduce librdkafka, C bindings, or a required C toolchain.

## Proposed Shape

The first buffered API should be opt-in and separate from the immediate producer path:

```rust
let mut producer = ProducerConfig::new(["localhost:9092"])
    .client_id("orders-api")
    .linger_ms(5)
    .max_records_per_batch(500)
    .max_batch_bytes(64 * 1024)
    .buffer_capacity(1024)
    .acks(Acks::All)
    .build_buffered()
    .await?;

let delivery = producer.send(ProducerRecord::to("orders").key("order-123").value("created")).await?;
let metadata = delivery.await?;
println!("{}-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());

producer.flush().await?;
producer.close().await?;
```

Names can change during implementation, but the behavioral split should remain:

- `Producer` remains an immediate request-oriented API.
- `BufferedProducer` owns the background queue and batching task.
- `BufferedProducer::send` enqueues one `ProducerRecord` and returns a delivery handle for that input record.
- `BufferedProducer::flush` waits until all records accepted before the flush have terminal delivery outcomes.
- `BufferedProducer::close` flushes, stops the background task, and rejects later sends.
- Dropping the owning `BufferedProducer` aborts its background task so it is not
  detached; pending delivery handles resolve as canceled. Call `close()` when
  accepted records must be flushed gracefully.

Current implementation status: `ProducerConfig::linger_ms`, `ProducerConfig::buffer_capacity`, `ProducerConfig::build_buffered`, `BufferedProducer::send`, `BufferedProducer::flush`, `BufferedProducer::close`, `BufferedProducer::is_closed`, and per-record `ProducerDelivery` handles exist. `flush`, `close`, linger expiry, record-count thresholds, and byte-count thresholds send accepted records through the existing `send_batch_report` path and complete delivery handles from per-record outcomes. The command queue defaults to 1024 records, applies async backpressure at capacity, and reports current and maximum outstanding buffered records through `ClientMetrics`.

For concurrent non-transactional enqueueing, `BufferedProducer::handle` returns
a cloneable `BufferedProducerHandle` that shares the same bounded command queue.
The owner must stop using all handles before `close`; transactional buffered
producers reject handles so transaction boundaries remain ordered through the
owner.

Transactional configurations use the same worker and queue. Begin, group-offset
attachment, commit, and abort are serialized with sends. Commit requires a
successful flush of all accepted records; partial delivery failures leave the
transaction active for abort. Close rejects an active transaction.

## Flush Triggers

The background batching task flushes pending records when any of these happens:

- a topic and explicit-partition buffer reaches `ProducerConfig::max_records_per_batch`
- a topic and explicit-partition buffer reaches `ProducerConfig::max_batch_bytes`, measured by encoded Kafka record-set bytes
- the oldest pending record reaches `ProducerConfig::linger_ms`
- the caller requests `flush`
- the producer is closed

`linger_ms(0)` should mean no intentional waiting. It can still batch records that are already queued when the background task drains the channel.

The underlying `send_batch_report` path still resolves metadata and enforces Produce request chunks per resolved topic, partition, and leader.

## Delivery Semantics

Each accepted record should complete exactly one delivery handle:

- success returns `RecordMetadata`
- broker Produce response failures return the same `Error` shape currently used by `send_batch`
- a record that reaches its `delivery_timeout_ms` deadline in the queue returns
  `Error::DeliveryDeadlineExceeded { possibly_transmitted: false, .. }`
  without being sent
- request-level failures follow the existing retry policy
- retryable per-partition Produce response failures retry only failed input records
- successful records from an earlier attempt are not sent again by the same buffered batch

If the background task stops before a record reaches a terminal Kafka result, the delivery handle should complete with an explicit client error rather than hanging.

## Internal Model

The first implementation can build on the existing batch path:

- use a bounded Tokio channel from `BufferedProducer::send` to the background task
- group queued records by topic and partition after metadata lookup
- use the existing Produce API negotiation and `send_batch_report` outcome model for request execution
- keep retry classification in the existing producer helpers
- use `tracing` events for enqueue, flush trigger, batch send, delivery success, delivery failure, and shutdown

The background task should own the inner `Producer` so the immediate producer path does not gain shared mutable state or hidden runtime assumptions.

## Implementation Slices

1. Done: add `ProducerConfig::linger_ms` and the `BufferedProducer` type skeleton with close/flush lifecycle tests.
2. Done: add bounded enqueue and per-record delivery handles without network I/O by testing task shutdown and delivery cancellation.
3. Done: wire the background task to `send_batch_report` and complete delivery handles from per-record outcomes.
4. Done: add linger, record-count, and byte-count flush trigger tests with a controllable clock.
5. Done: add a live smoke example that enqueues multiple records and fetches them back from Kafka 3.7.2.

M10 has focused tests for flush triggers and a passing live smoke result in GitHub Actions run `26999258762`.
