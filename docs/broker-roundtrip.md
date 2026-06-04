# Broker Roundtrip

kafrust includes opt-in broker roundtrip checks. The M2 check connects to one Kafka-compatible bootstrap broker, sends `ApiVersions v0`, then sends `Metadata v1`. The consumer group check sends `FindCoordinator v1` for `KAFRUST_GROUP_ID`.

Run it against a local broker:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=kafrust-smoke cargo test -p kafrust --test broker_roundtrip -- --nocapture
```

Or run the example:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 cargo run -p kafrust --example broker_roundtrip
```

The test is skipped when `KAFRUST_BOOTSTRAP_SERVERS` is not set, so normal CI does not require a Kafka broker.

Latest manual live smoke: on 2026-06-04, a local Kafka 3.7.2 KRaft broker passed the broker roundtrip test, `producer_send`, `consumer_fetch`, and `consumer_group_poll` against `kafrust-smoke`.

The `Live Kafka Smoke` GitHub Actions workflow runs the same broker roundtrip, producer, direct consumer, and consumer group checks against a Kafka 3.7.2 Docker container. It is available through manual dispatch and a weekly schedule, so the default pull request CI remains broker-free.

Requests made through `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig` use a 30 second request timeout by default. Override it with `request_timeout_ms` when running broker checks against slow or intentionally delayed environments.

When multiple bootstrap servers are configured, `ClientConfig::connect` tries them in order until one connection succeeds.

kafrust emits `tracing` events for Kafka request start, response receipt, request failure, and high-level producer, direct consumer, and consumer group operations. Events include operational metadata such as API key, API version, correlation ID, topic, partition, offset, group ID, member ID, generation ID, and byte or record counts, but not request, response, key, or value payload contents.
