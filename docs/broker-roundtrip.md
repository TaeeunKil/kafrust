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

Requests made through `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig` use a 30 second request timeout by default. Override it with `request_timeout_ms` when running broker checks against slow or intentionally delayed environments.
