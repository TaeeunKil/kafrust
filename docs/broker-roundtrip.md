# Broker Roundtrip

kafrust includes an opt-in broker roundtrip check for M2 development. It connects to one Kafka-compatible bootstrap broker, sends `ApiVersions v0`, then sends `Metadata v1`.

Run it against a local broker:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 cargo test -p kafrust --test broker_roundtrip -- --nocapture
```

Or run the example:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 cargo run -p kafrust --example broker_roundtrip
```

The test is skipped when `KAFRUST_BOOTSTRAP_SERVERS` is not set, so normal CI does not require a Kafka broker.
