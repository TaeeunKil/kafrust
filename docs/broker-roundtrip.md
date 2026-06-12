# Broker Roundtrip

kafrust includes opt-in broker roundtrip checks. The M2 check connects to one Kafka-compatible bootstrap broker, sends `ApiVersions v0`, then sends `Metadata v1`. The consumer group check sends `FindCoordinator v1` for `KAFRUST_GROUP_ID`.

Run it against a local broker:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=kafrust-smoke cargo test -p kafrust --test broker_roundtrip -- --nocapture
```

The roundtrip test and `broker_roundtrip` example accept
`KAFRUST_SECURITY_PROTOCOL`. Supported values are `plaintext`, `tls`/`ssl`,
`sasl_plaintext`, and `sasl_tls`/`sasl_ssl`. TLS requires building kafrust with
the non-default `tls` feature and using a broker certificate chain trusted by
the host OS:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9093 \
KAFRUST_SECURITY_PROTOCOL=tls \
cargo test -p kafrust --features tls --test broker_roundtrip -- --nocapture
```

For SASL/PLAIN broker checks, set `KAFRUST_SECURITY_PROTOCOL` to
`sasl_plaintext` or `sasl_tls` and provide `KAFRUST_SASL_USERNAME` plus
`KAFRUST_SASL_PASSWORD`. `sasl_tls` also requires the `tls` crate feature.

Or run the example:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 cargo run -p kafrust --example broker_roundtrip
```

The test is skipped when `KAFRUST_BOOTSTRAP_SERVERS` is not set, so normal CI does not require a Kafka broker.

Latest manual live smoke: on 2026-06-12, GitHub Actions run `27397850803` passed from `main` against Kafka 3.7.2. The plaintext job covered the broker roundtrip test, `producer_send`, `producer_send_batch`, `producer_buffered`, `consumer_fetch`, and `consumer_group_poll` against `kafrust-smoke`. The TLS job covered the broker roundtrip test and `broker_roundtrip` example with `KAFRUST_SECURITY_PROTOCOL=tls`. The SASL_PLAINTEXT job covered the broker roundtrip test and `broker_roundtrip` example with `KAFRUST_SECURITY_PROTOCOL=sasl_plaintext` and SASL/PLAIN credentials.

The `Live Kafka Smoke` GitHub Actions workflow runs plaintext broker roundtrip, single-record producer, batch producer, buffered producer, direct consumer, and consumer group checks against a Kafka 3.7.2 Docker container. It also runs TLS and SASL_PLAINTEXT broker roundtrip jobs against Kafka 3.7.2 Docker containers with secured listeners. The workflow is available through manual dispatch and a weekly schedule, so the default pull request CI remains broker-free.

See [Compatibility](compatibility.md) for the current tested broker matrix and the limits of the alpha compatibility claim.

Requests made through `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig` use a 30 second request timeout by default. Override it with `request_timeout_ms` when running broker checks against slow or intentionally delayed environments.

`ClientConfig::security_protocol` and the matching producer, consumer, and group builders default to `SecurityProtocol::Plaintext`. `SecurityProtocol::Tls` uses the non-default `tls` crate feature and is covered by the recorded broker roundtrip smoke profile. SASL/PLAIN authentication is implemented for configured `SaslPlaintext` and `SaslTls` connections; `SaslPlaintext` is covered by the recorded broker roundtrip smoke profile.

When multiple bootstrap servers are configured, `ClientConfig::connect` tries them in order until one connection succeeds.

kafrust emits `tracing` events for Kafka request start, response receipt, request failure, and high-level producer, direct consumer, and consumer group operations. Events include operational metadata such as API key, API version, correlation ID, topic, partition, offset, group ID, member ID, generation ID, and byte or record counts, but not request, response, key, or value payload contents.
