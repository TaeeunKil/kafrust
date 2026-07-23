# Broker Roundtrip

kafrust includes opt-in broker roundtrip checks. The M2 check connects to one Kafka-compatible bootstrap broker, sends `ApiVersions v0`, then sends `Metadata v1`. The consumer group check sends `FindCoordinator v1` for `KAFRUST_GROUP_ID`.
`KAFRUST_BOOTSTRAP_SERVERS` accepts Kafka's comma-separated bootstrap format,
for example `localhost:19092,localhost:19093`, so broker checks and smoke
examples can exercise bootstrap failover and multi-broker metadata.
Set `KAFRUST_EXPECTED_BROKERS` to make the metadata roundtrip assert a minimum
broker count in multi-broker profiles.

Run it against a local broker:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=kafrust-smoke cargo test -p kafrust --test broker_roundtrip -- --nocapture
```

The roundtrip test and smoke examples accept `KAFRUST_SECURITY_PROTOCOL`.
Supported values are `plaintext`, `tls`/`ssl`, `sasl_plaintext`, and
`sasl_tls`/`sasl_ssl`. TLS requires building kafrust with the non-default `tls`
feature and using a broker certificate chain trusted by the host OS:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9093 \
KAFRUST_SECURITY_PROTOCOL=tls \
cargo test -p kafrust --features tls --test broker_roundtrip -- --nocapture
```

Set `KAFRUST_TLS_SERVER_NAME` when the bootstrap host or IP address differs
from the broker certificate subject alternative name. Set
`KAFRUST_TLS_ROOT_CERT_DER_PATH` to add one DER-encoded root certificate while
keeping platform roots enabled.

For SASL broker checks, set `KAFRUST_SECURITY_PROTOCOL` to `sasl_plaintext` or
`sasl_tls` and provide `KAFRUST_SASL_USERNAME` plus
`KAFRUST_SASL_PASSWORD`. `KAFRUST_SASL_MECHANISM` defaults to `plain` and also
accepts `scram-sha-256` or `scram-sha-512`. `sasl_tls` also requires the `tls`
crate feature.

Or run the example:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 cargo run -p kafrust --example broker_roundtrip
```

The test is skipped when `KAFRUST_BOOTSTRAP_SERVERS` is not set, so normal CI does not require a Kafka broker.

Latest manual live smoke: on 2026-07-23, GitHub Actions run `29989550933` passed the single-node plaintext path against Kafka 3.7.2 and 4.3.1, plus the Kafka 3.7.2 multi-broker, TLS, SASL_PLAINTEXT, and SASL_SSL SCRAM profiles. The plaintext matrix covered broker roundtrip, single-record, batch, all four compression codecs, buffered producer, direct consumer, and consumer group paths. The TLS job additionally verified gzip, Snappy, LZ4, and Zstd batch production over a secured connection. The multi-broker job retained leader-stop producer and consumer failover coverage.

The `Live Kafka Smoke` GitHub Actions workflow runs the single-node plaintext path against Kafka 3.7.2 and 4.3.1. Kafka 3.7.2 additionally covers three-broker plaintext, TLS, SASL_PLAINTEXT, and SASL_SSL SCRAM profiles. The workflow is available through manual dispatch and a weekly schedule, so the default pull request CI remains broker-free.

See [Compatibility](compatibility.md) for the current tested broker matrix and the limits of the alpha compatibility claim.

Requests made through `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig` use a 30 second request timeout by default. Override it with `request_timeout_ms` when running broker checks against slow or intentionally delayed environments.

`ClientConfig::security_protocol` and the matching producer, consumer, and group builders default to `SecurityProtocol::Plaintext`. `SecurityProtocol::Tls` uses the non-default `tls` crate feature and is covered by the recorded broker roundtrip, producer, direct consumer, and consumer group smoke profile. TLS server name validation defaults to the bootstrap host and can be overridden with `tls_server_name(name)` or `KAFRUST_TLS_SERVER_NAME` for examples and broker checks. Extra DER root certificates can be added with `tls_root_certificate_der(bytes)` or `KAFRUST_TLS_ROOT_CERT_DER_PATH` for examples and broker checks. SASL/PLAIN and SASL/SCRAM-SHA-256/512 authentication are implemented for configured `SaslPlaintext` and `SaslTls` connections; SASL/PLAIN over `SaslPlaintext` and SASL/SCRAM-SHA-256 over `SaslTls` are covered by recorded broker roundtrip, producer, direct consumer, and consumer group smoke profiles.

When multiple bootstrap servers are configured, `ClientConfig::connect` tries them in order until one connection succeeds. Examples and opt-in broker tests parse comma-separated `KAFRUST_BOOTSTRAP_SERVERS` values into that same ordered list.

kafrust emits `tracing` events for Kafka request start, response receipt, request failure, and high-level producer, direct consumer, and consumer group operations. Events include operational metadata such as API key, API version, correlation ID, topic, partition, offset, group ID, member ID, generation ID, and byte or record counts, but not request, response, key, or value payload contents.
