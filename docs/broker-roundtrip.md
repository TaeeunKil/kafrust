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

Latest manual live smoke: on 2026-08-11, GitHub Actions run [`31474626799`](https://github.com/TaeeunKil/kafrust/actions/runs/31474626799) passed all nine jobs from `main`, including Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 single-node plaintext, Kafka 3.7.2 TLS, SASL_PLAINTEXT, SASL_SSL SCRAM, ACL-authorizer, and three-broker profiles. The plaintext matrix covered broker roundtrip, normal and idempotent single-record, batch, and buffered produce, `acks=0`, transactional commit and abort, read-uncommitted versus read-committed isolation, transactional group offset commit, all four compression codecs, direct consumer, consumer group, offset reset, position and watermark control, static membership, and heartbeat rejoin paths. The three-broker job additionally passed coordinator failover, reassignment, cooperative group, multi-member cooperative ownership transfer, transient-member rollback, member-loss recovery, and producer/direct-consumer/group recovery after stopping a broker. The secured jobs covered the corresponding TLS, SASL, SCRAM credential, ACL, and admin paths.

The `Live Kafka Smoke` GitHub Actions workflow runs the single-node plaintext path against Kafka 3.7.2 and 4.3.1. Kafka 3.7.2 additionally covers three-broker plaintext, TLS, SASL_PLAINTEXT, and SASL_SSL SCRAM profiles. The workflow is available through manual dispatch and a weekly schedule, so the default pull request CI remains broker-free.

See [Compatibility](compatibility.md) for the current tested broker matrix and the limits of the alpha compatibility claim.

Requests made through `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig` use a 30 second request timeout by default. Override it with `request_timeout_ms` when running broker checks against slow or intentionally delayed environments.

Broker response payload allocation is limited to 100 MiB per request by default. Override it with `max_response_bytes` on the same four configuration builders. A declared frame above the limit returns `Error::ResponseTooLarge { size, max }` before payload allocation; choose a limit large enough for the workload's metadata and fetch responses. After a request has been sent, a timeout, transport error, or framing error permanently retires that low-level connection so a later request cannot consume a stale partial response; high-level retry paths establish a replacement connection.

`ClientConfig::max_idle_broker_connections` bounds the idle broker connections
retained by `Producer` and direct `Consumer` instances built from the shared
configuration. The default is 64. A request takes its broker connection out
of the cache and returns it only after success; failed or poisoned connections
are therefore not reused. When the bound is reached, the oldest idle entry is
evicted in FIFO order. Producers and direct consumers built from cloned
`ClientConfig` values share this cache; Admin, group, and Share clients still
have separate connection-lifecycle paths.

`ClientConfig::security_protocol` and the matching producer, consumer, and group builders default to `SecurityProtocol::Plaintext`. `SecurityProtocol::Tls` uses the non-default `tls` crate feature and is covered by the recorded broker roundtrip, producer, direct consumer, and consumer group smoke profile. TLS server name validation defaults to the bootstrap host and can be overridden with `tls_server_name(name)` or `KAFRUST_TLS_SERVER_NAME` for examples and broker checks. Extra DER root certificates can be added with `tls_root_certificate_der(bytes)` or `KAFRUST_TLS_ROOT_CERT_DER_PATH` for examples and broker checks. SASL/PLAIN and SASL/SCRAM-SHA-256/512 authentication are implemented for configured `SaslPlaintext` and `SaslTls` connections; SASL/PLAIN over `SaslPlaintext` and SASL/SCRAM-SHA-256 over `SaslTls` are covered by recorded broker roundtrip, producer, direct consumer, and consumer group smoke profiles.

For issuer-aware OAUTHBEARER refresh, wrap an application-owned token source
with `CachedOAuthBearerTokenProvider`. The source returns
`OAuthBearerToken::new(value, expires_at)`. The wrapper refreshes inside the
configured window and temporarily uses a still-valid cached token when the
issuer is unavailable; after expiry it returns the issuer error instead of
authenticating with stale credentials. HTTP discovery, JWKS retrieval, and
provider-specific endpoint policy remain application-owned.

TLS mutual authentication is configured with `ClientConfig::tls_client_certificate_der` (repeat for the certificate chain) and `ClientConfig::tls_client_private_key_der`. The matching producer, direct consumer, consumer-group, ShareConsumer, and Streams-group setters forward to the same shared configuration. The certificate and private key must be provided together, are rejected for plaintext, and the private-key bytes are redacted from `Debug`. This path is implemented behind the `tls` feature; a live mTLS broker qualification remains open.

The broker-roundtrip test accepts `KAFRUST_TLS_CLIENT_CERT_DER_PATH` and
`KAFRUST_TLS_CLIENT_KEY_DER_PATH` for a live client certificate and private
key. The common example security adapter accepts the same variables for
producer, consumer, group, and Admin examples. The manual [`live-mtls.yml`](../.github/workflows/live-mtls.yml)
workflow generates a short-lived CA, requires client authentication on Kafka,
checks the TLS handshake with OpenSSL, and then runs the Admin, producer,
direct-consumer, low-level, and coordinator roundtrips on Kafka 3.7.2 or
4.3.1.

When multiple bootstrap servers are configured, `ClientConfig::connect` tries them in order until one connection succeeds. Examples and opt-in broker tests parse comma-separated `KAFRUST_BOOTSTRAP_SERVERS` values into that same ordered list.

kafrust emits `tracing` events for Kafka request start, response receipt, request failure, and high-level producer, direct consumer, and consumer group operations. Each request roundtrip runs inside a `kafka.request` span with API key, API version, correlation ID, and request byte count. Events include operational metadata such as topic, partition, offset, group ID, member ID, generation ID, and byte or record counts, but not request, response, key, or value payload contents.

`ClientMetrics` can be supplied through the `metrics` builder on `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, or `ConsumerGroupConfig`. Clones share lock-free counters across bootstrap, leader, coordinator, authentication, and retry connections. `snapshot()` reports started, successful, failed, timed-out, cancelled, and in-flight requests, high-level operation retry and recovery attempts, acknowledged produced records and topic-partition Produce chunks, records returned by consumer APIs, request and response payload bytes, and total and maximum latency. It also exposes an approximate fixed-bucket request latency histogram and `ClientMetricsSnapshot::latency_percentile(50)`, `(95)`, or `(99)` for upper-bound tail estimates. The buckets are 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, and above 10s. Retry attempts include producer sends, consumer fetches, metadata reconnects, transactional coordinator operations, and automatic consumer-group rejoins. Snapshot fields are sampled independently and can change while they are read.
