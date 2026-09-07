# Live Telemetry Payload-Limit Rerun — 2026-09-04

The bounded current-source payload-limit diagnostic was rerun from
`86f349bd5ce9d475fe5e1df1cfe8a238953eebf3` on the hosted Ubuntu runner. The
workflow used the Kafka 3.7.2 telemetry test image and the repository's
`telemetry_smoke` example with the `otlp` feature.

## Result

The [GitHub Actions run 33860595003](https://github.com/TaeeunKil/kafrust/actions/runs/33860595003)
passed its single job, `Kafka 3.7.2 KIP-714 payload limit`, in 66 seconds
(2026-09-04 09:53:54–09:55:02 UTC). Kafka was configured with a 128-byte
telemetry ceiling; the client created a metrics subscription and rejected the
oversized advertised payload before transmission. The retained smoke output
was:

```
telemetry-smoke-payload-limit size=2076 max=128
```

The run compiled the current source with Rust 1.81.0 and produced no artifact
or credential-bearing output. Container cleanup was handled by the hosted job
teardown.

## Qualification boundary

This is a short, single-broker, plaintext, current-source diagnostic. It
confirms the deterministic advertised-payload-limit behavior on Kafka 3.7.2;
it does not prove published-artifact compatibility, secured or multi-broker
telemetry, stable ClientInstanceId across broker replacement, 60-minute
collection, provider outage/throttle behavior, long-campaign qualification,
service-canary behavior, V1-17 completion, or release authorization.
