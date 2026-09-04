# Live Telemetry Diagnostics — 2026-09-04

These bounded current-source diagnostics ran from
`c8a2136ec45ffb308207728d3502e7dfeb7b2dde` on hosted Ubuntu with the Kafka
3.7.2 telemetry test image. They exercise the KIP-714 broker plugin and the
current checkout; they are not published-artifact or long-duration evidence.

## Results

| Run | Profile | Result | Job duration |
| --- | --- | --- | --- |
| [33841418670](https://github.com/TaeeunKil/kafrust/actions/runs/33841418670) | KIP-714 subscription mutation and terminating push | passed | 72 seconds |
| [33841420859](https://github.com/TaeeunKil/kafrust/actions/runs/33841420859) | KIP-714 advertised payload limit | passed | 66 seconds |

## Assertions

The telemetry smoke built and loaded the broker plugin, created a client
metrics subscription, and observed six pushes. The client reported two
subscription IDs after the broker-side filter mutation, then sent exactly one
terminating push. The retained output included payloads of 2,076 and 600 bytes
and `telemetry-smoke-ok pushes=6`; the broker log checks confirmed non-zero
regular and terminating payloads.

The payload-limit diagnostic configured a 128-byte broker ceiling and verified
that the client rejected the advertised 2,076-byte payload before accepting it,
reporting `telemetry-smoke-payload-limit size=2076 max=128`.

## Qualification boundary

These are short current-source diagnostics. They do not prove published
artifact compatibility, secured multi-broker broker replacement,
ClientInstanceId continuity, a 60-minute collection, provider outage or
throttle behavior, secret-scan completion, or release readiness. V1-17
remains `In progress`.
