# Kafka Client Telemetry Test Plugin

This directory contains the minimal broker-side `ClientTelemetry` plugin used
by the KIP-714 live qualification workflow. It is test infrastructure, not a
runtime dependency of either kafrust crate.

The image is based on Kafka 3.7.2 and compiles the plugin against the Kafka
server libraries already present in the image. The plugin emits bounded
`KAFRUST_TELEMETRY` log markers when the broker receives a client payload.
