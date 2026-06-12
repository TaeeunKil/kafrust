# API Stability

kafrust is pre-`1.0`. The public API is intended to stay small while protocol,
runtime, and broker compatibility behavior are still being validated.

## Versioning Policy

- Patch releases should avoid intentional breaking API changes.
- Minor `0.x` releases may include breaking changes when they clarify Kafka
  concepts, fix incorrect behavior, or remove API surface that was exposed too
  early.
- Breaking minor releases should include release notes that call out the changed
  types, methods, configuration defaults, and migration steps.
- Compatibility claims must stay tied to dated broker evidence in
  [Compatibility](compatibility.md).

## Stability Levels

Current high-level client types are alpha APIs:

- `ClientConfig`, `Client`, `SecurityProtocol`, `SaslMechanism`, and
  `SaslCredentials`
- `ProducerConfig`, `Producer`, `BufferedProducer`, `ProducerRecord`, and
  delivery/result types
- `ConsumerConfig`, `Consumer`, `ConsumerRecord`, and assignment types
- `ConsumerGroupConfig`, `ConsumerGroup`, and heartbeat handle types

These APIs preserve Kafka user-facing concepts, but names, builders, defaults,
error variants, and result shapes can still change in minor releases before
`1.0`.

The `kafrust-protocol` crate is also alpha. Wire-format behavior should be
grounded in Kafka API/version details, but protocol structs can be renamed,
moved, or revised as API coverage grows.

## Change Rules

- Keep public API additions minimal until the related broker behavior is tested.
- Prefer explicit error variants over string-only failures.
- Avoid hiding Kafka concepts behind broad abstractions.
- Add focused tests for new observable behavior.
- Update README or docs when public behavior, compatibility claims, or workflow
  expectations change.

## Migration Notes

Every breaking minor release should document:

- the old API and the replacement API
- whether behavior changed or only naming changed
- required feature flags, environment variables, or broker profile changes
- broker evidence used to justify new compatibility claims
