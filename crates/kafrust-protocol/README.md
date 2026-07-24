# kafrust-protocol

[![Crates.io](https://img.shields.io/crates/v/kafrust-protocol.svg)](https://crates.io/crates/kafrust-protocol)
[![Docs.rs](https://docs.rs/kafrust-protocol/badge.svg)](https://docs.rs/kafrust-protocol)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)

Kafka wire protocol primitives for kafrust.

`kafrust-protocol` is the runtime-free protocol crate used by the high-level
[`kafrust`](https://docs.rs/kafrust) client. It owns Kafka request and response
encoding, response decoding, request/response headers, frame handling, and
focused wire-format tests.

This crate is not a Kafka client by itself. Applications should normally depend
on `kafrust`; this crate is public so protocol behavior can be inspected,
tested, and reused where low-level Kafka messages are needed.

## Design Goals

- Keep Kafka API/version details explicit in type names.
- Keep wire-format code small and easy to audit.
- Avoid async runtime assumptions.
- Avoid `unsafe`.
- Add Kafka protocol surface only when encoding, decoding, and client behavior
  need it.

## Implemented Areas

The `0.2.x` protocol surface includes support used by the current high-level
client paths:

- primitive Kafka wire types
- nullable and compact strings/bytes/arrays
- tagged fields
- request and response headers
- frame encoding and decoding
- `ApiVersions v0`
- `Metadata v1`
- `CreateTopics v2`
- `FindCoordinator v1`
- `Produce v2` MessageSet and `Produce v3` RecordBatch paths
- `Fetch v2` and `Fetch v4` request/response decoding for MessageSet and
  RecordBatch records
- `JoinGroup v2`
- `SyncGroup v2`
- `Heartbeat v2`
- `OffsetFetch v2`
- `OffsetCommit v2`
- `SaslHandshake v1`
- `SaslAuthenticate v0`
- `InitProducerId v0`, `AddPartitionsToTxn v0`, `AddOffsetsToTxn v0`,
  `TxnOffsetCommit v0`, and `EndTxn v0`
- classic consumer protocol subscription and assignment payloads

## Encoding Example

```rust
use kafrust_protocol::api::api_versions::ApiVersionsRequestV0;

let request = ApiVersionsRequestV0 {
    correlation_id: 42,
    client_id: Some("kafrust".to_owned()),
};
let bytes = request.encode()?;

assert!(!bytes.is_empty());
# Ok::<(), kafrust_protocol::Error>(())
```

## Decoding Example

```rust
use kafrust_protocol::api::api_versions::ApiVersionsResponseV0;
use kafrust_protocol::codec::Decoder;

let response_body = [
    0, 0,       // error_code
    0, 0, 0, 1, // api_keys length
    0, 18,      // ApiVersions api key
    0, 0,       // min version
    0, 4,       // max version
];

let mut decoder = Decoder::new(&response_body);
let response = ApiVersionsResponseV0::decode_body(&mut decoder)?;

assert_eq!(response.error_code, 0);
assert_eq!(response.api_keys.len(), 1);
# Ok::<(), kafrust_protocol::Error>(())
```

## Compatibility

Protocol types can exist before a high-level client path is verified against a
real broker. kafrust compatibility claims are made from the high-level client
crate and repository docs, not from the presence of protocol structs alone.

The current `0.2.x` client compatibility claim is Apache Kafka `3.7.2`,
single-node KRaft, over `PLAINTEXT`.

## Current Limits

- This crate does not implement a broker.
- This crate does not perform network I/O.
- Protocol coverage is intentionally incomplete.
- SASL protocol structs do not perform authentication by themselves.
- Compression, transactions, idempotent producer protocol paths, and admin APIs
  are not complete yet.
- Public APIs are pre-`1.0` and can change between minor versions.

## Project Docs

- Repository: <https://github.com/TaeeunKil/kafrust>
- kafrust client crate: <https://docs.rs/kafrust>
- Roadmap: <https://github.com/TaeeunKil/kafrust/blob/main/docs/roadmap.md>
- Compatibility: <https://github.com/TaeeunKil/kafrust/blob/main/docs/compatibility.md>
