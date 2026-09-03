# V1 data-plane malformed trailing-byte matrix — 2026-09-03

## Scope

At source commit `f98275d35ebceba41dfeb77505fd08f857d48726`, the selected
data-plane response decoders call `Decoder::finish()` after consuming their
schema. `finish()` returns the typed `TrailingBytes` protocol error when any
input remains. The malformed integration test appends a sentinel byte to the
valid empty-body shape for every selected/fallback response version and asserts
that the one-byte suffix is rejected.

Coverage includes Produce v2/v7/v9/v11/v12/v13, Fetch v4/v11/v12/v13,
Metadata v1/v12, ListOffsets v1, OffsetForLeaderEpoch v3, and ApiVersions
v0/v3/v4. The existing negative-length, tagged-field, and truncated-prefix
tests remain in the same suite.

## Verification

```text
cargo test -p kafrust-protocol --test data_plane_malformed -- --nocapture
5 passed; 0 failed
python scripts/check_data_plane_manifest.py
data-plane manifest ok: 6 APIs, Kafka 4.3.1 metadata and header paths checked
```

The source commit also passed the required workspace format, check, test,
Clippy, documentation, and diff checks. Its stable/Rust 1.81 CI result is
recorded at
[run 33743888446](https://github.com/TaeeunKil/kafrust/actions/runs/33743888446).

## Boundary

This closes the selected data-plane decoder trailing-byte boundary and the
deterministic malformed slice. It does not claim official Apache response
oracles for every shape, live broker qualification, three-broker movement,
long campaigns, service-canary readiness, published compatibility completion,
or release authorization.
