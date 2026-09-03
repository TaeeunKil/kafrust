# V1 data-plane malformed prefix matrix — 2026-09-03

## Scope

At source commit `8842c654702010a0719049bb70f1458a66954c80`,
`crates/kafrust-protocol/tests/data_plane_malformed.rs` adds
`rejects_truncated_prefixes_for_every_selected_response_version`. The test
starts from the valid empty-body shape for every selected/fallback response
type and runs the decoder against every prefix shorter than the complete
body. Every incomplete prefix must return a typed decode error.

Coverage includes:

- Produce v2, v7, v9, v11, v12, and v13;
- Fetch v4, v11, v12, and v13;
- Metadata v1 and v12;
- ListOffsets v1;
- OffsetForLeaderEpoch v3; and
- ApiVersions v0, v3, and v4.

The existing negative collection-length and flexible-tag truncation tests
remain in the same suite. The manifest checker now requires this matrix test
alongside those boundary tests.

## Verification

```text
cargo test -p kafrust-protocol --test data_plane_malformed -- --nocapture
4 passed; 0 failed
python scripts/check_data_plane_manifest.py
data-plane manifest ok: 6 APIs, Kafka 4.3.1 metadata and header paths checked
```

The source commit also passed the required workspace format, check, test,
Clippy, documentation, and diff checks. Its stable/Rust 1.81 CI result is
recorded at
<https://github.com/TaeeunKil/kafrust/actions/runs/33740994749>.

## Boundary

This closes the deterministic truncated-prefix slice for selected response
versions. It does not by itself claim complete malformed length/trailing-byte
coverage, official Apache response oracles, live broker qualification,
three-broker movement, long campaigns, service-canary readiness, or release
authorization.
