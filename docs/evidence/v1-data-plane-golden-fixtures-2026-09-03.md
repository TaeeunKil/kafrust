# V1-03 Data-Plane Golden Fixtures — 2026-09-03

## Scope

The new integration fixture
[`data_plane_golden.rs`](../../crates/kafrust-protocol/tests/data_plane_golden.rs)
checks complete request bytes for the selected data-plane wire boundary. The
fixtures use empty or nullable collections deliberately so header version,
nullable encoding, fixed-width fields, compact-array counts, and tagged-field
terminators are visible without record-batch compression noise.

The field order and selected request/response header versions follow the
Apache Kafka 4.3.1 message schemas named by
[`data-plane-version-manifest.json`](data-plane-version-manifest.json) and its
official raw-schema URL template. Existing module tests continue to cover
non-empty record batches, topic UUIDs, flexible tags, and all four codecs.

## Covered request families

- Produce v2, v3, v7, v11, v12, and v13 (including the topic-UUID shape)
- Fetch v4, v11, v12, and v13
- Metadata v1 and v12
- ListOffsets v1
- ApiVersions v0, v3, and v4
- OffsetForLeaderEpoch v3

The test also asserts that Fetch v13's empty-body shape differs only in the
API-version header from Fetch v12, while Produce v13's topic UUID and compact
topic structure are retained. Nullable client IDs, transactional IDs,
nullable topic selectors, and empty compact/fixed arrays are represented
explicitly rather than normalized.

## Verification

`cargo test -p kafrust-protocol --test data_plane_golden -- --nocapture`
passed all three tests on 2026-09-03. This is deterministic current-source
evidence; the follow-up CI run and the accepted floor/pinned-current live
negotiation logs remain separate V1-03 gates.

## Boundary

These fixtures close the previously missing byte-auditable request-shape
slice. They do not by themselves prove every non-empty response fixture,
malformed boundary, codec roundtrip, transactional selection rule, or live
floor/pinned-current negotiation result required by V1-03.
