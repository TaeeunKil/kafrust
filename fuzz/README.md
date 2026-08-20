# kafrust fuzz targets

This directory contains libFuzzer targets for the public Kafka wire-protocol
decoders. It is intentionally a separate Cargo workspace so normal workspace
builds and docs do not require a nightly toolchain or libFuzzer.

Install the runner once:

```text
cargo install cargo-fuzz
rustup toolchain install nightly
```

Run one target from the repository root:

```text
cargo +nightly fuzz run frame
cargo +nightly fuzz run api_versions_response
```

The targets use bounded `Decoder` limits and treat malformed input as an
expected result. A crash, abort, or sanitizer report is a protocol defect and
must be reduced to a regression test before the target is restarted.

The current targets cover primitive/flexible decoding, request framing,
classic and modern group descriptions, Streams group responses, share-group
offsets, classic, member-aware, and topic-UUID OffsetCommit and OffsetFetch
responses, ListGroups v1/v4/v5 responses, and all five Kafka record
compression codecs.

The GitHub workflow compiles every target and runs a corpus-backed campaign
for each target. Each weekly/manual run gives every target a bounded time,
RSS, and per-input timeout budget, then uploads the generated corpus and any
libFuzzer crash artifacts. A crash must be reduced to a deterministic
regression test before the target is restarted; an uploaded corpus is
qualification evidence, not a substitute for the regression test.
