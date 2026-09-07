# Direct consumer high-watermark offset reset (2026-09-07)

## Scope

Source commit `758b8f9052164937f449e3985777ba87bf9ea340` adds a deterministic
scripted-broker regression for a direct consumer configured with
`OffsetResetPolicy::Latest`. The broker first returns `OFFSET_OUT_OF_RANGE` for
an assignment at offset `100`, then returns watermarks `(low=4, high=9)`. The
consumer must use the high watermark, issue the retry Fetch at offset `9`,
return no records, and advance the assignment position to `9`.

The fixture also asserts that the watermark lookup sends both Kafka ListOffsets
timestamps (`-2` for earliest and `-1` for latest); only the high-watermark
result is selected by the policy.

## Verification

The focused test is
`consumer::tests::resets_out_of_range_assignment_to_latest_offset`. It passed
on Windows with the complete required Rust validation:

```text
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features  # 509 + 13 + 38 + 5 + 285 + 5 + 5 tests; all passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The test uses only in-memory scripted TCP fixtures. No Docker resource or
external Kafka broker was created or modified.

## Boundary

This is local deterministic offset-reset evidence only. It does not claim
retention or unclean-election recovery, published-artifact behavior, live
leader-movement qualification, 100,000-record reconciliation, long campaigns,
service canaries, or release authorization.
