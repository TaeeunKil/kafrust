# V1-03 deterministic data-plane rerun — 2026-09-07

- source commit: `9eaca5fc72f18377a615694487102d9329919640`
- command: `cargo test -p kafrust-protocol --all-features --test data_plane_golden --test data_plane_malformed`
- environment: Windows workspace, Cargo test profile
- result: passed
- golden suite: 5 passed, 0 failed
- malformed suite: 5 passed, 0 failed
- elapsed test time: less than one second after compilation

The rerun covers selected request/response golden shapes, truncated and
negative collection lengths, flexible tagged-field truncation, trailing bytes,
and every selected response prefix. It is deterministic local evidence only;
it is not an Apache oracle, live broker qualification, published-artifact
evidence, or completion of V1-03.

