# macOS ARM64 Local Validation (2026-09-15)

This record captures the kafrust checks run on an Apple Silicon MacBook. It is
local workstation evidence, not a release qualification record. The tested
source was commit `c0407a74ef10b6cac2f9e267e01d481025d8205a` (`c0407a7`).

## Host and toolchain

| Item | Observed value |
| --- | --- |
| Operating system | macOS 26.6.2 (build 25G83) |
| Hardware | MacBook Pro 17,1; Apple M1; 8 cores; 8 GB memory |
| Native Rust target | `aarch64-apple-darwin` |
| Repository toolchain | Rust 1.81.0, from `rust-toolchain.toml` |
| Stable toolchain used for the passing validation | Rust 1.98.1 |
| Root volume | 228 GiB total, 95 GiB available at capture time |
| Broker runtime | Docker Desktop; `apache/kafka:4.3.1` |

## Workspace validation

The required package checks passed with the stable Rust toolchain:

| Command | Result |
| --- | --- |
| `cargo +stable fmt --all --check` | PASS |
| `cargo +stable check --workspace --all-targets --all-features` | PASS |
| `cargo +stable test --workspace --all-features` | PASS |
| `cargo +stable clippy --workspace --all-targets --all-features -- -D warnings` | PASS |
| `cargo +stable doc --workspace --all-features --no-deps` | PASS |
| `git diff --check` | PASS |

The workspace test run reported 892 passing tests/doctests and no failures,
including:

- `kafrust` unit tests: 530
- broker roundtrip tests: 13
- fault injection tests: 39
- public surface tests: 5
- `kafrust-protocol` unit tests: 285
- data-plane golden tests: 5
- data-plane malformed-input tests: 5
- doctests: 10

## Live Kafka smoke test

A single-node plaintext Kafka 4.3.1 broker was started locally on
`localhost:9092`. The opt-in broker suite was run with:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_DATA_PLANE_TOPIC=kafrust-smoke \
cargo +stable test -p kafrust --test broker_roundtrip --all-features -- --nocapture
```

Result: 13 passed, 0 failed. The run exercised metadata, produce, fetch,
list-offsets, offset-for-leader-epoch, and a data-plane produce/fetch record
roundtrip. The observed API versions included Metadata v12, Produce v13,
Fetch v13, ListOffsets v1, and OffsetForLeaderEpoch v3.

Share-specific cases were not claimed by this run because their opt-in
environment variables were not set. TLS, SASL, ACL, three-broker failover,
long soak, and performance campaigns were also not part of this workstation
smoke test.

## Rust 1.81 follow-up

The repository-selected Rust 1.81.0 toolchain was also attempted. Cargo stopped
before compilation while parsing the cached `security-framework 3.7.0`
manifest:

```text
feature `edition2024` is required
The package requires the Cargo feature called `edition2024`, but that feature
is not stabilized in this version of Cargo (1.81.0)
```

This is a dependency-resolution/toolchain compatibility issue, not a compiler
failure in kafrust source. No dependency or source change was made to conceal
the result. A future MSRV check should pin a dependency set compatible with
Cargo 1.81 before the repository can claim a passing Rust 1.81 run on this
machine.

## Conclusion and boundary

The M1 MacBook is sufficient for local kafrust development, the full stable
workspace validation suite, and a single-broker Kafka smoke test. This record
does not establish the macOS live-broker, security-matrix, failover, long-soak,
or performance claims reserved by the platform and compatibility documents.
