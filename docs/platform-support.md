# Platform Support

kafrust separates package portability from live Kafka qualification. A green
build or unit-test run proves that the crate works on that target's toolchain;
it does not prove broker compatibility, long-soak behavior, or a performance
SLO on that target.

## Current matrix

| Target | Build and test | Live Kafka evidence | Position |
| --- | --- | --- | --- |
| Linux x86_64 | Rust 1.81 and stable CI | Named Kafka 3.7.2 and 4.3.1 profiles | Primary qualified target |
| Windows x86_64 | Local Rust 1.81 checks and the platform CI workflow | Local bounded smoke only | Development and bounded validation target |
| macOS hosted target | The platform CI workflow records the runner target and runs the full feature test suite | No named live-broker or long-soak row | Build and test target; broker qualification remains open |
| Linux ARM64 | No committed CI or live-broker row | None | Unclaimed |
| macOS ARM64 | Native stable checks plus the optional bounded macOS lifetime diagnostic | No named release live-broker or long-soak row | Build/test and diagnostic evidence only; broker qualification remains open |

The repository has no client implementation that requires a Unix socket,
Windows-only API, or native Kafka client binding. The main portability risks
are therefore dependency resolution, TLS certificate-root behavior, and
runtime/socket differences. The `tls` feature must be checked on each claimed
target rather than inferred from the default build.

## Platform workflow

`.github/workflows/platform-compatibility.yml` runs on `windows-latest` and
`macos-latest` for every push and pull request. It records `rustc -vV` and the
runner architecture, then runs formatting, all-target/all-feature checking,
the workspace test suite, Clippy, and rustdoc. Linux remains covered by the
main CI workflow on Rust 1.81 and stable.

This workflow intentionally does not start Docker, Kafka, fault injection, or
long-running campaigns. Those tests require an isolated broker environment and
their evidence remains tied to the broker, artifact, workload, and runner
recorded by the corresponding compatibility or V1 milestone.

The repository also contains a native Apple Silicon diagnostic launcher at
`scripts/run_local_lifetime_diagnostic_macos.py`. It is intentionally separate
from the Linux/WSL qualification-shaped launcher: Docker Desktop's data volume
must be supplied explicitly, the default broker memory cap is 1 GiB per broker,
and its descriptor remains `qualified=false`.

## Local checks

On Windows, macOS, or Linux, run the same package checks before making a
platform claim:

```sh
cargo fmt --all --check
cargo check --workspace --all-targets --all-features
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
```

An Apple Silicon or Linux ARM64 build should record the target from
`rustc -vV`. A successful cross-compile is useful portability evidence, but it does
not replace a native runtime smoke or a live Kafka result when those are later
needed for a release claim.
