# Company workstation validation recheck (2026-09-04)

- date_utc: `2026-09-04`
- source_commit: `d1fe161563327a2d2c704224a00fa5a407fd1dc5`
- host: company Windows x64 workstation
- local runtime: WSL2 Ubuntu-T9, Linux `x86_64`, Rust 1.81.0
- evidence level: Local deterministic reproduction only

## Checks completed

The company WSL checkout reran the new idempotent cancellation regressions and
the complete scripted fault target with one build job:

```text
cargo test -p kafrust --lib producer::tests::cancels_idempotent -- --nocapture
cargo test -p kafrust --test fault_injection
```

Both cancellation tests passed and all 29 `fault_injection` tests passed. The
tests use in-memory/scripted fixtures and do not start Kafka or modify Docker
containers, networks, volumes, or images.

## Full-workspace resource observation

An initial `cargo test --workspace --all-features` attempt on the mounted
`/mnt/c` checkout reached the example-link stage but failed at
`consumer_partition_queue` with `/usr/bin/ld: final link failed: Cannot
allocate memory`. This is a WSL mounted-filesystem/linker resource failure,
not a Rust test assertion or source compilation failure. The WSL instance had
approximately 15 GiB RAM with approximately 11 GiB available when inspected.
The Windows stable/Rust 1.81 CI matrix remains the authoritative full-target
validation and passed for this source change.

WSL Git without an explicit line-ending policy reports the mounted Windows
checkout as modified because the files use CRLF. The equivalent check with
`git -c core.autocrlf=true status --short --branch` and
`git -c core.autocrlf=true diff --check` is clean; no repository content was
changed by this recheck.

## Boundary

This record strengthens company-host deterministic evidence only. It does not
qualify the published artifact, accepted-floor or pinned-current security
matrix, ten-cycle/100,000-record reconciliation, V1-21/V1-22 long campaigns,
V1-23 service canary, or any release/publication decision.
