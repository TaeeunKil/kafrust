# Company workstation scripted-fault and protocol fixture smoke (2026-09-03)

- source_commit: `e51384da6d0c3bc78e0d47db26827a5dee41ce69`
- host: company Windows x64 workstation
- runtime: Rust 1.81.0 (MSVC)
- evidence level: Local deterministic

## Checks that passed

At the pushed source head, the deterministic scripted-broker suite completed:

- `cargo test -p kafrust --test fault_injection -- --nocapture`: 19 passed,
  covering producer response loss/replay and terminal sequence errors,
  transaction outcome ambiguity, direct-consumer reconnect, classic/KIP-848
  group recovery, Share acknowledgement ambiguity/redelivery, and Admin
  coordinator/metadata routing.
- `cargo test -p kafrust-protocol --test data_plane_golden -- --nocapture`: 4
  passed for selected Produce, Fetch, Metadata, ListOffsets, ApiVersions, and
  OffsetForLeaderEpoch request/response shapes.
- `cargo test -p kafrust-protocol --test data_plane_malformed -- --nocapture`:
  3 passed for truncation, negative collection lengths, and flexible tagged
  sections.

These are deterministic local tests and do not require a broker. The WSL2
Linux attempt was not used for this record because that environment lacks the
`cc` linker; the same tests passed with the workstation's Windows Rust
toolchain. No source code was changed for this smoke.

## Boundary

The results strengthen the local V1-03 and V1-05 evidence slices but do not
close either milestone. They are not live floor/pinned-current qualification,
published-artifact evidence, long fault/SLO campaigns, service-canary proof,
or release authorization.
