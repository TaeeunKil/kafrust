# Published `0.4.0` package boundary (2026-09-09)

- source_commit: `628d130`
- client: `kafrust 0.4.0`
- protocol: `kafrust-protocol 0.4.0`
- publication order: protocol first, then client
- status: published and resolved from crates.io
- competitor decision: [`v1-23-published-competitor-comparison-2026-09-09.md`](v1-23-published-competitor-comparison-2026-09-09.md)

## Registry publication

Both coordinated crates are visible in the crates.io index. The registry API
reports these publication timestamps and checksums:

| Crate | Published at (UTC) | SHA-256 |
| --- | --- | --- |
| `kafrust-protocol 0.4.0` | `2026-09-09T04:24:58.098711Z` | `5c5f76a73b2d73a3a7644e8d91e78fdb9ae0434a071ac273410ddfd2d8b0f11a` |
| `kafrust 0.4.0` | `2026-09-09T04:25:42.314168Z` | `20459f0fb58c93e2517e2d11d35aa8a355d11e22514161eba3fbfc2ebe5fca0d` |

The downloaded crates.io archives matched the package archives produced from
the exact release worktree at `628d130`.

## Fresh external resolution

A new external Cargo project with an exact `kafrust = "=0.4.0"` dependency
resolved the published pair and produced a locked dependency graph containing
both `kafrust 0.4.0` and `kafrust-protocol 0.4.0`. The build passed with the
installed Rust `1.81.0` MSVC toolchain. The workstation's default Rust `1.75.0`
correctly rejected the package's declared Rust `1.81` minimum before compiling;
that is a toolchain compatibility result, not a registry or package failure.

The ordered commands were:

- `cargo publish -p kafrust-protocol --locked`
- `cargo publish -p kafrust --locked`
- `cargo +1.81.0-x86_64-pc-windows-msvc check --locked` in the fresh project
- `cargo search kafrust --limit 5`
- `cargo search kafrust-protocol --limit 5`

## Bounded qualification retained for this release

The accepted workstation-sized gate remains the six-hour secured phase and
twelve-hour plaintext phase recorded in
[`v1-local-bounded-followup-2026-09-08.md`](v1-local-bounded-followup-2026-09-08.md):

- secure: 2,160,150 attempted, acknowledged, and unique consumed records;
- plaintext: 1,080,150 attempted, acknowledged, and unique consumed records;
- loss, duplicates, and unknown outcomes: zero for both phases;
- both phases recovered successfully, drained final gauges, and stayed within
  the disk reserve/growth guard.

This is a scoped pre-1.0 package boundary. It does not close the full V1-20
matrix, V1-21 high-load fault campaign, V1-22 performance SLO, V1-23 canary,
V1-24 API freeze, or any `1.0.0` claim.
