# Company workstation return preflight and short broker verification

- date_utc: `2026-09-03T03:46:09Z`
- source_commit: `7c578f0a742214cdf290caa2a6e2f47950bc690a`
- host: company Windows x64 workstation
- local runtime: WSL2 Ubuntu-T9, Ubuntu 24.04.4, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3, root `/var/lib/docker`, 16 CPUs, approximately 855 GiB free
- WSL filesystem: approximately 856 GiB free
- Windows volume owning the WSL VHDX (`/mnt/t`): 629.3 GiB free
- GitHub runner: `wsl-ubuntu-t9`, `online`, `idle`, labels `self-hosted`,
  `Linux`, `X64`, `docker`, `wsl2` at capture time
- evidence level: local deterministic diagnostic only

## Network and capacity preflight

The generated WSL resolver (`10.255.255.254`) timed out for `github.com`. A
root-only temporary resolver override using the workstation DNS (`168.126.63.1`
and `8.8.8.8`) restored DNS and GitHub connectivity; the generated
`/etc/resolv.conf` was restored immediately after the diagnostic. No persistent
WSL network setting was changed.

The unchanged campaign guard reports `629 GiB` free on `/mnt/t`, below the
required `700 GiB` threshold. Docker root capacity is sufficient, but this
host-volume guard still prevents dispatch of the official V1-21/V1-22 long
campaigns. The guard was not weakened.

## Isolated short broker checks

Using only uniquely named containers and host ports (`19092`/`19093`), with no
Docker prune and no mutation of existing resources:

- Kafka 4.3.1 image digest
  `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`:
  `cargo test -p kafrust --test broker_roundtrip -- --nocapture` passed all 13
  tests in 0.54 seconds.
- Kafka 3.7.2 image digest
  `sha256:8bd63e1bd445e5e19427a4bdbcc3d23bf6efd774b058a41b36ba87fda7623e34`:
  the same 13-test suite passed in 0.54 seconds.

The containers were removed by the diagnostic's exit cleanup. These checks
confirm current-source broker connectivity and protocol roundtrips on the
company x86_64 workstation; they do not qualify a six-hour/24-hour campaign,
SLO, canary, release candidate, `0.3.7`, or `1.0.0`.

## Validation note

An additional WSL `cargo test --workspace --all-features` was started to
measure a full Linux build, but the first debug build on the Windows-mounted
workspace was still compiling after roughly ten minutes and was interrupted to
avoid unnecessary workstation load. No failure was observed. Required Rust
validation remains covered by the exact-head GitHub CI runs; this interruption
is not evidence of a passing or failing test suite.
