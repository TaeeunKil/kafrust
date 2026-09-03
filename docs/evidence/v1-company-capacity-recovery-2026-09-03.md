# Company WSL2 capacity recovery (2026-09-03)

- host: company Windows x64 workstation
- distribution: Ubuntu-T9, Ubuntu 24.04.4, Linux `x86_64`
- Docker root: `/var/lib/docker`
- runner: `wsl-ubuntu-t9`, `self-hosted`, `Linux`, `X64`, `docker`, `wsl2`
- evidence level: local infrastructure preflight and short published diagnostic

The exact stale backup
`T:\Backups\Ubuntu-full-2026-08-24.tar` was inspected before removal. It was
115,386,419,200 bytes (about 107.46 GiB), dated 2026-08-24, and was the only
large file under `T:\Backups`. The explicitly authorized removal changed no
other T: folders, the WSL VHDX, Docker images, containers, networks, or
volumes.

After removal, the unchanged campaign guard
[`check_campaign_capacity.sh`](../../scripts/check_campaign_capacity.sh)
reported:

- `/mnt/t`: 736 GiB free (guard: at least 700 GiB)
- Docker root `/var/lib/docker`: 854 GiB free (guard: at least 700 GiB)

WSL's generated resolver was then saved and temporarily overridden with the
workstation/public resolvers so the runner could reach GitHub. The runner
service was restarted and GitHub reported `wsl-ubuntu-t9 online=false busy`
before work, then `online=true busy=false` before the diagnostic. No persistent
WSL network configuration was changed; the generated resolver is restored by
the operator cleanup step.

The self-hosted preflight and published short fault diagnostic passed in
[run 33716428169](https://github.com/TaeeunKil/kafrust/actions/runs/33716428169).
The diagnostic used exact published `0.3.6`, Kafka 4.3.1, and a 120-second
three-broker schedule. Its descriptor is retained separately; this record only
establishes that the runner and capacity gates are now executable.

The two infrastructure prerequisites for dispatch are therefore recovered, but
the long campaigns themselves remain pending. Four V1-21 six-hour campaigns,
the 100-cycle and ambiguity families, and the V1-22 eight-hour matrix still
require their complete artifacts and adjudication. V1-23 still requires an
externally named service and approved canary.

## DNS recovery follow-up (2026-09-04)

The generated WSL resolver later regressed to `10.255.255.254`, leaving the
runner offline even though the runner and Docker services were active. An
authorized root-only operation verified `168.126.63.1` and `8.8.8.8`, applied
them temporarily to `/etc/resolv.conf`, and restarted only the runner service.
The listener reached `Listening for Jobs`, and GitHub reported the runner
online and idle again. The WSL VM, Docker daemon, and existing resources were
not restarted or pruned. Because the override is temporary, a persistent WSL
resolver policy remains a prerequisite for unattended long campaigns.
