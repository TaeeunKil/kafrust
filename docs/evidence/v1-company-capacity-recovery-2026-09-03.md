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

## DNS regeneration recheck (2026-09-04)

The resolver regenerated again as the WSL-managed symlink with
`nameserver 10.255.255.254`, while the runner service remained active. A
bounded root-only recovery saved the generated resolver (already retained at
`/var/tmp/codex-generated-resolv.conf`), replaced only `/etc/resolv.conf` with
`168.126.63.1` and `8.8.8.8`, and restarted only
`actions.runner.TaeeunKil-kafrust.wsl-ubuntu-t9.service`. GitHub then reported
`wsl-ubuntu-t9` online and idle. No WSL VM, Docker daemon, or existing Docker
resource was restarted, pruned, or modified. The recurrence confirms that a
persistent resolver policy is still required before unattended long campaigns.

## Persistent resolver policy staged (2026-09-04)

The named Ubuntu-T9 distribution now has `[network]` with
`generateResolvConf = false` in `/etc/wsl.conf`. The generated resolver was
copied to `/var/tmp/codex-generated-resolv.conf-2026-09-04.bak`, the prior
`/etc/wsl.conf` was retained at `/var/tmp/codex-wsl.conf-2026-09-04.bak`, and
`/etc/resolv.conf` was replaced with a regular `0644` file containing the
verified workstation/public resolvers `168.126.63.1` and `8.8.8.8`. Only the
runner service was restarted; Docker, the WSL VM, and existing containers,
networks, and volumes were not restarted, pruned, or modified.

After the change, `readlink -f /etc/resolv.conf` returned `/etc/resolv.conf`,
the runner service and Docker were active, and GitHub reported
`wsl-ubuntu-t9` online and idle. An approved `wsl --shutdown` was deliberately
not run, so persistence across a full WSL restart remains an explicit
verification step before an unattended long campaign. No long campaign was
dispatched from this change.

## Self-hosted runner lifecycle cancellation (2026-09-04)

A deliberately short published diagnostic was dispatched after the persistent
resolver policy was staged:
[run 33824960369](https://github.com/TaeeunKil/kafrust/actions/runs/33824960369)
used published `kafrust 0.3.6`, Kafka 4.3.1, a 120-second three-broker
schedule, and the `wsl-ubuntu-t9` self-hosted label. The runner accepted the
job and passed checkout and the capacity guard, but the job was cancelled in
the `Install Rust` step after the runner service was stopped at 10:14:19 KST.
Kafka startup was not reached, no campaign-scoped Docker resources were
created, and no client result or artifact was produced.

The host journal identifies a WSL shutdown, not a Rust or Kafka failure. The
Ubuntu-T9 boot containing the job ended at 10:14:26 KST, `last -x` records
repeated WSL poweroff/reboot cycles around 10:13--10:18, and systemd shows the
runner unit with `Restart=no` and no matching runner/WSL/Docker timer. The
service later started again and returned to `Listening for Jobs`; the runner
is now online and idle. The journal does not identify the Windows-side actor
that requested the shutdown, so the cause is recorded as an undetermined host
lifecycle event rather than an attributed operator action.

This is an infrastructure non-result. It does not count toward V1-21 or V1-22,
does not indicate a Docker capacity failure, and does not authorize a release.
Before any long campaign, keep a foreground WSL session or otherwise provide
a host-level lifetime guarantee, verify that the runner remains online through
the full short smoke, and perform the separately approved full-restart
resolver check. Existing Docker containers, networks, and volumes were not
restarted, pruned, or modified during this diagnostic.
