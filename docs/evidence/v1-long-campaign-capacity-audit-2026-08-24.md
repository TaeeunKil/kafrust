# V1-21/V1-22 Long-Campaign Capacity Audit (2026-08-24)

The long-campaign prerequisites were rechecked after the published refresh
commit `4a2b472d2ce1074cc83fe1df2cf672e021dcb2bf`.

## Observed capacity

At the time of the initial audit, the repository runner inventory endpoint
returned `total_count: 0`; no self-hosted runner was registered for
`TaeeunKil/kafrust`. The V1-21 fault
workflows and V1-22 performance workflow intentionally reject hosted labels
and require the pinned `self-hosted` label. The local Windows workstation also
does not have a Docker executable, so it cannot substitute for the required
Linux/Docker campaign environment.

The exact-head CI run
[32646817241](https://github.com/TaeeunKil/kafrust/actions/runs/32646817241)
passed both stable and Rust 1.81.0, including the manifest, adjudicator, and
runner-selection checks. This proves the qualification path is executable and
the guard is active; it does not create runner capacity or qualify a campaign.

## WSL2 preflight (2026-08-24)

A read-only preflight of the available `Ubuntu-T9` WSL environment found WSL2
with Ubuntu 24.04.4, Docker Engine client/server 29.5.3, systemd running, 16
logical CPUs, approximately 15 GiB memory, and 776 GiB free on the Linux
filesystem. This made the environment a technical runner candidate, but it
was not qualification capacity until a GitHub self-hosted runner was
explicitly registered and online.
The Windows host must also prevent sleep/restart during each uninterrupted
campaign, and the runner should use a Linux filesystem path rather than a
`/mnt/c` checkout.

V1-22's matrix allows up to eight concurrent jobs; one registered runner would
execute those jobs serially. Additional runner slots must be separately
isolated and resource-validated if concurrency is needed. Duplicating labels
does not create capacity and cannot substitute for a valid performance setup.

## Runner registration and preflight result (2026-08-24)

The WSL candidate was registered as `wsl-ubuntu-t9` with the labels
`self-hosted`, `Linux`, `X64`, `docker`, and `wsl2`. The repository inventory
then reported one online, idle runner. A first 60-second non-qualification
diagnostic (`32648644451`) reached the WSL runner and Docker cluster but failed
because the host did not expose a `python` executable. Installing Ubuntu's
`python-is-python3` and `jq` packages corrected that host prerequisite; the
diagnostic was not counted as qualification evidence, and its containers were
removed.

The repeat 60-second non-qualification diagnostic
[`32648820867`](https://github.com/TaeeunKil/kafrust/actions/runs/32648820867)
passed runner selection, checkout, Docker Kafka startup, published `0.3.6`
build, broker restart, descriptor validation, and artifact upload. The first
actual V1-21 campaign, `pinned-secured-six-hour-1`, was then dispatched as
[`32649020906`](https://github.com/TaeeunKil/kafrust/actions/runs/32649020906)
from source commit `54c8e21`. It is currently in progress and must not be
marked qualified until the six-hour descriptor and adjudicator checks pass.

## Consequence

V1-21 is now `In progress` with its first six-hour campaign running; the
remaining campaigns still require their own exact evidence. V1-22 remains
`In progress` because one runner serializes its 120-job, eight-hour matrix and
additional isolated capacity may be needed for a practical campaign schedule.
No hosted-runner diagnostic is promoted into either gate, and no threshold,
timeout, or matrix size is reduced to work around capacity.

## WSL runner interruption during the first campaign (2026-08-24)

The host-side runner inventory reported `wsl-ubuntu-t9` as `offline` with
`busy: true`, while GitHub still showed run
[`32649020906`](https://github.com/TaeeunKil/kafrust/actions/runs/32649020906)
as `in_progress` in the soak step. The WSL distribution was `Stopped` and a
direct start returned `Wsl/Service/CreateInstance/E_FAIL`; after an explicit
WSL shutdown, a retry returned `Wsl/Service/E_UNEXPECTED`. GitHub eventually
closed the run as `failure` at `2026-08-23T18:07:57Z`, with no downloadable
artifact. No fault-segment descriptor or qualification artifact was produced,
so this run is not evidence for V1-21 and must not be adjudicated as a pass.

Recovery requires restoring the host WSL service/VM (an elevated service
restart or host restart may be necessary), confirming Docker and the
self-hosted listener are online, and then rerunning the declared campaign
after the orphaned GitHub run reaches a terminal state. The campaign duration,
fault schedule, thresholds, and manifest identity remain unchanged.

## Host-volume capacity finding (2026-08-24)

Follow-up read-only host inspection found the concrete prerequisite failure:
the `T:` volume that stores `Ubuntu-T9` has only `31,256,576` free bytes
(approximately `29.8 MiB`), not 31 GiB as initially read. The registered
`T:\WSL\Ubuntu\ext4.vhdx` exists and is `830,855,970,816` bytes
(approximately `773.8 GiB`). `WslService`, `vmcompute`, and `hns` are running,
but Ubuntu-T9 remains `Stopped` and instance creation returns
`Wsl/Service/CreateInstance/E_FAIL`/`E_UNEXPECTED`. No recent disk/NTFS error
event was observed. The near-full host volume is therefore the primary
recovery hypothesis and must be relieved before filesystem repair is
considered; the VHDX itself must not be deleted or modified without a backup.

The volume breakdown explains the exhaustion: `T:\WSL\Ubuntu\ext4.vhdx` uses
`830,855,970,816` bytes (`773.79 GiB`) and the separate full export backup
`T:\Backups\Ubuntu-full.tar` uses `169,161,779,200` bytes (`157.54 GiB`).
Those two files account for approximately `931.33 GiB` of the volume. The
backup is not deleted as part of this audit; it should be moved to a different
volume (or otherwise retained) before any cleanup decision is made.

## Backup contents inventory (read-only, 2026-08-24)

The export tar was indexed without extraction (`3,723,551` members). Its
logical file totals are concentrated in `/var` (`114.76 GiB`) and `/home`
(`38.28 GiB`), with `/usr` at `2.28 GiB`. The largest single regular file is
the Docker JSON container log
`/var/lib/docker/containers/ee7487fb260abf351fa9f36cbc4ce26e7cf132ae760cd61a988fe5b2394ce84c/ee7487fb260abf351fa9f36cbc4ce84c-json.log`
at `14.65 GiB`. Many additional `0.34 GiB` containerd content blobs account
for substantial image/cache storage. The largest `/home` entries include a
`3.29 GiB` Git pack, a `1.04 GiB` Trivy database, and repeated project data
files around `0.6–0.9 GiB` each. This identifies Docker logs/image content and
duplicated project/cache data as the main internal consumers; no files were
removed or extracted during the inventory.

The tar inventory must not be mistaken for a live inventory of the larger
VHDX: the export was last written on `2026-08-12`, while the
`ext4.vhdx` length is `773.79 GiB` and it was last written on `2026-08-24`.
The VHDX is not marked as an NTFS sparse file. Its additional space may be
current Docker/container growth, deleted-but-unreclaimed ext4 blocks, or
filesystem free space reserved inside the image; only a successful WSL boot
and live filesystem inspection can distinguish those cases. No claim about
the current VHDX's internal top consumer is promoted until that inspection is
possible.

## Live VHDX consumer and runner recovery (2026-08-24)

After the export backup was copied to `C:\Users\user\Backups\Ubuntu-full.tar`
and the original `T:\Backups\Ubuntu-full.tar` was removed, `Ubuntu-T9` booted
normally. A live inspection then showed `/dev/sdd` at `771 GiB` used of `1007
GiB` with `185 GiB` available. The 773.79 GiB VHDX is therefore not an
unexplained filesystem allocation: Docker accounts for the dominant usage.

`docker system df` reported `631.7 GB` of reclaimable container writable
layers and `85.89 GB` of build cache (`61.44 GB` reclaimable). The three
containers named `kafrust-published-secure-multi-soak-1/2/3` each had a `211 GB`
writable layer, exited with code `255`, and were created by failed run
`32649020906`. Their diffs contain the run's Kafka log/data trees under
`/tmp/kafka-logs` and `/opt/kafka/logs`; these stale containers, not the small
Docker JSON log rotation files, are the primary live consumer. They are not
qualification evidence and must not be reused for a later campaign.

The registered `wsl-ubuntu-t9` listener was not installed as a systemd service
and was consequently offline after WSL recovery. It was restarted from the
existing configured runner directory; the repository inventory now reports one
online, idle runner with labels `self-hosted`, `Linux`, `X64`, `docker`, and
`wsl2`. This restores runner capacity, but it does not qualify V1-21 or V1-22.
The stale Kafka containers and reclaimable build cache still require an
explicit cleanup before dispatching another long campaign.

The stale containers were removed with their anonymous data volumes and the
unused Docker build cache was pruned. `/dev/sdd` then reported `110 GiB` used
and `846 GiB` available. A fresh WSL export completed with exit code `0` at
`T:\Backups\Ubuntu-full-2026-08-24.tar` (`115,386,419,200` bytes, last write
`2026-08-24 08:51:42`). Only after that success was the old C: copy
`C:\Users\user\Backups\Ubuntu-full.tar` deleted; it no longer exists. The
new export remains on T:, so it is a same-volume recovery copy rather than an
independent disaster-recovery location. Current host free space is
approximately `53.7 GB` on T: and `205.6 GB` on C:.

## Incident record and prevention controls

The complete root-cause, timeline, operational runbook, and follow-up list are
maintained in
[`v1-wsl-capacity-incident-2026-08-24.md`](v1-wsl-capacity-incident-2026-08-24.md).
The long-campaign workflows now check the Windows volume that owns the VHDX
(`df -P /mnt/t` under WSL) before dispatch, refuse less than `700 GiB` host or
`700 GiB` Docker-root free (one six-hour, three-replica campaign plus headroom),
and run an unconditional prefix-scoped container,
volume, network, and stale-build-cache cleanup. These guards prevent a known
capacity failure from being dispatched, but they do not qualify a campaign.

## Current runner preflight (2026-09-03)

The registered `wsl-ubuntu-t9` listener and its systemd service are present on
the company workstation. The repository inventory reported the runner online
after a root-only temporary resolver override, because WSL's generated
`10.255.255.254` resolver timed out for the GitHub Actions pipeline endpoints;
the company DNS server resolved them. The override was restored to the
generated resolver after the connectivity check, so no persistent WSL network
configuration was changed.

The long-campaign capacity guard was then run read-only and refused dispatch:
T: (`/mnt/t`) had `629.32 GiB` free while the guard requires `700 GiB`, whereas
the Docker root had `855 GiB` free. The only material movable item identified
on T: is `T:\Backups\Ubuntu-full-2026-08-24.tar` at `115,386,419,200` bytes
(`107.46 GiB`); `T:\WSL\Ubuntu\ext4.vhdx` is `208,931,913,728` bytes
(`194.58 GiB`). Moving the verified backup to an independent volume would
raise T: above the guard threshold, but no move or deletion was performed in
this preflight. The exact long-campaign manifest therefore remains not-run.

## Current runner connectivity recheck (2026-09-04)

The same company WSL distribution was rechecked from the workstation. The
runner listener process, its systemd service, and Docker service are active,
and the read-only capacity guard now passes with 736 GiB free on `/mnt/t` and
856 GiB free at Docker's `/var/lib/docker` root. GitHub's runner inventory,
however, reports `wsl-ubuntu-t9` as `offline`.

The generated `/etc/resolv.conf` still points at `10.255.255.254`; both
`github.com` and `broker.actions.githubusercontent.com` fail DNS resolution,
matching the Actions listener's repeated `Socket Error: TryAgain` and token or
broker timeout messages. A temporary public-DNS probe was not able to change
the resolver because the WSL user is not root and passwordless sudo is not
available. No resolver, Docker resource, or existing container was modified,
and no long campaign was dispatched. Recovery requires an authorized root-only
resolver fix (preferably persistent through the WSL networking policy), then a
fresh runner-online preflight before any V1-21/V1-22 dispatch.
