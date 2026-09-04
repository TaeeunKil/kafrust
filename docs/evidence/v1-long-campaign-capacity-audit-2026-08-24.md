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

## Root-only temporary DNS recovery (2026-09-04)

An authorized root-only operation verified that both workstation/public
resolvers (`168.126.63.1` and `8.8.8.8`) resolve the GitHub Actions endpoints.
The generated resolver was temporarily replaced with those two nameservers and
only the `actions.runner.TaeeunKil-kafrust.wsl-ubuntu-t9.service` unit was
restarted; Docker and the WSL VM were not restarted. The listener log then
reported `Listening for Jobs`, and the GitHub runner inventory reported
`wsl-ubuntu-t9` as `online`, `busy: false` with its existing labels.

This is a connectivity recovery, not a persistent WSL networking fix:
`/etc/resolv.conf` will be regenerated after a WSL restart unless an
administrator applies the resolver policy permanently. The capacity guard
still passes, but no long campaign was dispatched during this recheck. A
fresh online/idle preflight is required immediately before any official
V1-21/V1-22 campaign.

The follow-up bounded published diagnostic
[33817682088](https://github.com/TaeeunKil/kafrust/actions/runs/33817682088)
then ran on the recovered runner with Kafka 4.3.1 for 60.002 seconds. It
processed 1,736,700 unique 1-KiB records through leader, coordinator, and
combined events with zero loss, duplicates, or unknown outcomes, and drained
all final gauges. Its descriptor and non-qualification boundary are retained
in [`v1-company-selfhosted-short-dns-recovery-2026-09-04.md`](v1-company-selfhosted-short-dns-recovery-2026-09-04.md).

## Bounded ramp plan and resource model (2026-09-04)

The official V1-21 contract is not shortened by this plan. A short run is a
separate diagnostic used to establish WSL lifetime, resolver persistence,
Docker cleanup, and storage growth before dispatching an exact campaign. It
must use a diagnostic campaign identifier and must not be promoted by the V1
fault adjudicator.

The declared V1-21 floor is 10,000 records per second with 1 KiB payloads.
For a three-broker replicated topic, the lower-bound retained data is therefore
approximately `duration * 10,000 * 1,024 * 3`; Kafka indexes, segment headers,
logs, retries, and filesystem overhead require additional headroom. The
resulting planning values are:

| Diagnostic or gate | Duration | Replicated data lower bound | Recommended free space |
| --- | ---: | ---: | ---: |
| bounded smoke | 10 min | about 18 GiB | at least 50 GiB |
| bounded soak | 30 min | about 54 GiB | at least 100 GiB |
| lifetime diagnostic | 2 h | about 216 GiB | at least 300 GiB |
| V1-21 exact campaign | 6 h | about 648 GiB | at least 700 GiB |

Reducing the diagnostic payload to 256 B reduces the data component by roughly
four, but it no longer matches the V1-21 workload. One broker or replication
factor one is also cheaper but cannot provide the required failover evidence.
The safe progression is 10 minutes, then 30 minutes, then two hours, with
prefix-scoped cleanup and a before/after `df` and `docker system df` record at
each step. Only after the two-hour run, a controlled WSL restart, and one
complete foreground-lifetime smoke are successful should an exact six-hour
campaign be considered.

The latest workstation capacity probe found about 737 GiB free on the T:
volume and 857 GiB at Docker's root, which is technically above the guard but
leaves only about 37 GiB of host margin for an exact V1-21 run. The current
`Ubuntu-T9` state is `Stopped` and the repository runner is `offline`, so no
campaign is dispatchable until the runner is online and held alive for the
entire bounded diagnostic. Parallel exact campaigns are not safe on this host;
the four V1-21 campaigns must be serialized if they are eventually run here.

### Observed-rate correction for the current soak helper

The current published multi-soak helper does not impose a records-per-second
rate limit; it sends batches as fast as the broker and client permit. The
60-second company-runner diagnostic [33817682088](https://github.com/TaeeunKil/kafrust/actions/runs/33817682088)
therefore provides a more useful host-specific upper-bound planning signal than
the 10,000-records/s V1 floor: it acknowledged and consumed 1,736,700 1-KiB
records, or approximately 28,945 records/s. At replication factor three, that
observed rate implies roughly 4.97 GiB of retained broker data per minute before
Kafka segment/index and filesystem overhead:

| Unthrottled helper duration | Observed-rate lower bound | Safe planning reserve |
| --- | ---: | ---: |
| 10 min | about 50 GiB | at least 100 GiB |
| 30 min | about 149 GiB | at least 250 GiB |
| 2 h | about 596 GiB | at least 700 GiB; not recommended on this host |
| 6 h | about 1.79 TiB | not feasible on this host |

These are still lower bounds, not guarantees. A long diagnostic on this host
must either use a rate-limited helper or stop at the first pre-set disk-watermark
threshold. A resource-light lifetime probe could target 1,000 records/s with
256-byte payloads and replication factor three: about 2.6 GiB per hour (about
5.2 GiB for two hours), with a 20–30 GiB reserve. That probe exercises runner
lifetime, broker restart recovery, cleanup, and final gauge draining, but it is
not V1-21 throughput evidence. A 1-KiB, 10,000-records/s run remains the only
workload that can be compared directly with the V1-21 floor.

### Rate-limited RF3 lifetime diagnostic prepared (2026-09-04)

The repository now contains a separate
[`published-multi-soak-lifetime-diagnostic.yml`](../../.github/workflows/published-multi-soak-lifetime-diagnostic.yml)
workflow for the small long-run question. Its defaults are a three-broker
KRaft cluster, replication factor three, 1,000 records/s, 256-byte values, and
two hours. Hard input caps limit the duration to two hours, the rate to 5,000
records/s, and the payload to 256 bytes. The producer helper applies a global
batch rate limiter, the workflow samples both the WSL-owned volume and the
Docker root, and it aborts before either falls below the configured 40-GiB
watermark. Resource names contain the run ID and cleanup removes only that
prefix; it does not run a global Docker prune.

At the default workload the retained-data lower bound is about 2.6 GiB/hour,
or 5.2 GiB for two hours, so a 40-GiB watermark leaves operational headroom.
The diagnostic now stops broker 1 once halfway through the run for a fixed
10-second outage, which makes retry/recovery observable without changing the
rate or storage budget. The descriptor is forced to `status=diagnostic` and
`qualified=false`, with explicit non-claims for V1-21 throughput, V1-22 SLO,
service-canary, and release evidence. The workflow has not been dispatched because
`wsl-ubuntu-t9` is currently offline and `Ubuntu-T9` is stopped. A successful
run would validate lifetime/restart/cleanup behavior only; it would not close
the exact six-hour campaign.

The workflow and helper formatting were validated on pushed head `730dd77`;
the repository CI run [33847498831](https://github.com/TaeeunKil/kafrust/actions/runs/33847498831)
passed on both stable and Rust 1.81. This validates the dispatch path and
static safety checks only; it is not a campaign execution result.
