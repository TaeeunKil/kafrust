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
