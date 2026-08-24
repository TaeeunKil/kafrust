# V1 WSL Capacity Incident and Prevention Record

Date: 2026-08-24  
Incident: `V1-CAP-2026-08-24`  
Affected gates: V1-21, with V1-22 capacity risk  
Status: Recovered operationally; long-campaign qualification remains open

## Executive summary

The first declared V1-21 six-hour campaign
[`32649020906`](https://github.com/TaeeunKil/kafrust/actions/runs/32649020906)
failed without an artifact when the Ubuntu-T9 WSL instance stopped. The
immediate host failure was exhaustion of the Windows `T:` volume that stores
the WSL `ext4.vhdx`: only `29.8 MiB` remained. WSL consequently returned
`CreateInstance/E_FAIL` and `E_UNEXPECTED`, and the GitHub runner went offline.

The live VHDX inspection after recovery found the concrete capacity path.
Three exited Kafka containers left by the failed campaign each retained a
`211 GB` writable layer (`631.7 GB` total), mostly Kafka log/data trees under
`/tmp/kafka-logs` and `/opt/kafka/logs`. That size is consistent with the
declared workload's hundreds-of-gigabytes data budget (see below), so it is
not safe to label all of it a leak. The lifecycle failure was that the data
remained after the run failed. Docker build cache added `85.89 GB`, of which
`61.44 GB` was reclaimable. A separate `157.54 GiB` WSL export backup was also
stored on the same `T:` volume as the `773.79 GiB` VHDX. The combination
exhausted the host volume even though the filesystem inside the VHDX still had
reclaimable space.

This is an infrastructure and lifecycle failure, not a Kafka client result:
the run produced no descriptor or artifact and is not evidence for V1-21.

## Workload storage budget

The six-hour campaign is genuinely a large data operation. At the declared
minimum of `10,000` records/s, `21,600` seconds, and `1,024` payload bytes per
record, the logical payload lower bound is:

```text
10,000 × 21,600 × 1,024 = 221,184,000,000 bytes ≈ 206 GiB
```

With three Kafka replicas, the broker data lower bound is approximately
`618 GiB`, before segment indexes, metadata, retries, and filesystem overhead.
The observed `631.7 GB` across three broker writable layers is therefore in the
expected order of magnitude for one campaign. The correct prevention goal is
not to pretend this is a small test: it is to reserve capacity for one full
campaign, clean it after every outcome, and keep backups off the campaign
volume.

## Timeline and evidence

| Time / state | Evidence and consequence |
| --- | --- |
| 2026-08-23 15:35 UTC | The three `kafrust-published-secure-multi-soak-{1,2,3}` containers for run `32649020906` were created. |
| 2026-08-23 23:06 UTC | All three containers exited with code `255`; their writable layers remained because the workflow had no unconditional Docker cleanup. |
| 2026-08-24 preflight | `T:\WSL\Ubuntu\ext4.vhdx` measured `830,855,970,816` bytes (`773.79 GiB`); `T:\Backups\Ubuntu-full.tar` measured `169,161,779,200` bytes (`157.54 GiB`); T: free space was `31,256,576` bytes (`29.8 MiB`). |
| Recovery inspection | `docker system df` reported `631.7 GB` reclaimable container layers and `85.89 GB` build cache. Each stale Kafka container was `211 GB`. |
| Recovery | The old export was moved to C:, the stale containers and anonymous volumes were removed, and unused build cache was pruned. WSL then reported `846 GiB` available internally. |
| Runner repair | The listener was installed and enabled as `actions.runner.TaeeunKil-kafrust.wsl-ubuntu-t9.service`; GitHub now reports `wsl-ubuntu-t9` online and idle. |
| Backup replacement | `wsl --export` completed with exit code `0` to `T:\Backups\Ubuntu-full-2026-08-24.tar` (`115,386,419,200` bytes). Only then was the C: copy deleted. |

The new export is a same-volume recovery copy, not an independent disaster-
recovery backup. T: still holds the VHDX and the new tar; it must be relocated
to an independent volume before claiming backup resilience.

## Root cause and contributing factors

### Root cause

The long-campaign workflows created a workload that can legitimately consume
roughly `600+ GiB` of broker storage, but they had no capacity preflight and
never removed broker containers when a job failed or the runner disappeared.
The expected campaign allocation therefore became a persistent host
allocation, and competed with the VHDX's export backup, instead of being
reserved, observed, and reclaimed as a bounded campaign resource.

### Contributing factors

1. There was no pre-dispatch host-volume guard. Checking `/` inside WSL would
   have shown free filesystem blocks and missed the Windows volume holding the
   VHDX; the relevant WSL path is `/mnt/t`.
2. Docker build cache was allowed to accumulate across campaigns.
3. The VHDX and a full export backup shared the same nearly full Windows volume.
4. The GitHub runner listener was configured but not installed as a systemd
   service, so WSL recovery did not automatically restore runner capacity.
5. No post-failure operator checklist required `docker system df`, container
   cleanup, or host-volume verification before the next dispatch.

## Prevention controls now in the repository

The following controls are implemented in the long-campaign paths:

| Control | Implementation | Failure behavior |
| --- | --- | --- |
| Host-capacity preflight | [`scripts/check_campaign_capacity.sh`](../../scripts/check_campaign_capacity.sh) is called by the published multi-soak, secured multi-soak, and V1-22 performance workflows. On WSL it checks `/mnt/t`; otherwise it checks the runner root. | Refuses dispatch below `700 GiB` host free or `700 GiB` Docker-root free and prints `docker system df`; the threshold reserves one six-hour, three-replica workload plus headroom. |
| Unconditional cleanup | [`scripts/cleanup_campaign_docker.sh`](../../scripts/cleanup_campaign_docker.sh) runs with `if: always()` after diagnostics. | Removes only the workflow's container prefix and network, removes anonymous volumes, prunes build cache unused for 24 hours, and prints final capacity. |
| Runner lifecycle | Ubuntu-T9 now has an enabled systemd service for the configured GitHub runner. | WSL boot starts the listener automatically; a manual `run.sh` process is no longer the steady state. |
| Backup sequencing | A backup is not deleted until the replacement export has exit code `0` and a non-zero recorded size. | Prevents deleting the only known copy during a failed export. |

These are safety gates, not qualification evidence. A campaign that is
refused for capacity is `not-run`; a campaign that fails after starting still
requires its artifact and adjudicator checks.

## Required operational runbook

Before dispatching any V1-21/V1-22 long campaign:

1. Confirm the runner is online and idle in the repository inventory.
2. Run `bash scripts/check_campaign_capacity.sh` on the target runner and
   retain its output with the campaign evidence.
3. Confirm the backup target is independent of the VHDX volume, or record the
   run as lacking disaster-recovery backup coverage.
4. Dispatch only the declared manifest; do not lower duration, fault schedule,
   thresholds, or matrix size to fit capacity.

After success or failure:

1. Let the workflow's unconditional cleanup step finish; do not cancel it.
2. Verify no campaign-prefixed containers, networks, or large writable layers
   remain with `docker ps -a --size` and `docker system df`.
3. Verify both the Docker filesystem and the Windows volume (`df -h /mnt/t /`)
   remain above their guardrails before another dispatch.
4. If WSL stops, preserve the failed run as a non-result, restore the runner
   service, and perform the capacity audit before rerunning the exact manifest.

## Follow-up work still required

- Move the fresh export to independent storage; the current T: copy protects
  against accidental deletion but not T: or VHDX failure.
- Add a bounded broker writable-layer/retention monitor to the long-campaign
  harness. Any limit must be reviewed against the data-loss semantics before it
  is made part of a qualifying campaign.
- Exercise a controlled WSL restart and verify the systemd runner service comes
  online without manual intervention.
- Rerun the exact V1-21 six-hour manifest and complete all remaining campaigns;
  this incident does not close V1-21, V1-22, V1-23, or authorize a release.
