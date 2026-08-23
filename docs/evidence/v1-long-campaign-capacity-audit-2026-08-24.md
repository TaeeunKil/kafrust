# V1-21/V1-22 Long-Campaign Capacity Audit (2026-08-24)

The long-campaign prerequisites were rechecked after the published refresh
commit `4a2b472d2ce1074cc83fe1df2cf672e021dcb2bf`.

## Observed capacity

The repository runner inventory endpoint returned `total_count: 0`; no
self-hosted runner is registered for `TaeeunKil/kafrust`. The V1-21 fault
workflows and V1-22 performance workflow intentionally reject hosted labels
and require the pinned `self-hosted` label. The local Windows workstation also
does not have a Docker executable, so it cannot substitute for the required
Linux/Docker campaign environment.

The exact-head CI run
[32646817241](https://github.com/TaeeunKil/kafrust/actions/runs/32646817241)
passed both stable and Rust 1.81.0, including the manifest, adjudicator, and
runner-selection checks. This proves the qualification path is executable and
the guard is active; it does not create runner capacity or qualify a campaign.

## Consequence

V1-21 remains `In progress` with a capacity blocker for its four six-hour
campaigns and Share 100-cycle run. V1-22 remains `In progress` with the same
capacity blocker for its 120-job, eight-hour matrix. No hosted-runner
diagnostic is promoted into either gate, and no threshold, timeout, or matrix
size is reduced to work around the missing runner.
