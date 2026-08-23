# External Gate Unblock Runbook

This runbook turns the current V1 blocker into concrete prerequisites. It does
not lower the V1-21/V1-22 durations, shrink the matrix, or treat a reference
fixture as a V1-23 service canary.

## 1. Provide a safe long-campaign runner

V1-21 and V1-22 require a Linux machine with Docker and a runner process that
can stay alive for the full job. GitHub documents that Docker-based container
and service workflows require Linux plus Docker on a self-hosted runner; the
repository workflows deliberately reject `ubuntu-*`, `windows-*`, and
`macos-*` labels.

The operator should:

1. Provision an isolated Linux VM or host with Docker, enough disk for broker
   logs and retained artifacts, and uninterrupted network access to GitHub and
   crates.io.
2. In the repository's **Settings → Actions → Runners**, choose **New
   self-hosted runner**, select Linux x64, and run the exact download/configure
   commands GitHub generates. Do not commit or paste the registration token.
3. Keep the runner label `self-hosted` (additional labels such as `linux`,
   `x64`, and `docker` are fine) and run the runner as a service or supervised
   process.
4. Verify Docker and runner capacity before dispatching a campaign:

   ```text
   docker version
   docker info
   df -h
   gh api repos/TaeeunKil/kafrust/actions/runners \
     --jq '{total_count, runners: [.runners[] | {name,status,busy,labels:[.labels[].name]}]}'
   ```

   The last command must show at least one online runner with the
   `self-hosted` label. A small 60-second diagnostic may validate the host,
   but it is not V1-21 evidence.

Self-hosted runners execute repository workflow code. GitHub recommends
restricting them to private repositories or tightly controlled runner groups;
this repository is public, so use an isolated/ephemeral machine and do not
expose credentials or other sensitive services to it. See the official
[runner setup](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/add-runners)
and [secure-use guidance](https://docs.github.com/en/actions/reference/security/secure-use).

## 2. Run the V1-21 gates in manifest order

After the runner inventory is online, dispatch the exact published `0.3.6`
campaigns from `main` using the campaign IDs in
[`v1-21-fault-campaign-manifest.json`](../../evidence/v1-21-fault-campaign-manifest.json):

- `floor-classic-six-hour` on Kafka 3.7.2;
- `pinned-secured-six-hour-1`, `-2`, and `-3` on Kafka 4.3.1; and
- the 100-cycle classic, KIP-848, and Share member-loss workflows; and
- the five ambiguity families plus controlled retention/unclean-election
  fixtures.

Use one contiguous segment where the manifest permits it. Do not mark a row
passed until the retained descriptor has the exact artifact digest, broker
image identity, contiguous segment index, record-ID reconciliation, drained
gauges, and zero secret-scan findings. The existing stale runs remain
immutable non-results.

## 3. Run and lock V1-22 performance evidence

Dispatch `v1-22-performance-campaign.yml` with `runner_label=self-hosted` and
the exact published checksum already recorded in
[`published-baseline.json`](../../evidence/published-baseline.json). The matrix
is six profiles × two topologies × two security modes × five repetitions, with
two hours of warmup and six hours measured at ten-second intervals.

The first complete bundle is an evidence candidate only. Run the checked-in
adjudicator, review the distributions and competitor comparison, then commit a
locked baseline. Rerun with `require_baseline=true`; only a complete bundle
that passes the regression, RSS, retry, loss, duplicate, and final-gauge gates
can close V1-22.

## 4. Supply the V1-23 canary authority

The user/project owner must register all of the following; the in-repository
reference fixture cannot supply them:

```text
service_id:
owner:
repository/deployment:
approved_environment:
Kafka topology and security:
candidate artifact version:
rollback objective and approver:
credential-rotation owner:
```

Once supplied, run the migration stages in order: baseline, forward cutover,
fault observation, rollback, and post-rollback. Each stage must reconcile
business IDs, offsets, unknown outcomes, credentials, and duplicate risk before
promotion. Without this intake, V1-23 and its dependent API-freeze/RC/release
milestones remain blocked by design.

## 5. Let the fuzz schedule provide the weekly evidence

The two remaining V1-18 campaign sets must come from the weekly schedule in
`fuzz-qualification.yml`. Same-day duplicate manual dispatches are not a
substitute for consecutive weekly passes. Each set must retain all 40 shard
artifacts, corpus hashes, 900-second shard durations, and crash/OOM
dispositions before the manifest can move to `qualified`.
