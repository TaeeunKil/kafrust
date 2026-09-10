#!/usr/bin/env bash
set -euo pipefail

# Long campaigns run on a pinned self-hosted runner. On WSL, /mnt/t is the
# Windows volume that owns the Ubuntu VHDX; checking only / would miss host
# exhaustion and can prevent WSL from starting on the next job.
# The published soak helper is deliberately throughput-seeking when no
# limiter is configured.  A static 700 GiB floor is enough for short
# diagnostics, but it is not a safe dispatch guard for the six-hour and
# eight-hour qualification jobs.  Use the declared campaign duration to keep
# those jobs off a runner without several TiB of headroom.  Operator overrides
# may increase these floors, but never lower them.
campaign_seconds=0
if [[ "${KAFRUST_SOAK_SECONDS:-}" =~ ^[0-9]+$ ]]; then
  campaign_seconds=$((KAFRUST_SOAK_SECONDS))
fi
if [[ "${KAFRUST_BENCH_WARMUP_SECONDS:-}" =~ ^[0-9]+$ &&
      "${KAFRUST_BENCH_MEASURED_SECONDS:-}" =~ ^[0-9]+$ ]]; then
  campaign_seconds=$((KAFRUST_BENCH_WARMUP_SECONDS + KAFRUST_BENCH_MEASURED_SECONDS))
fi

minimum_free_gib=700
if ((campaign_seconds >= 28800)); then
  minimum_free_gib=4000
elif ((campaign_seconds >= 21600)); then
  minimum_free_gib=2600
fi

requested_host_free_gib="${KAFRUST_REQUIRED_HOST_FREE_GIB:-$minimum_free_gib}"
requested_docker_free_gib="${KAFRUST_REQUIRED_DOCKER_FREE_GIB:-$minimum_free_gib}"
[[ "$requested_host_free_gib" =~ ^[0-9]+$ ]] || { echo "KAFRUST_REQUIRED_HOST_FREE_GIB must be an integer" >&2; exit 1; }
[[ "$requested_docker_free_gib" =~ ^[0-9]+$ ]] || { echo "KAFRUST_REQUIRED_DOCKER_FREE_GIB must be an integer" >&2; exit 1; }
required_host_free_gib=$((requested_host_free_gib > minimum_free_gib ? requested_host_free_gib : minimum_free_gib))
required_docker_free_gib=$((requested_docker_free_gib > minimum_free_gib ? requested_docker_free_gib : minimum_free_gib))
echo "campaign duration: ${campaign_seconds}s; enforced capacity floor: ${minimum_free_gib} GiB"

capacity_path=/
if df -Pk /mnt/t >/dev/null 2>&1; then
  capacity_path=/mnt/t
fi

available_kib="$(df -Pk "$capacity_path" | awk 'NR == 2 {print $4}')"
docker_root="$(docker info --format '{{.DockerRootDir}}')"
docker_available_kib="$(df -Pk "$docker_root" | awk 'NR == 2 {print $4}')"

case "$available_kib" in
  ''|*[!0-9]*) echo "could not read free space for $capacity_path" >&2; exit 1 ;;
esac
case "$docker_available_kib" in
  ''|*[!0-9]*) echo "could not read free space for $docker_root" >&2; exit 1 ;;
esac

available_gib=$((available_kib / 1024 / 1024))
docker_available_gib=$((docker_available_kib / 1024 / 1024))
echo "campaign capacity path: $capacity_path (${available_gib} GiB free)"
echo "Docker root: $docker_root (${docker_available_gib} GiB free)"
docker system df

if ((available_gib < required_host_free_gib)); then
  echo "insufficient host capacity: need at least ${required_host_free_gib} GiB free" >&2
  exit 1
fi
if ((docker_available_gib < required_docker_free_gib)); then
  echo "insufficient Docker capacity: need at least ${required_docker_free_gib} GiB free" >&2
  exit 1
fi
