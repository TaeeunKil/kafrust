#!/usr/bin/env bash
set -euo pipefail

# Long campaigns run on a pinned self-hosted runner. On WSL, /mnt/t is the
# Windows volume that owns the Ubuntu VHDX; checking only / would miss host
# exhaustion and can prevent WSL from starting on the next job.
# A six-hour run at the declared 10,000 records/s minimum and 1 KiB payload
# produces about 206 GiB of logical data; three replicas need roughly 618 GiB
# before indexes, metadata, retries, and filesystem overhead.
required_host_free_gib="${KAFRUST_REQUIRED_HOST_FREE_GIB:-700}"
required_docker_free_gib="${KAFRUST_REQUIRED_DOCKER_FREE_GIB:-700}"

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
