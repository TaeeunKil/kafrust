#!/usr/bin/env bash
set -euo pipefail

container_prefix="${1:?container prefix is required}"
network_name="${2:?network name is required}"

set +e
mapfile -t containers < <(docker ps -a --format '{{.Names}}' | awk -v prefix="$container_prefix" 'index($0, prefix) == 1')
if ((${#containers[@]} > 0)); then
  docker rm -f -v "${containers[@]}"
fi
docker network rm "$network_name" 2>/dev/null || true
docker builder prune -af --filter 'until=24h' || true
docker system df
df -h /mnt/t / 2>/dev/null || df -h /
