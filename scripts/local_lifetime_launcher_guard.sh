#!/usr/bin/env bash

# Shared lifecycle primitives for the local lifetime diagnostic.  This file is
# sourced by the launcher and by the no-Docker abort tests; it intentionally
# contains no work at source time.

stop_process_group() {
  local pid="${1:-}"
  [[ -n "$pid" ]] || return 0
  # A leader may exit after TERM while a child remains in the same process
  # group.  Check the group ID as well as the leader PID so that child cannot
  # escape the bounded KILL fallback.
  if ! kill -0 "$pid" 2>/dev/null && ! kill -0 -- "-$pid" 2>/dev/null; then
    return 0
  fi

  # The launcher starts each long-lived helper under setsid.  Signal the
  # process group first so children cannot outlive their owning helper.
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  for _ in {1..20}; do
    if ! kill -0 "$pid" 2>/dev/null && ! kill -0 -- "-$pid" 2>/dev/null; then
      wait "$pid" 2>/dev/null || true
      return 0
    fi
    sleep 0.1
  done
  kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

cleanup_run_resources() {
  local resource_prefix="$1"
  local network_name="$2"
  local build_pid="${3:-}"
  local soak_pid="${4:-}"
  local sampler_pid="${5:-}"
  local fault_pid="${6:-}"

  stop_process_group "$build_pid"
  stop_process_group "$soak_pid"
  stop_process_group "$sampler_pid"
  stop_process_group "$fault_pid"
  while IFS= read -r container_name; do
    [[ -n "$container_name" ]] || continue
    docker rm -f -v "$container_name" >/dev/null 2>&1 || true
  done < <(
    docker ps -a --format '{{.Names}}' |
      awk -v prefix="$resource_prefix" 'index($0, prefix) == 1'
  )
  docker network rm "$network_name" >/dev/null 2>&1 || true
}
