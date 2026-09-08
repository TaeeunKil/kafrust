#!/usr/bin/env python3
"""Record low-cost local resource samples for the bounded lifetime helper.

The process counters are operating-system observations.  In particular,
``helper_process_tree_os_thread_count`` counts OS threads and is deliberately
not described as a Tokio-task count.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


PROC_ROOT = Path("/proc")
MEMORY_UNITS = {
    "B": 1,
    "kB": 1_000,
    "KB": 1_000,
    "MB": 1_000**2,
    "GB": 1_000**3,
    "TB": 1_000**4,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}
MEMORY_VALUE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)?\s*$")


def parse_memory_bytes(value: str) -> int:
    """Parse one Docker memory value, retaining byte precision."""

    match = MEMORY_VALUE.fullmatch(value)
    if match is None:
        raise ValueError(f"invalid memory value: {value!r}")
    number, unit = match.groups()
    factor = MEMORY_UNITS.get(unit or "B")
    if factor is None:
        raise ValueError(f"unsupported memory unit: {unit}")
    return int(float(number) * factor)


def parse_docker_memory_usage(value: str) -> tuple[int, int]:
    """Parse Docker's ``usage / limit`` memory field."""

    usage, separator, limit = value.partition("/")
    if not separator:
        raise ValueError(f"invalid Docker memory usage: {value!r}")
    return parse_memory_bytes(usage), parse_memory_bytes(limit)


def _status_values(status_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in status_path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition(":")
        if separator:
            values[key] = value.strip()
    return values


def _parent_pid(status_path: Path) -> int | None:
    try:
        value = _status_values(status_path).get("PPid")
        return int(value) if value is not None else None
    except (OSError, ValueError):
        return None


def process_tree_pids(pid: int, proc_root: Path = PROC_ROOT) -> list[int]:
    """Return the root PID and live descendants visible under ``proc_root``."""

    root = proc_root / str(pid)
    if not root.is_dir():
        return []
    parent_to_children: dict[int, list[int]] = {}
    for candidate in proc_root.iterdir():
        if not candidate.name.isdigit() or not candidate.is_dir():
            continue
        parent = _parent_pid(candidate / "status")
        if parent is not None:
            parent_to_children.setdefault(parent, []).append(int(candidate.name))

    result = [pid]
    pending = [pid]
    while pending:
        current = pending.pop()
        for child in parent_to_children.get(current, []):
            if child not in result:
                result.append(child)
                pending.append(child)
    return sorted(result)


def _process_metrics(pid: int, proc_root: Path) -> dict[str, int]:
    process_root = proc_root / str(pid)
    values = _status_values(process_root / "status")
    rss_kib = int(values.get("VmRSS", "0 kB").split()[0])
    os_thread_count = int(values.get("Threads", "0"))
    fd_root = process_root / "fd"
    open_fd_count = 0
    open_socket_count = 0
    try:
        descriptors = list(fd_root.iterdir())
    except OSError:
        descriptors = []
    for descriptor in descriptors:
        open_fd_count += 1
        try:
            if descriptor.is_symlink() and descriptor.readlink().as_posix().startswith("socket:"):
                open_socket_count += 1
        except OSError:
            continue
    return {
        "rss_bytes": rss_kib * 1024,
        "os_thread_count": os_thread_count,
        "open_fd_count": open_fd_count,
        "open_socket_count": open_socket_count,
    }


def sample_helper_process_tree(pid: int, proc_root: Path = PROC_ROOT) -> dict[str, object]:
    """Sample aggregate RSS, OS threads, descriptors, and sockets."""

    pids = process_tree_pids(pid, proc_root)
    totals = {
        "rss_bytes": 0,
        "os_thread_count": 0,
        "open_fd_count": 0,
        "open_socket_count": 0,
    }
    live_pids: list[int] = []
    for process_pid in pids:
        try:
            metrics = _process_metrics(process_pid, proc_root)
        except (OSError, ValueError):
            continue
        live_pids.append(process_pid)
        for key in totals:
            totals[key] += metrics[key]
    return {
        "helper_pid": pid,
        "helper_process_tree_alive": bool(live_pids),
        "helper_process_tree_process_count": len(live_pids),
        "helper_process_tree_rss_bytes": totals["rss_bytes"],
        # This is an OS thread count; it is not a Tokio task count.
        "helper_process_tree_os_thread_count": totals["os_thread_count"],
        "helper_process_tree_open_fd_count": totals["open_fd_count"],
        "helper_process_tree_open_socket_count": totals["open_socket_count"],
    }


def sample_docker_memory(container: str, docker_command: str = "docker") -> dict[str, object]:
    """Read one bounded, non-streaming Docker memory sample."""

    try:
        completed = subprocess.run(
            [docker_command, "stats", "--no-stream", "--format", "{{json .}}", container],
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as error:
        return {"error": f"docker stats failed: {error}"}
    if completed.returncode != 0:
        return {"error": f"docker stats exited with {completed.returncode}"}
    line = next((line for line in completed.stdout.splitlines() if line.strip()), "")
    try:
        fields = json.loads(line)
        usage, limit = parse_docker_memory_usage(str(fields["MemUsage"]))
        memory_percent = float(str(fields.get("MemPerc", "0%")).rstrip("%"))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        return {"error": f"unable to parse docker memory sample: {error}"}
    return {
        "memory_usage_bytes": usage,
        "memory_limit_bytes": limit,
        "memory_percent": memory_percent,
    }


def collect_sample(
    pid: int,
    docker_containers: list[str],
    disk_paths: list[str],
    *,
    proc_root: Path = PROC_ROOT,
    docker_command: str = "docker",
) -> dict[str, object]:
    now = time.time()
    helper = sample_helper_process_tree(pid, proc_root)
    disk_free: dict[str, object] = {}
    for path in disk_paths:
        try:
            disk_free[path] = {"free_bytes": shutil.disk_usage(path).free}
        except OSError as error:
            disk_free[path] = {"error": str(error)}
    docker_memory = {
        container: sample_docker_memory(container, docker_command)
        for container in docker_containers
    }
    return {
        "timestamp_utc": datetime.fromtimestamp(now, timezone.utc).isoformat(),
        "timestamp_unix_seconds": now,
        "helper": helper,
        "docker_memory": docker_memory,
        "disk_free": disk_free,
    }


def sample_until_exit(args: argparse.Namespace) -> int:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    saw_live_sample = False
    with args.output.open("w", encoding="utf-8") as handle:
        while True:
            sample = collect_sample(
                args.pid,
                args.docker_container,
                args.disk_path,
                docker_command=args.docker_command,
            )
            helper = sample["helper"]
            helper_alive = bool(helper["helper_process_tree_alive"])
            if helper_alive:
                saw_live_sample = True
                handle.write(json.dumps(sample, sort_keys=True) + "\n")
                handle.flush()
            elif saw_live_sample or args.once:
                break
            elif time.monotonic() - started >= args.startup_timeout_seconds:
                return 1
            if args.once:
                break
            time.sleep(args.interval_seconds)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval-seconds", type=float, default=10.0)
    parser.add_argument("--startup-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--docker-container", action="append", default=[])
    parser.add_argument("--disk-path", action="append", default=[])
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.interval_seconds <= 0 or args.startup_timeout_seconds <= 0:
        parser.error("sampling intervals and startup timeout must be positive")
    try:
        return sample_until_exit(args)
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        parser.exit(1, f"local resource sampler: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
