#!/usr/bin/env python3
"""Guard the bounded lifetime diagnostic's declared resource controls."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "published-multi-soak-lifetime-diagnostic.yml"
LOCAL_LAUNCHER = ROOT / "scripts" / "run_local_lifetime_diagnostic.sh"
LOCAL_GUARD = ROOT / "scripts" / "local_lifetime_launcher_guard.sh"
RESOURCE_SAMPLER = ROOT / "scripts" / "local_lifetime_resource_sampler.py"
REQUIRED_FRAGMENTS = {
    "--cpus=1.0": "per-broker CPU cap",
    "--memory=2g": "per-broker memory cap",
    "--pids-limit=512": "per-broker PID cap",
    "--log-opt max-size=50m": "per-broker log-size cap",
    "--log-opt max-file=3": "per-broker log-retention cap",
    "KAFRUST_DISK_WATERMARK_GIB": "disk-watermark enforcement",
}


def fail(message: str) -> int:
    print(f"lifetime diagnostic resource check failed: {message}", file=sys.stderr)
    return 1


def validate_workflow(workflow: str) -> None:
    for fragment, description in REQUIRED_FRAGMENTS.items():
        if fragment not in workflow:
            raise ValueError(f"missing {description}: {fragment}")

    if "docker system prune" in workflow or "docker volume prune" in workflow:
        raise ValueError("workflow must not run a global Docker prune")
    if "qualified: false" not in workflow and "qualified=false" not in workflow:
        raise ValueError("diagnostic descriptor must be forced to qualified=false")


def validate_local_launcher(launcher: str) -> None:
    required_fragments = {
        "--cpus=1.0": "per-broker CPU cap",
        "--memory=2g": "per-broker memory cap",
        "--pids-limit=512": "per-broker PID cap",
        "--log-opt max-size=50m": "per-broker log-size cap",
        "--log-opt max-file=3": "per-broker log-retention cap",
        '"qualified": False': "non-qualifying descriptor",
        "docker network inspect": "run-prefix collision guard",
        "KAFRUST_LOCAL_DURATION_SECONDS 21600": "small default duration",
        "KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND 100": "small default rate",
        "KAFRUST_LOCAL_PAYLOAD_BYTES 64": "small default payload",
        "CARGO_TARGET_DIR=\"$cargo_target_dir\"": "explicit cargo target directory",
        "setsid timeout --kill-after=10s 900s cargo build --quiet --release":
            "bounded compile-before-fault ordering",
        'while kill -0 "$build_pid"': "build-time guard polling",
        "local_lifetime_launcher_guard.sh": "shared process and resource guard",
        "cleanup_run_resources": "run-scoped cleanup",
        "disk_budget_check": "shared disk-budget guard",
        '"$cargo_target_dir/release/kafrust-local-lifetime-diagnostic"':
            "prebuilt helper execution",
        "local_lifetime_resource_sampler.py": "local resource sampler",
        "resource-samples.jsonl": "resource sample artifact",
        '"acknowledged_records"': "acknowledged record validation",
        '"consumed_unique_records"': "consumed-unique record validation",
        '"resource_samples":': "resource sample descriptor reference",
        "setsid timeout --kill-after=10s": "bounded helper process groups",
    }
    for fragment, description in required_fragments.items():
        if fragment not in launcher:
            raise ValueError(f"local launcher is missing {description}: {fragment}")
    if "docker system prune" in launcher or "docker volume prune" in launcher:
        raise ValueError("local launcher must not run a global Docker prune")
    if '"qualified": True' in launcher:
        raise ValueError("local launcher must not produce qualified=true")


def validate_local_guard(guard: str) -> None:
    required_fragments = {
        "stop_process_group": "process-group termination",
        'kill -0 -- "-$pid"': "process-group liveness checks",
        'kill -TERM -- "-$pid"': "group TERM cleanup",
        'kill -KILL -- "-$pid"': "bounded group KILL cleanup",
        "docker rm -f -v": "run-scoped container cleanup",
        "awk -v prefix=\"$resource_prefix\"": "run-prefix cleanup filter",
        'docker network rm "$network_name"': "run-scoped network cleanup",
    }
    for fragment, description in required_fragments.items():
        if fragment not in guard:
            raise ValueError(f"local launcher guard is missing {description}: {fragment}")
    if "docker system prune" in guard or "docker volume prune" in guard:
        raise ValueError("local launcher guard must not run a global Docker prune")


def validate_resource_sampler(sampler: str) -> None:
    required_fragments = {
        "helper_process_tree_rss_bytes": "helper RSS samples",
        "helper_process_tree_os_thread_count": "helper OS-thread samples",
        "helper_process_tree_open_fd_count": "helper file-descriptor samples",
        "docker stats": "Docker memory samples",
        '"disk_free"': "disk-free samples",
        '"timestamp_utc"': "time-series timestamps",
    }
    for fragment, description in required_fragments.items():
        if fragment not in sampler:
            raise ValueError(f"resource sampler is missing {description}: {fragment}")
    if "tokio_task_count" in sampler:
        raise ValueError("resource sampler must not label OS threads as Tokio tasks")


def main() -> int:
    try:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        launcher = LOCAL_LAUNCHER.read_text(encoding="utf-8")
        guard = LOCAL_GUARD.read_text(encoding="utf-8")
        sampler = RESOURCE_SAMPLER.read_text(encoding="utf-8")
        validate_workflow(workflow)
        validate_local_launcher(launcher)
        validate_local_guard(guard)
        validate_resource_sampler(sampler)
    except (OSError, ValueError) as error:
        return fail(str(error))

    print("lifetime diagnostic resource controls ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
