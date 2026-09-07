#!/usr/bin/env python3
"""Guard the bounded lifetime diagnostic's declared resource controls."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "published-multi-soak-lifetime-diagnostic.yml"
LOCAL_LAUNCHER = ROOT / "scripts" / "run_local_lifetime_diagnostic.sh"
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
        "docker rm -f -v": "run-scoped container cleanup",
        "docker network rm": "run-scoped network cleanup",
        "docker network inspect": "run-prefix collision guard",
        "KAFRUST_LOCAL_DURATION_SECONDS 21600": "small default duration",
        "KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND 100": "small default rate",
        "KAFRUST_LOCAL_PAYLOAD_BYTES 64": "small default payload",
    }
    for fragment, description in required_fragments.items():
        if fragment not in launcher:
            raise ValueError(f"local launcher is missing {description}: {fragment}")
    if "docker system prune" in launcher or "docker volume prune" in launcher:
        raise ValueError("local launcher must not run a global Docker prune")
    if '"qualified": True' in launcher:
        raise ValueError("local launcher must not produce qualified=true")


def main() -> int:
    try:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        launcher = LOCAL_LAUNCHER.read_text(encoding="utf-8")
        validate_workflow(workflow)
        validate_local_launcher(launcher)
    except (OSError, ValueError) as error:
        return fail(str(error))

    print("lifetime diagnostic resource controls ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
