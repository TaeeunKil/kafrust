#!/usr/bin/env python3
"""Guard the bounded lifetime diagnostic's declared resource controls."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "published-multi-soak-lifetime-diagnostic.yml"


def fail(message: str) -> int:
    print(f"lifetime diagnostic resource check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        workflow = WORKFLOW.read_text(encoding="utf-8")
    except OSError as error:
        return fail(str(error))

    required_fragments = {
        "--cpus=1.0": "per-broker CPU cap",
        "--memory=2g": "per-broker memory cap",
        "--pids-limit=512": "per-broker PID cap",
        "--log-opt max-size=50m": "per-broker log-size cap",
        "--log-opt max-file=3": "per-broker log-retention cap",
        "KAFRUST_DISK_WATERMARK_GIB": "disk-watermark enforcement",
    }
    for fragment, description in required_fragments.items():
        if fragment not in workflow:
            return fail(f"missing {description}: {fragment}")

    if "docker system prune" in workflow or "docker volume prune" in workflow:
        return fail("workflow must not run a global Docker prune")
    if "qualified: false" not in workflow and "qualified=false" not in workflow:
        return fail("diagnostic descriptor must be forced to qualified=false")

    print("lifetime diagnostic resource controls ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
