#!/usr/bin/env python3
"""Validate the versioned V1-18 fuzz campaign manifest."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-18-fuzz-campaign-manifest.json"
WORKFLOW = ROOT / ".github" / "workflows" / "fuzz.yml"
TARGETS = (
    "codec",
    "frame",
    "api_versions_response",
    "group_describe_response",
    "share_group_offsets_response",
    "streams_groups_response",
    "offset_commit_response",
    "offset_fetch_response",
    "compression",
    "list_groups_response",
)


def fail(message: str) -> int:
    print(f"v1 fuzz campaign check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        workflow = WORKFLOW.read_text(encoding="utf-8")
    except (OSError, json.JSONDecodeError) as error:
        return fail(str(error))
    if manifest.get("schema_version") != 1:
        return fail("schema_version must be 1")
    if tuple(manifest.get("targets", ())) != TARGETS:
        return fail("manifest targets must match the ten checked-in fuzz targets")
    discovery = manifest.get("discovery", {})
    qualification = manifest.get("qualification", {})
    if discovery.get("seconds_per_target") != 30:
        return fail("discovery duration must remain the 30-second smoke")
    if qualification.get("seconds_per_target", 0) < 3600:
        return fail("qualification duration must provide at least 60 minutes per target")
    if qualification.get("shards_per_target", 0) < 1:
        return fail("qualification must declare at least one shard")
    if qualification.get("job_timeout_minutes", 0) * 60 <= qualification["seconds_per_target"]:
        return fail("job timeout must exceed one target campaign duration")
    if "-max_total_time=30" not in workflow:
        return fail("workflow no longer shows the discovery smoke duration")
    if 'cargo +nightly fuzz check "$target"' not in workflow:
        return fail("workflow does not compile targets through the loop")
    if 'cargo +nightly fuzz run "$target"' not in workflow:
        return fail("workflow does not run targets through the loop")
    for target in TARGETS:
        if target not in workflow:
            return fail(f"workflow target list does not contain {target}")
    print(f"v1 fuzz campaign manifest ok: {len(TARGETS)} targets; qualification is 3600s/target")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
