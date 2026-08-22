#!/usr/bin/env python3
"""Validate the draft v1 compatibility matrix manifest."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-20-compatibility-matrix.json"
BROKERS = {"3.7.2", "3.8.1", "3.9.1", "4.0.0", "4.3.1", "not-applicable"}
TOPOLOGIES = {"single-node", "three-broker", "three-broker-controller-listener", "external-project"}
EVIDENCE = {"Live current-source", "Published artifact", "Packaged candidate"}
PROFILE_ID = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)+$")


def fail(message: str) -> int:
    print(f"v1 compatibility matrix check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return fail(str(error))

    if manifest.get("schema_version") != 1:
        return fail("schema_version must be 1")
    if manifest.get("status") not in {"draft", "frozen"}:
        return fail("status must be draft or frozen")
    if manifest.get("broker_lines") != ["3.7.2", "3.8.1", "3.9.1", "4.0.0", "4.3.1"]:
        return fail("broker_lines must preserve the V1-01 accepted order")
    policy = manifest.get("artifact_policy")
    if not isinstance(policy, dict) or policy.get("path_or_patch_dependencies") is not False:
        return fail("external projects must forbid path/patch dependencies")
    if policy.get("protocol_first_publication") is not True:
        return fail("protocol-first publication is required")

    profiles = manifest.get("profiles")
    if not isinstance(profiles, list) or not profiles:
        return fail("profiles must be a non-empty list")
    seen: set[str] = set()
    for profile in profiles:
        if not isinstance(profile, dict):
            return fail("each profile must be an object")
        required = {"id", "broker", "topology", "security", "group_protocol", "feature", "workloads", "evidence_required"}
        missing = sorted(required - profile.keys())
        if missing:
            return fail(f"{profile.get('id', '<unknown>')}: missing {missing}")
        identifier = profile["id"]
        if not isinstance(identifier, str) or not PROFILE_ID.fullmatch(identifier):
            return fail(f"invalid profile id {identifier!r}")
        if identifier in seen:
            return fail(f"duplicate profile id {identifier}")
        seen.add(identifier)
        if profile["broker"] not in BROKERS:
            return fail(f"{identifier}: unsupported broker {profile['broker']!r}")
        if profile["topology"] not in TOPOLOGIES:
            return fail(f"{identifier}: unsupported topology {profile['topology']!r}")
        if not isinstance(profile["workloads"], list) or not profile["workloads"]:
            return fail(f"{identifier}: workloads must be non-empty")
        evidence = profile["evidence_required"]
        if not isinstance(evidence, list) or not evidence or not set(evidence) <= EVIDENCE:
            return fail(f"{identifier}: unsupported evidence level")

    required_ids = {
        "floor-plaintext-core-single",
        "pinned-secure-scram-failover",
        "package-codec-feature-matrix",
    }
    missing = sorted(required_ids - seen)
    if missing:
        return fail(f"required profile ids are missing: {missing}")
    print(f"v1 compatibility matrix ok: {len(profiles)} draft profiles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
