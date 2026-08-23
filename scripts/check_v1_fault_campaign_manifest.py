#!/usr/bin/env python3
"""Validate the V1-21 fault/soak campaign manifest."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-21-fault-campaign-manifest.json"


def fail(message: str) -> int:
    print(f"v1 fault campaign check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return fail(str(error))
    if manifest.get("schema_version") != 1:
        return fail("schema_version must be 1")
    if manifest.get("status") not in {"preparation", "frozen"}:
        return fail("status must be preparation or frozen")
    policy = manifest.get("artifact_policy", {})
    for key in ("source_and_published_runs_are_separate", "exact_published_pair_required", "segment_identity_must_be_continuous"):
        if policy.get(key) is not True:
            return fail(f"artifact policy {key} must be true")
    bundle = manifest.get("result_bundle", {})
    if bundle.get("descriptor_glob") != "*fault-segment.json":
        return fail("result bundle must discover immutable fault segment descriptors")
    if bundle.get("adjudicator") != "scripts/check_v1_fault_results.py":
        return fail("fault result adjudicator path is not pinned")
    if bundle.get("requires_qualified_record_id_reconciliation") is not True or bundle.get("requires_contiguous_segment_indexes") is not True or bundle.get("requires_one_artifact_digest") is not True:
        return fail("fault result bundle qualification guards are incomplete")
    campaigns = manifest.get("campaigns")
    if not isinstance(campaigns, list) or len(campaigns) < 7:
        return fail("at least seven required campaigns must be named")
    seen: set[str] = set()
    for campaign in campaigns:
        identifier = campaign.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in seen:
            return fail(f"invalid or duplicate campaign id {identifier!r}")
        seen.add(identifier)
        if campaign.get("artifact_level") != "Published artifact":
            return fail(f"{identifier}: campaign must require published artifact evidence")
        if campaign.get("duration_seconds", -1) < 0:
            return fail(f"{identifier}: duration cannot be negative")
        if not isinstance(campaign.get("faults"), list) or not campaign["faults"]:
            return fail(f"{identifier}: faults must be non-empty")
    six_hour = [campaign for campaign in campaigns if campaign.get("duration_seconds") == 21_600]
    if len(six_hour) < 4:
        return fail("three pinned secured and one floor six-hour campaigns are required")
    churn = next((campaign for campaign in campaigns if campaign.get("id") == "member-loss-rejoin-cycles"), None)
    if churn is None or churn.get("minimum_cycles") != 100:
        return fail("member-loss campaign must require 100 cycles")
    ambiguity = next((campaign for campaign in campaigns if campaign.get("id") == "ambiguity-response-loss-families"), None)
    if ambiguity is None or ambiguity.get("minimum_outcomes_per_family") != 100:
        return fail("ambiguity campaign must require 100 outcomes per family")
    required_fields = {"artifact_digest", "workflow_sha", "segment_index", "record_id_reconciliation", "final_resource_gauges"}
    if not required_fields <= set(manifest.get("result_fields", ())):
        return fail("result fields omit immutable campaign/reconciliation identity")
    print(f"v1 fault campaign manifest ok: {len(campaigns)} campaigns; four six-hour gates named")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
