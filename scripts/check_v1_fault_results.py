#!/usr/bin/env python3
"""Adjudicate retained V1-21 published fault-campaign segments."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "docs" / "evidence" / "v1-21-fault-campaign-manifest.json"
HEX64 = re.compile(r"^[0-9a-f]{64}$")
HEX40 = re.compile(r"^[0-9a-f]{40}$")
IMAGE_DIGEST = re.compile(r"^(?:[A-Za-z0-9._:/-]+@)?sha256:[0-9a-f]{64}$|^[0-9a-f]{64}$")


class ResultError(ValueError):
    """A retained fault segment violates the V1-21 contract."""


def fail(message: str) -> int:
    print(f"v1 fault results check failed: {message}", file=sys.stderr)
    return 1


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ResultError(f"{path}: {error}") from error
    if not isinstance(value, dict):
        raise ResultError(f"{path}: top-level JSON value must be an object")
    return value


def integer(value: Any, field: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ResultError(f"{field} must be an integer")
    if (positive and value <= 0) or (not positive and value < 0):
        raise ResultError(f"{field} has an invalid value")
    return value


def number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResultError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ResultError(f"{field} must be finite and non-negative")
    return result


def validate_gauges(value: Any, path: Path) -> None:
    if not isinstance(value, dict):
        raise ResultError(f"{path}: final_resource_gauges must be an object")
    for field, raw in value.items():
        if integer(raw, f"final_resource_gauges.{field}") != 0:
            raise ResultError(f"{path}: final resource gauge {field} did not drain")


def validate_segment(path: Path, campaign_map: dict[str, dict[str, Any]]) -> tuple[str, int, int, dict[str, Any]]:
    descriptor = read_json(path)
    if descriptor.get("schema_version") != 1:
        raise ResultError(f"{path}: schema_version must be 1")
    campaign_id = descriptor.get("campaign_id")
    campaign = campaign_map.get(campaign_id)
    if campaign is None:
        raise ResultError(f"{path}: unknown campaign_id {campaign_id!r}")
    segment_index = integer(descriptor.get("segment_index"), "segment_index")
    segment_count = integer(descriptor.get("segment_count"), "segment_count", positive=True)
    if segment_index >= segment_count:
        raise ResultError(f"{path}: segment_index must be below segment_count")
    for field, pattern in (("artifact_digest", HEX64), ("broker_image_digest", IMAGE_DIGEST), ("workflow_sha", HEX40)):
        if not pattern.fullmatch(str(descriptor.get(field, ""))):
            raise ResultError(f"{path}: {field} must be an immutable hex identity")
    if descriptor.get("secret_scan_count") != 0:
        raise ResultError(f"{path}: secret_scan_count must be zero")
    if descriptor.get("continuity_claim") != "qualified":
        raise ResultError(f"{path}: cross-segment continuity is not qualified")
    result = descriptor.get("segment_result")
    if not isinstance(result, dict):
        raise ResultError(f"{path}: segment_result must be an object")
    if result.get("recovered") is not True:
        raise ResultError(f"{path}: segment did not report recovered=true")
    number(result.get("duration_seconds"), "segment_result.duration_seconds")
    validate_gauges(descriptor.get("final_resource_gauges"), path)
    reconciliation = descriptor.get("record_id_reconciliation")
    if not isinstance(reconciliation, dict) or reconciliation.get("qualified") is not True:
        raise ResultError(f"{path}: record-ID reconciliation is not qualified")
    integer(reconciliation.get("unique_records"), "record_id_reconciliation.unique_records", positive=True)
    if integer(reconciliation.get("loss_count"), "record_id_reconciliation.loss_count") != 0:
        raise ResultError(f"{path}: unaccounted record loss is non-zero")
    if integer(reconciliation.get("duplicate_count"), "record_id_reconciliation.duplicate_count") != 0:
        raise ResultError(f"{path}: duplicate record count is non-zero")
    if not HEX64.fullmatch(str(reconciliation.get("digest", ""))):
        raise ResultError(f"{path}: record reconciliation digest is not immutable")
    return campaign_id, segment_index, segment_count, descriptor


def validate_results(results_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    descriptors = sorted(results_dir.rglob("*fault-segment.json"))
    if not descriptors:
        raise ResultError(f"{results_dir}: no *fault-segment.json files found")
    campaigns = manifest.get("campaigns")
    if not isinstance(campaigns, list):
        raise ResultError("manifest campaigns must be a list")
    campaign_map = {campaign.get("id"): campaign for campaign in campaigns}
    grouped: dict[str, list[tuple[int, int, dict[str, Any]]]] = {}
    artifact_digests: set[str] = set()
    for path in descriptors:
        campaign_id, index, count, descriptor = validate_segment(path, campaign_map)
        grouped.setdefault(campaign_id, []).append((index, count, descriptor))
        artifact_digests.add(str(descriptor["artifact_digest"]))
    if len(artifact_digests) != 1:
        raise ResultError("all V1-21 campaigns must use one exact published artifact digest")
    for campaign_id, campaign in campaign_map.items():
        segments = grouped.get(campaign_id)
        if not segments:
            raise ResultError(f"missing campaign {campaign_id}")
        counts = {count for _, count, _ in segments}
        if len(counts) != 1:
            raise ResultError(f"{campaign_id}: segment_count changed between descriptors")
        count = next(iter(counts))
        indexes = sorted(index for index, _, _ in segments)
        if indexes != list(range(count)):
            raise ResultError(f"{campaign_id}: segment indexes are not contiguous")
        total_duration = sum(float(segment[2]["segment_result"]["duration_seconds"]) for segment in segments)
        required_duration = int(campaign.get("duration_seconds", 0))
        if total_duration < required_duration:
            raise ResultError(f"{campaign_id}: accumulated duration is below campaign requirement")
        if campaign_id == "member-loss-rejoin-cycles":
            cycles = sum(integer(segment[2]["segment_result"].get("cycle_count"), "cycle_count") for segment in segments)
            if cycles < int(campaign["minimum_cycles"]):
                raise ResultError(f"{campaign_id}: member-loss cycle count is below requirement")
        if campaign_id == "ambiguity-response-loss-families":
            totals: dict[str, int] = {}
            for _, _, segment in segments:
                outcomes = segment[2]["segment_result"].get("outcomes_by_family")
                if not isinstance(outcomes, dict):
                    raise ResultError(f"{campaign_id}: outcomes_by_family is required")
                for family, value in outcomes.items():
                    totals[family] = totals.get(family, 0) + integer(value, f"outcomes_by_family.{family}")
            for family in campaign["faults"]:
                if totals.get(family, 0) < int(campaign["minimum_outcomes_per_family"]):
                    raise ResultError(f"{campaign_id}: outcome count is below requirement for {family}")
        if campaign_id == "controlled-data-loss-fixtures":
            for _, _, segment in segments:
                if segment[2]["segment_result"].get("expected_outcomes_match") is not True:
                    raise ResultError(f"{campaign_id}: expected data-loss outcome did not match")
    return {"campaign_count": len(campaign_map), "segment_count": len(descriptors), "artifact_digest": next(iter(artifact_digests))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    try:
        summary = validate_results(args.results_dir, read_json(args.manifest))
    except ResultError as error:
        return fail(str(error))
    print(
        f"v1 fault results check ok: {summary['campaign_count']} campaigns, "
        f"{summary['segment_count']} contiguous segments; artifact {summary['artifact_digest']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
