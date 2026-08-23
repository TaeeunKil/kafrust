#!/usr/bin/env python3
"""Validate the V1-22 performance/SLO campaign manifest."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-22-performance-campaign-manifest.json"


def fail(message: str) -> int:
    print(f"v1 performance campaign check failed: {message}", file=sys.stderr)
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
    timing = manifest.get("timing", {})
    if timing.get("total_seconds") != 28_800 or timing.get("warmup_seconds") != 7_200 or timing.get("measured_seconds") != 21_600:
        return fail("campaign timing must be eight hours with two-hour warmup and six-hour measurement")
    if timing.get("sample_period_seconds") != 10:
        return fail("resource samples must be ten seconds apart")
    if manifest.get("repetitions") != 5:
        return fail("each profile requires five repetitions")
    profiles = manifest.get("profiles")
    if not isinstance(profiles, list) or len(profiles) < 6:
        return fail("at least six representative profiles must be named")
    ids = [profile.get("id") for profile in profiles]
    if any(not isinstance(identifier, str) or not identifier for identifier in ids) or len(set(ids)) != len(ids):
        return fail("profile IDs must be non-empty and unique")
    thresholds = manifest.get("thresholds", {})
    if thresholds.get("median_throughput_regression_fraction") != 0.20:
        return fail("throughput regression threshold must be 20 percent")
    if thresholds.get("p99_latency_regression_fraction") != 0.25:
        return fail("p99 latency regression threshold must be 25 percent")
    if thresholds.get("retry_ratio_max_fraction") != 0.01:
        return fail("steady-state retry ratio threshold must be one percent")
    if thresholds.get("final_resource_gauges_must_be_zero") is not True:
        return fail("final resource gauges must be required to drain")
    required_fields = {"artifact_digest", "profile_id", "repetition", "throughput", "latency_p50_p95_p99", "rss_baseline_terminal_slope", "retry_ratio", "final_resource_gauges", "attempted_records", "acknowledged_records", "unknown_outcomes", "record_id_reconciliation"}
    if not required_fields <= set(manifest.get("result_fields", ())):
        return fail("result fields omit a required SLO measurement")
    bundle = manifest.get("result_bundle", {})
    if bundle.get("descriptor_glob") != "*descriptor.json":
        return fail("result bundle must discover immutable descriptor files")
    if bundle.get("result_file_is_relative_to_descriptor") is not True:
        return fail("result files must be relative to their descriptor")
    if bundle.get("matrix_key") != "profile_id|topology|security|repetition":
        return fail("result bundle matrix key must include profile, topology, security, and repetition")
    if bundle.get("adjudicator") != "scripts/check_v1_performance_results.py":
        return fail("result bundle adjudicator path is not pinned")
    if bundle.get("qualification_requires_qualified_descriptor") is not True or bundle.get("qualification_requires_one_artifact_digest") is not True:
        return fail("result bundle qualification guards are incomplete")
    print(f"v1 performance campaign manifest ok: {len(profiles)} profiles; five 8-hour repetitions declared")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
