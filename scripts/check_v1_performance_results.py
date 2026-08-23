#!/usr/bin/env python3
"""Adjudicate V1-22 performance campaign result bundles.

The manifest checker proves that the campaign was designed correctly.  This
checker validates the retained descriptor/JSONL bundle and, when supplied, the
locked baseline used for regression decisions.  It intentionally does not
turn a short diagnostic into qualification: the descriptor timing and matrix
must satisfy the manifest supplied to it.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "docs" / "evidence" / "v1-22-performance-campaign-manifest.json"
HEX64 = re.compile(r"^[0-9a-f]{64}$")


class ResultError(ValueError):
    """A retained result bundle violates the V1-22 contract."""


def fail(message: str) -> int:
    print(f"v1 performance results check failed: {message}", file=sys.stderr)
    return 1


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ResultError(f"{path}: {error}") from error
    if not isinstance(value, dict):
        raise ResultError(f"{path}: top-level JSON value must be an object")
    return value


def number(value: Any, field: str, *, non_negative: bool = True) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResultError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or (non_negative and result < 0):
        raise ResultError(f"{field} must be finite and non-negative")
    return result


def integer(value: Any, field: str, *, non_negative: bool = True) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ResultError(f"{field} must be an integer")
    if non_negative and value < 0:
        raise ResultError(f"{field} must be non-negative")
    return value


def relative_result_path(descriptor_path: Path, result_file: Any, results_dir: Path) -> Path:
    if not isinstance(result_file, str) or not result_file:
        raise ResultError(f"{descriptor_path}: result_file must be a non-empty string")
    candidate = (descriptor_path.parent / result_file).resolve()
    root = results_dir.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise ResultError(f"{descriptor_path}: result_file escapes results directory") from error
    if not candidate.is_file():
        raise ResultError(f"{descriptor_path}: missing result file {result_file!r}")
    return candidate


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    final: dict[str, Any] | None = None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ResultError(f"{path}: {error}") from error
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise ResultError(f"{path}:{line_number}: invalid JSON: {error}") from error
        if not isinstance(row, dict):
            raise ResultError(f"{path}:{line_number}: JSONL row must be an object")
        mode = row.get("mode")
        if mode == "campaign-sample":
            samples.append(row)
        elif mode == "campaign-final":
            if final is not None:
                raise ResultError(f"{path}: more than one campaign-final row")
            final = row
    if final is None:
        raise ResultError(f"{path}: missing campaign-final row")
    return samples, final


def validate_samples(
    samples: list[dict[str, Any]],
    final: dict[str, Any],
    timing: dict[str, int],
    descriptor_path: Path,
) -> None:
    measured = timing["measured_seconds"]
    period = timing["sample_period_seconds"]
    expected_count = measured // period
    if measured % period != 0:
        raise ResultError("measured_seconds must be divisible by sample_period_seconds")
    if len(samples) != expected_count:
        raise ResultError(
            f"{descriptor_path}: expected {expected_count} samples, got {len(samples)}"
        )
    for index, row in enumerate(samples):
        if integer(row.get("sample_index"), "sample_index") != index:
            raise ResultError(f"{descriptor_path}: sample indexes are not contiguous")
        start = number(row.get("sample_start_seconds"), "sample_start_seconds")
        end = number(row.get("sample_end_seconds"), "sample_end_seconds")
        if start != index * period or end != (index + 1) * period:
            raise ResultError(f"{descriptor_path}: sample window has a gap or overlap")
        if row.get("profile") != final.get("profile"):
            raise ResultError(f"{descriptor_path}: sample profile differs from final profile")
        number(row.get("produce_records_per_second"), "produce_records_per_second")
        number(row.get("consume_records_per_second"), "consume_records_per_second")
        integer(row.get("requests_started"), "requests_started")
        integer(row.get("requests_failed"), "requests_failed")
        integer(row.get("retries"), "retries")
        number(row.get("retry_ratio"), "retry_ratio")
        if row.get("rss_bytes") is not None:
            integer(row.get("rss_bytes"), "rss_bytes")
        integer(row.get("in_flight_requests"), "in_flight_requests")
        integer(row.get("buffered_records"), "buffered_records")


def validate_final(
    final: dict[str, Any],
    descriptor: dict[str, Any],
    timing: dict[str, int],
    thresholds: dict[str, Any],
    descriptor_path: Path,
) -> tuple[str, float, float]:
    if final.get("mode") != "campaign-final":
        raise ResultError(f"{descriptor_path}: final row has the wrong mode")
    profile_id = descriptor.get("profile_id")
    if not isinstance(profile_id, str) or not profile_id or final.get("profile") != profile_id:
        raise ResultError(f"{descriptor_path}: final profile does not match descriptor")
    for field, expected in timing.items():
        if final.get(field) != expected:
            raise ResultError(f"{descriptor_path}: final {field} does not match descriptor timing")
    produced = integer(final.get("produced_records"), "produced_records")
    consumed = integer(final.get("consumed_records"), "consumed_records")
    if produced == 0 or produced != consumed:
        raise ResultError(f"{descriptor_path}: produced/consumed records do not reconcile")
    attempted = integer(final.get("attempted_records"), "attempted_records")
    acknowledged = integer(final.get("acknowledged_records"), "acknowledged_records")
    if attempted != produced or acknowledged != consumed or attempted != acknowledged:
        raise ResultError(f"{descriptor_path}: attempted/acknowledged records do not reconcile")
    if integer(final.get("unknown_outcomes"), "unknown_outcomes") != 0:
        raise ResultError(f"{descriptor_path}: unknown outcomes are non-zero")
    if integer(final.get("loss_count"), "loss_count") != 0:
        raise ResultError(f"{descriptor_path}: acknowledged loss is non-zero")
    if integer(final.get("duplicate_count"), "duplicate_count") != 0:
        raise ResultError(f"{descriptor_path}: duplicate count is non-zero")
    reconciliation = final.get("record_id_reconciliation")
    if not isinstance(reconciliation, dict) or reconciliation.get("qualified") is not True:
        raise ResultError(f"{descriptor_path}: record-ID reconciliation is not qualified")
    if integer(reconciliation.get("unique_records"), "record_id_reconciliation.unique_records") != consumed:
        raise ResultError(f"{descriptor_path}: record-ID unique count differs from consumed records")
    if integer(reconciliation.get("loss_count"), "record_id_reconciliation.loss_count") != 0:
        raise ResultError(f"{descriptor_path}: record-ID loss count is non-zero")
    if integer(reconciliation.get("duplicate_count"), "record_id_reconciliation.duplicate_count") != 0:
        raise ResultError(f"{descriptor_path}: record-ID duplicate count is non-zero")
    expected_digest = str(reconciliation.get("expected_digest", ""))
    observed_digest = str(reconciliation.get("observed_digest", ""))
    if not HEX64.fullmatch(expected_digest) or not HEX64.fullmatch(observed_digest):
        raise ResultError(f"{descriptor_path}: record-ID digests are not immutable")
    if expected_digest != observed_digest:
        raise ResultError(f"{descriptor_path}: expected and observed record-ID digests differ")
    if integer(final.get("in_flight_requests"), "in_flight_requests") != 0:
        raise ResultError(f"{descriptor_path}: in-flight requests did not drain")
    if integer(final.get("buffered_records"), "buffered_records") != 0:
        raise ResultError(f"{descriptor_path}: buffered records did not drain")
    retry_ratio = number(final.get("retry_ratio"), "retry_ratio")
    if retry_ratio > float(thresholds["retry_ratio_max_fraction"]):
        raise ResultError(f"{descriptor_path}: retry ratio exceeds manifest threshold")

    latency = final.get("latency_p50_p95_p99")
    if not isinstance(latency, dict):
        raise ResultError(f"{descriptor_path}: latency_p50_p95_p99 must be an object")
    for field in ("p50_ms", "p95_ms", "p99_ms"):
        number(latency.get(field), f"latency_p50_p95_p99.{field}")
    rss = final.get("rss_baseline_terminal_slope")
    if not isinstance(rss, dict):
        raise ResultError(f"{descriptor_path}: rss_baseline_terminal_slope must be an object")
    baseline = number(rss.get("baseline_bytes"), "rss.baseline_bytes")
    terminal = number(rss.get("terminal_bytes"), "rss.terminal_bytes")
    growth = number(rss.get("growth_bytes"), "rss.growth_bytes")
    slope = number(rss.get("slope_bytes_per_second"), "rss.slope_bytes_per_second", non_negative=False)
    sample_count = integer(rss.get("sample_count"), "rss.sample_count")
    if sample_count == 0 or terminal - baseline != growth:
        raise ResultError(f"{descriptor_path}: RSS baseline/terminal/growth are inconsistent")
    max_growth = int(thresholds["rss_growth_max_bytes"])
    if growth > max_growth or max(0.0, slope) * timing["measured_seconds"] > max_growth:
        raise ResultError(f"{descriptor_path}: RSS growth or extrapolated slope exceeds threshold")
    throughput = produced / timing["measured_seconds"]
    return profile_id, throughput, float(latency["p99_ms"])


def validate_descriptor(
    path: Path,
    results_dir: Path,
    manifest: dict[str, Any],
) -> tuple[tuple[str, str, str, int], float, float, str]:
    descriptor = read_json(path)
    if descriptor.get("schema_version") != 1:
        raise ResultError(f"{path}: schema_version must be 1")
    if descriptor.get("qualified") is not True:
        raise ResultError(f"{path}: descriptor is not marked qualified")
    profile_id = descriptor.get("profile_id")
    profile_map = {profile.get("id"): profile for profile in manifest.get("profiles", [])}
    if profile_id not in profile_map:
        raise ResultError(f"{path}: unknown profile_id {profile_id!r}")
    repetition = integer(descriptor.get("repetition"), "repetition")
    repetitions = int(manifest["repetitions"])
    if repetition < 1 or repetition > repetitions:
        raise ResultError(f"{path}: repetition must be between 1 and {repetitions}")
    artifact = descriptor.get("artifact")
    if not isinstance(artifact, dict) or not HEX64.fullmatch(str(artifact.get("artifact_digest", ""))):
        raise ResultError(f"{path}: artifact_digest must be a SHA-256 hex string")
    runner = descriptor.get("runner")
    if not isinstance(runner, dict):
        raise ResultError(f"{path}: runner must be an object")
    for field in ("image", "broker_image_digest", "broker_version", "topology", "security"):
        if not isinstance(runner.get(field), str) or not runner[field]:
            raise ResultError(f"{path}: runner.{field} is required")
    if runner["broker_version"] != manifest.get("broker"):
        raise ResultError(f"{path}: broker version differs from manifest")
    if runner["topology"] not in manifest.get("topologies", ()):
        raise ResultError(f"{path}: topology is not in the manifest matrix")
    if runner["security"] not in manifest.get("security", ()):
        raise ResultError(f"{path}: security profile is not in the manifest matrix")
    timing = descriptor.get("timing")
    expected_timing = {
        "warmup_seconds": int(manifest["timing"]["warmup_seconds"]),
        "measured_seconds": int(manifest["timing"]["measured_seconds"]),
        "sample_seconds": int(manifest["timing"]["sample_period_seconds"]),
    }
    if not isinstance(timing, dict) or any(timing.get(key) != value for key, value in expected_timing.items()):
        raise ResultError(f"{path}: descriptor timing does not match the manifest")
    workload = descriptor.get("workload")
    expected_profile = profile_map[profile_id]
    if not isinstance(workload, dict):
        raise ResultError(f"{path}: workload must be an object")
    for field in ("payload_bytes", "batch_size", "compression"):
        if workload.get(field) != expected_profile.get(field):
            raise ResultError(f"{path}: workload.{field} differs from profile {profile_id}")
    if workload.get("workers") != expected_profile.get("concurrency"):
        raise ResultError(f"{path}: workload.workers differs from profile concurrency")
    result_path = relative_result_path(path, descriptor.get("result_file"), results_dir)
    samples, final = read_jsonl(result_path)
    validate_samples(samples, final, {"measured_seconds": expected_timing["measured_seconds"], "sample_period_seconds": expected_timing["sample_seconds"]}, path)
    profile, throughput, p99 = validate_final(
        final,
        descriptor,
        {
            "warmup_seconds": expected_timing["warmup_seconds"],
            "measured_seconds": expected_timing["measured_seconds"],
            "sample_seconds": expected_timing["sample_seconds"],
        },
        manifest["thresholds"],
        path,
    )
    key = (profile, runner["topology"], runner["security"], repetition)
    return key, throughput, p99, str(artifact["artifact_digest"])


def validate_results(results_dir: Path, manifest: dict[str, Any], baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    descriptors = sorted(results_dir.rglob("*descriptor.json"))
    if not descriptors:
        raise ResultError(f"{results_dir}: no *descriptor.json files found")
    expected_profiles = {profile["id"] for profile in manifest["profiles"]}
    expected_topologies = set(manifest["topologies"])
    expected_security = set(manifest["security"])
    repetitions = int(manifest["repetitions"])
    expected_keys = {
        (profile, topology, security, repetition)
        for profile in expected_profiles
        for topology in expected_topologies
        for security in expected_security
        for repetition in range(1, repetitions + 1)
    }
    rows: dict[tuple[str, str, str, int], tuple[float, float, str]] = {}
    for path in descriptors:
        key, throughput, p99, digest = validate_descriptor(path, results_dir, manifest)
        if key in rows:
            raise ResultError(f"duplicate result key {key}")
        rows[key] = (throughput, p99, digest)
    missing = expected_keys - rows.keys()
    extra = rows.keys() - expected_keys
    if missing:
        raise ResultError(f"missing result combinations: {sorted(missing)[:3]}")
    if extra:
        raise ResultError(f"unexpected result combinations: {sorted(extra)[:3]}")
    digests = {row[2] for row in rows.values()}
    if len(digests) != 1:
        raise ResultError("all result descriptors must use one exact artifact digest")

    comparison: dict[str, Any] = {}
    if baseline is not None:
        if baseline.get("schema_version") != 1 or baseline.get("status") != "locked":
            raise ResultError("baseline must be schema_version 1 with status locked")
        baseline_profiles = baseline.get("profiles")
        if not isinstance(baseline_profiles, dict):
            raise ResultError("baseline profiles must be an object")
        for profile in expected_profiles:
            for topology in expected_topologies:
                for security in expected_security:
                    key = f"{profile}|{topology}|{security}"
                    values = [rows[(profile, topology, security, repetition)] for repetition in range(1, repetitions + 1)]
                    baseline_row = baseline_profiles.get(key)
                    if not isinstance(baseline_row, dict):
                        raise ResultError(f"baseline is missing {key}")
                    baseline_throughput = number(baseline_row.get("throughput_records_per_second"), f"baseline.{key}.throughput_records_per_second")
                    baseline_p99 = number(baseline_row.get("p99_latency_ms"), f"baseline.{key}.p99_latency_ms")
                    observed_throughput = float(median(value[0] for value in values))
                    observed_p99 = float(median(value[1] for value in values))
                    throughput_regression = (baseline_throughput - observed_throughput) / baseline_throughput
                    latency_regression = (observed_p99 - baseline_p99) / baseline_p99 if baseline_p99 else 0.0
                    if throughput_regression > float(manifest["thresholds"]["median_throughput_regression_fraction"]):
                        raise ResultError(f"{key}: median throughput regression exceeds threshold")
                    if latency_regression > float(manifest["thresholds"]["p99_latency_regression_fraction"]):
                        raise ResultError(f"{key}: median p99 latency regression exceeds threshold")
                    comparison[key] = {
                        "throughput_records_per_second": observed_throughput,
                        "p99_latency_ms": observed_p99,
                        "throughput_regression_fraction": throughput_regression,
                        "p99_latency_regression_fraction": latency_regression,
                    }
    return {"result_count": len(rows), "artifact_digest": next(iter(digests)), "comparison": comparison}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--baseline", type=Path)
    args = parser.parse_args()
    try:
        manifest = read_json(args.manifest)
        baseline = read_json(args.baseline) if args.baseline else None
        summary = validate_results(args.results_dir, manifest, baseline)
    except ResultError as error:
        return fail(str(error))
    print(
        f"v1 performance results check ok: {summary['result_count']} results; "
        f"artifact {summary['artifact_digest']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
