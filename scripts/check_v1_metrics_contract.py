#!/usr/bin/env python3
"""Validate the machine-readable V1-17 metrics contract against Rust source."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-17-metrics-contract.json"
SOURCE = ROOT / "crates" / "kafrust" / "src" / "metrics.rs"
FIELD_RE = re.compile(r"^\s*pub\s+(\w+):\s*(.+),\s*$")
EXPECTED_TYPES = {
    "requests_started": "u64",
    "requests_succeeded": "u64",
    "requests_failed": "u64",
    "requests_timed_out": "u64",
    "requests_cancelled": "u64",
    "broker_errors": "u64",
    "retries": "u64",
    "buffered_records": "u64",
    "max_buffered_records": "u64",
    "produced_records": "u64",
    "produce_batches": "u64",
    "consumed_records": "u64",
    "request_bytes": "u64",
    "response_bytes": "u64",
    "in_flight_requests": "u64",
    "max_in_flight_requests": "u64",
    "total_latency": "Duration",
    "max_latency": "Duration",
    "request_latency_buckets": "[u64; LATENCY_BUCKET_UPPER_BOUNDS_NS.len()]",
}
ALLOWED_KINDS = {"counter", "gauge", "histogram"}
ALLOWED_AGGREGATIONS = {"cumulative", "current", "peak", "cumulative-or-delta"}


def fail(message: str) -> int:
    print(f"v1 metrics contract check failed: {message}", file=sys.stderr)
    return 1


def snapshot_fields(source: str) -> dict[str, str]:
    start = source.index("pub struct ClientMetricsSnapshot {")
    end = source.index("}\n\nimpl ClientMetricsSnapshot", start)
    fields: dict[str, str] = {}
    for line in source[start:end].splitlines()[1:]:
        match = FIELD_RE.match(line)
        if match:
            fields[match.group(1)] = match.group(2).strip()
    return fields


def non_empty_string(value: Any, field: str, metric: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{metric} has missing {field}")


def validate(manifest: dict[str, Any], actual_fields: dict[str, str]) -> int:
    if manifest.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    if manifest.get("milestone") != "V1-17":
        raise ValueError("milestone must be V1-17")
    if manifest.get("status") != "deterministic-contract":
        raise ValueError("status must remain deterministic-contract")
    metrics = manifest.get("metrics")
    if not isinstance(metrics, list):
        raise ValueError("metrics must be a list")

    rows: dict[str, dict[str, Any]] = {}
    for row in metrics:
        if not isinstance(row, dict):
            raise ValueError("every metric entry must be an object")
        name = row.get("name")
        non_empty_string(name, "name", repr(name))
        if name in rows:
            raise ValueError(f"duplicate metric name: {name}")
        rows[name] = row
        for field in ("rust_type", "kind", "unit", "aggregation", "lifecycle"):
            non_empty_string(row.get(field), field, name)
        if row["kind"] not in ALLOWED_KINDS:
            raise ValueError(f"{name} has unsupported kind")
        if row["aggregation"] not in ALLOWED_AGGREGATIONS:
            raise ValueError(f"{name} has unsupported aggregation")
        if row.get("max_cardinality") != 1:
            raise ValueError(f"{name} must have max_cardinality=1")

    if set(rows) != set(actual_fields):
        raise ValueError(
            f"metric field set differs: missing={sorted(set(actual_fields) - set(rows))}, "
            f"unexpected={sorted(set(rows) - set(actual_fields))}"
        )
    if set(actual_fields) != set(EXPECTED_TYPES):
        raise ValueError("the Rust snapshot field set differs from the frozen V1 contract")
    for name, rust_type in actual_fields.items():
        if EXPECTED_TYPES[name] != rust_type:
            raise ValueError(f"Rust field {name} type changed: {rust_type}")
        if rows[name]["rust_type"] != rust_type:
            raise ValueError(f"{name} manifest rust_type does not match source")
    return len(rows)


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        source = SOURCE.read_text(encoding="utf-8")
        if not isinstance(manifest, dict):
            raise ValueError("manifest root must be an object")
        count = validate(manifest, snapshot_fields(source))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return fail(str(error))
    print(f"v1 metrics contract ok: {count} snapshot metrics; max cardinality=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
