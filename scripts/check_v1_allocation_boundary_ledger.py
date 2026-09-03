#!/usr/bin/env python3
"""Validate the reviewed V1-18 allocation-boundary ledger."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-18-allocation-boundary-ledger.json"
REQUIRED_FIELDS = {
    "boundary_id",
    "input",
    "limit",
    "source_path",
    "validation_point",
    "failure",
    "test",
    "allocation_behavior",
    "status",
}
EXPECTED_BOUNDARIES = {
    "response-frame",
    "collection-array",
    "string-length",
    "bytes-length",
    "compact-varint-length",
    "tagged-fields",
    "message-set-record-batch",
    "compressed-record-batch",
    "fetch-poll-budget",
    "partition-queue",
    "producer-batch-record-count",
    "producer-batch-byte-size",
    "buffered-producer-capacity",
    "telemetry-payload",
}


def fail(message: str) -> int:
    print(f"v1 allocation-boundary ledger check failed: {message}", file=sys.stderr)
    return 1


def non_empty_string(value: Any, field: str, boundary_id: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{boundary_id} has missing {field}")


def validate(manifest: dict[str, Any], root: Path = ROOT) -> int:
    if manifest.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    if manifest.get("milestone") != "V1-18":
        raise ValueError("milestone must be V1-18")
    if manifest.get("status") != "deterministic-boundary-ledger":
        raise ValueError("status must remain deterministic-boundary-ledger")
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ValueError("entries must be a list")

    rows: dict[str, dict[str, Any]] = {}
    for row in entries:
        if not isinstance(row, dict):
            raise ValueError("every boundary entry must be an object")
        missing = REQUIRED_FIELDS - set(row)
        if missing:
            raise ValueError(f"boundary entry is missing fields: {sorted(missing)}")
        boundary_id = row.get("boundary_id")
        non_empty_string(boundary_id, "boundary_id", repr(boundary_id))
        if boundary_id in rows:
            raise ValueError(f"duplicate boundary_id: {boundary_id}")
        rows[boundary_id] = row
        for field in REQUIRED_FIELDS - {"boundary_id", "source_path", "status"}:
            non_empty_string(row.get(field), field, boundary_id)
        source_path = row.get("source_path")
        non_empty_string(source_path, "source_path", boundary_id)
        resolved = (root / source_path).resolve()
        if root.resolve() not in resolved.parents:
            raise ValueError(f"{boundary_id} source_path escapes repository")
        if not resolved.is_file():
            raise ValueError(f"{boundary_id} source_path does not exist: {source_path}")
        if row.get("status") != "bounded":
            raise ValueError(f"{boundary_id} status must be bounded")
        behavior = row["allocation_behavior"].lower()
        if "bound" not in behavior or "before" not in behavior and "during" not in behavior:
            raise ValueError(f"{boundary_id} must describe bounded before/during-allocation behavior")

    actual = set(rows)
    if actual != EXPECTED_BOUNDARIES:
        raise ValueError(
            f"boundary set differs: missing={sorted(EXPECTED_BOUNDARIES - actual)}, "
            f"unexpected={sorted(actual - EXPECTED_BOUNDARIES)}"
        )
    return len(rows)


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            raise ValueError("manifest root must be an object")
        count = validate(manifest)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        return fail(str(error))
    print(f"v1 allocation-boundary ledger ok: {count} reviewed boundaries")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
