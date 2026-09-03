#!/usr/bin/env python3
"""Validate the machine-readable V1-15 session ownership inventory."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-15-ownership-inventory.json"
REQUIRED_OWNER_IDS = {
    "producer-idle-cache",
    "buffered-producer-worker",
    "admin-clone-cache",
    "direct-fetch-session",
    "classic-group-membership",
    "kip848-group-membership",
    "share-consumer-session",
    "streams-membership",
    "telemetry-subscription",
    "blocking-adapter-runtime",
}
REQUIRED_FIELDS = {
    "owner_id",
    "owner",
    "identity_lease",
    "capacity",
    "saturation_policy",
    "cancellation",
    "join_path",
    "fault_points",
    "verification",
    "evidence_level",
}
EVIDENCE_LEVELS = {"local-deterministic", "published-pending"}


def fail(message: str) -> int:
    print(f"v1 ownership inventory check failed: {message}", file=sys.stderr)
    return 1


def non_empty_string(value: Any, field: str, owner_id: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{owner_id} has missing {field}")


def non_empty_string_list(value: Any, field: str, owner_id: str) -> None:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{owner_id} has empty {field}")
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{owner_id} has a non-string {field} entry")


def validate(manifest: dict[str, Any]) -> int:
    if manifest.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    if manifest.get("milestone") != "V1-15":
        raise ValueError("milestone must be V1-15")
    if manifest.get("status") != "deterministic-inventory":
        raise ValueError("status must remain deterministic-inventory")
    owners = manifest.get("owners")
    if not isinstance(owners, list):
        raise ValueError("owners must be a list")

    rows: dict[str, dict[str, Any]] = {}
    for row in owners:
        if not isinstance(row, dict):
            raise ValueError("every owner entry must be an object")
        missing = REQUIRED_FIELDS - set(row)
        if missing:
            raise ValueError(f"owner entry is missing fields: {sorted(missing)}")
        owner_id = row.get("owner_id")
        non_empty_string(owner_id, "owner_id", repr(owner_id))
        if owner_id in rows:
            raise ValueError(f"duplicate owner_id: {owner_id}")
        rows[owner_id] = row
        for field in REQUIRED_FIELDS - {"owner_id", "capacity", "fault_points", "verification"}:
            non_empty_string(row.get(field), field, owner_id)
        capacity = row.get("capacity")
        if not isinstance(capacity, dict) or capacity.get("kind") != "finite":
            raise ValueError(f"{owner_id} capacity must have kind=finite")
        non_empty_string(capacity.get("description"), "capacity.description", owner_id)
        non_empty_string_list(row.get("fault_points"), "fault_points", owner_id)
        non_empty_string_list(row.get("verification"), "verification", owner_id)
        if row.get("evidence_level") not in EVIDENCE_LEVELS:
            raise ValueError(f"{owner_id} has an unsupported evidence_level")

    actual = set(rows)
    if actual != REQUIRED_OWNER_IDS:
        raise ValueError(
            f"owner set differs: missing={sorted(REQUIRED_OWNER_IDS - actual)}, "
            f"unexpected={sorted(actual - REQUIRED_OWNER_IDS)}"
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
    print(f"v1 ownership inventory ok: {count} stable owners; finite capacities declared")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
