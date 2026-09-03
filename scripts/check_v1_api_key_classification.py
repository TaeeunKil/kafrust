#!/usr/bin/env python3
"""Check the explicit V1 classification for every Kafka API key 0 through 92."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-14-api-key-classification.json"
API_DIR = ROOT / "crates" / "kafrust-protocol" / "src" / "api"
MOD_FILE = API_DIR / "mod.rs"
EXPECTED_CLASSIFICATIONS = {
    "stable-core",
    "expert",
    "experimental",
    "broker-internal",
    "excluded",
}
API_KEY_RE = re.compile(r"^\s*pub const ([A-Z0-9_]*API_KEY): i16 = (-?\d+);$", re.MULTILINE)


def fail(message: str) -> int:
    print(f"v1 API-key classification check failed: {message}", file=sys.stderr)
    return 1


def implemented_keys() -> set[int]:
    keys: set[int] = set()
    for path in API_DIR.glob("*.rs"):
        if path.name == "mod.rs":
            continue
        matches = API_KEY_RE.findall(path.read_text(encoding="utf-8"))
        keys.update(int(key) for _name, key in matches)
    return keys


def validate(manifest: dict[str, Any], actual_keys: set[int]) -> tuple[int, int, int]:
    minimum = manifest.get("api_key_min")
    maximum = manifest.get("api_key_max")
    if minimum != 0 or maximum != 92:
        raise ValueError("manifest must cover API keys 0 through 92")
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ValueError("entries must be a list")
    rows: dict[int, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("every entry must be an object")
        key = entry.get("api_key")
        if not isinstance(key, int) or not 0 <= key <= 92:
            raise ValueError(f"invalid API key: {key!r}")
        if key in rows:
            raise ValueError(f"duplicate API key: {key}")
        for field in ("name", "classification", "owner", "reason"):
            if not isinstance(entry.get(field), str) or not entry[field].strip():
                raise ValueError(f"API key {key} has missing {field}")
        if entry["classification"] not in EXPECTED_CLASSIFICATIONS:
            raise ValueError(f"API key {key} has unknown classification")
        rows[key] = entry

    expected_keys = set(range(93))
    if set(rows) != expected_keys:
        raise ValueError(
            f"manifest key set differs: missing={sorted(expected_keys - set(rows))}, "
            f"unexpected={sorted(set(rows) - expected_keys)}"
        )
    missing_implementation_class = {
        key
        for key, entry in rows.items()
        if key in actual_keys and entry["classification"] in {"broker-internal", "excluded"}
    }
    if missing_implementation_class:
        raise ValueError(
            "implemented keys cannot be broker-internal/excluded: "
            f"{sorted(missing_implementation_class)}"
        )
    unclassified_implementation = {
        key for key in actual_keys if rows[key]["classification"] not in EXPECTED_CLASSIFICATIONS
    }
    if unclassified_implementation:
        raise ValueError(f"implemented keys lack a classification: {sorted(unclassified_implementation)}")
    if rows[82]["classification"] != "excluded":
        raise ValueError("API key 82 must remain explicitly excluded until UpdateRaftVoter exists")
    internal = sum(entry["classification"] == "broker-internal" for entry in rows.values())
    excluded = sum(entry["classification"] == "excluded" for entry in rows.values())
    return len(rows), internal, excluded


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        rows, internal, excluded = validate(manifest, implemented_keys())
    except (OSError, json.JSONDecodeError, ValueError) as error:
        return fail(str(error))
    print(
        f"v1 API-key classification ok: {rows} keys; "
        f"broker-internal={internal}; excluded={excluded}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
