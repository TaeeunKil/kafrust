#!/usr/bin/env python3
"""Verify the dated owner/risk review matrix for V1-19 inventory entries."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from check_v1_sbom import ROOT


INVENTORY = ROOT / "docs" / "evidence" / "v1-19-unsafe-native-inventory.json"
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "v1-19-unsafe-native-review.json"
GENERATOR = "scripts/check_v1_unsafe_native_review.py"
INVENTORY_SHA256 = "6be7887b64ce8ca3bfe4f06f09ecdf1d201bca3a38a37b73a9f4b9601e18d840"


def fail(message: str) -> int:
    print(f"v1 unsafe/native review failed: {message}", file=sys.stderr)
    return 1


def classification(entry: dict[str, Any]) -> str:
    if entry.get("native_boundary"):
        return "native-platform-boundary"
    if entry.get("custom_build") or entry.get("links"):
        return "build-script-or-codegen"
    return "upstream-unsafe-implementation"


def review_action(kind: str) -> str:
    return {
        "native-platform-boundary": "retain only in the documented feature/target path; recheck parent graph, toolchain, and platform behavior on every update",
        "build-script-or-codegen": "retain only as an upstream build/code-generation boundary; recheck generated output and no-C default posture on every update",
        "upstream-unsafe-implementation": "retain as an upstream transitive implementation boundary; recheck advisory, source, and package drift on every update",
    }[kind]


def build_report(inventory: dict[str, Any]) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for entry in inventory.get("entries", []):
        kind = classification(entry)
        entries.append(
            {
                "name": entry["name"],
                "version": entry["version"],
                "source_kind": entry["source_kind"],
                "unsafe_constructs": entry["unsafe_constructs"],
                "custom_build": bool(entry["custom_build"]),
                "links": entry.get("links"),
                "native_boundary": bool(entry["native_boundary"]),
                "classification": kind,
                "owner": entry["owner"],
                "review_action": review_action(kind),
                "risk_disposition": "accepted for the 0.3.6 pre-1.0 candidate only; not a 1.0.0 safety certification",
            }
        )
    return {
        "schema_version": 1,
        "generator": GENERATOR,
        "date_utc": "2026-08-23",
        "inventory": "docs/evidence/v1-19-unsafe-native-inventory.json",
        "inventory_sha256": INVENTORY_SHA256,
        "scope": "all-feature runtime/build closure on x86_64-unknown-linux-gnu",
        "summary": {
            "entries_reviewed": len(entries),
            "native_platform_boundaries": sum(
                1 for entry in entries if entry["classification"] == "native-platform-boundary"
            ),
            "build_or_codegen_boundaries": sum(
                1 for entry in entries if entry["classification"] == "build-script-or-codegen"
            ),
            "upstream_unsafe_boundaries": sum(
                1 for entry in entries if entry["classification"] == "upstream-unsafe-implementation"
            ),
        },
        "entries": entries,
        "non_claims": [
            "not a source-code audit of every upstream unsafe block",
            "not a vulnerability or maintainer-trust guarantee",
            "not a universal no-native or no-C claim for optional TLS/platform features",
            "not final 1.0.0 risk acceptance; package, target, advisory, and dependency changes require re-review",
        ],
    }


def comparable(report: dict[str, Any]) -> tuple[Any, ...]:
    return (
        report.get("schema_version"),
        report.get("generator"),
        report.get("inventory"),
        report.get("inventory_sha256"),
        tuple(
            (
                entry.get("name"),
                entry.get("version"),
                entry.get("source_kind"),
                entry.get("classification"),
                entry.get("owner"),
                entry.get("review_action"),
                entry.get("risk_disposition"),
            )
            for entry in report.get("entries", [])
        ),
    )


def canonical_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, default=INVENTORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
        report = build_report(inventory)
        if args.check:
            expected = json.loads(args.output.read_text(encoding="utf-8"))
            if comparable(expected) != comparable(report):
                return fail("review matrix drifted from the unsafe/native inventory")
            if expected.get("summary", {}).get("entries_reviewed") != len(report["entries"]):
                return fail("review summary does not cover every inventory entry")
            print(
                f"v1 unsafe/native review ok: {len(report['entries'])} entries; "
                f"native={report['summary']['native_platform_boundaries']}"
            )
            return 0
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(canonical_json(report), encoding="utf-8")
        print(f"wrote {args.output}")
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        return fail(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
