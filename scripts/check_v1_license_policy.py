#!/usr/bin/env python3
"""Check the V1-19 runtime/build dependency license policy.

This is a deliberately small, dependency-free SPDX expression check.  Cargo
provides the package metadata; this script verifies that every package in the
same all-feature runtime/build closure used by the V1-19 SBOM has an SPDX
expression whose identifiers are accepted by the project's permissive-license
policy.  It is not an advisory, yank, copyright-notice, or source-code audit.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from check_v1_sbom import DEFAULT_PLATFORM, ROOT, cargo_metadata, runtime_closure, source_kind


DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "v1-19-license-policy.json"
GENERATOR = "scripts/check_v1_license_policy.py"
ROOT_NAMES = {"kafrust", "kafrust-protocol"}
SPDX_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9.+-]*")
OPERATORS = {"AND", "OR", "WITH"}

# All licenses currently resolved by the release closure are permissive and
# compatible with kafrust's MIT OR Apache-2.0 distribution choice.  Keep this
# list explicit so a newly introduced copyleft/proprietary expression fails
# review instead of silently expanding the policy.
ACCEPTED_SPDX_IDS = frozenset(
    {
        "0BSD",
        "Apache-2.0",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "ISC",
        "MIT",
        "Unicode-3.0",
        "Unlicense",
        "Zlib",
    }
)


def fail(message: str) -> int:
    print(f"v1 license policy check failed: {message}", file=sys.stderr)
    return 1


def expression_ids(expression: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                token
                for token in SPDX_ID.findall(expression)
                if token not in OPERATORS
            }
        )
    )


def package_rows(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    packages, _nodes, reachable = runtime_closure(metadata)
    rows: list[dict[str, Any]] = []
    for package_id in sorted(reachable):
        package = packages[package_id]
        expression = package.get("license")
        if not expression or package.get("license_file"):
            raise RuntimeError(
                f"{package.get('name', '<unknown>')} must provide an SPDX license expression"
            )
        identifiers = expression_ids(expression)
        if not identifiers:
            raise RuntimeError(f"{package['name']} has an empty SPDX expression")
        disallowed = sorted(set(identifiers) - ACCEPTED_SPDX_IDS)
        if disallowed:
            raise RuntimeError(
                f"{package['name']} uses disallowed SPDX identifiers: {', '.join(disallowed)}"
            )
        rows.append(
            {
                "name": package["name"],
                "version": package["version"],
                "source_kind": source_kind(package),
                "license_expression": expression,
                "spdx_ids": list(identifiers),
            }
        )
    return rows


def build_report(metadata: dict[str, Any], platform: str) -> dict[str, Any]:
    rows = package_rows(metadata)
    roots = {
        row["name"]: row["version"]
        for row in rows
        if row["name"] in ROOT_NAMES and row["source_kind"] == "workspace"
    }
    if set(roots) != ROOT_NAMES:
        raise RuntimeError(f"workspace roots missing from license closure: {sorted(roots)}")
    return {
        "schema_version": 1,
        "generator": GENERATOR,
        "platform": platform,
        "features": "all-features",
        "dependency_scope": "runtime-and-build",
        "policy": {
            "distribution_license": "MIT OR Apache-2.0",
            "accepted_spdx_ids": sorted(ACCEPTED_SPDX_IDS),
        },
        "workspace_roots": roots,
        "summary": {
            "packages": len(rows),
            "disallowed_identifiers": 0,
            "missing_expressions": 0,
        },
        "packages": rows,
        "non_claims": [
            "not an advisory or yank review",
            "not a copyright-notice or packaged-license-file audit",
            "not a transitive unsafe/native-code review",
            "not proof that every dependency is compatible with every downstream license",
        ],
    }


def comparable(report: dict[str, Any]) -> tuple[Any, ...]:
    policy = report.get("policy", {})
    package_keys = tuple(
        sorted(
            (
                package.get("name"),
                package.get("source_kind"),
                package.get("license_expression"),
                tuple(package.get("spdx_ids", [])),
            )
            for package in report.get("packages", [])
        )
    )
    roots = tuple(sorted((report.get("workspace_roots") or {}).items()))
    return (
        report.get("schema_version"),
        report.get("generator"),
        report.get("platform"),
        report.get("features"),
        report.get("dependency_scope"),
        policy.get("distribution_license"),
        tuple(policy.get("accepted_spdx_ids", [])),
        roots,
        package_keys,
    )


def canonical_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        report = build_report(cargo_metadata(args.platform), args.platform)
        if args.check:
            expected = json.loads(args.output.read_text(encoding="utf-8"))
            if comparable(expected) != comparable(report):
                return fail(
                    "license inventory/policy drifted; review the report before updating evidence"
                )
            print(
                f"v1 license policy ok: {len(report['packages'])} packages, "
                f"identifiers={len(report['policy']['accepted_spdx_ids'])}"
            )
            return 0
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(canonical_json(report), encoding="utf-8")
        print(f"wrote {args.output}")
    except (OSError, RuntimeError, json.JSONDecodeError, KeyError, subprocess.SubprocessError) as error:
        return fail(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
