#!/usr/bin/env python3
"""Check resolved registry package yank/provenance metadata for V1-19.

Cargo's local sparse-index cache records the crates.io checksum, publication
time, and yank bit for each resolved package.  This gate makes that evidence
visible and fails on a resolved yanked package or a missing index entry.  It is
not a live advisory scan and does not claim that a local index cache is a
current vulnerability database.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from check_v1_sbom import DEFAULT_PLATFORM, ROOT, cargo_metadata, runtime_closure, source_kind


DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "v1-19-registry-provenance.json"
GENERATOR = "scripts/check_v1_registry_provenance.py"


def fail(message: str) -> int:
    print(f"v1 registry provenance check failed: {message}", file=sys.stderr)
    return 1


def index_rows(index_file: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for chunk in index_file.read_bytes().split(b"\0"):
        if not chunk.startswith(b"{"):
            continue
        try:
            value = json.loads(chunk)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def find_index_row(crate_name: str, version: str) -> dict[str, Any] | None:
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    index_root = cargo_home / "registry" / "index"
    matches: list[dict[str, Any]] = []
    for index_file in index_root.glob("*/.cache/**/" + crate_name):
        matches.extend(row for row in index_rows(index_file) if row.get("vers") == version)
    if not matches:
        return None
    checksums = {row.get("cksum") for row in matches}
    if len(checksums) != 1:
        raise RuntimeError(f"registry index disagrees on checksum for {crate_name}@{version}")
    return matches[0]


def build_report(metadata: dict[str, Any], platform: str) -> dict[str, Any]:
    packages, _nodes, reachable = runtime_closure(metadata)
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for package_id in sorted(reachable):
        package = packages[package_id]
        if source_kind(package) != "registry":
            continue
        name = package["name"]
        version = package["version"]
        row = find_index_row(name, version)
        if row is None:
            missing.append(f"{name}@{version}")
            continue
        checksum = row.get("cksum")
        if not isinstance(checksum, str) or len(checksum) != 64:
            raise RuntimeError(f"registry checksum missing or malformed for {name}@{version}")
        rows.append(
            {
                "name": name,
                "version": version,
                "checksum": checksum,
                "yanked": bool(row.get("yanked", False)),
                "pubtime": row.get("pubtime"),
            }
        )
    if missing:
        raise RuntimeError("registry index entries missing: " + ", ".join(sorted(missing)))
    yanked = sorted(f"{row['name']}@{row['version']}" for row in rows if row["yanked"])
    if yanked:
        raise RuntimeError("resolved package versions are yanked: " + ", ".join(yanked))
    return {
        "schema_version": 1,
        "generator": GENERATOR,
        "platform": platform,
        "features": "all-features",
        "dependency_scope": "runtime-and-build",
        "registry": "crates.io sparse-index cache",
        "summary": {
            "registry_packages": len(rows),
            "missing_index_entries": len(missing),
            "yanked_packages": len(yanked),
            "checksum_entries": sum(1 for row in rows if row["checksum"]),
        },
        "packages": rows,
        "non_claims": [
            "not a live crates.io server query",
            "not an advisory or vulnerability database scan",
            "not a source provenance or maintainer trust guarantee",
            "workspace package archive hashes are recorded by the V1-19 SBOM gate",
        ],
    }


def comparable(report: dict[str, Any]) -> tuple[Any, ...]:
    packages = tuple(
        sorted((item.get("name"), item.get("yanked")) for item in report.get("packages", []))
    )
    return (
        report.get("schema_version"),
        report.get("generator"),
        report.get("platform"),
        report.get("features"),
        report.get("dependency_scope"),
        report.get("registry"),
        packages,
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
                return fail("registry package/yank inventory drifted; review the report before updating evidence")
            print(
                f"v1 registry provenance ok: {report['summary']['registry_packages']} packages; "
                f"yanked={report['summary']['yanked_packages']}"
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
