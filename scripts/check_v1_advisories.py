#!/usr/bin/env python3
"""Snapshot and verify OSV/RustSec advisory coverage for the V1-19 closure.

The refresh path performs one OSV ``querybatch`` request for the resolved
runtime/build packages.  The check path is intentionally offline: it verifies
that the committed snapshot still describes the exact package inventory, is
within its review window, and has no recorded critical/high result.  This
keeps ordinary CI deterministic while forcing a fresh advisory review when
the snapshot ages out or dependency resolution changes.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any

from check_v1_sbom import DEFAULT_PLATFORM, ROOT, cargo_metadata, runtime_closure, source_kind


DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "v1-19-advisories.json"
GENERATOR = "scripts/check_v1_advisories.py"
OSV_ENDPOINT = "https://api.osv.dev/v1/querybatch"
RUSTSEC_REPOSITORY = "https://github.com/RustSec/advisory-db"
RUSTSEC_REVISION = "bf5c0d245a92671908518d7e765914d437954ed6"
DEFAULT_MAX_AGE_DAYS = 30


def fail(message: str) -> int:
    print(f"v1 advisory check failed: {message}", file=sys.stderr)
    return 1


def inventory(metadata: dict[str, Any]) -> list[dict[str, str]]:
    packages, _nodes, reachable = runtime_closure(metadata)
    return [
        {
            "name": packages[package_id]["name"],
            "version": packages[package_id]["version"],
            "source_kind": source_kind(packages[package_id]),
        }
        for package_id in sorted(reachable)
    ]


def advisory_severity(vulnerability: dict[str, Any]) -> str | None:
    values: list[str] = []
    for item in vulnerability.get("severity", []):
        if isinstance(item, dict) and isinstance(item.get("score"), str):
            values.append(item["score"])
    database_specific = vulnerability.get("database_specific", {})
    if isinstance(database_specific, dict) and isinstance(database_specific.get("severity"), str):
        values.append(database_specific["severity"])
    normalized = {value.upper() for value in values}
    for level in ("CRITICAL", "HIGH", "MODERATE", "MEDIUM", "LOW"):
        if level in normalized:
            return level
    return None


def query_osv(packages: list[dict[str, str]]) -> list[dict[str, Any]]:
    queries = [
        {
            "package": {"name": package["name"], "ecosystem": "crates.io"},
            "version": package["version"],
        }
        for package in packages
    ]
    request = urllib.request.Request(
        OSV_ENDPOINT,
        data=json.dumps({"queries": queries}).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.load(response)
    results = payload.get("results")
    if not isinstance(results, list) or len(results) != len(packages):
        raise RuntimeError("OSV querybatch returned an unexpected result count")
    return results


def build_report(metadata: dict[str, Any], platform: str) -> dict[str, Any]:
    packages = inventory(metadata)
    results = query_osv(packages)
    rows: list[dict[str, Any]] = []
    for package, result in zip(packages, results):
        advisories: list[dict[str, Any]] = []
        for vulnerability in result.get("vulns", []):
            if not isinstance(vulnerability, dict):
                continue
            advisories.append(
                {
                    "id": vulnerability.get("id"),
                    "aliases": sorted(vulnerability.get("aliases", [])),
                    "summary": vulnerability.get("summary"),
                    "modified": vulnerability.get("modified"),
                    "withdrawn": vulnerability.get("withdrawn"),
                    "severity": advisory_severity(vulnerability),
                }
            )
        rows.append({**package, "advisories": sorted(advisories, key=lambda item: item.get("id") or "")})
    matches = [advisory for row in rows for advisory in row["advisories"]]
    critical_high = [item for item in matches if item.get("severity") in {"CRITICAL", "HIGH"}]
    return {
        "schema_version": 1,
        "generator": GENERATOR,
        "source": "OSV querybatch (RustSec advisory export)",
        "endpoint": OSV_ENDPOINT,
        "rustsec_repository": RUSTSEC_REPOSITORY,
        "rustsec_revision": RUSTSEC_REVISION,
        "observed_at_utc": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "max_age_days": DEFAULT_MAX_AGE_DAYS,
        "platform": platform,
        "features": "all-features",
        "dependency_scope": "runtime-and-build",
        "summary": {
            "packages_queried": len(rows),
            "packages_with_advisories": sum(1 for row in rows if row["advisories"]),
            "advisory_matches": len(matches),
            "critical_or_high_matches": len(critical_high),
        },
        "packages": rows,
        "non_claims": [
            "not a guarantee against undisclosed or future vulnerabilities",
            "not a source-provenance or maintainer-trust guarantee",
            "the offline CI check does not query OSV or crates.io",
            "unreviewed low/moderate or informational advisories still require owner review before stable release",
        ],
    }


def canonical_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def comparable(report: dict[str, Any]) -> tuple[Any, ...]:
    return (
        report.get("schema_version"),
        report.get("generator"),
        report.get("source"),
        report.get("endpoint"),
        report.get("rustsec_repository"),
        report.get("rustsec_revision"),
        report.get("platform"),
        report.get("features"),
        report.get("dependency_scope"),
        tuple(
            (
                item.get("name"),
                item.get("version"),
                item.get("source_kind"),
            )
            for item in report.get("packages", [])
        ),
    )


def age_days(observed_at: str, now: dt.datetime | None = None) -> int:
    timestamp = dt.datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    current = now or dt.datetime.now(dt.UTC)
    return max(0, (current - timestamp).days)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--allow-resolved-version-drift",
        action="store_true",
        help="allow transitive registry version re-resolution when package identities match",
    )
    args = parser.parse_args()
    try:
        metadata = cargo_metadata(args.platform)
        if args.check:
            expected = json.loads(args.output.read_text(encoding="utf-8"))
            current_rows = inventory(metadata)
            current_inventory = {tuple(item.values()) for item in current_rows}
            expected_inventory = {
                (item.get("name"), item.get("version"), item.get("source_kind"))
                for item in expected.get("packages", [])
            }
            if args.allow_resolved_version_drift:
                current_identity = {(item["name"], item["source_kind"]) for item in current_rows}
                expected_identity = {
                    (item.get("name"), item.get("source_kind"))
                    for item in expected.get("packages", [])
                }
                inventory_changed = current_identity != expected_identity
            else:
                inventory_changed = current_inventory != expected_inventory
            if inventory_changed:
                return fail("resolved package inventory changed; refresh the OSV snapshot")
            summary = expected.get("summary", {})
            if summary.get("advisory_matches") != 0:
                return fail("the advisory snapshot contains matches requiring manual review")
            if summary.get("critical_or_high_matches") != 0:
                return fail("the advisory snapshot records a critical/high match")
            observed_at = expected.get("observed_at_utc")
            if not isinstance(observed_at, str):
                return fail("snapshot observed_at_utc is missing")
            max_age = int(expected.get("max_age_days", DEFAULT_MAX_AGE_DAYS))
            if age_days(observed_at) > max_age:
                return fail(f"snapshot is older than {max_age} days; refresh it")
            print(
                f"v1 advisory snapshot ok: {len(expected_inventory)} packages; "
                f"matches={summary.get('advisory_matches', 0)}; age={age_days(observed_at)} days"
            )
            return 0
        report = build_report(metadata, args.platform)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(canonical_json(report), encoding="utf-8")
        print(f"wrote {args.output}")
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError, subprocess.SubprocessError) as error:
        return fail(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
