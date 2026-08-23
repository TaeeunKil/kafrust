#!/usr/bin/env python3
"""Inventory transitive unsafe/native boundaries for the V1-19 review.

The inventory is intentionally conservative: it scans the resolved runtime /
build closure's unpacked Rust sources for unsafe constructs and records custom
build/link metadata.  It supplies a review queue with an owner and rationale;
it does not certify third-party code as safe and does not replace an advisory
scanner or a human source review.
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


DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "v1-19-unsafe-native-inventory.json"
GENERATOR = "scripts/check_v1_unsafe_native_inventory.py"
ROOT_NAMES = {"kafrust", "kafrust-protocol"}
UNSAFE_CONSTRUCT = re.compile(r"\bunsafe\s+(?:fn|impl|trait|extern|\{)")
NATIVE_BOUNDARY_NAMES = {
    "getrandom",
    "libc",
    "mio",
    "ring",
    "rustls-platform-verifier",
    "socket2",
}


def fail(message: str) -> int:
    print(f"v1 unsafe/native inventory failed: {message}", file=sys.stderr)
    return 1


def source_unsafe_count(package: dict[str, Any]) -> int:
    package_dir = Path(package["manifest_path"]).parent
    roots = [package_dir / "src"]
    build_script = package_dir / "build.rs"
    if build_script.exists():
        roots.append(build_script)
    count = 0
    for root in roots:
        files = [root] if root.is_file() else root.rglob("*.rs") if root.exists() else []
        for path in files:
            try:
                count += len(UNSAFE_CONSTRUCT.findall(path.read_text(encoding="utf-8", errors="ignore")))
            except OSError as error:
                raise RuntimeError(f"unable to scan {path}: {error}") from error
    return count


def owner_and_rationale(package: dict[str, Any], custom_build: bool, native_boundary: bool) -> tuple[str, str]:
    name = package["name"]
    if name in ROOT_NAMES:
        return (
            "kafrust maintainers",
            "workspace protocol/client code; unsafe_code=forbid is a release invariant",
        )
    if name in {"ring", "rustls", "rustls-platform-verifier", "rustls-webpki", "untrusted", "zeroize"}:
        return (
            "upstream TLS/cryptography maintainers",
            "cryptographic or certificate verification boundary; optional TLS ownership is explicit",
        )
    if name in {"libc", "mio", "socket2", "getrandom", "tokio"}:
        return (
            "upstream OS/runtime maintainers",
            "platform, socket, entropy, or async-runtime boundary; target-specific unsafe code is upstream-owned",
        )
    if custom_build:
        return (
            "upstream build-tool maintainers",
            "custom build script or generated code; build-time behavior must stay feature/target scoped",
        )
    if native_boundary:
        return (
            "upstream platform-integration maintainers",
            "platform-facing dependency without a default C-client requirement",
        )
    return (
        "upstream crate maintainers",
        "transitive implementation unsafe boundary; version updates require re-running this inventory",
    )


def build_report(metadata: dict[str, Any], platform: str) -> dict[str, Any]:
    packages, _nodes, reachable = runtime_closure(metadata)
    entries: list[dict[str, Any]] = []
    for package_id in sorted(reachable):
        package = packages[package_id]
        custom_build = any("custom-build" in target.get("kind", []) for target in package.get("targets", []))
        unsafe_count = source_unsafe_count(package)
        native_boundary = bool(package.get("links")) or package["name"] in NATIVE_BOUNDARY_NAMES
        if not (unsafe_count or custom_build or native_boundary):
            continue
        owner, rationale = owner_and_rationale(package, custom_build, native_boundary)
        entries.append(
            {
                "name": package["name"],
                "version": package["version"],
                "source_kind": source_kind(package),
                "unsafe_constructs": unsafe_count,
                "custom_build": custom_build,
                "links": package.get("links"),
                "native_boundary": native_boundary,
                "owner": owner,
                "rationale": rationale,
                "review_status": "inventory-only; manual source and advisory review required",
            }
        )
    workspace = [entry for entry in entries if entry["name"] in ROOT_NAMES]
    if any(entry["unsafe_constructs"] for entry in workspace):
        raise RuntimeError("workspace package source contains an unsafe construct")
    return {
        "schema_version": 1,
        "generator": GENERATOR,
        "platform": platform,
        "features": "all-features",
        "dependency_scope": "runtime-and-build",
        "scan": {
            "unsafe_pattern": UNSAFE_CONSTRUCT.pattern,
            "roots": ["package src/ and build.rs when present"],
            "native_boundary_names": sorted(NATIVE_BOUNDARY_NAMES),
        },
        "summary": {
            "resolved_packages": len(reachable),
            "review_entries": len(entries),
            "unsafe_or_build_entries": sum(
                1 for entry in entries if entry["unsafe_constructs"] or entry["custom_build"]
            ),
            "native_boundary_entries": sum(1 for entry in entries if entry["native_boundary"]),
            "workspace_unsafe_constructs": sum(entry["unsafe_constructs"] for entry in workspace),
        },
        "entries": entries,
        "non_claims": [
            "not a source-level correctness or memory-safety certification",
            "not a proof that optional TLS or platform dependencies need no native tooling",
            "not an advisory, vulnerability, yank, or provenance review",
            "manual owner/rationale review is still required before V1-19 completion",
        ],
    }


def comparable(report: dict[str, Any]) -> tuple[Any, ...]:
    entries = tuple(
        sorted(
            (
                item.get("name"),
                item.get("source_kind"),
                item.get("custom_build"),
                item.get("links"),
                item.get("native_boundary"),
                item.get("owner"),
                item.get("rationale"),
            )
            for item in report.get("entries", [])
        )
    )
    scan = report.get("scan", {})
    return (
        report.get("schema_version"),
        report.get("generator"),
        report.get("platform"),
        report.get("features"),
        report.get("dependency_scope"),
        scan.get("unsafe_pattern"),
        tuple(scan.get("native_boundary_names", [])),
        entries,
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
                return fail("unsafe/native inventory drifted; review the report before updating evidence")
            print(
                f"v1 unsafe/native inventory ok: {report['summary']['review_entries']} review entries; "
                f"workspace_unsafe={report['summary']['workspace_unsafe_constructs']}"
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
