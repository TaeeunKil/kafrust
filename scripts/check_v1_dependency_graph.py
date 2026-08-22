#!/usr/bin/env python3
"""Check the V1-19 direct dependency graph and forbidden client posture."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROFILES = (
    ("default", ()),
    ("tls", ("--features", "tls")),
    ("blocking", ("--features", "blocking")),
    ("otlp", ("--features", "otlp")),
    ("all", ("--all-features",)),
)
FORBIDDEN = re.compile(r"^(?:librdkafka|rdkafka-sys|kafka-sys|rdkafka)(?:-|$)")
PACKAGE_LINE = re.compile(r"^([A-Za-z0-9_-]+) v")


def fail(message: str) -> int:
    print(f"v1 dependency graph check failed: {message}", file=sys.stderr)
    return 1


def run_cargo(args: tuple[str, ...]) -> str:
    result = subprocess.run(
        ["cargo", *args],
        cwd=ROOT,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        details = result.stdout + result.stderr
        raise RuntimeError(f"cargo {' '.join(args)} exited {result.returncode}:\n{details}")
    return result.stdout


def run_cargo_json(args: tuple[str, ...]) -> dict:
    """Run Cargo and recover JSON even when diagnostics share the stream."""
    result = subprocess.run(
        ["cargo", *args],
        cwd=ROOT,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(
            f"cargo {' '.join(args)} exited {result.returncode}:\n"
            f"{result.stdout}{result.stderr}"
        )
    combined = f"{result.stdout}\n{result.stderr}"
    start = combined.find("{")
    end = combined.rfind("}")
    if start < 0 or end < start:
        raise RuntimeError(
            f"cargo {' '.join(args)} produced no JSON; output was:\n{combined[:500]}"
        )
    try:
        return json.loads(combined[start : end + 1])
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"cargo {' '.join(args)} produced invalid JSON: {error}"
        ) from error


def tree_packages(features: tuple[str, ...]) -> set[str]:
    output = run_cargo(
        (
            "tree",
            "-p",
            "kafrust",
            "--edges",
            "normal",
            "--format",
            "{p}",
            *features,
        )
    )
    packages: set[str] = set()
    for line in output.splitlines():
        normalized = re.sub(r"^[^A-Za-z0-9]*", "", line)
        match = PACKAGE_LINE.match(normalized)
        if match:
            packages.add(match.group(1))
    return packages


def main() -> int:
    try:
        root_manifest = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
        if 'unsafe_code = "forbid"' not in root_manifest:
            return fail('workspace lint must keep unsafe_code = "forbid"')
        summaries = []
        for profile, features in PROFILES:
            packages = tree_packages(features)
            forbidden = sorted(package for package in packages if FORBIDDEN.match(package))
            if forbidden:
                return fail(f"{profile} profile contains forbidden packages: {', '.join(forbidden)}")
            summaries.append((profile, len(packages)))

        metadata_args = ("metadata", "--format-version", "1")
        if (ROOT / "Cargo.lock").exists():
            metadata_args += ("--locked",)
        metadata = run_cargo_json(metadata_args)
        packages = metadata.get("packages", [])
        missing_license = sorted(
            package.get("name", "<unknown>")
            for package in packages
            if not package.get("license") and not package.get("license_file")
        )
        if missing_license:
            return fail(
                "resolved packages missing license metadata: "
                + ", ".join(missing_license)
            )
        selected = {package["name"]: package for package in metadata["packages"] if package["name"] in {"kafrust", "kafrust-protocol"}}
        if set(selected) != {"kafrust", "kafrust-protocol"}:
            return fail("metadata omitted kafrust or kafrust-protocol")
        versions = {package["version"] for package in selected.values()}
        if versions != {"0.3.6"}:
            return fail(f"unexpected coordinated versions: {sorted(versions)}")
        print("v1 dependency graph ok")
        for profile, count in summaries:
            print(f"  {profile}: {count} unique normal-edge packages; forbidden=none")
        for name in ("kafrust", "kafrust-protocol"):
            package = selected[name]
            print(f"  {name}: {len(package['dependencies'])} direct dependencies")
        print(f"  resolved packages with license metadata: {len(packages)}")
    except (OSError, RuntimeError, json.JSONDecodeError, KeyError, subprocess.SubprocessError) as error:
        return fail(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
