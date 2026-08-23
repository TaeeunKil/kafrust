#!/usr/bin/env python3
"""Audit feature-specific native tooling and the default no-C build promise."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "v1-19-native-tooling.json"
DEFAULT_PLATFORM = "x86_64-unknown-linux-gnu"
PROFILES = {
    "default": (),
    "tls": ("--features", "tls"),
    "blocking": ("--features", "blocking"),
    "otlp": ("--features", "otlp"),
    "all": ("--all-features",),
}
NATIVE_CANDIDATES = {
    "bindgen",
    "cc",
    "clang-sys",
    "cmake",
    "libz-sys",
    "native-tls",
    "openssl",
    "openssl-sys",
    "pkg-config",
    "ring",
    "vcpkg",
    "zstd-sys",
}
PACKAGE_LINE = re.compile(r"^([A-Za-z0-9_-]+) v([0-9][^ ]*)")


def fail(message: str) -> int:
    print(f"v1 native-tooling check failed: {message}", file=sys.stderr)
    return 1


def run(command: list[str], *, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(
            f"{' '.join(command)} exited {result.returncode}:\n"
            f"{result.stdout}{result.stderr}"
        )
    return result.stdout


def metadata(platform: str) -> dict[str, Any]:
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--all-features",
            "--filter-platform",
            platform,
            "--format-version",
            "1",
        ],
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
            f"cargo metadata exited {result.returncode}:\n{result.stdout}{result.stderr}"
        )
    combined = f"{result.stdout}\n{result.stderr}"
    start = combined.find("{")
    end = combined.rfind("}")
    if start < 0 or end < start:
        raise RuntimeError("cargo metadata produced no JSON")
    try:
        value = json.loads(combined[start : end + 1])
    except json.JSONDecodeError as error:
        raise RuntimeError(f"cargo metadata produced invalid JSON: {error}") from error
    if not isinstance(value.get("packages"), list):
        raise RuntimeError("cargo metadata omitted packages")
    return value


def tree_packages(features: tuple[str, ...], platform: str) -> dict[str, str]:
    output = run(
        [
            "cargo",
            "tree",
            "-p",
            "kafrust",
            "--target",
            platform,
            "--edges",
            "normal",
            "--format",
            "{p}",
            *features,
        ]
    )
    packages: dict[str, str] = {}
    for line in output.splitlines():
        normalized = re.sub(r"^[^A-Za-z0-9]*", "", line)
        match = PACKAGE_LINE.match(normalized)
        if match:
            packages[match.group(1)] = match.group(2)
    return packages


def package_indicators(packages: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indicators: dict[str, dict[str, Any]] = {}
    for package in packages:
        name = package["name"]
        targets = package.get("targets", [])
        custom_build = any("custom-build" in target.get("kind", []) for target in targets)
        value = indicators.setdefault(
            name,
            {"links": set(), "custom_build": False, "versions": set()},
        )
        if package.get("links"):
            value["links"].add(package["links"])
        value["custom_build"] |= custom_build
        value["versions"].add(package["version"])
    return indicators


def profile_report(
    profile: str,
    features: tuple[str, ...],
    platform: str,
    indicators: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    packages = tree_packages(features, platform)
    unknown = sorted(set(packages) - set(indicators))
    if unknown:
        raise RuntimeError(f"{profile} tree packages missing from cargo metadata: {unknown}")
    native = []
    for name, version in sorted(packages.items()):
        if name not in NATIVE_CANDIDATES and not indicators[name]["links"]:
            continue
        native.append(
            {
                "name": name,
                "version": version,
                "links": sorted(indicators[name]["links"]),
                "custom_build": indicators[name]["custom_build"],
            }
        )
    return {
        "package_count": len(packages),
        "packages": sorted(packages),
        "native_tooling_candidates": native,
    }


def no_c_default_build() -> dict[str, Any]:
    environment = os.environ.copy()
    environment.update(
        {
            "CC": "kafrust-v1-no-c-compiler",
            "CXX": "kafrust-v1-no-cxx-compiler",
            "AR": "kafrust-v1-no-ar",
            "PKG_CONFIG": "kafrust-v1-no-pkg-config",
        }
    )
    command = ["cargo", "check", "-p", "kafrust", "--no-default-features", "--lib"]
    run(command, env=environment)
    return {
        "status": "passed",
        "command": "cargo check -p kafrust --no-default-features --lib",
        "compiler_environment": "CC/CXX/AR/PKG_CONFIG set to nonexistent tools",
    }


def build_report(platform: str) -> dict[str, Any]:
    data = metadata(platform)
    indicators = package_indicators(data["packages"])
    profiles = {
        name: profile_report(name, features, platform, indicators)
        for name, features in PROFILES.items()
    }
    return {
        "schema_version": 1,
        "platform": platform,
        "profiles": profiles,
        "default_no_c_build": no_c_default_build(),
        "non_claims": [
            "optional TLS uses ring and may require a native compiler/tooling",
            "this inventory does not prove transitive dependencies contain no unsafe code",
            "native-tool detection is not a security or supply-chain guarantee",
        ],
    }


def normalized_report(value: dict[str, Any]) -> dict[str, Any]:
    profiles = {}
    for name, profile in value.get("profiles", {}).items():
        profiles[name] = {
            "package_count": profile.get("package_count"),
            "packages": sorted(profile.get("packages", [])),
            "native_tooling_candidates": [
                {
                    "name": item.get("name"),
                    "links": sorted(item.get("links", [])),
                    "custom_build": bool(item.get("custom_build")),
                }
                for item in sorted(
                    profile.get("native_tooling_candidates", []),
                    key=lambda item: item.get("name", ""),
                )
            ],
        }
    return {
        "schema_version": value.get("schema_version"),
        "platform": value.get("platform"),
        "profiles": profiles,
        "non_claims": value.get("non_claims", []),
    }


def validate_report(value: dict[str, Any]) -> None:
    if value.get("schema_version") != 1:
        raise RuntimeError("native-tooling report schema_version must be 1")
    if set(value.get("profiles", {})) != set(PROFILES):
        raise RuntimeError("native-tooling report profiles are incomplete")
    no_c = value.get("default_no_c_build", {})
    if no_c.get("status") != "passed":
        raise RuntimeError("default no-C build evidence is not passed")
    all_candidates = [
        item
        for profile in value["profiles"].values()
        for item in profile.get("native_tooling_candidates", [])
    ]
    if not any(item.get("name") == "ring" for item in all_candidates):
        raise RuntimeError("TLS native-tooling inventory must record ring")
    if any(item.get("name") == "ring" for item in value["profiles"]["default"].get("native_tooling_candidates", [])):
        raise RuntimeError("default profile unexpectedly requires ring")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = build_report(args.platform)
        validate_report(report)
        output = args.output if args.output.is_absolute() else ROOT / args.output
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if args.write:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered, encoding="utf-8")
            print(f"wrote native-tooling report: {output}")
        else:
            if not output.is_file():
                raise RuntimeError(f"native-tooling report does not exist: {output}")
            existing = json.loads(output.read_text(encoding="utf-8"))
            validate_report(existing)
            if normalized_report(existing) != normalized_report(report):
                raise RuntimeError("native-tooling inventory drift detected")
            print(f"verified native-tooling inventory: {output}")
        for name, profile in report["profiles"].items():
            candidates = ", ".join(item["name"] for item in profile["native_tooling_candidates"]) or "none"
            print(f"  {name}: {profile['package_count']} packages; candidates={candidates}")
        print("  default no-C build: passed")
    except (OSError, RuntimeError, KeyError, TypeError, json.JSONDecodeError, subprocess.SubprocessError) as error:
        return fail(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
