#!/usr/bin/env python3
"""Generate and verify a deterministic CycloneDX SBOM for the V1-19 graph.

The SBOM is intentionally generated from Cargo's resolved metadata rather than
from a hand-maintained dependency list.  Only normal and build dependencies
reachable from the two workspace package roots are included; dev-only edges
are excluded because they are not shipped runtime dependencies.  A platform
is required so target-specific resolution is explicit and reproducible.

This tool does not replace an advisory scanner or a native-toolchain audit. It
is the package dependency inventory and drift gate used by V1-19.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections import deque
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "docs" / "evidence" / "v1-19-sbom.json"
DEFAULT_PLATFORM = "x86_64-unknown-linux-gnu"
ROOT_NAMES = {"kafrust", "kafrust-protocol"}
GENERATOR = "scripts/check_v1_sbom.py"


def fail(message: str) -> int:
    print(f"v1 SBOM check failed: {message}", file=sys.stderr)
    return 1


def cargo_metadata(platform: str) -> dict[str, Any]:
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--all-features",
        "--filter-platform",
        platform,
        "--format-version",
        "1",
    ]
    result = subprocess.run(
        command,
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
            f"{' '.join(command)} exited {result.returncode}:\n"
            f"{result.stdout}{result.stderr}"
        )
    combined = f"{result.stdout}\n{result.stderr}"
    start = combined.find("{")
    end = combined.rfind("}")
    if start < 0 or end < start:
        raise RuntimeError("cargo metadata produced no JSON")
    try:
        metadata = json.loads(combined[start : end + 1])
    except json.JSONDecodeError as error:
        raise RuntimeError(f"cargo metadata produced invalid JSON: {error}") from error
    if not isinstance(metadata.get("resolve"), dict):
        raise RuntimeError("cargo metadata did not return a dependency resolve graph")
    return metadata


def runtime_dependency_ids(node: dict[str, Any]) -> Iterable[str]:
    """Yield normal/build dependency package IDs, excluding dev-only edges."""

    for dependency in node.get("deps", []):
        kinds = dependency.get("dep_kinds", [])
        if not kinds or any(kind.get("kind") != "dev" for kind in kinds):
            yield dependency["pkg"]


def purl(package: dict[str, Any]) -> str:
    return f"pkg:cargo/{package['name']}@{package['version']}"


def licenses_for(package: dict[str, Any]) -> list[dict[str, Any]]:
    expression = package.get("license")
    if expression:
        return [{"expression": expression}]
    license_file = package.get("license_file")
    if license_file:
        return [{"license": {"name": license_file}}]
    raise RuntimeError(
        f"resolved package {package.get('name', '<unknown>')} has no license metadata"
    )


def source_kind(package: dict[str, Any]) -> str:
    source = package.get("source")
    if source is None:
        return "workspace" if package["name"] in ROOT_NAMES else "path"
    if source.startswith("registry+"):
        return "registry"
    return "other"


def component(package: dict[str, Any]) -> dict[str, Any]:
    reference = purl(package)
    return {
        "type": "library",
        "bom-ref": reference,
        "name": package["name"],
        "version": package["version"],
        "purl": reference,
        "scope": "required",
        "licenses": licenses_for(package),
        "properties": [
            {"name": "kafrust:source-kind", "value": source_kind(package)},
            {"name": "kafrust:package-id", "value": package["id"]},
        ],
    }


def runtime_closure(metadata: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], set[str]]:
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    root_ids = {
        package_id
        for package_id in metadata.get("workspace_members", [])
        if package_id in packages
    }
    root_names = {packages[package_id]["name"] for package_id in root_ids}
    if root_names != ROOT_NAMES:
        raise RuntimeError(
            f"workspace roots must be {sorted(ROOT_NAMES)}, found {sorted(root_names)}"
        )

    reachable: set[str] = set(root_ids)
    queue = deque(root_ids)
    while queue:
        package_id = queue.popleft()
        node = nodes.get(package_id)
        if node is None:
            raise RuntimeError(f"resolve graph omitted package node {package_id}")
        for dependency_id in runtime_dependency_ids(node):
            if dependency_id not in packages or dependency_id not in nodes:
                raise RuntimeError(f"resolve graph omitted dependency node {dependency_id}")
            if dependency_id not in reachable:
                reachable.add(dependency_id)
                queue.append(dependency_id)
    return packages, nodes, reachable


def build_bom(metadata: dict[str, Any], platform: str) -> dict[str, Any]:
    packages, nodes, reachable = runtime_closure(metadata)
    by_ref: dict[str, str] = {}
    components: list[dict[str, Any]] = []
    for package_id in sorted(reachable):
        package_component = component(packages[package_id])
        reference = package_component["bom-ref"]
        if reference in by_ref:
            raise RuntimeError(
                f"multiple resolved packages share CycloneDX reference {reference}"
            )
        by_ref[reference] = package_id
        components.append(package_component)
    components.sort(key=lambda item: item["bom-ref"])

    dependencies: list[dict[str, Any]] = []
    for package_id in sorted(reachable):
        reference = purl(packages[package_id])
        dependency_refs = sorted(
            {
                purl(packages[dependency_id])
                for dependency_id in runtime_dependency_ids(nodes[package_id])
                if dependency_id in reachable
            }
        )
        dependencies.append({"ref": reference, "dependsOn": dependency_refs})
    dependencies.sort(key=lambda item: item["ref"])

    root = next(package for package in packages.values() if package["name"] == "kafrust")
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {
            "component": component(root),
            "properties": [
                {"name": "kafrust:sbom:generator", "value": GENERATOR},
                {"name": "kafrust:sbom:platform", "value": platform},
                {"name": "kafrust:sbom:features", "value": "all-features"},
                {"name": "kafrust:sbom:dependency-scope", "value": "runtime-and-build"},
                {
                    "name": "kafrust:sbom:metadata-command",
                    "value": "cargo metadata --locked --all-features --filter-platform "
                    + platform,
                },
            ],
        },
        "components": components,
        "dependencies": dependencies,
    }


def validate_bom(bom: dict[str, Any]) -> None:
    if bom.get("bomFormat") != "CycloneDX" or bom.get("specVersion") != "1.5":
        raise RuntimeError("SBOM must be CycloneDX 1.5")
    components = bom.get("components")
    dependencies = bom.get("dependencies")
    if not isinstance(components, list) or not components:
        raise RuntimeError("SBOM must contain components")
    if not isinstance(dependencies, list) or not dependencies:
        raise RuntimeError("SBOM must contain dependency entries")
    references = {item.get("bom-ref") for item in components}
    if None in references or len(references) != len(components):
        raise RuntimeError("SBOM component references must be unique")
    for item in components:
        if not item.get("licenses"):
            raise RuntimeError(f"component {item.get('name')} has no license entry")
    dependency_refs = {item.get("ref") for item in dependencies}
    if dependency_refs != references:
        raise RuntimeError("SBOM dependency entries do not match component references")
    for item in dependencies:
        missing = sorted(set(item.get("dependsOn", [])) - references)
        if missing:
            raise RuntimeError(f"SBOM dependency entry references missing components: {missing}")
    metadata_properties = {
        item.get("name"): item.get("value")
        for item in bom.get("metadata", {}).get("properties", [])
    }
    if metadata_properties.get("kafrust:sbom:generator") != GENERATOR:
        raise RuntimeError("SBOM generator property is missing or stale")


def canonical_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_artifacts(artifact_dir: Path, versions: dict[str, str], required: bool) -> None:
    for name, version in sorted(versions.items()):
        archive = artifact_dir / f"{name}-{version}.crate"
        if not archive.is_file():
            if required:
                raise RuntimeError(f"missing package artifact {archive}")
            print(f"artifact {archive.name}: not present (not required)")
            continue
        print(f"artifact {archive.name}: sha256={sha256(archive)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="write the generated SBOM")
    mode.add_argument("--check", action="store_true", help="compare the generated SBOM")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--artifact-dir", type=Path, default=ROOT / "target" / "package")
    parser.add_argument("--require-artifacts", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        metadata = cargo_metadata(args.platform)
        bom = build_bom(metadata, args.platform)
        validate_bom(bom)
        versions = {
            package["name"]: package["version"]
            for package in metadata["packages"]
            if package["name"] in ROOT_NAMES and package.get("source") is None
        }
        if set(versions) != ROOT_NAMES:
            raise RuntimeError("metadata did not contain both workspace package versions")
        artifact_dir = args.artifact_dir
        if not artifact_dir.is_absolute():
            artifact_dir = ROOT / artifact_dir
        verify_artifacts(artifact_dir, versions, args.require_artifacts)

        output = args.output if args.output.is_absolute() else ROOT / args.output
        rendered = canonical_json(bom)
        if args.write:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered, encoding="utf-8")
            print(f"wrote deterministic CycloneDX SBOM: {output}")
        else:
            if not output.is_file():
                raise RuntimeError(f"SBOM file does not exist: {output}")
            existing = output.read_text(encoding="utf-8")
            if existing != rendered:
                raise RuntimeError(
                    f"SBOM drift detected in {output}; rerun with --write after review"
                )
            print(f"verified deterministic CycloneDX SBOM: {output}")
        print(f"  components: {len(bom['components'])}")
        print(f"  runtime/build dependency entries: {len(bom['dependencies'])}")
        print(f"  platform: {args.platform}")
    except (OSError, RuntimeError, KeyError, TypeError, subprocess.SubprocessError) as error:
        return fail(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
