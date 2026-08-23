"""Unit tests for the dependency-free V1-19 SBOM checker."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_v1_sbom.py"
SPEC = importlib.util.spec_from_file_location("check_v1_sbom", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

build_bom = MODULE.build_bom
validate_bom = MODULE.validate_bom


CLIENT = "path+file:///workspace/crates/kafrust#0.3.6"
PROTOCOL = "path+file:///workspace/crates/kafrust-protocol#0.3.6"
RUNTIME = "registry+https://github.com/rust-lang/crates.io-index#bytes@1.10.1"
DEV_ONLY = "registry+https://github.com/rust-lang/crates.io-index#test-helper@1.0.0"


def package(package_id: str, name: str, version: str, source: str | None) -> dict:
    return {
        "id": package_id,
        "name": name,
        "version": version,
        "license": "MIT OR Apache-2.0",
        "license_file": None,
        "source": source,
    }


def node(package_id: str, dependencies: list[tuple[str, str | None]]) -> dict:
    return {
        "id": package_id,
        "deps": [
            {
                "pkg": dependency_id,
                "dep_kinds": [{"kind": kind, "target": None}],
            }
            for dependency_id, kind in dependencies
        ],
    }


def fixture_metadata() -> dict:
    return {
        "packages": [
            package(CLIENT, "kafrust", "0.3.6", None),
            package(PROTOCOL, "kafrust-protocol", "0.3.6", None),
            package(RUNTIME, "bytes", "1.10.1", "registry+https://github.com/rust-lang/crates.io-index"),
            package(DEV_ONLY, "test-helper", "1.0.0", "registry+https://github.com/rust-lang/crates.io-index"),
        ],
        "workspace_members": [CLIENT, PROTOCOL],
        "resolve": {
            "nodes": [
                node(CLIENT, [(PROTOCOL, None), (DEV_ONLY, "dev")]),
                node(PROTOCOL, [(RUNTIME, None)]),
                node(RUNTIME, []),
                node(DEV_ONLY, []),
            ]
        },
    }


class SbomCheckerTests(unittest.TestCase):
    def test_runtime_graph_excludes_dev_only_edges(self) -> None:
        bom = build_bom(fixture_metadata(), "x86_64-unknown-linux-gnu")
        names = {component["name"] for component in bom["components"]}
        self.assertEqual(names, {"kafrust", "kafrust-protocol", "bytes"})
        self.assertNotIn("test-helper", names)
        validate_bom(bom)

    def test_validation_rejects_missing_component_reference(self) -> None:
        bom = build_bom(fixture_metadata(), "x86_64-unknown-linux-gnu")
        broken = copy.deepcopy(bom)
        broken["dependencies"][0]["dependsOn"].append("pkg:cargo/missing@1.0.0")
        with self.assertRaisesRegex(RuntimeError, "missing components"):
            validate_bom(broken)

    def test_validation_requires_generator_property(self) -> None:
        bom = build_bom(fixture_metadata(), "x86_64-unknown-linux-gnu")
        broken = copy.deepcopy(bom)
        broken["metadata"]["properties"] = []
        with self.assertRaisesRegex(RuntimeError, "generator property"):
            validate_bom(broken)


if __name__ == "__main__":
    unittest.main()
