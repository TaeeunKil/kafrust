"""Unit tests for the V1-19 native-tooling checker."""

from __future__ import annotations

import copy
import importlib.util
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_v1_native_tooling.py"
SPEC = importlib.util.spec_from_file_location("check_v1_native_tooling", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def report() -> dict:
    profiles = {
        name: {
            "package_count": 2,
            "packages": ["kafrust", "kafrust-protocol"],
            "native_tooling_candidates": [],
        }
        for name in MODULE.PROFILES
    }
    profiles["tls"]["native_tooling_candidates"] = [
        {"name": "ring", "version": "0.17.14", "links": ["ring_core"], "custom_build": True}
    ]
    profiles["all"]["native_tooling_candidates"] = copy.deepcopy(
        profiles["tls"]["native_tooling_candidates"]
    )
    return {
        "schema_version": 1,
        "platform": "x86_64-unknown-linux-gnu",
        "profiles": profiles,
        "default_no_c_build": {"status": "passed"},
        "non_claims": ["optional TLS may need native tooling"],
    }


class NativeToolingCheckerTests(unittest.TestCase):
    def test_valid_report_requires_tls_ring_and_default_no_c_pass(self) -> None:
        MODULE.validate_report(report())

    def test_default_ring_is_rejected(self) -> None:
        broken = report()
        broken["profiles"]["default"]["native_tooling_candidates"] = [
            {"name": "ring", "links": [], "custom_build": True}
        ]
        with self.assertRaisesRegex(RuntimeError, "default profile"):
            MODULE.validate_report(broken)

    def test_normalized_report_ignores_resolved_versions(self) -> None:
        first = report()
        second = copy.deepcopy(first)
        second["profiles"]["tls"]["native_tooling_candidates"][0]["version"] = "0.17.15"
        self.assertEqual(MODULE.normalized_report(first), MODULE.normalized_report(second))

    def test_tree_packages_parses_ansi_colored_cargo_output(self) -> None:
        output = "\x1b[1;32mkafrust v0.4.0\x1b[0m\n\x1b[1;32mring v0.17.14\x1b[0m\n"
        with patch.object(MODULE, "run", return_value=output):
            packages = MODULE.tree_packages(("--features", "tls"), MODULE.DEFAULT_PLATFORM)
        self.assertEqual(packages, {"kafrust": "0.4.0", "ring": "0.17.14"})

    def test_cargo_commands_disable_forced_terminal_color(self) -> None:
        completed = type("Completed", (), {"returncode": 0, "stdout": "", "stderr": ""})()
        with (
            patch.dict(os.environ, {"CARGO_TERM_COLOR": "always"}),
            patch.object(MODULE.subprocess, "run", return_value=completed) as run,
        ):
            MODULE.run(["cargo", "metadata"])
        environment = run.call_args.kwargs["env"]
        self.assertEqual(environment["CARGO_TERM_COLOR"], "never")
        self.assertEqual(environment["NO_COLOR"], "1")


if __name__ == "__main__":
    unittest.main()
