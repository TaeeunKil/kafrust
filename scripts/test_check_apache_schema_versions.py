"""Unit tests for the dependency-free Apache schema audit."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_apache_schema_versions.py"
SPEC = importlib.util.spec_from_file_location("check_apache_schema_versions", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ApacheSchemaAuditTests(unittest.TestCase):
    def test_parse_online_range_supports_closed_and_open_ranges(self) -> None:
        self.assertEqual(MODULE.parse_online_range("3-13"), (3, 13))
        self.assertEqual(MODULE.parse_online_range("0+"), (0, None))
        self.assertEqual(MODULE.parse_online_range("0"), (0, 0))

    def test_discovers_top_level_request_schemas(self) -> None:
        discovered = []
        for source in sorted(MODULE.API_DIR.glob("*.rs")):
            if source.name == "mod.rs":
                continue
            discovered.extend(MODULE.LOCAL_REQUEST_RE.findall(source.read_text(encoding="utf-8")))

        self.assertEqual(len(set(discovered)), 76)

    def test_online_all_checks_every_discovered_request_and_response(self) -> None:
        metadata: dict[str, tuple[int, str]] = {}
        for source in sorted(MODULE.API_DIR.glob("*.rs")):
            if source.name == "mod.rs":
                continue
            source_text = source.read_text(encoding="utf-8")
            local_keys = {int(value) for value in MODULE.API_KEY_RE.findall(source_text)}
            for request_name in set(MODULE.LOCAL_REQUEST_RE.findall(source_text)):
                response_name = request_name.removesuffix("Request") + "Response"
                metadata[request_name] = (next(iter(local_keys)), "request")
                metadata[response_name] = (next(iter(local_keys)), "response")

        def fake_fetch(_snapshot: dict[str, object], name: str) -> dict[str, object]:
            api_key, message_type = metadata[name]
            return {
                "name": name,
                "apiKey": api_key,
                "type": message_type,
                "validVersions": "0-100",
                "flexibleVersions": "50",
            }

        with (
            patch.object(MODULE, "fetch_schema", side_effect=fake_fetch),
            patch("builtins.print"),
        ):
            self.assertEqual(MODULE.online_all({}), 0)


if __name__ == "__main__":
    unittest.main()
