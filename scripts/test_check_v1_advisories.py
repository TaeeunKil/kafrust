import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).with_name("check_v1_advisories.py")
SBOM_SCRIPT = SCRIPT.with_name("check_v1_sbom.py")
SBOM_SPEC = importlib.util.spec_from_file_location("check_v1_sbom", SBOM_SCRIPT)
assert SBOM_SPEC and SBOM_SPEC.loader
SBOM_MODULE = importlib.util.module_from_spec(SBOM_SPEC)
sys.modules[SBOM_SPEC.name] = SBOM_MODULE
SBOM_SPEC.loader.exec_module(SBOM_MODULE)
SPEC = importlib.util.spec_from_file_location("check_v1_advisories", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class AdvisoryTests(unittest.TestCase):
    def test_severity_prefers_critical_then_high(self):
        self.assertEqual(MODULE.advisory_severity({"severity": [{"score": "HIGH"}]}), "HIGH")
        self.assertEqual(
            MODULE.advisory_severity({"severity": [{"score": "LOW"}], "database_specific": {"severity": "CRITICAL"}}),
            "CRITICAL",
        )
        self.assertIsNone(MODULE.advisory_severity({}))

    def test_age_days_is_deterministic(self):
        observed = "2026-08-20T00:00:00Z"
        now = datetime(2026, 8, 23, tzinfo=timezone.utc)
        self.assertEqual(MODULE.age_days(observed, now), 3)

    def test_comparable_ignores_timestamp_and_advisory_details(self):
        base = {
            "schema_version": 1,
            "generator": MODULE.GENERATOR,
            "source": "OSV querybatch (RustSec advisory export)",
            "endpoint": MODULE.OSV_ENDPOINT,
            "rustsec_repository": MODULE.RUSTSEC_REPOSITORY,
            "rustsec_revision": MODULE.RUSTSEC_REVISION,
            "platform": "x86_64-unknown-linux-gnu",
            "features": "all-features",
            "dependency_scope": "runtime-and-build",
            "packages": [{"name": "bytes", "version": "1.0.0", "source_kind": "registry", "advisories": []}],
        }
        changed = {**base, "observed_at_utc": "later", "packages": [{**base["packages"][0], "advisories": [{"id": "RUSTSEC-0000-0000"}]}]}
        self.assertEqual(MODULE.comparable(base), MODULE.comparable(changed))


if __name__ == "__main__":
    unittest.main()
