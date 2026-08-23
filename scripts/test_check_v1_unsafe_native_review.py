import importlib.util
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).with_name("check_v1_unsafe_native_review.py")
SBOM_SCRIPT = SCRIPT.with_name("check_v1_sbom.py")
SBOM_SPEC = importlib.util.spec_from_file_location("check_v1_sbom", SBOM_SCRIPT)
assert SBOM_SPEC and SBOM_SPEC.loader
SBOM_MODULE = importlib.util.module_from_spec(SBOM_SPEC)
sys.modules[SBOM_SPEC.name] = SBOM_MODULE
SBOM_SPEC.loader.exec_module(SBOM_MODULE)
SPEC = importlib.util.spec_from_file_location("check_v1_unsafe_native_review", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class UnsafeNativeReviewTests(unittest.TestCase):
    def test_classification_order(self):
        self.assertEqual(MODULE.classification({"native_boundary": True, "custom_build": True}), "native-platform-boundary")
        self.assertEqual(MODULE.classification({"native_boundary": False, "custom_build": True}), "build-script-or-codegen")
        self.assertEqual(MODULE.classification({"native_boundary": False, "custom_build": False}), "upstream-unsafe-implementation")

    def test_report_covers_every_entry(self):
        inventory = {
            "entries": [
                {
                    "name": "ring",
                    "version": "0.17.14",
                    "source_kind": "registry",
                    "unsafe_constructs": 1,
                    "custom_build": True,
                    "links": "ring_core",
                    "native_boundary": True,
                    "owner": "upstream crate maintainers",
                }
            ]
        }
        report = MODULE.build_report(inventory)
        self.assertEqual(report["summary"]["entries_reviewed"], 1)
        self.assertEqual(report["summary"]["native_platform_boundaries"], 1)
        self.assertEqual(report["entries"][0]["risk_disposition"].startswith("accepted"), True)


if __name__ == "__main__":
    unittest.main()
