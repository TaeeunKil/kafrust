import copy
import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_v1_allocation_boundary_ledger.py"
SPEC = importlib.util.spec_from_file_location("check_v1_allocation_boundary_ledger", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class AllocationBoundaryLedgerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = json.loads(MODULE.MANIFEST.read_text(encoding="utf-8"))

    def test_committed_ledger_has_exact_boundary_set(self):
        self.assertEqual(MODULE.validate(self.manifest), 14)

    def test_missing_boundary_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["entries"] = manifest["entries"][:-1]
        with self.assertRaisesRegex(ValueError, "boundary set differs"):
            MODULE.validate(manifest)

    def test_duplicate_boundary_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["entries"].append(copy.deepcopy(manifest["entries"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate boundary_id"):
            MODULE.validate(manifest)

    def test_missing_source_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["entries"][0]["source_path"] = "missing.rs"
        with self.assertRaisesRegex(ValueError, "does not exist"):
            MODULE.validate(manifest)

    def test_unbounded_behavior_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["entries"][0]["allocation_behavior"] = "unbounded allocation"
        with self.assertRaisesRegex(ValueError, "bounded before/during"):
            MODULE.validate(manifest)


if __name__ == "__main__":
    unittest.main()
