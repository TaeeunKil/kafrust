import copy
import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_v1_ownership_inventory.py"
SPEC = importlib.util.spec_from_file_location("check_v1_ownership_inventory", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class OwnershipInventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = json.loads(MODULE.MANIFEST.read_text(encoding="utf-8"))

    def test_committed_inventory_has_exact_owner_set(self):
        self.assertEqual(MODULE.validate(self.manifest), 10)

    def test_missing_owner_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["owners"] = manifest["owners"][:-1]
        with self.assertRaisesRegex(ValueError, "owner set differs"):
            MODULE.validate(manifest)

    def test_duplicate_owner_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["owners"].append(copy.deepcopy(manifest["owners"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate owner_id"):
            MODULE.validate(manifest)

    def test_unbounded_capacity_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["owners"][0]["capacity"]["kind"] = "unbounded"
        with self.assertRaisesRegex(ValueError, "kind=finite"):
            MODULE.validate(manifest)

    def test_empty_lifecycle_evidence_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["owners"][0]["verification"] = []
        with self.assertRaisesRegex(ValueError, "empty verification"):
            MODULE.validate(manifest)


if __name__ == "__main__":
    unittest.main()
