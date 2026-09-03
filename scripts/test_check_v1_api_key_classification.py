import copy
import importlib.util
import json
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("check_v1_api_key_classification.py")
SPEC = importlib.util.spec_from_file_location("check_v1_api_key_classification", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ApiKeyClassificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = json.loads(MODULE.MANIFEST.read_text(encoding="utf-8"))
        cls.actual_keys = MODULE.implemented_keys()

    def test_checked_in_manifest_covers_every_key(self):
        rows, internal, excluded = MODULE.validate(self.manifest, self.actual_keys)
        self.assertEqual(rows, 93)
        self.assertEqual(internal, 16)
        self.assertEqual(excluded, 1)

    def test_missing_key_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["entries"] = manifest["entries"][:-1]
        with self.assertRaisesRegex(ValueError, "key set differs"):
            MODULE.validate(manifest, self.actual_keys)

    def test_implemented_key_cannot_be_excluded(self):
        manifest = copy.deepcopy(self.manifest)
        next(entry for entry in manifest["entries"] if entry["api_key"] == 0)["classification"] = "excluded"
        with self.assertRaisesRegex(ValueError, "implemented keys"):
            MODULE.validate(manifest, self.actual_keys)

    def test_update_raft_voter_is_explicitly_excluded(self):
        manifest = copy.deepcopy(self.manifest)
        next(entry for entry in manifest["entries"] if entry["api_key"] == 82)["classification"] = "expert"
        with self.assertRaisesRegex(ValueError, "key 82"):
            MODULE.validate(manifest, self.actual_keys)


if __name__ == "__main__":
    unittest.main()
