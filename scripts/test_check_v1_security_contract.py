import copy
import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_v1_security_contract.py"
SPEC = importlib.util.spec_from_file_location("check_v1_security_contract", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SecurityContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = json.loads(MODULE.MANIFEST.read_text(encoding="utf-8"))
        cls.source = MODULE.CONFIG_SOURCE.read_text(encoding="utf-8")

    def test_committed_contract_matches_source_enums(self):
        self.assertEqual(MODULE.validate(self.manifest, self.source), 7)

    def test_missing_contract_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["entries"] = manifest["entries"][:-1]
        with self.assertRaisesRegex(ValueError, "contract set differs"):
            MODULE.validate(manifest, self.source)

    def test_protocol_drift_is_rejected(self):
        source = self.source.replace("    SaslTls,", "    SaslTls,\n    FutureProtocol,")
        with self.assertRaisesRegex(ValueError, "SecurityProtocol variants changed"):
            MODULE.validate(self.manifest, source)

    def test_redaction_policy_is_required(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["entries"][0]["redaction"] = "diagnostic policy"
        with self.assertRaisesRegex(ValueError, "redaction policy"):
            MODULE.validate(manifest, self.source)

    def test_unknown_source_path_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["entries"][0]["source_paths"] = ["missing.rs"]
        with self.assertRaisesRegex(ValueError, "source path is invalid"):
            MODULE.validate(manifest, self.source)


if __name__ == "__main__":
    unittest.main()
