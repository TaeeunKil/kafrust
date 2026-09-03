import copy
import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_v1_metrics_contract.py"
SPEC = importlib.util.spec_from_file_location("check_v1_metrics_contract", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class MetricsContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = json.loads(MODULE.MANIFEST.read_text(encoding="utf-8"))
        cls.fields = MODULE.snapshot_fields(MODULE.SOURCE.read_text(encoding="utf-8"))

    def test_committed_contract_matches_snapshot(self):
        self.assertEqual(MODULE.validate(self.manifest, self.fields), 19)

    def test_missing_metric_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["metrics"] = manifest["metrics"][:-1]
        with self.assertRaisesRegex(ValueError, "field set differs"):
            MODULE.validate(manifest, self.fields)

    def test_cardinality_is_bounded(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["metrics"][0]["max_cardinality"] = 2
        with self.assertRaisesRegex(ValueError, "max_cardinality=1"):
            MODULE.validate(manifest, self.fields)

    def test_source_type_drift_is_rejected(self):
        fields = dict(self.fields)
        fields["requests_started"] = "usize"
        with self.assertRaisesRegex(ValueError, "type changed"):
            MODULE.validate(self.manifest, fields)

    def test_duplicate_metric_is_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["metrics"].append(copy.deepcopy(manifest["metrics"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate metric name"):
            MODULE.validate(manifest, self.fields)


if __name__ == "__main__":
    unittest.main()
