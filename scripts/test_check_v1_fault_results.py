import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check_v1_fault_results.py")
SPEC = importlib.util.spec_from_file_location("check_v1_fault_results", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def manifest():
    return {
        "campaigns": [
            {
                "id": "member-loss-rejoin-cycles",
                "duration_seconds": 0,
                "minimum_cycles": 2,
                "faults": ["classic-member-loss"],
                "artifact_level": "Published artifact",
            }
        ]
    }


def write_segment(root: Path, *, index: int = 0, count: int = 1, qualified: bool = True, cycles: int = 2):
    descriptor = {
        "schema_version": 1,
        "campaign_id": "member-loss-rejoin-cycles",
        "segment_index": index,
        "segment_count": count,
        "artifact_digest": "a" * 64,
        "workflow_sha": "b" * 40,
        "broker_image_digest": "c" * 64,
        "segment_result": {"duration_seconds": 60, "recovered": True, "cycle_count": cycles},
        "record_id_reconciliation": {
            "qualified": qualified,
            "unique_records": 100,
            "loss_count": 0,
            "duplicate_count": 0,
            "digest": "d" * 64,
        },
        "final_resource_gauges": {"in_flight_requests": 0, "buffered_records": 0},
        "secret_scan_count": 0,
    }
    (root / f"segment-{index}-fault-segment.json").write_text(json.dumps(descriptor), encoding="utf-8")


class FaultResultsTests(unittest.TestCase):
    def test_qualified_segment_passes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_segment(root)
            summary = MODULE.validate_results(root, manifest())
            self.assertEqual(summary["campaign_count"], 1)

    def test_unqualified_reconciliation_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_segment(root, qualified=False)
            with self.assertRaises(MODULE.ResultError):
                MODULE.validate_results(root, manifest())

    def test_segment_gap_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_segment(root, index=0, count=2)
            write_segment(root, index=2, count=2)
            with self.assertRaises(MODULE.ResultError):
                MODULE.validate_results(root, manifest())


if __name__ == "__main__":
    unittest.main()
