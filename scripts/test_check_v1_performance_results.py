import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check_v1_performance_results.py")
SPEC = importlib.util.spec_from_file_location("check_v1_performance_results", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def manifest():
    return {
        "repetitions": 2,
        "profiles": [
            {
                "id": "profile",
                "payload_bytes": 1024,
                "batch_size": 2,
                "concurrency": 1,
                "compression": "none",
            }
        ],
        "topologies": ["single-node"],
        "security": ["PLAINTEXT"],
        "broker": "4.3.1",
        "timing": {"warmup_seconds": 2, "measured_seconds": 4, "sample_period_seconds": 2},
        "thresholds": {
            "retry_ratio_max_fraction": 0.01,
            "rss_growth_max_bytes": 64 * 1024 * 1024,
            "median_throughput_regression_fraction": 0.2,
            "p99_latency_regression_fraction": 0.25,
        },
    }


def write_bundle(root: Path, repetition: int, *, qualified: bool = True, p99: int = 4):
    digest = "a" * 64
    result_name = f"result-{repetition}.jsonl"
    rows = []
    for index in range(2):
        rows.append(
            {
                "mode": "campaign-sample",
                "profile": "profile",
                "sample_index": index,
                "sample_start_seconds": index * 2,
                "sample_end_seconds": (index + 1) * 2,
                "produce_records_per_second": 100.0,
                "consume_records_per_second": 100.0,
                "requests_started": 2,
                "requests_failed": 0,
                "retries": 0,
                "retry_ratio": 0.0,
                "request_p50_ms": 1.0,
                "request_p95_ms": 2.0,
                "request_p99_ms": 4.0,
                "rss_bytes": 100,
                "in_flight_requests": 0,
                "buffered_records": 0,
            }
        )
    rows.append(
        {
            "mode": "campaign-final",
            "profile": "profile",
            "warmup_seconds": 2,
            "measured_seconds": 4,
            "sample_seconds": 2,
            "produced_records": 400,
            "consumed_records": 400,
            "requests_started": 400,
            "retries": 0,
            "retry_ratio": 0.0,
            "latency_p50_p95_p99": {"p50_ms": 1, "p95_ms": 2, "p99_ms": p99},
            "rss_baseline_terminal_slope": {
                "baseline_bytes": 100,
                "terminal_bytes": 100,
                "growth_bytes": 0,
                "slope_bytes_per_second": 0,
                "sample_count": 2,
            },
            "loss_count": 0,
            "duplicate_count": 0,
            "in_flight_requests": 0,
            "buffered_records": 0,
        }
    )
    (root / result_name).write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    descriptor = {
        "schema_version": 1,
        "qualified": qualified,
        "campaign_id": "test",
        "profile_id": "profile",
        "repetition": repetition,
        "artifact": {"artifact_digest": digest},
        "runner": {
            "image": "ubuntu-test",
            "broker_image_digest": "sha256:test",
            "broker_version": "4.3.1",
            "topology": "single-node",
            "security": "PLAINTEXT",
        },
        "timing": {"warmup_seconds": 2, "measured_seconds": 4, "sample_seconds": 2},
        "workload": {"payload_bytes": 1024, "batch_size": 2, "workers": 1, "compression": "none"},
        "result_file": result_name,
    }
    (root / f"run-{repetition}-descriptor.json").write_text(json.dumps(descriptor), encoding="utf-8")


class PerformanceResultsTests(unittest.TestCase):
    def test_complete_bundle_and_baseline_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_bundle(root, 1)
            write_bundle(root, 2)
            baseline = {
                "schema_version": 1,
                "status": "locked",
                "profiles": {"profile|single-node|PLAINTEXT": {"throughput_records_per_second": 100, "p99_latency_ms": 4}},
            }
            summary = MODULE.validate_results(root, manifest(), baseline)
            self.assertEqual(summary["result_count"], 2)

    def test_missing_repetition_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_bundle(root, 1)
            with self.assertRaises(MODULE.ResultError):
                MODULE.validate_results(root, manifest())

    def test_unqualified_descriptor_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_bundle(root, 1, qualified=False)
            write_bundle(root, 2)
            with self.assertRaises(MODULE.ResultError):
                MODULE.validate_results(root, manifest())

    def test_latency_regression_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_bundle(root, 1, p99=6)
            write_bundle(root, 2, p99=6)
            baseline = {
                "schema_version": 1,
                "status": "locked",
                "profiles": {"profile|single-node|PLAINTEXT": {"throughput_records_per_second": 100, "p99_latency_ms": 4}},
            }
            with self.assertRaises(MODULE.ResultError):
                MODULE.validate_results(root, manifest(), baseline)


if __name__ == "__main__":
    unittest.main()
