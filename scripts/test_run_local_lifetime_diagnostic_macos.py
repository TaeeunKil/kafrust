import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("run_local_lifetime_diagnostic_macos.py")
SPEC = importlib.util.spec_from_file_location("run_local_lifetime_diagnostic_macos", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class MacLifetimeDiagnosticConfigTests(unittest.TestCase):
    def test_load_config_requires_external_capacity_path(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(MODULE.platform, "system", return_value="Darwin"), \
                    patch.object(MODULE.platform, "machine", return_value="arm64"), \
                    patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(MODULE.MacDiagnosticError, "CAPACITY_PATH"):
                    MODULE.load_config(Path(folder))

    def test_load_config_uses_low_memory_mac_profile(self):
        with tempfile.TemporaryDirectory() as folder:
            volume = (Path(folder) / "external").resolve()
            volume.mkdir()
            output = volume / "run"
            target = volume / "cargo-target"
            environment = {
                "KAFRUST_LOCAL_CAPACITY_PATH": str(volume),
                "KAFRUST_LOCAL_DOCKER_DATA_PATH": str(volume),
                "KAFRUST_LOCAL_OUTPUT_DIR": str(output),
                "KAFRUST_LOCAL_CARGO_TARGET_DIR": str(target),
                "KAFRUST_LOCAL_RUN_ID": "unit-test",
                "KAFRUST_LOCAL_DURATION_SECONDS": "60",
                "KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND": "1",
                "KAFRUST_LOCAL_PAYLOAD_BYTES": "64",
            }
            with patch.object(MODULE.platform, "system", return_value="Darwin"), \
                    patch.object(MODULE.platform, "machine", return_value="arm64"), \
                    patch.dict(os.environ, environment, clear=True):
                config = MODULE.load_config(Path(folder))
            self.assertEqual(config.broker_memory, "1g")
            self.assertEqual(config.duration_seconds, 60)
            self.assertTrue(config.output_dir.is_relative_to(volume))
            self.assertTrue(config.cargo_target_dir.is_relative_to(volume))

    def test_load_config_rejects_full_rate_day_budget(self):
        with tempfile.TemporaryDirectory() as folder:
            volume = Path(folder) / "external"
            volume.mkdir()
            environment = {
                "KAFRUST_LOCAL_CAPACITY_PATH": str(volume),
                "KAFRUST_LOCAL_DOCKER_DATA_PATH": str(volume),
                "KAFRUST_LOCAL_RUN_ID": "unit-test",
                "KAFRUST_LOCAL_DURATION_SECONDS": "86400",
                "KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND": "100",
            }
            with patch.object(MODULE.platform, "system", return_value="Darwin"), \
                    patch.object(MODULE.platform, "machine", return_value="arm64"), \
                    patch.dict(os.environ, environment, clear=True):
                with self.assertRaisesRegex(MODULE.MacDiagnosticError, "storage exceeds"):
                    MODULE.load_config(Path(folder))


if __name__ == "__main__":
    unittest.main()
