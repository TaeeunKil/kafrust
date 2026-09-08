import json
import tempfile
import unittest
from pathlib import Path

from scripts.run_bounded_campaign import (
    phase_command,
    phase_env,
    write_campaign_state,
)
from scripts.validate_bounded_campaign import DEFAULT_CONFIG, load_config


ROOT = Path(__file__).resolve().parents[1]


class BoundedCampaignRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config(DEFAULT_CONFIG)

    def test_group_command_maps_kip848_and_leave(self):
        phase = self.config["phases"][2]
        command = phase_command(
            root=ROOT,
            phase=phase,
            run_id="test-run",
            output_dir=Path("/tmp/test-run/kip848"),
            broker=self.config["broker"],
        )
        self.assertIn("group", command)
        self.assertIn("--protocol", command)
        self.assertIn("kip-848", command)
        self.assertIn("--member-exit", command)
        self.assertIn("leave", command)
        self.assertIn("4.3.1", command)

    def test_plaintext_environment_is_explicitly_plaintext(self):
        phase = self.config["phases"][-1]
        env = phase_env(
            phase=phase,
            run_id="test-run",
            output_dir=Path("/tmp/test-run/plaintext"),
            config=self.config,
        )
        self.assertEqual(env["KAFRUST_LOCAL_DURATION_SECONDS"], "43200")
        self.assertEqual(env["KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND"], "25")
        self.assertEqual(env["KAFRUST_LOCAL_PAYLOAD_BYTES"], "64")
        self.assertNotIn("KAFRUST_SASL_PASSWORD", env)

    def test_campaign_state_is_replaced_atomically(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "campaign-state.json"
            write_campaign_state(
                state,
                profile_id="windows-wsl-32g-low-rate",
                run_id="test-run",
                status="running",
                current_phase="secure-soak-6h",
                completed_phases=["classic-leave-group-churn"],
            )
            value = json.loads(state.read_text(encoding="utf-8"))
            self.assertEqual(value["status"], "running")
            self.assertEqual(value["current_phase"], "secure-soak-6h")
            self.assertFalse((Path(folder) / "campaign-state.tmp").exists())


if __name__ == "__main__":
    unittest.main()
