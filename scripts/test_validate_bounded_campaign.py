import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts.validate_bounded_campaign import (
    DEFAULT_CONFIG,
    CampaignConfigError,
    load_config,
    plan,
    readiness_blockers,
    validate_profile,
)


ROOT = Path(__file__).resolve().parents[1]


class BoundedCampaignProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config(DEFAULT_CONFIG)

    def test_profile_is_exact_and_secret_free(self):
        validate_profile(self.config, ROOT)
        self.assertEqual(
            [phase["id"] for phase in self.config["phases"]],
            [
                "classic-leave-group-churn",
                "classic-drop-group-churn",
                "kip848-leave-group-churn",
                "kip848-drop-group-churn",
                "secure-soak-6h",
                "plaintext-soak-12h",
            ],
        )
        serialized = json.dumps(self.config).lower()
        self.assertNotIn("kafrust-secret", serialized)
        self.assertNotIn("broker-secret", serialized)

    def test_each_long_phase_fits_the_declared_budget(self):
        result = plan(self.config, ROOT)
        by_id = {phase["id"]: phase for phase in result["phases"]}
        self.assertLessEqual(by_id["secure-soak-6h"]["estimated_storage_gib"], 20)
        self.assertLessEqual(by_id["plaintext-soak-12h"]["estimated_storage_gib"], 20)

    def test_secure_helper_is_ready_only_when_rate_contract_is_present(self):
        blockers = readiness_blockers(self.config, ROOT)
        self.assertFalse(blockers)
        self.assertTrue(plan(self.config, ROOT)["ready"])

    def test_wrong_order_is_rejected(self):
        config = copy.deepcopy(self.config)
        config["phases"][0], config["phases"][1] = config["phases"][1], config["phases"][0]
        with self.assertRaisesRegex(CampaignConfigError, "phase order"):
            validate_profile(config, ROOT)

    def test_unbounded_execution_is_rejected(self):
        config = copy.deepcopy(self.config)
        config["execution"]["max_concurrent_phases"] = 2
        with self.assertRaisesRegex(CampaignConfigError, "concurrency"):
            validate_profile(config, ROOT)

    def test_source_paths_cannot_escape_repository(self):
        config = copy.deepcopy(self.config)
        config["sources"]["disk_guard"] = "../outside"
        with self.assertRaisesRegex(CampaignConfigError, "escapes"):
            validate_profile(config, ROOT)

    def test_plan_does_not_emit_secret_values(self):
        config = copy.deepcopy(self.config)
        config["credentials"]["test_password"] = "do-not-print"
        # The profile schema rejects unknown credential behavior only through
        # the explicit password persistence/generator checks; the plan's field
        # filter still prevents accidental password output.
        value = plan(config, ROOT)
        self.assertNotIn("test_password", json.dumps(value))
        self.assertNotIn("do-not-print", json.dumps(value))


if __name__ == "__main__":
    unittest.main()
