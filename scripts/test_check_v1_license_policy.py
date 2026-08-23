import importlib.util
import unittest
from pathlib import Path
import sys


SCRIPT = Path(__file__).with_name("check_v1_license_policy.py")
SBOM_SCRIPT = SCRIPT.with_name("check_v1_sbom.py")
SBOM_SPEC = importlib.util.spec_from_file_location("check_v1_sbom", SBOM_SCRIPT)
assert SBOM_SPEC and SBOM_SPEC.loader
SBOM_MODULE = importlib.util.module_from_spec(SBOM_SPEC)
sys.modules[SBOM_SPEC.name] = SBOM_MODULE
SBOM_SPEC.loader.exec_module(SBOM_MODULE)
SPEC = importlib.util.spec_from_file_location("check_v1_license_policy", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class LicensePolicyTests(unittest.TestCase):
    def test_expression_ids_ignore_operators_and_parentheses(self):
        self.assertEqual(
            MODULE.expression_ids("(MIT OR Apache-2.0) AND Unicode-3.0"),
            ("Apache-2.0", "MIT", "Unicode-3.0"),
        )

    def test_current_policy_accepts_known_expression(self):
        self.assertEqual(
            set(MODULE.expression_ids("MIT OR Zlib OR Apache-2.0"))
            - MODULE.ACCEPTED_SPDX_IDS,
            set(),
        )

    def test_unknown_expression_is_rejected(self):
        self.assertNotEqual(
            set(MODULE.expression_ids("GPL-3.0-only")) - MODULE.ACCEPTED_SPDX_IDS,
            set(),
        )


if __name__ == "__main__":
    unittest.main()
