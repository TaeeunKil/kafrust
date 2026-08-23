import importlib.util
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).with_name("check_v1_unsafe_native_inventory.py")
SBOM_SCRIPT = SCRIPT.with_name("check_v1_sbom.py")
SBOM_SPEC = importlib.util.spec_from_file_location("check_v1_sbom", SBOM_SCRIPT)
assert SBOM_SPEC and SBOM_SPEC.loader
SBOM_MODULE = importlib.util.module_from_spec(SBOM_SPEC)
sys.modules[SBOM_SPEC.name] = SBOM_MODULE
SBOM_SPEC.loader.exec_module(SBOM_MODULE)
SPEC = importlib.util.spec_from_file_location("check_v1_unsafe_native_inventory", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class UnsafeNativeInventoryTests(unittest.TestCase):
    def test_unsafe_pattern_does_not_match_prose(self):
        self.assertEqual(len(MODULE.UNSAFE_CONSTRUCT.findall('/// unsafe code\nlet x = "unsafe";')), 0)

    def test_native_boundary_name_is_explicit(self):
        self.assertIn("ring", MODULE.NATIVE_BOUNDARY_NAMES)
        self.assertIn("libc", MODULE.NATIVE_BOUNDARY_NAMES)

    def test_owner_rationale_is_nonempty(self):
        owner, rationale = MODULE.owner_and_rationale(
            {"name": "ring"}, custom_build=True, native_boundary=True
        )
        self.assertTrue(owner)
        self.assertTrue(rationale)


if __name__ == "__main__":
    unittest.main()
