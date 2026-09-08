import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.local_lifetime_disk_budget import GIB, check_limits, estimate_bytes, main


class DiskBudgetTests(unittest.TestCase):
    def test_six_hour_default_fits_but_full_rate_day_does_not(self):
        self.assertLess(estimate_bytes(21600, 100, 64), 20 * GIB)
        self.assertGreater(estimate_bytes(86400, 100, 64), 20 * GIB)
        self.assertLess(estimate_bytes(86400, 25, 64), 20 * GIB)

    def test_growth_stops_while_disk_still_has_plenty_of_space(self):
        with self.assertRaisesRegex(ValueError, "growth"):
            check_limits({"host": 700 * GIB}, {"host": 680 * GIB}, 100 * GIB, 20 * GIB)

    def test_reserve_boundary_stops_even_with_small_growth(self):
        with self.assertRaisesRegex(ValueError, "reserve"):
            check_limits({"host": 110 * GIB}, {"host": 100 * GIB}, 100 * GIB, 20 * GIB)

    def test_below_growth_limit_passes(self):
        check_limits({"host": 700 * GIB}, {"host": 680 * GIB + 1}, 100 * GIB, 20 * GIB)

    def test_separate_output_volume_is_checked(self):
        with self.assertRaisesRegex(ValueError, "output"):
            check_limits({"docker": 700 * GIB, "output": 120 * GIB},
                         {"docker": 700 * GIB, "output": 100 * GIB}, 100 * GIB, 20 * GIB)


    def test_start_requires_reserve_plus_budget(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "budget.json"
            with patch("sys.argv", ["guard", "start", str(state), "--paths", folder]), \
                    patch("shutil.disk_usage", return_value=SimpleNamespace(free=119 * GIB)):
                with self.assertRaises(SystemExit) as raised:
                    main()
                self.assertEqual(raised.exception.code, 1)
            self.assertFalse(state.exists())

    def test_start_and_check_preserve_byte_precision(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "budget.json"
            with patch("sys.argv", ["guard", "start", str(state), "--paths", folder]), \
                    patch("shutil.disk_usage", return_value=SimpleNamespace(free=120 * GIB)):
                main()
            recorded = json.loads(state.read_text(encoding="utf-8"))
            self.assertEqual(recorded["budget"], 20 * GIB)
            with patch("sys.argv", ["guard", "check", str(state)]), \
                    patch("shutil.disk_usage", return_value=SimpleNamespace(free=100 * GIB + 1)):
                main()
            with patch("sys.argv", ["guard", "check", str(state)]), \
                    patch("shutil.disk_usage", return_value=SimpleNamespace(free=100 * GIB)):
                with self.assertRaises(SystemExit) as raised:
                    main()
                self.assertEqual(raised.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
