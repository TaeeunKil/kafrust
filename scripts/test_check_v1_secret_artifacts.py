import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check_v1_secret_artifacts.py")
SPEC = importlib.util.spec_from_file_location("check_v1_secret_artifacts", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SecretArtifactTests(unittest.TestCase):
    def test_clean_tree_has_no_findings(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clean.log"
            path.write_text("credentials are redacted\n", encoding="utf-8")
            self.assertEqual(MODULE.scan_paths([Path(directory)], (b"seeded-password",)), [])

    def test_marker_split_across_read_chunks_is_found(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "split.log"
            path.write_bytes(b"prefix-seeded-" + b"password-suffix")
            self.assertEqual(
                MODULE.scan_paths([path], (b"seeded-password",), chunk_size=5),
                [(path, 0)],
            )

    def test_missing_path_and_empty_markers_fail_closed(self):
        with self.assertRaises(MODULE.ScanError):
            MODULE.scan_paths([Path("does-not-exist")], (b"marker",))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clean.log"
            path.write_text("safe", encoding="utf-8")
            with self.assertRaises(MODULE.ScanError):
                MODULE.scan_paths([path], ())

    def test_marker_file_ignores_empty_lines_but_rejects_empty_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "markers.txt"
            path.write_text("\nseeded-password\n\n", encoding="utf-8")
            self.assertEqual(MODULE.read_marker_file(path), (b"seeded-password",))
            path.write_text("\n", encoding="utf-8")
            with self.assertRaises(MODULE.ScanError):
                MODULE.read_marker_file(path)


if __name__ == "__main__":
    unittest.main()
