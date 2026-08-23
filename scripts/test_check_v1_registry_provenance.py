import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check_v1_registry_provenance.py")
SBOM_SCRIPT = SCRIPT.with_name("check_v1_sbom.py")
SBOM_SPEC = importlib.util.spec_from_file_location("check_v1_sbom", SBOM_SCRIPT)
assert SBOM_SPEC and SBOM_SPEC.loader
SBOM_MODULE = importlib.util.module_from_spec(SBOM_SPEC)
sys.modules[SBOM_SPEC.name] = SBOM_MODULE
SBOM_SPEC.loader.exec_module(SBOM_MODULE)
SPEC = importlib.util.spec_from_file_location("check_v1_registry_provenance", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RegistryProvenanceTests(unittest.TestCase):
    def test_sparse_index_rows_are_decoded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bytes"
            path.write_bytes(
                b'\x03\x02\x00etag: test\x001.0.0\x00'
                b'{"name":"bytes","vers":"1.0.0","cksum":"' + b"a" * 64 + b'","yanked":false}\0'
            )
            self.assertEqual(MODULE.index_rows(path)[0]["vers"], "1.0.0")

    def test_comparable_ignores_version_and_checksum(self):
        base = {
            "schema_version": 1,
            "generator": MODULE.GENERATOR,
            "platform": "x86_64-unknown-linux-gnu",
            "features": "all-features",
            "dependency_scope": "runtime-and-build",
            "registry": "crates.io sparse-index cache",
            "packages": [{"name": "bytes", "version": "1.0.0", "checksum": "a", "yanked": False}],
        }
        changed = {**base, "packages": [{**base["packages"][0], "version": "1.1.0", "checksum": "b"}]}
        self.assertEqual(MODULE.comparable(base), MODULE.comparable(changed))


if __name__ == "__main__":
    unittest.main()
