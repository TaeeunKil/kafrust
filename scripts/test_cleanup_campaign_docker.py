import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "cleanup_campaign_docker.sh"


@unittest.skipUnless(os.name == "posix" and shutil.which("bash"), "requires a POSIX shell")
class CleanupCampaignDockerTests(unittest.TestCase):
    def test_removes_only_prefix_containers_and_exact_network_without_prune(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            docker = root / "docker"
            docker_log = root / "docker.log"
            removed = root / "removed.log"
            network_removed = root / "network-removed.log"
            unrelated = "other-campaign-container-1"
            prefix = "kafrust-campaign-test-"
            network = "kafrust-campaign-test"
            docker.write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                'printf \'%s\\n\' "$*" >> "$FAKE_DOCKER_LOG"\n'
                'case "$1" in\n'
                '  ps)\n'
                '    printf \'%s\\n\' "$FAKE_PREFIX_ONE" "$FAKE_PREFIX_TWO" "$FAKE_UNRELATED"\n'
                '    ;;\n'
                '  rm)\n'
                '    [[ "${2:-}" == "-f" && "${3:-}" == "-v" ]] || exit 91\n'
                '    shift 3\n'
                '    for name in "$@"; do printf \'%s\\n\' "$name" >> "$FAKE_REMOVED"; done\n'
                '    ;;\n'
                '  network)\n'
                '    [[ "${2:-}" == "rm" && "${3:-}" == "$FAKE_NETWORK" ]] || exit 92\n'
                '    printf \'%s\\n\' "$3" >> "$FAKE_NETWORK_REMOVED"\n'
                '    ;;\n'
                '  system)\n'
                '    [[ "${2:-}" == "df" ]] || exit 93\n'
                '    ;;\n'
                '  *)\n'
                '    printf \'unexpected docker operation: %s\\n\' "$*" >&2\n'
                '    exit 94\n'
                '    ;;\n'
                'esac\n',
                encoding="utf-8",
            )
            docker.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{root}{os.pathsep}{environment['PATH']}",
                    "FAKE_DOCKER_LOG": str(docker_log),
                    "FAKE_REMOVED": str(removed),
                    "FAKE_NETWORK_REMOVED": str(network_removed),
                    "FAKE_PREFIX_ONE": f"{prefix}1",
                    "FAKE_PREFIX_TWO": f"{prefix}2",
                    "FAKE_UNRELATED": unrelated,
                    "FAKE_NETWORK": network,
                }
            )
            result = subprocess.run(
                ["bash", str(SCRIPT), prefix, network],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                timeout=10,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                removed.read_text(encoding="utf-8").splitlines(),
                [f"{prefix}1", f"{prefix}2"],
            )
            self.assertEqual(network_removed.read_text(encoding="utf-8").splitlines(), [network])
            docker_operations = docker_log.read_text(encoding="utf-8").splitlines()
            self.assertTrue(any(line.startswith("ps -a") for line in docker_operations))
            self.assertIn(f"rm -f -v {prefix}1 {prefix}2", docker_operations)
            self.assertIn(f"network rm {network}", docker_operations)
            self.assertIn("system df", docker_operations)
            self.assertFalse(any("prune" in line for line in docker_operations))
            self.assertFalse(any(unrelated in line for line in docker_operations))


if __name__ == "__main__":
    unittest.main()
