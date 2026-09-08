import os
import shlex
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GUARD = ROOT / "scripts" / "local_lifetime_launcher_guard.sh"


def _pid_is_running(pid: int) -> bool:
    proc_stat = Path("/proc") / str(pid) / "stat"
    try:
        fields = proc_stat.read_text(encoding="utf-8").split()
        if len(fields) > 2 and fields[2] == "Z":
            return False
        os.kill(pid, 0)
        return True
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return False


def shutil_which(command: str) -> str | None:
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(directory) / command
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


@unittest.skipUnless(os.name == "posix" and shutil_which("bash"), "requires a POSIX shell")
class LauncherGuardTests(unittest.TestCase):
    def _start_group(
        self, marker: Path, *, ignores_term: bool = False
    ) -> tuple[subprocess.Popen, int]:
        child_command = "bash -c 'trap \"\" TERM; sleep 60'" if ignores_term else "sleep 60"
        command = (
            f"{child_command} & child=$!; "
            f"printf '%s\\n' \"$child\" > {shlex.quote(str(marker))}; "
            "wait"
        )
        process = subprocess.Popen(
            ["bash", "-c", command],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not marker.exists():
            time.sleep(0.01)
        self.assertTrue(marker.exists(), f"process group child marker was not written: {marker}")
        return process, int(marker.read_text(encoding="utf-8").strip())

    def test_mocked_low_space_aborts_all_groups_and_scopes_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            docker_bin = root / "docker"
            docker_log = root / "docker.log"
            docker_unrelated_touch = root / "unrelated-removed"
            docker_bin.write_text(
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                'printf \'%s\\n\' "$*" >> "$FAKE_DOCKER_LOG"\n'
                'if [[ "$1" == "ps" ]]; then\n'
                '  printf \'%s\\n\' "$FAKE_RUN_CONTAINER" "$FAKE_UNRELATED_CONTAINER"\n'
                'elif [[ "$1" == "rm" ]]; then\n'
                '  if [[ "${4:-}" == "$FAKE_UNRELATED_CONTAINER" ]]; then touch "$FAKE_UNRELATED_TOUCH"; fi\n'
                'elif [[ "$1" == "network" && "${2:-}" == "rm" ]]; then\n'
                '  :\n'
                'else\n'
                '  printf \'unexpected docker operation: %s\\n\' "$*" >&2\n'
                '  exit 91\n'
                'fi\n',
                encoding="utf-8",
            )
            docker_bin.chmod(0o755)
            disk_check = root / "mock-disk-check"
            disk_check.write_text(
                "#!/usr/bin/env bash\n"
                'printf \'mock-low-space\\n\' >> "$MOCK_DISK_LOG"\n'
                "exit 1\n",
                encoding="utf-8",
            )
            disk_check.chmod(0o755)
            disk_log = root / "disk.log"
            markers = [root / f"{name}.pid" for name in ("build", "helper", "sampler", "fault")]
            processes: list[subprocess.Popen] = []
            children: list[int] = []
            try:
                for marker in markers:
                    process, child = self._start_group(
                        marker, ignores_term=marker.name == "helper.pid"
                    )
                    processes.append(process)
                    children.append(child)

                prefix = "kafrust-local-lifetime-test-"
                network = "kafrust-local-lifetime-test"
                launcher_probe = root / "launcher-probe.sh"
                launcher_probe.write_text(
                    "#!/usr/bin/env bash\n"
                    "set -euo pipefail\n"
                    f"source {shlex.quote(str(GUARD))}\n"
                    'if "$MOCK_DISK_CHECK"; then\n'
                    "  exit 92\n"
                    "fi\n"
                    "cleanup_run_resources "
                    f"{shlex.quote(prefix)} {shlex.quote(network)} "
                    + " ".join(str(process.pid) for process in processes)
                    + "\n"
                    "exit 42\n",
                    encoding="utf-8",
                )
                launcher_probe.chmod(0o755)
                environment = os.environ.copy()
                environment.update(
                    {
                        "PATH": f"{root}{os.pathsep}{environment['PATH']}",
                        "FAKE_DOCKER_LOG": str(docker_log),
                        "FAKE_RUN_CONTAINER": f"{prefix}1",
                        "FAKE_UNRELATED_CONTAINER": "unrelated-container-1",
                        "FAKE_UNRELATED_TOUCH": str(docker_unrelated_touch),
                        "MOCK_DISK_CHECK": str(disk_check),
                        "MOCK_DISK_LOG": str(disk_log),
                    }
                )
                result = subprocess.run(
                    ["bash", str(launcher_probe)],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=environment,
                    timeout=30,
                )

                self.assertEqual(result.returncode, 42, result.stderr)
                self.assertTrue(disk_log.read_text(encoding="utf-8").strip())
                self.assertFalse(docker_unrelated_touch.exists())
                docker_operations = docker_log.read_text(encoding="utf-8").splitlines()
                self.assertTrue(any(line.startswith("ps -a") for line in docker_operations))
                self.assertIn(f"rm -f -v {prefix}1", docker_operations)
                self.assertIn(f"network rm {network}", docker_operations)
                self.assertFalse(any(line.startswith("run ") for line in docker_operations))
                self.assertFalse(any(line.startswith("exec ") for line in docker_operations))

                for process in processes:
                    process.wait(timeout=3)
                    self.assertIsNotNone(process.returncode)
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and any(
                    _pid_is_running(child) for child in children
                ):
                    time.sleep(0.05)
                self.assertFalse(any(_pid_is_running(child) for child in children))
            finally:
                for process in processes:
                    if process.poll() is None:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait(timeout=3)
if __name__ == "__main__":
    unittest.main()
