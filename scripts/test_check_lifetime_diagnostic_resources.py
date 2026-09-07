import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("check_lifetime_diagnostic_resources.py")
SPEC = importlib.util.spec_from_file_location("check_lifetime_diagnostic_resources", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


VALID_WORKFLOW = "\n".join((*MODULE.REQUIRED_FRAGMENTS, "qualified: false"))


class LifetimeDiagnosticResourceTests(unittest.TestCase):
    def test_declared_controls_pass(self):
        MODULE.validate_workflow(VALID_WORKFLOW)

    def test_missing_resource_cap_fails(self):
        workflow = VALID_WORKFLOW.replace("--memory=2g\n", "")
        with self.assertRaisesRegex(ValueError, "memory cap"):
            MODULE.validate_workflow(workflow)

    def test_global_prune_fails(self):
        with self.assertRaisesRegex(ValueError, "global Docker prune"):
            MODULE.validate_workflow(VALID_WORKFLOW + "\ndocker system prune")

    def test_qualification_flag_is_required(self):
        workflow = VALID_WORKFLOW.replace("qualified: false", "qualified: true")
        with self.assertRaisesRegex(ValueError, "qualified=false"):
            MODULE.validate_workflow(workflow)

    def test_local_launcher_declares_small_profile_and_scoped_cleanup(self):
        launcher = "\n".join(
            (
                "--cpus=1.0",
                "--memory=2g",
                "--pids-limit=512",
                "--log-opt max-size=50m",
                "--log-opt max-file=3",
                '"qualified": False',
                "docker rm -f -v",
                "docker network rm",
                "docker network inspect",
                "KAFRUST_LOCAL_DURATION_SECONDS 21600",
                "KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND 100",
                "KAFRUST_LOCAL_PAYLOAD_BYTES 64",
            )
        )
        MODULE.validate_local_launcher(launcher)

    def test_local_launcher_rejects_global_prune(self):
        launcher = "\n".join(
            (
                "--cpus=1.0",
                "--memory=2g",
                "--pids-limit=512",
                "--log-opt max-size=50m",
                "--log-opt max-file=3",
                '"qualified": False',
                "docker rm -f -v",
                "docker network rm",
                "docker network inspect",
                "KAFRUST_LOCAL_DURATION_SECONDS 21600",
                "KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND 100",
                "KAFRUST_LOCAL_PAYLOAD_BYTES 64",
                "docker system prune",
            )
        )
        with self.assertRaisesRegex(ValueError, "global Docker prune"):
            MODULE.validate_local_launcher(launcher)


if __name__ == "__main__":
    unittest.main()
