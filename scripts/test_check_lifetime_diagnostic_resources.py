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
                'CARGO_TARGET_DIR="$cargo_target_dir"',
                "setsid timeout --kill-after=10s 900s cargo build --quiet --release",
                'while kill -0 "$build_pid"',
                "local_lifetime_launcher_guard.sh",
                "cleanup_run_resources",
                "disk_budget_check",
                '"$cargo_target_dir/release/kafrust-local-lifetime-diagnostic"',
                "local_lifetime_resource_sampler.py",
                "resource-samples.jsonl",
                '"acknowledged_records"',
                '"consumed_unique_records"',
                '"resource_samples":',
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
                'CARGO_TARGET_DIR="$cargo_target_dir"',
                "setsid timeout --kill-after=10s 900s cargo build --quiet --release",
                'while kill -0 "$build_pid"',
                "local_lifetime_launcher_guard.sh",
                "cleanup_run_resources",
                "disk_budget_check",
                '"$cargo_target_dir/release/kafrust-local-lifetime-diagnostic"',
                "local_lifetime_resource_sampler.py",
                "resource-samples.jsonl",
                '"acknowledged_records"',
                '"consumed_unique_records"',
                '"resource_samples":',
                "docker system prune",
            )
        )
        with self.assertRaisesRegex(ValueError, "global Docker prune"):
            MODULE.validate_local_launcher(launcher)

    def test_guard_declares_group_and_scoped_cleanup(self):
        guard = "\n".join(
            (
                "stop_process_group",
                'kill -0 -- "-$pid"',
                'kill -TERM -- "-$pid"',
                'kill -KILL -- "-$pid"',
                "docker rm -f -v",
                'awk -v prefix="$resource_prefix"',
                'docker network rm "$network_name"',
            )
        )
        MODULE.validate_local_guard(guard)

    def test_guard_rejects_global_prune(self):
        guard = "\n".join(
            (
                "stop_process_group",
                'kill -0 -- "-$pid"',
                'kill -TERM -- "-$pid"',
                'kill -KILL -- "-$pid"',
                "docker rm -f -v",
                'awk -v prefix="$resource_prefix"',
                'docker network rm "$network_name"',
                "docker system prune",
            )
        )
        with self.assertRaisesRegex(ValueError, "global Docker prune"):
            MODULE.validate_local_guard(guard)

    def test_resource_sampler_names_os_threads_and_disk_docker_samples(self):
        sampler = "\n".join(
            (
                "helper_process_tree_rss_bytes",
                "helper_process_tree_os_thread_count",
                "helper_process_tree_open_fd_count",
                "docker stats",
                '"disk_free"',
                '"timestamp_utc"',
            )
        )
        MODULE.validate_resource_sampler(sampler)

    def test_resource_sampler_rejects_tokio_task_mislabel(self):
        sampler = "\n".join(
            (
                "helper_process_tree_rss_bytes",
                "helper_process_tree_os_thread_count",
                "helper_process_tree_open_fd_count",
                "docker stats",
                '"disk_free"',
                '"timestamp_utc"',
                "tokio_task_count",
            )
        )
        with self.assertRaisesRegex(ValueError, "Tokio tasks"):
            MODULE.validate_resource_sampler(sampler)


if __name__ == "__main__":
    unittest.main()
