import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.local_lifetime_resource_sampler import (
    collect_sample,
    parse_docker_memory_usage,
    parse_memory_bytes,
    sample_docker_memory,
    sample_helper_process_tree,
)


class ResourceSamplerTests(unittest.TestCase):
    def test_memory_values_keep_byte_precision(self):
        self.assertEqual(parse_memory_bytes("1.5GiB"), int(1.5 * 1024**3))
        self.assertEqual(parse_memory_bytes("512MiB"), 512 * 1024**2)
        self.assertEqual(
            parse_docker_memory_usage("12.5MiB / 2GiB"),
            (int(12.5 * 1024**2), 2 * 1024**3),
        )

    def test_helper_sample_aggregates_rss_os_threads_and_file_descriptors(self):
        with tempfile.TemporaryDirectory() as folder:
            proc_root = Path(folder)
            for pid, parent, rss_kib, threads, descriptors in (
                (100, 1, 12, 2, ("0", "1")),
                (101, 100, 4, 1, ("0",)),
            ):
                process = proc_root / str(pid)
                (process / "fd").mkdir(parents=True)
                (process / "status").write_text(
                    f"PPid:\t{parent}\nVmRSS:\t{rss_kib} kB\nThreads:\t{threads}\n",
                    encoding="utf-8",
                )
                for descriptor in descriptors:
                    (process / "fd" / descriptor).touch()

            sample = sample_helper_process_tree(100, proc_root)

        self.assertTrue(sample["helper_process_tree_alive"])
        self.assertEqual(sample["helper_process_tree_process_count"], 2)
        self.assertEqual(sample["helper_process_tree_rss_bytes"], 16 * 1024)
        self.assertEqual(sample["helper_process_tree_os_thread_count"], 3)
        self.assertEqual(sample["helper_process_tree_open_fd_count"], 3)
        self.assertEqual(sample["helper_process_tree_open_socket_count"], 0)

    def test_docker_memory_sample_is_structured_and_bounded(self):
        completed = SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "Name": "kafrust-local-lifetime-run-1",
                    "MemUsage": "12.5MiB / 2GiB",
                    "MemPerc": "0.61%",
                }
            ),
        )
        with patch("subprocess.run", return_value=completed) as run:
            sample = sample_docker_memory("kafrust-local-lifetime-run-1")

        run.assert_called_once()
        self.assertEqual(sample["memory_usage_bytes"], int(12.5 * 1024**2))
        self.assertEqual(sample["memory_limit_bytes"], 2 * 1024**3)
        self.assertEqual(sample["memory_percent"], 0.61)

    def test_collect_sample_keeps_named_disk_and_docker_series(self):
        with patch(
            "scripts.local_lifetime_resource_sampler.shutil.disk_usage",
            return_value=SimpleNamespace(free=1234),
        ), patch(
            "scripts.local_lifetime_resource_sampler.sample_docker_memory",
            return_value={"memory_usage_bytes": 10, "memory_limit_bytes": 20},
        ):
            sample = collect_sample(100, ["broker-1"], ["/host", "/docker"])

        self.assertIn("timestamp_utc", sample)
        self.assertEqual(sample["disk_free"]["/host"]["free_bytes"], 1234)
        self.assertEqual(sample["docker_memory"]["broker-1"]["memory_limit_bytes"], 20)


if __name__ == "__main__":
    unittest.main()
