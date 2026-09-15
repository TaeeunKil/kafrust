#!/usr/bin/env python3
"""Run a bounded, non-qualifying Kafka lifetime diagnostic on macOS.

The Linux launcher remains the reference workstation campaign. This native
macOS runner uses Docker Desktop, keeps all run output on a caller-selected
volume, and monitors that volume plus the configured Docker data volume. It
uses one-gigabyte broker caps by default so an Apple Silicon host with 8 GiB
of memory can run the three-broker diagnostic without claiming Linux-profile
equivalence.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

try:
    from .local_lifetime_disk_budget import GIB, check_limits, estimate_bytes
    from .local_lifetime_resource_sampler import parse_docker_memory_usage
except ImportError:  # Direct invocation: python scripts/run_local_lifetime_diagnostic_macos.py
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from local_lifetime_disk_budget import GIB, check_limits, estimate_bytes
    from local_lifetime_resource_sampler import parse_docker_memory_usage


RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
DEFAULT_DURATION_SECONDS = 21_600
DEFAULT_RATE_RECORDS_PER_SECOND = 25
DEFAULT_PAYLOAD_BYTES = 64
DEFAULT_DISK_RESERVE_GIB = 100
DEFAULT_DISK_BUDGET_GIB = 20
DEFAULT_KAFKA_VERSION = "4.3.1"
DEFAULT_BROKER_MEMORY = "1g"
BROKER_COUNT = 3


class MacDiagnosticError(RuntimeError):
    """A preflight, workload, or cleanup error in the local diagnostic."""


@dataclass(frozen=True)
class Config:
    repo_root: Path
    capacity_path: Path
    docker_data_path: Path
    output_dir: Path
    cargo_target_dir: Path
    duration_seconds: int
    rate_records_per_second: int
    payload_bytes: int
    disk_reserve_gib: int
    disk_budget_gib: int
    kafka_version: str
    broker_memory: str
    run_id: str

    @property
    def broker_image(self) -> str:
        return f"apache/kafka:{self.kafka_version}"

    @property
    def resource_prefix(self) -> str:
        return f"kafrust-macos-lifetime-{self.run_id}-"

    @property
    def network_name(self) -> str:
        return f"kafrust-macos-lifetime-{self.run_id}"

    @property
    def topic_name(self) -> str:
        return f"kafrust-macos-lifetime-{self.run_id}"

    @property
    def disk_paths(self) -> tuple[Path, ...]:
        output_volume = self.output_dir.parent
        while not output_volume.exists() and output_volume != output_volume.parent:
            output_volume = output_volume.parent
        values = (self.capacity_path, self.docker_data_path, output_volume)
        unique: list[Path] = []
        for value in values:
            resolved = value.resolve()
            if resolved not in unique:
                unique.append(resolved)
        return tuple(unique)


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name, str(default))
    try:
        return int(value)
    except ValueError as error:
        raise MacDiagnosticError(f"{name} must be an integer") from error


def require_command(name: str) -> None:
    if shutil.which(name) is None:
        raise MacDiagnosticError(f"missing command: {name}")


def path_is_under(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True


def load_config(repo_root: Path | None = None) -> Config:
    if platform.system() != "Darwin":
        raise MacDiagnosticError("this launcher requires macOS")
    if platform.machine() not in {"arm64", "aarch64"}:
        raise MacDiagnosticError("this launcher requires Apple Silicon (arm64)")

    root = (repo_root or Path(__file__).resolve().parents[1]).resolve()
    capacity_value = os.environ.get("KAFRUST_LOCAL_CAPACITY_PATH")
    docker_data_value = os.environ.get("KAFRUST_LOCAL_DOCKER_DATA_PATH")
    if not capacity_value:
        raise MacDiagnosticError(
            "set KAFRUST_LOCAL_CAPACITY_PATH to the mounted external SSD"
        )
    if not docker_data_value:
        raise MacDiagnosticError(
            "set KAFRUST_LOCAL_DOCKER_DATA_PATH to the volume containing Docker Desktop data"
        )

    capacity_path = Path(capacity_value).expanduser().resolve()
    docker_data_path = Path(docker_data_value).expanduser().resolve()
    if not capacity_path.is_dir():
        raise MacDiagnosticError(f"capacity path is not a directory: {capacity_path}")
    if not docker_data_path.is_dir():
        raise MacDiagnosticError(f"Docker data path is not a directory: {docker_data_path}")

    run_id = os.environ.get(
        "KAFRUST_LOCAL_RUN_ID",
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{os.getpid()}",
    )
    if not RUN_ID_RE.fullmatch(run_id):
        raise MacDiagnosticError("KAFRUST_LOCAL_RUN_ID contains unsupported characters")
    if len(run_id) > 24:
        raise MacDiagnosticError("KAFRUST_LOCAL_RUN_ID must be at most 24 characters")

    output_value = os.environ.get("KAFRUST_LOCAL_OUTPUT_DIR")
    output_dir = (
        Path(output_value).expanduser().resolve()
        if output_value
        else capacity_path / "kafrust-local-lifetime" / run_id
    )
    target_value = os.environ.get("KAFRUST_LOCAL_CARGO_TARGET_DIR")
    cargo_target_dir = (
        Path(target_value).expanduser().resolve()
        if target_value
        else output_dir / "external-project" / "target"
    )
    if output_dir.exists():
        raise MacDiagnosticError(f"output directory already exists: {output_dir}")
    if not path_is_under(output_dir, capacity_path):
        raise MacDiagnosticError("output directory must be on the selected external volume")
    if not path_is_under(cargo_target_dir, capacity_path):
        raise MacDiagnosticError("Cargo target directory must be on the selected external volume")

    duration_seconds = env_int("KAFRUST_LOCAL_DURATION_SECONDS", DEFAULT_DURATION_SECONDS)
    rate = env_int("KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND", DEFAULT_RATE_RECORDS_PER_SECOND)
    payload = env_int("KAFRUST_LOCAL_PAYLOAD_BYTES", DEFAULT_PAYLOAD_BYTES)
    reserve = env_int("KAFRUST_LOCAL_DISK_WATERMARK_GIB", DEFAULT_DISK_RESERVE_GIB)
    budget = env_int("KAFRUST_LOCAL_DISK_BUDGET_GIB", DEFAULT_DISK_BUDGET_GIB)
    if not 60 <= duration_seconds <= 86_400:
        raise MacDiagnosticError("duration must be between 60 seconds and 24 hours")
    if not 1 <= rate <= 100:
        raise MacDiagnosticError("rate must be between 1 and 100 records/s")
    if not 1 <= payload <= 256:
        raise MacDiagnosticError("payload must be between 1 and 256 bytes")
    if not 100 <= reserve <= 200:
        raise MacDiagnosticError("disk reserve must be between 100 and 200 GiB")
    if not 1 <= budget <= 20:
        raise MacDiagnosticError("disk growth budget must be between 1 and 20 GiB")
    if estimate_bytes(duration_seconds, rate, payload) > budget * GIB:
        raise MacDiagnosticError("estimated storage exceeds the configured growth budget")

    return Config(
        repo_root=root,
        capacity_path=capacity_path,
        docker_data_path=docker_data_path,
        output_dir=output_dir,
        cargo_target_dir=cargo_target_dir,
        duration_seconds=duration_seconds,
        rate_records_per_second=rate,
        payload_bytes=payload,
        disk_reserve_gib=reserve,
        disk_budget_gib=budget,
        kafka_version=os.environ.get("KAFRUST_LOCAL_KAFKA_VERSION", DEFAULT_KAFKA_VERSION),
        broker_memory=os.environ.get("KAFRUST_LOCAL_BROKER_MEMORY", DEFAULT_BROKER_MEMORY),
        run_id=run_id,
    )


def run_checked(command: Sequence[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(command),
            cwd=cwd,
            check=True,
            text=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "") or str(error)
        raise MacDiagnosticError(f"command failed: {' '.join(command)}: {detail.strip()}") from error


def preflight(config: Config) -> dict[str, Any]:
    for command in ("cargo", "docker", "python3"):
        require_command(command)
    run_checked(["docker", "info"])
    paths = {str(path): shutil.disk_usage(path).free for path in config.disk_paths}
    reserve = config.disk_reserve_gib * GIB
    budget = config.disk_budget_gib * GIB
    for path, available in paths.items():
        if available < reserve + budget:
            raise MacDiagnosticError(
                f"{path}: need at least {config.disk_reserve_gib + config.disk_budget_gib} GiB free"
            )
    return {
        "duration_seconds": config.duration_seconds,
        "rate_records_per_second": config.rate_records_per_second,
        "payload_bytes": config.payload_bytes,
        "estimated_storage_gib": round(
            estimate_bytes(
                config.duration_seconds,
                config.rate_records_per_second,
                config.payload_bytes,
            )
            / GIB,
            3,
        ),
        "broker_memory": config.broker_memory,
        "capacity_paths": paths,
        "output_dir": str(config.output_dir),
        "cargo_target_dir": str(config.cargo_target_dir),
    }


def start_disk_guard(config: Config) -> dict[str, Any]:
    config.output_dir.parent.mkdir(parents=True, exist_ok=True)
    config.output_dir.mkdir()
    config.cargo_target_dir.mkdir(parents=True, exist_ok=True)
    baseline = {str(path): shutil.disk_usage(path).free for path in config.disk_paths}
    reserve = config.disk_reserve_gib * GIB
    budget = config.disk_budget_gib * GIB
    for path, available in baseline.items():
        if available < reserve + budget:
            raise MacDiagnosticError(
                f"{path}: need reserve plus full run budget before starting"
            )
    state = {
        "baseline": baseline,
        "reserve": reserve,
        "budget": budget,
        "estimated_bytes": estimate_bytes(
            config.duration_seconds,
            config.rate_records_per_second,
            config.payload_bytes,
        ),
    }
    (config.output_dir / "disk-budget.json").write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return state


def check_disk_guard(state: dict[str, Any]) -> None:
    current = {path: shutil.disk_usage(path).free for path in state["baseline"]}
    check_limits(state["baseline"], current, state["reserve"], state["budget"])


def docker_memory_sample(container: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
        capture_output=True,
        check=False,
        text=True,
        timeout=5,
    )
    if completed.returncode != 0:
        return {"error": f"docker stats exited with {completed.returncode}"}
    line = next((value for value in completed.stdout.splitlines() if value.strip()), "")
    try:
        fields = json.loads(line)
        usage, limit = parse_docker_memory_usage(str(fields["MemUsage"]))
        percent = float(str(fields.get("MemPerc", "0%")).rstrip("%"))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        return {"error": f"unable to parse docker memory sample: {error}"}
    return {
        "memory_usage_bytes": usage,
        "memory_limit_bytes": limit,
        "memory_percent": percent,
    }


def record_sample(
    config: Config,
    sample_file: Any,
    helper: subprocess.Popen[str],
    containers: Sequence[str],
) -> None:
    sample = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "timestamp_unix_seconds": time.time(),
        "helper": {
            "helper_pid": helper.pid,
            "helper_process_tree_alive": helper.poll() is None,
            "helper_process_tree_process_count": None,
            "helper_process_tree_rss_bytes": None,
            "helper_process_tree_os_thread_count": None,
            "helper_process_tree_open_fd_count": None,
            "helper_process_tree_open_socket_count": None,
            "process_metrics_supported": False,
            "process_metrics_note": "macOS native runner records Docker and disk metrics; /proc metrics are unavailable",
        },
        "docker_memory": {name: docker_memory_sample(name) for name in containers},
        "disk_free": {
            str(path): {"free_bytes": shutil.disk_usage(path).free}
            for path in config.disk_paths
        },
    }
    sample_file.write(json.dumps(sample, sort_keys=True) + "\n")
    sample_file.flush()


def terminate_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)


def create_brokers(config: Config) -> list[str]:
    run_checked(["docker", "network", "create", config.network_name])
    cluster_id = run_checked(
        ["docker", "run", "--rm", config.broker_image, "/opt/kafka/bin/kafka-storage.sh", "random-uuid"]
    ).stdout.strip()
    containers = [f"{config.resource_prefix}{index}" for index in range(1, BROKER_COUNT + 1)]
    voters = ",".join(
        f"{index}@{config.resource_prefix}{index}:9093" for index in range(1, BROKER_COUNT + 1)
    )
    for index, container in enumerate(containers, start=1):
        port = 19_091 + index
        run_checked(
            [
                "docker",
                "run",
                "-d",
                "--name",
                container,
                "--hostname",
                container,
                "--network",
                config.network_name,
                "--cpus=1.0",
                f"--memory={config.broker_memory}",
                "--pids-limit=512",
                "--log-driver",
                "json-file",
                "--log-opt",
                "max-size=50m",
                "--log-opt",
                "max-file=3",
                "-p",
                f"{port}:9092",
                "-e",
                f"KAFKA_CLUSTER_ID={cluster_id}",
                "-e",
                f"KAFKA_NODE_ID={index}",
                "-e",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_LISTENERS=INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093",
                "-e",
                f"KAFKA_ADVERTISED_LISTENERS=INTERNAL://{container}:29092,EXTERNAL://localhost:{port}",
                "-e",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
                "-e",
                "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
                "-e",
                f"KAFKA_CONTROLLER_QUORUM_VOTERS={voters}",
                "-e",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2",
                "-e",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                config.broker_image,
            ]
        )
    for _ in range(120):
        probe = subprocess.run(
            [
                "docker",
                "exec",
                containers[0],
                "/opt/kafka/bin/kafka-broker-api-versions.sh",
                "--bootstrap-server",
                f"{containers[0]}:29092",
            ],
            capture_output=True,
            check=False,
            text=True,
        )
        if probe.returncode == 0:
            break
        time.sleep(2)
    else:
        logs = subprocess.run(
            ["docker", "logs", containers[0]], capture_output=True, check=False, text=True
        )
        raise MacDiagnosticError(f"Kafka did not become ready: {logs.stdout[-4000:]}")

    run_checked(
        [
            "docker",
            "exec",
            containers[0],
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            f"{containers[0]}:29092",
            "--create",
            "--topic",
            config.topic_name,
            "--partitions",
            "3",
            "--replication-factor",
            "3",
        ]
    )
    run_checked(
        [
            "python3",
            str(config.repo_root / "scripts" / "wait_for_kafka_topic.py"),
            "--topic",
            config.topic_name,
            "--partitions",
            "3",
            "--replicas",
            "3",
            "--containers",
            *containers,
        ],
        cwd=config.repo_root,
    )
    return containers


def cleanup(config: Config, containers: Sequence[str]) -> None:
    for container in containers:
        subprocess.run(["docker", "rm", "-f", "-v", container], check=False, capture_output=True)
    subprocess.run(["docker", "network", "rm", config.network_name], check=False, capture_output=True)


def write_helper_project(config: Config) -> tuple[Path, Path]:
    project_dir = config.output_dir / "external-project"
    source_dir = project_dir / "src"
    source_dir.mkdir(parents=True)
    shutil.copyfile(
        config.repo_root / ".github" / "published-multi-soak-smoke" / "src" / "main.rs",
        source_dir / "main.rs",
    )
    manifest = project_dir / "Cargo.toml"
    manifest.write_text(
        """[package]
name = "kafrust-macos-lifetime-diagnostic"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
kafrust = { path = "REPO_PATH/crates/kafrust" }
sha2 = "0.10"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
""".replace("REPO_PATH", str(config.repo_root)),
        encoding="utf-8",
    )
    return project_dir, manifest


def validate_result(result_file: Path) -> dict[str, Any]:
    result = json.loads(result_file.read_text(encoding="utf-8"))
    if result.get("recovered") is not True:
        raise MacDiagnosticError("diagnostic did not report broker recovery")
    for key in ("unknown_outcomes", "in_flight_requests", "buffered_records"):
        if result.get(key) != 0:
            raise MacDiagnosticError(f"result field {key!r} was not zero")
    count_fields = ("attempted_records", "acknowledged_records", "consumed_unique_records")
    counts = {key: result.get(key) for key in count_fields}
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in counts.values()):
        raise MacDiagnosticError("record counts were missing or invalid")
    if len(set(counts.values())) != 1:
        raise MacDiagnosticError("record counts did not reconcile")
    reconciliation = result.get("record_id_reconciliation", {})
    if reconciliation.get("loss_count") != 0 or reconciliation.get("duplicate_count") != 0:
        raise MacDiagnosticError("record-ID reconciliation reported loss or duplicates")
    if reconciliation.get("qualified") is not True:
        raise MacDiagnosticError("record-ID reconciliation was not qualified")
    return result


def write_descriptor(config: Config, result: dict[str, Any], result_file: Path, samples: Path) -> None:
    commit = subprocess.run(
        ["git", "-C", str(config.repo_root), "rev-parse", "HEAD"],
        capture_output=True,
        check=False,
        text=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "-C", str(config.repo_root), "status", "--porcelain"],
            capture_output=True,
            check=False,
            text=True,
        ).stdout.strip()
    )
    descriptor = {
        "schema_version": 1,
        "status": "diagnostic",
        "qualified": False,
        "qualification_reason": "native macOS rate-limited lifetime diagnostic; not V1-21 evidence",
        "campaign_id": config.run_id,
        "artifact": {"source_commit": commit, "working_tree_dirty": dirty},
        "runner": {
            "os": platform.system(),
            "architecture": platform.machine(),
            "broker_memory_cap": config.broker_memory,
            "process_metrics_supported": False,
        },
        "broker": {"image": config.kafka_version, "brokers": BROKER_COUNT},
        "workload": {
            "duration_seconds": config.duration_seconds,
            "rate_records_per_second": config.rate_records_per_second,
            "payload_bytes": config.payload_bytes,
            "replication_factor": 3,
            "partitions": 3,
            "disk_watermark_gib": config.disk_reserve_gib,
            "disk_growth_budget_gib": config.disk_budget_gib,
        },
        "fault": {"mode": "single-broker-restart", "broker": 1, "outage_seconds": 10},
        "storage": {
            "capacity_path": str(config.capacity_path),
            "docker_data_path": str(config.docker_data_path),
        },
        "result_file": result_file.name,
        "resource_samples": samples.name,
        "result": result,
        "non_claims": [
            "not V1-21 throughput evidence",
            "not V1-22 SLO evidence",
            "not published-artifact evidence",
            "not service-canary evidence",
            "not release authorization",
        ],
    }
    (config.output_dir / "macos-lifetime-descriptor.json").write_text(
        json.dumps(descriptor, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def run_campaign(config: Config) -> Path:
    for command in ("cargo", "docker", "python3"):
        require_command(command)
    run_checked(["docker", "info"])
    state = start_disk_guard(config)
    containers: list[str] = []
    helper: subprocess.Popen[str] | None = None
    stop_fault = threading.Event()
    fault_errors: list[str] = []
    result_file = config.output_dir / "macos-lifetime.json"
    samples_file = config.output_dir / "resource-samples.jsonl"
    stderr_file = config.output_dir / "helper-stderr.log"
    sample_handle: Any = None

    try:
        containers = create_brokers(config)
        check_disk_guard(state)
        project_dir, manifest = write_helper_project(config)
        environment = os.environ.copy()
        environment.update(
            {
                "KAFRUST_BOOTSTRAP_SERVERS": "localhost:19092,localhost:19093,localhost:19094",
                "KAFRUST_TOPIC": config.topic_name,
                "KAFRUST_SOAK_SECONDS": str(config.duration_seconds),
                "KAFRUST_SOAK_BATCH_SIZE": "50",
                "KAFRUST_SOAK_PAYLOAD_BYTES": str(config.payload_bytes),
                "KAFRUST_SOAK_RECORDS_PER_SECOND": str(config.rate_records_per_second),
                "CARGO_TARGET_DIR": str(config.cargo_target_dir),
            }
        )
        build = subprocess.run(
            ["cargo", "+stable", "build", "--quiet", "--release", "--manifest-path", str(manifest)],
            cwd=config.repo_root,
            env=environment,
            check=False,
            timeout=900,
            text=True,
        )
        if build.returncode != 0:
            raise MacDiagnosticError(f"diagnostic helper build failed with status {build.returncode}")
        check_disk_guard(state)

        helper_binary = config.cargo_target_dir / "release" / "kafrust-macos-lifetime-diagnostic"
        helper_stdout = result_file.open("w", encoding="utf-8")
        helper_stderr = stderr_file.open("w", encoding="utf-8")
        sample_handle = samples_file.open("w", encoding="utf-8")
        helper = subprocess.Popen(
            [str(helper_binary)],
            cwd=project_dir,
            env=environment,
            stdout=helper_stdout,
            stderr=helper_stderr,
            start_new_session=True,
            text=True,
        )

        def restart_broker() -> None:
            if stop_fault.wait(config.duration_seconds / 2):
                return
            try:
                run_checked(["docker", "stop", "-t", "1", containers[0]])
                if stop_fault.wait(10):
                    return
                run_checked(["docker", "start", containers[0]])
            except MacDiagnosticError as error:
                fault_errors.append(str(error))

        fault_thread = threading.Thread(target=restart_broker, name="kafrust-macos-fault", daemon=True)
        fault_thread.start()
        deadline = time.monotonic() + config.duration_seconds + 900
        while helper.poll() is None:
            if time.monotonic() >= deadline:
                terminate_process(helper)
                raise MacDiagnosticError("diagnostic helper exceeded its bounded execution timeout")
            check_disk_guard(state)
            record_sample(config, sample_handle, helper, containers)
            time.sleep(10)
        record_sample(config, sample_handle, helper, containers)
        stop_fault.set()
        fault_thread.join(timeout=15)
        if fault_errors:
            raise MacDiagnosticError(fault_errors[0])
        if helper.returncode != 0:
            detail = stderr_file.read_text(encoding="utf-8")[-4000:]
            raise MacDiagnosticError(f"diagnostic helper failed with status {helper.returncode}: {detail}")
        helper_stdout.close()
        helper_stderr.close()
        sample_handle.close()
        result = validate_result(result_file)
        check_disk_guard(state)
        write_descriptor(config, result, result_file, samples_file)
        return config.output_dir
    finally:
        stop_fault.set()
        if helper is not None:
            terminate_process(helper)
        if sample_handle is not None and not sample_handle.closed:
            sample_handle.close()
        cleanup(config, containers)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="check macOS, Docker, and external-volume capacity without starting a campaign",
    )
    args = parser.parse_args(argv)
    try:
        config = load_config()
        if args.preflight:
            print(json.dumps(preflight(config), indent=2, sort_keys=True))
        else:
            output_dir = run_campaign(config)
            print(f"macOS local lifetime diagnostic completed: {output_dir}")
        return 0
    except (MacDiagnosticError, OSError, subprocess.SubprocessError) as error:
        print(f"macOS local lifetime diagnostic: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
