#!/usr/bin/env python3
"""Run the validated Windows/WSL bounded campaign one phase at a time.

The runner starts no work until the profile and helper capability checks pass.
It monitors the existing disk guard while each child is alive, terminates the
child process group on a guard failure, and invokes only run-scoped cleanup.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Sequence

try:
    from .validate_bounded_campaign import (
        DEFAULT_CONFIG,
        CampaignConfigError,
        load_config,
        plan,
        validate_profile,
    )
except ImportError:  # Direct invocation: python scripts/run_bounded_campaign.py
    from validate_bounded_campaign import (  # type: ignore[no-redef]
        DEFAULT_CONFIG,
        CampaignConfigError,
        load_config,
        plan,
        validate_profile,
    )


# Docker uses the run ID in container hostnames; underscores are accepted by
# the old campaign validator but rejected by the container runtime.
RUN_ID_RE = re.compile(r"^[A-Za-z0-9.-]+$")
MAX_RUN_ID_LENGTH = 24
DEFAULT_KAFKA_VERSION = "3.7.2"


def fail(message: str) -> int:
    print(f"bounded campaign: {message}", file=sys.stderr)
    return 1


def require_linux_wsl() -> None:
    if platform.system() != "Linux":
        raise CampaignConfigError("execution must occur inside Linux/WSL; use run_bounded_campaign.ps1 from Windows")
    if platform.machine() not in {"x86_64", "AMD64"}:
        raise CampaignConfigError("campaign requires Linux x86_64")


def run_checked(command: Sequence[str], *, cwd: Path, env: dict[str, str]) -> None:
    subprocess.run(command, cwd=cwd, env=env, check=True)


def docker_root(cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(
        ["docker", "info", "--format", "{{.DockerRootDir}}"],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    value = result.stdout.strip()
    if not value:
        raise RuntimeError("Docker did not report DockerRootDir")
    return value


def capacity_path() -> str:
    mounted = Path("/mnt/t")
    return str(mounted) if mounted.is_dir() else "/"


def start_disk_guard(
    *,
    root: Path,
    state_path: Path,
    paths: Sequence[str],
    phase: dict[str, Any],
    host: dict[str, Any],
) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    duration = int(phase.get("duration_seconds", 60))
    rate = int(phase.get("rate_records_per_second", 1))
    payload = int(phase.get("payload_bytes", 64))
    run_checked(
        [
            "python3",
            str(root / "scripts/local_lifetime_disk_budget.py"),
            "start",
            str(state_path),
            "--paths",
            *paths,
            "--seconds",
            str(max(60, duration)),
            "--rate",
            str(max(1, min(100, rate))),
            "--payload",
            str(payload),
            "--reserve-gib",
            str(host["disk_reserve_gib"]),
            "--budget-gib",
            str(host["disk_growth_budget_gib"]),
        ],
        cwd=root,
        env=os.environ.copy(),
    )


def check_disk_guard(root: Path, state_path: Path, env: dict[str, str]) -> None:
    run_checked(
        ["python3", str(root / "scripts/local_lifetime_disk_budget.py"), "check", str(state_path)],
        cwd=root,
        env=env,
    )


def terminate_process_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if hasattr(os, "killpg"):
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            pass
        # Reap group descendants even if the leader exited after TERM.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def run_monitored(
    command: Sequence[str],
    *,
    cwd: Path,
    env: dict[str, str],
    state_path: Path,
    heartbeat: Callable[[], None] | None = None,
) -> None:
    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        env=env,
        start_new_session=True,
        text=True,
    )
    try:
        while process.poll() is None:
            try:
                check_disk_guard(cwd, state_path, env)
            except (OSError, subprocess.CalledProcessError) as error:
                terminate_process_group(process)
                raise RuntimeError(f"disk guard stopped the active phase: {error}") from error
            if heartbeat is not None:
                heartbeat()
            time.sleep(10)
        status = process.wait()
    except KeyboardInterrupt:
        terminate_process_group(process)
        raise
    if status != 0:
        raise RuntimeError(f"phase exited with status {status}")
    check_disk_guard(cwd, state_path, env)


def phase_output_dir(output_root: Path, phase_id: str) -> Path:
    if not RUN_ID_RE.fullmatch(phase_id):
        raise CampaignConfigError(f"unsafe phase id: {phase_id}")
    return output_root / phase_id


def write_campaign_state(
    path: Path,
    *,
    profile_id: str,
    run_id: str,
    status: str,
    current_phase: str | None,
    completed_phases: Sequence[str],
) -> None:
    payload = {
        "schema_version": 1,
        "profile_id": profile_id,
        "run_id": run_id,
        "status": status,
        "current_phase": current_phase,
        "completed_phases": list(completed_phases),
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def preflight_collisions(config: dict[str, Any], root: Path, run_id: str, env: dict[str, str]) -> None:
    """Reject all known names before any phase can invoke cleanup."""

    names: list[tuple[str, str]] = []
    for phase in config["phases"]:
        if phase["kind"] == "plaintext-soak":
            names.append(
                (
                    f"kafrust-local-lifetime-{run_id}-plaintext-",
                    f"kafrust-local-lifetime-{run_id}-plaintext",
                )
            )
            continue
        if phase["kind"] == "secure-group-churn":
            protocol = "classic" if phase["group_protocol"] == "classic" else "kip-848"
            phase_name = f"group-{protocol}-{phase['member_exit']}"
        else:
            phase_name = "secure-soak"
        names.append(
            (
                f"kafrust-bounded-{run_id}-{phase_name}-",
                f"kafrust-bounded-{run_id}-{phase_name}",
            )
        )
    for prefix, network in names:
        network_result = subprocess.run(
            ["docker", "network", "inspect", network],
            cwd=root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if network_result.returncode == 0:
            raise RuntimeError(f"run-scoped network already exists: {network}")
        container_result = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}"],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        if any(line.startswith(prefix) for line in container_result.stdout.splitlines()):
            raise RuntimeError(f"run-scoped container prefix already exists: {prefix}")


def phase_command(
    *,
    root: Path,
    phase: dict[str, Any],
    run_id: str,
    output_dir: Path,
    broker: dict[str, Any],
) -> list[str]:
    secure_helper = root / "scripts/run_bounded_secure_phase.sh"
    kind = phase["kind"]
    kafka_version = phase.get("kafka_version", DEFAULT_KAFKA_VERSION)
    if kind == "secure-group-churn":
        return [
            "bash",
            str(secure_helper),
            "group",
            "--protocol",
            "classic" if phase["group_protocol"] == "classic" else "kip-848",
            "--member-exit",
            phase["member_exit"],
            "--cycles",
            str(phase["cycles"]),
            "--kafka-version",
            kafka_version,
            "--run-id",
            run_id,
            "--output-dir",
            str(output_dir),
        ]
    if kind == "secure-soak":
        return [
            "bash",
            str(secure_helper),
            "soak",
            "--duration-seconds",
            str(phase["duration_seconds"]),
            "--rate-records-per-second",
            str(phase["rate_records_per_second"]),
            "--payload-bytes",
            str(phase["payload_bytes"]),
            "--batch-size",
            str(phase["batch_size"]),
            "--kafka-version",
            kafka_version,
            "--run-id",
            run_id,
            "--output-dir",
            str(output_dir),
        ]
    if kind == "plaintext-soak":
        return ["bash", str(root / "scripts/run_local_lifetime_diagnostic.sh")]
    raise CampaignConfigError(f"unsupported phase kind: {kind}")


def phase_env(
    *,
    phase: dict[str, Any],
    run_id: str,
    output_dir: Path,
    config: dict[str, Any],
) -> dict[str, str]:
    env = os.environ.copy()
    env["KAFRUST_KAFKA_VERSION"] = phase.get("kafka_version", DEFAULT_KAFKA_VERSION)
    env["KAFRUST_BOUNDED_RUN_ID"] = run_id
    env["KAFRUST_BOUNDED_OUTPUT_DIR"] = str(output_dir)
    if phase["kind"] == "plaintext-soak":
        env.update(
            {
                "KAFRUST_LOCAL_RUN_ID": f"{run_id}-plaintext",
                "KAFRUST_LOCAL_OUTPUT_DIR": str(output_dir),
                "KAFRUST_LOCAL_CARGO_TARGET_DIR": str(output_dir / "cargo-target"),
                "KAFRUST_LOCAL_DURATION_SECONDS": str(phase["duration_seconds"]),
                "KAFRUST_LOCAL_RATE_RECORDS_PER_SECOND": str(phase["rate_records_per_second"]),
                "KAFRUST_LOCAL_PAYLOAD_BYTES": str(phase["payload_bytes"]),
                "KAFRUST_LOCAL_DISK_WATERMARK_GIB": str(config["host"]["disk_reserve_gib"]),
                "KAFRUST_LOCAL_DISK_BUDGET_GIB": str(config["host"]["disk_growth_budget_gib"]),
            }
        )
        for name in (
            "KAFRUST_SECURITY_PROTOCOL",
            "KAFRUST_SASL_MECHANISM",
            "KAFRUST_SASL_USERNAME",
            "KAFRUST_SASL_PASSWORD",
            "KAFRUST_TLS_SERVER_NAME",
            "KAFRUST_TLS_ROOT_CERT_DER_PATH",
        ):
            env.pop(name, None)
    return env


def cleanup_phase(root: Path, config: dict[str, Any], run_id: str, phase: dict[str, Any], env: dict[str, str]) -> None:
    helper = root / config["sources"]["cleanup_helper"]
    phase_id = phase["id"]
    if phase["kind"] == "plaintext-soak":
        prefix = f"kafrust-local-lifetime-{run_id}-plaintext-"
        network = f"kafrust-local-lifetime-{run_id}-plaintext"
    else:
        protocol = phase.get("group_protocol", "secure-soak")
        if phase["kind"] == "secure-group-churn":
            phase_name = f"group-{('classic' if protocol == 'classic' else 'kip-848')}-{phase['member_exit']}"
        else:
            phase_name = "secure-soak"
        prefix = f"kafrust-bounded-{run_id}-{phase_name}-"
        network = f"kafrust-bounded-{run_id}-{phase_name}"
    subprocess.run(["bash", str(helper), prefix, network], cwd=root, env=env, check=False)


def write_campaign_provenance(output_root: Path, root: Path, env: dict[str, str]) -> None:
    source_root = env.get("KAFRUST_LOCAL_SOURCE_ROOT")
    if source_root:
        source_path = Path(source_root).resolve()
        commit = subprocess.run(
            ["git", "-C", str(source_path), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "-C", str(source_path), "status", "--porcelain"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        )
        payload = {
            "schema_version": 1,
            "source_mode": "local",
            "source_root": str(source_path),
            "source_commit": commit,
            "working_tree_dirty": dirty,
        }
    else:
        payload = {
            "schema_version": 1,
            "source_mode": "published",
            "published_version": env.get("KAFRUST_PUBLISHED_VERSION", "0.3.6"),
        }
    (output_root / "campaign-provenance.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def execute(
    config: dict[str, Any],
    root: Path,
    run_id: str,
    output_root: Path,
    only_phase: str | None = None,
) -> None:
    require_linux_wsl()
    if not RUN_ID_RE.fullmatch(run_id):
        raise CampaignConfigError("run ID contains unsupported characters")
    if len(run_id) > MAX_RUN_ID_LENGTH:
        raise CampaignConfigError(
            f"run ID is too long for Docker hostnames (maximum {MAX_RUN_ID_LENGTH} characters)"
        )
    phases = config["phases"]
    if only_phase is not None:
        matching = [phase for phase in phases if phase["id"] == only_phase]
        if not matching:
            raise CampaignConfigError(f"unknown phase: {only_phase}")
        phases = matching
    if output_root.exists():
        raise CampaignConfigError(f"output directory already exists: {output_root}")
    output_root.mkdir(parents=True)
    env = os.environ.copy()
    env["KAFRUST_BOUNDED_PROFILE"] = config["profile_id"]
    write_campaign_provenance(output_root, root, env)
    preflight_collisions(config, root, run_id, env)
    docker_root_path = docker_root(root, env)
    capacity = capacity_path()
    guard_dir = output_root / ".disk-budgets"
    shared_cargo_target = output_root / ".cargo-target"
    shared_cargo_target.mkdir()
    campaign_state = output_root / "campaign-state.json"
    completed_phases: list[str] = []
    write_campaign_state(
        campaign_state,
        profile_id=config["profile_id"],
        run_id=run_id,
        status="running",
        current_phase=None,
        completed_phases=completed_phases,
    )
    for phase in phases:
        phase_dir = phase_output_dir(output_root, phase["id"])
        state_path = guard_dir / f"{phase['id']}.json"
        command = phase_command(
            root=root,
            phase=phase,
            run_id=run_id,
            output_dir=phase_dir,
            broker=config["broker"],
        )
        phase_environment = phase_env(
            phase=phase,
            run_id=run_id,
            output_dir=phase_dir,
            config=config,
        )
        phase_environment["KAFRUST_BOUNDED_CARGO_TARGET_DIR"] = str(shared_cargo_target)
        if phase["kind"] == "plaintext-soak":
            phase_environment["KAFRUST_LOCAL_CARGO_TARGET_DIR"] = str(shared_cargo_target)
        phase_environment["KAFRUST_BOUNDED_DISK_STATE"] = str(state_path)
        start_disk_guard(
            root=root,
            state_path=state_path,
            paths=[capacity, docker_root_path, str(output_root), str(shared_cargo_target)],
            phase=phase,
            host=config["host"],
        )
        write_campaign_state(
            campaign_state,
            profile_id=config["profile_id"],
            run_id=run_id,
            status="running",
            current_phase=phase["id"],
            completed_phases=completed_phases,
        )
        print(f"starting bounded phase: {phase['id']}", flush=True)
        try:
            run_monitored(
                command,
                cwd=root,
                env=phase_environment,
                state_path=state_path,
                heartbeat=lambda: write_campaign_state(
                    campaign_state,
                    profile_id=config["profile_id"],
                    run_id=run_id,
                    status="running",
                    current_phase=phase["id"],
                    completed_phases=completed_phases,
                ),
            )
        except Exception:
            write_campaign_state(
                campaign_state,
                profile_id=config["profile_id"],
                run_id=run_id,
                status="failed",
                current_phase=phase["id"],
                completed_phases=completed_phases,
            )
            raise
        finally:
            cleanup_phase(root, config, run_id, phase, phase_environment)
        completed_phases.append(phase["id"])
        write_campaign_state(
            campaign_state,
            profile_id=config["profile_id"],
            run_id=run_id,
            status="running",
            current_phase=None,
            completed_phases=completed_phases,
        )
        print(f"completed bounded phase: {phase['id']}", flush=True)
    write_campaign_state(
        campaign_state,
        profile_id=config["profile_id"],
        run_id=run_id,
        status="completed",
        current_phase=None,
        completed_phases=completed_phases,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--only-phase",
        default=None,
        help="run one validated phase without repeating earlier completed phases",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="run-scoped output directory outside the repository",
    )
    parser.add_argument("--plan", action="store_true", help="print the secret-free plan and do not launch")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        validate_profile(config, args.root)
        result = plan(config, args.root)
        if args.plan:
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if not result["ready"]:
            for blocker in result["blockers"]:
                print(f"bounded campaign not ready: {blocker}", file=sys.stderr)
            return 78
        run_id = args.run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        output_root = args.output_root or Path(tempfile.gettempdir()) / "kafrust-bounded" / run_id
        execute(config, args.root, run_id, output_root, only_phase=args.only_phase)
    except (CampaignConfigError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        return fail(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
