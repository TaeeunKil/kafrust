#!/usr/bin/env python3
"""Validate and report the local bounded campaign profile.

This validator is deliberately independent of Docker and Kafka.  It checks the
auditable workload contract before a runner is allowed to create resources.
The secure soak helper is inspected for its rate-limit contract; a missing
contract is reported as a readiness blocker rather than being inferred from an
environment variable that the helper ignores.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = Path(__file__).with_name("bounded_campaign_profiles.json")
GIB = 1024**3
PROFILE_CONTRACTS = {
    "windows-wsl-32g-low-rate": {
        "secure_rate": 100,
        "plaintext_rate": 25,
        "requires_half_budget": False,
    },
    "windows-wsl-32g-half-budget": {
        "secure_rate": 50,
        "plaintext_rate": 12,
        "requires_half_budget": True,
    },
}


class CampaignConfigError(ValueError):
    """The profile is malformed or does not describe the requested campaign."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CampaignConfigError(message)


def _phase_map(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    phases = config.get("phases")
    _require(isinstance(phases, list), "phases must be an array")
    _require(all(isinstance(phase, dict) for phase in phases), "each phase must be an object")
    return {phase.get("id"): phase for phase in phases}


def validate_profile(config: dict[str, Any], root: Path = ROOT) -> None:
    """Validate the exact four-phase profile and referenced repository files."""

    _require(config.get("schema_version") == 1, "schema_version must be 1")
    profile_id = config.get("profile_id")
    _require(profile_id in PROFILE_CONTRACTS, "unexpected profile_id")
    contract = PROFILE_CONTRACTS[profile_id]

    execution = config.get("execution", {})
    _require(execution.get("platform") == "windows-wsl", "execution platform must be windows-wsl")
    _require(execution.get("sequential") is True, "campaign must be sequential")
    _require(execution.get("fail_fast") is True, "campaign must fail fast")
    _require(execution.get("max_concurrent_phases") == 1, "campaign concurrency must be one")
    _require(execution.get("cleanup_scope") == "run-scoped", "cleanup must be run-scoped")

    host = config.get("host", {})
    _require(host.get("memory_gib") == 32, "host memory must be 32 GiB")
    _require(host.get("disk_reserve_gib") == 100, "disk reserve must be 100 GiB")
    _require(host.get("disk_growth_budget_gib") == 20, "disk growth budget must be 20 GiB")
    _require(host.get("disk_budget_scope") == "active-phase", "disk budget scope must be active-phase")
    if contract["requires_half_budget"]:
        _require(host.get("soft_memory_budget_gib") == 8, "half-budget profile must declare an 8 GiB WSL soft budget")
        _require(host.get("cpu_budget_vcpu") == 4, "half-budget profile must declare a 4 vCPU budget")

    broker = config.get("broker", {})
    _require(broker.get("brokers") == 3, "campaign requires three brokers")
    _require(broker.get("partitions") == 3, "campaign requires three partitions")
    _require(broker.get("replication_factor") == 3, "campaign requires replication factor three")
    _require(broker.get("cpu_limit") == "1.0", "broker CPU limit must be 1.0")
    _require(broker.get("memory_limit") == "2g", "broker memory limit must be 2g")
    _require(broker.get("pids_limit") == 512, "broker PID limit must be 512")
    _require(broker.get("log_max_size") == "50m", "broker log size must be 50m")
    _require(broker.get("log_max_files") == 3, "broker log retention must be three files")

    credentials = config.get("credentials", {})
    _require(credentials.get("mode") == "ephemeral", "credentials must be ephemeral")
    _require(credentials.get("persist_password") is False, "password persistence must be disabled")
    _require(
        credentials.get("generator") == "openssl rand -hex 24",
        "credentials must be generated with openssl rand -hex 24",
    )
    _require("password" not in credentials.get("generator", "").lower(), "credential generator contains a password literal")

    sources = config.get("sources", {})
    for name, relative_path in sources.items():
        _require(isinstance(relative_path, str), f"source {name} must be a path")
        source = (root / relative_path).resolve()
        _require(source.is_relative_to(root.resolve()), f"source {name} escapes repository root")
        _require(source.is_file(), f"source {name} does not exist: {relative_path}")

    phases = config.get("phases")
    expected_ids = [
        "classic-leave-group-churn",
        "classic-drop-group-churn",
        "kip848-leave-group-churn",
        "kip848-drop-group-churn",
        "secure-soak-6h",
        "plaintext-soak-12h",
    ]
    _require(
        [phase.get("id") for phase in phases] == expected_ids,
        "phase order must be classic leave/drop, KIP-848 leave/drop, secure six-hour, plaintext 12-hour",
    )

    for phase in phases[:4]:
        _require(phase.get("kind") == "secure-group-churn", f"{phase['id']} must be secure group churn")
        _require(phase.get("cycles") == 100, f"{phase['id']} must run 100 cycles")
        _require(phase.get("member_exit") in {"leave", "drop"}, f"{phase['id']} must use leave or drop")
        _require(phase.get("payload_bytes") == 64, f"{phase['id']} payload must be 64 bytes")
        _require(phase.get("kafka_version") in {"3.7.2", "4.3.1"}, f"{phase['id']} Kafka version is unsupported")
    _require(
        [phase.get("group_protocol") for phase in phases[:2]] == ["classic", "classic"],
        "first two phases must use classic protocol",
    )
    _require(
        [phase.get("group_protocol") for phase in phases[2:4]] == ["kip-848", "kip-848"],
        "next two phases must use KIP-848",
    )
    _require(
        [phase.get("member_exit") for phase in phases[:4]] == ["leave", "drop", "leave", "drop"],
        "group phases must cover leave and drop for each protocol",
    )
    _require(phases[2].get("kafka_version") == "4.3.1", "KIP-848 phases must use Kafka 4.3.1")
    _require(phases[3].get("kafka_version") == "4.3.1", "KIP-848 phases must use Kafka 4.3.1")
    _require(phases[0].get("kafka_version") == "3.7.2", "classic phases must use Kafka 3.7.2")
    _require(phases[1].get("kafka_version") == "3.7.2", "classic phases must use Kafka 3.7.2")

    secure = phases[4]
    _require(secure.get("kind") == "secure-soak", "third phase must be secure soak")
    _require(secure.get("security_protocol") == "sasl_tls", "secure soak must use SASL_TLS")
    _require(secure.get("duration_seconds") == 21_600, "secure soak must run six hours")
    _require(
        secure.get("rate_records_per_second") == contract["secure_rate"],
        f"secure soak must run at {contract['secure_rate']} records/s for this profile",
    )
    _require(secure.get("payload_bytes") == 64, "secure soak payload must be 64 bytes")
    _require(secure.get("requires_rate_limiter") is True, "secure soak must require an explicit rate limiter")

    plaintext = phases[5]
    _require(plaintext.get("kind") == "plaintext-soak", "fourth phase must be plaintext soak")
    _require(plaintext.get("security_protocol") == "plaintext", "plaintext soak must use plaintext")
    _require(plaintext.get("duration_seconds") == 43_200, "plaintext soak must run 12 hours")
    _require(
        plaintext.get("rate_records_per_second") == contract["plaintext_rate"],
        f"plaintext soak must run at {contract['plaintext_rate']} records/s for this profile",
    )
    _require(plaintext.get("payload_bytes") == 64, "plaintext soak payload must be 64 bytes")
    _require(secure.get("kafka_version") == "4.3.1", "secure soak must use Kafka 4.3.1")
    _require(plaintext.get("kafka_version") == "4.3.1", "plaintext soak must use Kafka 4.3.1")

    # Use the same planning formula as the existing disk guard.  Both long
    # phases are bounded individually; the launcher must clean the active
    # broker resources before the next phase starts.
    for phase in (secure, plaintext):
        estimate = estimate_bytes(
            phase["duration_seconds"],
            phase["rate_records_per_second"],
            phase["payload_bytes"],
        )
        _require(
            estimate <= host["disk_growth_budget_gib"] * GIB,
            f"{phase['id']} estimated storage exceeds the 20 GiB phase budget",
        )


def estimate_bytes(seconds: int, rate: int, payload: int) -> int:
    """Match scripts/local_lifetime_disk_budget.py's admission estimate."""

    return seconds * rate * (payload + 1024) * 3 * 2 + 2 * GIB


def readiness_blockers(config: dict[str, Any], root: Path = ROOT) -> list[str]:
    """Return blockers that prevent a safe run without starting Kafka/Docker."""

    blockers: list[str] = []
    helper = (root / config["sources"]["secure_soak_helper"]).resolve()
    try:
        source = helper.read_text(encoding="utf-8")
    except OSError as error:
        blockers.append(f"could not inspect secure soak helper: {error}")
        return blockers

    required_token = "KAFRUST_SOAK_RECORDS_PER_SECOND"
    if (
        required_token not in source
        or "RateLimiter" not in source
        or "MAX_RECORDS_PER_SECOND" not in source
    ):
        blockers.append(
            "secure soak helper does not implement KAFRUST_SOAK_RECORDS_PER_SECOND; "
            "do not claim the 100 records/s secure phase until a parent-approved "
            "helper patch adds and tests an explicit limiter"
        )
    return blockers


def load_config(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise CampaignConfigError(f"could not read campaign config: {error}") from error
    _require(isinstance(value, dict), "campaign config must be an object")
    return value


def plan(config: dict[str, Any], root: Path = ROOT) -> dict[str, Any]:
    """Return a secret-free, machine-readable readiness plan."""

    phases = []
    for phase in config["phases"]:
        item = {key: value for key, value in phase.items() if "password" not in key.lower() and "secret" not in key.lower()}
        if phase["kind"] in {"secure-soak", "plaintext-soak"}:
            item["estimated_storage_gib"] = round(
                estimate_bytes(
                    phase["duration_seconds"],
                    phase["rate_records_per_second"],
                    phase["payload_bytes"],
                )
                / GIB,
                3,
            )
        phases.append(item)
    blockers = readiness_blockers(config, root)
    return {
        "schema_version": 1,
        "profile_id": config["profile_id"],
        "ready": not blockers,
        "blockers": blockers,
        "execution": config["execution"],
        "host": config["host"],
        "broker": config["broker"],
        "credential_mode": config["credentials"]["mode"],
        "phases": phases,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--plan", action="store_true", help="print the secret-free plan")
    parser.add_argument("--check", action="store_true", help="require all readiness checks to pass")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
        validate_profile(config, args.root)
        result = plan(config, args.root)
    except (CampaignConfigError, OSError) as error:
        print(f"bounded campaign validation failed: {error}", file=sys.stderr)
        return 1

    if args.plan or not args.check:
        print(json.dumps(result, indent=2, sort_keys=True))
    if args.check and result["blockers"]:
        for blocker in result["blockers"]:
            print(f"bounded campaign not ready: {blocker}", file=sys.stderr)
        return 78
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
