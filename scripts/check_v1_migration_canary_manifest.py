#!/usr/bin/env python3
"""Validate the V1-23 migration/canary manifest and fixture boundary."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-23-migration-canary-manifest.json"
FIXTURE = ROOT / ".github" / "published-rust-rdkafka-comparison"
WORKFLOW = ROOT / ".github" / "workflows" / "migration-canary.yml"


def fail(message: str) -> int:
    print(f"v1 migration canary check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return fail(str(error))
    if manifest.get("schema_version") != 1:
        return fail("schema_version must be 1")
    if manifest.get("status") not in {"preparation", "frozen"}:
        return fail("status must be preparation or frozen")
    if manifest.get("service_id") != "kafrust-reference-migration-canary":
        return fail("the reference service ID must remain stable")
    if not (FIXTURE / "Cargo.toml").is_file() or not (FIXTURE / "src" / "main.rs").is_file():
        return fail("reference comparison fixture is incomplete")
    try:
        workflow = WORKFLOW.read_text(encoding="utf-8")
    except OSError as error:
        return fail(f"missing migration workflow: {error}")
    for marker in ("KAFRUST_COMPARISON_IMPLEMENTATION: kafrust", "KAFRUST_COMPARISON_IMPLEMENTATION: rdkafka", "Compare normalized smoke results", "Upload canary evidence"):
        if marker not in workflow:
            return fail(f"migration workflow is missing {marker}")
    if manifest.get("client_implementations") != ["kafrust", "rust-rdkafka"]:
        return fail("both client implementations must be compared")
    stages = manifest.get("stages", ())
    required_stages = {"baseline", "forward-cutover", "fault-observe", "rollback", "post-rollback"}
    if not required_stages <= set(stages):
        return fail("migration stages omit a forward or rollback stage")
    smoke = manifest.get("smoke_gate", {})
    exit_gate = manifest.get("exit_gate", {})
    if smoke.get("minimum_unique_records") != 1000 or not smoke.get("isolated_topics") or not smoke.get("isolated_groups"):
        return fail("smoke gate must isolate topics/groups and require 1000 records")
    if exit_gate.get("minimum_unique_records") != 1_000_000 or exit_gate.get("zero_unexplained_divergence") is not True:
        return fail("exit gate must require one million records and zero unexplained divergence")
    if exit_gate.get("unknown_outcomes_reconciled_before_retry") is not True:
        return fail("unknown outcomes must be reconciled before retry")
    required_fields = {"service_id", "fixture_commit", "stage", "implementation", "unique_records", "offset_divergence", "rollback_result"}
    if not required_fields <= set(manifest.get("result_fields", ())):
        return fail("result fields omit migration or rollback evidence")
    print("v1 migration canary manifest ok: reference fixture and forward/rollback gates declared")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
