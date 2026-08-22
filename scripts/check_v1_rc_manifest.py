#!/usr/bin/env python3
"""Validate the V1-25 release-candidate preparation manifest."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-25-release-candidate-manifest.json"
FREEZE = ROOT / "docs" / "evidence" / "v1-24-api-freeze-manifest.json"


def fail(message: str) -> int:
    print(f"v1 RC manifest check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        freeze = json.loads(FREEZE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return fail(str(error))
    if manifest.get("schema_version") != 1:
        return fail("schema_version must be 1")
    if manifest.get("status") not in {"preparation", "frozen"}:
        return fail("status must be preparation or frozen")
    candidate = manifest.get("candidate", {})
    if candidate.get("client_version") != "1.0.0-rc.1" or candidate.get("protocol_version") != "1.0.0-rc.1":
        return fail("client and protocol must be coordinated at 1.0.0-rc.1")
    if manifest.get("freeze_input") != "docs/evidence/v1-24-api-freeze-manifest.json":
        return fail("freeze input must point to the V1-24 manifest")
    if freeze.get("candidate_line") != "1.0.0-rc.1":
        return fail("V1-24 freeze manifest must use the same RC identity")
    if manifest.get("dependencies") != ["V1-20", "V1-21", "V1-22", "V1-23", "V1-24"]:
        return fail("dependencies must preserve V1-20 through V1-24 order")
    expected_sequence = [
        "protocol-package-dry-run",
        "protocol-publish-after-explicit-authorization",
        "wait-for-fresh-registry-resolution",
        "external-rust-1-81-and-stable-protocol-build",
        "client-package-dry-run-against-exact-protocol-rc",
        "client-publish-after-explicit-authorization",
        "docs-rs-and-external-feature-verification",
        "github-prerelease-after-artifact-verification",
    ]
    if manifest.get("publication_sequence") != expected_sequence:
        return fail("publication sequence must remain protocol-first and approval-gated")
    policy = manifest.get("registry_policy", {})
    if policy.get("protocol_first") is not True or policy.get("client_exact_protocol_dependency") != "=1.0.0-rc.1":
        return fail("RC must publish protocol first and exact-pin the matching protocol RC")
    if policy.get("path_or_patch_dependencies") is not False:
        return fail("RC evidence cannot accept path or patch dependencies")
    if policy.get("publication_requires_explicit_authorization") is not True:
        return fail("RC publication must require explicit authorization")
    if policy.get("partial_protocol_publication_is_not_reused") is not True:
        return fail("partial protocol publication must not be reused")
    campaigns = manifest.get("campaign_requirements", {})
    if campaigns.get("fault_soak_hours") != 24 or campaigns.get("fuzz_minutes_per_target") != 60:
        return fail("RC campaign requirements must include 24-hour fault and 60-minute fuzz gates")
    if campaigns.get("performance_repetitions") != 5 or campaigns.get("migration_canary") != "forward-fault-observe-rollback-forward":
        return fail("RC performance and migration gates are incomplete")
    required = {
        "exact-package-hashes",
        "rust-1-81-and-stable-external-lockfiles",
        "v1-20-matrix-rerun",
        "v1-21-fault-and-resource-gates",
        "v1-22-slo-gates",
        "v1-23-service-canary-and-rollback",
        "docs-rs-pages",
        "github-prerelease",
    }
    if set(manifest.get("required_evidence", ())) != required:
        return fail("required RC evidence is incomplete or changed")
    print("v1 RC manifest ok: protocol-first exact-pin and campaign gates declared")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
