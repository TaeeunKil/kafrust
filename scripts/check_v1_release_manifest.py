#!/usr/bin/env python3
"""Validate the V1-26 stable-release preparation manifest."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-26-release-manifest.json"
RC = ROOT / "docs" / "evidence" / "v1-25-release-candidate-manifest.json"


def fail(message: str) -> int:
    print(f"v1 stable release manifest check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        rc = json.loads(RC.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return fail(str(error))
    if manifest.get("schema_version") != 1:
        return fail("schema_version must be 1")
    if manifest.get("status") not in {"preparation", "frozen"}:
        return fail("status must be preparation or frozen")
    release = manifest.get("release", {})
    if release.get("client_version") != "1.0.0" or release.get("protocol_version") != "1.0.0":
        return fail("stable client and protocol versions must be 1.0.0")
    if manifest.get("rc_input") != "docs/evidence/v1-25-release-candidate-manifest.json":
        return fail("stable release must name the RC manifest as its input")
    rc_candidate = rc.get("candidate", {})
    if rc_candidate.get("client_version") != "1.0.0-rc.1" or rc_candidate.get("protocol_version") != "1.0.0-rc.1":
        return fail("RC input must be a coordinated 1.0.0-rc.1 pair")
    if manifest.get("dependencies") != ["V1-25"]:
        return fail("stable release dependency must be V1-25")
    allowed_diff = {
        "package-version-identity",
        "release-notes",
        "support-and-migration-documentation",
        "generated-checksums-and-sbom",
    }
    if set(manifest.get("allowed_diff_from_rc", ())) != allowed_diff:
        return fail("stable diff must be metadata-only relative to the accepted RC")
    expected_sequence = [
        "protocol-package-dry-run",
        "protocol-publish-after-explicit-authorization",
        "wait-for-fresh-registry-resolution",
        "external-rust-1-81-and-stable-protocol-build",
        "client-package-dry-run-against-accepted-protocol",
        "client-publish-after-explicit-authorization",
        "docs-rs-and-critical-smoke-verification",
        "annotated-tag-after-artifact-verification",
        "github-release-after-tag-verification",
        "post-publish-service-canary-and-rollback",
    ]
    if manifest.get("publication_sequence") != expected_sequence:
        return fail("stable publication sequence must verify artifacts before tag/release")
    policy = manifest.get("registry_policy", {})
    if policy.get("protocol_first") is not True or policy.get("path_or_patch_dependencies") is not False:
        return fail("stable publication must be protocol-first and path/patch-free")
    if policy.get("publication_requires_explicit_authorization") is not True:
        return fail("stable publication must require explicit authorization")
    if policy.get("source_behavior_change_requires_new_rc") is not True:
        return fail("behavior changes after RC must create a new RC")
    required = {
        "stable-package-hashes-and-contents",
        "v1-19-tarball-matrix-and-sbom",
        "v1-20-full-matrix",
        "critical-published-smoke",
        "docs-rs-pages",
        "annotated-tag-and-github-release",
        "post-publish-service-canary-and-rollback",
        "support-policy-and-known-limits",
    }
    if set(manifest.get("required_evidence", ())) != required:
        return fail("required stable-release evidence is incomplete or changed")
    print("v1 stable release manifest ok: metadata-only diff and post-publish gates declared")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
