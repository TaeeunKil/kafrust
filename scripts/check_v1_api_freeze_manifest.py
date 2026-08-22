#!/usr/bin/env python3
"""Validate the V1-24 API-freeze preparation manifest and snapshot lock."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-24-api-freeze-manifest.json"
SNAPSHOT = ROOT / "docs" / "evidence" / "public-api-snapshot.json"
SNAPSHOT_CHECKER = ROOT / "scripts" / "public_api_snapshot.py"
SOURCE = ROOT / "crates" / "kafrust" / "src"


def fail(message: str) -> int:
    print(f"v1 API freeze check failed: {message}", file=sys.stderr)
    return 1


def public_declaration_digest() -> str:
    rows: list[str] = []
    for path in sorted(SOURCE.rglob("*.rs")):
        relative = path.relative_to(ROOT).as_posix()
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped.startswith("pub ") or stripped.startswith(("pub(crate)", "pub(super)", "pub(in ")):
                continue
            rows.append(f"{relative}\t{re.sub(r'\\s+', ' ', stripped)}")
    digest = hashlib.sha256()
    digest.update("\n".join(rows).encode("utf-8"))
    return digest.hexdigest()


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        snapshot = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return fail(str(error))
    if manifest.get("schema_version") != 1:
        return fail("schema_version must be 1")
    if manifest.get("status") not in {"preparation", "frozen"}:
        return fail("status must be preparation or frozen")
    if manifest.get("candidate_line") != "1.0.0-rc.1":
        return fail("candidate line must remain 1.0.0-rc.1")
    locked = manifest.get("snapshot", {})
    if locked.get("path") != "docs/evidence/public-api-snapshot.json":
        return fail("snapshot path must remain repository-relative and exact")
    expected = {
        "crate_version": snapshot.get("crate_version"),
        "feature_set": snapshot.get("feature_set"),
        "public_symbol_count": snapshot.get("public_symbol_count"),
        "root_module_count": len(snapshot.get("root_modules", [])),
        "root_export_count": len(snapshot.get("root_exports", [])),
        "public_declaration_sha256": snapshot.get("public_declaration_sha256"),
        "classification_counts": snapshot.get("counts"),
    }
    for key, value in expected.items():
        if locked.get(key) != value:
            return fail(f"snapshot lock differs for {key}: locked={locked.get(key)!r} actual={value!r}")
    if not re.fullmatch(r"[0-9a-f]{64}", str(locked.get("public_declaration_sha256", ""))):
        return fail("snapshot digest must be a SHA-256 hex string")
    if public_declaration_digest() != locked["public_declaration_sha256"]:
        return fail("source public declaration digest differs from the freeze input")
    profiles = manifest.get("feature_profiles")
    required_profiles = {"default", "tls", "blocking", "otlp", "all-features"}
    if not isinstance(profiles, list) or {profile.get("id") for profile in profiles} != required_profiles:
        return fail("feature profiles must cover default, tls, blocking, otlp, and all-features")
    if manifest.get("toolchains") != ["1.81.0", "stable"]:
        return fail("MSRV/stable toolchain order must remain explicit")
    policy = manifest.get("protocol_policy", {})
    if policy.get("protocol_first") is not True or policy.get("rc_client_exact_protocol_prerelease") is not True:
        return fail("protocol-first and exact RC protocol pin are required")
    if policy.get("path_or_patch_dependencies") is not False:
        return fail("freeze evidence cannot accept path or patch dependencies")
    if policy.get("publication_requires_explicit_authorization") is not True:
        return fail("publication authorization boundary must remain explicit")
    if manifest.get("freeze_inputs") != ["V1-20", "V1-21", "V1-22", "V1-23"]:
        return fail("V1-20 through V1-23 must be freeze inputs in dependency order")
    required_gates = {
        "public-api-snapshot",
        "public-surface-feature-profiles",
        "migration-notes-from-0.3.5",
        "exact-protocol-rc-lockfile",
        "rust-1.81-and-stable-package-build",
        "rustdoc-warning-free",
    }
    if set(manifest.get("required_gates", ())) != required_gates:
        return fail("required API-freeze gates are incomplete or changed")
    try:
        result = subprocess.run(
            [sys.executable, str(SNAPSHOT_CHECKER)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        return fail(f"cannot run public API snapshot checker: {error}")
    if result.returncode != 0:
        return fail(f"public API snapshot checker failed: {result.stderr.strip()}")
    print(
        f"v1 API freeze manifest ok: {locked['public_symbol_count']} symbols, "
        f"{locked['root_module_count']} modules, {locked['root_export_count']} root exports"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
