#!/usr/bin/env python3
"""Validate the V1-16 security mechanism and credential lifecycle contract."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "v1-16-security-contract.json"
CONFIG_SOURCE = ROOT / "crates" / "kafrust" / "src" / "config.rs"
REQUIRED_IDS = {
    "plaintext",
    "tls",
    "sasl-plain",
    "scram-sha-256",
    "scram-sha-512",
    "oauthbearer",
    "mtls-overlay",
}
EXPECTED_PROTOCOLS = {"Plaintext", "Tls", "SaslPlaintext", "SaslTls"}
EXPECTED_MECHANISMS = {"Plain", "ScramSha256", "ScramSha512", "OAuthBearer"}
ENTRY_FIELDS = {
    "contract_id",
    "kind",
    "protocols",
    "mechanisms",
    "source_paths",
    "validation",
    "failure",
    "rotation",
    "redaction",
    "tests",
}
VARIANT_RE = re.compile(r"^\s*([A-Z][A-Za-z0-9_]*)\s*[, {]", re.MULTILINE)


def fail(message: str) -> int:
    print(f"v1 security contract check failed: {message}", file=sys.stderr)
    return 1


def enum_variants(source: str, enum_name: str) -> set[str]:
    start = source.index(f"pub enum {enum_name}")
    body_start = source.index("{", start)
    body_end = source.index("}\n", body_start)
    return set(VARIANT_RE.findall(source[body_start + 1 : body_end]))


def non_empty_string(value: Any, field: str, contract_id: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{contract_id} has missing {field}")


def non_empty_string_list(value: Any, field: str, contract_id: str) -> None:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{contract_id} has empty {field}")
    for item in value:
        non_empty_string(item, field, contract_id)


def validate(manifest: dict[str, Any], source: str, root: Path = ROOT) -> int:
    if manifest.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    if manifest.get("milestone") != "V1-16":
        raise ValueError("milestone must be V1-16")
    if manifest.get("status") != "deterministic-security-contract":
        raise ValueError("status must remain deterministic-security-contract")
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ValueError("entries must be a list")
    protocols = enum_variants(source, "SecurityProtocol")
    mechanisms = enum_variants(source, "SaslMechanism")
    if protocols != EXPECTED_PROTOCOLS:
        raise ValueError(f"SecurityProtocol variants changed: {sorted(protocols)}")
    if mechanisms != EXPECTED_MECHANISMS:
        raise ValueError(f"SaslMechanism variants changed: {sorted(mechanisms)}")

    rows: dict[str, dict[str, Any]] = {}
    for row in entries:
        if not isinstance(row, dict):
            raise ValueError("every security entry must be an object")
        missing = ENTRY_FIELDS - set(row)
        if missing:
            raise ValueError(f"security entry is missing fields: {sorted(missing)}")
        contract_id = row.get("contract_id")
        non_empty_string(contract_id, "contract_id", repr(contract_id))
        if contract_id in rows:
            raise ValueError(f"duplicate contract_id: {contract_id}")
        rows[contract_id] = row
        for field in ("kind", "validation", "failure", "rotation", "redaction"):
            non_empty_string(row.get(field), field, contract_id)
        non_empty_string_list(row.get("source_paths"), "source_paths", contract_id)
        non_empty_string_list(row.get("tests"), "tests", contract_id)
        protocol_values = row.get("protocols")
        if not isinstance(protocol_values, list) or any(item not in protocols for item in protocol_values):
            raise ValueError(f"{contract_id} has an unknown protocol")
        mechanism_values = row.get("mechanisms")
        if not isinstance(mechanism_values, list) or any(item not in mechanisms for item in mechanism_values):
            raise ValueError(f"{contract_id} has an unknown mechanism")
        if "redact" not in row["redaction"].lower():
            raise ValueError(f"{contract_id} redaction policy is not explicit")
        for source_path in row["source_paths"]:
            resolved = (root / source_path).resolve()
            if root.resolve() not in resolved.parents or not resolved.is_file():
                raise ValueError(f"{contract_id} source path is invalid: {source_path}")

    actual = set(rows)
    if actual != REQUIRED_IDS:
        raise ValueError(
            f"security contract set differs: missing={sorted(REQUIRED_IDS - actual)}, "
            f"unexpected={sorted(actual - REQUIRED_IDS)}"
        )
    return len(rows)


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        source = CONFIG_SOURCE.read_text(encoding="utf-8")
        if not isinstance(manifest, dict):
            raise ValueError("manifest root must be an object")
        count = validate(manifest, source)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        return fail(str(error))
    print(f"v1 security contract ok: {count} transport/mechanism contracts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
