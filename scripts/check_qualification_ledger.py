#!/usr/bin/env python3
"""Validate the immutable qualification ledger used by the v1 program."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "docs" / "evidence" / "qualification-ledger.md"
HEADING_RE = re.compile(r"^## (Q-[A-Z0-9-]+)\s*$")
FIELD_RE = re.compile(r"^- ([a-z][a-z0-9_]*): (.+?)\s*$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
WORKFLOW_URL_RE = re.compile(r"^https://github\.com/[^/]+/[^/]+/actions/runs/[0-9]+(?:/.*)?$")
REQUIRED_FIELDS = frozenset(
    {
        "date_utc",
        "source_commit",
        "client_version",
        "protocol_version",
        "work_status",
        "evidence_level",
        "kafka_version",
        "kafka_image",
        "mode",
        "topology",
        "security",
        "group_protocol",
        "workload",
        "workflow",
        "fault",
        "duration",
        "record_count",
        "member_count",
        "repetition_count",
        "expected_errors",
        "observed_errors",
        "retry_count",
        "duplicate_count",
        "loss_count",
        "latency",
        "memory",
        "final_resource_gauges",
        "result",
        "artifact",
        "non_claims",
    }
)
WORK_STATUSES = frozenset({"Planned", "In progress", "Blocked", "Done", "Superseded"})
EVIDENCE_LEVELS = frozenset(
    {
        "Design",
        "Local deterministic",
        "CI",
        "Live current-source",
        "Packaged candidate",
        "Published artifact",
        "Service canary",
    }
)
FORBIDDEN_LABELS = re.compile(r"\b(?:latest|current|production-ready)\b", re.IGNORECASE)


@dataclass(frozen=True)
class Row:
    identifier: str
    fields: dict[str, str]


def parse_rows(path: Path = LEDGER) -> list[Row]:
    rows: list[Row] = []
    current_id: str | None = None
    current_fields: dict[str, str] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        heading = HEADING_RE.match(line)
        if heading:
            if current_id is not None:
                rows.append(Row(current_id, current_fields))
            current_id = heading.group(1)
            current_fields = {}
            continue
        if line.startswith("## "):
            if current_id is not None:
                rows.append(Row(current_id, current_fields))
                current_id = None
                current_fields = {}
            continue
        if current_id is None or not line.strip():
            continue
        field = FIELD_RE.match(line)
        if field is None:
            raise ValueError(f"{path}:{line_number}: expected '- field: value'")
        key, value = field.groups()
        if key in current_fields:
            raise ValueError(f"{path}:{line_number}: duplicate field {key!r} in {current_id}")
        current_fields[key] = value
    if current_id is not None:
        rows.append(Row(current_id, current_fields))
    return rows


def validate_rows(rows: Iterable[Row]) -> list[str]:
    errors: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if row.identifier in seen:
            errors.append(f"duplicate evidence id {row.identifier}")
        seen.add(row.identifier)
        missing = REQUIRED_FIELDS - row.fields.keys()
        unexpected = row.fields.keys() - REQUIRED_FIELDS
        if missing:
            errors.append(f"{row.identifier}: missing fields {sorted(missing)}")
        if unexpected:
            errors.append(f"{row.identifier}: unexpected fields {sorted(unexpected)}")
        for key, value in row.fields.items():
            if not value.strip():
                errors.append(f"{row.identifier}: empty {key}")
            if key != "evidence_level" and FORBIDDEN_LABELS.search(value):
                errors.append(f"{row.identifier}: relative or unqualified label in {key}")
        if not COMMIT_RE.fullmatch(row.fields.get("source_commit", "")):
            errors.append(f"{row.identifier}: source_commit must be a 40-character commit SHA")
        try:
            date.fromisoformat(row.fields.get("date_utc", ""))
        except ValueError:
            errors.append(f"{row.identifier}: date_utc must be YYYY-MM-DD")
        if row.fields.get("work_status") not in WORK_STATUSES:
            errors.append(f"{row.identifier}: unsupported work_status")
        if row.fields.get("evidence_level") not in EVIDENCE_LEVELS:
            errors.append(f"{row.identifier}: unsupported evidence_level")
        workflow = row.fields.get("workflow", "")
        if not (WORKFLOW_URL_RE.fullmatch(workflow) or workflow.startswith("scripts/")):
            errors.append(f"{row.identifier}: workflow must be an exact Actions URL or scripts path")
        if row.fields.get("result") not in {"passed", "failed", "blocked", "not-run"}:
            errors.append(f"{row.identifier}: result must be passed, failed, blocked, or not-run")
    return errors


def main() -> int:
    try:
        rows = parse_rows()
        errors = validate_rows(rows)
    except (OSError, ValueError) as error:
        print(f"qualification ledger failed: {error}")
        return 1
    if not rows:
        print("qualification ledger failed: no evidence rows")
        return 1
    if errors:
        print("qualification ledger failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"qualification ledger ok: {len(rows)} immutable rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
