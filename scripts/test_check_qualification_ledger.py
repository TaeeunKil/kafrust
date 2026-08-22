"""Unit tests for the v1 qualification-ledger checker."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_qualification_ledger.py"
SPEC = importlib.util.spec_from_file_location("check_qualification_ledger", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def fields(**overrides: str) -> dict[str, str]:
    values = {field: "not-applicable" for field in MODULE.REQUIRED_FIELDS}
    values.update(
        {
            "date_utc": "2026-08-22",
            "source_commit": "0" * 40,
            "work_status": "Done",
            "evidence_level": "CI",
            "workflow": "scripts/check_qualification_ledger.py",
            "result": "passed",
        }
    )
    values.update(overrides)
    return values


class QualificationLedgerTests(unittest.TestCase):
    def test_valid_row_has_no_errors(self) -> None:
        row = MODULE.Row("Q-TEST-001", fields())
        self.assertEqual(MODULE.validate_rows([row]), [])

    def test_missing_fields_and_invalid_status_are_rejected(self) -> None:
        values = fields(work_status="Unknown")
        values.pop("non_claims")
        errors = MODULE.validate_rows([MODULE.Row("Q-TEST-002", values)])
        self.assertTrue(any("missing fields" in error for error in errors))
        self.assertTrue(any("unsupported work_status" in error for error in errors))

    def test_unqualified_relative_labels_are_rejected(self) -> None:
        errors = MODULE.validate_rows(
            [MODULE.Row("Q-TEST-003", fields(artifact="latest published package"))]
        )
        self.assertTrue(any("relative or unqualified label" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
