"""Validate the migration note required by the V1-24 preparation gate."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "migration-from-rust-rdkafka.md"
BASELINE = ROOT / "docs" / "evidence" / "published-baseline.json"


def fail(message: str) -> int:
    print(f"v1 migration notes check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        text = DOC.read_text(encoding="utf-8")
        version = json.loads(BASELINE.read_text(encoding="utf-8"))["version"]
    except (OSError, KeyError, json.JSONDecodeError) as error:
        return fail(str(error))

    required = {
        f"Changes from kafrust 0.3.5 to {version}": "versioned migration heading",
        f'kafrust = "{version}"': "current dependency example",
        "not a drop-in compatibility claim": "non-drop-in disclaimer",
        "no intentional breaking public-API change": "patch breaking-change statement",
        "does not imply `1.0.0` stability": "pre-1.0 non-claim",
        "production SLO qualification": "production qualification non-claim",
    }
    for needle, label in required.items():
        if needle not in text:
            return fail(f"missing {label}: {needle}")
    if not re.search(r"^## Capability Gate\s*$", text, re.MULTILINE):
        return fail("capability gate is missing")
    print(f"v1 migration notes check ok: 0.3.5 -> {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
