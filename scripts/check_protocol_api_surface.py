"""Check that the protocol API modules expose the intended Kafka API keys.

This is a local, dependency-free surface gate. It does not claim to replace
Apache Kafka schema parity; it catches missing module registration, duplicate
keys, and accidental key drift before a schema-level comparison is added.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_DIR = ROOT / "crates" / "kafrust-protocol" / "src" / "api"
MOD_FILE = API_DIR / "mod.rs"

# This is the reviewed API-key set currently implemented by kafrust. Adding a
# new Kafka API is intentionally a manifest change, not an incidental source
# edit that bypasses protocol review.
EXPECTED_API_KEYS = {
    0,
    1,
    2,
    3,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    41,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
    49,
    50,
    51,
    55,
    57,
    60,
    61,
    64,
    65,
    66,
    68,
    69,
    71,
    72,
    74,
    75,
    76,
    77,
    78,
    79,
    80,
    81,
    83,
    84,
    85,
    86,
    87,
    88,
    89,
    90,
    91,
    92,
}

MODULE_RE = re.compile(r"^pub mod ([a-z0-9_]+);$", re.MULTILINE)
API_KEY_RE = re.compile(
    r"^\s*pub const ([A-Z0-9_]*API_KEY): i16 = (-?\d+);$", re.MULTILINE
)


def fail(message: str) -> int:
    print(f"protocol API surface check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    if not API_DIR.is_dir() or not MOD_FILE.is_file():
        return fail(f"missing protocol API directory or module file: {API_DIR}")

    registered_modules = set(MODULE_RE.findall(MOD_FILE.read_text(encoding="utf-8")))
    source_modules = {
        path.stem for path in API_DIR.glob("*.rs") if path.name != "mod.rs"
    }
    missing_registration = sorted(source_modules - registered_modules)
    missing_source = sorted(registered_modules - source_modules)
    if missing_registration:
        return fail(f"source modules missing from api/mod.rs: {missing_registration}")
    if missing_source:
        return fail(f"api/mod.rs modules without source files: {missing_source}")

    definitions: list[tuple[str, str, int]] = []
    for module in sorted(source_modules):
        path = API_DIR / f"{module}.rs"
        matches = API_KEY_RE.findall(path.read_text(encoding="utf-8"))
        if not matches:
            return fail(f"{path.relative_to(ROOT)} does not declare an API key")
        definitions.extend((module, name, int(key)) for name, key in matches)

    by_key: dict[int, list[str]] = {}
    for module, name, key in definitions:
        by_key.setdefault(key, []).append(f"{module}::{name}")
    duplicate_keys = {
        key: names for key, names in by_key.items() if len(names) > 1
    }
    if duplicate_keys:
        return fail(f"duplicate API keys: {duplicate_keys}")

    actual_keys = set(by_key)
    if actual_keys != EXPECTED_API_KEYS:
        return fail(
            "reviewed API-key manifest differs: "
            f"missing={sorted(EXPECTED_API_KEYS - actual_keys)}, "
            f"unexpected={sorted(actual_keys - EXPECTED_API_KEYS)}"
        )

    print(
        f"protocol API surface ok: {len(source_modules)} modules, "
        f"{len(definitions)} unique Kafka API keys"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
