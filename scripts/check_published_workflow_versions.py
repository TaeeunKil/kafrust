"""Keep published smoke workflow defaults aligned with the current crate.

Published workflows accept an explicit version for historical reruns, but an
unqualified dispatch must exercise the current registry baseline. This check
prevents a new publication from silently leaving the default smoke matrix on
an older artifact.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLIENT_MANIFEST = ROOT / "crates" / "kafrust" / "Cargo.toml"
WORKFLOW_DIR = ROOT / ".github" / "workflows"


def fail(message: str) -> int:
    print(f"published workflow version check failed: {message}", file=sys.stderr)
    return 1


def current_version() -> str:
    text = CLIENT_MANIFEST.read_text(encoding="utf-8")
    match = re.search(r"^version\s*=\s*\"([^\"]+)\"\s*$", text, re.MULTILINE)
    if match is None:
        raise ValueError("client Cargo.toml has no exact package version")
    return match.group(1)


def input_default(lines: list[str], workflow: Path) -> str:
    for index, line in enumerate(lines):
        if line.strip() != "kafrust_version:":
            continue
        for candidate in lines[index + 1 : index + 12]:
            if candidate.startswith("      ") and not candidate.startswith("        "):
                break
            match = re.match(r"\s+default:\s+\"([^\"]+)\"\s*$", candidate)
            if match is not None:
                return match.group(1)
        raise ValueError(f"{workflow.name}: kafrust_version input has no default")
    raise ValueError(f"{workflow.name}: missing kafrust_version input")


def main() -> int:
    try:
        version = current_version()
    except (OSError, ValueError) as error:
        return fail(str(error))

    workflows = sorted(WORKFLOW_DIR.glob("published-*.yml"))
    if not workflows:
        return fail("no published workflows found")

    checked = 0
    for workflow in workflows:
        try:
            text = workflow.read_text(encoding="utf-8")
            default = input_default(text.splitlines(), workflow)
        except (OSError, ValueError) as error:
            return fail(str(error))
        if default != version:
            return fail(f"{workflow.name}: default {default!r} does not match {version!r}")
        fallbacks = re.findall(r"inputs\.kafrust_version\s*\|\|\s*'([^']+)'", text)
        if any(fallback != version for fallback in fallbacks):
            return fail(f"{workflow.name}: fallback version does not match {version!r}")
        checked += 1

    print(f"published workflow version check ok: {checked} workflows default to {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
