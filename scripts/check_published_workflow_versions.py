"""Keep published smoke workflow defaults aligned with the current crate.

Published workflows accept an explicit version for historical reruns, but an
unqualified dispatch must exercise the current registry baseline. This check
prevents a new publication from silently leaving the default smoke matrix on
an older artifact.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "docs" / "evidence" / "published-baseline.json"
WORKFLOW_DIR = ROOT / ".github" / "workflows"


def fail(message: str) -> int:
    print(f"published workflow version check failed: {message}", file=sys.stderr)
    return 1


def published_version() -> str:
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    if baseline.get("schema_version") != 1:
        raise ValueError("published baseline has an unsupported schema version")
    if baseline.get("client_crate") != "kafrust" or baseline.get("protocol_crate") != "kafrust-protocol":
        raise ValueError("published baseline must name the coordinated kafrust crates")
    version = baseline.get("version")
    if not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("published baseline version must be a stable semver")
    evidence = baseline.get("boundary_evidence")
    if not isinstance(evidence, str) or not evidence:
        raise ValueError("published baseline must cite boundary evidence")
    if not (ROOT / evidence).is_file():
        raise ValueError(f"published baseline evidence does not exist: {evidence}")
    return version


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
        version = published_version()
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
