#!/usr/bin/env python3
"""Validate one downloaded V1-18 fuzz qualification campaign."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path


TARGETS = (
    "codec",
    "frame",
    "api_versions_response",
    "group_describe_response",
    "share_group_offsets_response",
    "streams_groups_response",
    "offset_commit_response",
    "offset_fetch_response",
    "compression",
    "list_groups_response",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def fail(message: str) -> int:
    print(f"v1 fuzz qualification artifact check failed: {message}", file=sys.stderr)
    return 1


def corpus_digest(corpus_dir: Path, target: str) -> str:
    files = sorted(path for path in corpus_dir.rglob("*") if path.is_file())
    if not files:
        raise ValueError(f"{target}: corpus is empty")
    lines = []
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  corpus/{target}/{path.name}\n")
    return hashlib.sha256("".join(lines).encode("utf-8")).hexdigest()


def find_corpus_dir(qualification: Path, target: str) -> Path:
    for ancestor in qualification.parents:
        candidate = ancestor / "kafrust" / "kafrust" / "fuzz" / "corpus" / target
        if candidate.is_dir():
            return candidate
    raise ValueError(f"{target}: uploaded corpus directory not found for {qualification}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact_dir", type=Path)
    parser.add_argument("--workflow-sha", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--shard-seconds", type=int, default=900)
    parser.add_argument("--target-seconds", type=int, default=3600)
    parser.add_argument("--rss-limit-mb", type=int, default=2048)
    args = parser.parse_args()

    root = args.artifact_dir.resolve()
    if not root.is_dir():
        return fail(f"artifact directory does not exist: {root}")
    try:
        qualifications = sorted(root.rglob("qualification.json"))
        expected_count = len(TARGETS) * 4
        if len(qualifications) != expected_count:
            return fail(f"expected {expected_count} qualification.json files, found {len(qualifications)}")
        rows = []
        for path in qualifications:
            row = json.loads(path.read_text(encoding="utf-8"))
            rows.append((path, row))
        pairs = {(row.get("target"), row.get("shard")) for _, row in rows}
        if len(pairs) != expected_count:
            return fail("target/shard pairs are duplicated or incomplete")
        if {row.get("target") for _, row in rows} != set(TARGETS):
            return fail("target set differs from the ten declared fuzz targets")
        if {row.get("shard") for _, row in rows} != {0, 1, 2, 3}:
            return fail("shards must be exactly 0, 1, 2, and 3")
        if args.shard_seconds * 4 < args.target_seconds:
            return fail("four shard durations do not provide the target campaign budget")
        expected_uri = f"https://github.com/TaeeunKil/kafrust/actions/runs/{args.run_id}"
        for path, row in rows:
            target = row.get("target")
            if row.get("duration_seconds") != args.shard_seconds:
                return fail(f"{target}: duration does not match --shard-seconds")
            if row.get("campaign_seconds_per_target") != args.target_seconds:
                return fail(f"{target}: cumulative campaign duration is wrong")
            if row.get("workflow_sha") != args.workflow_sha:
                return fail(f"{target}: workflow SHA differs from expected source")
            if row.get("toolchain") != "nightly" or row.get("rss_limit_mb") != args.rss_limit_mb:
                return fail(f"{target}: toolchain or RSS limit differs from the manifest")
            if row.get("result") != "passed" or row.get("artifact_uri") != expected_uri:
                return fail(f"{target}: result or artifact URI is not qualified")
            corpus_sha = row.get("corpus_sha256")
            if not isinstance(corpus_sha, str) or not SHA256_RE.fullmatch(corpus_sha):
                return fail(f"{target}: corpus_sha256 is not a SHA-256 value")
            corpus_dir = find_corpus_dir(path, target)
            if corpus_digest(corpus_dir, target) != corpus_sha:
                return fail(f"{target} shard {row.get('shard')}: corpus hash mismatch")
            unexpected = [item for item in path.parent.iterdir() if item.name != "qualification.json"]
            if unexpected:
                return fail(f"{target} shard {row.get('shard')}: crash/OOM artifact retained: {unexpected[0]}")
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return fail(str(error))
    print(
        f"v1 fuzz qualification artifacts ok: {len(rows)} shards, "
        f"{len(TARGETS)} targets, {args.target_seconds}s cumulative per target"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
