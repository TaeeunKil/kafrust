#!/usr/bin/env python3
"""Scan retained text artifacts for seeded credential markers.

This check is intentionally narrower than a general secret scanner: the
markers are deterministic fixtures used by the redaction tests, and findings
are reported by marker index rather than by printing credential material.  It
is suitable for retained evidence/artifact directories and does not inspect
workflow source, where fake credentials are required to configure disposable
brokers.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PATH = ROOT / "docs" / "evidence"
DEFAULT_MARKERS = (
    b"secret-password",
    b"secret-hmac",
    b"fresh-token",
    b"kafrust-secret",
    b"broker-secret",
    b"admin-secret",
    b"denied-secret",
)
CHUNK_SIZE = 64 * 1024


class ScanError(ValueError):
    """An artifact path or marker list cannot be scanned safely."""


def fail(message: str) -> int:
    print(f"v1 secret artifact check failed: {message}", file=sys.stderr)
    return 1


def iter_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if not path.exists():
            raise ScanError(f"artifact path does not exist: {path}")
        if path.is_file():
            files.append(path)
            continue
        if not path.is_dir():
            raise ScanError(f"artifact path is not a file or directory: {path}")
        files.extend(
            child
            for child in sorted(path.rglob("*"))
            if child.is_file() and not child.is_symlink()
        )
    return sorted(set(files))


def scan_file(path: Path, markers: tuple[bytes, ...], *, chunk_size: int = CHUNK_SIZE) -> set[int]:
    if not markers:
        raise ScanError("at least one marker is required")
    if chunk_size <= 0:
        raise ScanError("chunk_size must be positive")
    overlap = max(len(marker) for marker in markers) - 1
    tail = b""
    found: set[int] = set()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(chunk_size):
                window = tail + chunk
                for index, marker in enumerate(markers):
                    if marker in window:
                        found.add(index)
                tail = window[-overlap:] if overlap else b""
    except OSError as error:
        raise ScanError(f"cannot read artifact {path}: {error}") from error
    return found


def scan_paths(
    paths: list[Path], markers: tuple[bytes, ...] = DEFAULT_MARKERS, *, chunk_size: int = CHUNK_SIZE
) -> list[tuple[Path, int]]:
    findings: list[tuple[Path, int]] = []
    for path in iter_files(paths):
        findings.extend((path, index) for index in sorted(scan_file(path, markers, chunk_size=chunk_size)))
    return findings


def read_marker_file(path: Path) -> tuple[bytes, ...]:
    try:
        values = tuple(line.encode("utf-8") for line in path.read_text(encoding="utf-8").splitlines() if line)
    except (OSError, UnicodeError) as error:
        raise ScanError(f"cannot read marker file {path}: {error}") from error
    if not values:
        raise ScanError(f"marker file is empty: {path}")
    return values


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", action="append", type=Path, dest="paths", help="artifact file or directory to scan")
    parser.add_argument("--marker-file", type=Path, help="UTF-8 file containing one seeded marker per line")
    args = parser.parse_args()
    paths = args.paths or [DEFAULT_PATH]
    try:
        markers = read_marker_file(args.marker_file) if args.marker_file else DEFAULT_MARKERS
        files = iter_files(paths)
        findings: list[tuple[Path, int]] = []
        for path in files:
            findings.extend((path, index) for index in sorted(scan_file(path, markers)))
    except ScanError as error:
        return fail(str(error))
    if findings:
        details = ", ".join(f"{path}:marker-{index + 1}" for path, index in findings)
        return fail(f"{len(findings)} seeded marker finding(s): {details}")
    print(f"v1 secret artifact check ok: files={len(files)}, markers={len(markers)}, findings=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
