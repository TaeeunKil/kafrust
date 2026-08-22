#!/usr/bin/env python3
"""Verify that the published crate boundary is independent of the workspace.

The staged check packages both crates, unpacks those exact tarballs outside the
repository, and compiles fresh external projects against the unpacked package
artifacts.  The protocol dependency is supplied through a package-only Cargo
patch; no workspace source path is involved in the fixture.

The regression check intentionally compiles a small external project against
the immutable 0.3.5 protocol artifact and asserts the transaction types that
the current client needs are absent.  It protects the reason for the
coordinated 0.3.6 version bump from being silently forgotten.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import textwrap
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL_MANIFEST = ROOT / "crates" / "kafrust-protocol" / "Cargo.toml"
CLIENT_MANIFEST = ROOT / "crates" / "kafrust" / "Cargo.toml"
MISSING_035_TYPES = (
    "AddOffsetsToTxnRequestV3",
    "AddOffsetsToTxnResponseV3",
    "AddPartitionsToTxnRequestV3",
    "AddPartitionsToTxnResponseV3",
    "EndTxnRequestV3",
    "EndTxnResponseV3",
    "InitProducerIdRequestV2",
    "InitProducerIdResponseV2",
)
FEATURE_PROFILES = {
    "default": (),
    "tls": ("tls",),
    "blocking": ("blocking",),
    "otlp": ("otlp",),
    "all": ("blocking", "tls", "otlp"),
}


def run(command: Sequence[str], *, cwd: Path, expected: int = 0) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    result = subprocess.run(
        list(command),
        cwd=cwd,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    print(result.stdout, end="", flush=True)
    if result.returncode != expected:
        raise RuntimeError(
            f"command exited {result.returncode}, expected {expected}: {' '.join(command)}"
        )
    return result


def package_version(manifest: Path) -> str:
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if line.startswith("version = "):
            return line.split('"', 2)[1]
    raise RuntimeError(f"could not read package version from {manifest}")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_path(package_root: Path, name: str, version: str) -> Path:
    path = package_root / "target" / "package" / f"{name}-{version}.crate"
    if not path.is_file():
        raise RuntimeError(f"Cargo did not produce the expected package: {path}")
    return path


def safe_extract(archive: Path, destination: Path) -> Path:
    with tarfile.open(archive, "r:gz") as tar:
        members = tar.getmembers()
        for member in members:
            member_path = Path(member.name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise RuntimeError(f"unsafe path in {archive.name}: {member.name}")
            if any(part in {".git", "target"} for part in member_path.parts):
                raise RuntimeError(f"unexpected build metadata in {archive.name}: {member.name}")
            if member_path.name in {".env", ".envrc"} or member_path.suffix in {".pem", ".key"}:
                raise RuntimeError(f"possible secret in {archive.name}: {member.name}")
        try:
            tar.extractall(destination, filter="data")
        except TypeError:
            tar.extractall(destination)
    top_level = destination / archive.name.removesuffix(".crate")
    if not top_level.is_dir():
        raise RuntimeError(f"unexpected package layout in {archive.name}")
    return top_level


def write_fixture(fixture: Path, client_dir: Path, protocol_dir: Path, features: Sequence[str]) -> None:
    feature_text = ", ".join(f'"{feature}"' for feature in features)
    client_relative = os.path.relpath(client_dir, fixture).replace(os.sep, "/")
    protocol_relative = os.path.relpath(protocol_dir, fixture).replace(os.sep, "/")
    fixture.mkdir(parents=True)
    (fixture / "src").mkdir()
    (fixture / "src" / "main.rs").write_text(
        "fn main() {\n    let _ = kafrust::ClientConfig::new([\"localhost:9092\"]);\n}\n",
        encoding="utf-8",
    )
    (fixture / "Cargo.toml").write_text(
        textwrap.dedent(
            f"""
            [package]
            name = "kafrust-package-boundary-fixture"
            version = "0.0.0"
            edition = "2021"

            [dependencies]
            kafrust = {{ path = "{client_relative}", default-features = false, features = [{feature_text}] }}

            [patch.crates-io]
            kafrust-protocol = {{ path = "{protocol_relative}" }}
            """
        ).lstrip(),
        encoding="utf-8",
    )


def assert_lockfile(fixture: Path, version: str) -> None:
    lockfile = fixture / "Cargo.lock"
    if not lockfile.is_file():
        raise RuntimeError(f"external fixture did not generate {lockfile}")
    lock_text = lockfile.read_text(encoding="utf-8")
    for package in ("kafrust", "kafrust-protocol"):
        marker = f'name = "{package}"'
        if marker not in lock_text:
            raise RuntimeError(f"{package} is missing from the external lockfile")
    for package in ("kafrust", "kafrust-protocol"):
        block_start = lock_text.index(f'name = "{package}"')
        block = lock_text[block_start : lock_text.find("\n\n", block_start)]
        if f'version = "{version}"' not in block:
            raise RuntimeError(f"{package} does not resolve coordinated version {version}")
    root_text = str(ROOT.resolve()).replace("\\", "/")
    if root_text in lock_text.replace("\\", "/"):
        raise RuntimeError("external lockfile resolves a workspace source path")


def staged_check() -> None:
    protocol_version = package_version(PROTOCOL_MANIFEST)
    client_version = package_version(CLIENT_MANIFEST)
    if protocol_version != client_version:
        raise RuntimeError(f"crate versions diverge: protocol={protocol_version}, client={client_version}")

    run(["cargo", "generate-lockfile"], cwd=ROOT)
    with tempfile.TemporaryDirectory(prefix="kafrust-package-boundary-") as temp:
        temp_root = Path(temp)
        run(
            [
                "cargo",
                "package",
                "-p",
                "kafrust-protocol",
                "--all-features",
                "--locked",
                "--allow-dirty",
            ],
            cwd=ROOT,
        )
        run(
            [
                "cargo",
                "package",
                "-p",
                "kafrust",
                "--all-features",
                "--locked",
                "--no-verify",
                "--allow-dirty",
                "--config",
                'patch.crates-io.kafrust-protocol.path="crates/kafrust-protocol"',
            ],
            cwd=ROOT,
        )
        protocol_archive = package_path(ROOT, "kafrust-protocol", protocol_version)
        client_archive = package_path(ROOT, "kafrust", client_version)
        print(f"kafrust-protocol-{protocol_version}.crate sha256={sha256(protocol_archive)}")
        print(f"kafrust-{client_version}.crate sha256={sha256(client_archive)}")
        protocol_dir = safe_extract(protocol_archive, temp_root)
        client_dir = safe_extract(client_archive, temp_root)
        for label, features in FEATURE_PROFILES.items():
            fixture = temp_root / f"fixture-{label}"
            write_fixture(fixture, client_dir, protocol_dir, features)
            run(["cargo", "check", "--manifest-path", str(fixture / "Cargo.toml")], cwd=ROOT)
            run(["cargo", "check", "--manifest-path", str(fixture / "Cargo.toml"), "--locked"], cwd=ROOT)
            tree = run(["cargo", "tree", "--manifest-path", str(fixture / "Cargo.toml"), "--locked"], cwd=ROOT)
            if f"kafrust v{client_version}" not in tree.stdout:
                raise RuntimeError(f"cargo tree omitted kafrust {client_version} for {label}")
            if f"kafrust-protocol v{protocol_version}" not in tree.stdout:
                raise RuntimeError(f"cargo tree omitted kafrust-protocol {protocol_version} for {label}")
            assert_lockfile(fixture, client_version)
            print(f"package profile {label}: passed")


def regression_check() -> None:
    with tempfile.TemporaryDirectory(prefix="kafrust-package-regression-") as temp:
        project = Path(temp)
        (project / "src").mkdir()
        (project / "src" / "main.rs").write_text(
            "use kafrust_protocol::api::add_offsets_to_txn::{AddOffsetsToTxnRequestV3, AddOffsetsToTxnResponseV3};\n"
            "use kafrust_protocol::api::add_partitions_to_txn::{AddPartitionsToTxnRequestV3, AddPartitionsToTxnResponseV3};\n"
            "use kafrust_protocol::api::end_txn::{EndTxnRequestV3, EndTxnResponseV3};\n"
            "use kafrust_protocol::api::init_producer_id::{InitProducerIdRequestV2, InitProducerIdResponseV2};\n"
            "fn main() { let _ = (\n"
            "    std::mem::size_of::<AddOffsetsToTxnRequestV3>(), std::mem::size_of::<AddOffsetsToTxnResponseV3>(),\n"
            "    std::mem::size_of::<AddPartitionsToTxnRequestV3>(), std::mem::size_of::<AddPartitionsToTxnResponseV3>(),\n"
            "    std::mem::size_of::<EndTxnRequestV3>(), std::mem::size_of::<EndTxnResponseV3>(),\n"
            "    std::mem::size_of::<InitProducerIdRequestV2>(), std::mem::size_of::<InitProducerIdResponseV2>(),\n"
            "); }\n",
            encoding="utf-8",
        )
        (project / "Cargo.toml").write_text(
            textwrap.dedent(
                """
                [package]
                name = "kafrust-0-3-5-regression"
                version = "0.0.0"
                edition = "2021"

                [dependencies]
                kafrust-protocol = "=0.3.5"
                """
            ).lstrip(),
            encoding="utf-8",
        )
        result = run(
            [
                "cargo",
                "check",
                "--ignore-rust-version",
                "--manifest-path",
                str(project / "Cargo.toml"),
            ],
            cwd=ROOT,
            expected=101,
        )
        for missing_type in MISSING_035_TYPES:
            if missing_type not in result.stdout:
                raise RuntimeError(f"0.3.5 regression output omitted {missing_type}")
        print("published 0.3.5 protocol regression: reproduced")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staged", action="store_true", help="verify staged package tarballs")
    parser.add_argument("--regression", action="store_true", help="reproduce the published 0.3.5 mismatch")
    args = parser.parse_args()
    if args.staged == args.regression:
        parser.error("choose exactly one of --staged or --regression")
    try:
        if args.staged:
            staged_check()
        else:
            regression_check()
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"package boundary check failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
