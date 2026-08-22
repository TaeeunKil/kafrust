#!/usr/bin/env python3
"""Generate and verify the reviewed public API classification snapshot.

Generation uses rustdoc's JSON output because it is the only reliable source
for associated methods and re-exported symbols. Verification is deliberately
toolchain-independent: CI checks the committed snapshot against the source
root surface and a digest of every public declaration line.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
CRATE_SOURCE = ROOT / "crates" / "kafrust" / "src"
SNAPSHOT = ROOT / "docs" / "evidence" / "public-api-snapshot.json"
RUSTDOC_JSON = ROOT / "target" / "doc" / "kafrust.json"
SCHEMA_VERSION = 1
CLASSIFICATIONS = {"stable", "expert", "experimental", "excluded"}

OWNER_BY_MODULE = {
    "admin": "V1-13",
    "blocking": "V1-21",
    "client": "V1-07",
    "config": "V1-03",
    "consumer": "V1-05",
    "error": "V1-02",
    "group": "V1-08",
    "metrics": "V1-19",
    "producer": "V1-04",
    "protocol": "V1-02",
    "share_consumer": "V1-10",
    "streams": "V1-11",
    "telemetry": "V1-19",
}


def public_declaration_digest() -> str:
    """Hash public declaration lines without depending on line numbers."""

    rows: list[str] = []
    for path in sorted(CRATE_SOURCE.rglob("*.rs")):
        relative = path.relative_to(ROOT).as_posix()
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped.startswith("pub "):
                continue
            if stripped.startswith(("pub(crate)", "pub(super)", "pub(in ")):
                continue
            rows.append(f"{relative}\t{re.sub(r'\\s+', ' ', stripped)}")
    digest = hashlib.sha256()
    digest.update("\n".join(rows).encode("utf-8"))
    return digest.hexdigest()


def _feature(attrs: list[dict[str, Any]], inherited: str | None) -> str | None:
    for attr in attrs:
        text = attr.get("other", "") if isinstance(attr, dict) else str(attr)
        match = re.search(r'name: "feature".*?Some\("([^"]+)"\)', text)
        if match:
            return match.group(1)
    return inherited


def _item_kind(item: dict[str, Any]) -> str:
    inner = item.get("inner", {})
    if not inner:
        return "unknown"
    return next(iter(inner))


def _name(item: dict[str, Any]) -> str | None:
    if item.get("name"):
        return item["name"]
    use = item.get("inner", {}).get("use")
    return use.get("name") if use else None


def _source(item: dict[str, Any]) -> str | None:
    use = item.get("inner", {}).get("use")
    if use:
        return use.get("source")
    span = item.get("span")
    if span:
        return str(span.get("filename", "")).replace("\\", "/")
    return None


def _line(item: dict[str, Any]) -> int | None:
    span = item.get("span")
    return span.get("begin", [None])[0] if span else None


def _module_for(path: str, source: str | None) -> str:
    if path.startswith("kafrust::protocol") or (source or "").startswith("kafrust_protocol"):
        return "protocol"
    parts = path.split("::")
    if len(parts) > 1 and parts[0] == "kafrust":
        return parts[1]
    if source:
        return source.split("::", 1)[0]
    return "error"


def _classification(module: str) -> str:
    if module == "protocol":
        return "excluded"
    if module in {"streams", "share_consumer", "telemetry", "admin"}:
        return "experimental"
    if module in {"client", "blocking", "metrics"}:
        return "expert"
    return "stable"


def _symbol(path: str, item: dict[str, Any], feature: str | None, surface: str) -> dict[str, Any] | None:
    name = _name(item)
    if not name or item.get("visibility") != "public":
        return None
    source = _source(item)
    module = _module_for(path, source)
    return {
        "path": path,
        "kind": _item_kind(item),
        "surface": surface,
        "feature": feature,
        "source": source,
        "line": _line(item),
        "classification": _classification(module),
        "owner": OWNER_BY_MODULE.get(module, "V1-02"),
    }


def _walk_impls(
    item_id: int,
    parent_path: str,
    feature: str | None,
    index: dict[str, dict[str, Any]],
    symbols: list[dict[str, Any]],
    seen: set[tuple[str, str, str]],
) -> None:
    item = index.get(str(item_id))
    if not item or item.get("crate_id") != 0:
        return
    inner = item.get("inner", {})
    for impl_id in inner.get("struct", {}).get("impls", []) + inner.get("enum", {}).get("impls", []):
        impl = index.get(str(impl_id), {})
        for child_id in impl.get("inner", {}).get("impl", {}).get("items", []):
            child = index.get(str(child_id), {})
            if child.get("crate_id") != 0 or child.get("visibility") != "public":
                continue
            symbol = _symbol(f"{parent_path}::{_name(child)}", child, feature, "associated")
            if symbol:
                key = (symbol["path"], symbol["kind"], symbol["source"] or "")
                if key not in seen:
                    seen.add(key)
                    symbols.append(symbol)


def _walk_module(
    module_id: int,
    prefix: str,
    inherited_feature: str | None,
    surface: str,
    index: dict[str, dict[str, Any]],
    symbols: list[dict[str, Any]],
    seen: set[tuple[str, str, str],],
) -> None:
    module = index.get(str(module_id), {})
    module_feature = _feature(module.get("attrs", []), inherited_feature)
    for child_id in module.get("inner", {}).get("module", {}).get("items", []):
        child = index.get(str(child_id), {})
        if child.get("crate_id") != 0 or child.get("visibility") != "public":
            continue
        name = _name(child)
        if not name:
            continue
        path = f"{prefix}::{name}"
        child_feature = _feature(child.get("attrs", []), module_feature)
        child_surface = "root" if prefix == "kafrust" else surface
        symbol = _symbol(path, child, child_feature, child_surface)
        if symbol:
            key = (symbol["path"], symbol["kind"], symbol["source"] or "")
            if key not in seen:
                seen.add(key)
                symbols.append(symbol)
        kind = _item_kind(child)
        if kind == "module":
            _walk_module(child_id, path, child_feature, "module", index, symbols, seen)
        elif kind in {"struct", "enum"}:
            _walk_impls(child_id, path, child_feature, index, symbols, seen)


def _root_surface(symbols: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    modules = sorted(
        (
            {"name": item["path"].split("::")[-1], "feature": item["feature"]}
            for item in symbols
            if item["surface"] == "root" and item["kind"] == "module"
        ),
        key=lambda item: item["name"],
    )
    exports = sorted(
        (
            {
                "name": item["path"].split("::")[-1],
                "kind": item["kind"],
                "feature": item["feature"],
                "source": item["source"],
            }
            for item in symbols
            if item["surface"] == "root" and item["kind"] != "module"
        ),
        key=lambda item: (item["name"], item["kind"], item["source"] or ""),
    )
    return modules, exports


def generate(input_path: Path) -> dict[str, Any]:
    data = json.loads(input_path.read_text(encoding="utf-8"))
    index = data["index"]
    root_id = str(data["root"])
    symbols: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    _walk_module(root_id, "kafrust", None, "root", index, symbols, seen)
    symbols.sort(key=lambda item: (item["path"], item["kind"], item["source"] or ""))
    modules, exports = _root_surface(symbols)
    counts = Counter(item["classification"] for item in symbols)
    return {
        "schema_version": SCHEMA_VERSION,
        "crate": "kafrust",
        "crate_version": data.get("crate_version"),
        "feature_set": ["all-features"],
        "generator": "rustdoc-json",
        "root_modules": modules,
        "root_exports": exports,
        "counts": dict(sorted(counts.items())),
        "public_symbol_count": len(symbols),
        "public_declaration_sha256": public_declaration_digest(),
        "symbols": symbols,
    }


def _root_source_surface() -> tuple[set[str], set[str]]:
    text = (CRATE_SOURCE / "lib.rs").read_text(encoding="utf-8")
    modules = set(re.findall(r"^\s*pub\s+mod\s+([A-Za-z_]\w*)\s*;", text, re.MULTILINE))
    exports: set[str] = set(re.findall(r"^\s*pub\s+fn\s+([A-Za-z_]\w*)", text, re.MULTILINE))
    for match in re.finditer(r"^\s*pub\s+use\s+([\s\S]*?);", text, re.MULTILINE):
        statement = re.sub(r"//.*", "", match.group(1))
        if "{" not in statement:
            name = statement.split(" as ")[-1].strip().split("::")[-1]
            if name:
                exports.add(name)
            continue
        body = statement.split("{", 1)[1].rsplit("}", 1)[0]
        for entry in body.split(","):
            entry = entry.strip()
            if not entry:
                continue
            exports.add(entry.split(" as ")[-1].strip().split("::")[-1])
    return modules, exports


def check() -> None:
    snapshot = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported public API snapshot schema")
    if snapshot.get("public_declaration_sha256") != public_declaration_digest():
        raise ValueError(
            "public declaration digest changed; regenerate docs/evidence/public-api-snapshot.json"
        )
    source_modules, source_exports = _root_source_surface()
    snapshot_modules = {item["name"] for item in snapshot["root_modules"]}
    snapshot_exports = {item["name"] for item in snapshot["root_exports"]}
    if source_modules != snapshot_modules:
        raise ValueError(f"root modules differ: source={sorted(source_modules)} snapshot={sorted(snapshot_modules)}")
    if source_exports != snapshot_exports:
        raise ValueError(f"root exports differ: source={sorted(source_exports)} snapshot={sorted(snapshot_exports)}")
    symbols = snapshot.get("symbols", [])
    if snapshot.get("public_symbol_count") != len(symbols):
        raise ValueError("public symbol count does not match snapshot")
    for symbol in symbols:
        if symbol.get("classification") not in CLASSIFICATIONS:
            raise ValueError(f"invalid classification for {symbol.get('path')}")
        if not re.fullmatch(r"V1-\d{2}", symbol.get("owner", "")):
            raise ValueError(f"invalid owner for {symbol.get('path')}")
    actual_counts = dict(sorted(Counter(item["classification"] for item in symbols).items()))
    if actual_counts != snapshot.get("counts"):
        raise ValueError("classification counts do not match snapshot")
    print(
        f"public API snapshot ok: {len(symbols)} symbols, "
        f"{len(snapshot_modules)} modules, {len(snapshot_exports)} root exports"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--input", type=Path, default=RUSTDOC_JSON)
    args = parser.parse_args()
    try:
        if args.generate:
            SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
            SNAPSHOT.write_text(
                json.dumps(generate(args.input), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            print(f"wrote {SNAPSHOT.relative_to(ROOT)}")
        else:
            check()
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"public API snapshot error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
