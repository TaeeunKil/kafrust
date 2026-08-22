"""Check the reviewed v1 data-plane version and header manifest."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "evidence" / "data-plane-version-manifest.json"
METADATA = ROOT / "docs" / "apache-kafka-4.3.1-schema-metadata.json"
API_DIR = ROOT / "crates" / "kafrust-protocol" / "src" / "api"
CLIENT = ROOT / "crates" / "kafrust" / "src" / "client.rs"

PINNED_APACHE_FALLBACK = {
    "ListOffsets": {"valid_versions": "1-11", "flexible_versions": "6+"},
    "Metadata": {"valid_versions": "0-13", "flexible_versions": "9+"},
    "ApiVersions": {"valid_versions": "0-4", "flexible_versions": "3+"},
    "OffsetForLeaderEpoch": {"valid_versions": "2-4", "flexible_versions": "4+"},
}


def fail(message: str) -> int:
    print(f"data-plane manifest check failed: {message}", file=sys.stderr)
    return 1


def main() -> int:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        metadata = json.loads(METADATA.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return fail(str(error))

    if manifest.get("schema_version") != 1 or not isinstance(manifest.get("apis"), list):
        return fail("manifest schema is invalid")
    official = {entry.get("name"): entry for entry in metadata.get("schemas", [])}
    client_text = CLIENT.read_text(encoding="utf-8")
    checked = 0
    for entry in manifest["apis"]:
        required = {"name", "api_key", "module", "selected_high_level", "legacy_or_low_level", "source_types"}
        missing = sorted(required - set(entry))
        if missing:
            return fail(f"{entry.get('name')}: missing {missing}")
        name = entry["name"]
        module_path = API_DIR / f"{entry['module']}.rs"
        if not module_path.is_file():
            return fail(f"{name}: missing {module_path.relative_to(ROOT)}")
        source = module_path.read_text(encoding="utf-8")
        api_key = re.search(r"pub const API_KEY: i16 = (-?\d+);", source)
        if not api_key or int(api_key.group(1)) != entry["api_key"]:
            return fail(f"{name}: API key does not match local source")
        for source_type in entry["source_types"]:
            if not re.search(rf"\b{re.escape(source_type)}\b", source):
                return fail(f"{name}: missing local {source_type}")
        upstream = official.get(f"{name}Request") or PINNED_APACHE_FALLBACK.get(name)
        if not upstream:
            return fail(f"{name}: missing pinned Apache metadata")
        valid = upstream.get("valid_versions", "")
        minimum = int(valid.split("-", 1)[0])
        maximum = int(valid.rstrip("+").split("-", 1)[-1]) if "-" in valid else None
        for version in entry["selected_high_level"]:
            if version < minimum or (maximum is not None and version > maximum):
                return fail(f"{name}: selected v{version} is outside Apache {valid}")
        if entry.get("transaction_version_owner") and entry["transaction_version_owner"] != "V1-06":
            return fail(f"{name}: transaction selection must remain owned by V1-06")
        if name == "Produce" and entry.get("transactional_high_level_cap") != 11:
            return fail("Produce: transactional high-level cap must remain v11 until V1-06 qualifies TV2")
        if entry.get("request_header", {}).get("non_flexible") == 1 and ".encode_v1" not in source:
            return fail(f"{name}: non-flexible request header v1 is not encoded in source")
        if entry.get("request_header", {}).get("flexible") == 2 and ".encode_v2" not in source:
            return fail(f"{name}: flexible request header v2 is not encoded in source")
        for response_header in set(entry.get("response_header", {}).values()):
            marker = f"ResponseHeader::decode_v{response_header}"
            if marker not in client_text:
                return fail(f"{name}: client has no {marker} response-header path")
        checked += 1

    print(f"data-plane manifest ok: {checked} APIs, Kafka 4.3.1 metadata and header paths checked")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
