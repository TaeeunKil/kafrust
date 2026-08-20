"""Check local protocol versions against Apache Kafka 4.3.1 metadata.

The normal snapshot gate checks API identity and version bounds for high-risk
producer, fetch, offset, and modern consumer-group paths. ``--online-all``
extends the same identity and version checks to every local request/response
schema by reading the pinned Apache 4.3.1 tag. Neither mode claims field-level
or byte-for-byte schema parity; those remain separate implementation work.

Use ``--online`` when intentionally refreshing the audit against the pinned
Apache tag. The normal CI path is offline and deterministic.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "docs" / "apache-kafka-4.3.1-schema-metadata.json"
API_DIR = ROOT / "crates" / "kafrust-protocol" / "src" / "api"
API_KEY_RE = re.compile(
    r"\bpub const [A-Z0-9_]*API_KEY\s*:\s*i16\s*=\s*(-?\d+)\s*;"
)
LOCAL_REQUEST_RE = re.compile(
    r"^\s*pub (?:struct|type) ([A-Z][A-Za-z0-9]+Request)(?:V\d+)?\b",
    re.MULTILINE,
)


def fail(message: str) -> int:
    print(f"Apache schema version check failed: {message}", file=sys.stderr)
    return 1


def parse_range(value: str) -> tuple[int, int]:
    if value.endswith("+"):
        start = int(value[:-1])
        return start, start
    start, end = value.split("-", maxsplit=1)
    return int(start), int(end)


def parse_online_range(value: str) -> tuple[int, int | None]:
    """Parse Apache's closed or open-ended version ranges."""
    if value.endswith("+"):
        return int(value[:-1]), None
    if "-" not in value:
        version = int(value)
        return version, version
    start, end = value.split("-", maxsplit=1)
    return int(start), int(end)


def strip_json_comments(text: str) -> str:
    return re.sub(r"^\s*//.*$", "", text, flags=re.MULTILINE)


def load_snapshot() -> dict[str, object]:
    try:
        value = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read {SNAPSHOT.relative_to(ROOT)}: {error}") from error
    if not isinstance(value, dict) or not isinstance(value.get("schemas"), list):
        raise ValueError("snapshot must contain a top-level schemas array")
    return value


def fetch_schema(snapshot: dict[str, object], name: str) -> dict[str, object]:
    template = snapshot.get("source_url_template")
    if not isinstance(template, str):
        raise ValueError("snapshot is missing source_url_template")
    request = Request(template.format(name=name), headers={"User-Agent": "kafrust-schema-check"})
    with urlopen(request, timeout=20) as response:  # noqa: S310 - pinned public Apache URL
        payload = response.read().decode("utf-8")
    value = json.loads(strip_json_comments(payload))
    if not isinstance(value, dict):
        raise ValueError(f"Apache schema {name} is not a JSON object")
    return value


def local_schema_versions(source_text: str, name: str) -> list[int]:
    return [
        int(value)
        for value in re.findall(rf"\b{re.escape(name)}V(\d+)\b", source_text)
    ]


def online_all(snapshot: dict[str, object]) -> int:
    """Audit every top-level local request and its matching response schema."""
    errors: list[str] = []
    coverage_notes: list[str] = []
    checked = 0
    for source in sorted(API_DIR.glob("*.rs")):
        if source.name == "mod.rs":
            continue
        source_text = source.read_text(encoding="utf-8")
        local_keys = {int(value) for value in API_KEY_RE.findall(source_text)}
        request_names = sorted(set(LOCAL_REQUEST_RE.findall(source_text)))
        if not request_names:
            errors.append(f"{source.stem}: no top-level request type found")
            continue

        for request_name in request_names:
            for message_name, message_type in (
                (request_name, "request"),
                (request_name.removesuffix("Request") + "Response", "response"),
            ):
                try:
                    actual = fetch_schema(snapshot, message_name)
                except Exception as error:  # noqa: BLE001 - report audit failures clearly
                    errors.append(f"{source.stem}/{message_name}: online fetch failed: {error}")
                    continue

                if actual.get("name") != message_name:
                    errors.append(
                        f"{source.stem}/{message_name}: Apache name is {actual.get('name')!r}"
                    )
                if actual.get("type") != message_type:
                    errors.append(
                        f"{source.stem}/{message_name}: Apache type is "
                        f"{actual.get('type')!r}, expected {message_type!r}"
                    )
                api_key = actual.get("apiKey")
                if not isinstance(api_key, int) or api_key not in local_keys:
                    errors.append(
                        f"{source.stem}/{message_name}: Apache API key {api_key!r} "
                        f"is not declared locally ({sorted(local_keys)})"
                    )

                local_versions = local_schema_versions(source_text, message_name)
                valid_versions = actual.get("validVersions")
                if local_versions and isinstance(valid_versions, str):
                    valid_min, upstream_max = parse_online_range(valid_versions)
                    local_max = max(local_versions)
                    if local_max < valid_min:
                        errors.append(
                            f"{source.stem}/{message_name}: local max v{local_max} "
                            f"is below Apache minimum v{valid_min}"
                        )
                    if upstream_max is not None and local_max > upstream_max:
                        coverage_notes.append(
                            f"{source.stem}/{message_name}: local max v{local_max} "
                            f"is newer than pinned Apache max v{upstream_max}"
                        )

                    flexible_versions = actual.get("flexibleVersions", "none")
                    if isinstance(flexible_versions, str) and flexible_versions != "none":
                        flexible_min, _ = parse_online_range(flexible_versions)
                        if not any(version >= flexible_min for version in local_versions):
                            coverage_notes.append(
                                f"{source.stem}/{message_name}: no local version reaches "
                                f"Apache flexible boundary v{flexible_min}"
                            )
                checked += 1

    if errors:
        return fail("; ".join(errors))
    print(
        f"Apache schema online audit ok: Kafka 4.3.1, "
        f"{checked} request/response schemas across local protocol modules"
    )
    for note in coverage_notes:
        print(f"schema coverage note: {note}")
    return 0


def compare_upstream(expected: dict[str, object], actual: dict[str, object]) -> list[str]:
    mismatches: list[str] = []
    for field in ("name", "apiKey", "type", "validVersions", "flexibleVersions"):
        expected_field = {
            "apiKey": "api_key",
            "validVersions": "valid_versions",
            "flexibleVersions": "flexible_versions",
        }.get(field, field)
        if expected.get(expected_field) != actual.get(field):
            mismatches.append(
                f"{field}: snapshot={expected.get(expected_field)!r}, "
                f"Apache={actual.get(field)!r}"
            )
    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--online",
        action="store_true",
        help="fetch each schema from the pinned Apache Kafka tag before checking it",
    )
    parser.add_argument(
        "--online-all",
        action="store_true",
        help="audit every local request/response schema against the pinned Apache tag",
    )
    args = parser.parse_args()

    try:
        snapshot = load_snapshot()
    except ValueError as error:
        return fail(str(error))

    entries = snapshot["schemas"]
    assert isinstance(entries, list)
    if not entries:
        return fail("snapshot contains no schemas")

    if args.online_all:
        try:
            return online_all(snapshot)
        except OSError as error:
            return fail(f"cannot read local protocol source: {error}")

    errors: list[str] = []
    lagging: list[str] = []
    checked = 0
    for entry in entries:
        if not isinstance(entry, dict):
            errors.append("schema entry is not an object")
            continue
        required = {
            "name",
            "module",
            "api_key",
            "type",
            "valid_versions",
            "flexible_versions",
        }
        missing = sorted(required - set(entry))
        if missing:
            errors.append(f"schema entry is missing fields: {missing}")
            continue

        name = entry["name"]
        module = entry["module"]
        if not isinstance(name, str) or not isinstance(module, str):
            errors.append("schema name and module must be strings")
            continue
        source = API_DIR / f"{module}.rs"
        if not source.is_file():
            errors.append(f"{name}: missing local source {source.relative_to(ROOT)}")
            continue
        source_text = source.read_text(encoding="utf-8")
        local_keys = {int(value) for value in API_KEY_RE.findall(source_text)}
        if entry["api_key"] not in local_keys:
            errors.append(
                f"{name}: local API keys {sorted(local_keys)} do not contain "
                f"snapshot key {entry['api_key']}"
            )

        local_versions = [
            int(value)
            for value in re.findall(rf"\b{re.escape(name)}V(\d+)\b", source_text)
        ]
        if not local_versions:
            errors.append(f"{name}: no local {name}V<version> type found")
            continue

        local_max = max(local_versions)
        valid_min, upstream_max = parse_range(str(entry["valid_versions"]))
        if local_max > upstream_max:
            errors.append(
                f"{name}: local max v{local_max} exceeds Apache max v{upstream_max}"
            )
        if local_max < valid_min:
            errors.append(
                f"{name}: local max v{local_max} is below Apache minimum v{valid_min}"
            )

        flexible_min, _ = parse_range(str(entry["flexible_versions"]))
        if not any(version >= flexible_min for version in local_versions):
            errors.append(
                f"{name}: no local version reaches Apache flexible boundary "
                f"v{flexible_min}"
            )
        elif local_max < upstream_max:
            lagging.append(
                f"{name} local max v{local_max}, Apache stable range ends at v{upstream_max}"
            )

        if args.online:
            try:
                actual = fetch_schema(snapshot, name)
            except Exception as error:  # noqa: BLE001 - report URL audit failures clearly
                errors.append(f"{name}: online fetch failed: {error}")
            else:
                errors.extend(f"{name}: {mismatch}" for mismatch in compare_upstream(entry, actual))
        checked += 1

    if errors:
        return fail("; ".join(errors))

    print(
        f"Apache schema metadata ok: Kafka 4.3.1 snapshot, "
        f"{checked} schemas, local implementations within official bounds"
    )
    for item in lagging:
        print(f"schema coverage note: {item}")
    if args.online:
        print("Apache schema metadata online audit ok: pinned source matches snapshot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
