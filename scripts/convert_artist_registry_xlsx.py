#!/usr/bin/env python3
"""Convert the supplied artist seed workbook to the canonical JSON snapshot.

The converter intentionally uses only the Python standard library.  XLSX files
are ZIP containers, so this keeps the transformation reproducible in the
repository without making openpyxl a runtime dependency.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET
from zipfile import ZipFile


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"x": MAIN_NS, "r": REL_NS, "p": PACKAGE_REL_NS}

REGISTRY_SHEET = "Registry_Batch_001"
README_SHEET = "README"
EXPECTED_COLUMNS = [
    "Registry_ID",
    "Batch",
    "Entity_Type",
    "Display_Name",
    "Canonical_Name",
    "Alias_Examples",
    "Primary_Domain",
    "Source_Bucket",
    "Source_URL",
    "Verification_Status",
    "Monitoring_Priority",
    "Ambiguity_Flag",
    "Match_Key",
    "Token_Sort_Key",
    "Lookup_URL",
    "Wikidata_QID",
    "Is_Active_Confirmed",
    "Last_Verified_At",
    "Notes",
]


def _column_index(cell_reference: str) -> int:
    match = re.match(r"[A-Z]+", cell_reference)
    if not match:
        raise ValueError(f"Invalid XLSX cell reference: {cell_reference!r}")
    value = 0
    for char in match.group(0):
        value = value * 26 + ord(char) - ord("A") + 1
    return value - 1


def _shared_strings(archive: ZipFile) -> list[str]:
    try:
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    return [
        "".join(node.text or "" for node in item.iterfind(".//x:t", NS))
        for item in root.findall("x:si", NS)
    ]


def _sheet_paths(archive: ZipFile) -> dict[str, str]:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    targets = {
        item.attrib["Id"]: item.attrib["Target"].lstrip("/")
        for item in relationships.findall("p:Relationship", NS)
    }
    paths: dict[str, str] = {}
    for sheet in workbook.findall(".//x:sheet", NS):
        relationship_id = sheet.attrib[f"{{{REL_NS}}}id"]
        paths[sheet.attrib["name"]] = targets[relationship_id]
    return paths


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    kind = cell.attrib.get("t")
    value = cell.find("x:v", NS)
    if kind == "inlineStr":
        inline = cell.find("x:is", NS)
        if inline is None:
            return ""
        return "".join(node.text or "" for node in inline.iterfind(".//x:t", NS))
    if value is None or value.text is None:
        return ""
    if kind == "s":
        return shared_strings[int(value.text)]
    return value.text


def _read_sheet(
    archive: ZipFile, path: str, shared_strings: list[str]
) -> list[list[str]]:
    root = ET.fromstring(archive.read(path))
    sparse_rows: list[dict[int, str]] = []
    width = 0
    for row in root.findall(".//x:sheetData/x:row", NS):
        values: dict[int, str] = {}
        for cell in row.findall("x:c", NS):
            index = _column_index(cell.attrib["r"])
            values[index] = _cell_value(cell, shared_strings)
            width = max(width, index + 1)
        sparse_rows.append(values)
    return [[row.get(index, "") for index in range(width)] for row in sparse_rows]


def load_workbook_tables(path: Path) -> dict[str, list[list[str]]]:
    with ZipFile(path) as archive:
        shared_strings = _shared_strings(archive)
        sheet_paths = _sheet_paths(archive)
        return {
            name: _read_sheet(archive, sheet_path, shared_strings)
            for name, sheet_path in sheet_paths.items()
        }


def _nullable(value: str) -> str | None:
    stripped = value.strip()
    return stripped or None


def _nullable_bool(value: str) -> bool | None:
    normalized = value.strip().casefold()
    if not normalized:
        return None
    if normalized in {"1", "true", "yes", "да"}:
        return True
    if normalized in {"0", "false", "no", "нет"}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def _split_semicolon(value: str) -> list[str]:
    return [item.strip() for item in value.split(";") if item.strip()]


def _rows_as_dicts(rows: list[list[str]]) -> list[dict[str, str]]:
    if not rows:
        raise ValueError("Registry sheet is empty")
    headers = rows[0]
    if headers != EXPECTED_COLUMNS:
        raise ValueError(
            "Unexpected registry columns:\n"
            f"expected={EXPECTED_COLUMNS!r}\nactual={headers!r}"
        )
    return [dict(zip(headers, row, strict=True)) for row in rows[1:]]


def _validate_rows(rows: list[dict[str, str]]) -> None:
    required = [
        "Registry_ID",
        "Display_Name",
        "Canonical_Name",
        "Source_Bucket",
        "Source_URL",
        "Match_Key",
        "Token_Sort_Key",
    ]
    for index, row in enumerate(rows, start=2):
        missing = [field for field in required if not row[field].strip()]
        if missing:
            raise ValueError(f"Workbook row {index} lacks required fields: {missing}")

    identifiers = [row["Registry_ID"] for row in rows]
    duplicate_ids = sorted(key for key, count in Counter(identifiers).items() if count > 1)
    if duplicate_ids:
        raise ValueError(f"Duplicate Registry_ID values: {duplicate_ids}")

    duplicate_keys = {
        key
        for key, count in Counter(row["Match_Key"] for row in rows).items()
        if count > 1
    }
    incorrectly_unflagged = [
        row["Registry_ID"]
        for row in rows
        if row["Match_Key"] in duplicate_keys
        and row["Ambiguity_Flag"] != "duplicate_match_key_review"
    ]
    if incorrectly_unflagged:
        raise ValueError(
            "Duplicate match keys must fail closed; unflagged rows: "
            f"{incorrectly_unflagged}"
        )


def _read_readme(rows: list[list[str]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        if len(row) >= 2 and row[0]:
            result[row[0]] = row[1]
    return result


def build_snapshot(path: Path) -> dict[str, Any]:
    tables = load_workbook_tables(path)
    if REGISTRY_SHEET not in tables or README_SHEET not in tables:
        raise ValueError(
            f"Workbook must contain {README_SHEET!r} and {REGISTRY_SHEET!r}"
        )

    readme = _read_readme(tables[README_SHEET])
    rows = _rows_as_dicts(tables[REGISTRY_SHEET])
    _validate_rows(rows)

    match_key_counts = Counter(row["Match_Key"] for row in rows)
    source_counts = Counter(row["Source_Bucket"] for row in rows)
    priority_counts = Counter(row["Monitoring_Priority"] for row in rows)
    verification_counts = Counter(row["Verification_Status"] for row in rows)

    entities: list[dict[str, Any]] = []
    for row in rows:
        entity_type = {
            "person": "person",
            "person/stage_name": "person_stage_name",
        }.get(row["Entity_Type"], row["Entity_Type"])
        entity = {
            "registry_id": row["Registry_ID"],
            "batch": row["Batch"],
            "entity_type": entity_type,
            "display_name": row["Display_Name"],
            "canonical_name": row["Canonical_Name"],
            "aliases": _split_semicolon(row["Alias_Examples"]),
            "primary_domain": row["Primary_Domain"],
            "monitoring_priority": row["Monitoring_Priority"],
            "matching": {
                "match_key": row["Match_Key"],
                "token_sort_key": row["Token_Sort_Key"],
                "ambiguity_flags": _split_semicolon(row["Ambiguity_Flag"]),
                "duplicate_match_key": match_key_counts[row["Match_Key"]] > 1,
            },
            "identity_evidence": {
                "seed_source_bucket": row["Source_Bucket"],
                "seed_source_url": row["Source_URL"],
                "lookup_url": row["Lookup_URL"],
                "wikidata_qid": _nullable(row["Wikidata_QID"]),
                "verification_status": row["Verification_Status"],
                "active_confirmed": _nullable_bool(row["Is_Active_Confirmed"]),
                "last_verified_at": _nullable(row["Last_Verified_At"]),
                "notes": row["Notes"],
                "evidence_level": "bucket_seed",
            },
            # The workbook has no home/base geography.  Explicit unknowns make
            # it impossible for a consumer to treat list membership as proof
            # that an artist is non-local.
            "locality": {
                "status": "unknown",
                "country_code": None,
                "region_code": None,
                "city": None,
                "basis": None,
                "evidence": [],
                "verified_at": None,
                "valid_until": None,
            },
        }
        entities.append(entity)

    workbook_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    qid_count = sum(bool(row["Wikidata_QID"].strip()) for row in rows)
    active_count = sum(bool(row["Is_Active_Confirmed"].strip()) for row in rows)
    verified_at_count = sum(bool(row["Last_Verified_At"].strip()) for row in rows)
    duplicate_groups = sum(1 for count in match_key_counts.values() if count > 1)
    duplicate_rows = sum(count for count in match_key_counts.values() if count > 1)

    return {
        "schema_version": "kenigevents.artist_visit_registry.v1",
        "source": {
            "artifact_name": path.name,
            "sha256": workbook_hash,
            "workbook_generated_at": _nullable(readme.get("Generated_at", "")),
            "sheet": REGISTRY_SHEET,
            "batch": rows[0]["Batch"] if rows else None,
            "artifact_committed": False,
        },
        "safety_contract": {
            "purpose": "identity candidate recall for event participant matching",
            "list_membership_proves_non_locality": False,
            "absence_proves_non_locality": False,
            "locality_requires_row_level_evidence": True,
            "production_auto_publish_ready": False,
        },
        "profile": {
            "entity_count": len(rows),
            "unique_registry_id_count": len({row["Registry_ID"] for row in rows}),
            "unique_match_key_count": len(match_key_counts),
            "duplicate_match_key_group_count": duplicate_groups,
            "duplicate_match_key_row_count": duplicate_rows,
            "alias_enriched_entity_count": sum(bool(row["Alias_Examples"].strip()) for row in rows),
            "row_level_wikidata_qid_count": qid_count,
            "active_confirmed_count": active_count,
            "last_verified_at_count": verified_at_count,
            "source_bucket_counts": dict(sorted(source_counts.items())),
            "monitoring_priority_counts": dict(sorted(priority_counts.items())),
            "verification_status_counts": dict(sorted(verification_counts.items())),
        },
        "entities": entities,
    }


def render_snapshot(snapshot: dict[str, Any]) -> bytes:
    return (
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    ).encode("utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Source XLSX workbook")
    parser.add_argument("output", type=Path, help="Canonical JSON destination")
    parser.add_argument(
        "--expected-sha256",
        help="Fail when the input workbook does not match this provenance hash",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Compare generated bytes with output instead of writing it",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    actual_hash = hashlib.sha256(args.input.read_bytes()).hexdigest()
    if args.expected_sha256 and actual_hash != args.expected_sha256:
        raise SystemExit(
            f"Input SHA-256 mismatch: expected {args.expected_sha256}, got {actual_hash}"
        )

    rendered = render_snapshot(build_snapshot(args.input))
    if args.check:
        if not args.output.exists():
            raise SystemExit(f"Canonical output does not exist: {args.output}")
        if args.output.read_bytes() != rendered:
            raise SystemExit("Canonical output is stale; rerun converter without --check")
        print(f"OK: {args.output} is reproducible from {args.input}")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(rendered)
    print(f"Wrote {args.output} ({len(rendered)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
