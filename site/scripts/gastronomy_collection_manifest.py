#!/usr/bin/env python3
"""Build the local-only, exact-ID gastronomy collection manifest.

This stage consumes checked decisions.  It never classifies event prose and
never calls a provider.  Missing/partial decisions are technical failure, not
evidence for an empty public collection.
"""

from __future__ import annotations

import hashlib
import json
import argparse
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any
from pathlib import Path

SCHEMA_VERSION = "gastronomy-collection-v1"
POLICY_VERSION = "gastronomy_v1"
ROLES = {"core", "co_core"}


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _month_floor(value: date, months: int) -> date:
    index = value.year * 12 + value.month - 1 - months
    year, month = divmod(index, 12)
    return date(year, month + 1, min(value.day, 28))


def _snapshot(manifest: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not manifest:
        return None
    snapshot = {key: value for key, value in manifest.items() if key not in {"last_good", "manifest_hash"}}
    snapshot["manifest_hash"] = stable_hash(snapshot)
    return snapshot


def validate_manifest(
    manifest: Mapping[str, Any], *, catalog_event_ids: Sequence[int], expected_catalog_hash: str
) -> list[str]:
    errors: list[str] = []
    if manifest.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version")
    if manifest.get("policy_version") != POLICY_VERSION:
        errors.append("policy_version")
    if manifest.get("provider_calls") != 0:
        errors.append("provider_calls_must_be_zero")
    if manifest.get("catalog_hash") != expected_catalog_hash:
        errors.append("catalog_hash_mismatch")
    catalog_ids = {int(value) for value in catalog_event_ids}
    seen_ids: set[int] = set()
    seen_families: set[str] = set()
    for item in manifest.get("accepted") or []:
        if not isinstance(item, Mapping):
            errors.append("invalid_accepted_item")
            continue
        event_id = item.get("event_id")
        family_id = str(item.get("family_id") or "")
        if not isinstance(event_id, int) or event_id not in catalog_ids:
            errors.append("unknown_event_id")
            continue
        if event_id in seen_ids:
            errors.append("duplicate_event_id")
        if not family_id or family_id in seen_families:
            errors.append("duplicate_or_missing_family_id")
        if item.get("role") not in ROLES:
            errors.append("invalid_role")
        if item.get("occurrence") not in {"future", "recent"}:
            errors.append("invalid_occurrence")
        seen_ids.add(event_id)
        seen_families.add(family_id)
    claimed = manifest.get("manifest_hash")
    unhashed = {key: value for key, value in manifest.items() if key != "manifest_hash"}
    if claimed and claimed != stable_hash(unhashed):
        errors.append("manifest_hash_mismatch")
    return sorted(set(errors))


def build_manifest(
    events: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    *,
    current_date: str,
    catalog_hash: str,
    audit_complete: bool,
    generated_at: str | None = None,
    previous_manifest: Mapping[str, Any] | None = None,
    provider_calls: int = 0,
) -> dict[str, Any]:
    """Create one representative per explicitly supplied occurrence family."""

    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    by_id = {int(event["id"]): event for event in events}
    errors: list[str] = []
    candidates: list[dict[str, Any]] = []
    today = date.fromisoformat(current_date)
    recent_floor = _month_floor(today, 6).isoformat()
    if provider_calls != 0:
        errors.append("provider_calls_must_be_zero")
    if not audit_complete:
        errors.append("checked_audit_incomplete")
    for raw in decisions:
        try:
            event_id = int(raw.get("event_id"))
        except (TypeError, ValueError):
            errors.append("invalid_event_id")
            continue
        event = by_id.get(event_id)
        family_id = str(raw.get("family_id") or "").strip()
        role = str(raw.get("role") or "")
        if event is None or not family_id or role not in ROLES:
            errors.append("invalid_checked_decision")
            continue
        start_date = str(event.get("start_date") or "")
        try:
            date.fromisoformat(start_date)
        except ValueError:
            errors.append("invalid_event_date")
            continue
        occurrence = "future" if start_date >= current_date else "recent"
        if occurrence == "recent" and start_date < recent_floor:
            continue
        candidates.append(
            {"event_id": event_id, "family_id": family_id, "role": role, "occurrence": occurrence,
             "start_date": start_date}
        )

    # Future occurrence wins; otherwise retain the latest recent occurrence.
    accepted_by_family: dict[str, dict[str, Any]] = {}
    for item in sorted(candidates, key=lambda row: (row["family_id"], row["event_id"])):
        current = accepted_by_family.get(item["family_id"])
        if current is None:
            accepted_by_family[item["family_id"]] = item
        elif item["occurrence"] == "future" and current["occurrence"] == "recent":
            accepted_by_family[item["family_id"]] = item
        elif item["occurrence"] == current["occurrence"] == "future" and item["start_date"] < current["start_date"]:
            accepted_by_family[item["family_id"]] = item
        elif item["occurrence"] == current["occurrence"] == "recent" and item["start_date"] > current["start_date"]:
            accepted_by_family[item["family_id"]] = item
    accepted = [
        {key: item[key] for key in ("event_id", "family_id", "role", "occurrence")}
        for item in accepted_by_family.values()
    ]
    accepted.sort(key=lambda item: (item["occurrence"], item["family_id"], item["event_id"]))
    future_count = sum(item["occurrence"] == "future" for item in accepted)
    recent_count = sum(item["occurrence"] == "recent" for item in accepted)
    lifecycle = (
        "active" if future_count >= 3 else "low_supply" if future_count else
        "recent_empty" if recent_count else "dormant"
    )
    previous = _snapshot(previous_manifest)
    previous_valid = False
    if previous:
        probe = {**previous, "manifest_hash": previous.get("manifest_hash")}
        previous_valid = not validate_manifest(
            probe, catalog_event_ids=list(by_id), expected_catalog_hash=catalog_hash
        )
    failed = bool(errors)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "generated_at": generated_at,
        "current_date": current_date,
        "catalog_hash": catalog_hash,
        "provider_calls": provider_calls,
        "compute_status": "blocked" if failed else "pass",
        "quality_status": "blocked" if failed else "pass",
        "publication_status": "blocked" if failed else ("ready" if lifecycle != "dormant" else "shadow"),
        "lifecycle": "last_good" if failed and previous_valid else ("blocked" if failed else lifecycle),
        "failure_reason": ",".join(sorted(set(errors))) if failed else None,
        "accepted": accepted if not failed else [],
        "last_good": {"status": "available" if previous_valid else "absent", "manifest": previous if previous_valid else None},
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--decisions", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--previous", type=Path)
    args = parser.parse_args()
    events_payload = json.loads(args.events.read_text(encoding="utf-8"))
    events = events_payload.get("events") if isinstance(events_payload, dict) else events_payload
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    decisions_payload = json.loads(args.decisions.read_text(encoding="utf-8"))
    if not isinstance(events, list) or not isinstance(catalog, dict) or not isinstance(decisions_payload, dict):
        raise SystemExit("events/list, catalog/object and decisions/object are required")
    previous = json.loads(args.previous.read_text(encoding="utf-8")) if args.previous and args.previous.is_file() else None
    manifest = build_manifest(
        events,
        decisions_payload.get("decisions") or [],
        current_date=str(catalog.get("current_date") or ""),
        catalog_hash=stable_hash(catalog),
        audit_complete=(
            decisions_payload.get("schema_version") == "gastronomy-decisions-v1"
            and decisions_payload.get("policy_version") == POLICY_VERSION
            and decisions_payload.get("audit_status") == "complete"
        ),
        generated_at=str(catalog.get("generated_at") or datetime.now(timezone.utc).isoformat()),
        previous_manifest=previous,
        provider_calls=0,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
