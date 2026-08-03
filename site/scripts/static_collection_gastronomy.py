#!/usr/bin/env python3
"""Source-bound data-preparation contract for the gastronomy collection.

The module deliberately separates three stages:

* BGE/lexical recall produces candidate event IDs in ``collection-batch-v1``;
* a source-bound review store records the ``gastronomy_v1`` role and exact quote;
* only a complete owner-approved review may replace candidate IDs with exact
  collection membership in the common batch.

It does not render an Astro page and never grants public publication.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any

POLICY_VERSION = "gastronomy_v1"
QUEUE_SCHEMA_VERSION = "static-collection-gastronomy-review-queue-v1"
DECISION_STORE_SCHEMA_VERSION = "static-collection-gastronomy-decisions-v1"
MANIFEST_SCHEMA_VERSION = "static-collection-gastronomy-manifest-v1"
PRODUCT_SNAPSHOT_SCHEMA_VERSION = "static-collection-gastronomy-product-quality-v1"

ROLES = frozenset({"core", "co_core", "adjacent", "incidental", "unknown"})
ACCEPTED_MEMBERSHIP_ROLES = frozenset({"core", "co_core"})
DECISION_STORE_STATUSES = frozenset(
    {"starter_not_owner_approved", "provisional", "owner_approved"}
)
REVIEW_STATUSES = frozenset({"approved", "needs_review"})
REASON_CODES = frozenset(
    {
        "food_is_primary_program",
        "food_is_equal_program",
        "food_is_secondary_program",
        "venue_or_service_only",
        "ordinary_food_court",
        "metaphorical_title_or_art_object",
        "generic_market_without_food_focus",
        "insufficient_evidence",
        "conflicting_evidence",
    }
)
ROLE_REASON_CODES = {
    "core": frozenset({"food_is_primary_program"}),
    "co_core": frozenset({"food_is_equal_program"}),
    "adjacent": frozenset({"food_is_secondary_program"}),
    "incidental": frozenset(
        {
            "venue_or_service_only",
            "ordinary_food_court",
            "metaphorical_title_or_art_object",
            "generic_market_without_food_focus",
        }
    ),
    "unknown": frozenset({"insufficient_evidence", "conflicting_evidence"}),
}
TRUST_RANK = {"official": 4, "high": 3, "medium": 2, "low": 1}


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return []
        return list(decoded) if isinstance(decoded, list) else []
    return []


def _date(value: Any) -> str | None:
    raw = str(value or "").strip()[:10]
    if not raw:
        return None
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return None


def _utc(value: str | datetime | None = None) -> str:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _event_id(event: Mapping[str, Any]) -> int:
    try:
        return int(event.get("id", event.get("event_id")) or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _positive_ids(values: Any) -> list[int]:
    if not isinstance(values, list):
        return []
    result: set[int] = set()
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if parsed > 0:
            result.add(parsed)
    return sorted(result)


def normalize_mutual_occurrence_families(
    events: Sequence[Mapping[str, Any]],
) -> dict[int, tuple[int, ...]]:
    """Collapse only reciprocal occurrence links; infer no fuzzy duplicates."""

    by_id = {_event_id(event): dict(event) for event in events if _event_id(event) > 0}
    parent = {event_id: event_id for event_id in by_id}

    def find(value: int) -> int:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    links: dict[int, set[int]] = {}
    for event_id, event in by_id.items():
        raw_links = event.get("other_date_ids", event.get("linked_event_ids"))
        links[event_id] = {
            int(value)
            for value in _list(raw_links)
            if str(value).isdigit() and int(value) != event_id
        }
    for event_id, linked_ids in links.items():
        for linked_id in linked_ids:
            if linked_id in by_id and event_id in links.get(linked_id, set()):
                union(event_id, linked_id)
    members: dict[int, list[int]] = defaultdict(list)
    for event_id in sorted(by_id):
        members[find(event_id)].append(event_id)
    return {event_id: tuple(members[find(event_id)]) for event_id in by_id}


def family_id(event_id: int, members: Sequence[int]) -> str:
    normalized = sorted({int(value) for value in members if int(value) > 0})
    if not normalized:
        return f"event:{event_id}"
    return f"linked:{normalized[0]}" if len(normalized) > 1 else f"event:{event_id}"


def candidate_event_ids(batch: Mapping[str, Any]) -> list[int]:
    if batch.get("schema_version") != "collection-batch-v1":
        raise ValueError("collection batch schema_version mismatch")
    labels = batch.get("labels")
    if not isinstance(labels, Mapping):
        raise ValueError("collection batch labels are missing")
    gastronomy = labels.get("gastronomy")
    if not isinstance(gastronomy, Mapping):
        raise ValueError("collection batch has no gastronomy candidate head")
    ids = _positive_ids(gastronomy.get("item_ids"))
    if ids != gastronomy.get("item_ids"):
        raise ValueError("gastronomy candidate IDs must be sorted unique positive integers")
    if str(gastronomy.get("compute_status") or "") not in {"pass", "blocked"}:
        raise ValueError("gastronomy candidate head has no completed compute state")
    return ids


def select_primary_source(
    sources: Sequence[Mapping[str, Any]],
    *,
    evidence_trust_scope: str = "all",
) -> dict[str, Any] | None:
    """Select one deterministic source row for review, never by prose keywords."""

    if evidence_trust_scope not in {"all", "trusted"}:
        raise ValueError("evidence_trust_scope must be all or trusted")
    usable: list[dict[str, Any]] = []
    for raw in sources:
        source = dict(raw)
        text = str(source.get("source_text") or "").strip()
        url = str(source.get("source_url") or "").strip()
        trust = str(source.get("trust_level") or "").strip().casefold()
        if not text or not url:
            continue
        if evidence_trust_scope == "trusted" and trust not in {"official", "high"}:
            continue
        try:
            source_id = int(source.get("id", source.get("source_id")) or 0)
        except (TypeError, ValueError, OverflowError):
            continue
        if source_id <= 0:
            continue
        source["id"] = source_id
        usable.append(source)
    if not usable:
        return None

    def key(source: Mapping[str, Any]) -> tuple[int, int, int]:
        trust = TRUST_RANK.get(
            str(source.get("trust_level") or "").strip().casefold(), 0
        )
        source_type = str(source.get("source_type") or "").strip().casefold()
        source_priority = 2 if source_type.startswith("parser:") else 1
        return trust, source_priority, -int(source.get("id") or 0)

    return max(usable, key=key)


def _review_input_hash(
    event: Mapping[str, Any],
    source: Mapping[str, Any],
    *,
    family_members: Sequence[int],
) -> str:
    source_text = str(source.get("source_text") or "")
    payload = {
        "policy_version": POLICY_VERSION,
        "event": {
            "event_id": _event_id(event),
            "title": event.get("title"),
            "start_date": _date(event.get("start_date", event.get("date"))),
            "end_date": _date(event.get("end_date")),
            "event_type": event.get("event_type"),
            "venue": event.get("venue_name", event.get("location_name")),
            "city": event.get("city"),
            "family_members": sorted(int(value) for value in family_members),
        },
        "source": {
            "source_id": int(source.get("id") or 0),
            "source_url": str(source.get("source_url") or ""),
            "source_type": str(source.get("source_type") or ""),
            "trust_level": source.get("trust_level"),
            "source_text_sha256": hashlib.sha256(
                source_text.encode("utf-8")
            ).hexdigest(),
        },
    }
    return stable_hash(payload)


def build_review_queue(
    events: Sequence[Mapping[str, Any]],
    *,
    source_records_by_event: Mapping[int, Sequence[Mapping[str, Any]]],
    candidate_ids: Sequence[int],
    current_date: str,
    generated_at: str | datetime | None = None,
    source_scope: str = "fly-sqlite-snapshot",
    batch_sha256: str | None = None,
    evidence_trust_scope: str = "all",
) -> dict[str, Any]:
    current = date.fromisoformat(current_date).isoformat()
    by_id = {_event_id(event): dict(event) for event in events if _event_id(event) > 0}
    families = normalize_mutual_occurrence_families(list(by_id.values()))
    candidate_set = {int(value) for value in candidate_ids if int(value) in by_id}
    grouped: dict[str, list[int]] = defaultdict(list)
    for event_id in sorted(candidate_set):
        grouped[family_id(event_id, families[event_id])].append(event_id)

    rows: list[dict[str, Any]] = []
    source_bound = 0
    for family_key, candidate_members in sorted(grouped.items()):
        members = families[candidate_members[0]]
        source_options: list[
            tuple[tuple[int, int, str, int], int, dict[str, Any]]
        ] = []
        for member_event_id in sorted(candidate_members):
            selected = select_primary_source(
                source_records_by_event.get(member_event_id, ()),
                evidence_trust_scope=evidence_trust_scope,
            )
            if selected is None:
                continue
            try:
                bound_event_id = int(selected.get("event_id") or member_event_id)
            except (TypeError, ValueError, OverflowError):
                continue
            if bound_event_id != member_event_id:
                continue
            selected["event_id"] = bound_event_id
            trust = TRUST_RANK.get(
                str(selected.get("trust_level") or "").strip().casefold(), 0
            )
            source_type = str(selected.get("source_type") or "").strip().casefold()
            source_priority = 2 if source_type.startswith("parser:") else 1
            event_date = (
                _date(
                    by_id[member_event_id].get(
                        "start_date", by_id[member_event_id].get("date")
                    )
                )
                or "9999-12-31"
            )
            source_options.append(
                (
                    (-trust, -source_priority, event_date, member_event_id),
                    member_event_id,
                    selected,
                )
            )
        if source_options:
            _rank, event_id, source = min(source_options, key=lambda row: row[0])
        else:
            event_id = min(
                candidate_members,
                key=lambda member_event_id: (
                    _date(
                        by_id[member_event_id].get(
                            "start_date", by_id[member_event_id].get("date")
                        )
                    )
                    or "9999-12-31",
                    member_event_id,
                ),
            )
            source = None
        event = by_id[event_id]
        source_payload: dict[str, Any] | None = None
        input_hash: str | None = None
        review_state = "blocked_no_source"
        if source is not None:
            source_text = str(source.get("source_text") or "")
            source_payload = {
                "source_id": int(source["id"]),
                "event_id": int(source.get("event_id") or event_id),
                "source_type": str(source.get("source_type") or ""),
                "source_url": str(source.get("source_url") or ""),
                "trust_level": source.get("trust_level"),
                "source_text": source_text,
                "source_text_sha256": hashlib.sha256(
                    source_text.encode("utf-8")
                ).hexdigest(),
                "source_text_char_count": len(source_text),
            }
            input_hash = _review_input_hash(event, source, family_members=members)
            review_state = "ready_for_review"
            source_bound += 1
        rows.append(
            {
                "event_id": event_id,
                "family_id": family_key,
                "family_member_event_ids": list(members),
                "candidate_member_event_ids": sorted(candidate_members),
                "title": str(event.get("title") or ""),
                "start_date": _date(event.get("start_date", event.get("date"))),
                "end_date": _date(event.get("end_date"))
                or _date(event.get("start_date", event.get("date"))),
                "event_type": event.get("event_type"),
                "venue": event.get("venue_name", event.get("location_name")),
                "city": event.get("city"),
                "source": source_payload,
                "input_hash": input_hash,
                "review_state": review_state,
            }
        )

    queue: dict[str, Any] = {
        "schema_version": QUEUE_SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "generated_at": _utc(generated_at),
        "current_date": current,
        "source_scope": str(source_scope or "").strip(),
        "evidence_trust_scope": evidence_trust_scope,
        "collection_batch_sha256": batch_sha256,
        "candidate_label": "gastronomy",
        "coverage": {
            "candidate_event_count": len(candidate_set),
            "candidate_family_count": len(rows),
            "source_bound_family_count": source_bound,
            "blocked_family_count": len(rows) - source_bound,
        },
        "candidates": rows,
    }
    queue["queue_sha256"] = stable_hash(queue)
    return queue


def starter_decision_store() -> dict[str, Any]:
    return {
        "schema_version": DECISION_STORE_SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "status": "starter_not_owner_approved",
        "reviewed_at": None,
        "reviewer": None,
        "decisions": [],
    }


def validate_decision_store(
    store: Mapping[str, Any],
    *,
    queue: Mapping[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    if store.get("schema_version") != DECISION_STORE_SCHEMA_VERSION:
        errors.append("schema_version_mismatch")
    if store.get("policy_version") != POLICY_VERSION:
        errors.append("policy_version_mismatch")
    status = str(store.get("status") or "")
    if status not in DECISION_STORE_STATUSES:
        errors.append("status_invalid")
    if queue.get("schema_version") != QUEUE_SCHEMA_VERSION:
        errors.append("queue_schema_mismatch")
    if queue.get("policy_version") != POLICY_VERSION:
        errors.append("queue_policy_mismatch")
    declared_queue_hash = str(queue.get("queue_sha256") or "")
    if declared_queue_hash != stable_hash(
        {key: value for key, value in queue.items() if key != "queue_sha256"}
    ):
        errors.append("queue_sha256_mismatch")

    candidates = queue.get("candidates")
    if not isinstance(candidates, list):
        errors.append("queue_candidates_invalid")
        candidates = []
    by_family = {
        str(row.get("family_id") or ""): row
        for row in candidates
        if isinstance(row, Mapping) and str(row.get("family_id") or "")
    }
    raw_decisions = store.get("decisions")
    if not isinstance(raw_decisions, list):
        errors.append("decisions_invalid")
        raw_decisions = []

    normalized: list[dict[str, Any]] = []
    seen_families: set[str] = set()
    for index, raw in enumerate(raw_decisions):
        prefix = f"decisions[{index}]"
        if not isinstance(raw, Mapping):
            errors.append(f"{prefix}:not_object")
            continue
        row = dict(raw)
        family_key = str(row.get("family_id") or "")
        candidate = by_family.get(family_key)
        if not family_key or candidate is None:
            errors.append(f"{prefix}:family_not_in_queue")
            continue
        if family_key in seen_families:
            errors.append(f"{prefix}:duplicate_family")
            continue
        seen_families.add(family_key)
        try:
            event_id = int(row.get("event_id") or 0)
            source_id = int(row.get("source_id") or 0)
        except (TypeError, ValueError, OverflowError):
            errors.append(f"{prefix}:id_invalid")
            continue
        source = candidate.get("source")
        if not isinstance(source, Mapping):
            errors.append(f"{prefix}:candidate_source_missing")
            continue
        role = str(row.get("role") or "")
        reason = str(row.get("reason_code") or "")
        review_status = str(row.get("review_status") or "")
        confidence = row.get("confidence")
        quote = str(row.get("evidence_quote") or "").strip()
        if event_id != int(candidate.get("event_id") or 0):
            errors.append(f"{prefix}:event_id_mismatch")
        if source_id != int(source.get("source_id") or 0):
            errors.append(f"{prefix}:source_id_mismatch")
        if str(row.get("source_url") or "") != str(source.get("source_url") or ""):
            errors.append(f"{prefix}:source_url_mismatch")
        if str(row.get("input_hash") or "") != str(candidate.get("input_hash") or ""):
            errors.append(f"{prefix}:input_hash_stale")
        if role not in ROLES:
            errors.append(f"{prefix}:role_invalid")
        if reason not in REASON_CODES:
            errors.append(f"{prefix}:reason_code_invalid")
        elif role in ROLE_REASON_CODES and reason not in ROLE_REASON_CODES[role]:
            errors.append(f"{prefix}:role_reason_mismatch")
        if review_status not in REVIEW_STATUSES:
            errors.append(f"{prefix}:review_status_invalid")
        if (
            isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not 0 <= float(confidence) <= 1
        ):
            errors.append(f"{prefix}:confidence_invalid")
        source_text = str(source.get("source_text") or "")
        if role == "unknown":
            if quote:
                errors.append(f"{prefix}:unknown_quote_must_be_empty")
            if reason not in {"insufficient_evidence", "conflicting_evidence"}:
                errors.append(f"{prefix}:unknown_reason_invalid")
        elif not quote or quote not in source_text:
            errors.append(f"{prefix}:evidence_quote_not_exact")
        if status == "owner_approved":
            if review_status != "approved":
                errors.append(f"{prefix}:owner_store_requires_approved_rows")
            if not str(row.get("reviewer") or "").strip():
                errors.append(f"{prefix}:owner_row_reviewer_missing")
            if not str(row.get("reviewed_at") or "").strip():
                errors.append(f"{prefix}:owner_row_reviewed_at_missing")
            if row.get("manual_lock") is not True:
                errors.append(f"{prefix}:owner_row_manual_lock_required")
        normalized.append(
            {
                "event_id": event_id,
                "family_id": family_key,
                "input_hash": str(row.get("input_hash") or ""),
                "source_id": source_id,
                "source_url": str(row.get("source_url") or ""),
                "role": role,
                "confidence": float(confidence or 0.0),
                "evidence_quote": quote,
                "reason_code": reason,
                "review_status": review_status,
                "reviewed_at": row.get("reviewed_at"),
                "reviewer": row.get("reviewer"),
                "manual_lock": bool(row.get("manual_lock", True)),
            }
        )

    missing = sorted(set(by_family) - seen_families)
    if missing:
        warnings.append(f"unreviewed_candidate_families:{len(missing)}")
    if status == "owner_approved":
        if missing:
            errors.append("owner_store_has_unreviewed_candidate_families")
        if not str(store.get("reviewer") or "").strip():
            errors.append("owner_store_reviewer_missing")
        if not str(store.get("reviewed_at") or "").strip():
            errors.append("owner_store_reviewed_at_missing")
    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "normalized_decisions": sorted(
            normalized, key=lambda row: (row["family_id"], row["event_id"])
        ),
        "missing_family_ids": missing,
    }


def build_manifest(
    queue: Mapping[str, Any],
    store: Mapping[str, Any],
    *,
    generated_at: str | datetime | None = None,
) -> dict[str, Any]:
    validation = validate_decision_store(store, queue=queue)
    candidates = {
        str(row.get("family_id")): dict(row)
        for row in queue.get("candidates") or []
        if isinstance(row, Mapping)
    }
    decisions = {row["family_id"]: row for row in validation["normalized_decisions"]}
    accepted_items: list[dict[str, Any]] = []
    role_counts = {role: 0 for role in sorted(ROLES)}
    for family_key, decision in sorted(decisions.items()):
        role_counts[decision["role"]] += 1
        if (
            decision["review_status"] != "approved"
            or decision["role"] not in ACCEPTED_MEMBERSHIP_ROLES
        ):
            continue
        candidate = candidates[family_key]
        accepted_items.append(
            {
                "event_id": int(candidate["event_id"]),
                "family_id": family_key,
                "family_member_event_ids": list(
                    candidate.get("family_member_event_ids") or []
                ),
                "title": candidate.get("title"),
                "start_date": candidate.get("start_date"),
                "end_date": candidate.get("end_date"),
                "event_type": candidate.get("event_type"),
                "venue": candidate.get("venue"),
                "city": candidate.get("city"),
                "source_status": "grounded",
                "review_status": "accepted",
                "gastronomy_role": decision["role"],
                "reason_code": decision["reason_code"],
                "input_hash": decision["input_hash"],
                "source_id": decision["source_id"],
                "source_url": decision["source_url"],
                "evidence_quote": decision["evidence_quote"],
            }
        )
    accepted_items.sort(
        key=lambda row: (
            str(row.get("start_date") or "9999-12-31"),
            int(row["event_id"]),
        )
    )
    coverage = queue.get("coverage") if isinstance(queue.get("coverage"), Mapping) else {}
    complete = bool(
        validation["valid"]
        and store.get("status") == "owner_approved"
        and int(coverage.get("blocked_family_count") or 0) == 0
        and not validation["missing_family_ids"]
    )
    future_count = len(accepted_items)
    catalog_state = (
        "unknown"
        if not complete
        else "active"
        if future_count >= 3
        else "low_supply"
        if future_count >= 1
        else "dormant"
    )
    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "generated_at": _utc(generated_at),
        "current_date": queue.get("current_date"),
        "source_scope": queue.get("source_scope"),
        "queue_sha256": queue.get("queue_sha256"),
        "decision_store_sha256": stable_hash(store),
        "decision_store_status": store.get("status"),
        "extraction_status": "pass" if complete else "review_required",
        "publication_status": "blocked",
        "publication_reason": (
            "astro_page_out_of_scope"
            if complete
            else "gastronomy_source_review_incomplete"
        ),
        "catalog_state": catalog_state,
        "candidate_event_count": int(coverage.get("candidate_event_count") or 0),
        "candidate_family_count": int(coverage.get("candidate_family_count") or 0),
        "reviewed_family_count": len(decisions),
        "accepted_future_family_count": future_count,
        "recent_family_count": 0,
        "recent_history_status": "not_computed_in_future_candidate_batch",
        "role_counts": role_counts,
        "missing_family_ids": validation["missing_family_ids"],
        "validation_errors": validation["errors"],
        "validation_warnings": validation["warnings"],
        "item_ids": [int(row["event_id"]) for row in accepted_items],
        "items": accepted_items,
    }
    manifest["manifest_sha256"] = stable_hash(manifest)
    return manifest


def validate_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        errors.append("schema_version_mismatch")
    if manifest.get("policy_version") != POLICY_VERSION:
        errors.append("policy_version_mismatch")
    if manifest.get("publication_status") != "blocked":
        errors.append("publication_must_be_blocked")
    ids = _positive_ids(manifest.get("item_ids"))
    if ids != manifest.get("item_ids"):
        errors.append("item_ids_invalid")
    items = manifest.get("items")
    if not isinstance(items, list):
        errors.append("items_invalid")
        items = []
    item_ids = {
        int(row.get("event_id") or 0)
        for row in items
        if isinstance(row, Mapping)
    }
    if set(ids) != item_ids:
        errors.append("item_ids_items_mismatch")
    family_ids = [
        str(row.get("family_id") or "")
        for row in items
        if isinstance(row, Mapping)
    ]
    if any(not value for value in family_ids) or len(family_ids) != len(set(family_ids)):
        errors.append("family_ids_invalid")
    declared = str(manifest.get("manifest_sha256") or "")
    actual = stable_hash(
        {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    )
    if declared != actual:
        errors.append("manifest_sha256_mismatch")
    return {"valid": not errors, "errors": errors}


def overlay_collection_batch(
    batch: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Overlay exact membership into the common batch without public promotion."""

    output = copy.deepcopy(dict(batch))
    labels = output.get("labels")
    if not isinstance(labels, dict) or not isinstance(labels.get("gastronomy"), dict):
        raise ValueError("collection batch has no gastronomy candidate label")
    label = labels["gastronomy"]
    candidate_ids = _positive_ids(label.get("item_ids"))
    complete = (
        manifest.get("extraction_status") == "pass"
        and manifest.get("decision_store_status") == "owner_approved"
        and validate_manifest(manifest)["valid"]
    )
    label["candidate_item_ids"] = candidate_ids
    label["candidate_item_count"] = len(candidate_ids)
    label["gastronomy_policy_version"] = POLICY_VERSION
    label["gastronomy_manifest_sha256"] = manifest.get("manifest_sha256")
    label["catalog_state"] = manifest.get("catalog_state")
    if complete:
        ids = _positive_ids(manifest.get("item_ids"))
        label.update(
            {
                "strategy": "semantic_bge_source_grounded",
                "compute_status": "pass",
                "quality_status": "pass",
                "publication_status": "shadow",
                "item_ids": ids,
                "item_count": len(ids),
                "failure_codes": ["astro_page_out_of_scope"],
                "verified_supply_count": len(ids),
                "family_counts": {
                    "future": int(manifest.get("accepted_future_family_count") or 0),
                    "recent": int(manifest.get("recent_family_count") or 0),
                },
            }
        )
    else:
        failures = {
            str(value) for value in (label.get("failure_codes") or []) if str(value)
        }
        failures.add("gastronomy_source_review_incomplete")
        label.update(
            {
                "quality_status": "not_evaluated",
                "publication_status": "blocked",
                "item_ids": [],
                "item_count": 0,
                "failure_codes": sorted(failures),
                "verified_supply_count": 0,
            }
        )
    output.pop("batch_sha256", None)
    output["batch_sha256"] = stable_hash(output)
    return output


def build_product_quality_snapshot(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Return the soft product-monitor projection; WATCH is expected pre-page."""

    items = [
        {
            "event_id": int(row["event_id"]),
            "family_id": row.get("family_id"),
            "start_date": row.get("start_date"),
            "end_date": row.get("end_date"),
            "venue": row.get("venue"),
            "event_type": row.get("event_type"),
            "source_status": row.get("source_status"),
            "review_status": row.get("review_status"),
        }
        for row in manifest.get("items") or []
        if isinstance(row, Mapping)
    ]
    return {
        "schema_version": PRODUCT_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": manifest.get("generated_at"),
        "source_scope": "gastronomy-manifest",
        "input_fingerprint": manifest.get("manifest_sha256"),
        "stale_after_hours": 72,
        "collections": {
            "gastronomy": {
                "mode": "shadow",
                "state": (
                    "ready"
                    if manifest.get("extraction_status") == "pass"
                    else "blocked"
                ),
                "using_last_good": False,
                "watch_below_families": 3,
                "items": items,
            }
        },
    }
