#!/usr/bin/env python3
"""Versioned, ID-only handoff for static collection computation.

The batch keeps computation, quality approval, and publication permission as
three independent states.  It contains no cards or inferred event fields; every
item id must resolve in the frozen production catalog at the consumer boundary.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

SCHEMA_VERSION = "collection-batch-v1"
COMPUTE_STATUSES = frozenset({"pass", "failed", "blocked", "not_required"})
QUALITY_STATUSES = frozenset({"pass", "failed", "blocked", "not_evaluated"})
PUBLICATION_STATUSES = frozenset({"ready", "shadow", "blocked", "disabled"})
LAST_GOOD_STATUSES = frozenset({"absent", "available", "used", "rejected"})
_SHA256_KEYS = (
    "catalog_input_sha256",
    "model_sha256",
    "document_contract_sha256",
    "prototype_bank_sha256",
    "head_sha256",
)


def stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _iso_utc(value: str | datetime | None) -> str:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(value: Any) -> str | None:
    clean = str(value or "").strip().lower()
    return clean if len(clean) == 64 and all(ch in "0123456789abcdef" for ch in clean) else None


def _positive_ids(values: Iterable[Any]) -> list[int]:
    result: set[int] = set()
    for value in values:
        try:
            item_id = int(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if item_id > 0:
            result.add(item_id)
    return sorted(result)


def build_collection_label(
    *,
    strategy: str,
    compute_status: str,
    quality_status: str,
    publication_status: str,
    item_ids: Iterable[int] = (),
    hashes: Mapping[str, Any] | None = None,
    failure_codes: Iterable[str] = (),
    approved_empty: bool = False,
    approved_empty_reason: str | None = None,
    verified_supply_count: int | None = None,
    last_good: Mapping[str, Any] | None = None,
    family_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Build one bounded label record without conflating its three gates."""

    ids = _positive_ids(item_ids)
    failures = sorted({str(value).strip() for value in failure_codes if str(value).strip()})
    hash_values = {
        key: _sha256((hashes or {}).get(key))
        for key in _SHA256_KEYS
        if (hashes or {}).get(key) is not None
    }
    last = dict(last_good or {"status": "absent"})
    last.setdefault("status", "absent")
    record: dict[str, Any] = {
        "strategy": str(strategy or "").strip(),
        "compute_status": str(compute_status or "").strip(),
        "quality_status": str(quality_status or "").strip(),
        "publication_status": str(publication_status or "").strip(),
        "item_ids": ids,
        "item_count": len(ids),
        "hashes": hash_values,
        "failure_codes": failures,
        "approved_empty": bool(approved_empty),
        "approved_empty_reason": (
            str(approved_empty_reason).strip() if approved_empty_reason else None
        ),
        "verified_supply_count": (
            max(0, int(verified_supply_count))
            if verified_supply_count is not None
            else None
        ),
        "last_good": last,
    }
    if family_counts is not None:
        record["family_counts"] = {
            str(key): max(0, int(value)) for key, value in sorted(family_counts.items())
        }
    return record


def build_collection_batch(
    *,
    catalog_hash: str,
    labels: Mapping[str, Mapping[str, Any]],
    generated_at: str | datetime | None = None,
    snapshot: Mapping[str, Any] | None = None,
    policy_hash: str | None = None,
    model: Mapping[str, Any] | None = None,
    document_contract: Mapping[str, Any] | None = None,
    egress_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a canonical collection-batch-v1 payload and self hash."""

    normalized_labels: dict[str, dict[str, Any]] = {}
    for raw_label, raw in sorted(labels.items()):
        label = str(raw_label or "").strip()
        if not label or not isinstance(raw, Mapping):
            raise ValueError("collection labels require non-empty ids and object records")
        if label in normalized_labels:
            raise ValueError(f"duplicate collection label {label}")
        normalized_labels[label] = build_collection_label(
            strategy=str(raw.get("strategy") or ""),
            compute_status=str(raw.get("compute_status") or ""),
            quality_status=str(raw.get("quality_status") or ""),
            publication_status=str(raw.get("publication_status") or ""),
            item_ids=raw.get("item_ids") or (),
            hashes=raw.get("hashes") if isinstance(raw.get("hashes"), Mapping) else {},
            failure_codes=raw.get("failure_codes") or (),
            approved_empty=bool(raw.get("approved_empty")),
            approved_empty_reason=(str(raw.get("approved_empty_reason") or "").strip() or None),
            verified_supply_count=(
                int(raw["verified_supply_count"])
                if raw.get("verified_supply_count") is not None
                else None
            ),
            last_good=raw.get("last_good") if isinstance(raw.get("last_good"), Mapping) else None,
            family_counts=raw.get("family_counts") if isinstance(raw.get("family_counts"), Mapping) else None,
        )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _iso_utc(generated_at),
        "catalog_hash": str(catalog_hash or "").strip().lower(),
        "policy_hash": str(policy_hash or "").strip().lower() or None,
        "snapshot": dict(snapshot or {}),
        "model": dict(model or {}),
        "document_contract": dict(document_contract or {}),
        "labels": normalized_labels,
        "egress_receipt": dict(egress_receipt or {}),
    }
    payload["batch_sha256"] = stable_hash(payload)
    return payload


def validate_collection_batch(
    batch: Mapping[str, Any],
    *,
    catalog_item_ids: Iterable[int] | None = None,
    require_compute: bool = False,
) -> dict[str, Any]:
    """Validate a batch at exporter, kernel, runner, or Astro handoff boundaries."""

    errors: list[str] = []
    if batch.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version mismatch")
    try:
        _iso_utc(str(batch.get("generated_at") or ""))
    except (TypeError, ValueError):
        errors.append("generated_at must be an ISO datetime")
    if _sha256(batch.get("catalog_hash")) is None:
        errors.append("catalog_hash must be SHA-256")
    if batch.get("policy_hash") is not None and _sha256(batch.get("policy_hash")) is None:
        errors.append("policy_hash must be SHA-256 when present")
    catalog = set(_positive_ids(catalog_item_ids or ())) if catalog_item_ids is not None else None
    labels = batch.get("labels")
    if not isinstance(labels, Mapping) or not labels:
        errors.append("labels must be a non-empty object")
        labels = {}
    for label, raw in labels.items():
        prefix = f"labels.{label}"
        if not str(label).strip() or not isinstance(raw, Mapping):
            errors.append(f"{prefix} must be an object")
            continue
        compute = str(raw.get("compute_status") or "")
        quality = str(raw.get("quality_status") or "")
        publication = str(raw.get("publication_status") or "")
        if not str(raw.get("strategy") or "").strip():
            errors.append(f"{prefix}.strategy is required")
        if compute not in COMPUTE_STATUSES:
            errors.append(f"{prefix}.compute_status is invalid")
        if quality not in QUALITY_STATUSES:
            errors.append(f"{prefix}.quality_status is invalid")
        if publication not in PUBLICATION_STATUSES:
            errors.append(f"{prefix}.publication_status is invalid")
        if require_compute and compute == "not_required":
            errors.append(f"{prefix}.compute_status cannot be not_required")
        raw_ids = raw.get("item_ids")
        ids = _positive_ids(raw_ids or ()) if isinstance(raw_ids, list) else []
        if not isinstance(raw_ids, list) or ids != raw_ids:
            errors.append(f"{prefix}.item_ids must be sorted unique positive integers")
        if raw.get("item_count") != len(ids):
            errors.append(f"{prefix}.item_count mismatch")
        if catalog is not None:
            missing = sorted(set(ids) - catalog)
            if missing:
                errors.append(f"{prefix}.item_ids missing from catalog: {missing[:10]}")
        failures = raw.get("failure_codes")
        if not isinstance(failures, list) or any(not str(code).strip() for code in failures):
            errors.append(f"{prefix}.failure_codes must be strings")
            failures = []
        last_good = raw.get("last_good")
        if not isinstance(last_good, Mapping) or last_good.get("status") not in LAST_GOOD_STATUSES:
            errors.append(f"{prefix}.last_good status is invalid")
            last_good = {}
        if last_good.get("status") in {"available", "used"}:
            if _sha256(last_good.get("batch_sha256")) is None:
                errors.append(f"{prefix}.last_good.batch_sha256 is required")
            last_ids = last_good.get("item_ids")
            if not isinstance(last_ids, list) or _positive_ids(last_ids) != last_ids:
                errors.append(f"{prefix}.last_good.item_ids are invalid")
        hashes = raw.get("hashes")
        if not isinstance(hashes, Mapping):
            errors.append(f"{prefix}.hashes must be an object")
            hashes = {}
        for key, value in hashes.items():
            if key not in _SHA256_KEYS or _sha256(value) is None:
                errors.append(f"{prefix}.hashes.{key} is invalid")
        if compute in {"failed", "blocked"} and not failures:
            errors.append(f"{prefix} failed compute requires failure_codes")
        if publication == "ready" and quality != "pass":
            errors.append(f"{prefix} cannot be ready without quality pass")
        if publication == "ready" and compute != "pass" and last_good.get("status") != "used":
            errors.append(f"{prefix} ready output requires compute pass or used last-good")
        approved_empty = raw.get("approved_empty") is True
        if not ids and quality == "pass" and publication == "ready":
            if not approved_empty:
                errors.append(f"{prefix} empty ready output requires approved_empty")
            if not str(raw.get("approved_empty_reason") or "").strip():
                errors.append(f"{prefix} approved-empty reason is required")
            supply = raw.get("verified_supply_count")
            if isinstance(supply, bool) or not isinstance(supply, int) or supply < 0:
                errors.append(f"{prefix} approved-empty supply count is required")
        elif approved_empty:
            errors.append(f"{prefix} approved_empty is only valid for empty ready output")
    declared = batch.get("batch_sha256")
    unhashed = dict(batch)
    unhashed.pop("batch_sha256", None)
    if _sha256(declared) is None or declared != stable_hash(unhashed):
        errors.append("batch_sha256 mismatch")
    return {"valid": not errors, "errors": errors}


def write_collection_batch(path: str | Path, batch: Mapping[str, Any]) -> None:
    validation = validate_collection_batch(batch)
    if not validation["valid"]:
        raise ValueError("invalid collection batch: " + "; ".join(validation["errors"]))
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(batch, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)
