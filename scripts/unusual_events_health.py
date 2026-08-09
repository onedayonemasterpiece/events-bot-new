#!/usr/bin/env python3
"""Normalize existing StaticSiteBuilder artifacts into bounded Unusual health.

This module is deliberately an adapter, not another semantic pipeline.  It
never loads an encoder or scores an event.  It validates the current collection
BGE receipt, Unusual manifest/cache, and StaticSiteBuilder receipt, then emits a
small versioned operational contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

HEALTH_SCHEMA = "unusual-events-health-v1"
INPUT_SCHEMA = "unusual-events-health-resolver-v1"
REQUEST_SCHEMA = "unusual-events-health-request-v1"
BGE_RECEIPT_SCHEMA = "static-collection-bge-cache-receipt-v1"
MANIFEST_SCHEMAS = frozenset({"static_unusual_events_v1", "unusual-event-manifest-v1"})
CACHE_SCHEMA = "unusual-event-score-cache-v1"
BUILDER_SCHEMAS = frozenset({"static_site_build_result_v2", "static_site_success_receipt_v2"})
MODEL_ID = "BAAI/bge-m3"
MODEL_REVISION = "5617a9f61b028005a4858fdac845db406aefb181"
EMBEDDING_DIM = 1024
ENCODER_CONTRACT = "bge_m3_cpu_dense_fp32_l2_v1"
DOCUMENT_KIND = "collection_semantics_v1"
DOCUMENT_VERSION = "collection-semantics-doc-v1"
DEFAULT_TARGET_COUNT = 20
DEFAULT_MINIMUM_COUNT = 12
MAX_SELECTED = 30
MAX_SUPPORT_ROWS = 20
MAX_REASONS = 8
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_TEXT = 240
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:._-]{0,239}$")
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


class ContractError(ValueError):
    """Raised for an unreadable or structurally invalid input contract."""


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ContractError(f"input_missing:{path.name}")
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise ContractError(f"input_too_large:{path.name}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ContractError(f"input_invalid_json:{path.name}") from exc
    if not isinstance(value, dict):
        raise ContractError(f"input_not_object:{path.name}")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _rows(value: Any) -> list[Mapping[str, Any]]:
    return [row for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def _text(value: Any, *, limit: int = MAX_TEXT) -> str:
    clean = URL_RE.sub("[redacted-url]", str(value or "").strip())
    return clean[:limit]


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value.strip()):
        return int(value.strip())
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return round(float(value), 8)
    return None


def _sha(value: Any) -> str | None:
    clean = str(value or "").strip().lower()
    return clean if SHA256_RE.fullmatch(clean) else None


def _iso(value: Any) -> str | None:
    clean = str(value or "").strip()
    if not clean:
        return None
    try:
        parsed = datetime.fromisoformat(clean.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _today(value: Any) -> date | None:
    clean = str(value or "").strip()[:10]
    try:
        return date.fromisoformat(clean)
    except ValueError:
        return None


def _add(findings: list[dict[str, str]], code: str, message: str) -> None:
    if not any(item["code"] == code for item in findings):
        findings.append({"code": code, "message": _text(message)})


def _value(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "" and value != [] and value != {}:
            return value
    return None


def _builder_parts(builder: Mapping[str, Any], errors: list[dict[str, str]]) -> dict[str, Any]:
    schema = str(builder.get("schema_version") or "")
    if schema not in BUILDER_SCHEMAS:
        _add(errors, "builder.schema", "Unsupported StaticSiteBuilder receipt schema.")
    if schema == "static_site_build_result_v2":
        if builder.get("ok") is not True:
            _add(errors, "builder.failed", "StaticSiteBuilder did not finish successfully.")
        if builder.get("profile") != "production-candidate":
            _add(errors, "builder.profile", "Health evidence requires a production-candidate build.")
    repo_sha = str(builder.get("repo_sha") or "").strip().lower()
    run_id = _text(builder.get("run_id"))
    build_id = _text(builder.get("build_id"))
    fingerprint = _sha(builder.get("input_fingerprint"))
    snapshot = _mapping(builder.get("snapshot"))
    snapshot_sha = _sha(_value(snapshot.get("snapshot_sha256"), builder.get("snapshot_sha256")))
    snapshot_id = _text(_value(snapshot.get("snapshot_id"), builder.get("snapshot_id"))) or None
    semantic = _mapping(builder.get("semantic"))
    if not SHA40_RE.fullmatch(repo_sha):
        _add(errors, "builder.repo_sha", "Builder receipt has no full repository SHA.")
    if not run_id or not ID_RE.fullmatch(run_id):
        _add(errors, "builder.run_id", "Builder receipt has no bounded run identity.")
    if not build_id or not ID_RE.fullmatch(build_id):
        _add(errors, "builder.build_id", "Builder receipt has no bounded build identity.")
    if fingerprint is None:
        _add(errors, "builder.input_fingerprint", "Builder input fingerprint is absent or invalid.")
    if snapshot_sha is None:
        _add(errors, "builder.snapshot_sha256", "Builder snapshot SHA-256 is absent or invalid.")
    if not semantic:
        _add(errors, "builder.semantic", "Builder receipt has no shared semantic receipt.")
    return {
        "schema": schema,
        "repo_sha": repo_sha or None,
        "run_id": run_id or None,
        "build_id": build_id or None,
        "input_fingerprint": fingerprint,
        "snapshot_sha256": snapshot_sha,
        "snapshot_id": snapshot_id,
        "semantic": semantic,
        "finished_at": _iso(_value(builder.get("finished_at"), builder.get("effective_date"))),
        "published": bool(builder.get("published")),
    }


def _validate_bge(receipt: Mapping[str, Any], errors: list[dict[str, str]]) -> dict[str, Any]:
    if receipt.get("schema_version") != BGE_RECEIPT_SCHEMA:
        _add(errors, "bge.schema", "Collection BGE cache receipt schema mismatch.")
    metadata = _mapping(receipt.get("metadata"))
    expected = {
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "encoder_contract": ENCODER_CONTRACT,
        "document_kind": DOCUMENT_KIND,
        "document_version": DOCUMENT_VERSION,
        "provider_calls": 0,
    }
    for key, wanted in expected.items():
        actual = _value(metadata.get(key), receipt.get(key))
        if actual != wanted:
            _add(errors, f"bge.{key}", f"Collection BGE {key} contract mismatch.")
    if receipt.get("dtype") != "float32":
        _add(errors, "bge.dtype", "Collection BGE cache must be float32.")
    for key in ("artifact_sha256", "npz_sha256", "event_cache_identity_sha256", "prototype_bank_sha256", "classifier_sha256"):
        if _sha(_value(receipt.get(key), metadata.get(key))) is None:
            _add(errors, f"bge.{key}", f"Collection BGE {key} is absent or invalid.")
    event_count = _int(metadata.get("event_count"))
    encoded = _int(metadata.get("encoded_event_count"))
    reused = _int(metadata.get("reused_event_count"))
    hashes = receipt.get("event_text_hashes")
    if event_count is None or event_count <= 0:
        _add(errors, "bge.event_count", "Collection BGE event count must be positive.")
    if encoded is None or reused is None or encoded < 0 or reused < 0:
        _add(errors, "bge.reuse_counts", "Collection BGE encode/reuse counts are invalid.")
    elif event_count is not None and encoded + reused != event_count:
        _add(errors, "bge.coverage", "Collection BGE encode/reuse counts do not cover the artifact.")
    if not isinstance(hashes, Mapping) or (event_count is not None and len(hashes) != event_count):
        _add(errors, "bge.event_text_hashes", "Collection BGE event hash coverage is incomplete.")
    build = _mapping(metadata.get("build"))
    mode = "warm" if (reused or 0) > 0 else "cold"
    return {
        "model_id": _text(_value(metadata.get("model_id"), receipt.get("model_id"))) or None,
        "model_revision": _text(_value(metadata.get("model_revision"), receipt.get("model_revision"))) or None,
        "embedding_dim": _int(_value(metadata.get("embedding_dim"), receipt.get("embedding_dim"))),
        "encoder_contract": _text(_value(metadata.get("encoder_contract"), receipt.get("encoder_contract"))) or None,
        "document_kind": _text(_value(metadata.get("document_kind"), receipt.get("document_kind"))) or None,
        "document_version": _text(_value(metadata.get("document_version"), receipt.get("document_version"))) or None,
        "artifact_sha256": _sha(_value(receipt.get("artifact_sha256"), metadata.get("artifact_sha256"))),
        "npz_sha256": _sha(receipt.get("npz_sha256")),
        "provider_calls": _int(_value(metadata.get("provider_calls"), receipt.get("provider_calls"))),
        "event_count": event_count,
        "encoded_event_count": encoded,
        "reused_event_count": reused,
        "prototype_count": _int(metadata.get("prototype_count")),
        "build_mode": mode,
        "single_vector_contract": not any(item["code"].startswith("bge.") for item in errors),
        "coverage_complete": event_count is not None and encoded is not None and reused is not None and encoded + reused == event_count,
        "build": build,
        "prototype_bank_sha256": _sha(_value(receipt.get("prototype_bank_sha256"), metadata.get("prototype_bank_sha256"))),
        "classifier_sha256": _sha(_value(receipt.get("classifier_sha256"), metadata.get("classifier_sha256"))),
    }


def _row_id(row: Mapping[str, Any]) -> int | None:
    nested = _mapping(row.get("event_snapshot"))
    value = _int(_value(row.get("event_id"), row.get("id"), nested.get("id"), nested.get("event_id")))
    return value if value is not None and value > 0 else None


def _concept(row: Mapping[str, Any], event_id: int | None) -> str:
    value = _text(_value(row.get("concept_id"), row.get("unusual_concept_id")), limit=160)
    return value or (f"event:{event_id}" if event_id is not None else "")


def _support_row(row: Mapping[str, Any]) -> dict[str, Any]:
    event_id = _row_id(row)
    return {
        "event_id": event_id,
        "concept_id": _concept(row, event_id) or None,
        "family": _text(_value(row.get("family"), row.get("primary_family")), limit=80) or None,
        "score": _number(_value(row.get("unusual_score"), row.get("score"), row.get("confidence"))),
        "reason": _text(_value(row.get("reason"), row.get("exclusion_reason"), row.get("decision")), limit=120) or None,
    }


def _selected_row(row: Mapping[str, Any], errors: list[dict[str, str]]) -> dict[str, Any]:
    nested = _mapping(row.get("event_snapshot"))
    event_id = _row_id(row)
    concept = _concept(row, event_id)
    title = _text(_value(row.get("title"), nested.get("title")), limit=180)
    path = str(_value(row.get("path"), nested.get("path"), "") or "").strip()
    if path and (not path.startswith("/") or path.startswith("//") or "://" in path):
        _add(errors, "feed.absolute_url", "Selected rows may persist only same-origin relative paths.")
        path = ""
    raw_families = row.get("families")
    family = _text(
        _value(
            row.get("family"),
            row.get("primary_family"),
            raw_families[0] if isinstance(raw_families, list) and raw_families else None,
        ),
        limit=80,
    )
    reasons_value = _value(row.get("reason_codes"), row.get("reasons"), [])
    reasons = [
        _text(value, limit=100)
        for value in (reasons_value if isinstance(reasons_value, list) else [reasons_value])
        if _text(value, limit=100)
    ][:MAX_REASONS]
    image = _mapping(_value(row.get("image_policy"), nested.get("image_policy")))
    has_image = bool(
        _value(row.get("has_image"), nested.get("has_image"), nested.get("poster_sha256"), row.get("image_sha256"))
    )
    return {
        "event_id": event_id,
        "concept_id": concept or None,
        "title": title or None,
        "path": path or None,
        "date": _text(_value(row.get("date"), row.get("start_date"), nested.get("start_date")), limit=32) or None,
        "start_date": _text(_value(row.get("start_date"), row.get("date"), nested.get("start_date")), limit=32) or None,
        "end_date": _text(_value(row.get("end_date"), nested.get("end_date"), row.get("date"), nested.get("start_date")), limit=32) or None,
        "family": family or None,
        "score": _number(_value(row.get("unusual_score"), row.get("score"), row.get("confidence"))),
        "reasons": reasons,
        "image_policy": {
            "has_image": has_image,
            "status": _text(_value(image.get("status"), row.get("image_status")), limit=80) or None,
        },
        "image_required": row.get("image_required") if isinstance(row.get("image_required"), bool) else True,
    }


def _normalize_feed(
    manifest: Mapping[str, Any],
    *,
    target: int,
    minimum: int,
    errors: list[dict[str, str]],
    warnings: list[dict[str, str]],
) -> dict[str, Any]:
    raw_selected = manifest.get("items")
    if not isinstance(raw_selected, list):
        _add(errors, "feed.items", "Unusual manifest items must be an array.")
        raw_selected = []
    if len(raw_selected) > MAX_SELECTED:
        _add(errors, "feed.maximum", "Unusual manifest exceeds the absolute 30-card limit.")
    selected = [_selected_row(row, errors) for row in _rows(raw_selected[:MAX_SELECTED])]
    if len(selected) != len(raw_selected[:MAX_SELECTED]):
        _add(errors, "feed.item_shape", "Every selected item must be an object.")
    event_ids = [row["event_id"] for row in selected]
    concept_ids = [row["concept_id"] for row in selected]
    if any(value is None for value in event_ids):
        _add(errors, "feed.event_id", "Every selected item needs a positive event ID.")
    if any(value is None for value in concept_ids):
        _add(errors, "feed.concept_id", "Every selected item needs a bounded concept identity.")
    if len(set(event_ids)) != len(event_ids):
        _add(errors, "feed.duplicate_event", "Selected event IDs must be unique.")
    if len(set(concept_ids)) != len(concept_ids):
        _add(errors, "feed.duplicate_concept", "Selected concept IDs must be unique.")
    as_of = _today(_value(manifest.get("as_of_date"), _mapping(manifest.get("build")).get("as_of_date")))
    expired = [row for row in selected if as_of and _today(row.get("end_date")) and _today(row.get("end_date")) < as_of]
    if expired:
        _add(errors, "feed.expired", "Selected output contains expired events.")
    if len(selected) < target and len(selected) >= minimum:
        _add(warnings, "feed.under_target", "Selected feed is above minimum but below the provisional target.")
    candidate_rows = _rows(_value(manifest.get("candidate_items"), manifest.get("shadow_items"), []))
    near_rows = _rows(_value(manifest.get("near_threshold"), manifest.get("near_threshold_items"), []))
    exclusion_rows = _rows(_value(manifest.get("exclusions"), manifest.get("excluded_items"), []))
    duplicate_rows = _rows(_value(manifest.get("duplicates"), manifest.get("duplicate_items"), []))
    declared_expired = _rows(_value(manifest.get("expired"), manifest.get("expired_items"), []))
    visible_contract = [
        {
            "event_id": row["event_id"],
            "concept_id": row["concept_id"],
            "title": row["title"],
            "path": row["path"],
            "date": row["date"],
            "start_date": row["start_date"],
            "family": row["family"],
            "score": row["score"],
            "reasons": row["reasons"],
            "image_policy": row["image_policy"],
            "image_required": row["image_required"],
        }
        for row in selected
    ]
    return {
        "selected_count": len(selected),
        "target_count": target,
        "minimum_publish_count": minimum,
        "ordered_visible_event_ids": event_ids,
        "ordered_visible_concept_ids": concept_ids,
        "visible_event_ids": event_ids,
        "visible_concept_ids": concept_ids,
        "selected": selected,
        "candidate_count": (
            _int(manifest.get("candidate_count"))
            if _int(manifest.get("candidate_count")) is not None and _int(manifest.get("candidate_count")) >= 0
            else len(candidate_rows)
        ),
        "near_threshold": [_support_row(row) for row in near_rows[:MAX_SUPPORT_ROWS]],
        "exclusions": [_support_row(row) for row in exclusion_rows[:MAX_SUPPORT_ROWS]],
        "duplicates": [_support_row(row) for row in duplicate_rows[:MAX_SUPPORT_ROWS]],
        "expired": [_support_row(row) for row in (declared_expired + expired)[:MAX_SUPPORT_ROWS]],
        "visible_output_sha256": _stable_hash(visible_contract),
    }


def _quality(manifest: Mapping[str, Any]) -> dict[str, Any]:
    gate = _mapping(manifest.get("quality_gate"))
    observed = _mapping(_value(gate.get("observed"), gate.get("metrics")))
    safe_metrics: dict[str, Any] = {}
    for key in sorted(observed):
        value = observed[key]
        if isinstance(value, bool) or value is None:
            safe_metrics[str(key)[:80]] = value
        elif _number(value) is not None:
            safe_metrics[str(key)[:80]] = _number(value)
        elif isinstance(value, str):
            safe_metrics[str(key)[:80]] = _text(value, limit=120)
        if len(safe_metrics) >= 32:
            break
    status = _text(_value(gate.get("status"), gate.get("approval_status"), manifest.get("evaluation_approval_status"))).lower()
    return {
        "status": status or "unknown",
        "evidence_kind": _text(_value(observed.get("evidence_kind"), gate.get("evidence_kind")), limit=120) or None,
        "metrics": safe_metrics,
    }


def _empty_health(
    *,
    code: str,
    message: str,
    generated_at: str,
    target: int,
    minimum: int,
) -> dict[str, Any]:
    visible_hash = _stable_hash([])
    return {
        "schema_version": HEALTH_SCHEMA,
        "generated_at": generated_at,
        "health_status": "INCIDENT",
        "content_readiness": "BLOCKED",
        "repo_sha": None,
        "run_id": None,
        "as_of_date": None,
        "source": {
            "builder_receipt_schema": None,
            "repo_sha": None,
            "run_id": None,
            "build_id": None,
            "snapshot_id": None,
            "snapshot_sha256": None,
            "input_fingerprint": None,
            "as_of_date": None,
        },
        "bge": {
            "receipt_schema": None,
            "model_id": None,
            "model_revision": None,
            "embedding_dim": None,
            "encoder_contract": None,
            "document_kind": None,
            "document_version": None,
            "artifact_sha256": None,
            "npz_sha256": None,
            "provider_calls": None,
            "event_count": None,
            "encoded_event_count": None,
            "reused_event_count": None,
            "prototype_count": None,
            "build_mode": None,
            "single_vector_contract": False,
            "coverage_complete": False,
        },
        "contracts": {
            "collection_prototype_bank_sha256": None,
            "collection_head_sha256": None,
            "unusual_prototype_bank_sha256": None,
            "unusual_classifier_sha256": None,
            "document_contract_sha256": None,
            "visible_output_sha256": visible_hash,
        },
        "publication": {
            "manifest_schema": None,
            "manifest_sha256": None,
            "cache_schema": None,
            "cache_sha256": None,
            "delivery_status": "blocked",
            "migration": False,
            "last_good_fallback": False,
            "builder_published": False,
            "expected": False,
            "indexable": False,
            "canonical_path": "/neobychnoe/",
        },
        "quality": {"status": "unavailable", "evidence_kind": None, "metrics": {}},
        "feed": {
            "selected_count": 0,
            "target_count": target,
            "minimum_publish_count": minimum,
            "ordered_visible_event_ids": [],
            "ordered_visible_concept_ids": [],
            "visible_event_ids": [],
            "visible_concept_ids": [],
            "selected": [],
            "candidate_count": 0,
            "near_threshold": [],
            "exclusions": [],
            "duplicates": [],
            "expired": [],
            "visible_output_sha256": visible_hash,
        },
        "findings": {"errors": [{"code": _text(code, limit=120), "message": _text(message)}], "warnings": []},
        "closure": {"required_consecutive_runs": 2, "consecutive_healthy_ready_runs": 0, "eligible_to_close": False},
    }


def evaluate_health(
    *,
    bge_receipt: Mapping[str, Any],
    unusual_manifest: Mapping[str, Any],
    unusual_cache: Mapping[str, Any],
    builder_receipt: Mapping[str, Any],
    artifact_sha256s: Mapping[str, str] | None = None,
    target_count: int = DEFAULT_TARGET_COUNT,
    minimum_count: int = DEFAULT_MINIMUM_COUNT,
    previous_health: Mapping[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if minimum_count < 1 or target_count < minimum_count or target_count > MAX_SELECTED:
        raise ContractError("count_policy_invalid")
    now = _iso(generated_at) if generated_at else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if now is None:
        raise ContractError("generated_at_invalid")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    builder = _builder_parts(builder_receipt, errors)
    bge = _validate_bge(bge_receipt, errors)
    if unusual_manifest.get("schema_version") not in MANIFEST_SCHEMAS:
        _add(errors, "manifest.schema", "Unusual manifest schema mismatch.")
    if unusual_cache.get("schema_version") != CACHE_SCHEMA:
        _add(errors, "cache.schema", "Unusual cache schema mismatch.")
    if _int(_value(unusual_manifest.get("provider_calls"), 0)) != 0:
        _add(errors, "manifest.provider_calls", "Unusual manifest reports provider calls.")
    if _int(_value(unusual_cache.get("provider_calls"), 0)) != 0:
        _add(errors, "cache.provider_calls", "Unusual cache reports provider calls.")
    feed = _normalize_feed(
        unusual_manifest,
        target=target_count,
        minimum=minimum_count,
        errors=errors,
        warnings=warnings,
    )
    quality = _quality(unusual_manifest)
    delivery = _text(_value(unusual_manifest.get("delivery_status"), unusual_manifest.get("status"))).lower()
    accepted_quality = quality["status"] in {"approved", "pass", "ready"}
    fallback = delivery == "last_good_fallback"
    if not accepted_quality:
        _add(errors, "quality.not_approved", "Unusual quality gate is not approved.")
    if delivery in {"blocked", "disabled", "shadow", "failed", "not_approved"}:
        _add(errors, "publication.blocked", "Unusual publication is explicitly blocked.")
    if fallback:
        _add(warnings, "publication.last_good_fallback", "Publication is using a compatible last-good fallback.")
    migration_value = unusual_manifest.get("migration")
    migration = bool(_mapping(migration_value).get("enabled")) if isinstance(migration_value, Mapping) else bool(migration_value)
    if migration:
        _add(warnings, "publication.migration", "Migration mode is enabled; notification eligibility remains silent.")
    if feed["selected_count"] < minimum_count and accepted_quality:
        _add(errors, "feed.below_minimum", "Approved feed is below the provisional publication minimum.")

    semantic = builder["semantic"]
    supplied = dict(artifact_sha256s or {})
    manifest_sha = _sha(supplied.get("unusual_manifest"))
    cache_sha = _sha(supplied.get("unusual_cache"))
    bge_receipt_sha = _sha(supplied.get("bge_receipt"))
    if manifest_sha is None or cache_sha is None or bge_receipt_sha is None:
        _add(errors, "artifacts.sha256", "Exact artifact file SHA-256 values are required.")
    expected_pairs = (
        ("manifest_sha256", manifest_sha, "manifest"),
        ("unusual_cache_sha256", cache_sha, "cache"),
        ("vector_receipt_sha256", bge_receipt_sha, "bge_receipt"),
    )
    for semantic_key, observed, label in expected_pairs:
        declared = _sha(semantic.get(semantic_key))
        if declared is None or observed != declared:
            _add(errors, f"identity.{label}_sha256", f"Builder semantic receipt does not bind the {label} artifact.")
    if _sha(semantic.get("artifact_sha256")) != bge.get("artifact_sha256"):
        _add(errors, "identity.bge_artifact", "Builder and BGE artifact identities differ.")
    if _int(semantic.get("provider_calls")) != 0:
        _add(errors, "identity.provider_calls", "Builder semantic receipt does not prove zero provider calls.")
    if _int(semantic.get("artifact_event_count")) != bge.get("event_count"):
        _add(errors, "identity.event_count", "Builder and BGE event counts differ.")
    semantic_fingerprint = _sha(semantic.get("input_fingerprint"))
    if semantic_fingerprint != builder.get("input_fingerprint"):
        _add(errors, "identity.semantic_fingerprint", "Builder semantic fingerprint differs from the builder receipt.")
    manifest_fingerprint = _sha(_value(unusual_manifest.get("input_fingerprint"), _mapping(unusual_manifest.get("build")).get("input_fingerprint")))
    if manifest_fingerprint != builder.get("input_fingerprint"):
        _add(errors, "identity.manifest_fingerprint", "Unusual manifest fingerprint differs from the builder receipt.")
    manifest_snapshot = _sha(_value(unusual_manifest.get("source_snapshot_hash"), _mapping(unusual_manifest.get("build")).get("source_snapshot_hash")))
    if manifest_snapshot != builder.get("snapshot_sha256"):
        _add(errors, "identity.manifest_snapshot", "Unusual manifest snapshot differs from the builder receipt.")
    bge_build = bge.get("build") or {}
    if _sha(bge_build.get("input_fingerprint")) != builder.get("input_fingerprint"):
        _add(errors, "identity.bge_fingerprint", "BGE build fingerprint differs from the builder receipt.")
    if _sha(bge_build.get("source_snapshot_hash")) != builder.get("snapshot_sha256"):
        _add(errors, "identity.bge_snapshot", "BGE snapshot differs from the builder receipt.")
    manifest_build_id = _text(_value(unusual_manifest.get("build_id"), _mapping(unusual_manifest.get("build")).get("build_id")))
    if manifest_build_id != builder.get("build_id"):
        _add(errors, "identity.build_id", "Unusual manifest build identity differs from the builder receipt.")

    content_readiness = "BLOCKED" if errors and any(
        not item["code"].startswith("feed.below_minimum") for item in errors
    ) else "NOT_READY" if errors else "READY"
    health_status = "INCIDENT" if errors else "WATCH" if warnings else "HEALTHY"
    previous = previous_health if isinstance(previous_health, Mapping) else {}
    previous_closure = _mapping(previous.get("closure"))
    previous_streak = _int(previous_closure.get("consecutive_healthy_ready_runs")) or 0
    previous_source = _mapping(previous.get("source"))
    distinct = not (
        previous_source.get("run_id") == builder.get("run_id")
        and previous_source.get("build_id") == builder.get("build_id")
        and builder.get("run_id") is not None
    )
    if health_status == "HEALTHY" and content_readiness == "READY" and distinct:
        streak = previous_streak + 1
    elif health_status == "HEALTHY" and content_readiness == "READY":
        streak = previous_streak
        _add(warnings, "closure.duplicate_run", "The same builder run cannot advance closure readiness.")
        health_status = "WATCH"
    else:
        streak = 0
    eligible_to_close = health_status == "HEALTHY" and content_readiness == "READY" and streak >= 2
    document_hash = _stable_hash({"kind": bge.get("document_kind"), "version": bge.get("document_version")})
    manifest_prototype = _sha(_value(unusual_manifest.get("prototype_bank_sha256"), unusual_manifest.get("prototype_bank_hash")))
    manifest_classifier = _sha(_value(unusual_manifest.get("classifier_sha256"), unusual_manifest.get("classifier_hash")))
    as_of = _text(_value(unusual_manifest.get("as_of_date"), _mapping(unusual_manifest.get("build")).get("as_of_date")), limit=32) or None
    result = {
        "schema_version": HEALTH_SCHEMA,
        "generated_at": now,
        "health_status": health_status,
        "content_readiness": content_readiness,
        "repo_sha": builder.get("repo_sha"),
        "run_id": builder.get("run_id"),
        "as_of_date": as_of,
        "source": {
            "builder_receipt_schema": builder.get("schema"),
            "repo_sha": builder.get("repo_sha"),
            "run_id": builder.get("run_id"),
            "build_id": builder.get("build_id"),
            "snapshot_id": builder.get("snapshot_id"),
            "snapshot_sha256": builder.get("snapshot_sha256"),
            "input_fingerprint": builder.get("input_fingerprint"),
            "as_of_date": as_of,
        },
        "bge": {
            "receipt_schema": bge_receipt.get("schema_version"),
            **{key: bge.get(key) for key in (
                "model_id", "model_revision", "embedding_dim", "encoder_contract", "document_kind",
                "document_version", "artifact_sha256", "npz_sha256", "provider_calls", "event_count",
                "encoded_event_count", "reused_event_count", "prototype_count", "build_mode",
                "single_vector_contract", "coverage_complete",
            )},
        },
        "contracts": {
            "collection_prototype_bank_sha256": bge.get("prototype_bank_sha256"),
            "collection_head_sha256": bge.get("classifier_sha256"),
            "unusual_prototype_bank_sha256": manifest_prototype,
            "unusual_classifier_sha256": manifest_classifier,
            "document_contract_sha256": document_hash,
            "visible_output_sha256": feed["visible_output_sha256"],
        },
        "publication": {
            "manifest_schema": unusual_manifest.get("schema_version"),
            "manifest_sha256": manifest_sha,
            "cache_schema": unusual_cache.get("schema_version"),
            "cache_sha256": cache_sha,
            "delivery_status": delivery or quality["status"],
            "migration": migration,
            "last_good_fallback": fallback,
            "builder_published": builder.get("published", False),
            "expected": accepted_quality and delivery not in {"blocked", "disabled", "shadow", "failed", "not_approved"} and feed["selected_count"] >= minimum_count,
            "indexable": unusual_manifest.get("indexable") is True,
            "canonical_path": "/neobychnoe/",
        },
        "quality": quality,
        "feed": feed,
        "findings": {"errors": errors, "warnings": warnings},
        "closure": {
            "required_consecutive_runs": 2,
            "consecutive_healthy_ready_runs": streak,
            "eligible_to_close": eligible_to_close,
        },
    }
    return result


def render_markdown(health: Mapping[str, Any]) -> str:
    source = _mapping(health.get("source"))
    feed = _mapping(health.get("feed"))
    closure = _mapping(health.get("closure"))
    findings = _mapping(health.get("findings"))
    lines = [
        "# Unusual events production health",
        "",
        f"- Health: **{health.get('health_status')}**",
        f"- Content readiness: **{health.get('content_readiness')}**",
        f"- Build: `{source.get('build_id') or 'unavailable'}`",
        f"- Run: `{source.get('run_id') or 'unavailable'}`",
        f"- Feed: **{feed.get('selected_count', 0)}** / target {feed.get('target_count')} (minimum {feed.get('minimum_publish_count')})",
        f"- Closure streak: **{closure.get('consecutive_healthy_ready_runs', 0)} / {closure.get('required_consecutive_runs', 2)}**",
        "",
        "## Findings",
    ]
    all_findings = list(findings.get("errors") or []) + list(findings.get("warnings") or [])
    if not all_findings:
        lines.append("- None.")
    else:
        for item in all_findings[:40]:
            lines.append(f"- `{_text(_mapping(item).get('code'), limit=120)}` — {_text(_mapping(item).get('message'))}")
    lines.extend(["", "Canonical contract: `docs/features/unusual-events/unusual-events-production-health-v1.schema.json`", ""])
    return "\n".join(lines)


def _bundle_artifact(bundle: Mapping[str, Any], key: str) -> tuple[dict[str, Any], str | None]:
    entry = _mapping(_mapping(bundle.get("artifacts")).get(key))
    payload = entry.get("payload")
    if not isinstance(payload, dict):
        raise ContractError(f"resolver_artifact_missing:{key}")
    digest = _sha(entry.get("sha256"))
    if digest is None:
        raise ContractError(f"resolver_artifact_sha256:{key}")
    return payload, digest


def evaluate_bundle(
    bundle: Mapping[str, Any],
    *,
    target_count: int,
    minimum_count: int,
    previous_health: Mapping[str, Any] | None,
    generated_at: str | None,
) -> dict[str, Any]:
    if bundle.get("schema_version") != INPUT_SCHEMA:
        raise ContractError("resolver_schema_invalid")
    request_id = str(bundle.get("request_id") or "")
    if not ID_RE.fullmatch(request_id):
        raise ContractError("resolver_request_id_invalid")
    bge, bge_sha = _bundle_artifact(bundle, "bge_receipt")
    manifest, manifest_sha = _bundle_artifact(bundle, "unusual_manifest")
    cache, cache_sha = _bundle_artifact(bundle, "unusual_cache")
    builder, _ = _bundle_artifact(bundle, "builder_receipt")
    if str(builder.get("run_id") or "") != request_id:
        raise ContractError("resolver_builder_run_id_mismatch")
    return evaluate_health(
        bge_receipt=bge,
        unusual_manifest=manifest,
        unusual_cache=cache,
        builder_receipt=builder,
        artifact_sha256s={
            "bge_receipt": bge_sha or "",
            "unusual_manifest": manifest_sha or "",
            "unusual_cache": cache_sha or "",
        },
        target_count=target_count,
        minimum_count=minimum_count,
        previous_health=previous_health,
        generated_at=generated_at,
    )


def validate_request(value: Mapping[str, Any], *, expected_mode: str | None = None) -> str:
    if value.get("schema_version") != REQUEST_SCHEMA or value.get("accepted") is not True:
        raise ContractError("request_not_accepted")
    mode = str(value.get("run_mode") or "")
    if mode not in {"warm", "cold"} or (expected_mode and mode != expected_mode):
        raise ContractError("request_mode_mismatch")
    request_id = str(value.get("request_id") or "")
    if not ID_RE.fullmatch(request_id):
        raise ContractError("request_id_invalid")
    return request_id


def _load_previous(path: Path | None) -> dict[str, Any] | None:
    return _load_json(path) if path and path.is_file() else None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    evaluate = sub.add_parser("evaluate", help="Evaluate separate real builder artifacts.")
    evaluate.add_argument("--bge-receipt", type=Path, required=True)
    evaluate.add_argument("--unusual-manifest", type=Path, required=True)
    evaluate.add_argument("--unusual-cache", type=Path, required=True)
    evaluate.add_argument("--builder-receipt", type=Path, required=True)
    bundle = sub.add_parser("evaluate-bundle", help="Evaluate a same-pipeline resolver envelope.")
    bundle.add_argument("--input", type=Path, required=True)
    blocked = sub.add_parser("blocked", help="Emit an honest BLOCKED monitor result.")
    blocked.add_argument("--code", required=True)
    blocked.add_argument("--message", required=True)
    request = sub.add_parser("validate-request", help="Validate same-pipeline request acknowledgement.")
    request.add_argument("--input", type=Path, required=True)
    request.add_argument("--expected-mode", choices=("warm", "cold"))
    for target in (evaluate, bundle, blocked):
        target.add_argument("--output", type=Path, required=True)
        target.add_argument("--markdown-output", type=Path)
        target.add_argument("--previous-health", type=Path)
        target.add_argument("--target-count", type=int, default=DEFAULT_TARGET_COUNT)
        target.add_argument("--minimum-count", type=int, default=DEFAULT_MINIMUM_COUNT)
        target.add_argument("--generated-at")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "validate-request":
        request_id = validate_request(_load_json(args.input), expected_mode=args.expected_mode)
        print(request_id)
        return 0
    generated_at = _iso(args.generated_at) if args.generated_at else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        if args.command == "blocked":
            health = _empty_health(
                code=args.code,
                message=args.message,
                generated_at=generated_at or "",
                target=args.target_count,
                minimum=args.minimum_count,
            )
        elif args.command == "evaluate-bundle":
            health = evaluate_bundle(
                _load_json(args.input),
                target_count=args.target_count,
                minimum_count=args.minimum_count,
                previous_health=_load_previous(args.previous_health),
                generated_at=args.generated_at,
            )
        else:
            health = evaluate_health(
                bge_receipt=_load_json(args.bge_receipt),
                unusual_manifest=_load_json(args.unusual_manifest),
                unusual_cache=_load_json(args.unusual_cache),
                builder_receipt=_load_json(args.builder_receipt),
                artifact_sha256s={
                    "bge_receipt": _file_sha256(args.bge_receipt),
                    "unusual_manifest": _file_sha256(args.unusual_manifest),
                    "unusual_cache": _file_sha256(args.unusual_cache),
                },
                target_count=args.target_count,
                minimum_count=args.minimum_count,
                previous_health=_load_previous(args.previous_health),
                generated_at=args.generated_at,
            )
    except ContractError as exc:
        health = _empty_health(
            code="monitor.input_contract",
            message=str(exc),
            generated_at=generated_at or "",
            target=getattr(args, "target_count", DEFAULT_TARGET_COUNT),
            minimum=getattr(args, "minimum_count", DEFAULT_MINIMUM_COUNT),
        )
    _write_json(args.output, health)
    markdown = render_markdown(health)
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(markdown, encoding="utf-8")
    else:
        print(markdown)
    return 2 if health["health_status"] == "INCIDENT" else 0


if __name__ == "__main__":
    raise SystemExit(main())
