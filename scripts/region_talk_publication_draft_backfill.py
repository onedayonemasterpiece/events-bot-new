#!/usr/bin/env python3
"""Backfill grounded copy for legacy Region Talk social candidates.

Only role-scoped discovery sessions may fetch Telegram source text.  Existing
Gemini acceptance and delivery fields are monotonic: this worker updates draft
and draft-backfill fields only, never replaces the original publication
verdict.  Provider calls use the shared Supabase limiter plus a durable YDB
request/budget ledger.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "kaggle" / "RegionTalkCandidateReport") not in sys.path:
    sys.path.insert(0, str(ROOT / "kaggle" / "RegionTalkCandidateReport"))

import region_talk_candidate_report as rt  # type: ignore  # noqa: E402
from region_talk_llm_runtime import DurableGeminiBudget  # noqa: E402
from scripts import region_talk_goal_notify as notify  # noqa: E402
from scripts import region_talk_publication_finalizer as finalizer  # noqa: E402
from scripts.region_talk_vk_media_prefetch import local_vk_posts, parse_vk_post  # noqa: E402


DRAFT_BACKFILL_VERSION = "region_talk_publication_draft_backfill_v5_source_profile_writer_vnext"
EDITORIAL_WRITER_VERSION = notify.EDITORIAL_WRITER_VERSION
EDITORIAL_INPUT_CONTRACT = "region_talk_editorial_input_v3_source_profile"
EDITORIAL_OUTPUT_CONTRACT = notify.EDITORIAL_OUTPUT_CONTRACT
EDITORIAL_STAGE_EXECUTION_VERSION = "region_talk_writer_v12_publisher_reader_brief_v2"
MEDIA_MATERIALIZATION_CONTRACT_VERSION = "region_talk_media_materialization_v1"
LEGACY_REVIEW_MIGRATION_VERSION = "region_talk_legacy_review_to_v12_v5"
DRAFT_FIELDS = (
    "publication_draft_status",
    "publication_draft_title",
    "publication_draft_source_attribution",
    "publication_draft_telegram_text",
    "publication_draft_vk_text",
    "publication_draft_fact_points_json",
    "publication_draft_prompt_version",
    "publication_draft_contract_version",
    "publication_draft_input_fingerprint",
    "publication_draft_evidence_hash",
    "publication_draft_evidence_json",
    "publication_draft_history_json",
    "publication_draft_editorial_plan_json",
    "publication_draft_grounding_map_json",
    "publication_draft_critic_json",
    "publication_draft_stage_audit_json",
    "publication_draft_generation_attempts",
    "publication_presentation_mode",
    "publication_media_materialization_status",
    "publication_media_materialization_reason",
    "publication_media_materialization_contract_version",
    "publication_presentation_manifest_json",
    "source_profile_fingerprint",
)
SOURCE_ONBOARDING_FIELDS = (
    "source_onboarding_status",
    "source_onboarding_paragraph",
    "source_onboarding_profile_id",
    "source_onboarding_profile_fingerprint",
    "source_onboarding_writer_fingerprint",
    "source_onboarding_writer_prompt_version",
    "source_onboarding_entity_type",
    "source_onboarding_claim_ids_json",
    "source_onboarding_evidence_ids_json",
    "source_onboarding_selected_angle_id",
    "source_onboarding_publisher_dimensions_json",
    "source_onboarding_publisher_dimensions_status",
    "source_onboarding_summary_kind",
    "source_onboarding_llm_status",
    "source_onboarding_llm_reason",
    "source_onboarding_paragraph_chars",
)
TERMINAL_BACKFILL_STATUSES = {
    "ready",
    "llm_not_accepted",
    "needs_grounding_review",
    "source_text_unavailable",
    "unsupported_surface",
}

# These fields exist only in the local row wrapper.  Durable YDB projections
# may intentionally use other leading-underscore fields (for example the live
# source/profile overlay written by the finalizer), and those fields must stay
# inside the compare-and-swap snapshot.
ROW_RUNTIME_ONLY_FIELDS = {
    "_ydb_pk",
    "_ydb_updated_at",
    "_source_onboarding_profile",
    "_candidate_corrections",
    "_strong_read_expected_payload",
    "_strong_live_source_expected_fingerprint",
    "_strong_live_source_exact_pks",
    "_strong_live_source_requires_external_scan",
}


def durable_publication_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in row.items()
        if str(key) not in ROW_RUNTIME_ONLY_FIELDS
    }

_BANNED_COPY_PATTERNS = (
    r"\bуникальн\w*\b",
    r"\bневероятн\w*\b",
    r"\bобязательно\s+посмотр\w*\b",
    r"\bв\s+данной\s+статье\b",
    r"\bв\s+рамках\b",
)

_EDITORIAL_PROVIDER_PACING_LOCK = threading.Lock()
_EDITORIAL_PROVIDER_LAST_CALL = 0.0
_EDITORIAL_PROVIDER_STAGE_DELAY_SECONDS = 0.0


def pace_editorial_provider_call() -> None:
    """Keep sequential editorial stages below the configured project RPM."""

    global _EDITORIAL_PROVIDER_LAST_CALL
    delay = max(0.0, float(_EDITORIAL_PROVIDER_STAGE_DELAY_SECONDS))
    if delay <= 0:
        return
    with _EDITORIAL_PROVIDER_PACING_LOCK:
        now = time.monotonic()
        remaining = delay - (now - _EDITORIAL_PROVIDER_LAST_CALL)
        if _EDITORIAL_PROVIDER_LAST_CALL and remaining > 0:
            time.sleep(remaining)
        _EDITORIAL_PROVIDER_LAST_CALL = time.monotonic()
_FIRST_PERSON_OWNERSHIP = re.compile(
    r"\b(?:мы|наш(?:а|е|и|его|ему|ими)?|нам|нами)\s+"
    r"(?:увидел\w*|заметил\w*|почувствовал\w*|проехал\w*|посетил\w*|снял\w*)\b",
    re.I,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def telegram_post_ref(value: str) -> tuple[str, int] | None:
    normalized = notify.canonical_post_url({"post_url": value})
    match = re.fullmatch(r"https://t\.me/([^/]+)/([0-9]+)", normalized, re.I)
    if not match or match.group(1).lower() in {"c", "joinchat"}:
        return None
    return match.group(1), int(match.group(2))


def social_post_surface(value: str) -> str:
    if telegram_post_ref(value) is not None:
        return "telegram"
    if parse_vk_post(value) is not None:
        return "vk"
    return ""


def content_lane(row: dict[str, Any]) -> str:
    origin = str(row.get("content_origin_type") or "").lower()
    return "article" if origin in {"editorial_publication", "academic_publication"} else "social"


def current_editorial_draft(row: dict[str, Any]) -> bool:
    stored_profile = str(row.get("source_profile_fingerprint") or "").strip()
    live_profile = str(row.get("_live_source_profile_fingerprint") or "").strip()
    return bool(
        notify.is_publication_draft_ready(row)
        and str(row.get("publication_draft_prompt_version") or "") == EDITORIAL_WRITER_VERSION
        and str(row.get("publication_draft_contract_version") or "") == EDITORIAL_OUTPUT_CONTRACT
        and stored_profile
        and live_profile
        and stored_profile == live_profile
    )


def _json_value(value: Any, default: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed if isinstance(parsed, type(default)) else default


def _canonical_url(row: dict[str, Any]) -> str:
    return notify.canonical_post_url({
        "post_url": row.get("post_url") or row.get("canonical_url") or row.get("url")
    })


def source_profile_fingerprint(row: dict[str, Any]) -> str:
    profile = (
        row.get("_source_onboarding_profile")
        if isinstance(row.get("_source_onboarding_profile"), dict)
        else {}
    )
    return str(
        row.get("_live_source_profile_fingerprint")
        or profile.get("profile_fingerprint")
        or profile.get("source_profile_fingerprint")
        or profile.get("profile_hash")
        or row.get("source_onboarding_profile_fingerprint")
        or row.get("source_profile_fingerprint")
        or ""
    ).strip()


def source_profile_reader_brief(row: dict[str, Any]) -> str:
    profile = (
        row.get("_source_onboarding_profile")
        if isinstance(row.get("_source_onboarding_profile"), dict)
        else {}
    )
    projection = _json_value(
        profile.get("copy_projection_json") or profile.get("copy_projection"), {}
    )
    return re.sub(
        r"\s+", " ", str(
            profile.get("reader_brief")
            or profile.get("short_reader_brief")
            or projection.get("short_reader_brief")
            or projection.get("reader_brief")
            or row.get("source_onboarding_paragraph")
            or profile.get("profile_summary")
            or ""
        )
    ).strip()


_PUBLISHER_DIMENSION_SUPPORTS = {
    "outlet_identity": "publisher.identity",
    "intended_audience": "publisher.audience",
    "distinctive_value": "publisher.distinctive_value",
}


def normalized_publisher_dimensions(
    profile: dict[str, Any],
    *,
    fallback_row: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Project imported sidecar dimensions into the Writer evidence schema.

    Publisher sidecars deliberately store their source schema: identity is a
    string while audience/value are evidence-linked arrays.  Older onboarding
    profiles already store ``{text, evidence_ids}`` objects.  The article
    Writer consumes the latter shape, so normalize both without an LLM call.
    """

    raw_dimensions = (
        profile.get("publisher_dimensions_json")
        or profile.get("profile_dimensions")
        or (fallback_row or {}).get("source_onboarding_publisher_dimensions_json")
        or {}
    )
    dimensions = _json_value(raw_dimensions, {})
    if not isinstance(dimensions, dict):
        return {}
    evidence = _json_value(profile.get("evidence") or profile.get("evidence_json"), [])
    evidence = evidence if isinstance(evidence, list) else []

    normalized: dict[str, dict[str, Any]] = {}
    for key in ("outlet_identity", "intended_audience", "distinctive_value"):
        value = dimensions.get(key)
        values = value if isinstance(value, list) else [value]
        texts: list[str] = []
        refs: list[str] = []
        bases: list[str] = []
        for item in values:
            if isinstance(item, dict):
                text = str(
                    item.get("text")
                    or item.get("label")
                    or item.get("summary")
                    or item.get("value")
                    or ""
                ).strip()
                item_refs = item.get("evidence_ids") or item.get("evidence_refs") or []
                if isinstance(item_refs, str):
                    item_refs = [item_refs]
                refs.extend(str(ref).strip() for ref in item_refs if str(ref).strip())
                if str(item.get("basis") or "").strip():
                    bases.append(str(item.get("basis") or "").strip())
            else:
                text = str(item or "").strip()
            if text:
                texts.append(re.sub(r"\s+", " ", text))
        if not refs:
            support = _PUBLISHER_DIMENSION_SUPPORTS.get(str(key), "")
            for item in evidence:
                if not isinstance(item, dict):
                    continue
                supports = item.get("supports") or []
                if isinstance(supports, str):
                    supports = [supports]
                evidence_id = str(item.get("evidence_id") or "").strip()
                if support in supports and evidence_id:
                    refs.append(evidence_id)
        clean_refs = list(dict.fromkeys(refs))
        if texts and clean_refs:
            normalized[str(key)] = {
                "text": "; ".join(texts[:3])[:600].rstrip(),
                "basis": bases[0] if bases else (
                    "explicit" if key == "outlet_identity" else "editorial_inference"
                ),
                "evidence_ids": clean_refs[:12],
            }
    return normalized


def source_profile_ready(row: dict[str, Any]) -> bool:
    profile = (
        row.get("_source_onboarding_profile")
        if isinstance(row.get("_source_onboarding_profile"), dict)
        else {}
    )
    fingerprint = source_profile_fingerprint(row)
    if not fingerprint or not source_profile_reader_brief(row):
        return False
    if profile and str(profile.get("profile_status") or "").lower() != "ready":
        return False
    publisher_profile = bool(
        profile
        and (
            str(profile.get("profile_kind") or "") == "publisher"
            or profile.get("publisher_profile_id")
            or profile.get("profile_dimensions")
        )
    )
    if publisher_profile:
        if not (
            str(profile.get("usable_without_profile_llm") or "").lower() in {"true", "1"}
            or profile.get("usable_without_profile_llm") is True
        ):
            return False
        if str(profile.get("scope") or "").lower() != "external":
            return False
        if str(profile.get("public_copy_eligibility") or "").lower() != "allowed":
            return False
    profile_fp = str(
        profile.get("profile_fingerprint") or profile.get("source_profile_fingerprint") or ""
    ).strip()
    if profile_fp and profile_fp != fingerprint:
        return False
    if content_lane(row) == "article":
        dimensions = normalized_publisher_dimensions(profile, fallback_row=row)
        if not (
            isinstance(dimensions, dict)
            and set(notify.PUBLISHER_READER_BRIEF_DIMENSIONS).issubset(dimensions)
            and all(
                isinstance(dimensions.get(key), dict)
                and str(dimensions[key].get("text") or dimensions[key].get("value") or "").strip()
                and bool(dimensions[key].get("evidence_ids") or dimensions[key].get("evidence_refs"))
                for key in notify.PUBLISHER_READER_BRIEF_DIMENSIONS
            )
        ):
            return False
    return True


def bind_source_profile(row: dict[str, Any], profile: dict[str, Any] | None) -> None:
    """Attach a freshly read reusable profile and its deterministic projection."""

    current = dict(profile or {})
    row["_source_onboarding_profile"] = current
    fingerprint = str(
        current.get("profile_fingerprint")
        or current.get("source_profile_fingerprint")
        or current.get("profile_hash")
        or ""
    ).strip()
    row["_live_source_profile_fingerprint"] = fingerprint
    if not current:
        return
    reader_brief = source_profile_reader_brief(row)
    if reader_brief:
        row["source_onboarding_paragraph"] = reader_brief
    if fingerprint:
        row["source_onboarding_profile_fingerprint"] = fingerprint
    row["source_onboarding_profile_id"] = (
        current.get("source_profile_id") or row.get("source_onboarding_profile_id") or ""
    )
    row["source_onboarding_entity_type"] = (
        current.get("entity_type") or row.get("source_onboarding_entity_type") or "unknown"
    )
    if str(current.get("profile_status") or "").lower() == "ready" and reader_brief:
        row["source_onboarding_status"] = "ready"
    dimensions = current.get("publisher_dimensions_json") or current.get("profile_dimensions")
    if dimensions not in (None, ""):
        normalized_dimensions = normalized_publisher_dimensions(current, fallback_row=row)
        row["source_onboarding_publisher_dimensions_json"] = json.dumps(
            normalized_dimensions, ensure_ascii=False, separators=(",", ":")
        )
        row["source_onboarding_publisher_dimensions_status"] = (
            "ready"
            if set(notify.PUBLISHER_READER_BRIEF_DIMENSIONS).issubset(normalized_dimensions)
            else "needs_review"
        )
        row["source_onboarding_summary_kind"] = notify.PUBLISHER_READER_BRIEF_KIND


MEDIA_EVIDENCE_FIELDS = (
    "selected_media_materialization_json",
    "selected_media_materialization_fingerprint",
    "media_materialization_items_json",
    "publication_primary_image_url",
    "selected_image_url",
    "image_url_or_local_path",
    "associated_image_url",
    "selected_media_ids",
    "media_manifest_items",
    "input_media_manifest_hash",
    "expected_image_count",
    "fetched_image_count",
    "browser_materialization_status",
    "image_queue_status",
    "presentation_recommendation",
    "image_quality_terminality",
    "image_vlm_article_association_supported",
    "image_vlm_best_ordinal",
    "media_kind",
)


def attach_latest_media_evidence(
    publications: list[dict[str, Any]],
    image_rows: list[dict[str, Any]],
) -> None:
    """Fill missing presentation evidence from the durable image ledger.

    ImageDiagnostic and the publication finalizer intentionally keep separate
    audit rows.  A publication row may therefore predate the media-first draft
    contract even though an article hero or a reviewed social-album selection
    already exists.  Copy only missing presentation fields; never overwrite
    publication verdict/copy or newer explicit media.
    """

    latest: dict[str, dict[str, Any]] = {}
    for image_row in image_rows:
        url = _canonical_url(image_row)
        if not url:
            continue
        previous = latest.get(url)
        if previous is None or str(image_row.get("updated_at") or "") >= str(previous.get("updated_at") or ""):
            latest[url] = image_row
    for publication in publications:
        image_row = latest.get(_canonical_url(publication))
        if not image_row:
            continue
        for field in MEDIA_EVIDENCE_FIELDS:
            if publication.get(field) in (None, "", [], {}) and image_row.get(field) not in (None, "", [], {}):
                publication[field] = image_row[field]


# Compatibility for callers/tests written while only article heroes were
# joined.  The behavior is intentionally broader now.
attach_latest_article_media_evidence = attach_latest_media_evidence


def publication_history(rows: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    """Return bounded real publication/approval history, never mere queue rows."""

    selected: list[dict[str, Any]] = []
    for row in rows:
        statuses = {
            str(row.get("status") or "").lower(),
            str(row.get("plan_status") or "").lower(),
            str(row.get("target_publication_status") or "").lower(),
            str(row.get("public_publication_status") or "").lower(),
        }
        published = bool(statuses & {"published", "target_published", "completed"})
        approved = (
            str(row.get("operator_review_decision") or "") == "approved"
            and str(row.get("operator_review_rewrite_status") or "") == "clean"
            and bool(str(row.get("operator_review_fingerprint") or "").strip())
            and str(row.get("operator_review_fingerprint") or "").strip()
            == notify.publication_operator_review_fingerprint(row)
        )
        if not (published or approved):
            continue
        p1, p2 = editorial_paragraphs(str(row.get("publication_draft_telegram_text") or ""))
        editorial_plan = _json_value(row.get("publication_draft_editorial_plan_json"), {})
        selected.append({
            "candidate_url": _canonical_url(row),
            "source": str(row.get("publication_draft_source_attribution") or row.get("source_title") or "")[:220],
            "lane": content_lane(row),
            "paragraph_1": p1[:500],
            "paragraph_2": p2[:500],
            "title": str(row.get("publication_draft_title") or row.get("publication_title") or "")[:180],
            "throughline_mode": str(editorial_plan.get("throughline_mode") or "")[:80],
            "state": "published" if published else "operator_approved",
            "event_at": str(
                row.get("published_at") or row.get("target_published_at")
                or row.get("operator_review_observed_at") or row.get("updated_at") or ""
            ),
        })
    selected.sort(key=lambda item: item["event_at"], reverse=True)
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in selected:
        key = item["candidate_url"]
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item)
        if len(result) >= max(0, min(5, int(limit))):
            break
    return result


def recent_history_requires_fresh_start(history: list[dict[str, Any]]) -> bool:
    """Two transitions among the latest three force a non-template opening."""

    return sum(
        1 for item in history[:3]
        if str(item.get("throughline_mode") or "") in {
            "explicit_transition", "contrast_or_scale_shift",
        }
    ) >= 2


def article_intake_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        keys = {
            str(row.get("external_publication_id") or "").strip(),
            _canonical_url(row),
        } - {""}
        for key in keys:
            previous = index.get(key)
            if previous is None or str(row.get("updated_at") or "") >= str(previous.get("updated_at") or ""):
                index[key] = row
    return index


def reusable_profile_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index social and publisher profiles across domain/web key projections."""

    index: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        keys = {
            str(row.get("canonical_source_key") or "").strip().lower(),
            str(row.get("source_key") or "").strip().lower(),
        } - {""}
        domain = str(row.get("source_domain") or row.get("domain") or "").strip().lower()
        if domain:
            keys.update({"domain:" + domain, "web:" + domain})
        for key in list(keys):
            if key.startswith("domain:"):
                keys.add("web:" + key.split(":", 1)[1])
            elif key.startswith("web:"):
                keys.add("domain:" + key.split(":", 1)[1])
        for key in keys:
            previous = index.get(key)
            if previous is None or str(row.get("updated_at") or "") >= str(previous.get("updated_at") or ""):
                index[key] = row
    return index


def correction_index(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        url = _canonical_url(row)
        if url:
            index.setdefault(url, []).append(dict(row))
    return index


def _source_name(row: dict[str, Any], intake: dict[str, Any] | None = None) -> str:
    publication = (intake or {}).get("publication") if isinstance((intake or {}).get("publication"), dict) else {}
    return str(
        row.get("publication_draft_source_attribution")
        or publication.get("source_name")
        or row.get("source_title")
        or "Источник"
    ).strip()[:220]


def build_editorial_evidence(
    row: dict[str, Any],
    *,
    source_text: str = "",
    fetched: dict[str, Any] | None = None,
    intake: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an ID-addressable lossless evidence pack for the writer stages."""

    evidence: list[dict[str, Any]] = []
    source_name = _source_name(row, intake)
    if source_name:
        evidence.append({"evidence_id": "source.name", "kind": "source_profile_fact", "text": source_name})
    source_url = str(row.get("source_url") or "").strip()
    if source_url:
        evidence.append({"evidence_id": "source.url", "kind": "source_profile_fact", "text": source_url})
    onboarding = source_profile_reader_brief(row)
    if source_profile_ready(row) and onboarding:
        evidence.append({
            "evidence_id": "source.profile",
            "kind": "source_profile_fact",
            "text": onboarding,
            "profile_fingerprint": source_profile_fingerprint(row),
        })
    required_publisher_evidence_ids: list[str] = []
    if content_lane(row) == "article":
        profile = row.get("_source_onboarding_profile") if isinstance(row.get("_source_onboarding_profile"), dict) else {}
        # Use the exact same normalized projection as the readiness gate.  The
        # imported publisher sidecar deliberately stores strings/lists while
        # older profiles store {text, evidence_ids} objects.  Reading the raw
        # form here allowed a profile to pass readiness but silently omitted
        # its three required Writer evidence IDs.
        publisher_dimensions = normalized_publisher_dimensions(
            profile,
            fallback_row=row,
        )
        if isinstance(publisher_dimensions, dict):
            for key in ("outlet_identity", "intended_audience", "distinctive_value"):
                value = publisher_dimensions.get(key) if isinstance(publisher_dimensions.get(key), dict) else {}
                text = str(value.get("text") or value.get("value") or "").strip()
                if not text:
                    continue
                evidence_id = "source.publisher." + key
                evidence.append({
                    "evidence_id": evidence_id,
                    "kind": "source_profile_fact",
                    "text": text,
                    "upstream_evidence_ids": [
                        str(ref)
                        for ref in (value.get("evidence_ids") or value.get("evidence_refs") or [])
                        if str(ref)
                    ],
                })
                required_publisher_evidence_ids.append(evidence_id)
    if source_text.strip():
        evidence.append({
            "evidence_id": "content.exact_text",
            "kind": "content_fact",
            "text": source_text.strip(),
            "lossless": True,
        })
    if intake:
        for index, item in enumerate(intake.get("evidence") or [], 1):
            if not isinstance(item, dict):
                continue
            evidence_id = str(item.get("evidence_id") or f"article.evidence.{index}")
            text = str(item.get("paraphrase") or item.get("quote_short") or item.get("text") or "").strip()
            if text:
                evidence.append({
                    "evidence_id": evidence_id,
                    "kind": "content_fact",
                    "text": text,
                    "source_url": item.get("source_url") or item.get("url") or "",
                })
        publication = intake.get("publication") if isinstance(intake.get("publication"), dict) else {}
        for field in ("title", "abstract", "summary"):
            value = str(publication.get(field) or "").strip()
            if value:
                evidence.append({"evidence_id": f"article.{field}", "kind": "content_fact", "text": value})
    visual_ids = []
    if (
        str(row.get("original_photo_evidence") or "").lower() == "true"
        or row.get("selected_image_url") or row.get("publication_primary_image_url")
        or _json_value(row.get("selected_media_materialization_json"), [])
    ):
        evidence.append({
            "evidence_id": "visual.original_media",
            "kind": "visual_observation",
            "text": "У исходного материала есть отобранный и сопоставленный с ним визуальный ряд.",
        })
        visual_ids.append("visual.original_media")
    # Deduplicate by ID without weakening the first, most specific evidence.
    unique: dict[str, dict[str, Any]] = {}
    for item in evidence:
        unique.setdefault(str(item["evidence_id"]), item)
    return {
        "input_contract": EDITORIAL_INPUT_CONTRACT,
        "candidate": {
            "candidate_id": str(row.get("publication_candidate_id") or row.get("external_publication_id") or ""),
            "lane": content_lane(row),
            "url": _canonical_url(row),
            "externality_status": "verified",
        },
        "source_profile": {
            "name": source_name,
            "url": source_url,
            "fingerprint": source_profile_fingerprint(row),
            "reader_brief": onboarding,
        },
        "evidence": list(unique.values()),
        "required_publisher_evidence_ids": required_publisher_evidence_ids,
        "visual_hook_evidence_ids": visual_ids,
        "fetched": dict(fetched or {}),
    }


def editorial_paragraphs(text: str) -> tuple[str, str]:
    body = str(text or "").strip()
    body = re.split(r"\n(?:\*\*|<b>)?(?:Источник|Оригинал)(?:\*\*|</b>)?:", body, maxsplit=1, flags=re.I)[0].strip()
    parts = [part.strip() for part in re.split(r"\n\s*\n", body) if part.strip()]
    return (parts[0] if parts else "", parts[1] if len(parts) > 1 else "")


def validate_editorial_output(
    output: dict[str, Any],
    evidence_ids: set[str],
    *,
    evidence_kinds: dict[str, str] | None = None,
    required_publisher_evidence_ids: set[str] | None = None,
    row: dict[str, Any] | None = None,
) -> list[str]:
    violations: list[str] = []
    public_copy = output.get("public_copy") if isinstance(output.get("public_copy"), dict) else {}
    raw_p1 = str(public_copy.get("paragraph_1") or "").strip()
    raw_p2 = str(public_copy.get("paragraph_2") or "").strip()
    if any(notify.contains_contrastive_not_a_cliche(value) for value in (raw_p1, raw_p2)):
        violations.append("contrastive_not_a_cliche")
    p1 = re.sub(r"\s+", " ", raw_p1).strip()
    p2 = re.sub(r"\s+", " ", raw_p2).strip()
    if not (90 <= len(p1) <= 360):
        violations.append("paragraph_1_length")
    if not (45 <= len(p2) <= 420):
        violations.append("paragraph_2_length")
    if len(p1) + len(p2) > 750:
        violations.append("editorial_copy_too_long")
    violations.extend(notify.validate_publication_body(p1, p2, row=row))
    if row is not None:
        visible_length = _caption_visible_length(row, p1, p2)
        if not (
            notify.PUBLIC_CAPTION_MIN_VISIBLE_CHARS
            <= visible_length
            <= notify.PUBLIC_CAPTION_MAX_VISIBLE_CHARS
        ):
            violations.append(f"caption_visible_length:{visible_length}")
    combined = p1 + " " + p2
    if any(re.search(pattern, combined, re.I) for pattern in _BANNED_COPY_PATTERNS):
        violations.append("banned_lexeme")
    if notify.contains_contrastive_not_a_cliche(combined):
        violations.append("contrastive_not_a_cliche")
    if _FIRST_PERSON_OWNERSHIP.search(combined):
        violations.append("third_person_boundary")
    language_sample = combined
    if row is not None:
        # A Latin-script channel/outlet name is required attribution, not
        # evidence that the surrounding Russian editorial copy changed
        # language. Remove only exact source-owned labels plus @handles.
        source_labels = {
            _source_name(row),
            str(row.get("source_title") or "").strip(),
            str(row.get("source_name") or "").strip(),
            str(row.get("source_username") or "").strip(),
        }
        for label in sorted((value for value in source_labels if value), key=len, reverse=True):
            language_sample = re.sub(re.escape(label), "", language_sample, flags=re.I)
        language_sample = re.sub(r"(?<!\w)@[A-Za-z0-9_]+", "", language_sample)
    cyrillic = len(re.findall(r"[А-Яа-яЁё]", language_sample))
    letters = len(re.findall(r"[A-Za-zА-Яа-яЁё]", language_sample))
    if letters and cyrillic / letters < 0.95:
        violations.append("russian_language")
    grounding = output.get("grounding_map") if isinstance(output.get("grounding_map"), list) else []
    evidence_kinds = dict(evidence_kinds or {})
    for evidence_id in evidence_ids:
        if evidence_id in evidence_kinds:
            continue
        if evidence_id.startswith(("content.", "article.")):
            evidence_kinds[evidence_id] = "content_fact"
        elif evidence_id.startswith("source."):
            evidence_kinds[evidence_id] = "source_profile_fact"
    if not grounding:
        violations.append("grounding_map_missing")
    grounding_by_sentence: dict[tuple[int, int], dict[str, Any]] = {}
    for item in grounding:
        if not isinstance(item, dict):
            violations.append("grounding_map_invalid")
            continue
        try:
            paragraph_index = int(item.get("paragraph_index") or 0)
            sentence_index = int(item.get("sentence_index") or 0)
        except (TypeError, ValueError):
            paragraph_index = sentence_index = 0
        if paragraph_index not in {1, 2} or sentence_index <= 0:
            violations.append("grounding_map_invalid")
        else:
            grounding_by_sentence[(paragraph_index, sentence_index)] = item
        refs = {str(value) for value in (item.get("evidence_ids") or [])}
        if not refs or not refs.issubset(evidence_ids):
            violations.append("unknown_or_empty_evidence_id")
        if item.get("third_person_maintained") is not True:
            violations.append("third_person_not_confirmed")
    paragraph_sentences = {1: notify.editorial_sentences(p1), 2: notify.editorial_sentences(p2)}
    for paragraph_index, sentences in paragraph_sentences.items():
        for sentence_index, sentence in enumerate(sentences, 1):
            item = grounding_by_sentence.get((paragraph_index, sentence_index))
            if item is None:
                violations.append("sentence_grounding_missing")
                continue
            mapped_sentence = re.sub(r"\s+", " ", str(item.get("sentence_text") or "")).strip()
            if mapped_sentence != sentence:
                violations.append("sentence_grounding_text_mismatch")
    hook = grounding_by_sentence.get((1, 1), {})
    hook_refs = {str(value) for value in (hook.get("evidence_ids") or []) if str(value)}
    if not hook_refs or any(evidence_kinds.get(ref) != "content_fact" for ref in hook_refs):
        violations.append("hook_not_grounded_in_content")
    source_sentence = grounding_by_sentence.get((1, 2), {})
    source_refs = {
        str(value) for value in (source_sentence.get("evidence_ids") or []) if str(value)
    }
    if not source_refs or not any(
        evidence_kinds.get(ref) == "source_profile_fact" for ref in source_refs
    ):
        violations.append("source_sentence_not_grounded_in_profile")
    if required_publisher_evidence_ids and not set(required_publisher_evidence_ids).issubset(source_refs):
        # This is a provenance guardrail rather than a keyword proxy for
        # meaning: the Writer and Critic remain responsible for expressing the
        # outlet identity, intended reader and distinctive value naturally.
        violations.append("publisher_reader_brief_not_grounded_in_source_sentence")
    for sentence_index in range(1, len(paragraph_sentences[2]) + 1):
        detail = grounding_by_sentence.get((2, sentence_index), {})
        refs = {str(value) for value in (detail.get("evidence_ids") or []) if str(value)}
        if not refs or not any(evidence_kinds.get(ref) == "content_fact" for ref in refs):
            violations.append("detail_not_grounded_in_content")
    # Required publisher dimensions are a profile-readiness input contract.
    # Compact public copy need not enumerate every dimension, but none may be
    # missing from the supplied evidence pack before Writer starts.
    if required_publisher_evidence_ids and not set(required_publisher_evidence_ids).issubset(evidence_ids):
        violations.append("missing_publisher_reader_brief")
    return sorted(set(violations))


def validate_critic_output(
    critic: dict[str, Any],
    *,
    required_publisher_evidence_ids: set[str] | None = None,
) -> list[str]:
    """Require the LLM critic to attest all publisher-reader dimensions.

    Deterministic code only checks the critic's typed decision contract.  The
    semantic judgment itself stays with the LLM critic.
    """

    if not required_publisher_evidence_ids:
        return []
    checks = (
        critic.get("publisher_reader_brief_checks")
        if isinstance(critic.get("publisher_reader_brief_checks"), dict)
        else {}
    )
    required_checks = (
        "outlet_identity_covered",
        "intended_audience_covered",
        "distinctive_value_covered",
        "useful_for_read_or_skip_decision",
    )
    return [
        "publisher_reader_brief_critic_check_failed"
        for _ in [0]
        if any(checks.get(key) is not True for key in required_checks)
    ]


def _caption_visible_length(row: dict[str, Any], paragraph_1: str, paragraph_2: str) -> int:
    label = notify.publication_source_cta(row)[0]
    return len(notify.public_caption_visible_text(paragraph_1, paragraph_2, label))


def visible_caption_contract(row: dict[str, Any]) -> dict[str, int]:
    fixed_chars = _caption_visible_length(row, "", "")
    return {
        "min_chars": notify.PUBLIC_CAPTION_MIN_VISIBLE_CHARS,
        "max_chars": notify.PUBLIC_CAPTION_MAX_VISIBLE_CHARS,
        "target_min_chars": 320,
        "target_max_chars": 700,
        "fixed_attribution_chars": fixed_chars,
        "required_editorial_chars_min": max(0, 320 - fixed_chars),
        "required_editorial_chars_max": max(0, 700 - fixed_chars),
    }


def caption_length_repair(row: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
    public_copy = output.get("public_copy") if isinstance(output.get("public_copy"), dict) else {}
    p1 = re.sub(r"\s+", " ", str(public_copy.get("paragraph_1") or "")).strip()
    p2 = re.sub(r"\s+", " ", str(public_copy.get("paragraph_2") or "")).strip()
    actual = _caption_visible_length(row, p1, p2)
    target_min = 320
    return {
        "actual_visible_chars": actual,
        "absolute_min_visible_chars": notify.PUBLIC_CAPTION_MIN_VISIBLE_CHARS,
        "target_visible_min_chars": target_min,
        "target_visible_max_chars": 700,
        "required_added_editorial_chars": max(0, target_min - actual),
        "instruction": (
            "Rewrite both grounded paragraphs and reach the numeric target. "
            "Add source-backed specifics from the supplied evidence; do not add generic filler or new facts."
        ),
    }


def render_public_copy(row: dict[str, Any], output: dict[str, Any]) -> tuple[str, str, str]:
    public_copy = output.get("public_copy") if isinstance(output.get("public_copy"), dict) else {}
    raw_p1 = str(public_copy.get("paragraph_1") or "").strip()
    raw_p2 = str(public_copy.get("paragraph_2") or "").strip()
    if any(notify.contains_contrastive_not_a_cliche(value) for value in (raw_p1, raw_p2)):
        raise ValueError("contrastive_not_a_cliche")
    p1 = re.sub(r"\s+", " ", raw_p1).strip()
    p2 = re.sub(r"\s+", " ", raw_p2).strip()
    body_violations = notify.validate_publication_body(p1, p2, row=row)
    if body_violations:
        raise ValueError(",".join(body_violations))
    url = _canonical_url(row)
    source = _source_name(row)
    cta_label, _cta_url, cta_kind = notify.publication_source_cta(row)
    plain = f"{p1}\n\n{p2}\n\n{notify.publication_footer_plain(row)}"
    visible_length = _caption_visible_length(row, p1, p2)
    if not (
        notify.PUBLIC_CAPTION_MIN_VISIBLE_CHARS
        <= visible_length
        <= notify.PUBLIC_CAPTION_MAX_VISIBLE_CHARS
    ):
        raise ValueError(f"caption_visible_length:{visible_length}")
    # The persisted metadata is the renderer input for exact revision identity;
    # it cannot grant publication permission.
    links = json.dumps({
        "source_label": source,
        "original_url": url,
        "cta_label": cta_label,
        "cta_kind": cta_kind,
        "source_profile_fingerprint": source_profile_fingerprint(row),
        "channel_label": notify.REGION_TALK_PUBLIC_CHANNEL_LABEL,
        "channel_url": notify.REGION_TALK_PUBLIC_CHANNEL_URL,
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return plain, plain, links


def publication_candidate_id(row: dict[str, Any]) -> str:
    return str(
        row.get("publication_candidate_id")
        or row.get("external_publication_id")
        or row.get("candidate_id")
        or ""
    ).strip()


def has_published_status(row: dict[str, Any]) -> bool:
    statuses = {
        str(row.get("status") or "").strip().lower(),
        str(row.get("plan_status") or "").strip().lower(),
        str(row.get("target_publication_status") or "").strip().lower(),
        str(row.get("public_publication_status") or "").strip().lower(),
    }
    return bool(statuses & {"published", "target_published", "completed"})


def backfill_is_actionable(
    row: dict[str, Any],
    *,
    now: datetime | None = None,
    surface: str = "all",
    force_regenerate: bool = False,
) -> bool:
    if has_published_status(row) or notify.candidate_has_pending_correction(row):
        return False
    if not notify.is_confirmed_publication(row):
        return False
    if current_editorial_draft(row) and not force_regenerate:
        return False
    row_surface = social_post_surface(str(row.get("post_url") or ""))
    row_lane = content_lane(row)
    if row_lane == "article":
        row_surface = "article"
    if not row_surface or surface not in {"all", row_surface, row_lane}:
        return False
    if force_regenerate:
        # The CLI permits force only with an explicit candidate URL. Once
        # confirmation/surface checks have passed, bypass terminal status and
        # retry cooldown so an operator can repair that exact unpublished row.
        return True
    status = str(row.get("publication_draft_backfill_status") or "").strip().lower()
    row_backfill_version = str(row.get("publication_draft_backfill_version") or "").strip()
    current_backfill = row_backfill_version == DRAFT_BACKFILL_VERSION
    invalid_current_draft = bool(
        current_backfill
        and status == "ready"
        and not notify.is_publication_draft_ready(row)
    )
    if (
        not force_regenerate
        and not invalid_current_draft
        and current_backfill
        and status in TERMINAL_BACKFILL_STATUSES
    ):
        return False
    retry_at = (
        parse_time(row.get("publication_draft_backfill_next_attempt_after"))
        if current_backfill
        else None
    )
    return retry_at is None or retry_at <= (now or utc_now())


def select_rows(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    now: datetime | None = None,
    surface: str = "all",
    force_regenerate: bool = False,
    candidate_urls: set[str] | None = None,
    published_urls: set[str] | None = None,
    published_candidate_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    candidate_urls = set(candidate_urls or ())
    published_urls = set(published_urls or ())
    published_candidate_ids = set(published_candidate_ids or ())
    selected = [
        row for row in rows
        if backfill_is_actionable(
            row, now=now, surface=surface, force_regenerate=force_regenerate
        )
        and (not candidate_urls or notify.canonical_post_url(row) in candidate_urls)
        and notify.canonical_post_url(row) not in published_urls
        and publication_candidate_id(row) not in published_candidate_ids
    ]
    selected.sort(key=lambda row: (
        str(row.get("sent_to_chat") or "").lower() == "true",
        int(row.get("publication_rank") or 999999),
        -float(row.get("publication_score") or row.get("publication_pre_score") or 0),
        notify.canonical_post_url(row),
    ))
    return selected[: max(0, int(limit))]


def select_media_materialization_rows(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    surface: str = "all",
    candidate_urls: set[str] | None = None,
    published_urls: set[str] | None = None,
    published_candidate_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Select current accepted copy blocked solely by its media manifest."""

    candidate_urls = set(candidate_urls or ())
    published_urls = set(published_urls or ())
    published_candidate_ids = set(published_candidate_ids or ())
    selected: list[dict[str, Any]] = []
    for row in rows:
        url = notify.canonical_post_url(row)
        row_lane = content_lane(row)
        row_surface = "article" if row_lane == "article" else social_post_surface(url)
        if (
            not notify.is_confirmed_publication(row)
            or str(row.get("publication_draft_prompt_version") or "") != EDITORIAL_WRITER_VERSION
            or str(row.get("publication_draft_contract_version") or "") != EDITORIAL_OUTPUT_CONTRACT
            or (
                str(row.get("publication_draft_status") or "") != "media_materialization_pending"
                and not (candidate_urls and url in candidate_urls)
            )
            or surface not in {"all", row_surface, row_lane}
            or (candidate_urls and url not in candidate_urls)
            or url in published_urls
            or publication_candidate_id(row) in published_candidate_ids
        ):
            continue
        selected.append(row)
    selected.sort(key=lambda row: (
        int(row.get("publication_rank") or 999999),
        -float(row.get("publication_score") or row.get("publication_pre_score") or 0),
        notify.canonical_post_url(row),
    ))
    return selected[: max(0, int(limit))]


def draft_request_fingerprint(row: dict[str, Any], text: str, *, model: str) -> str:
    payload = {
        "version": DRAFT_BACKFILL_VERSION,
        "post_url": notify.canonical_post_url(row),
        "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "original_llm_decision": row.get("llm_decision") or row.get("publication_llm_decision"),
        "prompt_version": EDITORIAL_WRITER_VERSION,
        "output_contract": EDITORIAL_OUTPUT_CONTRACT,
        "source_profile_fingerprint": source_profile_fingerprint(row),
        "model": model,
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _correction_reason_codes(correction: dict[str, Any]) -> set[str]:
    return {
        str(value).strip().lower()
        for value in _json_value(correction.get("reason_codes") or correction.get("reason_codes_json"), [])
        if str(value).strip()
    }


def candidate_correction_requires_re_adjudication(
    row: dict[str, Any],
    corrections: list[dict[str, Any]],
) -> bool:
    url = _canonical_url(row)
    for correction in corrections:
        if _canonical_url(correction) != url:
            continue
        action = str(
            correction.get("recommended_action")
            or correction.get("candidate_correction_recommended_action")
            or ""
        ).strip().lower()
        status = str(
            correction.get("review_status")
            or correction.get("correction_status")
            or correction.get("status")
            or "pending"
        ).strip().lower()
        live_revalidation = str(
            correction.get("live_revalidation_status")
            or correction.get("revalidation_status")
            or ""
        ).strip().lower()
        regeneration_allowed = correction.get("regeneration_allowed")
        candidate_mutation_allowed = correction.get("candidate_mutation_allowed")
        resolved = status in {
            "approved_external", "resolved_external", "retained_external", "dismissed", "superseded",
        }
        hard_locality = bool(_correction_reason_codes(correction) & {
            "regional_local_edition", "local_correspondent", "federal_brand_not_sufficient",
        })
        if not resolved and (
            hard_locality
            or status in {"unreviewed", "pending", "queued", "needs_review"}
            or live_revalidation == "pending_live_revalidation"
            or regeneration_allowed is False
            or str(regeneration_allowed or "").lower() == "false"
            or candidate_mutation_allowed is False
            or str(candidate_mutation_allowed or "").lower() == "false"
            or action in {"re_adjudicate_externality", "manual_research_review", "block"}
        ):
            return True
    return False


def correction_block_updates(
    row: dict[str, Any], correction: dict[str, Any]
) -> dict[str, Any]:
    """Block only copy generation; preserve the accepted verdict for review."""

    payload = {
        "canonical_url": _canonical_url(correction) or _canonical_url(row),
        "recommended_action": correction.get("recommended_action"),
        "reason_codes": sorted(_correction_reason_codes(correction)),
        "review_status": correction.get("review_status") or correction.get("status") or "pending",
        "requires_live_ydb_revalidation": bool(
            correction.get("requires_live_ydb_revalidation", True)
        ),
    }
    correction_fp = hashlib.sha256(json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    return {
        "publication_draft_status": "blocked_externality_re_adjudication",
        "publication_draft_backfill_status": "needs_externality_re_adjudication",
        "publication_draft_backfill_reason": "candidate_correction_requires_explicit_review",
        "publication_draft_backfill_next_attempt_after": "",
        "publication_draft_backfill_version": DRAFT_BACKFILL_VERSION,
        "publication_draft_backfill_provider_called": "false",
        "publication_draft_backfill_provider_call_count": 0,
        "externality_re_adjudication_status": "pending",
        "candidate_correction_recommended_action": str(
            correction.get("recommended_action") or "re_adjudicate_externality"
        ),
        "candidate_correction_fingerprint": correction_fp,
        "candidate_correction_evidence_json": json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
        "source_profile_fingerprint": source_profile_fingerprint(row),
    }


def strong_read_row(pool: Any, ydb: Any, table: str, pk: str) -> dict[str, Any]:
    """Read one exact ledger row in a serializable transaction before mutation."""

    if not pk:
        return {}
    query_text = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"

    def op(session: Any) -> dict[str, Any]:
        query = session.prepare(query_text)
        result = session.transaction(ydb.SerializableReadWrite()).execute(
            query, {"$pk": pk}, commit_tx=True
        )
        rows = result[0].rows if result else []
        if not rows:
            return {}
        raw = rows[0].payload_json
        payload = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
        payload["_ydb_pk"] = pk
        return payload

    return dict(pool.retry_operation_sync(op) or {})


def strong_read_kind_rows_complete(
    pool: Any,
    ydb: Any,
    table: str,
    kind: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Read a small safety kind in one serializable snapshot or fail incomplete."""

    if not re.fullmatch(r"[A-Za-z0-9_:-]+", kind):
        raise ValueError(f"unsafe YDB kind: {kind!r}")
    max_items = max(1, int(limit))
    prefix = kind + ":"
    prefix_upper = kind + ";"
    query_text = (
        f"DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; "
        f"SELECT pk, payload_json FROM `{table}` "
        f"WHERE pk >= $prefix AND pk < $prefix_upper "
        f"ORDER BY pk LIMIT {max_items + 1};"
    )

    def op(session: Any) -> list[dict[str, Any]]:
        query = session.prepare(query_text)
        result = session.transaction(ydb.SerializableReadWrite()).execute(
            query,
            {"$prefix": prefix, "$prefix_upper": prefix_upper},
            commit_tx=True,
        )
        rows = result[0].rows if result else []
        if len(rows) > max_items:
            raise RuntimeError(f"strong read incomplete for {kind}: limit={max_items}")
        output: list[dict[str, Any]] = []
        for item in rows:
            raw = item.payload_json
            payload = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
            payload["_ydb_pk"] = str(item.pk)
            output.append(payload)
        return output

    return list(pool.retry_operation_sync(op) or [])


def refresh_strong_live_source_fingerprint(
    pool: Any,
    ydb: Any,
    table: str,
    row: dict[str, Any],
    *,
    scan_limit: int,
) -> None:
    """Attach a current authoritative source projection before provider work.

    ``publication_candidate_item`` stores the accepted source fingerprint, but
    the matching *live* fingerprint is a read-time overlay.  A strong reread of
    only the candidate therefore cannot be passed directly to
    ``is_confirmed_publication``.  Read the exact social-source rows (and the
    small external-publication source kind for articles), then retain the read
    identities so the final candidate transaction can repeat the same gate.
    """

    source_key = finalizer.canonical_source_key_for_row(row).strip().lower()
    if not source_key:
        row["_live_authoritative_source_fingerprint"] = ""
        row["_live_authoritative_source_found"] = "false"
        row["_strong_live_source_expected_fingerprint"] = ""
        row["_strong_live_source_exact_pks"] = []
        row["_strong_live_source_requires_external_scan"] = False
        return

    exact_pks = [
        f"{kind}:{source_key}"
        for kind in ("source_queue_item", "source_status_item", "online_source_item")
    ]
    source_rows: list[dict[str, Any]] = []
    for pk in exact_pks:
        item = strong_read_row(pool, ydb, table, pk)
        if item:
            source_rows.append(item)

    requires_external_scan = content_lane(row) == "article"
    if requires_external_scan:
        external_rows = strong_read_kind_rows_complete(
            pool,
            ydb,
            table,
            "external_publication_source_item",
            int(scan_limit),
        )
        source_rows.extend(
            item
            for item in external_rows
            if finalizer.canonical_source_key_for_row(item).strip().lower() == source_key
        )

    notify.attach_live_source_fingerprints([row], source_rows)
    row["_strong_live_source_expected_fingerprint"] = str(
        row.get("_live_authoritative_source_fingerprint") or ""
    )
    row["_strong_live_source_exact_pks"] = exact_pks
    row["_strong_live_source_requires_external_scan"] = requires_external_scan


def upsert_publication_row(
    pool: Any,
    ydb: Any,
    table: str,
    row: dict[str, Any],
    updates: dict[str, Any],
    *,
    correction_limit: int = 5000,
) -> dict[str, Any]:
    """CAS one candidate and correction gate in the same serializable tx."""

    now_iso = utc_now().isoformat()
    pk = str(row.get("_ydb_pk") or "")
    if not pk:
        raise RuntimeError("publication row has no durable YDB primary key")
    expected_payload = row.get("_strong_read_expected_payload")
    if not isinstance(expected_payload, dict):
        expected_payload = durable_publication_payload(row)
    expected_raw = json.dumps(
        expected_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    candidate_query = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"
    correction_prefix = "publisher_profile_candidate_correction_item:"
    correction_query = (
        f"DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; "
        f"SELECT pk, payload_json FROM `{table}` "
        f"WHERE pk >= $prefix AND pk < $prefix_upper "
        f"ORDER BY pk LIMIT {max(1, int(correction_limit)) + 1};"
    )
    source_query = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"
    external_source_prefix = "external_publication_source_item:"
    external_source_query = (
        f"DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; "
        f"SELECT pk, payload_json FROM `{table}` "
        f"WHERE pk >= $prefix AND pk < $prefix_upper "
        f"ORDER BY pk LIMIT {max(1, int(correction_limit)) + 1};"
    )
    upsert_query = f"""
DECLARE $pk AS Utf8;
DECLARE $kind AS Utf8;
DECLARE $payload_json AS Json;
DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    def op(session: Any) -> dict[str, Any]:
        transaction = session.transaction(ydb.SerializableReadWrite())
        current_result = transaction.execute(
            session.prepare(candidate_query), {"$pk": pk}, commit_tx=False
        )
        current_rows = current_result[0].rows if current_result else []
        if len(current_rows) != 1:
            raise RuntimeError("candidate_missing_on_final_serializable_reread")
        raw_current = current_rows[0].payload_json
        current = (
            json.loads(raw_current) if isinstance(raw_current, str) else dict(raw_current or {})
        )
        current_raw = json.dumps(
            current, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        if has_published_status(current):
            raise RuntimeError("candidate_published_before_final_mutation")
        if current_raw != expected_raw:
            raise RuntimeError("candidate_changed_since_strong_reread")

        expected_source_fingerprint = str(
            row.get("_strong_live_source_expected_fingerprint") or ""
        )
        exact_source_pks = [
            str(value)
            for value in (row.get("_strong_live_source_exact_pks") or [])
            if str(value)
        ]
        if exact_source_pks or row.get("_strong_live_source_requires_external_scan"):
            live_source_rows: list[dict[str, Any]] = []
            for source_pk in exact_source_pks:
                source_result = transaction.execute(
                    session.prepare(source_query), {"$pk": source_pk}, commit_tx=False
                )
                rows = source_result[0].rows if source_result else []
                if rows:
                    raw = rows[0].payload_json
                    source = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
                    source["_ydb_pk"] = source_pk
                    live_source_rows.append(source)
            if row.get("_strong_live_source_requires_external_scan"):
                source_result = transaction.execute(
                    session.prepare(external_source_query),
                    {
                        "$prefix": external_source_prefix,
                        "$prefix_upper": external_source_prefix[:-1] + ";",
                    },
                    commit_tx=False,
                )
                external_rows = source_result[0].rows if source_result else []
                if len(external_rows) > max(1, int(correction_limit)):
                    raise RuntimeError(
                        "current external source kind read incomplete before final mutation"
                    )
                source_key = finalizer.canonical_source_key_for_row(current).strip().lower()
                for item in external_rows:
                    raw = item.payload_json
                    source = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
                    source["_ydb_pk"] = str(item.pk)
                    if finalizer.canonical_source_key_for_row(source).strip().lower() == source_key:
                        live_source_rows.append(source)
            live_candidate = dict(current)
            notify.attach_live_source_fingerprints([live_candidate], live_source_rows)
            current_source_fingerprint = str(
                live_candidate.get("_live_authoritative_source_fingerprint") or ""
            )
            if (
                not expected_source_fingerprint
                or current_source_fingerprint != expected_source_fingerprint
                or not notify.is_confirmed_publication(live_candidate)
            ):
                raise RuntimeError("candidate_source_changed_since_strong_reread")

        correction_result = transaction.execute(
            session.prepare(correction_query),
            {"$prefix": correction_prefix, "$prefix_upper": correction_prefix[:-1] + ";"},
            commit_tx=False,
        )
        correction_rows = correction_result[0].rows if correction_result else []
        if len(correction_rows) > max(1, int(correction_limit)):
            raise RuntimeError("current correction kind read incomplete before final mutation")
        corrections: list[dict[str, Any]] = []
        for item in correction_rows:
            raw = item.payload_json
            correction = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
            correction["_ydb_pk"] = str(item.pk)
            corrections.append(correction)
        blocking = next((
            correction for correction in corrections
            if candidate_correction_requires_re_adjudication(current, [correction])
        ), None)
        effective_updates = (
            correction_block_updates({**row, **current}, blocking)
            if blocking is not None
            else dict(updates)
        )
        payload = {**current, **effective_updates, "updated_at": now_iso}
        transaction.execute(
            session.prepare(upsert_query),
            {
                "$pk": pk,
                "$kind": "publication_candidate_item",
                "$payload_json": json.dumps(payload, ensure_ascii=False),
                "$updated_at": now_iso,
            },
            commit_tx=True,
        )
        return effective_updates

    return dict(pool.retry_operation_sync(op) or {})


def build_client(transport: str) -> Any:
    auth_env = notify.TELETHON_TRANSPORT_AUTH_ENVS.get(transport)
    if not auth_env:
        raise RuntimeError(f"unsupported Region Talk Telethon transport: {transport}")
    bundle = notify.decode_discovery_bundle(str(os.getenv(auth_env) or ""))
    api_id = str(os.getenv("TELEGRAM_API_ID") or os.getenv("TG_API_ID") or "").strip()
    api_hash = str(os.getenv("TELEGRAM_API_HASH") or os.getenv("TG_API_HASH") or "").strip()
    if not api_id or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID/API_HASH (or TG_ aliases) are required")
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.sessions import StringSession  # type: ignore
    except Exception as exc:
        raise RuntimeError("Telethon is required for Region Talk draft backfill") from exc
    return TelegramClient(
        StringSession(str(bundle["session"])),
        int(api_id),
        api_hash,
        request_retries=0,
        connection_retries=0,
        retry_delay=0,
        auto_reconnect=False,
        flood_sleep_threshold=0,
        raise_last_call_error=True,
        receive_updates=False,
        sequential_updates=True,
        device_model=str(bundle.get("device_model") or "Region Talk draft backfill"),
        system_version=str(bundle.get("system_version") or "Linux"),
        app_version=str(bundle.get("app_version") or "1.0"),
        lang_code=str(bundle.get("lang_code") or "ru"),
        system_lang_code=str(bundle.get("system_lang_code") or "ru"),
    )


async def fetch_exact_text(client: Any, row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    ref = telegram_post_ref(str(row.get("post_url") or ""))
    if ref is None:
        raise RuntimeError("unsupported Telegram post URL")
    handle, message_id = ref
    message = await client.get_messages(handle, ids=message_id)
    if message is None or int(getattr(message, "id", 0) or 0) != message_id:
        raise RuntimeError("exact Telegram message is unavailable")
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()
    if not text:
        raise RuntimeError("exact Telegram message has no text")
    date = getattr(message, "date", None)
    fields = {
        "handle": handle,
        "post_id": str(message_id),
        "post_date": date.isoformat() if date is not None else str(row.get("post_date") or ""),
    }
    if getattr(message, "video", None) is not None:
        fields["media_kind"] = "video"
    elif getattr(message, "photo", None) is not None:
        fields["media_kind"] = "image"
    return text, fields


def _vk_selected_media_materialization(
    row: dict[str, Any], post: dict[str, Any]
) -> list[dict[str, Any]]:
    """Project a legacy VK visual selection onto durable direct refs.

    Older ImageDiagnostic rows retained the reviewed media ids and content
    hashes but predated the durable refetch locator.  ``wall.getById`` returns
    the exact source attachments, so the draft/media backfill can repair that
    transport evidence without repeating either the visual or editorial LLM.
    """

    photo_urls: list[str] = []
    for attachment in post.get("attachments") or []:
        photo = attachment.get("photo") if isinstance(attachment, dict) else None
        if attachment.get("type") != "photo" or not isinstance(photo, dict):
            continue
        sizes = [item for item in (photo.get("sizes") or []) if isinstance(item, dict) and item.get("url")]
        if sizes:
            best = max(
                sizes,
                key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0),
            )
            photo_urls.append(str(best["url"]))

    selected_ids = [
        str(value) for value in _json_value(row.get("selected_media_ids"), []) if str(value)
    ]
    manifest = [
        item for item in _json_value(row.get("media_manifest_items"), [])
        if isinstance(item, dict)
    ]
    manifest_by_id = {str(item.get("media_id") or ""): item for item in manifest}
    materialized: list[dict[str, Any]] = []
    for output_ordinal, media_id in enumerate(selected_ids[:6], 1):
        match = re.search(r"([0-9]+)$", media_id)
        attachment_ordinal = int(match.group(1)) if match else output_ordinal
        if not (1 <= attachment_ordinal <= len(photo_urls)):
            continue
        source_ref = photo_urls[attachment_ordinal - 1]
        reviewed = str((manifest_by_id.get(media_id) or {}).get("content_sha256") or "")
        locator = {
            "method": "vk_wall_photo_attachment",
            "post_url": _canonical_url(row),
            "media_id": media_id,
            "attachment_ordinal": attachment_ordinal,
            "source_url": source_ref,
        }
        item = {
            "media_id": media_id,
            "ordinal": output_ordinal,
            "kind": "image",
            "source_ref": source_ref,
            "refetch_locator": locator,
        }
        if reviewed:
            item["reviewed_content_sha256"] = reviewed
        item["materialization_fingerprint"] = hashlib.sha256(
            json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        materialized.append(item)
    return materialized


def fetch_vk_text(row: dict[str, Any], posts: dict[str, dict[str, Any]], error: str) -> tuple[str, dict[str, Any]]:
    ref = parse_vk_post(str(row.get("post_url") or ""))
    if ref is None:
        raise RuntimeError("unsupported VK post URL")
    owner_id, post_id = ref
    post = posts.get(f"{owner_id}_{post_id}") or {}
    if not post:
        raise RuntimeError(error or "exact VK post is unavailable")
    if int(post.get("owner_id") or 0) != owner_id or int(post.get("id") or 0) != post_id:
        raise RuntimeError("VK API returned a different post")
    text = str(post.get("text") or "").strip()
    if not text:
        raise RuntimeError("exact VK post has no text")
    timestamp = int(post.get("date") or 0)
    fields = {
        "platform": "vk",
        "post_id": str(post_id),
        "post_date": (
            datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            if timestamp
            else str(row.get("post_date") or "")
        ),
    }
    selected_materialization = _vk_selected_media_materialization(row, post)
    if selected_materialization:
        fields["selected_media_materialization_json"] = json.dumps(
            selected_materialization, ensure_ascii=False, separators=(",", ":")
        )
        fields["selected_media_materialization_fingerprint"] = hashlib.sha256(
            fields["selected_media_materialization_json"].encode("utf-8")
        ).hexdigest()
    return text, fields


def retry_updates(row: dict[str, Any], *, transport: str, reason: str) -> dict[str, Any]:
    attempts = int(row.get("publication_draft_backfill_attempt_count") or 0) + 1
    terminal = attempts >= 3
    return {
        "publication_draft_backfill_status": "source_text_unavailable" if terminal else "retry_due",
        "publication_draft_backfill_reason": reason[:500],
        "publication_draft_backfill_transport": transport,
        "publication_draft_backfill_attempt_count": attempts,
        "publication_draft_backfill_last_attempt_at": utc_now().isoformat(),
        "publication_draft_backfill_next_attempt_after": "" if terminal else (utc_now() + timedelta(hours=24)).isoformat(),
        "publication_draft_backfill_version": DRAFT_BACKFILL_VERSION,
    }


async def collect_source_texts(
    rows: list[dict[str, Any]],
    *,
    transport: str,
    delay_min: float,
    delay_max: float,
) -> tuple[dict[str, tuple[str, dict[str, Any], str]], dict[str, str]]:
    """Fetch exact social text before releasing the Telegram session lease."""

    fetched: dict[str, tuple[str, dict[str, Any], str]] = {}
    errors: dict[str, str] = {}
    telegram_rows = [row for row in rows if social_post_surface(str(row.get("post_url") or "")) == "telegram"]
    if telegram_rows:
        with notify.discovery_session_lease(transport):
            client = build_client(transport)
            await client.connect()
            try:
                if not await client.is_user_authorized():
                    raise RuntimeError(
                        f"{notify.TELETHON_TRANSPORT_AUTH_ENVS[transport]} is not authorized"
                    )
                for index, row in enumerate(telegram_rows):
                    url = notify.canonical_post_url(row)
                    try:
                        text, fields = await fetch_exact_text(client, row)
                        fetched[url] = (text, fields, transport)
                    except Exception as exc:
                        errors[url] = f"{type(exc).__name__}: {str(exc)[:400]}"
                    if index + 1 < len(telegram_rows):
                        await asyncio.sleep(random.uniform(delay_min, delay_max))
            finally:
                await client.disconnect()

    vk_rows = [row for row in rows if social_post_surface(str(row.get("post_url") or "")) == "vk"]
    if vk_rows:
        post_ids = []
        for row in vk_rows:
            ref = parse_vk_post(str(row.get("post_url") or ""))
            if ref:
                post_ids.append(f"{ref[0]}_{ref[1]}")
        posts, vk_error = local_vk_posts(post_ids)
        for row in vk_rows:
            url = notify.canonical_post_url(row)
            try:
                text, fields = fetch_vk_text(row, posts, vk_error)
                fetched[url] = (text, fields, "vk_api")
            except Exception as exc:
                errors[url] = f"{type(exc).__name__}: {str(exc)[:400]}"
    return fetched, errors


def publication_media_plan(row: dict[str, Any]) -> dict[str, Any]:
    """Describe the exact media revision the notifier must materialize.

    Source attribution is the reuse policy.  This function is a product and
    integrity gate only: an association/presentation manifest is never called
    reviewable unless the renderer has an exact URL/path or source-post ref.
    """

    lane = content_lane(row)
    post_url = _canonical_url(row)
    media_kind = str(row.get("media_kind") or "").lower()
    selected_ids = [str(value) for value in _json_value(row.get("selected_media_ids"), []) if str(value)]
    explicit_items: list[dict[str, Any]] = []
    for field in (
        "selected_media_materialization_json", "media_materialization_items_json",
        "publication_media_items", "selected_media", "media_manifest_items",
    ):
        for raw in _json_value(row.get(field), []):
            if not isinstance(raw, dict):
                continue
            locator = raw.get("refetch_locator") if isinstance(raw.get("refetch_locator"), dict) else {}
            ref = str(
                raw.get("url") or raw.get("source_url") or raw.get("local_path")
                or raw.get("path") or raw.get("source_post_url") or raw.get("source_ref")
                or locator.get("source_url") or locator.get("post_url") or ""
            ).strip()
            if ref:
                item = {
                    "media_id": str(raw.get("media_id") or raw.get("id") or ref),
                    "ordinal": int(raw.get("ordinal") or len(explicit_items) + 1),
                    "kind": str(raw.get("kind") or raw.get("media_kind") or "image"),
                    "ref": ref,
                }
                for evidence_key in (
                    "reviewed_content_sha256",
                    "materialization_fingerprint",
                    "refetch_locator",
                ):
                    value = raw.get(evidence_key)
                    if value not in (None, "", {}, []):
                        item[evidence_key] = value
                explicit_items.append(item)
        if field == "selected_media_materialization_json" and explicit_items:
            break
    scalar_ref = str(
        row.get("publication_primary_image_url") or row.get("selected_image_url")
        or row.get("image_url_or_local_path") or row.get("associated_image_url") or ""
    ).strip()

    if lane == "article":
        if scalar_ref and not explicit_items:
            explicit_items.append({"media_id": "hero:1", "ordinal": 1, "kind": "image", "ref": scalar_ref})
        explicit_items.sort(key=lambda item: int(item.get("ordinal") or 0))
        items = explicit_items[:1]
        terminal_fallback = rt.is_external_link_article_candidate(row)
        if not items and terminal_fallback:
            mode = "link_preview_fallback"
            status, reason = "fallback", "article_source_media_terminally_unavailable"
        elif not items:
            mode = "article_hero"
            status, reason = "pending", "associated_article_hero_not_materialized"
        else:
            mode = "article_hero"
            status, reason = "ready", "associated_article_hero_has_exact_ref"
    elif media_kind == "video":
        if scalar_ref and not explicit_items and not (
            telegram_post_ref(post_url) and scalar_ref.startswith(post_url + "#")
        ):
            explicit_items.append({"media_id": "hero:1", "ordinal": 1, "kind": "video", "ref": scalar_ref})
        mode = "social_video"
        items = explicit_items[:1] or ([{"media_id": "source:video", "ordinal": 1, "kind": "video", "ref": post_url}] if telegram_post_ref(post_url) else [])
        status, reason = ("ready", "exact_source_video_ref") if items else ("pending", "source_video_not_materialized")
    else:
        try:
            expected = int(row.get("expected_image_count") or 0)
        except (TypeError, ValueError):
            expected = 0
        photo_led = bool(explicit_items or selected_ids or expected or str(row.get("original_photo_evidence") or "").lower() == "true")
        album = max(len(explicit_items), len(selected_ids), expected) >= 3
        if photo_led and album:
            mode = "social_album"
            if explicit_items:
                items = explicit_items[:6]
            elif telegram_post_ref(post_url):
                # A Telegram grouped post is itself a durable exact refetch
                # locator.  The notifier resolves the anchor's grouped_id,
                # downloads the ordered album and then enforces the actual
                # 3..6 item contract before delivery.  Requiring three fake
                # manifest rows here would keep valid source albums pending
                # even though their exact materialization path already exists.
                items = [{
                    "media_id": value,
                    "ordinal": index,
                    "kind": "image",
                    "ref": post_url,
                } for index, value in enumerate(selected_ids[:6] or ["source:album"], 1)]
            else:
                items = []
            exact_source_album_ref = bool(
                not explicit_items
                and telegram_post_ref(post_url)
                and items
                and str(items[0].get("media_id") or "") == "source:album"
            )
            status, reason = (
                ("ready", "exact_source_album_ref")
                if exact_source_album_ref
                else (("ready", "ordered_source_album_ref") if len(items) >= 3 else ("pending", "ordered_album_3_to_6_not_materialized"))
            )
        elif photo_led:
            if scalar_ref and not explicit_items:
                explicit_items.append({"media_id": "hero:1", "ordinal": 1, "kind": "image", "ref": scalar_ref})
            mode = "social_hero"
            items = explicit_items[:1] or ([{"media_id": selected_ids[0] if selected_ids else "source:hero", "ordinal": 1, "kind": "image", "ref": post_url}] if telegram_post_ref(post_url) else [])
            status, reason = ("ready", "exact_source_hero_ref") if items else ("pending", "source_hero_not_materialized")
        else:
            mode = "link_preview_fallback"
            items = []
            status, reason = "fallback", "no_usable_source_media"
    manifest = {
        "contract_version": MEDIA_MATERIALIZATION_CONTRACT_VERSION,
        "mode": mode,
        "status": status,
        "reason": reason,
        "items": items,
    }
    return manifest


def _stage_prompt(stage: str, payload: dict[str, Any]) -> str:
    common = {
        "product": "Region Talk / О Калининграде говорят",
        "language": "Russian only",
        "contract_version": EDITORIAL_WRITER_VERSION,
        "strict_grounding": "Every factual sentence must cite existing evidence_ids; content and source-profile evidence are separate and must never substitute for each other.",
        "style_guard": "Do not build an adversative contrast from a negation particle followed later in the same sentence by an adversative conjunction. State the intended observation directly.",
    }
    if stage == "strategy":
        task = {
            "task": "Do not write public copy. Return one editorial strategy as JSON.",
            "output": {
                "status": "ready|insufficient_evidence|policy_conflict",
                "throughline_mode": "explicit_transition|contrast_or_scale_shift|fresh_start",
                "source_angle_id": "string",
                "why_this_material_now": "one Russian sentence",
                "opening_device": "Russian instruction",
                "used_history_urls": ["only URLs from channel_context, max 1"],
                "visual_hook_evidence_ids": ["only existing IDs"],
            },
            "rules": [
                "Use a history bridge only when genuinely supported; fresh_start beats a forced transition.",
                "Plan the first sentence as a 45-110 character content-fact hook from the current material; source name, profile or prestige must not pad that hook.",
                "Plan the second sentence as one compact value statement grounded only in the ready reusable source profile. When required_publisher_evidence_ids is non-empty, that one sentence must tell the reader what the outlet is, who it is useful for, and what distinguishes its editorial value.",
                "Paragraph 2 must eventually sell the click through 1-2 specific details without exhausting the original.",
            ],
        }
    elif stage == "writer":
        task = {
            "task": "Write exactly two editorial paragraphs and a sentence-level grounding map as JSON.",
            "output": {
                "status": "draft_ready|insufficient_evidence|policy_conflict",
                "public_copy": {
                    "paragraph_1": "exactly 2 sentences: 45-110 char content hook, then compact source value",
                    "paragraph_2": "1-2 concrete content-detail sentences",
                },
                "grounding_map": [{
                    "paragraph_index": "1|2",
                    "sentence_index": 1,
                    "sentence_text": "exact sentence",
                    "claim_type": "source_profile_fact|content_fact|source_impression|visual_observation|history_bridge",
                    "evidence_ids": ["existing ID"],
                    "third_person_maintained": True,
                }],
            },
            "rules": [
                "Paragraph 1 has exactly two sentences. Sentence 1 is a 45-110 character hook grounded only in content_fact evidence from this material; do not mention or praise the source there. Sentence 2 is a compact source-value statement grounded in source_profile_fact evidence.",
                "For an article with non-empty required_publisher_evidence_ids, sentence 2 must concisely answer all three reader questions: what kind of outlet this is, who will find it useful, and what distinguishes its coverage. Cite every required_publisher_evidence_id on that sentence. A bare attribution such as 'the outlet published this review' is invalid.",
                "Paragraph 2 has exactly 1-2 concrete observations from content_fact evidence, strictly in third person, without exhausting the original.",
                "Every paragraph 2 sentence must cite at least one evidence ID whose kind is content_fact. It may additionally cite visual evidence, but visual.original_media alone is invalid; omit a purely visual sentence when no content_fact supports it.",
                "Do not use first-person plural for another author's experience.",
                "Warm observational editorial tone; no clickbait, PR jargon, dossier or exhaustive summary.",
                "Write body only: no URL, link, CTA, source footer or metatext such as 'публикация позволяет', 'материал представляет ценность' or 'оригинал доступен'. The deterministic renderer owns the linked CTA and channel footer.",
                "Never claim 'известный', 'ведущий', 'главный', 'крупнейший' or 'обязательный'. Finish every sentence; ellipses and truncation artifacts are forbidden.",
                "Express every positive observation directly; the negation-plus-adversative contrast template is forbidden even with punctuation, a dash or a line break between its parts.",
                "Treat input.visible_caption_contract as a hard output schema. Count characters, including spaces and punctuation, and keep the exact rendered visible caption inside its min/max range.",
                "If input.length_repair exists, add at least required_added_editorial_chars across the two paragraphs while preserving every claim's evidence IDs. The retry must meet target_visible_min_chars, not merely the absolute lower boundary.",
                "Mention photos/video only when visual_hook_evidence_ids is non-empty. Source media is intentionally reused with explicit attribution.",
            ],
        }
    else:
        task = {
            "task": "Critique the draft against evidence and strategy. Return JSON only.",
            "output": {
                "status": "pass|rewrite|reject",
                "reason_codes": ["string"],
                "feedback": "concise Russian rewrite instruction",
                "publisher_reader_brief_checks": {
                    "outlet_identity_covered": "boolean",
                    "intended_audience_covered": "boolean",
                    "distinctive_value_covered": "boolean",
                    "useful_for_read_or_skip_decision": "boolean"
                }
            },
            "hard_fails": ["unsupported_claim", "voice_violation", "visual_hallucination", "forced_history_bridge", "wrong_language", "not_exactly_two_paragraphs", "hook_not_content_grounded", "source_sentence_not_profile_grounded", "publisher_reader_brief_incomplete", "paragraph_url", "body_cta_or_metatext", "incomplete_sentence", "unsupported_prestige", "contrastive_not_a_cliche", "missing_publisher_reader_brief"],
            "pass_only_if": "Both paragraphs are grounded, distinct, editorial, specific and motivate opening the original without replacing it. When required_publisher_evidence_ids is non-empty, pass only when paragraph 1 sentence 2 tells the reader what the outlet is, who it is useful for, and what distinguishes it; set all publisher_reader_brief_checks booleans explicitly.",
        }
    return json.dumps({**common, **task, "input": payload}, ensure_ascii=False)


def call_editorial_stage(
    *,
    stage: str,
    payload: dict[str, Any],
    request_fingerprint: str,
    model: str,
    default_env: str,
    budget: DurableGeminiBudget,
) -> tuple[dict[str, Any], bool]:
    stage_fingerprint = hashlib.sha256(f"{request_fingerprint}|{stage}".encode("utf-8")).hexdigest()
    reservation = budget.reserve(stage_fingerprint)
    status = str(reservation.get("status") or "")
    if status == "replay":
        return dict(reservation.get("result") or {}), False
    if status in {"busy", "exhausted"}:
        return {"_stage_status": "rate_limited", "reason": "durable_budget_" + status}, False

    prompt = _stage_prompt(stage, payload)
    try:
        client = rt.get_region_talk_llm_gateway(default_env)
        pace_editorial_provider_call()

        async def invoke() -> tuple[str, Any]:
            return await client.generate_content_async(
                model=model,
                prompt=prompt,
                generation_config={"temperature": 0.2 if stage == "writer" else 0.1, "response_mime_type": "application/json"},
                max_output_tokens=1800,
            )

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            raw, _usage = executor.submit(lambda: asyncio.run(invoke())).result(timeout=75)
        result = rt.parse_llm_json(raw)
        result.update({
            "_stage_status": "ok",
            "_stage": stage,
            "_model": str(getattr(_usage, "model", "") or model),
            "_requested_model": model,
            "_request_fingerprint": stage_fingerprint,
            "_prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "_usage_input_tokens": getattr(_usage, "input_tokens", ""),
            "_usage_output_tokens": getattr(_usage, "output_tokens", ""),
            "_usage_total_tokens": getattr(_usage, "total_tokens", ""),
        })
    except Exception as exc:
        result = {
            "_stage_status": "error", "_stage": stage, "_model": model,
            "_request_fingerprint": stage_fingerprint,
            "_prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "reason": f"{type(exc).__name__}: {str(exc)[:300]}",
        }
    budget.complete(stage_fingerprint, result)
    return result, True


def generate_editorial_draft(
    row: dict[str, Any],
    *,
    evidence_pack: dict[str, Any],
    history: list[dict[str, Any]],
    model: str,
    default_env: str,
    budget: DurableGeminiBudget,
) -> tuple[dict[str, Any], int]:
    if not source_profile_ready(row):
        return ({
            "publication_draft_status": "needs_source_profile",
            "publication_draft_backfill_status": "needs_source_profile",
            "publication_draft_backfill_reason": "ready_reusable_source_profile_required",
            "publication_draft_backfill_next_attempt_after": "",
            "source_profile_fingerprint": source_profile_fingerprint(row),
        }, 0)
    evidence_raw = json.dumps(evidence_pack, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    history_raw = json.dumps(history[:5], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    evidence_hash = hashlib.sha256(evidence_raw.encode("utf-8")).hexdigest()
    strategy_input = {**evidence_pack, "channel_context": history[:5]}
    request_fp = hashlib.sha256(
        json.dumps({
            "version": EDITORIAL_WRITER_VERSION,
            "stage_execution_version": EDITORIAL_STAGE_EXECUTION_VERSION,
            "candidate": evidence_pack.get("candidate"),
            "evidence_hash": evidence_hash,
            "history_hash": hashlib.sha256(history_raw.encode("utf-8")).hexdigest(),
            "model": model,
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    calls = 0
    strategy, called = call_editorial_stage(
        stage="strategy", payload=strategy_input, request_fingerprint=request_fp,
        model=model, default_env=default_env, budget=budget,
    )
    calls += int(called)
    if strategy.get("_stage_status") != "ok" or strategy.get("status") != "ready":
        deferred = strategy.get("_stage_status") != "ok"
        return ({
            "publication_draft_backfill_status": "retry_due" if deferred else "needs_grounding_review",
            "publication_draft_backfill_reason": str(strategy.get("reason") or strategy.get("status") or "strategy_not_ready")[:500],
            "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat() if deferred else "",
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
        }, calls)

    evidence_ids = {str(item.get("evidence_id")) for item in evidence_pack.get("evidence") or [] if isinstance(item, dict)}
    evidence_kinds = {
        str(item.get("evidence_id")): str(item.get("kind") or "")
        for item in evidence_pack.get("evidence") or []
        if isinstance(item, dict) and str(item.get("evidence_id") or "")
    }
    required_publisher_ids = {
        str(value) for value in (evidence_pack.get("required_publisher_evidence_ids") or []) if str(value)
    }
    used_history = set(str(value) for value in (strategy.get("used_history_urls") or []))
    allowed_history = {str(item.get("candidate_url") or "") for item in history}
    if (
        not used_history.issubset(allowed_history)
        or len(used_history) > 1
        or recent_history_requires_fresh_start(history)
    ):
        strategy["throughline_mode"] = "fresh_start"
        strategy["used_history_urls"] = []

    writer_payload = {
        "editorial_plan": strategy,
        "visible_caption_contract": visible_caption_contract(row),
        **evidence_pack,
    }
    writer, called = call_editorial_stage(
        stage="writer", payload=writer_payload, request_fingerprint=request_fp + "|writer1",
        model=model, default_env=default_env, budget=budget,
    )
    calls += int(called)
    if writer.get("_stage_status") != "ok":
        return ({
            "publication_draft_backfill_status": "retry_due",
            "publication_draft_backfill_reason": str(writer.get("reason") or "writer_stage_deferred")[:500],
            "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
        }, calls)
    violations = validate_editorial_output(
        writer, evidence_ids, evidence_kinds=evidence_kinds,
        required_publisher_evidence_ids=required_publisher_ids, row=row,
    )
    attempts = 1
    if violations:
        retry_payload = {
            **writer_payload,
            "previous_draft": writer,
            "deterministic_feedback": violations,
            "length_repair": (
                caption_length_repair(row, writer)
                if any(item.startswith("caption_visible_length:") for item in violations)
                else None
            ),
        }
        writer, called = call_editorial_stage(
            stage="writer", payload=retry_payload, request_fingerprint=request_fp + "|writer2",
            model=model, default_env=default_env, budget=budget,
        )
        calls += int(called)
        attempts = 2
        if writer.get("_stage_status") != "ok":
            return ({
                "publication_draft_backfill_status": "retry_due",
                "publication_draft_backfill_reason": str(writer.get("reason") or "writer_retry_deferred")[:500],
                "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
                "publication_draft_input_fingerprint": request_fp,
                "publication_draft_evidence_hash": evidence_hash,
                "publication_draft_evidence_json": evidence_raw,
                "publication_draft_history_json": history_raw,
            }, calls)
        violations = (
            validate_editorial_output(
                writer, evidence_ids, evidence_kinds=evidence_kinds,
                required_publisher_evidence_ids=required_publisher_ids, row=row,
            )
            if writer.get("_stage_status") == "ok" else ["writer_retry_failed"]
        )
    if violations or writer.get("status") != "draft_ready":
        return ({
            "publication_draft_backfill_status": "needs_grounding_review",
            "publication_draft_backfill_reason": ",".join(violations or [str(writer.get("status") or "writer_not_ready")])[:500],
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_grounding_map_json": json.dumps(writer.get("grounding_map") or [], ensure_ascii=False, separators=(",", ":")),
            # Do not leave a previous generation's pass visible when the
            # current run stopped before Critic.
            "publication_draft_critic_json": "",
            "publication_draft_stage_audit_json": json.dumps({"strategy": strategy, "writer": writer}, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_generation_attempts": attempts,
        }, calls)

    critic_payload = {"editorial_plan": strategy, "draft": writer, **evidence_pack}
    critic, called = call_editorial_stage(
        stage="critic", payload=critic_payload, request_fingerprint=request_fp + "|critic1",
        model=model, default_env=default_env, budget=budget,
    )
    calls += int(called)
    if critic.get("_stage_status") != "ok":
        return ({
            "publication_draft_backfill_status": "retry_due",
            "publication_draft_backfill_reason": str(critic.get("reason") or "critic_stage_deferred")[:500],
            "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_grounding_map_json": json.dumps(writer.get("grounding_map") or [], ensure_ascii=False, separators=(",", ":")),
            "publication_draft_critic_json": json.dumps(critic, ensure_ascii=False, separators=(",", ":")),
        }, calls)
    if critic.get("_stage_status") == "ok" and critic.get("status") == "rewrite" and attempts < 2:
        writer, called = call_editorial_stage(
            stage="writer", payload={**writer_payload, "previous_draft": writer, "critic_feedback": critic},
            request_fingerprint=request_fp + "|writer2critic", model=model, default_env=default_env, budget=budget,
        )
        calls += int(called)
        attempts = 2
        if writer.get("_stage_status") != "ok":
            return ({
                "publication_draft_backfill_status": "retry_due",
                "publication_draft_backfill_reason": str(writer.get("reason") or "critic_rewrite_deferred")[:500],
                "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
                "publication_draft_input_fingerprint": request_fp,
                "publication_draft_evidence_hash": evidence_hash,
                "publication_draft_evidence_json": evidence_raw,
                "publication_draft_history_json": history_raw,
            }, calls)
        violations = validate_editorial_output(
            writer, evidence_ids, evidence_kinds=evidence_kinds,
            required_publisher_evidence_ids=required_publisher_ids, row=row,
        )
        if not violations:
            critic_payload["draft"] = writer
            critic, called = call_editorial_stage(
                stage="critic", payload=critic_payload, request_fingerprint=request_fp + "|critic2",
                model=model, default_env=default_env, budget=budget,
            )
            calls += int(called)
            if critic.get("_stage_status") != "ok":
                return ({
                    "publication_draft_backfill_status": "retry_due",
                    "publication_draft_backfill_reason": str(critic.get("reason") or "critic_retry_deferred")[:500],
                    "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
                    "publication_draft_input_fingerprint": request_fp,
                    "publication_draft_evidence_hash": evidence_hash,
                    "publication_draft_evidence_json": evidence_raw,
                    "publication_draft_history_json": history_raw,
                }, calls)
    if (
        critic.get("_stage_status") != "ok"
        or critic.get("status") != "pass"
        or validate_editorial_output(
            writer, evidence_ids, evidence_kinds=evidence_kinds,
            required_publisher_evidence_ids=required_publisher_ids, row=row,
        )
        or validate_critic_output(
            critic,
            required_publisher_evidence_ids=required_publisher_ids,
        )
    ):
        return ({
            "publication_draft_backfill_status": "needs_grounding_review",
            "publication_draft_backfill_reason": str(critic.get("reason_codes") or critic.get("reason") or "critic_not_passed")[:500],
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_grounding_map_json": json.dumps(writer.get("grounding_map") or [], ensure_ascii=False, separators=(",", ":")),
            "publication_draft_critic_json": json.dumps(critic, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_generation_attempts": attempts,
        }, calls)

    try:
        telegram_text, vk_text, link_meta = render_public_copy(row, writer)
    except ValueError as exc:
        return ({
            "publication_draft_backfill_status": "needs_grounding_review",
            "publication_draft_backfill_reason": str(exc),
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_grounding_map_json": json.dumps(writer.get("grounding_map") or [], ensure_ascii=False, separators=(",", ":")),
            "publication_draft_critic_json": json.dumps(critic, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_stage_audit_json": json.dumps({"strategy": strategy, "writer": writer, "critic": critic}, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_generation_attempts": attempts,
        }, calls)
    media = publication_media_plan(row)
    media_reviewable = media["status"] in {"ready", "fallback"}
    title = str(row.get("publication_draft_title") or row.get("publication_title") or row.get("short_summary") or _source_name(row))[:140]
    fact_points = [{
        "claim": str(item.get("sentence_text") or "")[:500],
        "evidence_ids": item.get("evidence_ids") or [],
    } for item in writer.get("grounding_map") or [] if isinstance(item, dict) and item.get("sentence_text")]
    return ({
        "publication_draft_status": "ready_for_operator_review" if media_reviewable else "media_materialization_pending",
        "publication_draft_title": title,
        "publication_draft_source_attribution": _source_name(row),
        "publication_draft_telegram_text": telegram_text,
        "publication_draft_vk_text": vk_text,
        "publication_draft_fact_points_json": json.dumps(fact_points, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_prompt_version": EDITORIAL_WRITER_VERSION,
        "publication_draft_contract_version": EDITORIAL_OUTPUT_CONTRACT,
        "publication_draft_input_fingerprint": request_fp,
        "publication_draft_evidence_hash": evidence_hash,
        "publication_draft_evidence_json": evidence_raw,
        "publication_draft_history_json": history_raw,
        "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_grounding_map_json": json.dumps(writer.get("grounding_map") or [], ensure_ascii=False, separators=(",", ":")),
        "publication_draft_critic_json": json.dumps(critic, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_stage_audit_json": json.dumps({"strategy": strategy, "writer": writer, "critic": critic}, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_generation_attempts": attempts,
        "publication_draft_link_metadata_json": link_meta,
        "source_profile_fingerprint": source_profile_fingerprint(row),
        "publication_presentation_mode": media["mode"],
        "publication_media_materialization_status": media["status"],
        "publication_media_materialization_reason": media["reason"],
        "publication_media_materialization_contract_version": MEDIA_MATERIALIZATION_CONTRACT_VERSION,
        "publication_presentation_manifest_json": json.dumps(media, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_backfill_status": "ready" if media_reviewable else "media_materialization_pending",
        "publication_draft_backfill_reason": "critic_passed" if media_reviewable else media["reason"],
        "publication_draft_backfill_next_attempt_after": "" if media_reviewable else (utc_now() + timedelta(hours=1)).isoformat(),
    }, calls)


def build_draft_updates(
    row: dict[str, Any],
    *,
    text: str,
    fetched: dict[str, Any],
    source_transport: str,
    intake: dict[str, Any] | None,
    history: list[dict[str, Any]],
    model: str,
    default_env: str,
    budget: DurableGeminiBudget,
) -> tuple[dict[str, Any], bool]:
    generation_row = {**row, **{
        field: fetched[field]
        for field in MEDIA_EVIDENCE_FIELDS
        if fetched.get(field) not in (None, "", [], {})
    }}
    if not source_profile_ready(generation_row):
        fingerprint = draft_request_fingerprint(row, text, model=model)
        return ({
            "publication_draft_status": "needs_source_profile",
            "publication_draft_backfill_status": "needs_source_profile",
            "publication_draft_backfill_reason": "ready_reusable_source_profile_required",
            "publication_draft_backfill_next_attempt_after": "",
            "publication_draft_backfill_transport": source_transport,
            "publication_draft_backfill_attempt_count": int(
                row.get("publication_draft_backfill_attempt_count") or 0
            ) + 1,
            "publication_draft_backfill_last_attempt_at": utc_now().isoformat(),
            "publication_draft_backfill_text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "publication_draft_backfill_request_fingerprint": fingerprint,
            "publication_draft_backfill_version": DRAFT_BACKFILL_VERSION,
            "publication_draft_backfill_provider_called": "false",
            "publication_draft_backfill_provider_call_count": 0,
            "publication_draft_backfill_llm_gate_status": "blocked",
            "source_profile_fingerprint": source_profile_fingerprint(generation_row),
        }, False)
    evidence_pack = build_editorial_evidence(
        generation_row, source_text=text, fetched=fetched, intake=intake,
    )
    verdict, provider_calls = generate_editorial_draft(
        generation_row,
        evidence_pack=evidence_pack,
        history=history,
        model=model,
        default_env=default_env,
        budget=budget,
    )
    fingerprint = str(verdict.get("publication_draft_input_fingerprint") or draft_request_fingerprint(row, text, model=model))
    base_updates = {
        "publication_draft_backfill_transport": source_transport,
        "publication_draft_backfill_attempt_count": int(row.get("publication_draft_backfill_attempt_count") or 0) + 1,
        "publication_draft_backfill_last_attempt_at": utc_now().isoformat(),
        "publication_draft_backfill_text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "publication_draft_backfill_request_fingerprint": fingerprint,
        "publication_draft_backfill_version": DRAFT_BACKFILL_VERSION,
        "publication_draft_backfill_provider_called": str(provider_calls > 0).lower(),
        "publication_draft_backfill_provider_call_count": provider_calls,
        "source_profile_fingerprint": source_profile_fingerprint(generation_row),
        # The writer is a copy stage, not a second publication verdict.
        "publication_draft_backfill_llm_gate_status": "ok" if verdict.get("publication_draft_backfill_status") in {"ready", "needs_grounding_review", "media_materialization_pending"} else "deferred",
        **{
            field: fetched[field]
            for field in MEDIA_EVIDENCE_FIELDS
            if fetched.get(field) not in (None, "", [], {})
        },
    }
    if (
        str(row.get("publication_draft_prompt_version") or "") != EDITORIAL_WRITER_VERSION
        and str(row.get("legacy_review_migration_version") or "") != LEGACY_REVIEW_MIGRATION_VERSION
    ):
        # One-time audit projection requested by the product owner.  It makes
        # every historical candidate eligible for a rewrite, but is never an
        # approval of the new exact copy+media fingerprint.
        base_updates.update({
            "legacy_review_migration_version": LEGACY_REVIEW_MIGRATION_VERSION,
            "legacy_review_migrated_at": utc_now().isoformat(),
            "legacy_principle_status": "approved",
            "legacy_copy_status": "rewrite_requested",
            "legacy_operator_review_fingerprint": str(row.get("operator_review_fingerprint") or ""),
            "legacy_operator_review_decision": str(row.get("operator_review_decision") or ""),
            "legacy_operator_review_rewrite_status": str(row.get("operator_review_rewrite_status") or ""),
            "operator_review_fingerprint": "",
            "operator_review_decision": "pending",
            "operator_review_rewrite_status": "clean",
            "operator_review_positive": False,
            "operator_review_negative": False,
            "operator_review_rewrite_requested": False,
        })
    return ({**base_updates, **verdict}, provider_calls > 0)


def build_media_materialization_updates(
    row: dict[str, Any], *, fetched: dict[str, Any]
) -> dict[str, Any]:
    """Repair only the exact media manifest while preserving accepted copy."""

    media_fields = {
        field: fetched[field]
        for field in MEDIA_EVIDENCE_FIELDS
        if fetched.get(field) not in (None, "", [], {})
    }
    media = publication_media_plan({**row, **media_fields})
    reviewable = media["status"] in {"ready", "fallback"}
    return {
        **media_fields,
        "publication_draft_status": (
            "ready_for_operator_review" if reviewable else "media_materialization_pending"
        ),
        "publication_presentation_mode": media["mode"],
        "publication_media_materialization_status": media["status"],
        "publication_media_materialization_reason": media["reason"],
        "publication_media_materialization_contract_version": MEDIA_MATERIALIZATION_CONTRACT_VERSION,
        "publication_presentation_manifest_json": json.dumps(
            media, ensure_ascii=False, separators=(",", ":")
        ),
        "publication_draft_backfill_status": (
            "ready" if reviewable else "media_materialization_pending"
        ),
        "publication_draft_backfill_reason": (
            "media_materialization_repaired" if reviewable else media["reason"]
        ),
        "publication_draft_backfill_next_attempt_after": (
            "" if reviewable else (utc_now() + timedelta(hours=1)).isoformat()
        ),
        "publication_draft_backfill_provider_called": "false",
        "publication_draft_backfill_provider_call_count": 0,
    }


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    global _EDITORIAL_PROVIDER_STAGE_DELAY_SECONDS
    _EDITORIAL_PROVIDER_STAGE_DELAY_SECONDS = max(
        0.0, float(getattr(args, "stage_delay_seconds", 0.0) or 0.0)
    )
    ydb = driver = pool = table = None
    selected: list[dict[str, Any]] = []
    try:
        ydb, driver, pool, table, rows = notify.read_publication_rows(int(args.scan_limit))
        external_intakes = notify.read_kind_rows(
            pool, ydb, table, "external_publication_intake_item", int(args.scan_limit)
        )
        onboarding_profiles = notify.read_kind_rows(
            pool, ydb, table, "source_onboarding_profile_item", int(args.scan_limit)
        )
        publisher_profiles = notify.read_kind_rows(
            pool, ydb, table, "publisher_profile_item", int(args.scan_limit)
        )
        correction_rows = notify.read_kind_rows(
            pool, ydb, table, "publisher_profile_candidate_correction_item", int(args.scan_limit)
        )
        image_rows = notify.read_kind_rows(
            pool, ydb, table, "image_queue_item", int(args.scan_limit)
        )
        attach_latest_media_evidence(rows, image_rows)
        schedules = notify.read_kind_rows(
            pool, ydb, table, "publication_schedule_item", int(args.history_limit)
        )
        logs = notify.read_kind_rows(
            pool, ydb, table, "publication_log_item", int(args.history_limit)
        )
        logs += notify.read_kind_rows(
            pool, ydb, table, "region_talk_publication_log", int(args.history_limit)
        )
        history = publication_history([*schedules, *logs, *rows], limit=5)
        published_rows = [
            item for item in [*schedules, *logs, *rows] if has_published_status(item)
        ]
        published_urls = {
            notify.canonical_post_url(item)
            for item in published_rows
            if notify.canonical_post_url(item)
        }
        published_candidate_ids = {
            publication_candidate_id(item)
            for item in published_rows
            if publication_candidate_id(item)
        }
        intakes = article_intake_index(external_intakes)
        profiles_by_key = reusable_profile_index([*onboarding_profiles, *publisher_profiles])
        corrections_by_url = correction_index(correction_rows)
        for row in rows:
            source_key = finalizer.canonical_source_key_for_row(row).strip().lower()
            bind_source_profile(row, profiles_by_key.get(source_key))
        candidate_urls = {
            notify.canonical_post_url({"post_url": value})
            for value in (getattr(args, "candidate_url", None) or [])
            if notify.canonical_post_url({"post_url": value})
        }
        if bool(getattr(args, "materialize_only", False)):
            selected = select_media_materialization_rows(
                rows,
                limit=int(args.limit),
                surface=str(args.surface),
                candidate_urls=candidate_urls,
                published_urls=published_urls,
                published_candidate_ids=published_candidate_ids,
            )
        else:
            selected = select_rows(
                rows,
                limit=int(args.limit),
                surface=str(args.surface),
                force_regenerate=bool(getattr(args, "force_regenerate", False)),
                candidate_urls=candidate_urls,
                published_urls=published_urls,
                published_candidate_ids=published_candidate_ids,
            )
        if args.dry_run or not selected:
            return {
                "ok": True,
                "stage": "publication_draft_backfill",
                "dry_run": bool(args.dry_run),
                "selected": [notify.canonical_post_url(row) for row in selected],
                "selected_total": len(selected),
                "ready_total": 0,
                "failed_total": 0,
                "transport": str(args.transport),
                "blocked_corrections": sum(
                    1 for row in selected
                    if candidate_correction_requires_re_adjudication(
                        row, corrections_by_url.get(_canonical_url(row), [])
                    )
                ),
            }

        model = str(args.model or os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.5-flash-lite")
        default_env = str(
            args.default_env_var_name
            or os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME")
            or "GOOGLE_API_KEY3"
        )
        budget = DurableGeminiBudget(
            pool,
            ydb,
            table,
            budget_id=str(args.llm_budget_id),
            budget_max=int(args.llm_budget_max),
            owner_prefix="region-talk-draft-backfill",
        )
        results: list[dict[str, Any]] = []
        for row in selected:
            row["_candidate_corrections"] = corrections_by_url.get(_canonical_url(row), [])
        social_selected = [
            row for row in selected
            if content_lane(row) == "social"
            and not candidate_correction_requires_re_adjudication(
                row, row.get("_candidate_corrections") or []
            )
        ]
        fetched_by_url, fetch_errors = await collect_source_texts(
            social_selected,
            transport=str(args.transport),
            delay_min=float(args.delay_min),
            delay_max=float(args.delay_max),
        )
        for index, row in enumerate(selected):
            selected_ephemeral = {
                key: value
                for key, value in row.items()
                if str(key) in ROW_RUNTIME_ONLY_FIELDS
            }
            live_row = strong_read_row(
                pool, ydb, table, str(row.get("_ydb_pk") or "")
            )
            if not live_row:
                results.append({
                    "post_url": _canonical_url(row),
                    "status": "skipped_missing_on_strong_reread",
                    "provider_called": False,
                })
                continue
            row = {**live_row, **selected_ephemeral, "_ydb_pk": live_row["_ydb_pk"]}
            row["_strong_read_expected_payload"] = durable_publication_payload(live_row)
            url = notify.canonical_post_url(row)
            source_key = finalizer.canonical_source_key_for_row(row).strip().lower()
            profile = profiles_by_key.get(source_key)
            if profile and profile.get("_ydb_pk"):
                profile = strong_read_row(
                    pool, ydb, table, str(profile.get("_ydb_pk") or "")
                )
            bind_source_profile(row, profile)
            refresh_strong_live_source_fingerprint(
                pool,
                ydb,
                table,
                row,
                scan_limit=int(args.scan_limit),
            )
            # The initial scan is discovery only. Refresh the complete safety
            # kind now so a correction created after selection is observed
            # before any Writer provider stage. The final CAS repeats this
            # read inside the same transaction as the candidate mutation.
            live_corrections = strong_read_kind_rows_complete(
                pool,
                ydb,
                table,
                "publisher_profile_candidate_correction_item",
                int(args.scan_limit),
            )
            blocking_correction = next((
                correction for correction in live_corrections
                if candidate_correction_requires_re_adjudication(row, [correction])
            ), None)
            if blocking_correction is not None:
                updates = correction_block_updates(row, blocking_correction)
                updates = upsert_publication_row(
                    pool, ydb, table, row, updates, correction_limit=int(args.scan_limit)
                )
                results.append({
                    "post_url": url,
                    "status": updates["publication_draft_backfill_status"],
                    "provider_called": False,
                })
                continue
            if has_published_status(row) or not notify.is_confirmed_publication(row):
                results.append({
                    "post_url": url,
                    "status": "skipped_after_strong_reread",
                    "provider_called": False,
                })
                continue
            fetched_item = fetched_by_url.get(url)
            intake = intakes.get(str(row.get("external_publication_id") or "")) or intakes.get(url)
            if bool(getattr(args, "materialize_only", False)):
                if content_lane(row) == "article":
                    fetched = {}
                    source_transport = "retained_article_intake"
                elif fetched_item is None:
                    updates = {
                        "publication_draft_backfill_status": "media_materialization_pending",
                        "publication_draft_backfill_reason": fetch_errors.get(url) or "exact source media unavailable",
                        "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
                        "publication_draft_backfill_provider_called": "false",
                        "publication_draft_backfill_provider_call_count": 0,
                    }
                    updates = upsert_publication_row(
                        pool, ydb, table, row, updates, correction_limit=int(args.scan_limit)
                    )
                    results.append({"post_url": url, "status": updates["publication_draft_backfill_status"], "provider_called": False})
                    continue
                else:
                    _text, fetched, source_transport = fetched_item
                updates = build_media_materialization_updates(row, fetched=fetched)
                updates["publication_draft_backfill_transport"] = source_transport
                updates = upsert_publication_row(
                    pool, ydb, table, row, updates, correction_limit=int(args.scan_limit)
                )
                results.append({
                    "post_url": url,
                    "status": updates["publication_draft_backfill_status"],
                    "provider_called": False,
                })
                if index + 1 < len(selected):
                    await asyncio.sleep(random.uniform(float(args.delay_min), float(args.delay_max)))
                continue
            if content_lane(row) == "article":
                if not intake:
                    updates = retry_updates(row, transport="retained_article_intake", reason="retained article evidence unavailable")
                    updates = upsert_publication_row(
                        pool, ydb, table, row, updates, correction_limit=int(args.scan_limit)
                    )
                    results.append({"post_url": url, "status": updates["publication_draft_backfill_status"]})
                    continue
                text, fetched, source_transport = "", {}, "retained_article_intake"
            elif fetched_item is None:
                source_transport = "vk_api" if social_post_surface(url) == "vk" else str(args.transport)
                updates = retry_updates(
                    row,
                    transport=source_transport,
                    reason=fetch_errors.get(url) or "exact source text unavailable",
                )
                updates = upsert_publication_row(
                    pool, ydb, table, row, updates, correction_limit=int(args.scan_limit)
                )
                results.append({"post_url": url, "status": updates["publication_draft_backfill_status"]})
                continue
            else:
                text, fetched, source_transport = fetched_item
            onboarding_updates = {
                field: row.get(field)
                for field in SOURCE_ONBOARDING_FIELDS
                if row.get(field) not in (None, "")
            }
            updates, provider_called = build_draft_updates(
                row,
                text=text,
                fetched=fetched,
                source_transport=source_transport,
                intake=intake,
                history=history,
                model=model,
                default_env=default_env,
                budget=budget,
            )
            updates = {**onboarding_updates, **updates}
            updates = upsert_publication_row(
                pool, ydb, table, row, updates, correction_limit=int(args.scan_limit)
            )
            results.append({
                "post_url": url,
                "status": updates["publication_draft_backfill_status"],
                "provider_called": provider_called,
            })
            if index + 1 < len(selected):
                await asyncio.sleep(random.uniform(float(args.delay_min), float(args.delay_max)))
        return {
            "ok": True,
            "stage": "publication_draft_backfill",
            "dry_run": False,
            "selected_total": len(selected),
            "ready_total": sum(1 for item in results if item["status"] == "ready"),
            "failed_total": sum(1 for item in results if item["status"] != "ready"),
            "transport": str(args.transport),
            "surface": str(args.surface),
            "llm_budget_id": str(args.llm_budget_id),
            "results": results,
        }
    finally:
        if driver is not None:
            driver.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill grounded Region Talk social drafts")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--transport", choices=tuple(notify.TELETHON_TRANSPORT_AUTH_ENVS), default=None)
    parser.add_argument("--surface", choices=("all", "telegram", "vk", "article", "social"), default="all")
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--scan-limit", type=int, default=5000)
    parser.add_argument("--history-limit", type=int, default=5000)
    parser.add_argument("--model", default="")
    parser.add_argument("--default-env-var-name", default="")
    parser.add_argument("--llm-budget-id", default="")
    parser.add_argument("--llm-budget-max", type=int, default=20)
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=5.0)
    parser.add_argument("--stage-delay-seconds", type=float, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--candidate-url", action="append", default=[], help="regenerate only this exact canonical candidate URL; repeatable")
    parser.add_argument("--force-regenerate", action="store_true", help="regenerate a current-version draft; requires --candidate-url")
    parser.add_argument("--materialize-only", action="store_true", help="repair pending exact media manifests without calling the editorial LLM")
    args = parser.parse_args()
    notify.load_env(args.env_file)
    if args.stage_delay_seconds is None:
        args.stage_delay_seconds = float(
            os.getenv("REGION_TALK_DRAFT_BACKFILL_STAGE_DELAY_SECONDS") or "5.5"
        )
    args.transport = args.transport or os.getenv("REGION_TALK_DRAFT_BACKFILL_TRANSPORT") or "telethon_discovery2"
    if args.transport not in notify.TELETHON_TRANSPORT_AUTH_ENVS:
        raise RuntimeError(f"unsupported REGION_TALK_DRAFT_BACKFILL_TRANSPORT: {args.transport}")
    args.limit = max(0, min(10, int(args.limit)))
    if args.force_regenerate and not args.candidate_url:
        raise RuntimeError("--force-regenerate requires at least one --candidate-url")
    if args.force_regenerate and args.materialize_only:
        raise RuntimeError("--force-regenerate and --materialize-only are mutually exclusive")
    args.llm_budget_id = args.llm_budget_id or os.getenv("REGION_TALK_DRAFT_BACKFILL_BUDGET_ID") or utc_now().strftime("region-talk-draft-backfill-%Y%m%d")
    payload = asyncio.run(execute(args))
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
