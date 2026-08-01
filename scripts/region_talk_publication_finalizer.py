#!/usr/bin/env python3
"""Build and verify the Region Talk publication shortlist from live YDB.

This is the bounded finalization side of CandidateReport: it consumes text/vector
rows and RegionTalkImageDiagnostic `actual_scored` rows from YDB, joins the
authoritative source queue, ranks a lightweight publication shortlist, calls
Gemini Lite through the existing Supabase `google_ai_reserve` limiter, writes
`publication_candidate_item` rows back to YDB, and exports a small XLSX for
operator review.
"""
from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import csv
import hashlib
import html
import importlib.util
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
try:
    from openpyxl import Workbook
except ModuleNotFoundError:  # keep live-YDB finalization usable in slim envs
    Workbook = None  # type: ignore[assignment]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "kaggle" / "RegionTalkCandidateReport") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "kaggle" / "RegionTalkCandidateReport"))
import region_talk_candidate_report as rt  # type: ignore  # noqa: E402
from region_talk_llm_runtime import (  # noqa: E402
    DurableGeminiBudget,
    completed_llm_result_is_replayable,
)


POST_URL_NORMALIZATION_VERSION = "region_talk_post_url_v1"
PUBLICATION_FINALIZER_STATE_VERSION = "region_talk_publication_finalizer_v4"
PUBLICATION_ELIGIBILITY_EVIDENCE_STORAGE_MAX_CHARS = 700
AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION = "region_talk_source_fingerprint_v3"
SOURCE_ONBOARDING_EVIDENCE_VERSION = "region_talk_source_onboarding_evidence_v1"
SOURCE_ONBOARDING_PROFILE_PROMPT_VERSION = "region_talk_source_onboarding_profile_v1"
SOURCE_ONBOARDING_WRITER_PROMPT_VERSION = "region_talk_source_onboarding_writer_v1"
SOURCE_ONBOARDING_ENTITY_TYPES = {"person", "collective", "thematic_channel", "media_brand", "unknown"}
PUBLIC_TME_FALLBACK_ENV = "REGION_TALK_ALLOW_PUBLIC_TME_S_FALLBACK"
TERMINAL_PUBLICATION_STATUSES = {
    "gemini_accept",
    "gemini_reject",
    "gemini_needs_review",
}
TERMINAL_CANDIDATE_STATUSES = {
    "llm_confirmed",
    "llm_rejected",
    "llm_needs_review",
    "filtered_before_llm",
    "sent_to_chat",
    "accepted_for_publication",
}
RETRYABLE_PUBLICATION_STATUSES = {
    "gemini_rate_limited",
    "gemini_error",
    "gemini_unknown",
    # ``no_text_for_gemini`` was incorrectly terminal before the media lane
    # completed.  Keep it retryable so existing rows migrate into the exact
    # post text-restore contour without a manual backfill.
    "no_text_for_gemini",
    "text_restore_pending",
}
RETRYABLE_CANDIDATE_STATUSES = {
    "llm_budget_deferred",
    "llm_error",
    "retry_due",
    "awaiting_text_restore",
}
ELIGIBLE_VERDICTS = {"eligible", "accept", "allow", "allowed", "pass"}
REJECT_VERDICTS = {
    "reject",
    "rejected",
    "ineligible",
    "local",
    "local_source",
    "local_region_source",
    "local_kaliningrad_source_for_separate_monitoring",
    "spam",
    "spam_source_reject",
    "spam_or_commercial_hashtag_source",
    "blocked",
}


def gemini_request_fingerprint(row: dict[str, Any], *, model: str) -> str:
    payload = {
        "post_url": normalize_post_url(str(row.get("post_url") or "")),
        "text_hash": hashlib.sha256(str(row.get("text") or row.get("short_summary") or "").encode("utf-8")).hexdigest(),
        "image": [row.get("overall_media_score"), row.get("postcardness_score"), row.get("image_queue_status")],
        # Eligibility uses the complete authoritative-source fingerprint,
        # including monotonic scan counters. Gemini request identity must not:
        # scanning one more post without changing source classification cannot
        # justify another paid provider call.
        "source": gemini_source_context_fingerprint(row),
        "gate": row.get("publication_eligibility_gate_version"),
        "prompt": rt.REGION_TALK_FINAL_VERIFIER_PROMPT_VERSION,
        "model": model,
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def gemini_source_context_fingerprint(row: dict[str, Any]) -> str:
    source = row.get("_authoritative_source")
    if not isinstance(source, dict) or not source:
        # Compatibility for persisted/test rows that predate the dedicated
        # context contract. Live finalizer rows always carry the source join.
        return str(row.get("authoritative_source_fingerprint") or "")
    payload = {
        "version": "region_talk_gemini_source_context_v1",
        "canonical_source_key": canonical_source_key_for_row(source),
        "source_queue_status": source.get("source_queue_status") or "",
        "source_scope": source.get("source_scope") or "",
        "source_geo_class": source.get("source_geo_class") or "",
        "source_topic_class": source.get("source_topic_class") or "",
        "source_quick_class": source.get("source_quick_class") or "",
        "external_blogger_evidence_status": source.get("external_blogger_evidence_status") or "",
        "monitoring_exclusion_reason": source.get("monitoring_exclusion_reason") or "",
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _completed_llm_result_is_replayable(result: dict[str, Any]) -> bool:
    return completed_llm_result_is_replayable(result)


def require_google_genai_runtime() -> None:
    """Fail before reserving product budget when the provider SDK is absent."""

    try:
        available = importlib.util.find_spec("google.genai") is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        available = False
    if not available:
        raise RuntimeError(
            "Region Talk finalizer requires the official google-genai runtime before reserving Gemini budget"
        )


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip(); v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except (TypeError, ValueError):
        return default


def normalize_post_url(url: str) -> str:
    """Return the URL-level identity used by all finalizer joins and state keys."""
    raw = str(url or "").strip()
    if not raw:
        return ""
    candidate = raw if re.match(r"^[a-z][a-z0-9+.-]*://", raw, re.I) else "https://" + raw.lstrip("/")
    try:
        parsed = urllib.parse.urlsplit(candidate)
    except ValueError:
        return raw.rstrip("/")
    host = (parsed.hostname or "").lower()
    parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
    if host in {"t.me", "www.t.me", "telegram.me", "www.telegram.me"}:
        if parts and parts[0].lower() == "s":
            parts = parts[1:]
        if parts and parts[0].lower() not in {"c", "joinchat"} and not parts[0].startswith("+"):
            parts[0] = parts[0].lstrip("@").lower()
        return "https://t.me/" + "/".join(parts) if parts else "https://t.me"
    if host in {"vk.com", "www.vk.com", "m.vk.com"}:
        return "https://vk.com/" + "/".join(parts) if parts else "https://vk.com"
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    return urllib.parse.urlunsplit((scheme, netloc, path, "", ""))


def _telegram_source_url_from_post(url: str) -> str:
    normalized = normalize_post_url(url)
    match = re.match(r"https://t\.me/([^/]+)(?:/[0-9]+)?$", normalized, re.I)
    if not match or match.group(1).lower() in {"c", "joinchat"}:
        return ""
    return "https://t.me/" + match.group(1).lower()


def canonical_source_key_for_row(row: dict[str, Any]) -> str:
    explicit = str(row.get("canonical_source_key") or "").strip()
    for prefix in ("source_queue_item:", "source_status_item:", "online_source_item:"):
        if explicit.startswith(prefix):
            explicit = explicit[len(prefix):]
    if explicit:
        return explicit.lower().rstrip("/")
    source_url = str(
        row.get("source_url")
        or row.get("canonical_url")
        or row.get("normalized_url")
        or row.get("keyword_hit_source_url")
        or ""
    ).strip()
    if not source_url:
        source_url = _telegram_source_url_from_post(str(row.get("post_url") or ""))
    platform = rt.normalize_source_platform(str(row.get("platform") or row.get("platform_guess") or ""), source_url)
    handle = str(
        row.get("handle")
        or row.get("username_or_handle")
        or row.get("username")
        or row.get("recommended_username")
        or ""
    )
    return str(rt.canonical_source_key(platform, handle, source_url) or "").lower().rstrip("/")


def authoritative_source_index(
    source_items: dict[str, dict[str, Any]],
    source_status_items: dict[str, dict[str, Any]],
    online_source_items: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Merge canonical YDB source surfaces and index only by canonical source key."""
    state: dict[str, Any] = {"unified_source_queue": {}}
    rt.merge_ydb_source_queue_status_items(state, source_items, source_status_items, online_source_items)
    indexed: dict[str, dict[str, Any]] = {}
    for source in (state.get("unified_source_queue") or {}).values():
        if not isinstance(source, dict):
            continue
        key = canonical_source_key_for_row(source)
        if key:
            indexed[key] = source
    return indexed


def authoritative_source_fingerprint(source: dict[str, Any] | None) -> str:
    if not isinstance(source, dict) or not source:
        return ""
    payload = {
        "version": AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
        "canonical_source_key": canonical_source_key_for_row(source),
        "source_queue_status": source.get("source_queue_status") or "",
        "source_scope": source.get("source_scope") or "",
        "source_geo_class": source.get("source_geo_class") or "",
        "source_topic_class": source.get("source_topic_class") or "",
        "source_quick_class": source.get("source_quick_class") or "",
        "monitoring_exclusion_reason": source.get("monitoring_exclusion_reason") or "",
        "source_surface_filter_version": source.get("source_surface_filter_version") or "",
        "source_surface_filter_reason": source.get("source_surface_filter_reason") or "",
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _terminal_decision_blocks_text_restore(publication: dict[str, Any]) -> bool:
    """Keep delivered/operator/Gemini verdicts monotonic under stale rows."""
    status = str(publication.get("publication_status") or "").strip().lower()
    candidate_status = str(publication.get("publication_candidate_status") or "").strip().lower()
    llm_decision = str(publication.get("llm_decision") or publication.get("publication_llm_decision") or "").strip().lower()
    if str(publication.get("sent_to_chat") or "").strip().lower() == "true" or candidate_status == "sent_to_chat":
        return True
    # Some historical rows persisted the provider verdict before the normalized
    # publication status.  The decision itself is durable cost/product
    # evidence and must not be reopened merely because a stale projection still
    # says ``awaiting_text_restore``.
    if llm_decision in {"accept", "reject"}:
        return True
    if status in TERMINAL_PUBLICATION_STATUSES or status.startswith("operator_rejected"):
        return True
    # ``filtered_before_llm`` is the one legacy status intentionally allowed
    # to migrate from the former terminal no-text bug.
    if (
        candidate_status in (TERMINAL_CANDIDATE_STATUSES - {"filtered_before_llm"})
        or candidate_status.startswith(("tombstoned", "revoked"))
    ):
        return True
    if str(publication.get("publication_tombstone") or "").strip().lower() == "true":
        return True
    if str(publication.get("publication_revoked") or "").strip().lower() == "true":
        return True
    return False


def finalization_trigger(publication: dict[str, Any] | None, *, now_iso: str, reverify_existing: bool = False) -> str:
    """Classify one normalized URL as never-finalized, retry-due, or inactive."""
    if not publication:
        return "never_finalized"
    status = str(publication.get("publication_status") or "").strip().lower()
    candidate_status = str(publication.get("publication_candidate_status") or "").strip().lower()
    gate_status = str(publication.get("llm_gate_status") or "").strip().lower()
    # A delivered post is immutable for provider-cost purposes. Later source
    # attestation changes may update its audit fields, but must never spend a
    # second Gemini request or masquerade as a newly accepted candidate.
    if str(publication.get("sent_to_chat") or "").strip().lower() == "true" or candidate_status == "sent_to_chat":
        return ""
    if status.startswith("operator_rejected"):
        return ""
    if status in {"no_text_for_gemini", "text_restore_pending"}:
        # Legacy ``no_text_for_gemini`` rows were incorrectly marked
        # ``filtered_before_llm`` and therefore looked terminal through the
        # candidate-status field.  Explicitly reopen them before the generic
        # terminal checks below.
        return "" if _terminal_decision_blocks_text_restore(publication) else "retry_due"
    if reverify_existing:
        return "reverify_requested"
    if status.startswith("eligibility_") or candidate_status.startswith(("tombstoned", "revoked", "eligibility_")):
        return "never_finalized"
    if status in TERMINAL_PUBLICATION_STATUSES or candidate_status in TERMINAL_CANDIDATE_STATUSES:
        return ""
    retryable = (
        status in RETRYABLE_PUBLICATION_STATUSES
        or candidate_status in RETRYABLE_CANDIDATE_STATUSES
        or gate_status in {"rate_limited", "error"}
    )
    if retryable:
        retry_at = _parse_time(publication.get("next_attempt_after"))
        now = _parse_time(now_iso) or datetime.now(timezone.utc)
        return "retry_due" if retry_at is None or retry_at <= now else ""
    # A row without a terminal Gemini/no-text outcome has never been finalized,
    # even if an older producer wrote an unrelated publication status.
    return "never_finalized"


def _publication_by_normalized_url(rows: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows.values():
        normalized = normalize_post_url(str(row.get("post_url") or ""))
        if not normalized:
            continue
        previous = latest.get(normalized)
        row_time = str(row.get("updated_at") or row.get("_ydb_updated_at") or "")
        previous_time = str((previous or {}).get("updated_at") or (previous or {}).get("_ydb_updated_at") or "")
        if previous is None or row_time >= previous_time:
            latest[normalized] = row
    return latest


def telegram_public_text(url: str, *, timeout: int = 15) -> str:
    if not _env_flag(PUBLIC_TME_FALLBACK_ENV, False):
        return ""
    match = re.match(r"https?://t\.me/([^/]+)/([0-9]+)$", normalize_post_url(url), re.I)
    if not match:
        return ""
    handle, post_id = match.group(1), match.group(2)
    try:
        page = requests.get(
            f"https://t.me/s/{handle}/{post_id}",
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        ).text
    except Exception:
        return ""
    marker = f'data-post="{handle}/{post_id}"'
    idx = page.find(marker)
    if idx < 0:
        return ""
    start = page.rfind('<div class="tgme_widget_message_wrap', 0, idx)
    end = page.find('<div class="tgme_widget_message_wrap', idx + 10)
    block = page[start : end if end > 0 else len(page)]
    text_match = re.search(r'<div class="tgme_widget_message_text js-message_text"[^>]*>(.*?)</div>', block, re.S)
    if not text_match:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text_match.group(1))
    text = re.sub(r"<.*?>", "", text)
    return re.sub(r"\n{3,}", "\n\n", html.unescape(text)).strip()


def source_class_guess(title: str, url: str = "", row: dict[str, Any] | None = None) -> str:
    payload = {
        **(row or {}),
        "source_title": title,
        "resolved_title": (row or {}).get("resolved_title") or title,
        "source_url": url or (row or {}).get("source_url") or "",
        "canonical_url": url or (row or {}).get("canonical_url") or (row or {}).get("source_url") or "",
        "handle": url or (row or {}).get("handle") or "",
    }
    terminal = rt.source_local_region_terminal_fields(payload)
    if terminal.get("source_queue_status") == rt.LOCAL_REGION_SOURCE_STATUS:
        return "local_region_source"
    if terminal.get("source_queue_status") == rt.SPAM_SOURCE_STATUS:
        return "spam_source_reject"
    return "nonlocal_travel_or_general_source"


def publication_pre_score(row: dict[str, Any]) -> float:
    nonlocal_bonus = 0.35 if row.get("source_class_guess") == "nonlocal_travel_or_general_source" else -0.2
    video_manual_review = rt.is_video_media_candidate(row)
    visual = float(row.get("overall_media_score") or 0)
    postcard = float(row.get("postcardness_score") or 0)
    candidate = float(row.get("candidate_score") or 0)
    vector = 0.12 if row.get("vector_gate_status") == "vector_accept_candidate" else 0.06
    # Video has no image-model score by design. Keep it below a strong scored
    # photo, but do not rank it as zero-quality before the operator can watch it.
    manual_video = 0.18 if video_manual_review else 0.0
    external_article = rt.uses_external_link_article_lane(row)
    article_quality = float(row.get("external_research_quality_score") or 0) * 0.55 if external_article else 0.0
    return round(
        nonlocal_bonus
        + visual * 0.45
        + postcard * 0.20
        + candidate * 0.15
        + vector
        + manual_video
        + article_quality,
        4,
    )


def source_profile_id(source_key: str) -> str:
    return "rtsp_" + hashlib.sha256(str(source_key or "").encode("utf-8")).hexdigest()[:20]


def _compact_evidence_text(value: Any, limit: int = 500) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit].rstrip()


def build_source_onboarding_evidence(
    source: dict[str, Any] | None,
    source_memory_rows: list[dict[str, Any]],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    """Build a compact, public, auditable evidence pack for one source.

    This stage performs no biographical inference. It only consolidates the
    authoritative source ledger, the externally verified blogger registry and
    a bounded set of authored post excerpts already present in candidate
    memory. Every future claim must cite one of the generated evidence ids.
    """
    source = dict(source or {})
    source_key = canonical_source_key_for_row(source or candidate)
    title = str(source.get("source_title") or source.get("resolved_title") or candidate.get("source_title") or "").strip()
    source_url = str(source.get("source_url") or source.get("canonical_url") or candidate.get("source_url") or "").strip()
    evidence: list[dict[str, Any]] = []

    def add(kind: str, text: Any, *, url: str = "", date: str = "") -> None:
        excerpt = _compact_evidence_text(text)
        if not excerpt:
            return
        identity = (kind, excerpt.lower(), normalize_post_url(url) if url else "")
        if any((item["kind"], str(item["excerpt"]).lower(), str(item.get("url") or "")) == identity for item in evidence):
            return
        evidence.append({
            "evidence_id": f"E{len(evidence) + 1}",
            "kind": kind,
            "excerpt": excerpt,
            "url": normalize_post_url(url) if url else "",
            "date": str(date or "")[:40],
        })

    if title or source_url:
        add("authoritative_source_identity", f"Название: {title}. Публичный адрес: {source_url}.", url=source_url)
    if str(source.get("source_topic_class") or "") in {"editorial_publication", "academic_publication"}:
        add(
            "external_publication_source",
            "Тип источника: " + str(source.get("source_topic_class") or "")
            + ". Внешний по отношению к Калининградской области источник. Основание: "
            + str(source.get("source_externality_basis") or "проверено внешним research-контрактом"),
            url=source_url,
        )

    external_parts = []
    external_field_labels = [
        ("external_blogger_name", "имя/название"),
        ("external_blogger_segment", "тип"),
        ("external_blogger_region_relation", "отношение к региону"),
        ("external_blogger_visit_period", "период поездки"),
        ("external_blogger_locations", "упомянутые места"),
        ("external_blogger_confirmation_basis", "основание подтверждения"),
    ]
    for field, label in external_field_labels:
        value = source.get(field) or candidate.get(field)
        if value not in (None, ""):
            external_parts.append(f"{label}: {value}")
    evidence_url = str(
        source.get("external_blogger_evidence_url")
        or source.get("evidence_url")
        or candidate.get("external_blogger_evidence_url")
        or ""
    )
    if external_parts:
        add("external_open_source_registry", "; ".join(external_parts), url=evidence_url)

    ordered_memory = sorted(
        [dict(row) for row in source_memory_rows if isinstance(row, dict)],
        key=lambda row: str(row.get("post_date") or row.get("updated_at") or ""),
        reverse=True,
    )
    candidate_url = normalize_post_url(str(candidate.get("post_url") or ""))
    if candidate_url:
        # A compacted historical memory row may already own this URL but have
        # no body. Always prefer the current finalizer candidate for that URL;
        # it may contain text just restored by CandidateReport. This keeps the
        # evidence pack auditable without inventing biography or using web
        # fallback, and avoids treating a valid authored post as identity-only.
        ordered_memory = [
            candidate,
            *[
                row for row in ordered_memory
                if normalize_post_url(str(row.get("post_url") or "")) != candidate_url
            ],
        ]
    for row in ordered_memory:
        if len(evidence) >= 8:
            break
        post_url = str(row.get("post_url") or "")
        excerpt = (
            row.get("full_text")
            or row.get("text")
            or row.get("text_excerpt")
            or row.get("short_summary")
            or row.get("why_keep_in_memory")
            or ""
        )
        add("authored_post_excerpt", excerpt, url=post_url, date=str(row.get("post_date") or ""))

    fingerprint_payload = {
        "version": SOURCE_ONBOARDING_EVIDENCE_VERSION,
        "source_key": source_key,
        "title": title,
        "source_url": source_url,
        "evidence": evidence,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    authored_count = sum(1 for item in evidence if item["kind"] == "authored_post_excerpt")
    external_count = sum(
        1 for item in evidence
        if item["kind"] in {"external_open_source_registry", "external_publication_source"}
    )
    status = "sufficient" if source_key and title and (authored_count >= 1 or external_count >= 1) else "insufficient"
    return {
        "source_profile_id": source_profile_id(source_key),
        "canonical_source_key": source_key,
        "source_title": title,
        "source_url": source_url,
        "evidence_status": status,
        "evidence_version": SOURCE_ONBOARDING_EVIDENCE_VERSION,
        "evidence_fingerprint": fingerprint,
        "evidence_pack_json": json.dumps(evidence, ensure_ascii=False, separators=(",", ":")),
        "evidence_items_total": len(evidence),
        "authored_post_evidence_total": authored_count,
        "external_registry_evidence_total": external_count,
        "candidate_post_url": candidate_url,
    }


def _profile_index(rows: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows.values():
        key = str(row.get("canonical_source_key") or "").strip().lower()
        if not key:
            continue
        previous = out.get(key)
        if previous is None or str(row.get("updated_at") or row.get("_ydb_updated_at") or "") >= str(previous.get("updated_at") or previous.get("_ydb_updated_at") or ""):
            out[key] = dict(row)
    return out


def _memory_rows_by_source(memory_rows: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in memory_rows.values():
        key = canonical_source_key_for_row(row)
        if key:
            out.setdefault(key, []).append(dict(row))
    return out


FINALIZER_MEMORY_AUTHORITY_FIELDS = (
    "source_scope", "source_geo_class", "source_topic_class", "source_quick_class",
    "source_queue_status", "kaliningrad_oblast_only_scope", "kaliningrad_mention_role",
    "matched_place_names", "external_geo_mentions", "mentioned_external_regions",
    "mentioned_external_countries", "is_ad_or_promo", "is_multi_region_roundup",
    "is_multi_topic_digest", "is_digest_or_roundup", "current_stage",
    "current_lifecycle_status", "vector_gate_status", "vector_content_type",
    "text_vector_fusion_status", "external_bge_m3_status", "processing_policy_version",
)

EXTERNAL_PUBLICATION_IMAGE_SCOPE_FIELDS = {
    "kaliningrad_oblast_only_scope", "kaliningrad_mention_role", "matched_place_names",
    "external_geo_mentions", "mentioned_external_regions", "mentioned_external_countries",
    "is_ad_or_promo", "is_multi_region_roundup", "is_multi_topic_digest", "is_digest_or_roundup",
}


def merge_image_and_memory_for_finalizer(image: dict[str, Any], memory: dict[str, Any]) -> dict[str, Any]:
    """Merge durable image evidence with the latest authoritative text state.

    Candidate memory normally owns text/scope. External-publication canary
    runs may deliberately stop after the early image handoff, however, so the
    refreshed image row can be newer than the older memory row. In that narrow
    case its external scope attestation wins until a still-newer memory refresh
    supersedes it.
    """
    row = {**memory, **image}
    origin = str(image.get("content_origin_type") or memory.get("content_origin_type") or "")
    image_updated = str(image.get("updated_at") or image.get("queue_item_updated_at") or image.get("_ydb_updated_at") or "")
    memory_updated = str(memory.get("updated_at") or memory.get("_ydb_updated_at") or "")
    newer_external_image_scope = bool(
        origin in rt.EXTERNAL_PUBLICATION_ORIGIN_TYPES
        and image_updated
        and image_updated >= memory_updated
    )
    for field in FINALIZER_MEMORY_AUTHORITY_FIELDS:
        if (
            newer_external_image_scope
            and field in EXTERNAL_PUBLICATION_IMAGE_SCOPE_FIELDS
            and image.get(field) not in (None, "")
        ):
            continue
        if field in memory and memory.get(field) not in (None, ""):
            row[field] = memory.get(field)
    return row


def read_live_rows(
    limit_images: int,
    limit_memory: int,
    *,
    reverify_existing: bool = False,
) -> tuple[Any, Any, Any, str, list[dict[str, Any]], list[dict[str, Any]]]:
    ydb, driver, cfg = rt.ydb_connect()
    table = rt.ydb_kv_table_path(cfg)
    pool = ydb.SessionPool(driver)

    def op(session: Any) -> tuple[
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
    ]:
        images = rt.ydb_select_kind_items(session, ydb, table, "image_queue_item", limit=limit_images)
        memory = rt.ydb_select_kind_items(session, ydb, table, "candidate_memory_item", limit=limit_memory)
        publications = rt.ydb_select_kind_items(session, ydb, table, "publication_candidate_item", limit=limit_images)
        sources = rt.ydb_select_kind_items(session, ydb, table, "source_queue_item", limit=limit_memory)
        source_statuses = rt.ydb_select_kind_items(session, ydb, table, "source_status_item", limit=limit_memory)
        online_sources = rt.ydb_select_kind_items(session, ydb, table, "online_source_item", limit=limit_memory)
        external_publication_sources = rt.ydb_select_kind_items(
            session, ydb, table, "external_publication_source_item", limit=limit_memory
        )
        onboarding_profiles = rt.ydb_select_kind_items(session, ydb, table, "source_onboarding_profile_item", limit=limit_memory)
        return images, memory, publications, sources, source_statuses, online_sources, external_publication_sources, onboarding_profiles

    (
        images_by_pk,
        memory_by_pk,
        publications_by_pk,
        source_items,
        source_status_items,
        online_source_items,
        external_publication_source_items,
        onboarding_profile_items,
    ) = pool.retry_operation_sync(op)
    memory_by_url = _publication_by_normalized_url(memory_by_pk)
    publication_by_url = _publication_by_normalized_url(publications_by_pk)
    sources_by_key = authoritative_source_index(
        source_items,
        source_status_items,
        {**online_source_items, **external_publication_source_items},
    )
    # Reuse the one paginated live snapshot for pre-image source-attestation
    # priority.  The previous implementation scanned candidate/source/status
    # kinds a second time after this complete read.  On the live compact table
    # that redundant pass could hit YDB DEADLINE_EXCEEDED even though the first
    # snapshot had already succeeded, aborting the finalizer before any useful
    # work.  Writes remain chunked separately; this change removes duplicate
    # reads without weakening the strict text/source gate.
    strict_source_priority_rows = strict_text_candidate_source_priority_updates(
        list(memory_by_pk.values()),
        sources_by_key,
        now_iso=rt.utc_now_iso(),
    )
    memory_by_source = _memory_rows_by_source(memory_by_pk)
    onboarding_profiles_by_source = _profile_index(onboarding_profile_items)
    now_iso = rt.utc_now_iso()
    rows_by_url: dict[str, dict[str, Any]] = {}
    finalizer_inputs = dict(images_by_pk)
    actionable_media_urls = {
        normalize_post_url(str(item.get("post_url") or ""))
        for item in images_by_pk.values()
        if (
            item.get("image_queue_status") == "actual_scored"
            and item.get("image_model_input_type") == "actual_image"
        )
        or rt.is_video_media_candidate(item)
    }
    for memory_pk, memory in memory_by_pk.items():
        post_url = normalize_post_url(str(memory.get("post_url") or ""))
        if (
            not post_url
            or post_url in actionable_media_urls
            or not rt.is_external_link_article_candidate(memory)
        ):
            continue
        finalizer_inputs["external-link:" + str(memory_pk)] = {
            **memory,
            "_ydb_pk": "",
            "image_queue_status": "not_required_link_only",
            "image_model_input_type": "not_required_link_only",
            "media_review_mode": "link_only_no_media_reuse",
        }

    for image in finalizer_inputs.values():
        actual_image = image.get("image_queue_status") == "actual_scored" and image.get("image_model_input_type") == "actual_image"
        video_manual_review = rt.is_video_media_candidate(image)
        external_link_article = rt.uses_external_link_article_lane(image)
        if not actual_image and not video_manual_review and not external_link_article:
            continue
        original_post_url = str(image.get("post_url") or "")
        post_url = normalize_post_url(original_post_url)
        if not post_url:
            continue
        memory = memory_by_url.get(post_url, {})
        publication = publication_by_url.get(post_url, {})
        row = merge_image_and_memory_for_finalizer(image, memory)
        # ImageDiagnostic stores a snapshot of the text/source gate at queue
        # admission time.  Candidate memory is the authoritative, refreshable
        # text decision.  A later policy refresh (for example an ambiguous
        # place-name false positive) must therefore override the stale image
        # snapshot before publication eligibility is evaluated.
        row["_image_ydb_pk"] = str(image.get("_ydb_pk") or "")
        row["_memory_ydb_pk"] = str(memory.get("_ydb_pk") or "")
        row["_image_payload"] = dict(image)
        row["_memory_payload"] = dict(memory)
        if video_manual_review:
            row["media_kind"] = "video"
            row["manual_media_review_required"] = "true"
            row["video_manual_review_eligible"] = "true"
            row["media_review_mode"] = "operator_video_review"
        elif external_link_article:
            row["media_kind"] = "external_article_link"
            row["manual_media_review_required"] = "false"
            row["media_review_mode"] = "link_only_no_media_reuse"
        row["original_post_url"] = original_post_url
        row["post_url"] = post_url
        row["post_url_normalization_version"] = POST_URL_NORMALIZATION_VERSION
        source_key = canonical_source_key_for_row(row)
        authoritative_source = sources_by_key.get(source_key)
        row["canonical_source_key"] = source_key
        row["authoritative_source_found"] = str(bool(authoritative_source)).lower()
        row["_authoritative_source"] = authoritative_source
        row["_previous_publication"] = publication
        row["finalization_trigger"] = finalization_trigger(
            publication,
            now_iso=now_iso,
            reverify_existing=reverify_existing,
        )
        row["attempt_count"] = int(publication.get("attempt_count") or 0)
        if publication and not reverify_existing:
            for key in [
                "publication_status", "publication_candidate_status", "llm_gate_status", "llm_decision", "llm_reason",
                "llm_model", "content_type", "visit_evidence_type", "next_attempt_after", "last_attempt_at",
                "publication_draft_status", "publication_draft_title", "publication_draft_source_attribution",
                "publication_draft_telegram_text", "publication_draft_vk_text",
                "publication_draft_fact_points_json", "publication_draft_prompt_version",
                "publication_draft_contract_version", "publication_draft_input_fingerprint",
                "publication_draft_evidence_hash", "publication_draft_evidence_json",
                "publication_draft_history_json", "publication_draft_editorial_plan_json",
                "publication_draft_grounding_map_json", "publication_draft_critic_json",
                "publication_draft_stage_audit_json",
                "publication_draft_generation_attempts", "publication_draft_link_metadata_json",
                "publication_presentation_mode", "publication_media_materialization_status",
                "publication_media_materialization_reason", "publication_media_materialization_contract_version",
                "publication_presentation_manifest_json",
                "selected_media_ids", "selected_primary_media_id", "selected_primary_image_ordinal",
                "selected_media_materialization_json", "selected_media_materialization_fingerprint",
                "media_materialization_items_json", "input_media_manifest_hash",
                "presentation_recommendation", "presentation_recommendation_reason", "presentation_max_assets",
                "publication_draft_backfill_status", "publication_draft_backfill_reason",
                "publication_draft_backfill_next_attempt_after", "publication_draft_backfill_version",
                "legacy_review_migration_version", "legacy_review_migrated_at",
                "legacy_principle_status", "legacy_copy_status", "legacy_operator_review_fingerprint",
                "legacy_operator_review_decision", "legacy_operator_review_rewrite_status",
                "operator_review_fingerprint", "operator_review_decision", "operator_review_rewrite_status",
                "operator_review_positive", "operator_review_negative", "operator_review_rewrite_requested",
                "publication_eligibility_verdict", "publication_eligibility_evidence", "publication_eligibility_gate_version",
                "sent_to_chat", "sent_message_id", "sent_at", "sent_chat_id", "delivery_key", "delivery_random_id",
                "source_onboarding_status", "source_onboarding_paragraph", "source_onboarding_profile_id",
                "source_onboarding_profile_fingerprint", "source_onboarding_writer_fingerprint",
                "source_onboarding_entity_type", "source_onboarding_claim_ids_json",
                "source_onboarding_evidence_ids_json", "source_onboarding_selected_angle_id",
            ]:
                if publication.get(key) not in (None, ""):
                    row[key] = publication.get(key)
            row["existing_publication_candidate"] = "true"
        source_payload = {**row, **(authoritative_source or {})}
        row["source_class_guess"] = source_class_guess(
            str(source_payload.get("source_title") or ""),
            str(source_payload.get("source_url") or source_payload.get("canonical_url") or ""),
            source_payload,
        )
        if not authoritative_source:
            row["source_class_guess"] = "unknown_source"
        row["short_summary"] = memory.get("short_summary") or image.get("short_summary") or ""
        row["text"] = (
            image.get("full_text")
            or memory.get("full_text")
            or memory.get("text")
            or memory.get("text_excerpt")
            or ""
        )
        onboarding_evidence = build_source_onboarding_evidence(
            authoritative_source,
            memory_by_source.get(source_key, []),
            row,
        )
        row["_source_onboarding_evidence"] = onboarding_evidence
        row["_source_onboarding_profile"] = onboarding_profiles_by_source.get(source_key, {})
        row["publication_pre_score"] = publication_pre_score(row)
        previous = rows_by_url.get(post_url)
        if previous is None or str(row.get("updated_at") or row.get("_ydb_updated_at") or "") >= str(previous.get("updated_at") or previous.get("_ydb_updated_at") or ""):
            rows_by_url[post_url] = row
    rows = list(rows_by_url.values())
    rows.sort(key=lambda r: (-float(r.get("publication_pre_score") or 0), r.get("source_class_guess") != "nonlocal_travel_or_general_source"))
    return ydb, driver, pool, table, rows, strict_source_priority_rows


def _json_evidence(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return str(value or "")


def publication_eligibility_evidence_fingerprint(value: Any) -> str:
    """Return a durable identity for the complete eligibility evidence.

    Publication rows intentionally cap long audit strings at 700 characters.
    Comparing that stored prefix with the next full helper response made every
    terminal tombstone look changed on every finalizer run.  Keep the compact
    human-readable prefix, but compare the full evidence through this digest.
    """
    return hashlib.sha256(_json_evidence(value).encode("utf-8")).hexdigest()


def _eligibility_fields(row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    authoritative_source = row.get("_authoritative_source")
    row["authoritative_source_fingerprint_version"] = AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION
    row["authoritative_source_fingerprint"] = authoritative_source_fingerprint(authoritative_source)
    try:
        raw = rt.publication_eligibility(row, authoritative_source)
        result = raw if isinstance(raw, dict) else {"verdict": "review", "evidence": {"invalid_helper_result": str(raw)}}
    except Exception as exc:
        result = {
            "verdict": "review",
            "evidence": {"publication_eligibility_error": f"{type(exc).__name__}: {str(exc)[:240]}"},
            "gate_version": "helper_error",
        }
    verdict = str(
        result.get("publication_eligibility_verdict")
        or result.get("eligibility_verdict")
        or result.get("publication_eligibility")
        or result.get("eligibility_status")
        or result.get("verdict")
        or result.get("decision")
        or result.get("status")
        or "review"
    ).strip().lower()
    if result.get("eligible") is True and verdict == "review":
        verdict = "eligible"
    elif result.get("eligible") is False and verdict == "review":
        verdict = "review"
    evidence = result.get(
        "publication_eligibility_evidence",
        result.get("eligibility_evidence", result.get("evidence", result.get("reason", ""))),
    )
    gate_version = str(
        result.get("publication_eligibility_gate_version")
        or result.get("eligibility_gate_version")
        or result.get("gate_version")
        or result.get("version")
        or "unknown"
    )

    # The helper owns the semantic policy; these are fail-closed safety rails for
    # missing authoritative joins and already-terminal local/spam source rows.
    if not isinstance(authoritative_source, dict) or not authoritative_source:
        verdict = "review"
        evidence = {"reason": "authoritative_source_not_found", "helper_evidence": evidence}
    else:
        terminal = rt.source_local_region_terminal_fields(authoritative_source)
        terminal_status = str(terminal.get("source_queue_status") or "")
        if terminal_status in {str(rt.LOCAL_REGION_SOURCE_STATUS), str(rt.SPAM_SOURCE_STATUS)}:
            verdict = "reject"
            evidence = {
                "reason": "authoritative_source_terminal_local_or_spam",
                "source_queue_status": terminal_status,
                "helper_evidence": evidence,
            }
    if verdict in ELIGIBLE_VERDICTS:
        normalized_verdict = "eligible"
    elif verdict in REJECT_VERDICTS:
        normalized_verdict = "reject"
    else:
        normalized_verdict = "review"
    row["publication_eligibility_verdict"] = normalized_verdict
    row["publication_eligibility_evidence"] = _json_evidence(evidence)
    row["publication_eligibility_evidence_fingerprint"] = publication_eligibility_evidence_fingerprint(
        row["publication_eligibility_evidence"]
    )
    row["publication_eligibility_gate_version"] = gate_version
    return normalized_verdict, result


def _source_evidence_priority_is_current(source: dict[str, Any], *, target_posts: int) -> bool:
    try:
        stored_target = int(float(source.get("publication_source_evidence_target_posts") or 0))
    except (TypeError, ValueError):
        stored_target = 0
    return (
        rt._rt_bool(source.get("publication_source_evidence_priority"))
        and stored_target == target_posts
    )


def source_evidence_priority_updates(rows: list[dict[str, Any]], *, now_iso: str) -> list[dict[str, Any]]:
    """Return source-ledger rows blocked *only* by missing source evidence.

    This is a bounded completion lane, not a broad rescan: a source is promoted
    only after its post already has current dual-vector/text evidence and a
    strong actual-image score, and the unchanged publication gate would accept
    it if the source were durably confirmed external.
    """
    target_posts = max(1, _env_int("REGION_TALK_PUBLICATION_SOURCE_MIN_SCANNED_POSTS", 5))
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        previous = row.get("_previous_publication") if isinstance(row.get("_previous_publication"), dict) else {}
        candidate_status = str(row.get("publication_candidate_status") or previous.get("publication_candidate_status") or "")
        publication_status = str(row.get("publication_status") or previous.get("publication_status") or "")
        if candidate_status in {"llm_rejected", "llm_needs_review", "filtered_before_llm", "revoked"} or candidate_status.startswith(("tombstoned", "revoked")) or publication_status in {"gemini_reject", "gemini_needs_review", "eligibility_revoked"} or publication_status.startswith("eligibility_"):
            continue
        source = row.get("_authoritative_source")
        if not isinstance(source, dict) or not source:
            continue
        current = rt.publication_eligibility(row, source)
        if str(current.get("primary_reason") or "") != "source_verdict_unknown":
            continue
        external_probe = {**source, "source_scope": "external"}
        if not bool(rt.publication_eligibility(row, external_probe).get("eligible")):
            continue
        try:
            posts_scanned = int(float(source.get("posts_scanned") or 0))
        except (TypeError, ValueError):
            posts_scanned = 0
        if posts_scanned >= target_posts:
            continue
        key = canonical_source_key_for_row(source) or canonical_source_key_for_row(row)
        if not key:
            continue
        post_url = normalize_post_url(str(row.get("post_url") or ""))
        if _source_evidence_priority_is_current(source, target_posts=target_posts):
            continue
        updated = {
            **source,
            "publication_source_evidence_priority": "true",
            "publication_source_evidence_post_url": post_url,
            "publication_source_evidence_requested_at": now_iso,
            "publication_source_evidence_target_posts": target_posts,
            "priority_lane": "publication_source_evidence",
            "priority_reason": "strong_finalist_needs_source_attestation",
            "priority_updated_at": now_iso,
            "next_action": "scan_source_to_complete_publication_attestation",
            "queue_item_updated_at": now_iso,
        }
        by_key[key] = updated
    return list(by_key.values())


def strict_text_candidate_source_priority_updates(
    candidate_rows: list[dict[str, Any]],
    sources_by_key: dict[str, dict[str, Any]],
    *,
    now_iso: str,
) -> list[dict[str, Any]]:
    """Prioritize bounded source attestation before image handoff.

    An exact post that already passed current KO-only E5+BGE text checks should
    not wait in a generic source backlog merely because only one source post
    has been observed. This lane spends up to the same five-post attestation
    target, without bypassing local/spam or final publication gates.
    """
    target_posts = max(1, _env_int("REGION_TALK_PUBLICATION_SOURCE_MIN_SCANNED_POSTS", 5))
    active_stages = {
        "image_fetch_retry_needed", "needs_image_review", "good_text_weak_media",
        "semantic_candidate", "favorite", "low_substance_but_region_relevant",
    }
    by_key: dict[str, dict[str, Any]] = {}
    for row in candidate_rows:
        if str(row.get("vector_gate_status") or "") != "vector_accept_candidate":
            continue
        if str(row.get("text_vector_fusion_status") or "") != "fused_e5_bge_m3":
            continue
        if not rt._rt_bool(row.get("kaliningrad_oblast_only_scope")):
            continue
        if str(row.get("current_stage") or "") not in active_stages:
            continue
        key = canonical_source_key_for_row(row)
        source = sources_by_key.get(key) if key else None
        if not isinstance(source, dict) or not source:
            continue
        terminal = rt.source_local_region_terminal_fields(source)
        if str(terminal.get("source_queue_status") or "") in {str(rt.LOCAL_REGION_SOURCE_STATUS), str(rt.SPAM_SOURCE_STATUS)}:
            continue
        if str(source.get("source_scope") or "") == "external" or str(source.get("source_geo_class") or "") in {"nonlocal_russia", "external"}:
            continue
        try:
            posts_scanned = int(float(source.get("posts_scanned") or 0))
        except (TypeError, ValueError):
            posts_scanned = 0
        if posts_scanned >= target_posts:
            continue
        post_url = normalize_post_url(str(row.get("post_url") or ""))
        if _source_evidence_priority_is_current(source, target_posts=target_posts):
            continue
        by_key[key] = {
            **source,
            "publication_source_evidence_priority": "true",
            "publication_source_evidence_post_url": post_url,
            "publication_source_evidence_requested_at": now_iso,
            "publication_source_evidence_target_posts": target_posts,
            "priority_lane": "publication_source_evidence",
            "priority_reason": "strict_text_candidate_needs_source_attestation",
            "priority_updated_at": now_iso,
            "next_action": "scan_source_to_complete_publication_attestation_before_image",
            "queue_item_updated_at": now_iso,
        }
    return list(by_key.values())


def read_strict_text_candidate_source_priority_rows(
    pool: Any,
    ydb: Any,
    table: str,
    *,
    limit: int,
    now_iso: str,
) -> list[dict[str, Any]]:
    def op(session: Any) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        memory = rt.ydb_select_kind_items(session, ydb, table, "candidate_memory_item", limit=limit)
        sources = rt.ydb_select_kind_items(session, ydb, table, "source_queue_item", limit=limit)
        statuses = rt.ydb_select_kind_items(session, ydb, table, "source_status_item", limit=limit)
        return memory, sources, statuses

    memory, sources, statuses = pool.retry_operation_sync(op)
    source_index = authoritative_source_index(sources, statuses, {})
    return strict_text_candidate_source_priority_updates(
        list(memory.values()),
        source_index,
        now_iso=now_iso,
    )


def source_evidence_priority_clear_updates(rows: list[dict[str, Any]], *, now_iso: str) -> list[dict[str, Any]]:
    """Clear attestation priority after the associated post is terminally out."""
    by_key: dict[str, dict[str, Any]] = {}
    terminal_candidates = {"llm_rejected", "llm_needs_review", "filtered_before_llm", "revoked"}
    terminal_publications = {"gemini_reject", "gemini_needs_review", "eligibility_revoked"}
    for row in rows:
        previous = row.get("_previous_publication") if isinstance(row.get("_previous_publication"), dict) else {}
        candidate_status = str(row.get("publication_candidate_status") or previous.get("publication_candidate_status") or "")
        publication_status = str(row.get("publication_status") or previous.get("publication_status") or "")
        if candidate_status not in terminal_candidates and not candidate_status.startswith(("tombstoned", "revoked")) and publication_status not in terminal_publications and not publication_status.startswith("eligibility_"):
            continue
        source = row.get("_authoritative_source")
        if not isinstance(source, dict) or not source or not rt._rt_bool(source.get("publication_source_evidence_priority")):
            continue
        key = canonical_source_key_for_row(source) or canonical_source_key_for_row(row)
        if not key:
            continue
        by_key[key] = {
            **source,
            "publication_source_evidence_priority": "false",
            "publication_source_evidence_post_url": "",
            "publication_source_evidence_cleared_at": now_iso,
            "publication_source_evidence_clear_reason": "publication_terminal_non_candidate",
            "priority_lane": "",
            "priority_reason": "",
            "priority_updated_at": now_iso,
            "next_action": "normal_queue_policy",
            "queue_item_updated_at": now_iso,
        }
    return list(by_key.values())


def write_source_evidence_priority_rows(
    pool: Any,
    ydb: Any,
    table: str,
    rows: list[dict[str, Any]],
    *,
    run_id: str,
) -> int:
    if not rows:
        return 0
    now = rt.utc_now_iso()
    items: list[tuple[str, str, dict[str, Any]]] = []
    for row in rows:
        payload = {k: v for k, v in row.items() if not str(k).startswith("_")}
        payload.update({"run_id": run_id, "updated_at": now, "last_seen_run_id": run_id})
        key = canonical_source_key_for_row(payload)
        if not key:
            continue
        pk = str(row.get("_ydb_pk") or "") or "source_queue_item:" + key
        if not pk.startswith("source_queue_item:"):
            pk = "source_queue_item:" + key
        items.append((pk, "source_queue_item", payload))
    if not items:
        return 0

    def op(session: Any) -> int:
        rt.ensure_ydb_kv_table(ydb, session, table)
        return rt.ydb_upsert_json_many(session, ydb, table, items, now, chunk_size=20, timeout_seconds=8)

    return int(pool.retry_operation_sync(op) or 0)


def _previous_was_publishable(row: dict[str, Any]) -> bool:
    previous = row.get("_previous_publication") if isinstance(row.get("_previous_publication"), dict) else row
    return (
        str(previous.get("publication_status") or "") == "gemini_accept"
        or str(previous.get("publication_status") or "") == "eligibility_revoked"
        or str(previous.get("publication_candidate_status") or "") in {"llm_confirmed", "sent_to_chat", "accepted_for_publication"}
        or str(previous.get("publication_candidate_status") or "") == "revoked"
        or str(previous.get("publication_revoked") or "").lower() == "true"
    )


def _ineligible_state_is_current(row: dict[str, Any], verdict: str) -> bool:
    previous = row.get("_previous_publication")
    if not isinstance(previous, dict) or not previous:
        return False
    previous_status = str(previous.get("publication_status") or "")
    previous_candidate_status = str(previous.get("publication_candidate_status") or "")
    is_tombstone = previous_status.startswith("eligibility_") or previous_candidate_status.startswith(("tombstoned", "revoked"))
    previous_evidence = str(previous.get("publication_eligibility_evidence") or "")
    current_evidence = str(row.get("publication_eligibility_evidence") or "")
    previous_evidence_fingerprint = str(previous.get("publication_eligibility_evidence_fingerprint") or "")
    current_evidence_fingerprint = str(
        row.get("publication_eligibility_evidence_fingerprint")
        or publication_eligibility_evidence_fingerprint(current_evidence)
    )
    evidence_is_current = (
        previous_evidence_fingerprint == current_evidence_fingerprint
        if previous_evidence_fingerprint
        else (
            previous_evidence == current_evidence
            # Backward compatibility for rows written before the fingerprint:
            # compact_record stored exactly the first 700 characters.
            or (
                len(previous_evidence) == PUBLICATION_ELIGIBILITY_EVIDENCE_STORAGE_MAX_CHARS
                and current_evidence.startswith(previous_evidence)
            )
        )
    )
    return (
        is_tombstone
        and str(previous.get("publication_eligibility_verdict") or "") == verdict
        and str(previous.get("publication_eligibility_gate_version") or "")
        == str(row.get("publication_eligibility_gate_version") or "")
        and evidence_is_current
        and str(previous.get("authoritative_source_fingerprint_version") or "")
        == AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION
        and str(previous.get("authoritative_source_fingerprint") or "")
        == str(row.get("authoritative_source_fingerprint") or "")
    )


def _mark_ineligible(row: dict[str, Any], verdict: str, *, now_iso: str) -> None:
    revoked = _previous_was_publishable(row)
    row["publication_status"] = "eligibility_revoked" if revoked else f"eligibility_{verdict}_tombstone"
    row["publication_candidate_status"] = "revoked" if revoked else f"tombstoned_{verdict}"
    row["publication_tombstone"] = "true"
    row["publication_revoked"] = str(revoked).lower()
    row["revoked_at"] = now_iso if revoked else ""
    row["finalization_status"] = "terminal"
    row["llm_attempted_this_run"] = "false"
    row["next_attempt_after"] = ""


def _review_state_is_current(row: dict[str, Any]) -> bool:
    previous = row.get("_previous_publication") if isinstance(row.get("_previous_publication"), dict) else {}
    base_current = bool(
        previous
        and str(previous.get("publication_status") or "") == "needs_visual_review"
        and str(previous.get("publication_eligibility_verdict") or "") == "review"
        and str(previous.get("publication_eligibility_gate_version") or "")
        == str(row.get("publication_eligibility_gate_version") or "")
        and str(previous.get("publication_eligibility_evidence_fingerprint") or "")
        == str(row.get("publication_eligibility_evidence_fingerprint") or "")
        and str(previous.get("authoritative_source_fingerprint") or "")
        == str(row.get("authoritative_source_fingerprint") or "")
    )
    if not base_current:
        return False
    origin = str(row.get("content_origin_type") or "")
    if origin not in rt.EXTERNAL_PUBLICATION_ORIGIN_TYPES:
        return True
    # v1 external publication rows predated durable origin/rights/image-review
    # fields. Re-write that review row once instead of treating its older gate
    # fingerprint as proof that the operator contract is complete.
    required_external_projection = (
        "content_origin_type", "external_publication_id", "external_research_quality_score",
        "rights_policy", "media_use_policy", "media_reuse_allowed",
        "image_quality_decision", "image_quality_reason",
        "kaliningrad_oblast_only_scope", "kaliningrad_mention_role",
    )
    return all(previous.get(field) == row.get(field) for field in required_external_projection)


def _mark_review_pending(row: dict[str, Any]) -> None:
    row["publication_status"] = "needs_visual_review"
    row["publication_candidate_status"] = "visual_review_pending"
    row["publication_tombstone"] = "false"
    row["publication_revoked"] = "false"
    row["revoked_at"] = ""
    row["finalization_status"] = "review_pending"
    row["llm_attempted_this_run"] = "false"
    row["next_attempt_after"] = ""


def _mark_text_restore_pending(row: dict[str, Any]) -> None:
    """Keep an otherwise eligible post active until Telethon restores text.

    Image scoring is asynchronous.  Treating a temporarily absent/pruned post
    body as a terminal publication verdict caused the storage compactor to
    delete the only remaining working copy before Gemini could run.  The
    publication row now acts as a durable retry marker and the exact post URL
    is handed back to CandidateReport's normal human-like Telethon queue.
    """

    row["publication_status"] = "text_restore_pending"
    row["publication_candidate_status"] = "awaiting_text_restore"
    row["publication_tombstone"] = "false"
    row["publication_revoked"] = "false"
    row["revoked_at"] = ""
    row["finalization_status"] = "retryable"
    row["llm_attempted_this_run"] = "false"
    row["next_attempt_after"] = ""
    row["text_restore_reason"] = "eligible_post_text_missing_after_async_media_scoring"
    row["next_action"] = "refetch_exact_post_text_then_finalize"


def _reconcile_terminal_provider_decision(
    row: dict[str, Any],
    previous: dict[str, Any],
    *,
    eligibility_verdict: str,
    now_iso: str,
) -> bool:
    """Keep paid/delivered terminal evidence out of review/restore loops.

    Current hard eligibility can still revoke an earlier acceptance.  A review
    verdict blocks an unsent acceptance without spending Gemini again, while a
    delivered row and a provider rejection remain terminal and monotonic.
    """
    attestation_fields = (
        "publication_eligibility_verdict", "publication_eligibility_evidence",
        "publication_eligibility_evidence_fingerprint", "publication_eligibility_gate_version",
        "authoritative_source_fingerprint", "authoritative_source_fingerprint_version",
        "authoritative_source_found",
    )
    attestation_changed_from_previous = any(
        str(previous.get(key) or "") != str(row.get(key) or "")
        for key in attestation_fields
    )
    before = {
        key: row.get(key)
        for key in (
            "publication_status", "publication_candidate_status",
            "publication_tombstone", "publication_revoked", "revoked_at",
            "finalization_status", "llm_attempted_this_run",
            "next_attempt_after", "next_action", "text_restore_reason",
            *attestation_fields,
        )
    }
    decision = str(
        previous.get("llm_decision")
        or previous.get("publication_llm_decision")
        or row.get("llm_decision")
        or row.get("publication_llm_decision")
        or ""
    ).strip().lower()
    previous_status = str(previous.get("publication_status") or "").strip().lower()
    previous_candidate_status = str(previous.get("publication_candidate_status") or "").strip().lower()
    # Historical rows did not always persist ``llm_decision`` even though the
    # normalized terminal status is authoritative.  Infer the provider verdict
    # before reconciling so a newly discovered local/spam source can still
    # revoke an old accept, and an old reject cannot re-enter text restoration.
    if not decision:
        if previous_status == "gemini_accept" or previous_candidate_status in {
            "llm_confirmed", "sent_to_chat", "accepted_for_publication",
        }:
            decision = "accept"
        elif previous_status == "gemini_reject" or previous_candidate_status == "llm_rejected":
            decision = "reject"
    sent = (
        str(previous.get("sent_to_chat") or row.get("sent_to_chat") or "").strip().lower() == "true"
        or previous_candidate_status == "sent_to_chat"
    )

    if eligibility_verdict not in {"eligible", "review"} and decision == "accept":
        _mark_ineligible(row, eligibility_verdict, now_iso=now_iso)
    elif decision == "reject":
        row["publication_status"] = "gemini_reject"
        row["publication_candidate_status"] = "llm_rejected"
        row["publication_tombstone"] = "false"
        row["publication_revoked"] = "false"
        row["revoked_at"] = ""
        row["finalization_status"] = "terminal"
        row["llm_attempted_this_run"] = "false"
        row["next_attempt_after"] = ""
    elif sent:
        row["publication_status"] = "gemini_accept"
        row["publication_candidate_status"] = "sent_to_chat"
        row["publication_tombstone"] = "false"
        row["publication_revoked"] = "false"
        row["revoked_at"] = ""
        row["finalization_status"] = "terminal"
        row["llm_attempted_this_run"] = "false"
        row["next_attempt_after"] = ""
    elif decision == "accept" and eligibility_verdict == "review":
        # Preserve the provider verdict fields on ``row`` but block delivery
        # until source/media evidence is explicitly resolved.
        _mark_review_pending(row)
    elif decision == "accept":
        row["publication_status"] = "gemini_accept"
        row["publication_candidate_status"] = "llm_confirmed"
        row["publication_tombstone"] = "false"
        row["publication_revoked"] = "false"
        row["revoked_at"] = ""
        row["finalization_status"] = "terminal"
        row["llm_attempted_this_run"] = "false"
        row["next_attempt_after"] = ""
    else:
        # Operator/tombstone terminal rows without a provider decision retain
        # their authoritative previous lifecycle.
        for key in (
            "publication_status", "publication_candidate_status",
            "publication_tombstone", "publication_revoked", "revoked_at",
            "finalization_status", "next_attempt_after",
        ):
            if previous.get(key) not in (None, ""):
                row[key] = previous.get(key)
        row["llm_attempted_this_run"] = "false"

    # A terminal repair must also clear the stale restore request that caused
    # the contradiction.  Review-after-accept has its own explicit status and
    # likewise must not enqueue a text refetch.
    row["next_action"] = ""
    row["text_restore_reason"] = ""
    after = {key: row.get(key) for key in before}
    return after != before or attestation_changed_from_previous


def _eligibility_only_tombstone_can_reopen(previous: dict[str, Any], verdict: str) -> bool:
    """Allow changed deterministic eligibility to reach Gemini for the first time.

    Provider and operator decisions remain monotonic. Only a tombstone created
    by the eligibility gate itself, with no durable Gemini verdict, may reopen
    when the current evidence becomes eligible or reviewable.
    """
    if verdict not in {"eligible", "review"}:
        return False
    publication_status = str(previous.get("publication_status") or "").strip().lower()
    candidate_status = str(previous.get("publication_candidate_status") or "").strip().lower()
    provider_decision = str(
        previous.get("llm_decision")
        or previous.get("publication_llm_decision")
        or ""
    ).strip().lower()
    return bool(
        publication_status.startswith("eligibility_")
        and candidate_status.startswith("tombstoned_")
        and provider_decision not in {"accept", "reject", "needs_review"}
    )


def _retry_after(now_iso: str, gate_status: str) -> str:
    now = _parse_time(now_iso) or datetime.now(timezone.utc)
    env_name = (
        "REGION_TALK_FINALIZER_RATE_LIMIT_RETRY_SECONDS"
        if gate_status == "rate_limited"
        else "REGION_TALK_FINALIZER_ERROR_RETRY_SECONDS"
    )
    default = 3600 if gate_status == "rate_limited" else 900
    return (now + timedelta(seconds=max(1, _env_int(env_name, default)))).isoformat()


def verify_rows(
    rows: list[dict[str, Any]],
    *,
    max_llm: int,
    model: str,
    default_env_var_name: str,
    now_iso: str | None = None,
    durable_budget: DurableGeminiBudget | None = None,
) -> list[dict[str, Any]]:
    now_iso = now_iso or rt.utc_now_iso()
    results: list[dict[str, Any]] = []
    llm_calls = 0
    for row in rows:
        verdict, _raw_eligibility = _eligibility_fields(row)
        previous = row.get("_previous_publication") if isinstance(row.get("_previous_publication"), dict) else {}
        if (
            previous
            and _terminal_decision_blocks_text_restore(previous)
            and not _eligibility_only_tombstone_can_reopen(previous, verdict)
        ):
            if verdict not in {"eligible", "review"} and _ineligible_state_is_current(row, verdict):
                continue
            if _reconcile_terminal_provider_decision(
                row,
                previous,
                eligibility_verdict=verdict,
                now_iso=now_iso,
            ):
                results.append(row)
            continue
        if verdict == "review":
            if _review_state_is_current(row):
                continue
            _mark_review_pending(row)
            results.append(row)
            continue
        if verdict != "eligible":
            if _ineligible_state_is_current(row, verdict):
                continue
            _mark_ineligible(row, verdict, now_iso=now_iso)
            results.append(row)
            continue
        if not row.get("finalization_trigger"):
            previous = row.get("_previous_publication") if isinstance(row.get("_previous_publication"), dict) else {}
            attestation_is_current = (
                str(previous.get("publication_eligibility_verdict") or "") == "eligible"
                and str(previous.get("publication_eligibility_gate_version") or "")
                == str(row.get("publication_eligibility_gate_version") or "")
                and str(previous.get("authoritative_source_fingerprint") or "")
                == str(row.get("authoritative_source_fingerprint") or "")
                and str(previous.get("authoritative_source_fingerprint_version") or "")
                == AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION
            )
            if previous and not attestation_is_current:
                row["llm_attempted_this_run"] = "false"
                row["finalization_status"] = "terminal"
                row["next_attempt_after"] = ""
                results.append(row)
            elif (
                previous
                and str(previous.get("publication_status") or "") == "gemini_accept"
                and str(previous.get("sent_to_chat") or "").lower() != "true"
                and not (
                    str(previous.get("source_onboarding_status") or "") == "ready"
                    and str(previous.get("source_onboarding_paragraph") or "").strip()
                )
            ):
                # Gemini verdict is terminal and must not be repeated. Keep the
                # accepted unsent row in this run only so the bounded profile/
                # writer tail can finish before operator delivery.
                row["llm_attempted_this_run"] = "false"
                row["finalization_status"] = "terminal"
                row["next_attempt_after"] = ""
                results.append(row)
            continue
        if not row.get("text"):
            row["text"] = telegram_public_text(str(row.get("post_url") or ""))
        if not row.get("text") and row.get("short_summary"):
            row["text"] = "summary: " + str(row.get("short_summary") or "")
        if not row.get("text"):
            _mark_text_restore_pending(row)
            results.append(row)
            continue
        if llm_calls >= max(0, max_llm):
            continue
        fingerprint = gemini_request_fingerprint(row, model=model)
        replay_result: dict[str, Any] | None = None
        if durable_budget is not None:
            reservation = durable_budget.reserve(fingerprint)
            reservation_status = str(reservation.get("status") or "")
            if reservation_status == "replay":
                replay_result = dict(reservation.get("result") or {})
            elif reservation_status in {"busy", "exhausted"}:
                row["publication_status"] = "gemini_rate_limited"
                row["publication_candidate_status"] = "llm_budget_deferred"
                row["finalization_status"] = "retryable"
                row["llm_attempted_this_run"] = "false"
                row["llm_budget_id"] = durable_budget.budget_id
                row["llm_budget_status"] = reservation_status
                row["next_attempt_after"] = _retry_after(now_iso, "rate_limited")
                results.append(row)
                continue
        if replay_result is None:
            llm_calls += 1
        row["publication_rank"] = max(1, llm_calls)
        row["attempt_count"] = int(row.get("attempt_count") or 0) + 1
        row["last_attempt_at"] = now_iso
        row["llm_attempted_this_run"] = str(replay_result is None).lower()
        row["llm_request_fingerprint"] = fingerprint
        row["llm_prompt_version"] = rt.REGION_TALK_FINAL_VERIFIER_PROMPT_VERSION
        row["llm_budget_id"] = durable_budget.budget_id if durable_budget is not None else ""
        evidence = {
            "stage": "final_publication_verifier",
            "overall_media_score": row.get("overall_media_score"),
            "postcardness_score": row.get("postcardness_score"),
            "aesthetic_score": row.get("aesthetic_score"),
            "image_model_input_type": row.get("image_model_input_type"),
            "image_queue_status": row.get("image_queue_status"),
            "vector_gate_status": row.get("vector_gate_status"),
            "source_geo_class": row.get("source_class_guess"),
            "source_topic_class": row.get("source_topic_class") or "travel/general",
            "publication_text_story_score": row.get("candidate_score"),
            "media_kind": row.get("media_kind"),
            "media_review_mode": row.get("media_review_mode"),
            "manual_media_review_required": row.get("manual_media_review_required"),
        }
        print(
            f"[region-talk-finalizer] Gemini {llm_calls}/{max(0, max_llm)} {row.get('post_url')} "
            f"source={row.get('source_title')} pre_score={row.get('publication_pre_score')}",
            flush=True,
        )
        if replay_result is not None:
            llm_verdict = replay_result
            row["llm_budget_status"] = "replayed_completed_request"
        else:
            try:
                llm_verdict = rt.call_region_talk_semantic_llm(
                    row,
                    evidence,
                    model=model,
                    default_env_var_name=default_env_var_name,
                )
            except Exception as exc:
                llm_verdict = {"llm_gate_status": "error", "llm_reason": f"{type(exc).__name__}: {str(exc)[:240]}"}
            if durable_budget is not None:
                durable_budget.complete(fingerprint, llm_verdict)
                row["llm_budget_status"] = "completed"
        row.update(llm_verdict)
        gate_status = str(llm_verdict.get("llm_gate_status") or "unknown").lower()
        decision = str(llm_verdict.get("llm_decision") or "").lower()
        if gate_status == "ok" and decision == "accept":
            row["publication_status"] = "gemini_accept"
            row["publication_candidate_status"] = "llm_confirmed"
            row["finalization_status"] = "terminal"
            row["next_attempt_after"] = ""
        elif gate_status == "ok":
            row["publication_status"] = "gemini_reject" if decision == "reject" else "gemini_needs_review"
            row["publication_candidate_status"] = "llm_rejected" if decision == "reject" else "llm_needs_review"
            row["finalization_status"] = "terminal"
            row["next_attempt_after"] = ""
        else:
            retry_status = "rate_limited" if gate_status == "rate_limited" else "error" if gate_status == "error" else "unknown"
            row["publication_status"] = "gemini_" + retry_status
            row["publication_candidate_status"] = "llm_budget_deferred" if retry_status == "rate_limited" else "llm_error"
            row["finalization_status"] = "retryable"
            row["next_attempt_after"] = _retry_after(now_iso, retry_status)
        results.append(row)
    return results


def _evidence_items(evidence_row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = evidence_row.get("evidence_pack_json")
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        value = []
    return [dict(item) for item in (value or []) if isinstance(item, dict)]


def _json_list(value: Any) -> list[Any]:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = []
    return list(parsed) if isinstance(parsed, list) else []


def _structured_llm_call(
    prompt: str,
    *,
    model: str,
    default_env_var_name: str,
    max_output_tokens: int = 1200,
) -> dict[str, Any]:
    """Call the existing Supabase-governed Gemini gateway for JSON output."""
    timeout = max(1.0, float(os.getenv("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS") or "45"))
    try:
        client = rt.get_region_talk_llm_gateway(default_env_var_name)

        async def call() -> tuple[str, Any]:
            return await client.generate_content_async(
                model=model,
                prompt=prompt,
                generation_config={"temperature": 0.1, "response_mime_type": "application/json"},
                max_output_tokens=max_output_tokens,
            )

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="region-talk-onboarding")
        future = executor.submit(lambda: asyncio.run(call()))
        timed_out = False
        try:
            text, usage = future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            timed_out = True
            future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            if not timed_out:
                executor.shutdown(wait=True, cancel_futures=False)
        return {
            "llm_gate_status": "ok",
            "data": rt.parse_llm_json(text),
            "llm_model": model,
            "llm_usage_input_tokens": getattr(usage, "input_tokens", ""),
            "llm_usage_output_tokens": getattr(usage, "output_tokens", ""),
            "llm_usage_total_tokens": getattr(usage, "total_tokens", ""),
        }
    except concurrent.futures.TimeoutError:
        return {"llm_gate_status": "error", "llm_model": model, "llm_reason": f"TimeoutError after {timeout:.1f}s"}
    except Exception as exc:
        message = f"{type(exc).__name__}: {str(exc)[:240]}"
        lowered = message.lower()
        status = "rate_limited" if "429" in lowered or "rate limit" in lowered or "resource_exhausted" in lowered else "error"
        return {"llm_gate_status": status, "llm_model": model, "llm_reason": message}


def _call_structured_with_budget(
    prompt: str,
    *,
    fingerprint: str,
    model: str,
    default_env_var_name: str,
    durable_budget: DurableGeminiBudget | None,
) -> tuple[dict[str, Any], bool]:
    if durable_budget is not None:
        reservation = durable_budget.reserve(fingerprint)
        status = str(reservation.get("status") or "")
        if status == "replay":
            return dict(reservation.get("result") or {}), False
        if status in {"busy", "exhausted"}:
            return {"llm_gate_status": "rate_limited", "llm_reason": "durable_budget_" + status}, False
    result = _structured_llm_call(
        prompt,
        model=model,
        default_env_var_name=default_env_var_name,
    )
    if durable_budget is not None:
        durable_budget.complete(fingerprint, result)
    return result, True


def _source_profile_prompt(evidence_row: dict[str, Any]) -> str:
    return """Ты готовишь доказательный профиль автора/канала для редактора Region Talk.
Используй ТОЛЬКО факты из evidence_pack. Не угадывай место жительства, профессию, популярность или мотивацию.
Верни только JSON:
{
  "status": "ready|needs_review",
  "entity_type": "person|collective|thematic_channel|media_brand|unknown",
  "profile_summary": "краткое нейтральное резюме",
  "claims": [{"claim_id":"C1","text":"один атомарный факт","evidence_ids":["E1"]}],
  "candidate_angles": [{"angle_id":"A1","text":"релевантный ракурс","claim_ids":["C1"],"evidence_ids":["E1"]}],
  "conflicts": [],
  "missing_fields": []
}
Каждый claim и angle обязан ссылаться на существующие evidence_ids. Если данных мало, status=needs_review.

SOURCE:
""" + json.dumps({
        "canonical_source_key": evidence_row.get("canonical_source_key"),
        "source_title": evidence_row.get("source_title"),
        "source_url": evidence_row.get("source_url"),
        "evidence_pack": _evidence_items(evidence_row),
    }, ensure_ascii=False, indent=2)


def normalize_source_onboarding_profile(
    result: dict[str, Any],
    evidence_row: dict[str, Any],
    *,
    model: str,
    profile_fingerprint: str,
) -> dict[str, Any]:
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    evidence_ids = {str(item.get("evidence_id") or "") for item in _evidence_items(evidence_row)} - {""}
    claims: list[dict[str, Any]] = []
    claim_ids: set[str] = set()
    invalid_refs: list[str] = []
    for index, raw in enumerate(data.get("claims") or [], start=1):
        if not isinstance(raw, dict):
            continue
        claim_id = str(raw.get("claim_id") or f"C{index}")
        refs = [str(ref) for ref in (raw.get("evidence_ids") or []) if str(ref)]
        if not refs or not set(refs).issubset(evidence_ids):
            invalid_refs.append(claim_id)
            continue
        text = _compact_evidence_text(raw.get("text"), 320)
        if text:
            claims.append({"claim_id": claim_id, "text": text, "evidence_ids": refs})
            claim_ids.add(claim_id)
    angles: list[dict[str, Any]] = []
    for index, raw in enumerate(data.get("candidate_angles") or [], start=1):
        if not isinstance(raw, dict):
            continue
        angle_id = str(raw.get("angle_id") or f"A{index}")
        refs = [str(ref) for ref in (raw.get("evidence_ids") or []) if str(ref)]
        cited_claims = [str(ref) for ref in (raw.get("claim_ids") or []) if str(ref)]
        if (refs and not set(refs).issubset(evidence_ids)) or (cited_claims and not set(cited_claims).issubset(claim_ids)):
            invalid_refs.append(angle_id)
            continue
        text = _compact_evidence_text(raw.get("text"), 320)
        if text and (refs or cited_claims):
            angles.append({"angle_id": angle_id, "text": text, "claim_ids": cited_claims, "evidence_ids": refs})
    entity_type = str(data.get("entity_type") or "unknown")
    if entity_type not in SOURCE_ONBOARDING_ENTITY_TYPES:
        entity_type = "unknown"
    requested_status = str(data.get("status") or "needs_review").lower()
    status = "ready" if (
        result.get("llm_gate_status") == "ok"
        and requested_status == "ready"
        and claims
        and not invalid_refs
        and evidence_row.get("evidence_status") == "sufficient"
    ) else "needs_review"
    return {
        "source_profile_id": evidence_row.get("source_profile_id"),
        "canonical_source_key": evidence_row.get("canonical_source_key"),
        "source_title": evidence_row.get("source_title"),
        "source_url": evidence_row.get("source_url"),
        "profile_status": status,
        "entity_type": entity_type,
        "profile_summary": _compact_evidence_text(data.get("profile_summary"), 600),
        "claims_json": json.dumps(claims, ensure_ascii=False, separators=(",", ":")),
        "candidate_angles_json": json.dumps(angles, ensure_ascii=False, separators=(",", ":")),
        "conflicts_json": json.dumps(data.get("conflicts") or [], ensure_ascii=False, separators=(",", ":")),
        "missing_fields_json": json.dumps(data.get("missing_fields") or [], ensure_ascii=False, separators=(",", ":")),
        "invalid_reference_ids_json": json.dumps(invalid_refs, ensure_ascii=False),
        "evidence_version": evidence_row.get("evidence_version"),
        "evidence_fingerprint": evidence_row.get("evidence_fingerprint"),
        "profile_prompt_version": SOURCE_ONBOARDING_PROFILE_PROMPT_VERSION,
        "profile_fingerprint": profile_fingerprint,
        "profile_model": model,
        "profile_llm_status": result.get("llm_gate_status"),
        "profile_llm_reason": result.get("llm_reason", ""),
    }


def _candidate_onboarding_prompt(row: dict[str, Any], profile: dict[str, Any], evidence_row: dict[str, Any]) -> str:
    external_publication = str(row.get("content_origin_type") or "") in {
        "editorial_publication", "academic_publication",
    }
    subject = "издании/журнале" if external_publication else "блогере/канале"
    purpose = (
        "Абзац должен кратко представить тип и тематический ракурс издания, а затем объяснить, чем интересна эта публикация."
        if external_publication
        else "Абзац должен объяснить, кто автор, его подтверждённый ракурс и почему именно этот пост интересен."
    )
    return "Напиши один доказательный вводный абзац о " + subject + " для редактора Region Talk.\n" + """Длина строго 300–600 знаков, русский язык, без рекламы и превосходных степеней.
""" + purpose + """
Используй ТОЛЬКО claims/angles профиля и evidence_pack. Не называй человека жителем региона без прямого доказательства.
Верни только JSON:
{"status":"ready|needs_review","onboarding_paragraph":"...","claim_ids":["C1"],"evidence_ids":["E1"],"selected_angle_id":"A1"}

INPUT:
    """ + json.dumps({
        "candidate_post_url": row.get("post_url"),
        "content_origin_type": row.get("content_origin_type"),
        "candidate_post_summary": row.get("short_summary") or row.get("llm_reason") or "",
        "source_title": row.get("source_title"),
        "profile_summary": profile.get("profile_summary"),
        "claims": _json_list(profile.get("claims_json")),
        "candidate_angles": _json_list(profile.get("candidate_angles_json")),
        "evidence_pack": _evidence_items(evidence_row),
    }, ensure_ascii=False, indent=2)


def normalize_candidate_onboarding(
    result: dict[str, Any],
    *,
    profile: dict[str, Any],
    evidence_row: dict[str, Any],
    writer_fingerprint: str,
) -> dict[str, Any]:
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    paragraph = re.sub(r"\s+", " ", str(data.get("onboarding_paragraph") or "")).strip()
    claims = _json_list(profile.get("claims_json"))
    angles = _json_list(profile.get("candidate_angles_json"))
    valid_claims = {str(item.get("claim_id") or "") for item in claims if isinstance(item, dict)} - {""}
    valid_angles = {str(item.get("angle_id") or "") for item in angles if isinstance(item, dict)} - {""}
    valid_evidence = {str(item.get("evidence_id") or "") for item in _evidence_items(evidence_row)} - {""}
    claim_refs = [str(value) for value in (data.get("claim_ids") or []) if str(value)]
    evidence_refs = [str(value) for value in (data.get("evidence_ids") or []) if str(value)]
    angle_id = str(data.get("selected_angle_id") or "")
    refs_valid = bool(
        claim_refs
        and evidence_refs
        and set(claim_refs).issubset(valid_claims)
        and set(evidence_refs).issubset(valid_evidence)
        and (not angle_id or angle_id in valid_angles)
    )
    status = "ready" if (
        result.get("llm_gate_status") == "ok"
        and str(data.get("status") or "").lower() == "ready"
        and 300 <= len(paragraph) <= 600
        and refs_valid
    ) else "needs_review"
    return {
        "source_onboarding_status": status,
        "source_onboarding_paragraph": paragraph if status == "ready" else "",
        "source_onboarding_profile_id": profile.get("source_profile_id"),
        "source_onboarding_profile_fingerprint": profile.get("profile_fingerprint"),
        "source_onboarding_writer_fingerprint": writer_fingerprint,
        "source_onboarding_writer_prompt_version": SOURCE_ONBOARDING_WRITER_PROMPT_VERSION,
        "source_onboarding_entity_type": profile.get("entity_type"),
        "source_onboarding_claim_ids_json": json.dumps(claim_refs, ensure_ascii=False),
        "source_onboarding_evidence_ids_json": json.dumps(evidence_refs, ensure_ascii=False),
        "source_onboarding_selected_angle_id": angle_id,
        "source_onboarding_llm_status": result.get("llm_gate_status"),
        "source_onboarding_llm_reason": result.get("llm_reason", ""),
        "source_onboarding_paragraph_chars": len(paragraph),
    }


def enrich_accepted_rows_with_onboarding(
    rows: list[dict[str, Any]],
    *,
    max_llm: int,
    model: str,
    default_env_var_name: str,
    durable_budget: DurableGeminiBudget | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    profiles_to_write: dict[str, dict[str, Any]] = {}
    calls = 0
    stats = {"profile_calls": 0, "writer_calls": 0, "profiles_reused": 0, "paragraphs_ready": 0, "needs_review": 0}
    for row in rows:
        if str(row.get("publication_status") or "") != "gemini_accept":
            continue
        if str(row.get("sent_to_chat") or "").lower() == "true":
            continue
        if str(row.get("source_onboarding_status") or "") == "ready" and row.get("source_onboarding_paragraph"):
            stats["paragraphs_ready"] += 1
            continue
        evidence_row = row.get("_source_onboarding_evidence") if isinstance(row.get("_source_onboarding_evidence"), dict) else {}
        if evidence_row.get("evidence_status") != "sufficient":
            row["source_onboarding_status"] = "needs_review"
            row["source_onboarding_llm_reason"] = "insufficient_public_evidence"
            stats["needs_review"] += 1
            continue
        profile = row.get("_source_onboarding_profile") if isinstance(row.get("_source_onboarding_profile"), dict) else {}
        profile_is_current = bool(
            profile
            and str(profile.get("profile_status") or "") == "ready"
            and str(profile.get("evidence_fingerprint") or "") == str(evidence_row.get("evidence_fingerprint") or "")
            and str(profile.get("profile_prompt_version") or "") == SOURCE_ONBOARDING_PROFILE_PROMPT_VERSION
        )
        if profile_is_current:
            stats["profiles_reused"] += 1
        elif calls < max_llm:
            profile_fingerprint = hashlib.sha256(json.dumps({
                "kind": "source_onboarding_profile",
                "evidence_fingerprint": evidence_row.get("evidence_fingerprint"),
                "prompt_version": SOURCE_ONBOARDING_PROFILE_PROMPT_VERSION,
                "model": model,
            }, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
            result, attempted = _call_structured_with_budget(
                _source_profile_prompt(evidence_row),
                fingerprint="onboarding-profile-" + profile_fingerprint,
                model=model,
                default_env_var_name=default_env_var_name,
                durable_budget=durable_budget,
            )
            calls += int(attempted)
            stats["profile_calls"] += int(attempted)
            profile = normalize_source_onboarding_profile(
                result, evidence_row, model=model, profile_fingerprint=profile_fingerprint,
            )
            profiles_to_write[str(profile.get("source_profile_id") or evidence_row.get("canonical_source_key") or "")] = profile
        else:
            profile = {}

        if str(profile.get("profile_status") or "") != "ready":
            row["source_onboarding_status"] = "needs_review"
            row["source_onboarding_profile_id"] = evidence_row.get("source_profile_id")
            row["source_onboarding_llm_reason"] = "profile_not_ready_or_llm_budget_exhausted"
            stats["needs_review"] += 1
            continue
        if calls >= max_llm:
            row["source_onboarding_status"] = "needs_review"
            row["source_onboarding_profile_id"] = profile.get("source_profile_id")
            row["source_onboarding_llm_reason"] = "writer_llm_budget_exhausted"
            stats["needs_review"] += 1
            continue
        writer_fingerprint = hashlib.sha256(json.dumps({
            "kind": "source_onboarding_writer",
            "post_url": normalize_post_url(str(row.get("post_url") or "")),
            "profile_fingerprint": profile.get("profile_fingerprint"),
            "prompt_version": SOURCE_ONBOARDING_WRITER_PROMPT_VERSION,
            "model": model,
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        result, attempted = _call_structured_with_budget(
            _candidate_onboarding_prompt(row, profile, evidence_row),
            fingerprint="onboarding-writer-" + writer_fingerprint,
            model=model,
            default_env_var_name=default_env_var_name,
            durable_budget=durable_budget,
        )
        calls += int(attempted)
        stats["writer_calls"] += int(attempted)
        row.update(normalize_candidate_onboarding(
            result,
            profile=profile,
            evidence_row=evidence_row,
            writer_fingerprint=writer_fingerprint,
        ))
        if row.get("source_onboarding_status") == "ready":
            stats["paragraphs_ready"] += 1
        else:
            stats["needs_review"] += 1
    return rows, list(profiles_to_write.values()), stats


def write_source_onboarding_rows(
    pool: Any,
    ydb: Any,
    table: str,
    *,
    evidence_rows: list[dict[str, Any]],
    profile_rows: list[dict[str, Any]],
    run_id: str,
) -> dict[str, int]:
    now = rt.utc_now_iso()
    items: list[tuple[str, str, dict[str, Any]]] = []
    seen: set[tuple[str, str]] = set()
    for kind, rows, id_field in (
        ("source_onboarding_evidence_item", evidence_rows, "source_profile_id"),
        ("source_onboarding_profile_item", profile_rows, "source_profile_id"),
    ):
        for row in rows:
            item_id = str(row.get(id_field) or "")
            if not item_id or (kind, item_id) in seen:
                continue
            seen.add((kind, item_id))
            payload = {
                **{k: v for k, v in row.items() if not str(k).startswith("_")},
                "run_id": run_id,
                "updated_at": now,
                "last_seen_run_id": run_id,
            }
            items.append((f"{kind}:{item_id}", kind, payload))
    if not items:
        return {"evidence_written": 0, "profiles_written": 0}

    def op(session: Any) -> int:
        rt.ensure_ydb_kv_table(ydb, session, table)
        return rt.ydb_upsert_json_many(session, ydb, table, items, now, chunk_size=20, timeout_seconds=8)

    pool.retry_operation_sync(op)
    return {
        "evidence_written": sum(1 for _pk, kind, _payload in items if kind == "source_onboarding_evidence_item"),
        "profiles_written": sum(1 for _pk, kind, _payload in items if kind == "source_onboarding_profile_item"),
    }


def write_publication_rows(pool: Any, ydb: Any, table: str, rows: list[dict[str, Any]], run_id: str) -> int:
    now = rt.utc_now_iso()
    fields = [
        "run_id", "updated_at", "last_seen_run_id", "post_url", "original_post_url", "post_url_normalization_version",
        "canonical_source_key", "authoritative_source_found", "source_title", "source_url", "post_date",
        "content_origin_type", "publication_content_type", "publication_language", "external_publication_id",
        "external_research_quality_score", "source_overview", "diversity_topics",
        "rights_policy", "media_use_policy", "media_reuse_allowed",
        "authoritative_source_fingerprint", "authoritative_source_fingerprint_version",
        "publication_rank", "publication_pre_score", "publication_status", "publication_candidate_status", "overall_media_score", "postcardness_score",
        "aesthetic_score", "technical_quality_score", "publication_safety_score", "image_queue_status", "image_model_input_type",
        "image_quality_decision", "image_quality_reason", "image_quality_terminality",
        "image_decision_contract_version", "image_acquisition_status", "expected_image_count", "fetched_image_count",
        "images_scored_actual_count", "input_media_manifest_hash", "visual_content_track", "visual_fit_score",
        "web_gallery_discovery_status", "web_gallery_discovered_count", "web_gallery_used_count", "web_gallery_discovery_version",
        "image_vlm_status", "image_vlm_decision", "image_vlm_reason", "image_vlm_prompt_version", "image_vlm_decision_version",
        "image_vlm_editorial_suitability_score", "image_vlm_aesthetic_score", "image_vlm_publication_safety_score",
        "operator_visual_decision", "operator_visual_decision_reason", "operator_visual_decision_at",
        "operator_visual_decision_version", "operator_visual_strong_publishable_image", "operator_visual_media_manifest_hash",
        "media_kind", "media_review_mode", "manual_media_review_required", "video_manual_review_eligible",
        "vector_gate_status", "candidate_score", "source_class_guess", "short_summary", "text", "llm_gate_status",
        "llm_decision", "llm_reason", "llm_model", "llm_limit_source", "content_type", "visit_evidence_type",
        "publication_draft_status", "publication_draft_title", "publication_draft_source_attribution",
        "publication_draft_telegram_text", "publication_draft_vk_text",
        "publication_draft_fact_points_json", "publication_draft_prompt_version",
        "publication_draft_contract_version", "publication_draft_input_fingerprint",
        "publication_draft_evidence_hash", "publication_draft_evidence_json",
        "publication_draft_history_json", "publication_draft_editorial_plan_json",
        "publication_draft_grounding_map_json", "publication_draft_critic_json",
        "publication_draft_stage_audit_json",
        "publication_draft_generation_attempts", "publication_draft_link_metadata_json",
        "publication_presentation_mode", "publication_media_materialization_status",
        "publication_media_materialization_reason", "publication_media_materialization_contract_version",
        "publication_presentation_manifest_json",
        "selected_media_ids", "selected_primary_media_id", "selected_primary_image_ordinal",
        "selected_media_materialization_json", "selected_media_materialization_fingerprint",
        "media_materialization_items_json", "input_media_manifest_hash",
        "presentation_recommendation", "presentation_recommendation_reason", "presentation_max_assets",
        "publication_draft_backfill_status", "publication_draft_backfill_reason",
        "publication_draft_backfill_next_attempt_after", "publication_draft_backfill_version",
        "publication_draft_backfill_transport", "publication_draft_backfill_attempt_count",
        "publication_draft_backfill_last_attempt_at", "publication_draft_backfill_text_hash",
        "publication_draft_backfill_request_fingerprint", "publication_draft_backfill_provider_called",
        "publication_draft_backfill_provider_call_count", "publication_draft_backfill_llm_gate_status",
        "legacy_review_migration_version", "legacy_review_migrated_at",
        "legacy_principle_status", "legacy_copy_status", "legacy_operator_review_fingerprint",
        "legacy_operator_review_decision", "legacy_operator_review_rewrite_status",
        "operator_review_fingerprint", "operator_review_decision", "operator_review_rewrite_status",
        "operator_review_positive", "operator_review_negative", "operator_review_rewrite_requested",
        "has_firsthand_visit_evidence", "emotion_or_impression_evidence", "review_or_opinion_evidence",
        "memorable_detail_evidence", "original_photo_evidence", "whole_post_about_kaliningrad_oblast_score",
        "kaliningrad_oblast_only_scope", "kaliningrad_mention_role", "llm_usage_input_tokens", "llm_usage_output_tokens", "llm_usage_total_tokens",
        "publication_eligibility_verdict", "publication_eligibility_evidence", "publication_eligibility_gate_version",
        "publication_eligibility_evidence_fingerprint",
        "publication_tombstone", "publication_revoked", "revoked_at", "finalization_status", "finalization_trigger",
        "text_restore_reason", "next_action",
        "attempt_count", "last_attempt_at", "next_attempt_after", "llm_attempted_this_run", "finalizer_state_version",
        "llm_request_fingerprint", "llm_prompt_version", "llm_budget_id", "llm_budget_status",
        "source_onboarding_status", "source_onboarding_paragraph", "source_onboarding_profile_id",
        "source_onboarding_profile_fingerprint", "source_onboarding_writer_fingerprint",
        "source_onboarding_writer_prompt_version", "source_onboarding_entity_type",
        "source_onboarding_claim_ids_json", "source_onboarding_evidence_ids_json",
        "source_onboarding_selected_angle_id", "source_onboarding_llm_status",
        "source_onboarding_llm_reason", "source_onboarding_paragraph_chars",
        "sent_to_chat", "sent_message_id", "sent_at", "sent_chat_id", "delivery_key", "delivery_random_id",
    ]
    items = []
    for row in rows:
        durable_row = dict(row)
        terminal_text = bool(
            str(row.get("sent_to_chat") or "").lower() == "true"
            or str(row.get("finalization_status") or "").lower() == "terminal"
            or str(row.get("llm_decision") or "").lower() in {"accept", "reject"}
            or str(row.get("publication_status") or "").lower().startswith(("gemini_accept", "gemini_reject", "operator_rejected"))
        )
        if terminal_text:
            durable_row.pop("text", None)
            durable_row.pop("full_text", None)
            durable_row.pop("text_excerpt", None)
            durable_row.pop("short_summary", None)
        payload = rt.compact_record(
            {
                **durable_row,
                "run_id": run_id,
                "updated_at": now,
                "last_seen_run_id": run_id,
                "finalizer_state_version": PUBLICATION_FINALIZER_STATE_VERSION,
                "post_url": normalize_post_url(str(row.get("post_url") or "")),
            },
            fields,
            max_len=700,
        )
        # A retryable Gemini row still needs the exact post body. Preserve it
        # losslessly only while active; terminal rows above delete it entirely.
        if not terminal_text and str(durable_row.get("text") or ""):
            payload["text"] = str(durable_row["text"])
        # The staged writer's grounding is a publication audit contract, not a
        # display excerpt.  Never truncate it through compact_record.
        for lossless_field in (
            "publication_draft_evidence_json",
            "publication_draft_history_json",
            "publication_draft_editorial_plan_json",
            "publication_draft_grounding_map_json",
            "publication_draft_critic_json",
            "publication_draft_stage_audit_json",
            "publication_presentation_manifest_json",
            "selected_media_materialization_json",
            "media_materialization_items_json",
        ):
            if durable_row.get(lossless_field) not in (None, ""):
                payload[lossless_field] = durable_row[lossless_field]
        key = payload.get("post_url") or payload.get("image_queue_id") or str(row.get("publication_rank"))
        if key:
            items.append(("publication_candidate_item:" + str(key).replace("publication_candidate_item:", ""), "publication_candidate_item", payload))
    if not items:
        return 0

    def op(session: Any) -> int:
        rt.ensure_ydb_kv_table(ydb, session, table)
        return rt.ydb_upsert_json_many(session, ydb, table, items, now, chunk_size=20, timeout_seconds=8)

    return int(pool.retry_operation_sync(op) or 0)


def write_text_restore_post_link_rows(
    pool: Any,
    ydb: Any,
    table: str,
    rows: list[dict[str, Any]],
    *,
    run_id: str,
) -> int:
    """Re-open exact Telegram links whose active text was compacted too soon.

    This is a queue handoff only; the finalizer never fetches Telegram or falls
    back to public web HTML. CandidateReport consumes these rows through its
    role-scoped DISCOVERY1 Telethon session and existing request governor.
    """

    pending = [
        row
        for row in rows
        if str(row.get("publication_status") or "") == "text_restore_pending"
        and not _terminal_decision_blocks_text_restore(row)
        and re.match(r"^https?://t\.me/[^/]+/[0-9]+$", normalize_post_url(str(row.get("post_url") or "")), re.I)
    ]
    if not pending:
        return 0
    now = rt.utc_now_iso()

    def op(session: Any) -> int:
        existing_items = rt.ydb_select_kind_items(
            session,
            ydb,
            table,
            "post_link_queue_item",
            limit=5000,
        )
        existing_by_url = {
            normalize_post_url(str(item.get("post_url") or item.get("keyword_hit_post_url") or "")): (pk, item)
            for pk, item in existing_items.items()
            if normalize_post_url(str(item.get("post_url") or item.get("keyword_hit_post_url") or ""))
        }
        items: list[tuple[str, str, dict[str, Any]]] = []
        for row in pending:
            post_url = normalize_post_url(str(row.get("post_url") or ""))
            existing_pk, existing = existing_by_url.get(post_url, ("", {}))
            queue_id = str(existing.get("post_link_queue_id") or "postlink_" + rt.stable_hash(post_url))
            source_url = str(row.get("source_url") or existing.get("source_url") or "")
            source_key = str(row.get("canonical_source_key") or existing.get("canonical_source_key") or "")
            existing_status = str(existing.get("post_link_status") or "")
            active_retry_statuses = {
                "",
                "pending_fetch",
                "retry_fetch",
                "fetch_error",
                "retry_wait_entity_cache",
            }
            keep_active_retry = existing_status in active_retry_statuses and bool(existing)
            payload = {
                **{k: v for k, v in existing.items() if not str(k).startswith("_ydb_")},
                "post_link_queue_id": queue_id,
                "post_link_status": existing_status if keep_active_retry else "retry_fetch",
                "post_link_priority": 0,
                "priority_reason": "publication_text_restore_after_active_payload_prune",
                "publication_text_restore_requested": "true",
                "publication_text_restore_reason": row.get("text_restore_reason") or "eligible_post_text_missing_after_async_media_scoring",
                "publication_text_restore_requested_at": row.get("updated_at") or row.get("last_attempt_at") or now,
                "publication_text_restore_request_run_id": run_id,
                "post_url": post_url,
                "keyword_hit_post_url": post_url,
                "source_key": source_key,
                "canonical_source_key": source_key,
                "source_url": source_url,
                "keyword_hit_source_url": source_url,
                "source_title": row.get("source_title") or existing.get("source_title") or "",
                "handle": row.get("handle") or existing.get("handle") or "",
                "username_or_handle": row.get("username_or_handle") or row.get("handle") or existing.get("username_or_handle") or existing.get("handle") or "",
                "post_date": row.get("post_date") or existing.get("post_date") or "",
                "discovery_type": "publication_text_restore",
                "edge_type": "publication_text_restore",
                "first_seen_run_id": existing.get("first_seen_run_id") or run_id,
                "last_seen_run_id": run_id,
                "run_id": run_id,
                "updated_at": now,
                "next_action": "refetch_exact_post_then_finalize_publication",
            }
            if not keep_active_retry:
                payload.update({
                    "last_attempt_run_id": "",
                    "last_attempt_at": "",
                    "fetch_error_code": "",
                    "fetch_error_message": "",
                    "next_attempt_after": "",
                })
            pk = str(existing_pk or f"post_link_queue_item:{queue_id}")
            items.append((pk, "post_link_queue_item", payload))
        rt.ensure_ydb_kv_table(ydb, session, table)
        return rt.ydb_upsert_json_many(session, ydb, table, items, now, chunk_size=20, timeout_seconds=8)

    return int(pool.retry_operation_sync(op) or 0)


def prune_terminal_working_text(pool: Any, ydb: Any, table: str, rows: list[dict[str, Any]]) -> dict[str, int]:
    """Remove post text after a final publication verdict becomes durable.

    Full text is legitimate working state while E5/BGE, media and Gemini still
    need it. After accept/reject/sent it has no operational purpose. Keep only
    hashes, model scores and decision evidence across every projection.
    """
    terminal_urls = {
        normalize_post_url(str(row.get("post_url") or ""))
        for row in rows
        if (
            str(row.get("sent_to_chat") or "").lower() == "true"
            or str(row.get("finalization_status") or "").lower() == "terminal"
            or str(row.get("llm_decision") or "").lower() in {"accept", "reject", "needs_review"}
            or str(row.get("publication_status") or "").lower().startswith(
                ("gemini_accept", "gemini_reject", "gemini_needs_review", "operator_rejected")
            )
        )
    } - {""}
    if not terminal_urls:
        return {"terminal_urls": 0, "rows_pruned": 0}

    def read(session: Any) -> list[tuple[str, str, dict[str, Any]]]:
        out: list[tuple[str, str, dict[str, Any]]] = []
        for kind, limit in [
            ("processed_post_item", 20000),
            ("candidate_memory_item", 5000),
            ("image_queue_item", 5000),
            ("text_vector_enrichment_item", 20000),
        ]:
            for pk, payload in rt.ydb_select_kind_items(session, ydb, table, kind, limit=limit).items():
                if normalize_post_url(str(payload.get("post_url") or "")) in terminal_urls:
                    out.append((pk, kind, dict(payload)))
        return out

    matched = pool.retry_operation_sync(read)
    now = rt.utc_now_iso()
    items: list[tuple[str, str, dict[str, Any]]] = []
    for pk, kind, payload in matched:
        changed = False
        for field in (
            "text", "full_text", "text_excerpt", "short_summary", "raw",
            "why_keep_in_memory", "keyword_hit_text_excerpt",
        ):
            if field in payload:
                payload.pop(field, None)
                changed = True
        if not changed:
            continue
        payload["text_payload_pruned_terminal"] = True
        payload["text_payload_pruned_at"] = now
        items.append((pk, kind, {k: v for k, v in payload.items() if not str(k).startswith("_ydb_")}))
    if not items:
        return {"terminal_urls": len(terminal_urls), "rows_pruned": 0}

    def write(session: Any) -> int:
        rt.ensure_ydb_kv_table(ydb, session, table)
        return rt.ydb_upsert_json_many(session, ydb, table, items, now, chunk_size=50, timeout_seconds=8)

    return {
        "terminal_urls": len(terminal_urls),
        "rows_pruned": int(pool.retry_operation_sync(write) or 0),
    }


def write_xlsx(path: Path, verified: list[dict[str, Any]], all_rows: list[dict[str, Any]], include_unverified: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "publication_rank", "publication_status", "llm_decision", "publication_pre_score", "post_url", "source_title",
        "source_class_guess", "overall_media_score", "postcardness_score", "aesthetic_score", "candidate_score",
        "vector_gate_status", "content_type", "visit_evidence_type", "llm_reason",
        "source_onboarding_status", "source_onboarding_paragraph", "short_summary", "text",
    ]
    verified_ids = {row.get("post_url") for row in verified}
    export_rows = verified + [row for row in all_rows if row.get("post_url") not in verified_ids][:include_unverified]
    if Workbook is None:
        csv_path = path.with_suffix(".csv")
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            writer.writeheader()
            for row in export_rows:
                writer.writerow({col: row.get(col, "") for col in cols})
        return csv_path
    wb = Workbook()
    ws = wb.active
    ws.title = "publication_shortlist"
    ws.append(cols)
    for row in export_rows:
        ws.append([row.get(col, "") for col in cols])
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = min(70, max(12, max(len(str(cell.value or "")) for cell in col[:30]) + 2))
    wb.save(path)
    return path


def is_newly_accepted_in_run(row: dict[str, Any]) -> bool:
    if str(row.get("publication_status") or "").lower() != "gemini_accept":
        return False
    previous = row.get("_previous_publication") if isinstance(row.get("_previous_publication"), dict) else {}
    previous_status = str(previous.get("publication_status") or "").lower()
    previous_candidate_status = str(previous.get("publication_candidate_status") or "").lower()
    was_already_accepted_or_sent = (
        previous_status == "gemini_accept"
        or previous_candidate_status in {"llm_confirmed", "sent_to_chat", "accepted_for_publication"}
        or str(previous.get("sent_to_chat") or "").lower() == "true"
    )
    return not was_already_accepted_or_sent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--max-llm", type=int, default=10)
    parser.add_argument(
        "--llm-budget-id",
        default="",
    )
    parser.add_argument("--llm-budget-max", type=int, default=None)
    parser.add_argument("--limit-images", type=int, default=5000)
    parser.add_argument("--limit-memory", type=int, default=20000)
    parser.add_argument("--model", default=os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.1-flash-lite")
    parser.add_argument("--default-env-var-name", default=os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "artifacts" / "codex" / "region-talk-finalizer")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--prioritize-source-evidence-only",
        action="store_true",
        help="Promote only strong finalists blocked by missing source attestation; do not call Gemini.",
    )
    parser.add_argument("--reverify-existing", action="store_true", help="Ignore existing publication_candidate_item verifier statuses and call Gemini again.")
    args = parser.parse_args()
    load_env(args.env_file)
    # Local finalization uses the same proven endpoint/database/IAM discovery
    # path as the orchestrator. The .env intentionally need not persist a
    # short-lived YC token.
    import scripts.region_talk_orchestrator as orchestrator  # noqa: PLC0415
    orchestrator.ensure_child_ydb_env(allow_yc_fallback=True)
    args.llm_budget_id = args.llm_budget_id or os.getenv("REGION_TALK_LLM_BUDGET_ID") or datetime.now(timezone.utc).strftime("region-talk-debug-%Y%m%d")
    args.llm_budget_max = _env_int("REGION_TALK_LLM_BUDGET_MAX", 100) if args.llm_budget_max is None else args.llm_budget_max
    os.environ.setdefault("REGION_TALK_LLM_MODEL", args.model)
    os.environ.setdefault("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME", args.default_env_var_name)
    os.environ.setdefault("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "45")
    os.environ.setdefault("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", os.environ.get("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "45"))
    os.environ.setdefault("REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS", "2200")
    # One logical finalizer request equals at most one provider attempt. A
    # failed attempt is retried by the durable finalizer state, not invisibly
    # inside the provider gateway, keeping the <=100 budget auditable.
    os.environ.setdefault("GOOGLE_AI_MAX_RETRIES", "1")
    run_id = args.run_id or "region-talk-finalizer-local-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.output_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (
        ydb,
        driver,
        pool,
        table,
        rows,
        strict_source_priority_rows,
    ) = read_live_rows(
        args.limit_images,
        args.limit_memory,
        reverify_existing=args.reverify_existing,
    )
    priority_now = rt.utc_now_iso()
    source_priority_rows = source_evidence_priority_updates(rows, now_iso=priority_now)
    source_priority_rows.extend(strict_source_priority_rows)
    source_priority_clear_rows = source_evidence_priority_clear_updates(rows, now_iso=priority_now)
    source_rows_by_key = {
        canonical_source_key_for_row(row): row
        for row in source_priority_rows + source_priority_clear_rows
        if canonical_source_key_for_row(row)
    }
    source_priority_written = 0 if args.dry_run else write_source_evidence_priority_rows(
        pool, ydb, table, list(source_rows_by_key.values()), run_id=run_id,
    )
    durable_budget = None
    if not args.dry_run and not args.prioritize_source_evidence_only and args.max_llm > 0:
        require_google_genai_runtime()
        durable_budget = DurableGeminiBudget(
            pool,
            ydb,
            table,
            budget_id=args.llm_budget_id,
            budget_max=min(100, max(0, int(args.llm_budget_max))),
        )
    verified = [] if args.dry_run or args.prioritize_source_evidence_only else verify_rows(
        rows,
        max_llm=min(100, max(0, int(args.max_llm))),
        model=args.model,
        default_env_var_name=args.default_env_var_name,
        durable_budget=durable_budget,
    )
    verifier_provider_calls = len([row for row in verified if row.get("llm_attempted_this_run") == "true"])
    onboarding_stats = {"profile_calls": 0, "writer_calls": 0, "profiles_reused": 0, "paragraphs_ready": 0, "needs_review": 0}
    onboarding_profile_rows: list[dict[str, Any]] = []
    if not args.dry_run and not args.prioritize_source_evidence_only:
        verified, onboarding_profile_rows, onboarding_stats = enrich_accepted_rows_with_onboarding(
            verified,
            max_llm=max(0, min(100, int(args.max_llm)) - verifier_provider_calls),
            model=args.model,
            default_env_var_name=args.default_env_var_name,
            durable_budget=durable_budget,
        )
    evidence_by_profile = {
        str(evidence.get("source_profile_id") or ""): evidence
        for row in verified
        for evidence in [row.get("_source_onboarding_evidence")]
        if (
            str(row.get("publication_status") or "") == "gemini_accept"
            and isinstance(evidence, dict)
            and evidence.get("source_profile_id")
        )
    }
    onboarding_write = (
        {"evidence_written": 0, "profiles_written": 0}
        if args.dry_run
        else write_source_onboarding_rows(
            pool,
            ydb,
            table,
            evidence_rows=list(evidence_by_profile.values()),
            profile_rows=onboarding_profile_rows,
            run_id=run_id,
        )
    )
    written = 0 if args.dry_run else write_publication_rows(pool, ydb, table, verified, run_id)
    text_restore_links_written = (
        0
        if args.dry_run
        else write_text_restore_post_link_rows(pool, ydb, table, verified, run_id=run_id)
    )
    text_prune = (
        {"terminal_urls": 0, "rows_pruned": 0}
        if args.dry_run
        else prune_terminal_working_text(pool, ydb, table, verified)
    )
    shortlist_artifact = write_xlsx(out_dir / "region-talk-publication-shortlist.xlsx", verified, rows, include_unverified=50)
    payload = {
        "run_id": run_id,
        "finalizer_input_rows": len(rows),
        "actual_scored_rows": sum(1 for row in rows if row.get("image_queue_status") == "actual_scored" and row.get("image_model_input_type") == "actual_image"),
        "video_manual_review_rows": sum(1 for row in rows if rt.is_video_media_candidate(row)),
        "external_link_article_rows": sum(1 for row in rows if rt.uses_external_link_article_lane(row)),
        "llm_calls": verifier_provider_calls + onboarding_stats["profile_calls"] + onboarding_stats["writer_calls"],
        "verifier_llm_calls": verifier_provider_calls,
        "onboarding_profile_llm_calls": onboarding_stats["profile_calls"],
        "onboarding_writer_llm_calls": onboarding_stats["writer_calls"],
        "onboarding_profiles_reused": onboarding_stats["profiles_reused"],
        "onboarding_paragraphs_ready": onboarding_stats["paragraphs_ready"],
        "onboarding_needs_review": onboarding_stats["needs_review"],
        "onboarding_evidence_rows_written": onboarding_write["evidence_written"],
        "onboarding_profile_rows_written": onboarding_write["profiles_written"],
        "accepted_new": sum(1 for row in verified if is_newly_accepted_in_run(row)),
        "accepted_total": sum(1 for row in rows if row.get("publication_status") == "gemini_accept" or row.get("publication_candidate_status") == "llm_confirmed"),
        "written": written,
        "text_restore_pending_total": sum(
            1 for row in verified if row.get("publication_status") == "text_restore_pending"
        ),
        "text_restore_post_links_written": text_restore_links_written,
        "terminal_text_urls_pruned": int(text_prune.get("terminal_urls") or 0),
        "terminal_text_rows_pruned": int(text_prune.get("rows_pruned") or 0),
        "source_evidence_priority_total": len(source_priority_rows),
        "source_evidence_priority_written": source_priority_written,
        "source_evidence_priority_cleared_total": len(source_priority_clear_rows),
        "llm_budget_id": durable_budget.budget_id if durable_budget is not None else args.llm_budget_id,
        "llm_budget_max": durable_budget.budget_max if durable_budget is not None else min(100, max(0, int(args.llm_budget_max))),
        "llm_budget_reserved_total": durable_budget.used_total if durable_budget is not None else 0,
        "llm_budget_replayed_total": durable_budget.replayed_total if durable_budget is not None else 0,
        "llm_budget_blocked_total": durable_budget.blocked_total if durable_budget is not None else 0,
        "shortlist_artifact": str(shortlist_artifact),
        "xlsx": str(shortlist_artifact) if shortlist_artifact.suffix == ".xlsx" else "",
        "verified": verified,
        "top_actual": rows[:50],
    }
    (out_dir / "publication_finalizer_results.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: payload[k] for k in ["run_id", "finalizer_input_rows", "actual_scored_rows", "video_manual_review_rows", "external_link_article_rows", "llm_calls", "verifier_llm_calls", "onboarding_profile_llm_calls", "onboarding_writer_llm_calls", "onboarding_profiles_reused", "onboarding_paragraphs_ready", "onboarding_needs_review", "onboarding_evidence_rows_written", "onboarding_profile_rows_written", "accepted_new", "accepted_total", "written", "text_restore_pending_total", "text_restore_post_links_written", "terminal_text_urls_pruned", "terminal_text_rows_pruned", "source_evidence_priority_total", "source_evidence_priority_written", "source_evidence_priority_cleared_total", "llm_budget_id", "llm_budget_max", "llm_budget_reserved_total", "llm_budget_replayed_total", "llm_budget_blocked_total", "shortlist_artifact"]}, ensure_ascii=False, indent=2))
    try:
        driver.stop(timeout=5)
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
