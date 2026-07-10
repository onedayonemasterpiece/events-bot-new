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
import csv
import hashlib
import html
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
if str(PROJECT_ROOT / "kaggle" / "RegionTalkCandidateReport") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "kaggle" / "RegionTalkCandidateReport"))
import region_talk_candidate_report as rt  # type: ignore  # noqa: E402


POST_URL_NORMALIZATION_VERSION = "region_talk_post_url_v1"
PUBLICATION_FINALIZER_STATE_VERSION = "region_talk_publication_finalizer_v2"
AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION = "region_talk_source_fingerprint_v1"
PUBLIC_TME_FALLBACK_ENV = "REGION_TALK_ALLOW_PUBLIC_TME_S_FALLBACK"
TERMINAL_PUBLICATION_STATUSES = {
    "gemini_accept",
    "gemini_reject",
    "gemini_needs_review",
    "no_text_for_gemini",
}
TERMINAL_CANDIDATE_STATUSES = {
    "llm_confirmed",
    "llm_rejected",
    "llm_needs_review",
    "filtered_before_llm",
    "sent_to_chat",
    "accepted_for_publication",
}
RETRYABLE_PUBLICATION_STATUSES = {"gemini_rate_limited", "gemini_error", "gemini_unknown"}
RETRYABLE_CANDIDATE_STATUSES = {"llm_budget_deferred", "llm_error", "retry_due"}
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
        "queue_item_updated_at": source.get("queue_item_updated_at") or source.get("updated_at") or source.get("_ydb_updated_at") or "",
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


def finalization_trigger(publication: dict[str, Any] | None, *, now_iso: str, reverify_existing: bool = False) -> str:
    """Classify one normalized URL as never-finalized, retry-due, or inactive."""
    if reverify_existing:
        return "reverify_requested"
    if not publication:
        return "never_finalized"
    status = str(publication.get("publication_status") or "").strip().lower()
    candidate_status = str(publication.get("publication_candidate_status") or "").strip().lower()
    gate_status = str(publication.get("llm_gate_status") or "").strip().lower()
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
    visual = float(row.get("overall_media_score") or 0)
    postcard = float(row.get("postcardness_score") or 0)
    candidate = float(row.get("candidate_score") or 0)
    vector = 0.12 if row.get("vector_gate_status") == "vector_accept_candidate" else 0.06
    return round(nonlocal_bonus + visual * 0.45 + postcard * 0.20 + candidate * 0.15 + vector, 4)


def read_live_rows(limit_images: int, limit_memory: int, *, reverify_existing: bool = False) -> tuple[Any, Any, Any, str, list[dict[str, Any]]]:
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
    ]:
        images = rt.ydb_select_kind_items(session, ydb, table, "image_queue_item", limit=limit_images)
        memory = rt.ydb_select_kind_items(session, ydb, table, "candidate_memory_item", limit=limit_memory)
        publications = rt.ydb_select_kind_items(session, ydb, table, "publication_candidate_item", limit=limit_images)
        sources = rt.ydb_select_kind_items(session, ydb, table, "source_queue_item", limit=limit_memory)
        source_statuses = rt.ydb_select_kind_items(session, ydb, table, "source_status_item", limit=limit_memory)
        online_sources = rt.ydb_select_kind_items(session, ydb, table, "online_source_item", limit=limit_memory)
        return images, memory, publications, sources, source_statuses, online_sources

    (
        images_by_pk,
        memory_by_pk,
        publications_by_pk,
        source_items,
        source_status_items,
        online_source_items,
    ) = pool.retry_operation_sync(op)
    memory_by_url = _publication_by_normalized_url(memory_by_pk)
    publication_by_url = _publication_by_normalized_url(publications_by_pk)
    sources_by_key = authoritative_source_index(source_items, source_status_items, online_source_items)
    now_iso = rt.utc_now_iso()
    rows_by_url: dict[str, dict[str, Any]] = {}
    for image in images_by_pk.values():
        if image.get("image_queue_status") != "actual_scored" or image.get("image_model_input_type") != "actual_image":
            continue
        original_post_url = str(image.get("post_url") or "")
        post_url = normalize_post_url(original_post_url)
        if not post_url:
            continue
        memory = memory_by_url.get(post_url, {})
        publication = publication_by_url.get(post_url, {})
        row = {**memory, **image}
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
                "publication_eligibility_verdict", "publication_eligibility_evidence", "publication_eligibility_gate_version",
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
        row["text"] = memory.get("text") or memory.get("text_excerpt") or ""
        row["publication_pre_score"] = publication_pre_score(row)
        previous = rows_by_url.get(post_url)
        if previous is None or str(row.get("updated_at") or row.get("_ydb_updated_at") or "") >= str(previous.get("updated_at") or previous.get("_ydb_updated_at") or ""):
            rows_by_url[post_url] = row
    rows = list(rows_by_url.values())
    rows.sort(key=lambda r: (-float(r.get("publication_pre_score") or 0), r.get("source_class_guess") != "nonlocal_travel_or_general_source"))
    return ydb, driver, pool, table, rows


def _json_evidence(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return str(value or "")


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
    row["publication_eligibility_gate_version"] = gate_version
    return normalized_verdict, result


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
    return (
        is_tombstone
        and str(previous.get("publication_eligibility_verdict") or "") == verdict
        and str(previous.get("publication_eligibility_gate_version") or "")
        == str(row.get("publication_eligibility_gate_version") or "")
        and str(previous.get("publication_eligibility_evidence") or "")
        == str(row.get("publication_eligibility_evidence") or "")
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
) -> list[dict[str, Any]]:
    now_iso = now_iso or rt.utc_now_iso()
    results: list[dict[str, Any]] = []
    llm_calls = 0
    for row in rows:
        verdict, _raw_eligibility = _eligibility_fields(row)
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
            continue
        if not row.get("text"):
            row["text"] = telegram_public_text(str(row.get("post_url") or ""))
        if not row.get("text") and row.get("short_summary"):
            row["text"] = "summary: " + str(row.get("short_summary") or "")
        if not row.get("text"):
            row["publication_status"] = "no_text_for_gemini"
            row["publication_candidate_status"] = "filtered_before_llm"
            row["finalization_status"] = "terminal"
            row["llm_attempted_this_run"] = "false"
            row["next_attempt_after"] = ""
            results.append(row)
            continue
        if llm_calls >= max(0, max_llm):
            continue
        llm_calls += 1
        row["publication_rank"] = llm_calls
        row["attempt_count"] = int(row.get("attempt_count") or 0) + 1
        row["last_attempt_at"] = now_iso
        row["llm_attempted_this_run"] = "true"
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
        }
        print(
            f"[region-talk-finalizer] Gemini {llm_calls}/{max(0, max_llm)} {row.get('post_url')} "
            f"source={row.get('source_title')} pre_score={row.get('publication_pre_score')}",
            flush=True,
        )
        try:
            llm_verdict = rt.call_region_talk_semantic_llm(
                row,
                evidence,
                model=model,
                default_env_var_name=default_env_var_name,
            )
        except Exception as exc:
            llm_verdict = {"llm_gate_status": "error", "llm_reason": f"{type(exc).__name__}: {str(exc)[:240]}"}
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


def write_publication_rows(pool: Any, ydb: Any, table: str, rows: list[dict[str, Any]], run_id: str) -> int:
    now = rt.utc_now_iso()
    fields = [
        "run_id", "updated_at", "last_seen_run_id", "post_url", "original_post_url", "post_url_normalization_version",
        "canonical_source_key", "authoritative_source_found", "source_title", "source_url", "post_date",
        "authoritative_source_fingerprint", "authoritative_source_fingerprint_version",
        "publication_rank", "publication_pre_score", "publication_status", "publication_candidate_status", "overall_media_score", "postcardness_score",
        "aesthetic_score", "technical_quality_score", "publication_safety_score", "image_queue_status", "image_model_input_type",
        "vector_gate_status", "candidate_score", "source_class_guess", "short_summary", "text", "llm_gate_status",
        "llm_decision", "llm_reason", "llm_model", "llm_limit_source", "content_type", "visit_evidence_type",
        "has_firsthand_visit_evidence", "emotion_or_impression_evidence", "review_or_opinion_evidence",
        "memorable_detail_evidence", "original_photo_evidence", "whole_post_about_kaliningrad_oblast_score",
        "kaliningrad_mention_role", "llm_usage_input_tokens", "llm_usage_output_tokens", "llm_usage_total_tokens",
        "publication_eligibility_verdict", "publication_eligibility_evidence", "publication_eligibility_gate_version",
        "publication_tombstone", "publication_revoked", "revoked_at", "finalization_status", "finalization_trigger",
        "attempt_count", "last_attempt_at", "next_attempt_after", "llm_attempted_this_run", "finalizer_state_version",
    ]
    items = []
    for row in rows:
        payload = rt.compact_record(
            {
                **row,
                "run_id": run_id,
                "updated_at": now,
                "last_seen_run_id": run_id,
                "finalizer_state_version": PUBLICATION_FINALIZER_STATE_VERSION,
                "post_url": normalize_post_url(str(row.get("post_url") or "")),
            },
            fields,
            max_len=1800,
        )
        key = payload.get("post_url") or payload.get("image_queue_id") or str(row.get("publication_rank"))
        if key:
            items.append(("publication_candidate_item:" + str(key).replace("publication_candidate_item:", ""), "publication_candidate_item", payload))
    if not items:
        return 0

    def op(session: Any) -> int:
        rt.ensure_ydb_kv_table(ydb, session, table)
        return rt.ydb_upsert_json_many(session, ydb, table, items, now, chunk_size=20, timeout_seconds=8)

    return int(pool.retry_operation_sync(op) or 0)


def write_xlsx(path: Path, verified: list[dict[str, Any]], all_rows: list[dict[str, Any]], include_unverified: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "publication_rank", "publication_status", "llm_decision", "publication_pre_score", "post_url", "source_title",
        "source_class_guess", "overall_media_score", "postcardness_score", "aesthetic_score", "candidate_score",
        "vector_gate_status", "content_type", "visit_evidence_type", "llm_reason", "short_summary", "text",
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--max-llm", type=int, default=10)
    parser.add_argument("--limit-images", type=int, default=5000)
    parser.add_argument("--limit-memory", type=int, default=20000)
    parser.add_argument("--model", default=os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.1-flash-lite")
    parser.add_argument("--default-env-var-name", default=os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "artifacts" / "codex" / "region-talk-finalizer")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reverify-existing", action="store_true", help="Ignore existing publication_candidate_item verifier statuses and call Gemini again.")
    args = parser.parse_args()
    load_env(args.env_file)
    os.environ.setdefault("REGION_TALK_LLM_MODEL", args.model)
    os.environ.setdefault("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME", args.default_env_var_name)
    os.environ.setdefault("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "45")
    os.environ.setdefault("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", os.environ.get("REGION_TALK_LLM_CALL_TIMEOUT_SECONDS", "45"))
    os.environ.setdefault("REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS", "2200")
    run_id = args.run_id or "region-talk-finalizer-local-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.output_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    ydb, driver, pool, table, rows = read_live_rows(args.limit_images, args.limit_memory, reverify_existing=args.reverify_existing)
    verified = [] if args.dry_run else verify_rows(rows, max_llm=args.max_llm, model=args.model, default_env_var_name=args.default_env_var_name)
    written = 0 if args.dry_run else write_publication_rows(pool, ydb, table, verified, run_id)
    shortlist_artifact = write_xlsx(out_dir / "region-talk-publication-shortlist.xlsx", verified, rows, include_unverified=50)
    payload = {
        "run_id": run_id,
        "actual_scored_rows": len(rows),
        "llm_calls": len([row for row in verified if row.get("llm_attempted_this_run") == "true"]),
        "accepted_new": sum(1 for row in verified if row.get("publication_status") == "gemini_accept"),
        "accepted_total": sum(1 for row in rows if row.get("publication_status") == "gemini_accept" or row.get("publication_candidate_status") == "llm_confirmed"),
        "written": written,
        "shortlist_artifact": str(shortlist_artifact),
        "xlsx": str(shortlist_artifact) if shortlist_artifact.suffix == ".xlsx" else "",
        "verified": verified,
        "top_actual": rows[:50],
    }
    (out_dir / "publication_finalizer_results.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: payload[k] for k in ["run_id", "actual_scored_rows", "llm_calls", "accepted_new", "accepted_total", "written", "shortlist_artifact"]}, ensure_ascii=False, indent=2))
    try:
        driver.stop(timeout=5)
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
