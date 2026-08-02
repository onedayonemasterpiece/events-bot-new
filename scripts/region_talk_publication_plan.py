#!/usr/bin/env python3
"""Build the durable Region Talk daily article/social publication plan.

The planner never calls Telegram or VK.  It reads Gemini-confirmed candidates,
actual target-publication history and compatible BGE vectors from YDB, then
recalculates every future unlocked slot.  Published/locked slots are immutable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_goal_notify import (  # noqa: E402
    attach_live_source_fingerprints,
    attach_latest_bge_vectors,
    getenv_bool,
    is_confirmed_publication,
    is_publication_draft_ready as publication_draft_ready,
    load_env,
    publication_operator_review_fingerprint,
    read_kind_rows,
    read_publication_rows,
)
from scripts.region_talk_review_queue import (  # noqa: E402
    DAILY_PLAN_POLICY_VERSION,
    build_daily_publication_plan,
    canonical_url,
    content_lane,
)


DEFAULT_TIMEZONE = "Europe/Kaliningrad"
DEFAULT_ARTICLE_TIME = "12:00"
DEFAULT_SOCIAL_TIME = "18:00"
EVIDENCE_PROJECTION_DRAFT_VERSION = "region_talk_external_evidence_projection_v1"


def _read_current_kind_rows_complete(
    pool: Any,
    ydb: Any,
    table: str,
    kind: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Read a current YDB kind with a limit+1 completeness proof."""

    max_rows = max(1, int(limit))
    # Unit callers use simple sentinels; production YDB exposes SnapshotReadOnly.
    if not hasattr(ydb, "SnapshotReadOnly"):
        rows = read_kind_rows(pool, ydb, table, kind, max_rows + 1)
        if len(rows) > max_rows:
            raise RuntimeError(f"publication plan current read truncated: {kind}")
        return rows
    if not re.fullmatch(r"[A-Za-z0-9_:-]+", kind):
        raise ValueError(f"unsafe YDB kind: {kind!r}")
    prefix = kind + ":"
    prefix_upper = kind + ";"
    after = prefix
    out: list[dict[str, Any]] = []
    page_size = min(500, max_rows + 1)
    def op(session: Any) -> list[dict[str, Any]]:
        local_after = after
        tx = session.transaction(ydb.SnapshotReadOnly())
        try:
            while len(out) < max_rows + 1:
                query_text = (
                    "DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; DECLARE $after AS Utf8; "
                    f"SELECT pk, payload_json, updated_at FROM `{table}` "
                    "WHERE pk >= $prefix AND pk < $prefix_upper AND pk > $after "
                    f"ORDER BY pk LIMIT {min(page_size, max_rows + 1 - len(out))};"
                )
                query = session.prepare(query_text)
                result_sets = tx.execute(
                    query,
                    {"$prefix": prefix, "$prefix_upper": prefix_upper, "$after": local_after},
                    commit_tx=False,
                )
                batch = result_sets[0].rows if result_sets else []
                if not batch:
                    break
                for raw in batch:
                    local_after = str(raw.pk)
                    payload = raw.payload_json
                    row = json.loads(payload) if isinstance(payload, str) else dict(payload or {})
                    if isinstance(row, dict):
                        row.setdefault("_ydb_pk", local_after)
                        row.setdefault("_ydb_updated_at", str(getattr(raw, "updated_at", "") or ""))
                        out.append(row)
                if len(batch) < page_size:
                    break
            tx.commit()
            return out
        except Exception:
            try:
                tx.rollback()
            except Exception:
                pass
            raise

    pool.retry_operation_sync(op)
    if len(out) > max_rows:
        raise RuntimeError(f"publication plan current read truncated: {kind}")
    return out


def external_intake_fingerprint(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    payload = {
        key: row.get(key)
        for key in (
            "external_publication_id", "canonical_url", "request_id", "research_request_id",
            "input_json_sha256", "raw_input_json_sha256", "canonical_evidence_urls",
            "intake_at", "intake_received_at", "imported_at", "normalized_title", "authors", "identity_keys", "intake_status",
            "review_status", "publication_permission", "intake_revision",
            "revision", "decision", "policy_classification",
            "operator_policy_override", "source_assessment", "publication",
            "region_relevance", "quality_assessment", "media_and_rights",
            "editorial_pack", "evidence", "provenance",
        )
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def current_external_intake_allows_published_candidate(
    publication: dict[str, Any],
    intake: dict[str, Any] | None,
) -> bool:
    """Fence planning on the exact clean intake used by the normal LLM funnel."""

    if not isinstance(intake, dict):
        return False
    decision = intake.get("decision") if isinstance(intake.get("decision"), dict) else {}
    policy = intake.get("policy_classification") if isinstance(intake.get("policy_classification"), dict) else {}
    statuses = {
        str(intake.get("intake_status") or "").lower(),
        str(intake.get("review_status") or "").lower(),
        str(decision.get("import_status") or "").lower(),
    }
    if statuses & {"manual_review_required", "needs_manual_review", "needs_review", "blocked", "rejected"}:
        return False
    return bool(
        decision.get("import_status") == "ready_for_region_talk_scoring"
        and decision.get("downstream_readiness") == "candidate_report"
        and policy.get("product_policy_match") is True
        and not policy.get("hard_exclusion_codes")
        and str(publication.get("external_intake_fingerprint") or "")
        == external_intake_fingerprint(intake)
    )


def _plan_decision_snapshot_fingerprint(
    publications: list[dict[str, Any]],
    schedule: list[dict[str, Any]],
    intakes: list[dict[str, Any]],
    sources: list[dict[str, Any]] | None = None,
) -> str:
    def compact(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            [
                {key: value for key, value in row.items() if not str(key).startswith("_ydb_")}
                for row in rows
            ],
            key=lambda row: str(row.get("_ydb_pk") or row.get("post_url") or row.get("external_publication_id") or ""),
        )
    raw = json.dumps(
        {
            "publications": compact(publications),
            "schedule": compact(schedule),
            "intakes": compact(intakes),
            "sources": compact(sources or []),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def operator_review_approved_clean(row: dict[str, Any]) -> bool:
    """Require approval for the exact current copy/media review revision."""

    return bool(
        str(row.get("operator_review_fingerprint") or "")
        == publication_operator_review_fingerprint(row)
        and str(row.get("operator_review_decision") or "") == "approved"
        and str(row.get("operator_review_rewrite_status") or "") == "clean"
    )


def evidence_projected_article_draft(
    publication_row: dict[str, Any],
    intake_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Project already evidence-backed research copy into a legacy article draft.

    This is deliberately not a semantic writer.  The imported ``editorial_pack``
    was produced under the strict external-research contract, every used copy
    surface must cite public evidence, and the publication row must already
    carry the terminal Gemini acceptance.  The projection only formats those
    supported sentences for Telegram/VK; it never invents or paraphrases a
    claim and it is not available to social posts.
    """

    if publication_draft_ready(publication_row) or not isinstance(intake_row, dict):
        return None
    if content_lane(publication_row) != "article":
        return None
    if str(publication_row.get("llm_gate_status") or "").lower() != "ok":
        return None
    if str(publication_row.get("llm_decision") or "").lower() != "accept":
        return None

    decision = intake_row.get("decision") if isinstance(intake_row.get("decision"), dict) else {}
    policy = (
        intake_row.get("policy_classification")
        if isinstance(intake_row.get("policy_classification"), dict)
        else {}
    )
    source = (
        intake_row.get("source_assessment")
        if isinstance(intake_row.get("source_assessment"), dict)
        else {}
    )
    if (
        decision.get("import_status") != "ready_for_region_talk_scoring"
        or decision.get("downstream_readiness") != "candidate_report"
        or policy.get("product_policy_match") is not True
        or policy.get("hard_exclusion_codes")
        or source.get("scope") != "external"
    ):
        return None

    publication = intake_row.get("publication") if isinstance(intake_row.get("publication"), dict) else {}
    editorial = intake_row.get("editorial_pack") if isinstance(intake_row.get("editorial_pack"), dict) else {}
    evidence_rows = [
        item for item in (intake_row.get("evidence") or [])
        if isinstance(item, dict) and str(item.get("evidence_id") or "").strip()
    ]
    evidence_by_id = {str(item["evidence_id"]): item for item in evidence_rows}
    support_rows = [
        item for item in (editorial.get("copy_support") or [])
        if isinstance(item, dict)
    ]
    support_by_surface: dict[str, list[str]] = {}
    for item in support_rows:
        surface = str(item.get("surface") or "").strip()
        refs = [str(ref).strip() for ref in (item.get("evidence_refs") or []) if str(ref).strip()]
        if surface and refs and set(refs).issubset(evidence_by_id):
            support_by_surface[surface] = refs

    title = str(editorial.get("title_short") or publication.get("title") or "").strip()
    teaser = str(editorial.get("teaser") or "").strip()
    takeaway = str(editorial.get("reader_takeaway") or "").strip()
    caveat = str(editorial.get("caveat") or "").strip()
    required_surfaces = {"teaser": teaser, "reader_takeaway": takeaway}
    if not title or not all(required_surfaces.values()):
        return None
    if any(surface not in support_by_surface for surface in required_surfaces):
        return None
    if caveat and "caveat" not in support_by_surface:
        return None

    source_name = str(publication.get("source_name") or publication_row.get("source_title") or "").strip()
    authors = [str(value).strip() for value in (publication.get("authors") or []) if str(value).strip()]
    attribution = source_name + ((" · " + ", ".join(authors)) if authors else "")
    url = canonical_url(publication_row) or canonical_url(intake_row)
    if not source_name or not url:
        return None

    body_parts = [title, teaser, takeaway]
    if caveat:
        body_parts.append("Важно: " + caveat)
    body_parts.extend(["Источник: " + attribution, "Оригинал: " + url])
    telegram_text = "\n\n".join(body_parts)
    vk_text = telegram_text

    fact_points = []
    for surface, claim in [("teaser", teaser), ("reader_takeaway", takeaway), ("caveat", caveat)]:
        if not claim:
            continue
        refs = support_by_surface.get(surface) or []
        excerpts = [
            str(evidence_by_id[ref].get("paraphrase") or evidence_by_id[ref].get("quote_short") or "").strip()
            for ref in refs
        ]
        excerpts = [value for value in excerpts if value]
        if not excerpts:
            return None
        fact_points.append({
            "claim": claim,
            "support_excerpt": " ".join(excerpts)[:1200],
            "evidence_ids": refs,
        })
    if not (1 <= len(fact_points) <= 3):
        return None

    return {
        "publication_draft_status": "ready_for_operator_review",
        "publication_draft_title": title,
        "publication_draft_source_attribution": attribution,
        "publication_draft_telegram_text": telegram_text,
        "publication_draft_vk_text": vk_text,
        "publication_draft_fact_points_json": json.dumps(
            fact_points, ensure_ascii=False, separators=(",", ":")
        ),
        "publication_draft_prompt_version": EVIDENCE_PROJECTION_DRAFT_VERSION,
        "publication_draft_generation_mode": "evidence_copy_projection",
    }


def _canonical_candidate_id(row: dict[str, Any]) -> str:
    return str(
        row.get("publication_candidate_id")
        or row.get("external_publication_id")
        or row.get("candidate_id")
        or ""
    ).strip()


def _published_status(row: dict[str, Any]) -> bool:
    values = {
        str(row.get("status") or "").lower(),
        str(row.get("plan_status") or "").lower(),
        str(row.get("target_publication_status") or "").lower(),
        str(row.get("public_publication_status") or "").lower(),
    }
    return bool(values & {"published", "target_published", "completed"})


def _published_identity(
    rows: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    urls: set[str] = set()
    ids: set[str] = set()
    for row in rows:
        if not _published_status(row):
            continue
        url = canonical_url(row)
        if url:
            urls.add(url)
        candidate_id = _canonical_candidate_id(row)
        if candidate_id:
            ids.add(candidate_id)
    return urls, ids


def _matching_publication(
    row: dict[str, Any],
    publications_by_url: dict[str, dict[str, Any]],
    publications_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return dict(
        publications_by_url.get(canonical_url(row))
        or publications_by_id.get(_canonical_candidate_id(row))
        or row
    )


def _parse_clock(value: str) -> time:
    try:
        hour, minute = [int(part) for part in value.strip().split(":", 1)]
        return time(hour=hour, minute=minute)
    except (TypeError, ValueError):
        raise ValueError(f"invalid HH:MM time: {value!r}")


def _scheduled_for(day: str, lane: str, tz: ZoneInfo, article_time: time, social_time: time) -> str:
    clock = article_time if lane == "article" else social_time
    return datetime.combine(date.fromisoformat(day), clock, tzinfo=tz).isoformat()


def _compact_plan_row(
    row: dict[str, Any],
    *,
    snapshot_id: str,
    generated_at: str,
    tz: ZoneInfo,
    article_time: time,
    social_time: time,
) -> dict[str, Any]:
    lane = str(row.get("content_lane") or "")
    day = str(row.get("plan_date") or "")
    url = canonical_url(row)
    status = str(row.get("plan_status") or row.get("status") or "planned")
    keep = {
        "plan_slot_id": f"{day}:{lane}",
        "plan_date": day,
        "content_lane": lane,
        "plan_status": status,
        "scheduled_for": _scheduled_for(day, lane, tz, article_time, social_time),
        "target_platforms": ["telegram", "vk"],
        "publication_candidate_id": _canonical_candidate_id(row),
        "candidate_url": url,
        "post_url": url,
        "source_title": row.get("source_title") or row.get("publication_source_name") or "",
        "publication_title": row.get("publication_draft_title") or row.get("publication_title") or row.get("title") or row.get("short_summary") or "",
        "publication_draft_status": row.get("publication_draft_status") or "",
        "publication_draft_prompt_version": row.get("publication_draft_prompt_version") or "",
        "content_origin_type": row.get("content_origin_type") or "",
        "external_publication_id": row.get("external_publication_id") or "",
        "external_intake_fingerprint": row.get("external_intake_fingerprint") or "",
        "external_intake_revision": row.get("external_intake_revision") or "",
        "plan_reevaluation_required": bool(row.get("plan_reevaluation_required")),
        "plan_reevaluation_reason": row.get("plan_reevaluation_reason") or "",
        "prepared_identity_frozen": bool(row.get("prepared_identity_frozen")),
        "quality_score": row.get("quality_score"),
        "rank_score": row.get("rank_score"),
        "max_similarity_to_selected_or_history": row.get("max_similarity_to_selected_or_history"),
        "nearest_url": row.get("nearest_url") or "",
        "diversity_mode": row.get("diversity_mode") or "",
        "pair_similarity": row.get("pair_similarity"),
        "pair_similarity_mode": row.get("pair_similarity_mode") or "",
        "pair_similarity_threshold": row.get("pair_similarity_threshold"),
        "pair_diversity_relaxed": bool(row.get("pair_diversity_relaxed")),
        "vacancy_reason": row.get("vacancy_reason") or "",
        "slot_locked": bool(row.get("slot_locked")),
        "queue_policy_version": DAILY_PLAN_POLICY_VERSION,
        "snapshot_id": snapshot_id,
        "generated_at": generated_at,
        "updated_at": generated_at,
    }
    return {key: value for key, value in keep.items() if value not in (None, "", []) or key in {
        "candidate_url", "post_url", "publication_candidate_id", "external_publication_id",
        "vacancy_reason", "source_title", "publication_title", "content_origin_type",
        "publication_draft_status", "publication_draft_prompt_version",
        "external_intake_fingerprint", "external_intake_revision", "plan_reevaluation_reason",
    }}


def _snapshot_id(generated_at: str, rows: list[dict[str, Any]]) -> str:
    identity = [
        (row.get("plan_date"), row.get("content_lane"), canonical_url(row), row.get("plan_status"))
        for row in rows
    ]
    raw = json.dumps([generated_at, identity], ensure_ascii=False, separators=(",", ":"))
    return "rtdayplan_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _upsert_rows(pool: Any, ydb: Any, table: str, rows: list[tuple[str, str, dict[str, Any]]]) -> int:
    query_text = f"""
DECLARE $pk AS Utf8;
DECLARE $kind AS Utf8;
DECLARE $payload_json AS Json;
DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    if not rows:
        return 0

    def write_all(session: Any) -> None:
        query = session.prepare(query_text)
        tx = session.transaction(ydb.SerializableReadWrite())
        for index, (pk, kind, payload) in enumerate(rows):
            updated_at = str(payload.get("updated_at") or datetime.now(timezone.utc).isoformat())
            tx.execute(
                query,
                {
                    "$pk": pk,
                    "$kind": kind,
                    "$payload_json": json.dumps(payload, ensure_ascii=False),
                    "$updated_at": updated_at,
                },
                commit_tx=index == len(rows) - 1,
            )

    pool.retry_operation_sync(write_all)
    return len(rows)


def build_plan(args: argparse.Namespace) -> dict[str, Any]:
    ydb, driver, pool, table, publications = read_publication_rows(max(args.scan_limit, 5000))
    try:
        # `read_publication_rows` opens the configured driver, but its notifier
        # compatibility read is stale.  Replace every planning input with a
        # current, complete SnapshotReadOnly refresh.
        if hasattr(ydb, "SnapshotReadOnly"):
            publications = _read_current_kind_rows_complete(
                pool, ydb, table, "publication_candidate_item", max(args.scan_limit, 5000)
            )
        vectors = _read_current_kind_rows_complete(pool, ydb, table, "text_vector_enrichment_item", args.vector_scan_limit)
        schedule = _read_current_kind_rows_complete(pool, ydb, table, "publication_schedule_item", args.history_limit)
        semantic_history = _read_current_kind_rows_complete(pool, ydb, table, "publication_semantic_history_item", args.history_limit)
        external_intakes = _read_current_kind_rows_complete(
            pool, ydb, table, "external_publication_intake_item", args.history_limit
        )
        source_limit = max(5000, int(os.getenv("REGION_TALK_NOTIFY_SOURCE_SCAN_LIMIT") or "20000"))
        current_sources: list[dict[str, Any]] = []
        for source_kind in (
            "source_queue_item",
            "source_status_item",
            "online_source_item",
            "external_publication_source_item",
        ):
            current_sources.extend(
                _read_current_kind_rows_complete(pool, ydb, table, source_kind, source_limit)
            )
        # Replacing the notifier's stale publication read must not discard its
        # live authoritative-source join: `is_confirmed_publication()` requires
        # the current source fingerprint on every candidate.
        attach_live_source_fingerprints(publications, current_sources)
        logs = _read_current_kind_rows_complete(pool, ydb, table, "publication_log_item", args.history_limit)
        logs += _read_current_kind_rows_complete(pool, ydb, table, "region_talk_publication_log", args.history_limit)
        decision_snapshot_fingerprint = _plan_decision_snapshot_fingerprint(
            publications, schedule, external_intakes, current_sources
        )
        attach_latest_bge_vectors(publications, vectors)
        attach_latest_bge_vectors(schedule, vectors)
        attach_latest_bge_vectors(semantic_history, vectors)

        by_url = {canonical_url(row): row for row in publications if canonical_url(row)}
        by_id = {_canonical_candidate_id(row): row for row in publications if _canonical_candidate_id(row)}
        published_urls, published_ids = _published_identity(schedule + logs + publications)

        history: list[dict[str, Any]] = []
        history_seen: set[str] = set()
        for row in schedule + logs + publications:
            if not _published_status(row):
                continue
            item = _matching_publication(row, by_url, by_id)
            url = canonical_url(item)
            if url and url not in history_seen:
                history_seen.add(url)
                history.append(item)
        for row in semantic_history:
            if str(row.get("history_status") or row.get("target_history_status") or "").lower() not in {
                "published", "target_published"
            }:
                continue
            url = canonical_url(row)
            if url and url not in history_seen:
                history_seen.add(url)
                history.append(row)
        attach_latest_bge_vectors(history, vectors)

        intake_by_id = {
            str(row.get("external_publication_id") or ""): row
            for row in external_intakes
            if str(row.get("external_publication_id") or "")
        }
        eligible: list[dict[str, Any]] = []
        confirmed_by_lane = {"article": 0, "social": 0}
        missing_draft_by_lane = {"article": 0, "social": 0}
        projected_draft_rows: list[dict[str, Any]] = []
        reaction_gate_enabled = getenv_bool("REGION_TALK_REACTION_GATE_ENABLED", False)
        reaction_gate_blocked_by_lane = {"article": 0, "social": 0}
        for row in publications:
            if not is_confirmed_publication(row):
                continue
            row = dict(row)
            lane = content_lane(row)
            confirmed_by_lane[lane] += 1
            external_id = str(row.get("external_publication_id") or "")
            if external_id and not current_external_intake_allows_published_candidate(
                row,
                intake_by_id.get(external_id),
            ):
                # A normal Gemini acceptance remains durable evidence, but a
                # missing/changed/manual intake cannot advance into a release.
                continue
            # Article intake remains evidence for the staged v8 writer.  The
            # old deterministic teaser/takeaway projection is intentionally
            # no longer promoted to review-ready public copy.
            if not publication_draft_ready(row):
                missing_draft_by_lane[lane] += 1
                continue
            if reaction_gate_enabled and not operator_review_approved_clean(row):
                reaction_gate_blocked_by_lane[lane] += 1
                continue
            url = canonical_url(row)
            candidate_id = _canonical_candidate_id(row)
            if (url and url in published_urls) or (candidate_id and candidate_id in published_ids):
                continue
            eligible.append(row)

        tz = ZoneInfo(args.timezone)
        local_now = datetime.now(tz)
        article_clock = _parse_clock(args.article_time)
        social_clock = _parse_clock(args.social_time)
        if args.start_date:
            start = date.fromisoformat(args.start_date)
        else:
            start = local_now.date()
            today = start.isoformat()
            today_has_plan = any(str(row.get("plan_date") or "") == today for row in schedule)
            # A first-ever late-day plan must not create already-expired slots.
            # Normal afternoon/evening recalculations retain the morning plan
            # below and may still replace only slots whose time has not passed.
            if not today_has_plan and local_now.time() >= min(article_clock, social_clock):
                start += timedelta(days=1)
        final_day = start.fromordinal(start.toordinal() + max(0, args.days - 1)).isoformat()
        locked_slots: dict[tuple[str, str], dict[str, Any]] = {}
        for row in schedule:
            day = str(row.get("plan_date") or "")
            lane = str(row.get("content_lane") or "")
            status = str(row.get("plan_status") or row.get("status") or "").lower()
            scheduled_raw = str(row.get("scheduled_for") or "")
            try:
                scheduled_at = datetime.fromisoformat(scheduled_raw)
                if scheduled_at.tzinfo is None:
                    scheduled_at = scheduled_at.replace(tzinfo=tz)
            except (TypeError, ValueError):
                scheduled_at = None
            elapsed_planned = status == "planned" and scheduled_at is not None and scheduled_at <= local_now
            matched = _matching_publication(row, by_url, by_id)
            candidate_id = _canonical_candidate_id(row)
            candidate_url = canonical_url(row)
            prepared = bool(
                status in {"prepared", "reviewed", "approved", "locked"}
                or row.get("prepared_identity_frozen")
                or publication_draft_ready(matched)
            )
            prepared_changed = False
            reevaluation_reason = ""
            if prepared:
                if not candidate_id and not candidate_url:
                    prepared_changed = True
                    reevaluation_reason = "prepared_slot_missing_candidate_identity"
                elif canonical_url(matched) != candidate_url and _canonical_candidate_id(matched) != candidate_id:
                    prepared_changed = True
                    reevaluation_reason = "prepared_candidate_missing_from_current_ledger"
                elif row.get("external_publication_id") or matched.get("external_publication_id"):
                    external_id = str(
                        row.get("external_publication_id")
                        or matched.get("external_publication_id")
                        or ""
                    )
                    intake = intake_by_id.get(external_id)
                    captured = str(
                        row.get("external_intake_fingerprint")
                        or matched.get("external_intake_fingerprint")
                        or ""
                    )
                    if not captured or captured != external_intake_fingerprint(intake):
                        prepared_changed = True
                        reevaluation_reason = "prepared_external_intake_revision_changed_or_missing"
                if (
                    str(matched.get("operator_review_decision") or "")
                    and not operator_review_approved_clean(matched)
                ):
                    prepared_changed = True
                    reevaluation_reason = "prepared_operator_review_revision_changed"
            if (
                start.isoformat() <= day <= final_day
                and lane in {"article", "social"}
                and (status in {"locked", "published"} or elapsed_planned or prepared)
            ):
                locked_slots[(day, lane)] = matched | {
                    "plan_status": (
                        "manual_review_required"
                        if prepared_changed
                        else "locked" if elapsed_planned else status
                    ),
                    "content_lane": lane,
                    "prepared_identity_frozen": True,
                    "plan_reevaluation_required": prepared_changed,
                    "plan_reevaluation_reason": reevaluation_reason,
                }

        planned = build_daily_publication_plan(
            eligible,
            history=history,
            start_date=start,
            days=args.days,
            diversity_weight=args.diversity_weight,
            pair_similarity_threshold=args.pair_similarity_threshold,
            locked_slots=locked_slots,
        )
        generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        snapshot_id = _snapshot_id(generated_at, planned)
        compact_rows = [
            _compact_plan_row(
                row,
                snapshot_id=snapshot_id,
                generated_at=generated_at,
                tz=tz,
                article_time=article_clock,
                social_time=social_clock,
            )
            for row in planned
        ]

        counts = {
            "confirmed_article": confirmed_by_lane["article"],
            "confirmed_social": confirmed_by_lane["social"],
            "draft_ready_article": confirmed_by_lane["article"] - missing_draft_by_lane["article"],
            "draft_ready_social": confirmed_by_lane["social"] - missing_draft_by_lane["social"],
            "draft_missing_article": missing_draft_by_lane["article"],
            "draft_missing_social": missing_draft_by_lane["social"],
            "draft_projected_article": len(projected_draft_rows),
            "reaction_gate_enabled": reaction_gate_enabled,
            "reaction_gate_blocked_article": reaction_gate_blocked_by_lane["article"],
            "reaction_gate_blocked_social": reaction_gate_blocked_by_lane["social"],
            "eligible_article": sum(content_lane(row) == "article" for row in eligible),
            "eligible_social": sum(content_lane(row) == "social" for row in eligible),
            "published_history_article": sum(content_lane(row) == "article" for row in history),
            "published_history_social": sum(content_lane(row) == "social" for row in history),
            "planned_article": sum(row.get("content_lane") == "article" and row.get("plan_status") in {"planned", "locked", "published"} for row in compact_rows),
            "planned_social": sum(row.get("content_lane") == "social" and row.get("plan_status") in {"planned", "locked", "published"} for row in compact_rows),
            "vacant_article": sum(row.get("content_lane") == "article" and row.get("plan_status") == "vacant" for row in compact_rows),
            "vacant_social": sum(row.get("content_lane") == "social" and row.get("plan_status") == "vacant" for row in compact_rows),
        }
        snapshot = {
            "snapshot_id": snapshot_id,
            "queue_policy_version": DAILY_PLAN_POLICY_VERSION,
            "generated_at": generated_at,
            "timezone": args.timezone,
            "start_date": start.isoformat(),
            "days": args.days,
            "article_time": args.article_time,
            "social_time": args.social_time,
            "target_platforms": ["telegram", "vk"],
            "counts": counts,
            "rows": compact_rows,
            "updated_at": generated_at,
        }
        writes: list[tuple[str, str, dict[str, Any]]] = []
        for row in projected_draft_rows:
            pk = str(row.get("_ydb_pk") or "").strip()
            if not pk:
                pk = "publication_candidate_item:" + canonical_url(row)
            payload = {key: value for key, value in row.items() if not key.startswith("_")}
            payload["updated_at"] = generated_at
            writes.append((pk, "publication_candidate_item", payload))
        for row in compact_rows:
            if row.get("slot_locked") and not row.get("plan_reevaluation_required"):
                continue
            writes.append((
                "publication_schedule_item:" + str(row["plan_slot_id"]),
                "publication_schedule_item",
                row,
            ))
        writes.append(("publication_schedule_snapshot:latest", "publication_schedule_snapshot", snapshot))

        # Reconcile the semantic history only from actual target publication
        # evidence.  Operator-chat delivery is intentionally not treated as a
        # public publication.
        for row in history:
            url = canonical_url(row)
            if not url:
                continue
            candidate_id = _canonical_candidate_id(row) or hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
            payload = {
                "publication_candidate_id": candidate_id,
                "post_url": url,
                "canonical_url": url,
                "content_lane": content_lane(row),
                "content_origin_type": row.get("content_origin_type") or "",
                "history_status": "target_published",
                "published_at": row.get("published_at") or row.get("target_published_at") or "",
                "model_id": row.get("model_id") or "",
                "encoder_contract": row.get("encoder_contract") or "",
                "embedding_dim": row.get("embedding_dim") or 0,
                "embedding_vector_encoding": row.get("embedding_vector_encoding") or "",
                "embedding_vector_f16_b64": row.get("embedding_vector_f16_b64") or "",
                "updated_at": generated_at,
            }
            writes.append((
                "publication_semantic_history_item:" + candidate_id,
                "publication_semantic_history_item",
                payload,
            ))

        if args.execute:
            # Final revision fence: do not write a plan built from inputs that
            # changed during ranking/copy preparation.  The next scheduled
            # cycle recomputes from the new current snapshot.
            current_publications = (
                _read_current_kind_rows_complete(
                    pool, ydb, table, "publication_candidate_item", max(args.scan_limit, 5000)
                )
                if hasattr(ydb, "SnapshotReadOnly")
                else publications
            )
            current_schedule = _read_current_kind_rows_complete(
                pool, ydb, table, "publication_schedule_item", args.history_limit
            )
            current_intakes = _read_current_kind_rows_complete(
                pool, ydb, table, "external_publication_intake_item", args.history_limit
            )
            final_sources: list[dict[str, Any]] = []
            for source_kind in (
                "source_queue_item",
                "source_status_item",
                "online_source_item",
                "external_publication_source_item",
            ):
                final_sources.extend(
                    _read_current_kind_rows_complete(pool, ydb, table, source_kind, source_limit)
                )
            attach_live_source_fingerprints(current_publications, final_sources)
            final_fingerprint = _plan_decision_snapshot_fingerprint(
                current_publications, current_schedule, current_intakes, final_sources
            )
            if final_fingerprint != decision_snapshot_fingerprint:
                return {
                    "ok": False,
                    "stage": "publication_plan",
                    "status": "deferred_live_state_changed",
                    "executed": False,
                    "planned_ydb_rows": len(writes),
                    "written_ydb_rows": 0,
                    "next_action": "recompute_from_current_ydb_on_next_cycle",
                    "counts": counts,
                    "rows": compact_rows,
                }
        written = _upsert_rows(pool, ydb, table, writes) if args.execute else 0
        result = {
            "ok": True,
            "stage": "publication_plan",
            "executed": bool(args.execute),
            "snapshot_id": snapshot_id,
            "queue_policy_version": DAILY_PLAN_POLICY_VERSION,
            "counts": counts,
            "planned_ydb_rows": len(writes),
            "written_ydb_rows": written,
            "rows": compact_rows,
        }
        return result
    finally:
        driver.stop(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Region Talk 1 article + 1 social post daily plan")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--days", type=int, default=int(os.getenv("REGION_TALK_PUBLICATION_PLAN_DAYS") or "14"))
    parser.add_argument("--timezone", default=os.getenv("REGION_TALK_PUBLICATION_TIMEZONE") or DEFAULT_TIMEZONE)
    parser.add_argument("--article-time", default=os.getenv("REGION_TALK_ARTICLE_TIME_LOCAL") or DEFAULT_ARTICLE_TIME)
    parser.add_argument("--social-time", default=os.getenv("REGION_TALK_SOCIAL_TIME_LOCAL") or DEFAULT_SOCIAL_TIME)
    parser.add_argument("--diversity-weight", type=float, default=float(os.getenv("REGION_TALK_PUBLICATION_DIVERSITY_WEIGHT") or "0.35"))
    parser.add_argument("--pair-similarity-threshold", type=float, default=float(os.getenv("REGION_TALK_PUBLICATION_PAIR_SIMILARITY_THRESHOLD") or "0.82"))
    parser.add_argument("--scan-limit", type=int, default=5000)
    parser.add_argument("--vector-scan-limit", type=int, default=20000)
    parser.add_argument("--history-limit", type=int, default=5000)
    args = parser.parse_args()
    load_env(args.env_file)
    try:
        result = build_plan(args)
    except Exception as exc:
        result = {"ok": False, "stage": "publication_plan", "error": f"{type(exc).__name__}: {str(exc)[:800]}"}
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
