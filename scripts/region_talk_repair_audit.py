#!/usr/bin/env python3
"""Read-only product audit for the Region Talk source-profile repair backlog.

This script never writes YDB, never calls an LLM and never touches Telegram.
It produces a sanitized inventory of unpublished accepted candidates and proves
whether the source-profile Writer repair reached a current operator revision.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(ROOT))
if str(ROOT / "kaggle" / "RegionTalkCandidateReport") not in os.sys.path:
    os.sys.path.insert(0, str(ROOT / "kaggle" / "RegionTalkCandidateReport"))

from scripts import region_talk_goal_notify as notify  # noqa: E402
from scripts import region_talk_publication_draft_backfill as backfill  # noqa: E402
from scripts import region_talk_publication_finalizer as finalizer  # noqa: E402


ACCEPTED_STATUSES = {"llm_confirmed", "sent_to_chat", "accepted_for_publication"}
PUBLICATION_KINDS = (
    "publication_candidate_item",
    "publication_delivery_item",
    "publication_schedule_item",
    "publication_log_item",
    "region_talk_publication_log",
)
SOURCE_KINDS = (
    "source_queue_item",
    "source_status_item",
    "online_source_item",
    "external_publication_source_item",
)
PROFILE_KINDS = (
    "source_onboarding_profile_item",
    "source_profile_capture_item",
    "publisher_profile_item",
    "publisher_profile_candidate_correction_item",
    "external_publication_intake_item",
)


def _s(value: Any) -> str:
    return str(value or "").strip()


def _bool(value: Any) -> bool:
    return value is True or _s(value).lower() in {"1", "true", "yes"}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _timestamp(row: dict[str, Any]) -> str:
    values = [
        _s(row.get(key))
        for key in (
            "sent_at",
            "operator_review_observed_at",
            "publication_draft_backfill_updated_at",
            "publication_draft_updated_at",
            "updated_at",
            "created_at",
        )
        if _s(row.get(key))
    ]
    return max(values) if values else ""


def _accepted_marker(row: dict[str, Any]) -> bool:
    if _bool(row.get("publication_tombstone")) or _bool(row.get("publication_revoked")):
        return False
    return (
        _s(row.get("publication_candidate_status")) in ACCEPTED_STATUSES
        or _s(row.get("publication_status")) == "gemini_accept"
    )


def _published(rows: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    urls: set[str] = set()
    candidate_ids: set[str] = set()
    for row in rows:
        if not backfill.has_published_status(row):
            continue
        url = notify.canonical_post_url(row)
        candidate_id = backfill.publication_candidate_id(row)
        if url:
            urls.add(url)
        if candidate_id:
            candidate_ids.add(candidate_id)
    return urls, candidate_ids


def _delivery_index(rows: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    by_url: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        url = notify.canonical_post_url(row)
        candidate_id = _s(row.get("publication_candidate_id"))
        if url:
            by_url[url].append(row)
        if candidate_id:
            by_candidate[candidate_id].append(row)
    for index in (by_url, by_candidate):
        for key in list(index):
            index[key].sort(key=_timestamp, reverse=True)
    return dict(by_url), dict(by_candidate)


def _latest_delivery(
    row: dict[str, Any],
    by_url: dict[str, list[dict[str, Any]]],
    by_candidate: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    url = notify.canonical_post_url(row)
    candidate_id = backfill.publication_candidate_id(row)
    candidates.extend(by_url.get(url, []))
    candidates.extend(by_candidate.get(candidate_id, []))
    if not candidates:
        return {}
    candidates.sort(key=_timestamp, reverse=True)
    return candidates[0]


def _source_name(row: dict[str, Any]) -> str:
    return _s(
        row.get("publication_draft_source_attribution")
        or row.get("source_title")
        or row.get("source_name")
        or row.get("channel_title")
        or row.get("publisher_name")
    )[:220]


def _profile_kind(profile: dict[str, Any], lane: str) -> str:
    explicit = _s(profile.get("profile_kind") or profile.get("source_profile_kind"))
    if explicit:
        return explicit
    return "publisher" if lane == "article" else "social"


def _profile_evidence_count(profile: dict[str, Any]) -> int:
    evidence = profile.get("evidence") or profile.get("evidence_json") or []
    if isinstance(evidence, str):
        try:
            evidence = json.loads(evidence)
        except (TypeError, ValueError, json.JSONDecodeError):
            evidence = []
    return len(evidence) if isinstance(evidence, list) else 0


def _capture_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        keys = {
            _s(row.get("canonical_source_key")).lower(),
            _s(row.get("source_key")).lower(),
        } - {""}
        for key in keys:
            current = index.get(key)
            if current is None or _timestamp(row) >= _timestamp(current):
                index[key] = row
    return index


def _reason_codes(
    *,
    strict_confirmed: bool,
    profile_exists: bool,
    profile_ready: bool,
    current_draft: bool,
    draft_ready: bool,
    intake_exists: bool,
    lane: str,
    correction_blocked: bool,
    sent_current_revision: bool,
    delivery_exists: bool,
) -> list[str]:
    reasons: list[str] = []
    if not strict_confirmed:
        reasons.append("strict_confirmation_or_source_fingerprint_stale")
    if correction_blocked:
        reasons.append("blocked_candidate_correction")
    if not profile_exists:
        reasons.append("missing_source_profile")
    elif not profile_ready:
        reasons.append("source_profile_not_writer_ready")
    if lane == "article" and not intake_exists:
        reasons.append("missing_retained_article_intake")
    if not draft_ready:
        reasons.append("draft_not_operator_ready")
    elif not current_draft:
        reasons.append("draft_stale_against_writer_or_profile")
    if current_draft and not sent_current_revision:
        reasons.append("current_revision_not_delivered_to_operator")
    elif not delivery_exists:
        reasons.append("no_operator_delivery_evidence")
    return reasons


def execute(scan_limit: int) -> dict[str, Any]:
    ydb = notify.ensure_ydb_module()
    endpoint, database = notify.ydb_endpoint_database()
    driver = ydb.Driver(
        endpoint=endpoint,
        database=database,
        credentials=notify.ydb_credentials(ydb),
    )
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = notify.ydb_table_path(database)
    try:
        rows_by_kind: dict[str, list[dict[str, Any]]] = {}
        for kind in (*PUBLICATION_KINDS, *SOURCE_KINDS, *PROFILE_KINDS):
            rows_by_kind[kind] = notify.read_kind_rows(
                pool, ydb, table, kind, int(scan_limit)
            )

        publications = rows_by_kind["publication_candidate_item"]
        source_rows = [row for kind in SOURCE_KINDS for row in rows_by_kind[kind]]
        onboarding_profiles = rows_by_kind["source_onboarding_profile_item"]
        publisher_profiles = rows_by_kind["publisher_profile_item"]
        profiles = [*onboarding_profiles, *publisher_profiles]
        captures = rows_by_kind["source_profile_capture_item"]
        corrections = rows_by_kind["publisher_profile_candidate_correction_item"]
        intakes = rows_by_kind["external_publication_intake_item"]
        deliveries = rows_by_kind["publication_delivery_item"]
        history_rows = [
            *publications,
            *rows_by_kind["publication_schedule_item"],
            *rows_by_kind["publication_log_item"],
            *rows_by_kind["region_talk_publication_log"],
        ]

        notify.attach_live_source_fingerprints(publications, source_rows)
        notify.attach_live_profile_and_corrections(publications, profiles, corrections)
        profiles_by_key = backfill.reusable_profile_index(profiles)
        captures_by_key = _capture_index(captures)
        corrections_by_url = backfill.correction_index(corrections)
        intakes_by_key = backfill.article_intake_index(intakes)
        delivery_by_url, delivery_by_candidate = _delivery_index(deliveries)
        published_urls, published_candidate_ids = _published(history_rows)

        candidates: list[dict[str, Any]] = []
        for row in publications:
            if not _accepted_marker(row):
                continue
            url = notify.canonical_post_url(row)
            candidate_id = backfill.publication_candidate_id(row)
            if backfill.has_published_status(row) or url in published_urls or candidate_id in published_candidate_ids:
                continue

            lane = backfill.content_lane(row)
            source_key = finalizer.canonical_source_key_for_row(row).strip().lower()
            profile = dict(profiles_by_key.get(source_key) or {})
            backfill.bind_source_profile(row, profile)
            capture = dict(captures_by_key.get(source_key) or {})
            intake = (
                intakes_by_key.get(_s(row.get("external_publication_id")))
                or intakes_by_key.get(url)
                or {}
            )
            row_corrections = corrections_by_url.get(url, [])
            correction_blocked = backfill.candidate_correction_requires_re_adjudication(
                row, row_corrections
            )
            strict_confirmed = notify.is_confirmed_publication(row)
            profile_exists = bool(profile)
            profile_ready = backfill.source_profile_ready(row)
            draft_ready = notify.is_publication_draft_ready(row)
            current_draft = backfill.current_editorial_draft(row)
            delivery = _latest_delivery(row, delivery_by_url, delivery_by_candidate)
            sent_message_id = max(
                _int(row.get("sent_message_id")),
                _int(delivery.get("message_id")),
                _int(delivery.get("sent_message_id")),
            )
            sent_review_fp = _s(
                row.get("sent_operator_review_fingerprint")
                or delivery.get("operator_review_fingerprint")
            )
            current_review_fp = ""
            if draft_ready:
                try:
                    current_review_fp = notify.publication_operator_review_fingerprint(row)
                except Exception:
                    current_review_fp = ""
            sent_current_revision = bool(
                sent_message_id > 0
                and current_review_fp
                and sent_review_fp == current_review_fp
            )
            dimensions = (
                backfill.normalized_publisher_dimensions(profile, fallback_row=row)
                if lane == "article"
                else {}
            )
            reasons = _reason_codes(
                strict_confirmed=strict_confirmed,
                profile_exists=profile_exists,
                profile_ready=profile_ready,
                current_draft=current_draft,
                draft_ready=draft_ready,
                intake_exists=bool(intake),
                lane=lane,
                correction_blocked=correction_blocked,
                sent_current_revision=sent_current_revision,
                delivery_exists=bool(delivery or sent_message_id),
            )
            status = (
                "operator_current"
                if not reasons
                else "blocked_correction"
                if correction_blocked
                else "needs_source_profile"
                if not profile_ready
                else "needs_writer_repair"
                if not current_draft
                else "needs_operator_delivery"
            )
            candidates.append({
                "candidate_id": candidate_id,
                "url": url,
                "lane": lane,
                "source_key": source_key,
                "source_name": _source_name(row),
                "candidate_status": _s(row.get("publication_candidate_status") or row.get("publication_status")),
                "strict_confirmed": strict_confirmed,
                "profile": {
                    "exists": profile_exists,
                    "ready": profile_ready,
                    "kind": _profile_kind(profile, lane),
                    "fingerprint": backfill.source_profile_fingerprint(row),
                    "reader_brief_chars": len(backfill.source_profile_reader_brief(row)),
                    "evidence_count": _profile_evidence_count(profile),
                    "capture_exists": bool(capture),
                    "capture_status": _s(capture.get("capture_status") or capture.get("status")),
                    "publisher_dimensions": sorted(dimensions),
                },
                "article_intake_exists": bool(intake),
                "correction_blocked": correction_blocked,
                "draft": {
                    "ready": draft_ready,
                    "current": current_draft,
                    "status": _s(row.get("publication_draft_status")),
                    "backfill_status": _s(row.get("publication_draft_backfill_status")),
                    "backfill_reason": _s(row.get("publication_draft_backfill_reason"))[:240],
                    "writer_version": _s(row.get("publication_draft_prompt_version")),
                    "contract_version": _s(row.get("publication_draft_contract_version")),
                    "stored_profile_fingerprint": _s(row.get("source_profile_fingerprint")),
                },
                "operator_delivery": {
                    "exists": bool(delivery or sent_message_id),
                    "message_id": sent_message_id,
                    "sent_at": _s(row.get("sent_at") or delivery.get("sent_at") or delivery.get("updated_at")),
                    "sent_current_revision": sent_current_revision,
                    "sent_writer_version": _s(row.get("sent_publication_draft_prompt_version")),
                    "review_decision": _s(row.get("operator_review_decision")),
                    "review_rewrite_status": _s(row.get("operator_review_rewrite_status")),
                },
                "repair_status": status,
                "repair_reasons": reasons,
                "updated_at": _timestamp(row),
            })

        candidates.sort(key=lambda item: (
            item["repair_status"] == "operator_current",
            item["lane"],
            item["source_key"],
            item["url"],
        ))
        status_counts = Counter(item["repair_status"] for item in candidates)
        reason_counts = Counter(reason for item in candidates for reason in item["repair_reasons"])
        lane_counts = Counter(item["lane"] for item in candidates)
        article_candidates = [item for item in candidates if item["lane"] == "article"]
        social_candidates = [item for item in candidates if item["lane"] == "social"]
        missing_article_sources = sorted({
            item["source_key"]
            for item in article_candidates
            if not item["profile"]["ready"] or not item["article_intake_exists"]
        })
        missing_social_sources = sorted({
            item["source_key"]
            for item in social_candidates
            if not item["profile"]["ready"]
        })
        imported_publisher_keys = sorted({
            key for key, profile in profiles_by_key.items()
            if _s(profile.get("profile_kind")) == "publisher"
            or bool(profile.get("publisher_profile_id"))
            or bool(profile.get("profile_dimensions"))
        })
        delivered_current = sum(
            1 for item in candidates if item["operator_delivery"]["sent_current_revision"]
        )
        current_drafts = sum(1 for item in candidates if item["draft"]["current"])
        profile_ready_total = sum(1 for item in candidates if item["profile"]["ready"])
        return {
            "schema_version": "region-talk-repair-audit-v1",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "read_only": True,
            "scan_limit_per_kind": int(scan_limit),
            "writer_expected": notify.EDITORIAL_WRITER_VERSION,
            "writer_contract_expected": notify.EDITORIAL_OUTPUT_CONTRACT,
            "counts": {
                "publication_rows": len(publications),
                "accepted_unpublished_candidates": len(candidates),
                "article_candidates": lane_counts.get("article", 0),
                "social_candidates": lane_counts.get("social", 0),
                "profile_ready": profile_ready_total,
                "current_source_profile_writer_drafts": current_drafts,
                "current_revisions_delivered_to_operator": delivered_current,
                "operator_current": status_counts.get("operator_current", 0),
                "needs_source_profile": status_counts.get("needs_source_profile", 0),
                "needs_writer_repair": status_counts.get("needs_writer_repair", 0),
                "needs_operator_delivery": status_counts.get("needs_operator_delivery", 0),
                "blocked_correction": status_counts.get("blocked_correction", 0),
                "publisher_profile_rows": len(publisher_profiles),
                "social_profile_rows": len(onboarding_profiles),
                "source_profile_capture_rows": len(captures),
                "publication_delivery_rows": len(deliveries),
            },
            "status_counts": dict(sorted(status_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
            "missing_article_source_keys": missing_article_sources,
            "missing_social_source_keys": missing_social_sources,
            "imported_publisher_profile_keys": imported_publisher_keys,
            "candidate_rows": candidates,
            "raw_kind_counts": {
                kind: len(rows_by_kind[kind]) for kind in sorted(rows_by_kind)
            },
        }
    finally:
        driver.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scan-limit", type=int, default=5000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = execute(max(1, min(int(args.scan_limit), 5000)))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report["counts"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
