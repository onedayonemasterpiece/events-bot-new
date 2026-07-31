#!/usr/bin/env python3
"""Read-only Region Talk review-card and diversified queue helpers."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import struct
from datetime import date, datetime, timedelta, timezone
from typing import Any


QUEUE_POLICY_VERSION = "region_talk_mmr_adjacency_v1"
DAILY_PLAN_POLICY_VERSION = "region_talk_daily_pair_antivector_v1"
SUPPORTED_VECTOR_ENCODING = "f16_le_base64"
ARTICLE_ORIGIN_TYPES = {"editorial_publication", "academic_publication"}


def canonical_url(row: dict[str, Any]) -> str:
    return str(row.get("post_url") or row.get("canonical_url") or "").strip().rstrip("/")


def quality_score(row: dict[str, Any]) -> float:
    for field in ("publication_score", "publication_pre_score", "external_research_quality_score", "candidate_score"):
        try:
            value = float(row.get(field))
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            return round(max(0.0, min(1.0, value)), 6)
    quality = row.get("quality_assessment") if isinstance(row.get("quality_assessment"), dict) else {}
    try:
        value = float(quality.get("normalized_score"))
    except (TypeError, ValueError):
        value = 0.0
    if math.isfinite(value):
        return round(max(0.0, min(1.0, value)), 6)
    return 0.0


def source_name(row: dict[str, Any]) -> str:
    publication = row.get("publication") if isinstance(row.get("publication"), dict) else {}
    return str(row.get("source_title") or row.get("publication_source_name") or publication.get("source_name") or "Источник").strip()


def decode_vector(row: dict[str, Any]) -> tuple[list[float] | None, dict[str, Any]]:
    encoded = str(row.get("embedding_vector_f16_b64") or row.get("publication_diversity_vector_f16_b64") or "").strip()
    encoding = str(row.get("embedding_vector_encoding") or row.get("publication_diversity_vector_encoding") or "").strip()
    model = str(row.get("model_id") or row.get("publication_diversity_model_id") or "").strip()
    contract = str(row.get("encoder_contract") or row.get("publication_diversity_encoder_contract") or "").strip()
    try:
        dim = int(row.get("embedding_dim") or row.get("publication_diversity_embedding_dim") or 0)
    except (TypeError, ValueError):
        dim = 0
    meta = {"model_id": model, "encoder_contract": contract, "embedding_dim": dim, "encoding": encoding}
    if not encoded:
        return None, {**meta, "vector_status": "missing"}
    if encoding != SUPPORTED_VECTOR_ENCODING or not model or not contract or dim <= 0:
        return None, {**meta, "vector_status": "incompatible_contract"}
    try:
        raw = base64.b64decode(encoded, validate=True)
        if len(raw) != dim * 2:
            raise ValueError("dimension mismatch")
        values = [float(value[0]) for value in struct.iter_unpack("<e", raw)]
    except (ValueError, struct.error):
        return None, {**meta, "vector_status": "invalid_payload"}
    norm = math.sqrt(sum(value * value for value in values))
    if not math.isfinite(norm) or norm <= 0:
        return None, {**meta, "vector_status": "zero_or_nonfinite"}
    return [value / norm for value in values], {**meta, "vector_status": "compatible"}


def cosine_if_compatible(left: dict[str, Any], right: dict[str, Any]) -> tuple[float | None, str]:
    left_vector, left_meta = decode_vector(left)
    right_vector, right_meta = decode_vector(right)
    if left_vector is None:
        return None, "left_" + str(left_meta.get("vector_status") or "missing")
    if right_vector is None:
        return None, "right_" + str(right_meta.get("vector_status") or "missing")
    contract = ("model_id", "encoder_contract", "embedding_dim", "encoding")
    if any(left_meta.get(field) != right_meta.get(field) for field in contract):
        return None, "incompatible_vector_contract"
    return max(-1.0, min(1.0, sum(a * b for a, b in zip(left_vector, right_vector)))), "compatible"


def _facets(row: dict[str, Any]) -> tuple[str, set[str], str]:
    source = str(row.get("canonical_source_key") or row.get("source_id") or row.get("source_title") or "").strip().lower()
    raw_topics = row.get("diversity_topics") or row.get("topics") or row.get("matched_place_names") or ""
    if isinstance(raw_topics, list):
        topics = {str(value).strip().lower() for value in raw_topics if str(value).strip()}
    else:
        topics = {value.strip().lower() for value in str(raw_topics).replace(",", ";").split(";") if value.strip()}
    content_type = str(row.get("content_origin_type") or row.get("content_type") or row.get("vector_content_type") or "").strip().lower()
    return source, topics, content_type


def heuristic_similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    left_source, left_topics, left_type = _facets(left)
    right_source, right_topics, right_type = _facets(right)
    value = 0.0
    if left_source and left_source == right_source:
        value = max(value, 0.82)
    if left_topics and right_topics and left_topics & right_topics:
        value = max(value, 0.68)
    if left_type and left_type == right_type:
        value = max(value, 0.35)
    return value


def _max_similarity(candidate: dict[str, Any], references: list[dict[str, Any]]) -> tuple[float, dict[str, Any]]:
    if not references:
        return 0.0, {"nearest_url": "", "diversity_mode": "not_applicable", "fallback_reasons": []}
    best = 0.0
    best_row: dict[str, Any] = {}
    best_mode = "heuristic_fallback"
    fallback_reasons: set[str] = set()
    compared = 0
    for reference in references:
        if canonical_url(candidate) and canonical_url(candidate) == canonical_url(reference):
            continue
        compared += 1
        similarity, status = cosine_if_compatible(candidate, reference)
        if similarity is None:
            fallback_reasons.add(status)
            similarity = heuristic_similarity(candidate, reference)
            current_mode = "heuristic_fallback"
        else:
            current_mode = "bge_m3_vector"
        if similarity > best:
            best = similarity
            best_row = reference
            best_mode = current_mode
    if not compared:
        return 0.0, {"nearest_url": "", "diversity_mode": "not_applicable", "fallback_reasons": []}
    return round(best, 6), {
        "nearest_url": canonical_url(best_row),
        "diversity_mode": best_mode,
        "fallback_reasons": sorted(fallback_reasons),
    }


def max_similarity(candidate: dict[str, Any], references: list[dict[str, Any]]) -> tuple[float, dict[str, Any]]:
    """Public wrapper used by durable queue planners and diagnostics."""

    return _max_similarity(candidate, references)


def content_lane(row: dict[str, Any]) -> str:
    """Split external articles from Telegram/VK/social publication posts."""

    explicit = str(row.get("content_lane") or row.get("publication_lane") or "").strip().lower()
    if explicit in {"article", "social"}:
        return explicit
    origin = str(row.get("content_origin_type") or "").strip().lower()
    if origin in ARTICLE_ORIGIN_TYPES or row.get("external_publication_id"):
        return "article"
    return "social"


def build_daily_publication_plan(
    candidates: list[dict[str, Any]],
    *,
    history: list[dict[str, Any]] | None = None,
    start_date: date,
    days: int = 14,
    diversity_weight: float = 0.35,
    pair_similarity_threshold: float = 0.82,
    locked_slots: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build one article and one social slot per day.

    Each lane is ranked against its own long-term published history and all
    earlier selections in that lane.  The social choice is additionally kept
    away from the article selected for the same day.  Future planned slots are
    intentionally not locks: callers may recalculate them whenever candidates
    arrive.  Only explicit ``locked``/``published`` slots belong in
    ``locked_slots``.
    """

    history = [dict(row) for row in (history or [])]
    locked_slots = dict(locked_slots or {})
    remaining: dict[str, list[dict[str, Any]]] = {"article": [], "social": []}
    seen: set[str] = set()
    for row in candidates:
        url = canonical_url(row)
        if not url or url in seen:
            continue
        seen.add(url)
        remaining[content_lane(row)].append(dict(row))

    lane_references = {
        lane: [row for row in history if content_lane(row) == lane]
        for lane in ("article", "social")
    }
    result: list[dict[str, Any]] = []
    pair_similarity_threshold = max(-1.0, min(1.0, float(pair_similarity_threshold)))

    for offset in range(max(0, int(days))):
        day = (start_date + timedelta(days=offset)).isoformat()
        selected_today: dict[str, dict[str, Any]] = {}
        for lane in ("article", "social"):
            fixed = locked_slots.get((day, lane))
            if fixed:
                fixed_row = {
                    **fixed,
                    "plan_date": day,
                    "content_lane": lane,
                    "queue_policy_version": DAILY_PLAN_POLICY_VERSION,
                    "slot_locked": True,
                }
                result.append(fixed_row)
                selected_today[lane] = fixed_row
                lane_references[lane].append(fixed_row)
                continue

            pool = remaining[lane]
            if not pool:
                result.append({
                    "plan_date": day,
                    "content_lane": lane,
                    "plan_status": "vacant",
                    "queue_policy_version": DAILY_PLAN_POLICY_VERSION,
                    "slot_locked": False,
                    "vacancy_reason": f"no_eligible_{lane}_candidate",
                })
                continue

            references = list(lane_references[lane])
            if lane == "social" and selected_today.get("article"):
                references.append(selected_today["article"])
            ranked = rank_publication_queue(
                pool,
                history=references,
                limit=len(pool),
                diversity_weight=diversity_weight,
                adjacency_threshold=1.0,
            )
            chosen = ranked[0]
            pair_relaxed = False
            pair_similarity = 0.0
            pair_mode = "not_applicable"
            if lane == "social" and selected_today.get("article"):
                article = selected_today["article"]
                alternatives: list[tuple[dict[str, Any], float, dict[str, Any]]] = []
                for row in ranked:
                    similarity, meta = _max_similarity(row, [article])
                    alternatives.append((row, similarity, meta))
                below = [item for item in alternatives if item[1] < pair_similarity_threshold]
                selected = below[0] if below else alternatives[0]
                chosen, pair_similarity, pair_meta = selected
                pair_mode = str(pair_meta.get("diversity_mode") or "heuristic_fallback")
                pair_relaxed = not bool(below)

            planned = {
                **chosen,
                "plan_date": day,
                "content_lane": lane,
                "plan_status": "planned",
                "queue_policy_version": DAILY_PLAN_POLICY_VERSION,
                "slot_locked": False,
                "pair_similarity": round(float(pair_similarity), 6),
                "pair_similarity_mode": pair_mode,
                "pair_similarity_threshold": pair_similarity_threshold,
                "pair_diversity_relaxed": pair_relaxed,
            }
            result.append(planned)
            selected_today[lane] = planned
            lane_references[lane].append(planned)
            chosen_url = canonical_url(chosen)
            remaining[lane] = [row for row in pool if canonical_url(row) != chosen_url]
    return result


def rank_publication_queue(
    candidates: list[dict[str, Any]],
    *,
    history: list[dict[str, Any]] | None = None,
    limit: int = 20,
    diversity_weight: float = 0.28,
    adjacency_threshold: float = 0.86,
) -> list[dict[str, Any]]:
    """Greedy MMR with an explicit previous-neighbour guard.

    Compatible BGE-M3 vectors are preferred. Missing/mismatched vectors use a
    disclosed source/topic/content fallback; they are never silently compared.
    """
    history = list(history or [])
    diversity_weight = max(0.0, min(1.0, float(diversity_weight)))
    adjacency_threshold = max(-1.0, min(1.0, float(adjacency_threshold)))
    remaining: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in candidates:
        url = canonical_url(row)
        if not url or url in seen:
            continue
        seen.add(url)
        remaining.append(dict(row))
    selected: list[dict[str, Any]] = []
    cap = max(0, min(int(limit), len(remaining)))
    while remaining and len(selected) < cap:
        evaluated: list[tuple[bool, float, float, str, dict[str, Any], dict[str, Any]]] = []
        for row in remaining:
            history_sim, history_meta = _max_similarity(row, history)
            selected_sim, selected_meta = _max_similarity(row, selected)
            max_sim = max(history_sim, selected_sim)
            nearest_meta = history_meta if history_sim >= selected_sim else selected_meta
            previous_sim = 0.0
            previous_mode = "not_applicable"
            if selected:
                vector_previous, status = cosine_if_compatible(row, selected[-1])
                if vector_previous is None:
                    previous_sim = heuristic_similarity(row, selected[-1])
                    previous_mode = "heuristic_fallback:" + status
                else:
                    previous_sim = vector_previous
                    previous_mode = "bge_m3_vector"
            rank_score = quality_score(row) - float(diversity_weight) * max_sim
            violates = bool(selected and previous_sim >= float(adjacency_threshold))
            evidence = {
                "quality_score": quality_score(row),
                "rank_score": round(rank_score, 6),
                "max_similarity_to_selected_or_history": round(max_sim, 6),
                "similarity_penalty": round(float(diversity_weight) * max_sim, 6),
                "nearest_url": nearest_meta.get("nearest_url") or "",
                "diversity_mode": nearest_meta.get("diversity_mode") or "heuristic_fallback",
                "fallback_reasons": nearest_meta.get("fallback_reasons") or [],
                "similarity_to_previous": round(previous_sim, 6),
                "previous_similarity_mode": previous_mode,
                "adjacency_threshold": float(adjacency_threshold),
            }
            evaluated.append((violates, -rank_score, -quality_score(row), canonical_url(row), row, evidence))
        non_violating = [entry for entry in evaluated if not entry[0]]
        pool = non_violating or evaluated
        chosen = min(pool, key=lambda item: (item[1], item[2], item[3]))
        row, evidence = chosen[4], chosen[5]
        evidence["adjacency_relaxed"] = bool(chosen[0] and not non_violating)
        evidence["adjacency_relax_reason"] = "all_remaining_candidates_exceed_threshold" if evidence["adjacency_relaxed"] else ""
        selected.append({
            **row,
            "queue_rank": len(selected) + 1,
            "queue_policy_version": QUEUE_POLICY_VERSION,
            **evidence,
        })
        remaining = [item for item in remaining if canonical_url(item) != canonical_url(row)]
    return selected


def queue_snapshot(rows: list[dict[str, Any]], *, requested_by: str = "", generated_at: str | None = None) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    identity = json.dumps(
        [(canonical_url(row), row.get("queue_rank"), row.get("rank_score")) for row in rows],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    snapshot_id = "rtqueue_" + hashlib.sha256((generated_at + "\0" + requested_by + "\0" + identity).encode("utf-8")).hexdigest()[:24]
    return {
        "snapshot_id": snapshot_id,
        "queue_policy_version": QUEUE_POLICY_VERSION,
        "generated_at": generated_at,
        "requested_by": requested_by,
        "count": len(rows),
        "rows": rows,
    }


def review_card(row: dict[str, Any]) -> str:
    url = canonical_url(row)
    publication = row.get("publication") if isinstance(row.get("publication"), dict) else {}
    editorial = row.get("editorial_pack") if isinstance(row.get("editorial_pack"), dict) else {}
    source = source_name(row)
    title = str(row.get("publication_title") or row.get("title") or publication.get("title") or row.get("short_summary") or editorial.get("title_short") or "Материал").strip()
    reason = str(row.get("publication_llm_reason") or row.get("llm_reason") or row.get("why_selected") or editorial.get("why_selected") or "").strip()[:420]
    overview = str(row.get("source_overview") or row.get("source_onboarding_paragraph") or editorial.get("source_overview") or "").strip()[:420]
    def display_metric(*fields: str) -> Any:
        for field in fields:
            value = row.get(field)
            if value is not None and str(value).strip() != "":
                return value
        return "—"

    media = display_metric("overall_media_score", "final_visual_score")
    postcard = display_metric("postcardness_score", "clip_postcardness_score")
    return "\n".join(filter(None, [
        f"🔎 Region Talk · {source}",
        title,
        url,
        f"Оценка: итог {quality_score(row):.3f} · изображение {media} · открыточность {postcard}",
        f"О публикации: {overview}" if overview else "",
        f"Почему: {reason}" if reason else "",
        "Права на изображение: только оценка, без переиспользования" if str(row.get("media_use_policy") or "") == "score_only_no_reuse" else "",
    ]))


def queue_messages(snapshot: dict[str, Any], *, max_chars: int = 3900) -> list[str]:
    header = (
        f"📚 Region Talk · очередь публикаций\n"
        f"snapshot: {snapshot.get('snapshot_id')}\n"
        f"policy: {snapshot.get('queue_policy_version')}\n"
        f"Кандидатов: {snapshot.get('count', 0)}"
    )
    blocks = [header]
    for row in snapshot.get("rows") or []:
        relaxed = " · соседство ослаблено" if row.get("adjacency_relaxed") else ""
        fallback = " · fallback" if row.get("diversity_mode") == "heuristic_fallback" else ""
        blocks.append(
            f"{row.get('queue_rank')}. {source_name(row)}\n"
            f"{canonical_url(row)}\n"
            f"rank={float(row.get('rank_score') or 0):.3f} · quality={float(row.get('quality_score') or 0):.3f} · "
            f"max_sim={float(row.get('max_similarity_to_selected_or_history') or 0):.3f}{fallback}{relaxed}"
        )
    messages: list[str] = []
    current = ""
    for block in blocks:
        proposed = block if not current else current + "\n\n" + block
        if len(proposed) > max_chars and current:
            messages.append(current)
            current = block
        else:
            current = proposed
    if current:
        messages.append(current)
    return messages
