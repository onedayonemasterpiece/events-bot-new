from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import platform
import random
import re
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

STAGE_NAME = "acq_comment_semantic_retrieval.v1"
DEFAULT_MODELS = ["intfloat/multilingual-e5-base", "BAAI/bge-m3"]
DEFAULT_GATE_MODEL = "intfloat/multilingual-e5-base"
DEFAULT_SCORING_METHOD = "positive_negative_margin"

INTENT_SETS: dict[str, list[str]] = {
    "route_poi_far_context": [
        "люди обсуждают поездки по Калининградской области",
        "люди обсуждают интересные места региона",
        "люди обсуждают туризм в Калининградской области",
        "люди делятся впечатлениями о местах, куда можно съездить",
        "люди обсуждают красивые места, города, побережье, замки, форты или музеи",
        "люди обсуждают, стоит ли посещать разные места области",
        "люди сравнивают туристические места",
        "люди спрашивают мнение о местах перед поездкой",
        "люди обсуждают отдых за пределами Калининграда",
        "люди обсуждают короткие поездки по области",
    ],
    "route_poi_medium_interest": [
        "человек спрашивает, куда съездить из Калининграда",
        "человек ищет место для поездки на один день",
        "человек спрашивает, что посмотреть за день",
        "человек спрашивает, что посмотреть в городе области",
        "человек ищет маршрут по Калининградской области",
        "человек спрашивает, куда поехать на выходных",
        "человек спрашивает, какие места стоит посетить",
        "человек ищет достопримечательности рядом",
        "человек спрашивает, как добраться до интересного места",
        "человек хочет понять, стоит ли место поездки",
        "человек спрашивает, что посмотреть кроме главной достопримечательности",
        "человек ищет маршрут для прогулки или поездки",
    ],
    "route_poi_close_actionable": [
        "посоветуйте маршрут на один день из Калининграда",
        "куда поехать из Калининграда на электричке",
        "что посмотреть в Светлогорске за один день",
        "что посмотреть в Зеленоградске за один день",
        "что посмотреть в Балтийске за один день",
        "что посмотреть в Янтарном за один день",
        "что посмотреть в Черняховске за один день",
        "стоит ли ехать в конкретное место",
        "стоит ли смотреть этот замок, форт, кирху, музей или парк",
        "как добраться до этого места без машины",
        "что совместить с этим местом в одной поездке",
        "что посмотреть рядом с этим местом",
        "куда съездить с детьми по области",
        "куда съездить на машине на один день",
        "куда съездить на выходные недалеко от Калининграда",
    ],
    "event_far_context": [
        "люди обсуждают мероприятия и афишу",
        "люди обсуждают концерты, выставки, лекции, спектакли или фестивали",
        "люди интересуются культурными событиями",
        "люди выбирают, на какое мероприятие пойти",
        "люди обсуждают городские мероприятия",
        "люди спрашивают о событиях на выходные",
    ],
    "event_close_question": [
        "человек спрашивает, во сколько начинается мероприятие",
        "человек спрашивает, сколько длится мероприятие",
        "человек спрашивает, где проходит мероприятие",
        "человек спрашивает, есть ли билеты",
        "человек спрашивает, сколько стоит билет",
        "человек спрашивает, нужна ли регистрация",
        "человек спрашивает, можно ли прийти с детьми",
        "человек спрашивает, не отменили ли событие",
        "человек спрашивает, перенесли ли мероприятие",
        "человек спрашивает, как попасть на мероприятие",
        "человек спрашивает программу мероприятия",
        "человек спрашивает, будет ли мероприятие сегодня, завтра или в выходные",
    ],
    "organizer_comment_fit": [
        "в комментариях под постом организатора задают практические вопросы о событии",
        "люди уточняют возрастные ограничения события",
        "люди уточняют вход, регистрацию или билеты",
        "люди спрашивают, где встреча или вход",
        "люди спрашивают, что будет при плохой погоде",
        "люди спрашивают, подходит ли событие детям",
        "люди спрашивают про доступность, коляски или собак",
        "люди спрашивают, остались ли места",
        "люди спрашивают, будет ли запись или трансляция",
    ],
    "event_site_search_or_listing": [
        "человек ищет сайт с афишей мероприятий",
        "человек спрашивает где посмотреть календарь событий",
        "человек ищет список выставок или популярных мероприятий",
        "человек спрашивает где найти поиск по событиям",
    ],
    "organizer_submission_or_partnership": [
        "человек спрашивает куда прислать анонс события",
        "организатор спрашивает как добавить мероприятие в афишу",
        "человек спрашивает про публикацию события или информационное партнёрство",
    ],
    "badge_filter_need": [
        "человек ищет события по Пушкинской карте",
        "человек ищет бесплатные мероприятия",
        "человек спрашивает мероприятия для детей или семьи",
        "человек ищет благотворительные события",
        "человек спрашивает есть ли запись или трансляция события",
    ],
    "negative_intents": [
        "политический спор без вопроса о поездке, месте или событии",
        "общий флуд",
        "оскорбление или эмоциональный комментарий без полезного вопроса",
        "благодарность без запроса информации",
        "комментарий круто без намерения посетить",
        "комментарий не нравится без вопроса",
        "реклама без вопроса пользователя",
        "обсуждение бытовой темы без туристического или событийного смысла",
        "обсуждение транспорта без связи с поездкой к месту",
        "обсуждение погоды без связи с поездкой или мероприятием",
        "вопрос не связан с местом, маршрутом, достопримечательностью или мероприятием",
    ],
}

POSITIVE_INTENT_SETS = [name for name in INTENT_SETS if name != "negative_intents"]
ROUTE_INTENT_SETS = {"route_poi_far_context", "route_poi_medium_interest", "route_poi_close_actionable"}
EVENT_INTENT_SETS = {"event_far_context", "event_close_question", "event_site_search_or_listing", "badge_filter_need"}
ORGANIZER_INTENT_SETS = {"organizer_comment_fit", "organizer_submission_or_partnership"}

_TOKEN_RE = re.compile(r"[\wА-Яа-яЁё]+", re.U)
_HTML_RE = re.compile(r"<[^>]+>")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_ROUTE_DEST_RE = re.compile(
    r"(?i)\b(светлогорск\w*|зеленоградск\w*|балтийск\w*|янтарн\w*|черняховск\w*|советск\w*|гусев\w*|куршск\w*\s+кос\w*|зам\w+|форт\w*|кирх\w*|побереж\w+)\b"
)
_TRANSPORT_RE = re.compile(r"(?i)\b(электричк\w*|поезд\w*|автобус\w*|машин\w*|без\s+машин\w*)\b")


def truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def semantic_retrieval_enabled() -> bool:
    return truthy(os.getenv("ACQ_ENABLE_COMMENT_SEMANTIC_RETRIEVAL"))


def _json_env(name: str, default: Any) -> Any:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _int_env(name: str, default: int, *, min_value: int | None = None) -> int:
    try:
        value = int(os.getenv(name) or default)
    except Exception:
        value = int(default)
    if min_value is not None:
        value = max(min_value, value)
    return value


def normalize_comment_text(text: str) -> str:
    cleaned = _CONTROL_RE.sub(" ", str(text or ""))
    cleaned = _HTML_RE.sub(" ", cleaned)
    return " ".join(cleaned.split())


def _prefix_text(model_name: str, text: str, *, is_query: bool) -> str:
    if "multilingual-e5" in model_name.lower():
        return ("query: " if is_query else "passage: ") + text
    return text


class HashingEmbeddingBackend:
    """Small deterministic backend for tests/offline smoke; not used for quality claims."""

    def __init__(self, dim: int = 96) -> None:
        self.dim = dim

    def encode(self, texts: list[str], *, model_name: str, is_query: bool, batch_size: int, max_length: int) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            vec = [0.0] * self.dim
            for token in _TOKEN_RE.findall(text.lower()):
                digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
                idx = int.from_bytes(digest[:4], "little") % self.dim
                sign = -1.0 if digest[4] & 1 else 1.0
                vec[idx] += sign
            out.append(_normalize(vec))
        return out


class SentenceTransformerBackend:
    def __init__(self) -> None:
        self._models: dict[str, Any] = {}

    def _ensure_libs(self) -> None:
        try:
            import sentence_transformers  # noqa: F401
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "sentence-transformers"])

    def _model(self, model_name: str) -> Any:
        self._ensure_libs()
        if model_name not in self._models:
            from sentence_transformers import SentenceTransformer

            self._models[model_name] = SentenceTransformer(model_name, device=os.getenv("ACQ_COMMENT_RETRIEVAL_DEVICE") or None)
        return self._models[model_name]

    def encode(self, texts: list[str], *, model_name: str, is_query: bool, batch_size: int, max_length: int) -> Any:
        model = self._model(model_name)
        prepared = [_prefix_text(model_name, text, is_query=is_query) for text in texts]
        # SentenceTransformer accepts max_seq_length as a mutable property.
        try:
            model.max_seq_length = int(max_length)
        except Exception:
            pass
        return model.encode(
            prepared,
            batch_size=batch_size,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )


def _normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(float(x) * float(x) for x in vec))
    if norm <= 0:
        return vec
    return [float(x) / norm for x in vec]


def _dot(a: Any, b: Any) -> float:
    return float(sum(float(x) * float(y) for x, y in zip(a, b)))


def _to_list_matrix(matrix: Any) -> list[list[float]]:
    if hasattr(matrix, "tolist"):
        return matrix.tolist()
    return [list(row) for row in matrix]


def _peak_ram_mb() -> float | None:
    try:
        import resource

        value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        # Linux reports KB, macOS bytes. Kaggle/Linux path is KB.
        return value / 1024.0 if value > 10_000 else value / (1024.0 * 1024.0)
    except Exception:
        return None


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = (len(ordered) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(ordered[lo])
    return float(ordered[lo] * (hi - pos) + ordered[hi] * (pos - lo))


def _mean(values: list[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _action_for_intent(intent_set: str) -> str:
    if intent_set in ROUTE_INTENT_SETS:
        return "trip_route_poi_recommendation"
    if intent_set == "organizer_comment_fit":
        return "organizer_visibility_clarification"
    if intent_set == "organizer_submission_or_partnership":
        return "organizer_submission_or_partnership"
    if intent_set == "event_site_search_or_listing":
        return "event_site_search_or_listing"
    if intent_set == "badge_filter_need":
        return "badge_filter_need"
    return "event_recommendation_reply"


def _route_target_hint(text: str, intent_set: str) -> dict[str, Any]:
    if intent_set not in ROUTE_INTENT_SETS:
        return {"route_target_status": "not_applicable", "destination_hint": "", "poi_hints": [], "transport_hint": "unknown", "event_ids": []}
    destinations = [m.group(0) for m in _ROUTE_DEST_RE.finditer(text or "")]
    transport = "unknown"
    transport_match = _TRANSPORT_RE.search(text or "")
    if transport_match:
        raw = transport_match.group(0).lower()
        if "электр" in raw or "поезд" in raw:
            transport = "train"
        elif "автоб" in raw:
            transport = "bus"
        elif "машин" in raw:
            transport = "car" if "без" not in raw else "unknown"
    return {
        "route_target_status": "route_needed",
        "destination_hint": destinations[0] if destinations else "",
        "poi_hints": destinations[:5],
        "transport_hint": transport,
        "event_ids": [],
    }


def _surface_key(record: dict[str, Any]) -> str:
    return str(record.get("surface_key") or record.get("surface_external_id") or record.get("surface_url") or "unknown")


def _dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for rec in records:
        text = normalize_comment_text(str(rec.get("text") or rec.get("text_snapshot") or ""))
        if not text:
            continue
        key = (_surface_key(rec), text[:500].casefold())
        if key in seen:
            continue
        seen.add(key)
        item = dict(rec)
        item["text"] = text
        item["text_snapshot"] = text[:500]
        item.setdefault("surface_key", _surface_key(item))
        out.append(item)
    return out


def _score_comment(
    comment_vec: Any,
    intent_vectors: dict[str, list[Any]],
    negative_vectors: list[Any],
) -> list[dict[str, Any]]:
    negative_scores = [_dot(comment_vec, neg) for neg in negative_vectors]
    negative_score = max(negative_scores) if negative_scores else 0.0
    rows: list[dict[str, Any]] = []
    for intent_set, vectors in intent_vectors.items():
        scores = [_dot(comment_vec, vec) for vec in vectors]
        if not scores:
            continue
        max_score = max(scores)
        top3 = sorted(scores, reverse=True)[:3]
        centroid_score = _mean(scores)
        margin = max_score - negative_score
        top_idx = scores.index(max_score)
        rows.append({
            "intent_set": intent_set,
            "max_positive_similarity": float(max_score),
            "top3_positive_mean": float(_mean(top3)),
            "centroid_similarity": float(centroid_score),
            "positive_negative_margin": float(margin),
            "positive_score": float(max_score),
            "negative_score": float(negative_score),
            "top_intent_phrase": INTENT_SETS[intent_set][top_idx] if top_idx < len(INTENT_SETS[intent_set]) else "",
            "top_intent_score": float(max_score),
        })
    return rows


def _rank_candidates(rows: list[dict[str, Any]], *, scoring_method: str) -> list[dict[str, Any]]:
    rows = [r for r in rows if r.get("intent_set") != "negative_intents"]
    rows.sort(key=lambda r: float(r.get(scoring_method) or r.get("score") or 0.0), reverse=True)
    for idx, row in enumerate(rows, start=1):
        row["rank_global"] = idx
        row["score"] = float(row.get(scoring_method) or 0.0)
    per_surface: dict[str, int] = defaultdict(int)
    for row in rows:
        key = str(row.get("surface_key") or "unknown")
        per_surface[key] += 1
        row["rank_within_surface"] = per_surface[key]
    total = max(1, len(rows))
    for row in rows:
        pct = 100.0 * int(row["rank_global"]) / total
        bucket = "top_10pct"
        if pct <= 0.5:
            bucket = "top_0_5pct"
        elif pct <= 1:
            bucket = "top_1pct"
        elif pct <= 3:
            bucket = "top_3pct"
        elif pct <= 5:
            bucket = "top_5pct"
        row["funnel_bucket"] = bucket
    return rows


def _surface_profile(surface_key: str, surface_records: list[dict[str, Any]], rows: list[dict[str, Any]], *, scoring_method: str) -> dict[str, Any]:
    by_intent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_intent[str(row.get("intent_set"))].append(row)
    semantic_presence: dict[str, dict[str, Any]] = {}
    dominant: list[str] = []
    for intent_set in POSITIVE_INTENT_SETS:
        intent_rows = by_intent.get(intent_set, [])
        scores = [float(r.get(scoring_method) or r.get("score") or 0.0) for r in intent_rows]
        p95 = _percentile(scores, 0.95)
        candidate_count_top3pct = sum(1 for r in intent_rows if str(r.get("funnel_bucket")) in {"top_0_5pct", "top_1pct", "top_3pct"})
        if candidate_count_top3pct >= 5 or p95 >= 0.18:
            level = "high"
        elif candidate_count_top3pct >= 2 or p95 >= 0.10:
            level = "medium"
        elif scores and (candidate_count_top3pct >= 1 or p95 > 0.03):
            level = "low"
        else:
            level = "none"
        if level != "none":
            if intent_set in ROUTE_INTENT_SETS and "route_poi" not in dominant:
                dominant.append("route_poi")
            elif intent_set in EVENT_INTENT_SETS and "event_questions" not in dominant:
                dominant.append("event_questions")
            elif intent_set in ORGANIZER_INTENT_SETS and "organizer_fit" not in dominant:
                dominant.append("organizer_fit")
        examples = [r.get("comment_id") for r in sorted(intent_rows, key=lambda r: float(r.get(scoring_method) or 0.0), reverse=True)[:3]]
        semantic_presence[intent_set] = {
            "present": level != "none",
            "level": level,
            "top_score_p95": round(p95, 6),
            "candidate_count_top3pct": int(candidate_count_top3pct),
            "example_comment_ids": [str(x) for x in examples if x is not None],
        }
    comments_total = len(surface_records)
    actionable = sum(1 for r in rows if str(r.get("funnel_bucket")) in {"top_0_5pct", "top_1pct", "top_3pct"})
    if actionable >= 3 and dominant:
        decision = "monitor"
        reason = f"{actionable} top semantic candidates; interests={', '.join(dominant)}"
    elif dominant and comments_total >= 5:
        decision = "sample_more"
        reason = f"semantic signal present but only {actionable} top candidates"
    elif dominant:
        decision = "low_priority"
        reason = "weak semantic signal in a small sample"
    else:
        decision = "reject"
        reason = "no acquisition-relevant semantic signal in embedded comments"
    dates = [str(r.get("created_at") or "") for r in surface_records if r.get("created_at")]
    return {
        "surface_key": surface_key,
        "platform": surface_records[0].get("platform") if surface_records else None,
        "surface_type": surface_records[0].get("surface_type") if surface_records else None,
        "comments_total": comments_total,
        "comments_embedded": comments_total,
        "period": {"min_created_at": min(dates) if dates else None, "max_created_at": max(dates) if dates else None},
        "semantic_presence": semantic_presence,
        "dominant_detected_interests": dominant,
        "monitoring_decision_hint": decision,
        "monitoring_reason": reason,
        "llm_budget_recommendation": {
            "send_top_comments_to_llm": min(actionable, _int_env("ACQ_COMMENT_RETRIEVAL_MAX_LLM_CANDIDATES", 80, min_value=1)),
            "reason": "top retrieval candidates only; no full-comment LLM pass",
        },
    }


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_xlsx(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
    except Exception:
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "manual_review"
    headers = [
        "label", "action_class", "is_actionable_reply_opportunity", "false_positive_type", "model_disagreement_bucket",
        "model_name", "intent_set", "score", "rank_global", "rank_within_surface", "surface_key", "platform", "surface_type",
        "context_url", "text_snapshot", "top_intent_phrase", "positive_score", "negative_score", "funnel_bucket",
        "destination_hint", "transport_hint",
    ]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill("solid", fgColor="D9EAF7")
    for row in rows:
        ws.append([row.get(h) for h in headers])
        url_cell = ws.cell(ws.max_row, headers.index("context_url") + 1)
        if row.get("context_url"):
            url_cell.hyperlink = str(row.get("context_url"))
            url_cell.style = "Hyperlink"
    for idx, width in enumerate([12, 28, 28, 24, 28, 34, 28, 12, 12, 16, 28, 10, 18, 45, 70, 55, 14, 14, 14, 22, 16], start=1):
        ws.column_dimensions[chr(64 + idx) if idx <= 26 else "Z"].width = width
    ws.freeze_panes = "A2"

    summary = wb.create_sheet("summary")
    summary.append(["metric", "value"])
    summary.append(["rows", len(rows)])
    summary.append(["generated_at", datetime.now(timezone.utc).isoformat()])
    for cell in summary[1]:
        cell.font = Font(bold=True)
    wb.save(path)


def _select_manual_rows(candidates: list[dict[str, Any]], *, max_rows: int) -> list[dict[str, Any]]:
    rows = candidates[: max_rows // 2]
    # Include near-threshold and lower-ranked rows for calibration, deterministic sample.
    if len(candidates) > len(rows):
        step = max(1, len(candidates) // max(1, max_rows - len(rows)))
        rows.extend(candidates[len(rows)::step][: max_rows - len(rows)])
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows[:max_rows]:
        key = (str(row.get("context_url")), str(row.get("model_name")))
        if key in seen:
            continue
        seen.add(key)
        enriched = dict(row)
        enriched.setdefault("label", "")
        enriched.setdefault("is_actionable_reply_opportunity", "")
        enriched.setdefault("false_positive_type", "")
        out.append(enriched)
    return out


def _report_md(summary: dict[str, Any], profiles: list[dict[str, Any]], candidates: list[dict[str, Any]], speed_rows: list[dict[str, Any]]) -> str:
    top_route = [c for c in candidates if c.get("candidate_action_type") == "trip_route_poi_recommendation"][:10]
    lines = [
        "# Subscriber Acquisition Comment Semantic Retrieval Dry Run",
        "",
        "## Executive summary",
        f"- Stage: `{STAGE_NAME}`",
        f"- Comments embedded: {summary.get('comments_embedded', 0)} / {summary.get('comments_total', 0)}",
        f"- Surfaces profiled: {summary.get('surface_profiles_count', 0)}",
        f"- Candidate rows: {summary.get('candidate_count', 0)}",
        f"- Recommended gate model: `{summary.get('recommended_model')}`",
        "- No external Telegram/VK sends and no full-comment LLM pass were used.",
        "",
        "## Speed benchmark",
        "| model | batch | max_length | comments/sec | total sec | peak RAM MB |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in speed_rows:
        lines.append(f"| {row.get('model_name')} | {row.get('batch_size')} | {row.get('max_length')} | {row.get('comments_per_sec', 0):.2f} | {row.get('total_sec', 0):.2f} | {row.get('peak_ram_mb') or ''} |")
    lines.extend(["", "## Top monitoring surfaces", "| decision | surface | comments | interests | reason |", "| --- | --- | ---: | --- | --- |"])
    for profile in sorted(profiles, key=lambda p: (p.get("monitoring_decision_hint") != "monitor", -(p.get("comments_embedded") or 0)))[:20]:
        lines.append(f"| {profile.get('monitoring_decision_hint')} | {profile.get('surface_key')} | {profile.get('comments_embedded')} | {', '.join(profile.get('dominant_detected_interests') or [])} | {profile.get('monitoring_reason')} |")
    lines.extend(["", "## Best route/POI examples", "| score | surface | url | text |", "| ---: | --- | --- | --- |"])
    for row in top_route:
        text = str(row.get("text_snapshot") or "").replace("|", " ")[:160]
        lines.append(f"| {row.get('score', 0):.4f} | {row.get('surface_key')} | {row.get('context_url')} | {text} |")
    return "\n".join(lines) + "\n"


def _load_models_from_env() -> list[str]:
    models = _json_env("ACQ_COMMENT_RETRIEVAL_MODELS_JSON", DEFAULT_MODELS)
    if isinstance(models, str):
        models = [models]
    out = [str(m).strip() for m in models if str(m).strip()]
    return out or list(DEFAULT_MODELS)


def _batch_for_model(model_name: str) -> int:
    key = "ACQ_COMMENT_RETRIEVAL_BGE_BATCH_SIZE" if "bge" in model_name.lower() else "ACQ_COMMENT_RETRIEVAL_E5_BATCH_SIZE"
    default = 8 if "bge" in model_name.lower() else 32
    return _int_env(key, default, min_value=1)


def run_comment_semantic_retrieval(
    comment_records: list[dict[str, Any]],
    *,
    surfaces_by_external: dict[str, dict[str, Any]] | None = None,
    output_dir: str | Path | None = None,
    backend: Any | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    output_path = Path(output_dir or os.getenv("ACQ_OUTPUT_DIR") or "/kaggle/working")
    output_path.mkdir(parents=True, exist_ok=True)
    scoring_method = os.getenv("ACQ_COMMENT_RETRIEVAL_SCORING_METHOD") or DEFAULT_SCORING_METHOD
    max_length = _int_env("ACQ_COMMENT_RETRIEVAL_MAX_LENGTH", 128, min_value=16)
    max_llm_candidates = _int_env("ACQ_COMMENT_RETRIEVAL_MAX_LLM_CANDIDATES", _int_env("ACQ_MAX_LLM_CALLS_PER_RUN", 80, min_value=1), min_value=1)
    max_manual_rows = _int_env("ACQ_COMMENT_RETRIEVAL_MANUAL_SAMPLE_ROWS", 800, min_value=20)
    models = _load_models_from_env()
    records = _dedupe_records(comment_records)
    backend = backend or SentenceTransformerBackend()
    progress_callback = progress_callback or (lambda _phase, _payload: None)
    progress_callback("loading_comments", {"comments_total": len(comment_records), "comments_after_filter": len(records), "progress_percent": 5})

    positive_phrases = {name: phrases for name, phrases in INTENT_SETS.items() if name != "negative_intents"}
    negative_phrases = INTENT_SETS["negative_intents"]
    all_candidates: list[dict[str, Any]] = []
    speed_rows: list[dict[str, Any]] = []
    distributions: list[dict[str, Any]] = []

    for model_index, model_name in enumerate(models, start=1):
        model_started = time.perf_counter()
        batch_size = _batch_for_model(model_name)
        progress_callback("embedding_intents", {"model_name": model_name, "progress_percent": 5 + int(10 * model_index / max(1, len(models)))})
        intent_vectors: dict[str, list[Any]] = {}
        intent_embed_sec = 0.0
        for intent_set, phrases in positive_phrases.items():
            t0 = time.perf_counter()
            intent_vectors[intent_set] = _to_list_matrix(backend.encode(phrases, model_name=model_name, is_query=True, batch_size=batch_size, max_length=max_length))
            intent_embed_sec += time.perf_counter() - t0
        t0 = time.perf_counter()
        negative_vectors = _to_list_matrix(backend.encode(negative_phrases, model_name=model_name, is_query=True, batch_size=batch_size, max_length=max_length))
        intent_embed_sec += time.perf_counter() - t0

        comments = [r["text"] for r in records]
        progress_callback("embedding_comments", {"model_name": model_name, "comments_total": len(comments), "comments_processed": 0, "progress_percent": 20})
        t0 = time.perf_counter()
        comment_vectors = _to_list_matrix(backend.encode(comments, model_name=model_name, is_query=False, batch_size=batch_size, max_length=max_length)) if comments else []
        comment_embedding_sec = time.perf_counter() - t0
        progress_callback("scoring", {"model_name": model_name, "comments_processed": len(comments), "progress_percent": 60})
        t0 = time.perf_counter()
        model_rows: list[dict[str, Any]] = []
        for rec, vec in zip(records, comment_vectors):
            scored = _score_comment(vec, intent_vectors, negative_vectors)
            for row in scored:
                enriched = dict(rec)
                enriched.update(row)
                enriched["model_name"] = model_name
                enriched["max_length"] = max_length
                enriched["batch_size"] = batch_size
                enriched["candidate_action_type"] = _action_for_intent(str(row.get("intent_set")))
                enriched["target_hint"] = _route_target_hint(str(rec.get("text") or ""), str(row.get("intent_set")))
                enriched["destination_hint"] = enriched["target_hint"].get("destination_hint")
                enriched["transport_hint"] = enriched["target_hint"].get("transport_hint")
                model_rows.append(enriched)
        model_rows = _rank_candidates(model_rows, scoring_method=scoring_method)
        scoring_sec = time.perf_counter() - t0
        all_candidates.extend(model_rows)
        scores = [float(r.get("score") or 0.0) for r in model_rows]
        distributions.append({
            "model_name": model_name,
            "scoring_method": scoring_method,
            "count": len(scores),
            "p50": _percentile(scores, 0.50),
            "p90": _percentile(scores, 0.90),
            "p95": _percentile(scores, 0.95),
            "p99": _percentile(scores, 0.99),
            "max": max(scores) if scores else 0.0,
        })
        total_sec = time.perf_counter() - model_started
        comments_per_sec = len(records) / total_sec if total_sec > 0 else 0.0
        speed_rows.append({
            "model_name": model_name,
            "device": os.getenv("ACQ_COMMENT_RETRIEVAL_DEVICE") or "auto",
            "batch_size": batch_size,
            "max_length": max_length,
            "comments_total": len(records),
            "intent_embedding_sec": round(intent_embed_sec, 4),
            "comment_embedding_sec": round(comment_embedding_sec, 4),
            "scoring_sec": round(scoring_sec, 4),
            "total_sec": round(total_sec, 4),
            "comments_per_sec": round(comments_per_sec, 4),
            "comments_per_hour": round(comments_per_sec * 3600, 2),
            "peak_ram_mb": round(_peak_ram_mb() or 0.0, 2),
            "cpu": platform.processor() or platform.machine(),
        })

    # Cross-model ranking and disagreement labels.
    all_candidates = _rank_candidates(all_candidates, scoring_method=scoring_method)
    by_context_model = {(str(c.get("context_url")), str(c.get("model_name")), str(c.get("intent_set"))): c for c in all_candidates}
    models_set = {str(m) for m in models}
    for c in all_candidates:
        context = str(c.get("context_url"))
        intent = str(c.get("intent_set"))
        present = {m for m in models_set if (context, m, intent) in by_context_model and int(by_context_model[(context, m, intent)].get("rank_global") or 999999) <= max(1000, max_llm_candidates * 10)}
        if len(present) == len(models_set):
            bucket = "both_models"
        elif str(c.get("model_name")) in present:
            bucket = f"{c.get('model_name')}_only"
        else:
            bucket = "near_threshold"
        c["model_disagreement_bucket"] = bucket

    gate_model = os.getenv("ACQ_COMMENT_RETRIEVAL_GATE_MODEL") or (DEFAULT_GATE_MODEL if DEFAULT_GATE_MODEL in models else models[0])
    gate_candidates = [c for c in all_candidates if c.get("model_name") == gate_model]
    gate_candidates = _rank_candidates(gate_candidates, scoring_method=scoring_method)[:max_llm_candidates]

    progress_callback("surface_profile", {"surfaces": len({r.get('surface_key') for r in records}), "progress_percent": 78})
    profiles: list[dict[str, Any]] = []
    rows_by_surface: dict[str, list[dict[str, Any]]] = defaultdict(list)
    records_by_surface: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in all_candidates:
        # Profiles are based on selected gate model to avoid double-counting two benchmark models.
        if row.get("model_name") == gate_model:
            rows_by_surface[str(row.get("surface_key") or "unknown")].append(row)
    for rec in records:
        records_by_surface[str(rec.get("surface_key") or "unknown")].append(rec)
    for surface_key, surface_records in records_by_surface.items():
        profile = _surface_profile(surface_key, surface_records, rows_by_surface.get(surface_key, []), scoring_method=scoring_method)
        if surfaces_by_external and surface_key in surfaces_by_external:
            s = surfaces_by_external[surface_key]
            profile["surface_title"] = s.get("title")
            profile["surface_url"] = s.get("url")
        profiles.append(profile)

    artifact_prefix = os.getenv("ACQ_COMMENT_RETRIEVAL_ARTIFACT_PREFIX") or "comment_retrieval"
    candidates_csv = output_path / f"{artifact_prefix}_candidates.csv"
    profiles_csv = output_path / f"{artifact_prefix}_surface_profiles.csv"
    distributions_csv = output_path / f"{artifact_prefix}_score_distributions.csv"
    speed_csv = output_path / f"{artifact_prefix}_speed_metrics.csv"
    manual_xlsx = output_path / f"{artifact_prefix}_manual_review_sample.xlsx"
    report_md = output_path / f"{artifact_prefix}_report.md"
    summary_json = output_path / "acq_comment_retrieval_run_summary.json"

    candidate_fields = [
        "run_id", "surface_key", "platform", "surface_type", "context_url", "comment_id", "post_id", "topic_id", "thread_id",
        "created_at", "text_snapshot", "model_name", "max_length", "batch_size", "intent_set", "score", "positive_score", "negative_score",
        "top_intent_phrase", "top_intent_score", "rank_global", "rank_within_surface", "funnel_bucket", "candidate_action_type",
        "destination_hint", "transport_hint", "model_disagreement_bucket",
    ]
    _write_csv(candidates_csv, all_candidates, candidate_fields)
    _write_csv(profiles_csv, [{**p, "semantic_presence": json.dumps(p.get("semantic_presence"), ensure_ascii=False), "dominant_detected_interests": ",".join(p.get("dominant_detected_interests") or [])} for p in profiles], [
        "surface_key", "platform", "surface_type", "surface_title", "surface_url", "comments_total", "comments_embedded", "monitoring_decision_hint", "monitoring_reason", "dominant_detected_interests", "semantic_presence",
    ])
    _write_csv(distributions_csv, distributions, ["model_name", "scoring_method", "count", "p50", "p90", "p95", "p99", "max"])
    _write_csv(speed_csv, speed_rows, ["model_name", "device", "batch_size", "max_length", "comments_total", "intent_embedding_sec", "comment_embedding_sec", "scoring_sec", "total_sec", "comments_per_sec", "comments_per_hour", "peak_ram_mb", "cpu"])
    manual_rows = _select_manual_rows(all_candidates, max_rows=max_manual_rows)
    _write_xlsx(manual_xlsx, manual_rows)

    summary = {
        "stage": STAGE_NAME,
        "run_id": os.getenv("KAGGLE_RUN_ID") or f"acq-comment-retrieval-{int(time.time())}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "models": models,
        "recommended_model": gate_model,
        "scoring_method": scoring_method,
        "comments_total": len(comment_records),
        "comments_after_filter": len(records),
        "comments_embedded": len(records),
        "surface_profiles_count": len(profiles),
        "candidate_count": len(all_candidates),
        "llm_gate_candidate_count": len(gate_candidates),
        "estimated_llm_reduction_vs_all_comments": round(1.0 - (len(gate_candidates) / max(1, len(records))), 6),
        "speed_metrics": speed_rows,
        "score_distributions": distributions,
        "artifacts": {
            "summary_json": str(summary_json),
            "candidates_csv": str(candidates_csv),
            "surface_profiles_csv": str(profiles_csv),
            "score_distributions_csv": str(distributions_csv),
            "speed_metrics_csv": str(speed_csv),
            "manual_review_xlsx": str(manual_xlsx),
            "report_md": str(report_md),
        },
        "total_sec": round(time.perf_counter() - started, 4),
    }
    report_md.write_text(_report_md(summary, profiles, gate_candidates, speed_rows), encoding="utf-8")
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    progress_callback("complete", {"comments_embedded": len(records), "candidate_count": len(all_candidates), "progress_percent": 100})
    return {
        "summary": summary,
        "surface_profiles": profiles,
        "candidates": all_candidates,
        "llm_gate_candidates": gate_candidates,
        "artifacts": summary["artifacts"],
    }
