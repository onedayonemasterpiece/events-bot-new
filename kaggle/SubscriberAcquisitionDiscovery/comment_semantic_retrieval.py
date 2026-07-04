from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import platform
import random
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
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
_QUESTION_SIGNAL_RE = re.compile(
    r"(?i)(\?|"
    r"\b(?:куда|где|когда|как|что|сколько|какой|какая|какие|какое|зачем|почему)\b|"
    r"\b(?:подскажите|посоветуйте|посоветуй|ищу|ищем|интересует|нужен|нужна|нужно)\b|"
    r"\b(?:есть\s+ли|будет\s+ли|можно\s+ли|нужна\s+ли|нужно\s+ли|кто\s+знает)\b)"
)
_OFFER_NOISE_RE = re.compile(
    r"(?i)\b("
    r"сохраняйте|записывайтесь|приглашаем|предлагаем|предлагаю|приходите|жд[её]м\s+вас|"
    r"бронируйте|бронь|забронировать|купить\s+билет|стоимость|цена|скидк\w*|акци\w*|"
    r"в\s+наличии|прода[её]тся|продам|сдам|аренда|работа|ваканси\w*|резюме|"
    r"экскурси\w*\s+(?:по|на|в)|тур(?:ы|ов|ом)?|заезды|каждую\s+неделю|"
    r"пишите\s+(?:в\s+)?(?:лс|личку|директ)|whatsapp|ватсап|тел\.|телефон|подробности\s+по\s+ссылке"
    r")\b"
)
_URL_OR_MENTION_RE = re.compile(r"(?i)(https?://|t\.me/|vk\.com/|@[A-Za-z0-9_]{4,})")
_ROUTE_CONTEXT_RE = re.compile(
    r"(?i)\b(куда\s+(?:съездить|поехать|сходить)|что\s+посмотреть|маршрут|достопримечательн\w*|"
    r"посетить|светлогорск\w*|зеленоградск\w*|балтийск\w*|янтарн\w*|черняховск\w*|"
    r"куршск\w*\s+кос\w*|зам\w+|форт\w*|кирх\w*|электричк\w*|автобус\w*)\b"
)
_EVENT_CONTEXT_RE = re.compile(
    r"(?i)\b(мероприяти\w*|событи\w*|афиш\w*|концерт\w*|выставк\w*|спектакл\w*|"
    r"фестивал\w*|лекци\w*|мастер[- ]?класс\w*|билет\w*|регистрац\w*|начина\w*|"
    r"начало|длится|возрастн\w*|вход|программ\w*|запись|трансляц\w*|"
    r"куда\s+сходить|выходн\w*)\b"
)
_ORGANIZER_SUBMIT_CONTEXT_RE = re.compile(r"(?i)\b(афиш\w*|анонс\w*|мероприяти\w*|событи\w*|добавить|прислать|опубликовать|партн[её]рств\w*)\b")
_BADGE_CONTEXT_RE = re.compile(r"(?i)\b(пушкинск\w*|бесплатн\w*|дет\w*|семейн\w*|льгот\w*|инвалид\w*|доступн\w*|запись|трансляц\w*)\b")


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


def _text_quality_features(text: str) -> dict[str, Any]:
    compact = normalize_comment_text(text)
    tokens = _TOKEN_RE.findall(compact)
    question_signal = bool(_QUESTION_SIGNAL_RE.search(compact))
    offer_signal = bool(_OFFER_NOISE_RE.search(compact))
    link_signal = bool(_URL_OR_MENTION_RE.search(compact))
    # URLs/mentions without a question are commonly ads, cross-posts or source
    # announcements. Keep them in artifacts, but do not spend LLM/reply budget.
    link_offer_signal = link_signal and not question_signal and len(tokens) >= 6
    too_short_statement = len(tokens) <= 2 and not question_signal
    if offer_signal:
        noise_type = "explicit_offer_or_ad"
    elif link_offer_signal:
        noise_type = "link_or_crosspost_without_question"
    elif too_short_statement:
        noise_type = "too_short_non_question"
    elif not question_signal:
        noise_type = "non_question_statement"
    else:
        noise_type = ""
    hard_noise = noise_type in {"explicit_offer_or_ad", "link_or_crosspost_without_question", "too_short_non_question"}
    return {
        "question_signal": question_signal,
        "offer_signal": offer_signal,
        "link_signal": link_signal,
        "noise_type": noise_type,
        "hard_noise": hard_noise,
        "token_count": len(tokens),
    }


def _intent_has_text_support(text: str, intent_set: str) -> bool:
    compact = normalize_comment_text(text)
    if intent_set in ROUTE_INTENT_SETS:
        return bool(_ROUTE_CONTEXT_RE.search(compact))
    if intent_set in {"event_far_context", "event_close_question", "event_site_search_or_listing"}:
        return bool(_EVENT_CONTEXT_RE.search(compact))
    if intent_set == "organizer_comment_fit":
        # Generic questions like “как зовут детей?” must not become organizer
        # clarification candidates without event/logistics context.
        return bool(_EVENT_CONTEXT_RE.search(compact))
    if intent_set == "organizer_submission_or_partnership":
        return bool(_ORGANIZER_SUBMIT_CONTEXT_RE.search(compact))
    if intent_set == "badge_filter_need":
        return bool(_BADGE_CONTEXT_RE.search(compact))
    return True


def _apply_text_quality_to_candidate(row: dict[str, Any], *, scoring_method: str) -> None:
    features = _text_quality_features(str(row.get("text") or row.get("text_snapshot") or ""))
    intent_supported = _intent_has_text_support(str(row.get("text") or row.get("text_snapshot") or ""), str(row.get("intent_set") or ""))
    raw_score = float(row.get(scoring_method) or row.get("score") or 0.0)
    relation = str(row.get("relation") or "").strip()
    source_post_relation = relation in {"vk_social_wall_post"} or bool(row.get("is_post"))
    source_post_allowed = truthy(os.getenv("ACQ_ALLOW_SOURCE_POST_REPLY_CANDIDATES"))
    if features["hard_noise"]:
        penalty = 0.35
    elif source_post_relation and not source_post_allowed:
        penalty = 0.28
    elif not intent_supported:
        penalty = 0.18
    elif not features["question_signal"]:
        penalty = 0.08
    else:
        penalty = 0.0
    boost = 0.04 if features["question_signal"] else 0.0
    score_for_rank = raw_score + boost - penalty
    row["raw_score"] = raw_score
    row["question_boost"] = boost
    row["noise_penalty"] = penalty
    row["score_for_rank"] = score_for_rank
    row["question_signal"] = bool(features["question_signal"])
    row["candidate_noise_type"] = features["noise_type"]
    if source_post_relation and not source_post_allowed:
        row["candidate_noise_type"] = "source_post_not_comment"
    elif not intent_supported:
        row["candidate_noise_type"] = "intent_without_text_support"
    row["intent_text_supported"] = bool(intent_supported)
    row["pre_llm_candidate_eligible"] = bool(
        features["question_signal"]
        and not features["hard_noise"]
        and intent_supported
        and (not source_post_relation or source_post_allowed)
    )


def _parse_created_at(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _period_stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    dates = [dt for dt in (_parse_created_at(r.get("created_at")) for r in records) if dt is not None]
    if not dates:
        return {
            "period_min_created_at": None,
            "period_max_created_at": None,
            "period_days": None,
            "period_label": "unknown_period",
        }
    start = min(dates)
    end = max(dates)
    seconds = max(0.0, (end - start).total_seconds())
    # A single-day/newest-N sample is still a sample period, not infinite rate.
    days = max(1.0, seconds / 86400.0)
    return {
        "period_min_created_at": start.isoformat(),
        "period_max_created_at": end.isoformat(),
        "period_days": round(days, 3),
        "period_label": f"{start.date().isoformat()}..{end.date().isoformat()} ({round(days, 1)}d)",
    }


def _latest_n_records(records: list[dict[str, Any]], *, limit: int = 100) -> list[dict[str, Any]]:
    dated: list[tuple[datetime, dict[str, Any]]] = []
    undated: list[dict[str, Any]] = []
    for record in records:
        dt = _parse_created_at(record.get("created_at"))
        if dt is None:
            undated.append(record)
        else:
            dated.append((dt, record))
    dated.sort(key=lambda item: item[0], reverse=True)
    ordered = [record for _dt, record in dated] + undated
    return ordered[: max(1, int(limit))]


def _rate(count: int | float, period_days: Any, multiplier: float) -> float | None:
    try:
        days = float(period_days)
    except Exception:
        return None
    if days <= 0:
        return None
    return round(float(count) / days * multiplier, 3)


def _per_100(count: int | float, total: int | float) -> float | None:
    try:
        denominator = float(total)
    except Exception:
        return None
    if denominator <= 0:
        return None
    return round(float(count) / denominator * 100.0, 3)


def _truthy_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _question_pattern_label(text: str, *, action_type: str = "", intent_set: str = "") -> str:
    compact = normalize_comment_text(text).casefold()
    if not compact:
        return "other_question"
    if "пушкин" in compact:
        return "event_badge_pushkin_card"
    if re.search(r"\b(бесплатн|свободн|льгот|инвалид|доступн)\w*", compact):
        return "event_badge_access_or_free"
    if re.search(r"\b(куда\s+(?:съездить|поехать)|что\s+посмотреть|маршрут|на\s+один\s+день|выходн)", compact):
        if re.search(r"\b(дет|реб[её]н|семь|семейн)\w*", compact):
            return "route_with_children"
        if re.search(r"\b(электрич|поезд|автобус|без\s+машин|машин)\w*", compact):
            return "route_transport_or_car"
        return "route_where_to_go"
    if re.search(r"\b(как\s+добраться|доехать|ехать|транспорт)\b", compact):
        return "route_transport_or_car"
    if re.search(r"\b(куда|где).{0,20}(афиш|событ|мероприят|выставк|концерт)", compact):
        return "event_site_search"
    if re.search(r"\b(добавить|прислать|опубликовать|партн[её]рств)\w*.*\b(афиш|анонс|событ|мероприят)", compact):
        return "organizer_submission"
    if re.search(r"\b(билет|стоимост|цена|сколько\s+стоит|вход)\w*", compact):
        return "event_ticket_or_price"
    if re.search(r"\b(регистрац|запис|мест[ао]|остал)\w*", compact):
        return "event_registration_or_seats"
    if re.search(r"\b(возраст|лет|дет|реб[её]н)\w*", compact):
        return "event_age_or_children"
    if re.search(r"\b(во\s+сколько|когда|начал|длит|программ|расписан)\w*", compact):
        return "event_time_schedule_program"
    if re.search(r"\b(где|адрес|локац|место|вход|встреч)\w*", compact):
        return "event_location_or_entry"
    if re.search(r"\b(запись|трансляц|онлайн|стрим)\w*", compact):
        return "event_recording_or_stream"
    if action_type == "trip_route_poi_recommendation" or intent_set.startswith("route_poi"):
        return "route_other"
    if action_type == "organizer_submission_or_partnership":
        return "organizer_submission"
    if action_type == "badge_filter_need":
        return "event_badge_other"
    if action_type == "event_site_search_or_listing":
        return "event_site_search"
    if str(intent_set) in {"event_close_question", "organizer_comment_fit"}:
        return "event_logistics_other"
    return "other_question"


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
    rows.sort(key=lambda r: float(r.get("score_for_rank") if r.get("score_for_rank") is not None else (r.get(scoring_method) or r.get("score") or 0.0)), reverse=True)
    for idx, row in enumerate(rows, start=1):
        row["rank_global"] = idx
        row["score"] = float(row.get("score_for_rank") if row.get("score_for_rank") is not None else (row.get(scoring_method) or 0.0))
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
        scores = [float(r.get("score") if r.get("score") is not None else (r.get(scoring_method) or 0.0)) for r in intent_rows]
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
        examples = [r.get("comment_id") for r in sorted(intent_rows, key=lambda r: float(r.get("score") if r.get("score") is not None else (r.get(scoring_method) or 0.0)), reverse=True)[:3]]
        semantic_presence[intent_set] = {
            "present": level != "none",
            "level": level,
            "top_score_p95": round(p95, 6),
            "candidate_count_top3pct": int(candidate_count_top3pct),
            "example_comment_ids": [str(x) for x in examples if x is not None],
        }
    comments_total = len(surface_records)
    actionable = sum(1 for r in rows if str(r.get("funnel_bucket")) in {"top_0_5pct", "top_1pct", "top_3pct"})
    eligible_questions = sum(1 for r in rows if r.get("pre_llm_candidate_eligible"))
    if actionable >= 3 and dominant:
        decision = "monitor"
        reason = f"{actionable} top question-like semantic candidates; interests={', '.join(dominant)}"
    elif dominant and comments_total >= 5:
        decision = "sample_more"
        reason = f"semantic signal present but only {actionable} top candidates"
    elif dominant:
        decision = "low_priority"
        reason = "weak semantic signal in a small sample"
    else:
        decision = "reject"
        reason = "no acquisition-relevant semantic signal in embedded comments"
    period = _period_stats(surface_records)
    latest_100_records = _latest_n_records(surface_records, limit=100)
    latest_100_period = _period_stats(latest_100_records)
    relation_counts = Counter(str(r.get("relation") or "unknown") for r in surface_records)
    author_ids = {
        str(r.get("author_id") or "").strip()
        for r in surface_records
        if str(r.get("author_id") or "").strip()
    }
    return {
        "surface_key": surface_key,
        "platform": surface_records[0].get("platform") if surface_records else None,
        "surface_type": surface_records[0].get("surface_type") if surface_records else None,
        "comments_total": comments_total,
        "comments_embedded": comments_total,
        "eligible_question_candidates": eligible_questions,
        "period": {
            "min_created_at": period["period_min_created_at"],
            "max_created_at": period["period_max_created_at"],
        },
        "period_min_created_at": period["period_min_created_at"],
        "period_max_created_at": period["period_max_created_at"],
        "period_days": period["period_days"],
        "period_label": period["period_label"],
        "comments_per_day": _rate(comments_total, period["period_days"], 1),
        "comments_per_week": _rate(comments_total, period["period_days"], 7),
        "comments_per_30d": _rate(comments_total, period["period_days"], 30),
        "comments_per_90d": _rate(comments_total, period["period_days"], 90),
        "latest_100_comments": len(latest_100_records),
        "latest_100_min_created_at": latest_100_period["period_min_created_at"],
        "latest_100_max_created_at": latest_100_period["period_max_created_at"],
        "latest_100_period_days": latest_100_period["period_days"],
        "latest_100_period_label": latest_100_period["period_label"],
        "unique_commenters": len(author_ids) if author_ids else None,
        "unique_commenters_observed": bool(author_ids),
        "relation_counts": dict(relation_counts),
        "comment_records": sum(count for rel, count in relation_counts.items() if rel != "vk_social_wall_post"),
        "source_post_records": int(relation_counts.get("vk_social_wall_post") or 0),
        "semantic_presence": semantic_presence,
        "dominant_detected_interests": dominant,
        "monitoring_decision_hint": decision,
        "monitoring_reason": reason,
        "llm_budget_recommendation": {
            "send_top_comments_to_llm": min(actionable, _int_env("ACQ_COMMENT_RETRIEVAL_MAX_LLM_CANDIDATES", 80, min_value=1)),
            "reason": "top retrieval candidates only; no full-comment LLM pass",
        },
    }


def _best_context_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("context_url") or row.get("comment_id") or row.get("text_snapshot") or "")
        if not key:
            continue
        if key not in best or float(row.get("rank_global") or 999999) < float(best[key].get("rank_global") or 999999):
            best[key] = row
    return sorted(best.values(), key=lambda r: float(r.get("rank_global") or 999999))


def _examples_text(rows: list[dict[str, Any]], *, limit: int = 3) -> str:
    examples: list[str] = []
    for row in _best_context_rows(rows)[:limit]:
        text = str(row.get("text_snapshot") or "").replace("\n", " ")[:140]
        url = str(row.get("context_url") or "")
        if url:
            examples.append(f"{text} — {url}")
        else:
            examples.append(text)
    return "\n".join(examples)


def _surface_decision_summaries(
    profiles: list[dict[str, Any]],
    *,
    eligible_rows_by_surface: dict[str, list[dict[str, Any]]],
    all_gate_rows_by_surface: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for profile in profiles:
        surface_key = str(profile.get("surface_key") or "")
        eligible = _best_context_rows(eligible_rows_by_surface.get(surface_key, []))
        all_rows = _best_context_rows(all_gate_rows_by_surface.get(surface_key, []))
        route_rows = [r for r in eligible if r.get("candidate_action_type") == "trip_route_poi_recommendation"]
        event_rows = [r for r in eligible if r.get("candidate_action_type") == "event_recommendation_reply"]
        organizer_submit_rows = [r for r in eligible if r.get("candidate_action_type") == "organizer_submission_or_partnership"]
        badge_rows = [r for r in eligible if r.get("candidate_action_type") == "badge_filter_need"]
        ask_context_rows = [
            r for r in eligible
            if str(r.get("intent_set")) in {"event_close_question", "organizer_comment_fit"}
        ]
        comment_rows = [r for r in eligible if str(r.get("relation") or "") != "vk_social_wall_post" and not _truthy_value(r.get("is_post"))]
        source_post_rows = [r for r in all_rows if str(r.get("relation") or "") == "vk_social_wall_post" or _truthy_value(r.get("is_post"))]
        noise_rows = [r for r in all_rows if str(r.get("candidate_noise_type") or "")]
        answerable_count = len(route_rows) + len(event_rows) + len(organizer_submit_rows) + len(badge_rows)
        ask_count = len(ask_context_rows)
        if answerable_count >= 3 and ask_count >= 2:
            recommendation = "both_monitor_replies_and_ask_clarifications"
        elif answerable_count >= 3:
            recommendation = "monitor_for_reply_opportunities"
        elif ask_count >= 2:
            recommendation = "ask_organizer_clarification_questions"
        elif answerable_count or ask_count:
            recommendation = "sample_more"
        else:
            recommendation = "reject_or_low_priority"
        parts: list[str] = []
        if route_rows:
            parts.append(f"route/POI вопросов: {len(route_rows)}")
        if event_rows:
            parts.append(f"event-вопросов: {len(event_rows)}")
        if organizer_submit_rows:
            parts.append(f"organizer submission вопросов: {len(organizer_submit_rows)}")
        if badge_rows:
            parts.append(f"badge/filter вопросов: {len(badge_rows)}")
        if ask_context_rows:
            parts.append(f"паттернов для уточняющих вопросов организаторам: {len(ask_context_rows)}")
        if noise_rows:
            parts.append(f"отфильтровано рекламных/не-вопросных сигналов: {len(noise_rows)}")
        summary_ru = "; ".join(parts) if parts else "полезных вопросных сигналов не найдено"
        out.append({
            "surface_key": surface_key,
            "platform": profile.get("platform"),
            "surface_type": profile.get("surface_type"),
            "surface_title": profile.get("surface_title"),
            "surface_url": profile.get("surface_url"),
            "members_or_subscribers": profile.get("members_or_subscribers"),
            "period_min_created_at": profile.get("period_min_created_at"),
            "period_max_created_at": profile.get("period_max_created_at"),
            "period_days": profile.get("period_days"),
            "period_label": profile.get("period_label"),
            "unique_commenters": profile.get("unique_commenters"),
            "unique_commenters_note": "" if profile.get("unique_commenters_observed") else "not_collected_in_this_run",
            "comments_total": profile.get("comments_total"),
            "comments_embedded": profile.get("comments_embedded"),
            "comments_per_day": profile.get("comments_per_day"),
            "comments_per_week": profile.get("comments_per_week"),
            "comments_per_30d": profile.get("comments_per_30d"),
            "comments_per_90d": profile.get("comments_per_90d"),
            "latest_100_comments": profile.get("latest_100_comments"),
            "latest_100_min_created_at": profile.get("latest_100_min_created_at"),
            "latest_100_max_created_at": profile.get("latest_100_max_created_at"),
            "latest_100_period_days": profile.get("latest_100_period_days"),
            "latest_100_period_label": profile.get("latest_100_period_label"),
            "recommendation": recommendation,
            "summary_ru": summary_ru,
            "answerable_question_candidates": answerable_count,
            "ask_clarification_contexts": ask_count,
            "eligible_comment_contexts": len(comment_rows),
            "source_post_contexts": len(source_post_rows),
            "route_poi_questions": len(route_rows),
            "event_questions": len(event_rows),
            "organizer_submission_questions": len(organizer_submit_rows),
            "badge_filter_questions": len(badge_rows),
            "answerable_questions_per_30d": _rate(answerable_count, profile.get("period_days"), 30),
            "answerable_questions_per_90d": _rate(answerable_count, profile.get("period_days"), 90),
            "answerable_questions_per_100_comments": _per_100(answerable_count, profile.get("comments_embedded") or 0),
            "ask_contexts_per_30d": _rate(ask_count, profile.get("period_days"), 30),
            "relation_counts_json": json.dumps(profile.get("relation_counts") or {}, ensure_ascii=False),
            "filtered_noise_contexts": len(noise_rows),
            "dominant_detected_interests": ",".join(profile.get("dominant_detected_interests") or []),
            "monitoring_decision_hint": profile.get("monitoring_decision_hint"),
            "monitoring_reason": profile.get("monitoring_reason"),
            "answerable_examples": _examples_text([*route_rows[:2], *event_rows[:2], *organizer_submit_rows[:1], *badge_rows[:1]], limit=4),
            "ask_question_examples": _examples_text(ask_context_rows, limit=3),
        })
    return sorted(out, key=lambda r: (
        str(r.get("recommendation")) not in {"both_monitor_replies_and_ask_clarifications", "monitor_for_reply_opportunities", "ask_organizer_clarification_questions"},
        -int(r.get("answerable_question_candidates") or 0),
        -int(r.get("ask_clarification_contexts") or 0),
        str(r.get("surface_key") or ""),
    ))


def _build_question_patterns(rows: list[dict[str, Any]], *, limit_examples: int = 3) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in _best_context_rows([r for r in rows if r.get("pre_llm_candidate_eligible")]):
        text = str(row.get("text_snapshot") or row.get("text") or "")
        pattern = _question_pattern_label(
            text,
            action_type=str(row.get("candidate_action_type") or ""),
            intent_set=str(row.get("intent_set") or ""),
        )
        surface_key = str(row.get("surface_key") or "")
        action = str(row.get("candidate_action_type") or "")
        key = (surface_key, pattern, action)
        item = grouped.setdefault(key, {
            "surface_key": surface_key,
            "platform": row.get("platform"),
            "surface_type": row.get("surface_type"),
            "pattern": pattern,
            "candidate_action_type": action,
            "intent_sets": set(),
            "models": set(),
            "count": 0,
            "example_texts": [],
            "example_urls": [],
        })
        item["count"] += 1
        if row.get("intent_set"):
            item["intent_sets"].add(str(row.get("intent_set")))
        if row.get("model_name"):
            item["models"].add(str(row.get("model_name")))
        if len(item["example_texts"]) < limit_examples and text:
            item["example_texts"].append(text[:220])
        if len(item["example_urls"]) < limit_examples and row.get("context_url"):
            item["example_urls"].append(str(row.get("context_url")))
    out: list[dict[str, Any]] = []
    for item in grouped.values():
        out.append({
            **{k: v for k, v in item.items() if k not in {"intent_sets", "models", "example_texts", "example_urls"}},
            "intent_sets": ",".join(sorted(item["intent_sets"])),
            "models": ",".join(sorted(item["models"])),
            "example_texts": "\n".join(item["example_texts"]),
            "example_urls": "\n".join(item["example_urls"]),
        })
    return sorted(out, key=lambda r: (-int(r.get("count") or 0), str(r.get("surface_key") or ""), str(r.get("pattern") or "")))


def _model_example_rows(candidates: list[dict[str, Any]], model_name: str, *, limit: int = 200) -> list[dict[str, Any]]:
    rows = [r for r in candidates if str(r.get("model_name") or "") == model_name and r.get("pre_llm_candidate_eligible")]
    return _best_context_rows(rows)[:limit]


def _scope_rows(
    *,
    run_id: str,
    records: list[dict[str, Any]],
    profiles: list[dict[str, Any]],
    models: list[str],
    gate_model: str,
    scoring_method: str,
    summary_note: str | None = None,
) -> list[dict[str, Any]]:
    period = _period_stats(records)
    platforms = Counter(str(r.get("platform") or "unknown") for r in records)
    surface_types = Counter(str(r.get("surface_type") or "unknown") for r in records)
    relations = Counter(str(r.get("relation") or "unknown") for r in records)
    return [
        {"metric": "run_id", "value": run_id},
        {"metric": "stage", "value": STAGE_NAME},
        {"metric": "scope_note", "value": summary_note or "Limited to the Kaggle seed payload and configured per-run budgets; not a full historical DB scan unless the payload/budgets covered it."},
        {"metric": "models", "value": ", ".join(models)},
        {"metric": "gate_model_for_llm_budget", "value": gate_model},
        {"metric": "scoring_method", "value": scoring_method},
        {"metric": "comments_after_filter", "value": len(records)},
        {"metric": "surfaces_profiled", "value": len(profiles)},
        {"metric": "period_min_created_at", "value": period["period_min_created_at"]},
        {"metric": "period_max_created_at", "value": period["period_max_created_at"]},
        {"metric": "period_days", "value": period["period_days"]},
        {"metric": "platform_comment_records_json", "value": json.dumps(dict(platforms), ensure_ascii=False)},
        {"metric": "surface_type_records_json", "value": json.dumps(dict(surface_types), ensure_ascii=False)},
        {"metric": "relation_records_json", "value": json.dumps(dict(relations), ensure_ascii=False)},
        {"metric": "ACQ_MAX_SURFACES_PER_RUN", "value": os.getenv("ACQ_MAX_SURFACES_PER_RUN", "")},
        {"metric": "ACQ_MAX_MESSAGES_PER_SURFACE", "value": os.getenv("ACQ_MAX_MESSAGES_PER_SURFACE", "")},
        {"metric": "ACQ_MAX_THREADS_PER_SURFACE", "value": os.getenv("ACQ_MAX_THREADS_PER_SURFACE", "")},
        {"metric": "ACQ_MAX_VK_SURFACES_PER_RUN", "value": os.getenv("ACQ_MAX_VK_SURFACES_PER_RUN", "")},
        {"metric": "ACQ_MAX_VK_POSTS_PER_SURFACE", "value": os.getenv("ACQ_MAX_VK_POSTS_PER_SURFACE", "")},
        {"metric": "ACQ_MAX_VK_COMMENTS_PER_POST", "value": os.getenv("ACQ_MAX_VK_COMMENTS_PER_POST", "")},
    ]


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_xlsx(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    surface_summaries: list[dict[str, Any]] | None = None,
    model_examples: dict[str, list[dict[str, Any]]] | None = None,
    question_patterns: list[dict[str, Any]] | None = None,
    scope_rows: list[dict[str, Any]] | None = None,
) -> None:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
        from openpyxl.utils import get_column_letter
    except Exception:
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "manual_review"
    headers = [
        "label", "action_class", "is_actionable_reply_opportunity", "false_positive_type", "model_disagreement_bucket",
        "model_name", "intent_set", "score", "rank_global", "rank_within_surface", "surface_key", "platform", "surface_type",
        "relation", "is_post", "author_id", "created_at", "context_url", "text_snapshot", "top_intent_phrase", "positive_score", "negative_score", "funnel_bucket",
        "destination_hint", "transport_hint", "question_signal", "candidate_noise_type", "intent_text_supported", "pre_llm_candidate_eligible",
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
    for idx, width in enumerate([12, 28, 28, 24, 28, 34, 28, 12, 12, 16, 28, 10, 18, 18, 10, 16, 22, 45, 70, 55, 14, 14, 14, 22, 16, 14, 24, 20, 24], start=1):
        ws.column_dimensions[get_column_letter(idx)].width = width
    ws.freeze_panes = "A2"

    if surface_summaries:
        surf = wb.create_sheet("surface_summary")
        surf_headers = [
            "recommendation", "surface_key", "platform", "surface_type", "surface_title", "surface_url",
            "members_or_subscribers", "period_min_created_at", "period_max_created_at", "period_days", "unique_commenters",
            "comments_total", "comments_embedded", "comments_per_day", "comments_per_week", "comments_per_30d", "comments_per_90d",
            "latest_100_comments", "latest_100_min_created_at", "latest_100_max_created_at", "latest_100_period_days",
            "summary_ru", "answerable_question_candidates", "answerable_questions_per_30d", "answerable_questions_per_90d",
            "answerable_questions_per_100_comments", "ask_clarification_contexts", "ask_contexts_per_30d", "eligible_comment_contexts", "source_post_contexts",
            "route_poi_questions", "event_questions", "organizer_submission_questions", "badge_filter_questions",
            "filtered_noise_contexts", "relation_counts_json", "unique_commenters_note", "answerable_examples", "ask_question_examples",
        ]
        surf.append(surf_headers)
        for cell in surf[1]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="D9EAF7")
        for row in surface_summaries:
            surf.append([row.get(h) for h in surf_headers])
            url_cell = surf.cell(surf.max_row, surf_headers.index("surface_url") + 1)
            if row.get("surface_url"):
                url_cell.hyperlink = str(row.get("surface_url"))
                url_cell.style = "Hyperlink"
        surf.freeze_panes = "A2"
        for idx, width in enumerate([34, 30, 10, 18, 30, 42, 16, 22, 22, 12, 18, 14, 14, 14, 14, 14, 14, 14, 22, 22, 14, 70, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 60, 24, 90, 90], start=1):
            surf.column_dimensions[get_column_letter(idx)].width = width

    for model_name, example_rows in (model_examples or {}).items():
        model_key = str(model_name).lower()
        if "multilingual-e5" in model_key:
            sheet_name = "eligible_e5_base"
        elif "bge-m3" in model_key:
            sheet_name = "eligible_bge_m3"
        else:
            sheet_name = ("eligible_" + re.sub(r"[^A-Za-z0-9]+", "_", model_name).strip("_"))[:31] or "eligible_model"
        ex = wb.create_sheet(sheet_name)
        ex_headers = [
            "model_name", "score", "rank_global", "rank_within_surface", "surface_key", "platform", "surface_type",
            "relation", "is_post", "created_at", "intent_set", "candidate_action_type", "candidate_noise_type",
            "context_url", "text_snapshot", "top_intent_phrase", "destination_hint", "transport_hint",
        ]
        ex.append(ex_headers)
        for cell in ex[1]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="D9EAF7")
        for row in example_rows:
            ex.append([row.get(h) for h in ex_headers])
            url_cell = ex.cell(ex.max_row, ex_headers.index("context_url") + 1)
            if row.get("context_url"):
                url_cell.hyperlink = str(row.get("context_url"))
                url_cell.style = "Hyperlink"
        ex.freeze_panes = "A2"
        for idx, width in enumerate([34, 12, 12, 16, 30, 10, 18, 18, 10, 22, 28, 30, 24, 45, 80, 55, 22, 18], start=1):
            ex.column_dimensions[get_column_letter(idx)].width = width

    if question_patterns:
        qp = wb.create_sheet("question_patterns")
        qp_headers = [
            "surface_key", "platform", "surface_type", "pattern", "candidate_action_type",
            "count", "intent_sets", "models", "example_texts", "example_urls",
        ]
        qp.append(qp_headers)
        for cell in qp[1]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="D9EAF7")
        for row in question_patterns:
            qp.append([row.get(h) for h in qp_headers])
        qp.freeze_panes = "A2"
        for idx, width in enumerate([30, 10, 18, 28, 34, 10, 45, 45, 90, 70], start=1):
            qp.column_dimensions[get_column_letter(idx)].width = width

    if scope_rows:
        scope = wb.create_sheet("scope")
        scope.append(["metric", "value"])
        for cell in scope[1]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="D9EAF7")
        for row in scope_rows:
            scope.append([row.get("metric"), row.get("value")])
        scope.freeze_panes = "A2"
        scope.column_dimensions["A"].width = 36
        scope.column_dimensions["B"].width = 120

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


def _report_md(
    summary: dict[str, Any],
    profiles: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    speed_rows: list[dict[str, Any]],
    surface_summaries: list[dict[str, Any]] | None = None,
) -> str:
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
    if surface_summaries:
        lines.extend(["", "## Surface decision summary", "| recommendation | surface | comments | summary |", "| --- | --- | ---: | --- |"])
        for row in surface_summaries[:20]:
            lines.append(f"| {row.get('recommendation')} | {row.get('surface_key')} | {row.get('comments_embedded')} | {str(row.get('summary_ru') or '').replace('|', ' ')} |")
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
                enriched["scoring_method"] = scoring_method
                enriched["candidate_action_type"] = _action_for_intent(str(row.get("intent_set")))
                enriched["target_hint"] = _route_target_hint(str(rec.get("text") or ""), str(row.get("intent_set")))
                enriched["destination_hint"] = enriched["target_hint"].get("destination_hint")
                enriched["transport_hint"] = enriched["target_hint"].get("transport_hint")
                _apply_text_quality_to_candidate(enriched, scoring_method=scoring_method)
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
    gate_candidates = [c for c in all_candidates if c.get("model_name") == gate_model and c.get("pre_llm_candidate_eligible")]
    gate_candidates = _rank_candidates(gate_candidates, scoring_method=scoring_method)[:max_llm_candidates]

    progress_callback("surface_profile", {"surfaces": len({r.get('surface_key') for r in records}), "progress_percent": 78})
    profiles: list[dict[str, Any]] = []
    rows_by_surface: dict[str, list[dict[str, Any]]] = defaultdict(list)
    all_gate_rows_by_surface: dict[str, list[dict[str, Any]]] = defaultdict(list)
    records_by_surface: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in all_candidates:
        # Profiles are based on selected gate model to avoid double-counting two benchmark models.
        if row.get("model_name") == gate_model:
            all_gate_rows_by_surface[str(row.get("surface_key") or "unknown")].append(row)
            if row.get("pre_llm_candidate_eligible"):
                rows_by_surface[str(row.get("surface_key") or "unknown")].append(row)
    for rec in records:
        records_by_surface[str(rec.get("surface_key") or "unknown")].append(rec)
    for surface_key, surface_records in records_by_surface.items():
        profile = _surface_profile(surface_key, surface_records, rows_by_surface.get(surface_key, []), scoring_method=scoring_method)
        if surfaces_by_external and surface_key in surfaces_by_external:
            s = surfaces_by_external[surface_key]
            profile["surface_title"] = s.get("title")
            profile["surface_url"] = s.get("url")
            reach = s.get("reach") if isinstance(s.get("reach"), dict) else {}
            profile["members_or_subscribers"] = (
                reach.get("members")
                or reach.get("members_count")
                or reach.get("subscribers")
                or reach.get("participants")
                or None
            )
        profiles.append(profile)
    surface_summaries = _surface_decision_summaries(
        profiles,
        eligible_rows_by_surface=rows_by_surface,
        all_gate_rows_by_surface=all_gate_rows_by_surface,
    )
    all_eligible_rows = [c for c in all_candidates if c.get("pre_llm_candidate_eligible")]
    question_patterns = _build_question_patterns(all_eligible_rows)
    model_examples = {model_name: _model_example_rows(all_candidates, model_name) for model_name in models}
    run_id = os.getenv("KAGGLE_RUN_ID") or f"acq-comment-retrieval-{int(time.time())}"
    scope_rows = _scope_rows(
        run_id=run_id,
        records=records,
        profiles=profiles,
        models=models,
        gate_model=gate_model,
        scoring_method=scoring_method,
    )

    artifact_prefix = os.getenv("ACQ_COMMENT_RETRIEVAL_ARTIFACT_PREFIX") or "comment_retrieval"
    candidates_csv = output_path / f"{artifact_prefix}_candidates.csv"
    profiles_csv = output_path / f"{artifact_prefix}_surface_profiles.csv"
    surface_summary_csv = output_path / f"{artifact_prefix}_surface_decision_summary.csv"
    question_patterns_csv = output_path / f"{artifact_prefix}_question_patterns.csv"
    distributions_csv = output_path / f"{artifact_prefix}_score_distributions.csv"
    speed_csv = output_path / f"{artifact_prefix}_speed_metrics.csv"
    manual_xlsx = output_path / f"{artifact_prefix}_manual_review_sample.xlsx"
    report_md = output_path / f"{artifact_prefix}_report.md"
    summary_json = output_path / "acq_comment_retrieval_run_summary.json"

    candidate_fields = [
        "run_id", "surface_key", "platform", "surface_type", "relation", "is_post", "author_id", "retrieval", "search_query",
        "context_url", "comment_id", "post_id", "topic_id", "thread_id",
        "created_at", "text_snapshot", "model_name", "max_length", "batch_size", "intent_set", "score", "positive_score", "negative_score",
        "scoring_method", "raw_score", "question_boost", "noise_penalty", "top_intent_phrase", "top_intent_score", "rank_global", "rank_within_surface",
        "funnel_bucket", "candidate_action_type", "destination_hint", "transport_hint", "question_signal", "candidate_noise_type",
        "intent_text_supported", "pre_llm_candidate_eligible", "model_disagreement_bucket",
    ]
    _write_csv(candidates_csv, all_candidates, candidate_fields)
    _write_csv(profiles_csv, [{**p, "semantic_presence": json.dumps(p.get("semantic_presence"), ensure_ascii=False), "dominant_detected_interests": ",".join(p.get("dominant_detected_interests") or [])} for p in profiles], [
        "surface_key", "platform", "surface_type", "surface_title", "surface_url", "members_or_subscribers",
        "period_min_created_at", "period_max_created_at", "period_days", "period_label", "unique_commenters",
        "comments_total", "comments_embedded", "comment_records", "source_post_records",
        "comments_per_day", "comments_per_week", "comments_per_30d", "comments_per_90d",
        "latest_100_comments", "latest_100_min_created_at", "latest_100_max_created_at", "latest_100_period_days", "latest_100_period_label",
        "eligible_question_candidates", "monitoring_decision_hint", "monitoring_reason", "dominant_detected_interests", "semantic_presence",
    ])
    _write_csv(surface_summary_csv, surface_summaries, [
        "surface_key", "platform", "surface_type", "surface_title", "surface_url", "members_or_subscribers",
        "period_min_created_at", "period_max_created_at", "period_days", "period_label", "unique_commenters", "unique_commenters_note",
        "comments_total", "comments_embedded", "comments_per_day", "comments_per_week", "comments_per_30d", "comments_per_90d",
        "latest_100_comments", "latest_100_min_created_at", "latest_100_max_created_at", "latest_100_period_days", "latest_100_period_label",
        "recommendation", "summary_ru", "answerable_question_candidates", "answerable_questions_per_30d",
        "answerable_questions_per_90d", "answerable_questions_per_100_comments", "ask_clarification_contexts",
        "ask_contexts_per_30d", "eligible_comment_contexts", "source_post_contexts",
        "route_poi_questions", "event_questions", "organizer_submission_questions", "badge_filter_questions",
        "filtered_noise_contexts", "relation_counts_json", "dominant_detected_interests",
        "monitoring_decision_hint", "monitoring_reason", "answerable_examples", "ask_question_examples",
    ])
    _write_csv(question_patterns_csv, question_patterns, [
        "surface_key", "platform", "surface_type", "pattern", "candidate_action_type", "count", "intent_sets", "models", "example_texts", "example_urls",
    ])
    _write_csv(distributions_csv, distributions, ["model_name", "scoring_method", "count", "p50", "p90", "p95", "p99", "max"])
    _write_csv(speed_csv, speed_rows, ["model_name", "device", "batch_size", "max_length", "comments_total", "intent_embedding_sec", "comment_embedding_sec", "scoring_sec", "total_sec", "comments_per_sec", "comments_per_hour", "peak_ram_mb", "cpu"])
    manual_rows = _select_manual_rows(all_candidates, max_rows=max_manual_rows)
    _write_xlsx(
        manual_xlsx,
        manual_rows,
        surface_summaries=surface_summaries,
        model_examples=model_examples,
        question_patterns=question_patterns,
        scope_rows=scope_rows,
    )

    summary = {
        "stage": STAGE_NAME,
        "run_id": run_id,
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
            "surface_decision_summary_csv": str(surface_summary_csv),
            "question_patterns_csv": str(question_patterns_csv),
            "score_distributions_csv": str(distributions_csv),
            "speed_metrics_csv": str(speed_csv),
            "manual_review_xlsx": str(manual_xlsx),
            "report_md": str(report_md),
        },
        "total_sec": round(time.perf_counter() - started, 4),
    }
    report_md.write_text(_report_md(summary, profiles, gate_candidates, speed_rows, surface_summaries), encoding="utf-8")
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    progress_callback("complete", {"comments_embedded": len(records), "candidate_count": len(all_candidates), "progress_percent": 100})
    return {
        "summary": summary,
        "surface_profiles": profiles,
        "surface_decision_summaries": surface_summaries,
        "candidates": all_candidates,
        "llm_gate_candidates": gate_candidates,
        "artifacts": summary["artifacts"],
    }
