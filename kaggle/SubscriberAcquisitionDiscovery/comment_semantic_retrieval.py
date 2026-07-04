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
        "вопрос про аренду квартиры, покупку жилья, ипотеку или риэлтора без связи с событием или маршрутом",
        "вопрос про врача, клинику, лечение, анализы или запись к врачу без связи с событием или маршрутом",
        "обсуждение транспорта без связи с поездкой к месту",
        "обсуждение погоды без связи с поездкой или мероприятием",
        "объявление о потерянном телефоне, найденных вещах или пропаже без связи с событием",
        "обсуждение цены бензина, дизеля, топлива или заправок без маршрута поездки",
        "обсуждение выборов, политики или чиновников без вопроса о событии",
        "реклама товаров с маркетплейса, артикулом, wildberries или ссылкой на товар",
        "реклама помощи студентам, учебных работ, экзаменов, зачётов или услуг",
        "реклама психологических, эзотерических или системных расстановок без события",
        "жалоба на ремонт дороги, трамвай, остановку или городское благоустройство без события",
        "новостной пост о погоде, температуре моря или происшествии без вопроса пользователя",
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
_REAL_ESTATE_SCOPE_RE = re.compile(
    r"(?i)\b(недвижимост\w*|квартир\w*|комнат\w*|аренд\w*|снять\s+жиль[её]|сниму|сда[её]тся|"
    r"жк|ипотек\w*|риэлт\w*|новостройк\w*|застройщик\w*)\b"
)
_MEDICINE_SCOPE_RE = re.compile(
    r"(?i)\b(врач\w*|клиник\w*|стоматолог\w*|лор\b|педиатр\w*|терапевт\w*|поликлиник\w*|"
    r"больниц\w*|лечени\w*|анализ\w*|мрт\b|узи\b|при[её]м\s+врач\w*|запис\w*\s+к\s+врач)\b"
)
_SURFACE_MEDICINE_SCOPE_RE = re.compile(
    r"(?i)\b(врач\w*|клиник\w*|стоматолог\w*|педиатр\w*|здоровь\w*|симптом\w*|"
    r"грудн\w*\s+вскармливан\w*|гв\b|лечени\w*|психолог\w*|диет\w*|худе\w*)\b"
)


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


def _float_env(name: str, default: float, *, min_value: float | None = None) -> float:
    try:
        value = float(os.getenv(name) or default)
    except Exception:
        value = float(default)
    if min_value is not None:
        value = max(min_value, value)
    return value


def _max_comment_age_days() -> int:
    return _int_env("ACQ_COMMENT_RETRIEVAL_MAX_COMMENT_AGE_DAYS", 365, min_value=1)


def _stale_activity_days() -> int:
    return _int_env("ACQ_COMMENT_RETRIEVAL_STALE_ACTIVITY_DAYS", 92, min_value=1)


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


def _out_of_scope_noise_type(text: str) -> str:
    compact = normalize_comment_text(text)
    if not compact:
        return ""
    # Keep mixed questions when they also contain explicit route/event context:
    # "снимаем квартиру, куда съездить?" is a route question, but "где снять
    # квартиру?" is out of acquisition scope.
    has_our_context = bool(_ROUTE_CONTEXT_RE.search(compact) or _EVENT_CONTEXT_RE.search(compact) or _ORGANIZER_SUBMIT_CONTEXT_RE.search(compact))
    if has_our_context:
        return ""
    if _REAL_ESTATE_SCOPE_RE.search(compact):
        return "out_of_scope_real_estate"
    if _MEDICINE_SCOPE_RE.search(compact):
        return "out_of_scope_medicine"
    return ""


def _surface_out_of_scope_type(profile: dict[str, Any]) -> str:
    text = " ".join(str(profile.get(key) or "") for key in ["surface_title", "surface_url", "surface_key", "surface_type"])
    if not text:
        return ""
    has_our_context = bool(_ROUTE_CONTEXT_RE.search(text) or _EVENT_CONTEXT_RE.search(text) or _ORGANIZER_SUBMIT_CONTEXT_RE.search(text))
    if has_our_context:
        return ""
    if _REAL_ESTATE_SCOPE_RE.search(text):
        return "out_of_scope_real_estate_surface"
    if _SURFACE_MEDICINE_SCOPE_RE.search(text):
        return "out_of_scope_medicine_surface"
    return ""


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


def _deterministic_prefilter_enabled() -> bool:
    # Default is deliberately false. In the vector-scan funnel every collected
    # comment/post context must be embedded and the LLM gate must receive the
    # top semantic rows, not a regex/question-signal shortlist.
    return truthy(os.getenv("ACQ_COMMENT_RETRIEVAL_DETERMINISTIC_PREFILTER"))


def _apply_text_quality_to_candidate(row: dict[str, Any], *, scoring_method: str) -> None:
    features = _text_quality_features(str(row.get("text") or row.get("text_snapshot") or ""))
    intent_supported = _intent_has_text_support(str(row.get("text") or row.get("text_snapshot") or ""), str(row.get("intent_set") or ""))
    out_of_scope = _out_of_scope_noise_type(str(row.get("text") or row.get("text_snapshot") or ""))
    raw_score = float(row.get(scoring_method) or row.get("score") or 0.0)
    relation = str(row.get("relation") or "").strip()
    source_post_relation = relation in {"vk_social_wall_post", "tg_channel_post_context"} or bool(row.get("is_post"))
    source_post_allowed = truthy(os.getenv("ACQ_ALLOW_SOURCE_POST_REPLY_CANDIDATES"))
    diagnostic_noise_type = features["noise_type"]
    if source_post_relation and not source_post_allowed:
        diagnostic_noise_type = "source_post_context"
    elif out_of_scope:
        diagnostic_noise_type = out_of_scope
    elif not intent_supported:
        diagnostic_noise_type = "intent_without_text_support"
    if not _deterministic_prefilter_enabled():
        row["raw_score"] = raw_score
        row["question_boost"] = 0.0
        row["noise_penalty"] = 0.0
        row["score_for_rank"] = raw_score
        row["question_signal"] = bool(features["question_signal"])
        row["candidate_noise_type"] = diagnostic_noise_type
        row["intent_text_supported"] = bool(intent_supported)
        row["pre_llm_candidate_eligible"] = True
        row["llm_gate_selection_basis"] = "semantic_top_n_no_deterministic_prefilter"
        return
    if features["hard_noise"]:
        penalty = 0.35
    elif out_of_scope:
        penalty = 0.32
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
    elif out_of_scope:
        row["candidate_noise_type"] = out_of_scope
    elif not intent_supported:
        row["candidate_noise_type"] = "intent_without_text_support"
    row["intent_text_supported"] = bool(intent_supported)
    row["pre_llm_candidate_eligible"] = bool(
        features["question_signal"]
        and not features["hard_noise"]
        and not out_of_scope
        and intent_supported
        and (not source_post_relation or source_post_allowed)
    )
    row["llm_gate_selection_basis"] = "legacy_deterministic_prefilter"


def _record_analysis_text(record: dict[str, Any]) -> str:
    """Text embedded for retrieval: comment plus bounded parent/source context."""
    text = normalize_comment_text(str(record.get("text") or record.get("text_snapshot") or ""))
    parent = normalize_comment_text(str(record.get("reply_parent_text_snapshot") or record.get("reply_parent_text") or ""))
    source_post = normalize_comment_text(str(record.get("source_post_text_snapshot") or record.get("source_post_text") or ""))
    source_title = normalize_comment_text(str(record.get("source_context_title") or ""))
    context_parts: list[str] = []
    if source_post and source_post != text:
        context_parts.append(f"Исходный пост/тема: {source_post[:700]}")
    elif source_title:
        context_parts.append(f"Тема/контекст: {source_title[:300]}")
    if parent:
        context_parts.append(f"Родительский комментарий: {parent[:500]}")
    if not context_parts:
        return text
    return ("Текущий комментарий/пост: " + text + "\n\nКонтекст для понимания:\n" + "\n".join(context_parts))[:1800]


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


def _days_since(value: Any, *, now: datetime | None = None) -> float | None:
    dt = _parse_created_at(value)
    if dt is None:
        return None
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return max(0.0, (now - dt).total_seconds() / 86400.0)


def _latest_created_at(rows: Iterable[dict[str, Any]]) -> str | None:
    dates = [dt for dt in (_parse_created_at(row.get("created_at")) for row in rows) if dt is not None]
    if not dates:
        return None
    return max(dates).isoformat()


def _source_run_id_from_env() -> str:
    return (os.getenv("KAGGLE_RUN_ID") or os.getenv("ACQ_SOURCE_RUN_ID") or "").strip()


def _source_run_provenance() -> str:
    if (os.getenv("KAGGLE_RUN_ID") or "").strip():
        return "kaggle_run_id"
    if (os.getenv("ACQ_SOURCE_RUN_ID") or "").strip():
        return "explicit_source_run_id"
    return "missing_run_id_no_increment_claim"


def _record_within_analysis_window(record: dict[str, Any], *, now: datetime | None = None) -> bool:
    dt = _parse_created_at(record.get("created_at"))
    if dt is None:
        # Keep undated rows visible rather than silently losing potentially
        # current data; the report will show unknown dates.
        return True
    days = _days_since(dt, now=now)
    return days is None or days <= _max_comment_age_days()


def _record_usage_scope(record: dict[str, Any], *, now: datetime | None = None) -> str:
    if _record_within_analysis_window(record, now=now):
        return "monitoring_candidate"
    return "historical_calibration"


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


_CANONICAL_QUESTIONS_RU = {
    "route_with_children": "Куда съездить с детьми на один день по Калининградской области?",
    "route_transport_or_car": "Куда поехать из Калининграда на электричке/автобусе/машине и что посмотреть по пути?",
    "route_where_to_go": "Куда съездить или что посмотреть в Калининградской области на один день/выходные?",
    "route_other": "Какой маршрут или место в Калининградской области посоветуете под этот запрос?",
    "event_site_search": "Где посмотреть актуальную афишу, выставки или календарь событий?",
    "organizer_submission": "Куда прислать анонс события или как добавить мероприятие в афишу?",
    "event_ticket_or_price": "Сколько стоит вход/билет и где его купить?",
    "event_registration_or_seats": "Нужна ли регистрация и остались ли места?",
    "event_age_or_children": "Есть ли возрастные ограничения и подходит ли событие детям?",
    "event_time_schedule_program": "Когда начало, сколько длится событие и какая программа?",
    "event_location_or_entry": "Где проходит событие, какой адрес/вход/место встречи?",
    "event_recording_or_stream": "Будет ли запись, трансляция или онлайн-доступ?",
    "event_badge_pushkin_card": "Можно ли посетить событие по Пушкинской карте?",
    "event_badge_access_or_free": "Есть ли бесплатный вход, льготы или условия доступности?",
    "event_badge_other": "Есть ли быстрые признаки события: бесплатно, детям, Пушкинская карта, доступность?",
    "event_logistics_other": "Какой важной практической информации о событии не хватает?",
    "other_question": "Какой полезный ответ можно дать на этот вопрос без рекламы?",
}


def _canonical_question_for_pattern(pattern: str) -> str:
    return _CANONICAL_QUESTIONS_RU.get(str(pattern or ""), _CANONICAL_QUESTIONS_RU["other_question"])


def _is_actual_question_text(text: str) -> bool:
    compact = normalize_comment_text(text)
    if not compact:
        return False
    if "?" in compact:
        return True
    return bool(re.match(
        r"(?i)^(?:а\s+)?(?:подскажите|посоветуйте|скажите|кто\s+знает|где|куда|когда|как|что|сколько|какой|какая|какие|есть\s+ли|будет\s+ли|можно\s+ли|нужна\s+ли)\b",
        compact,
    ))


_REPORT_REJECT_NOISE_TYPES = {
    "explicit_offer_or_ad",
    "link_or_crosspost_without_question",
    "too_short_non_question",
    "intent_without_text_support",
    "source_post_not_comment",
}


def _is_source_post_context(row: dict[str, Any]) -> bool:
    relation = str(row.get("relation") or "")
    return relation in {"vk_social_wall_post", "tg_channel_post_context"} or _truthy_value(row.get("is_post"))


def _row_score(row: dict[str, Any]) -> float:
    for key in ["score", "score_for_rank", "positive_negative_margin"]:
        try:
            return float(row.get(key))
        except Exception:
            continue
    return 0.0


def _report_min_comment_score() -> float:
    return _float_env("ACQ_COMMENT_RETRIEVAL_REPORT_MIN_COMMENT_SCORE", 0.01)


def _report_min_source_post_score() -> float:
    return _float_env("ACQ_COMMENT_RETRIEVAL_REPORT_MIN_SOURCE_POST_SCORE", 0.01)


def _report_noise_rejected(row: dict[str, Any]) -> bool:
    noise = str(row.get("candidate_noise_type") or "").strip()
    return bool(noise in _REPORT_REJECT_NOISE_TYPES or noise.startswith("out_of_scope"))


def _report_candidate_eligible(row: dict[str, Any], *, allow_source_posts: bool = True, allow_historical: bool = False) -> bool:
    """Rows suitable for human-facing summaries/examples.

    This is intentionally *not* the LLM-gate selector.  The default discovery
    gate still receives top-N vector rows without a deterministic prefilter.
    The report/catalog path is stricter so garbage rows do not become
    “canonical questions” or selected-surface evidence.
    """
    scope = str(row.get("candidate_usage_scope") or "")
    if scope == "historical_calibration" and not allow_historical:
        return False
    if scope not in {"", "monitoring_candidate", "historical_calibration"}:
        return False
    text = str(row.get("text_snapshot") or row.get("text") or "")
    features = _text_quality_features(text)
    intent = str(row.get("intent_set") or "")
    action = str(row.get("candidate_action_type") or "")
    intent_supported = bool(_truthy_value(row.get("intent_text_supported")) and _intent_has_text_support(text, intent))
    if _is_source_post_context(row):
        if not allow_source_posts:
            return False
        if intent not in {"organizer_comment_fit", "event_close_question"}:
            return False
        if not intent_supported:
            return False
        return _row_score(row) >= _report_min_source_post_score()
    if _report_noise_rejected(row):
        return False
    if features["hard_noise"] or _out_of_scope_noise_type(text):
        return False
    if not (_truthy_value(row.get("question_signal")) and features["question_signal"]):
        return False
    if not intent_supported:
        return False
    if not _is_actual_question_text(text):
        return False
    if action == "organizer_visibility_clarification" and intent not in {"organizer_comment_fit", "event_close_question"}:
        return False
    return _row_score(row) >= _report_min_comment_score()


def _report_real_question_row(row: dict[str, Any], *, allow_historical: bool = True) -> bool:
    return (
        not _is_source_post_context(row)
        and _report_candidate_eligible(row, allow_source_posts=False, allow_historical=allow_historical)
        and _is_actual_question_text(str(row.get("text_snapshot") or row.get("text") or ""))
    )


def _surface_key(record: dict[str, Any]) -> str:
    return str(record.get("surface_key") or record.get("surface_external_id") or record.get("surface_url") or "unknown")


def _dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
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
        age_days = _days_since(item.get("created_at"), now=now)
        usage_scope = _record_usage_scope(item, now=now)
        item["comment_age_days"] = round(age_days, 1) if age_days is not None else None
        item["is_within_monitoring_window"] = usage_scope == "monitoring_candidate"
        item["candidate_usage_scope"] = usage_scope
        analysis_text = _record_analysis_text(item)
        item["analysis_text"] = analysis_text
        item["analysis_context_snapshot"] = "\n".join(
            part for part in [
                normalize_comment_text(str(item.get("source_post_text_snapshot") or item.get("source_post_text") or item.get("source_context_title") or ""))[:700],
                normalize_comment_text(str(item.get("reply_parent_text_snapshot") or item.get("reply_parent_text") or ""))[:500],
            ]
            if part
        )[:1000]
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
    days_since_latest_activity = _days_since(period.get("period_max_created_at"))
    if days_since_latest_activity is not None and days_since_latest_activity > _stale_activity_days():
        decision = "reject_stale_inactive"
        reason = (
            f"latest observed comment is {days_since_latest_activity:.0f} days old; "
            f"current acquisition requires activity within {_stale_activity_days()} days. "
            "Keep only as low-frequency revival watch."
        )
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
        "latest_comment_at": period["period_max_created_at"],
        "days_since_latest_activity": round(days_since_latest_activity, 1) if days_since_latest_activity is not None else None,
        "stale_activity_days": _stale_activity_days(),
        "analysis_max_comment_age_days": _max_comment_age_days(),
        "freshness_status": "stale_inactive" if days_since_latest_activity is not None and days_since_latest_activity > _stale_activity_days() else "active_or_unknown",
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


def _selection_status_from_recommendation(recommendation: str, *, surface_status: str = "", scan_state: str = "") -> str:
    rec = str(recommendation or "").strip()
    status = str(surface_status or "").strip()
    state = str(scan_state or "").strip()
    if rec in {"both_monitor_replies_and_ask_clarifications", "monitor_for_reply_opportunities", "ask_organizer_clarification_questions"}:
        return "selected"
    if rec in {"sample_more", "low_priority"}:
        return "candidate"
    if rec in {"reject_stale_inactive", "reject_or_low_priority"} or rec.startswith("reject") or rec.startswith("out_of_scope"):
        return "rejected"
    if status.startswith("rejected") or state.startswith("checked_no") or state in {"resolved_no_comments", "checked_inaccessible"}:
        return "rejected"
    if rec == "not_profiled_or_no_comment_signal" and state in {"scanned", "comments_available", "resolved_no_comments", "checked_inaccessible"}:
        return "rejected"
    return "candidate"


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
        comment_eligible = [r for r in eligible if not _is_source_post_context(r)]
        route_rows = [r for r in comment_eligible if r.get("candidate_action_type") == "trip_route_poi_recommendation"]
        event_rows = [r for r in comment_eligible if r.get("candidate_action_type") == "event_recommendation_reply"]
        organizer_submit_rows = [r for r in comment_eligible if r.get("candidate_action_type") == "organizer_submission_or_partnership"]
        badge_rows = [r for r in comment_eligible if r.get("candidate_action_type") == "badge_filter_need"]
        site_rows = [r for r in comment_eligible if r.get("candidate_action_type") == "event_site_search_or_listing"]
        event_question_rows = [*event_rows, *site_rows, *badge_rows]
        ask_context_rows = [
            r for r in eligible
            if str(r.get("intent_set")) in {"event_close_question", "organizer_comment_fit"}
        ]
        comment_rows = comment_eligible
        source_post_rows = [r for r in all_rows if _is_source_post_context(r)]
        noise_rows = [r for r in all_rows if str(r.get("candidate_noise_type") or "")]
        answerable_count = len(route_rows) + len(event_rows) + len(site_rows) + len(organizer_submit_rows) + len(badge_rows)
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
        surface_scope_noise = _surface_out_of_scope_type(profile)
        if profile.get("monitoring_decision_hint") == "reject_stale_inactive":
            recommendation = "reject_stale_inactive"
        elif surface_scope_noise:
            recommendation = surface_scope_noise
        parts: list[str] = []
        if route_rows:
            parts.append(f"route/POI вопросов: {len(route_rows)}")
        if event_rows:
            parts.append(f"event-вопросов: {len(event_rows)}")
        if organizer_submit_rows:
            parts.append(f"organizer submission вопросов: {len(organizer_submit_rows)}")
        if site_rows:
            parts.append(f"поиск/афиша вопросов: {len(site_rows)}")
        if badge_rows:
            parts.append(f"badge/filter вопросов: {len(badge_rows)}")
        if ask_context_rows:
            own_post_count = len([r for r in ask_context_rows if _is_source_post_context(r)])
            if own_post_count:
                parts.append(f"контекстов постов для уточняющих вопросов организаторам: {own_post_count}")
            comment_ask_count = len(ask_context_rows) - own_post_count
            if comment_ask_count:
                parts.append(f"вопросов/комментариев для organizer-уточнений: {comment_ask_count}")
        if noise_rows:
            if _deterministic_prefilter_enabled():
                parts.append(f"отфильтровано рекламных/не-вопросных сигналов: {len(noise_rows)}")
            else:
                parts.append(f"диагностических шумовых/не-вопросных сигналов без prefilter-отсечения: {len(noise_rows)}")
        summary_ru = "; ".join(parts) if parts else "полезных вопросных сигналов не найдено"
        if recommendation == "reject_stale_inactive":
            summary_ru = (
                f"устаревшая активность: последний комментарий {profile.get('latest_comment_at') or 'неизвестно'}, "
                f"{profile.get('days_since_latest_activity') or '?'} дней назад; не выбирать сейчас, только revival-watch"
            )
        elif surface_scope_noise:
            summary_ru = f"поверхность не по теме acquisition ({surface_scope_noise}); не выбирать для ответов/маршрутов/событий"
        increment_status = _surface_increment_status(profile, {
            "comments_embedded": profile.get("comments_embedded"),
        })
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
            "latest_comment_at": profile.get("latest_comment_at"),
            "latest_event_question_at": _latest_created_at(event_question_rows),
            "latest_route_recommendation_at": _latest_created_at(route_rows),
            "increment_status": increment_status,
            "is_incremental_last_run": _is_counted_increment_status(increment_status),
            "last_analyzed_run_id": profile.get("last_analyzed_run_id"),
            "discovered_at": profile.get("discovered_at"),
            "discovered_in_run_id": profile.get("discovered_in_run_id"),
            "discovery_source_context": profile.get("discovery_source_context"),
            "days_since_latest_activity": profile.get("days_since_latest_activity"),
            "freshness_status": profile.get("freshness_status"),
            "analysis_max_comment_age_days": profile.get("analysis_max_comment_age_days"),
            "stale_activity_days": profile.get("stale_activity_days"),
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
            "selection_status": _selection_status_from_recommendation(recommendation),
            "summary_ru": summary_ru,
            "answerable_question_candidates": answerable_count,
            "ask_clarification_contexts": ask_count,
            "eligible_comment_contexts": len(comment_rows),
            "source_post_contexts": len(source_post_rows),
            "route_poi_questions": len(route_rows),
            "event_questions": len(event_rows),
            "event_site_search_questions": len(site_rows),
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
            "answerable_examples": _examples_text([*route_rows[:2], *event_rows[:2], *site_rows[:1], *organizer_submit_rows[:1], *badge_rows[:1]], limit=4),
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
    for row in _best_context_rows([r for r in rows if _report_real_question_row(r)]):
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
            "canonical_question_ru": _canonical_question_for_pattern(pattern),
            "example_questions": [],
            "example_urls": [],
        })
        item["count"] += 1
        if row.get("intent_set"):
            item["intent_sets"].add(str(row.get("intent_set")))
        if row.get("model_name"):
            item["models"].add(str(row.get("model_name")))
        if len(item["example_questions"]) < limit_examples and text:
            item["example_questions"].append(text[:220])
        if len(item["example_urls"]) < limit_examples and row.get("context_url"):
            item["example_urls"].append(str(row.get("context_url")))
    out: list[dict[str, Any]] = []
    for item in grouped.values():
        out.append({
            **{k: v for k, v in item.items() if k not in {"intent_sets", "models", "example_questions", "example_urls"}},
            "intent_sets": ",".join(sorted(item["intent_sets"])),
            "models": ",".join(sorted(item["models"])),
            "example_questions": "\n".join(item["example_questions"]),
            "example_urls": "\n".join(item["example_urls"]),
        })
    return sorted(out, key=lambda r: (-int(r.get("count") or 0), str(r.get("surface_key") or ""), str(r.get("pattern") or "")))


def _canonical_question_catalog_rows(rows: list[dict[str, Any]], *, limit_examples: int = 5) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in _best_context_rows([r for r in rows if _report_real_question_row(r)]):
        text = str(row.get("text_snapshot") or row.get("text") or "")
        action = str(row.get("candidate_action_type") or "")
        pattern = _question_pattern_label(text, action_type=action, intent_set=str(row.get("intent_set") or ""))
        key = (pattern, action)
        item = grouped.setdefault(key, {
            "pattern": pattern,
            "candidate_action_type": action,
            "canonical_question_ru": _canonical_question_for_pattern(pattern),
            "real_question_examples_total": 0,
            "monitoring_candidate_examples": 0,
            "historical_calibration_examples": 0,
            "surfaces_count": set(),
            "example_questions": [],
            "example_urls": [],
            "source_note_ru": "Эталон собран из реальных вопросительных комментариев; historical используется для шаблонов/QA, fresh — для мониторинга.",
        })
        item["real_question_examples_total"] += 1
        scope = str(row.get("candidate_usage_scope") or "")
        if scope == "monitoring_candidate":
            item["monitoring_candidate_examples"] += 1
        elif scope == "historical_calibration":
            item["historical_calibration_examples"] += 1
        if row.get("surface_key"):
            item["surfaces_count"].add(str(row.get("surface_key")))
        if len(item["example_questions"]) < limit_examples:
            item["example_questions"].append(text[:220])
        if len(item["example_urls"]) < limit_examples and row.get("context_url"):
            item["example_urls"].append(str(row.get("context_url")))
    out: list[dict[str, Any]] = []
    for item in grouped.values():
        out.append({
            **{k: v for k, v in item.items() if k not in {"surfaces_count", "example_questions", "example_urls"}},
            "surfaces_count": len(item["surfaces_count"]),
            "example_questions": "\n".join(item["example_questions"]),
            "example_urls": "\n".join(item["example_urls"]),
        })
    return sorted(out, key=lambda r: (-int(r.get("monitoring_candidate_examples") or 0), -int(r.get("real_question_examples_total") or 0), str(r.get("pattern") or "")))


def _model_example_rows(candidates: list[dict[str, Any]], model_name: str, *, limit: int = 200) -> list[dict[str, Any]]:
    rows = [r for r in candidates if str(r.get("model_name") or "") == model_name and _report_candidate_eligible(r)]
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
    usage_scopes = Counter(str(r.get("candidate_usage_scope") or _record_usage_scope(r)) for r in records)
    return [
        {"metric": "run_id", "value": run_id},
        {"metric": "report_generated_at", "value": datetime.now(timezone.utc).isoformat()},
        {"metric": "KAGGLE_RUN_ID", "value": os.getenv("KAGGLE_RUN_ID", "")},
        {"metric": "ACQ_SOURCE_RUN_ID", "value": os.getenv("ACQ_SOURCE_RUN_ID", "")},
        {"metric": "source_run_provenance", "value": _source_run_provenance()},
        {"metric": "stage", "value": STAGE_NAME},
        {"metric": "scope_note", "value": summary_note or "Limited to the Kaggle seed payload and configured per-run budgets; not a full historical DB scan unless the payload/budgets covered it."},
        {"metric": "models", "value": ", ".join(models)},
        {"metric": "gate_model_for_llm_budget", "value": gate_model},
        {"metric": "scoring_method", "value": scoring_method},
        {"metric": "comments_after_filter", "value": len(records)},
        {"metric": "monitoring_window_comments", "value": usage_scopes.get("monitoring_candidate", 0)},
        {"metric": "historical_calibration_comments", "value": usage_scopes.get("historical_calibration", 0)},
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
        {"metric": "ACQ_RUNTIME_DEADLINE_SECONDS", "value": os.getenv("ACQ_RUNTIME_DEADLINE_SECONDS", "")},
        {"metric": "ACQ_ENABLE_VK_MEMBER_PROFILE_DISCOVERY", "value": os.getenv("ACQ_ENABLE_VK_MEMBER_PROFILE_DISCOVERY", "")},
        {"metric": "ACQ_MAX_VK_MEMBER_PROFILES_DISCOVERED_PER_RUN", "value": os.getenv("ACQ_MAX_VK_MEMBER_PROFILES_DISCOVERED_PER_RUN", "")},
        {"metric": "ACQ_MAX_VK_AUTHOR_PROFILES_DISCOVERED_PER_RUN", "value": os.getenv("ACQ_MAX_VK_AUTHOR_PROFILES_DISCOVERED_PER_RUN", "")},
    ]


def _intent_catalog_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for intent_set, phrases in INTENT_SETS.items():
        action = "negative_filter" if intent_set == "negative_intents" else _action_for_intent(intent_set)
        for idx, phrase in enumerate(phrases, start=1):
            rows.append({
                "intent_set": intent_set,
                "intent_set_ru": _INTENT_SET_LABELS_RU.get(intent_set, intent_set),
                "phrase_order": idx,
                "model_phrase": phrase,
                "candidate_action_type": action,
                "is_negative": intent_set == "negative_intents",
                "note_ru": "Эта фраза эмбеддится как эталон смысла; top_intent_phrase — ближайшая такая фраза для строки-кандидата.",
            })
    return rows


_INTENT_SET_LABELS_RU = {
    "route_poi_far_context": "Маршруты/места: широкий контекст",
    "route_poi_medium_interest": "Маршруты/места: интерес",
    "route_poi_close_actionable": "Маршруты/места: можно отвечать",
    "event_far_context": "События: широкий контекст",
    "event_close_question": "События: практический вопрос",
    "organizer_comment_fit": "Организатор: можно задать уточняющий вопрос",
    "event_site_search_or_listing": "Сайт/афиша/подборки",
    "organizer_submission_or_partnership": "Организатор: добавить событие/партнёрство",
    "badge_filter_need": "Фильтры/признаки события",
    "negative_intents": "Минус-смыслы/шум",
}


_SCAN_STATES_TOUCHED = {
    "scanned",
    "scanned_this_run",
    "comments_available",
    "commentability_resolved",
    "resolved_commentability",
    "resolved_no_comments",
    "rejected_after_resolve",
    "checked_inaccessible",
}


_COUNTED_INCREMENT_STATUSES = {
    "newly_discovered_this_run",
    "newly_discovered_and_analyzed_this_run",
    "analyzed_comments_this_run",
    "touched_no_eligible_comments_this_run",
}


def _is_counted_increment_status(status: Any) -> bool:
    return str(status or "") in _COUNTED_INCREMENT_STATUSES


def _surface_increment_status(surface: dict[str, Any], summary: dict[str, Any]) -> str:
    source_run_id = _source_run_id_from_env()
    if not source_run_id:
        return "no_verified_run_id"
    discovered_run = str(surface.get("discovered_in_run_id") or "").strip()
    discovered_this_run = bool(discovered_run and discovered_run == source_run_id)
    touched_run = (
        surface.get("last_analyzed_run_id")
        or summary.get("last_analyzed_run_id")
        or surface.get("scan_run_id")
        or surface.get("run_id")
        or ""
    )
    touched_this_run = str(touched_run).strip() == source_run_id
    if int(summary.get("comments_embedded") or 0) > 0:
        if discovered_this_run:
            return "newly_discovered_and_analyzed_this_run"
        if touched_this_run:
            return "analyzed_comments_this_run"
        return "analyzed_comments_unverified_run"
    if discovered_this_run:
        return "newly_discovered_this_run"
    scan_state = str(surface.get("scan_state") or "").strip().lower()
    if touched_this_run and scan_state in _SCAN_STATES_TOUCHED:
        return "touched_no_eligible_comments_this_run"
    return "queued_or_existing"


def _surface_inventory_rows(
    surfaces_by_external: dict[str, dict[str, Any]] | None,
    surface_summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key = {str(row.get("surface_key") or ""): row for row in surface_summaries}
    rows: list[dict[str, Any]] = []
    keys = set(by_key)
    if surfaces_by_external:
        keys.update(str(k) for k in surfaces_by_external)
    for key in sorted(k for k in keys if k):
        surface = (surfaces_by_external or {}).get(key) or {}
        summary = by_key.get(key) or {}
        reach = surface.get("reach") if isinstance(surface.get("reach"), dict) else {}
        recommendation = summary.get("recommendation") or "not_profiled_or_no_comment_signal"
        selection_status = summary.get("selection_status") or _selection_status_from_recommendation(
            str(recommendation),
            surface_status=str(surface.get("status") or ""),
            scan_state=str(surface.get("scan_state") or ""),
        )
        increment_status = _surface_increment_status(surface, summary)
        rows.append({
            "surface_key": key,
            "platform": surface.get("platform") or summary.get("platform"),
            "surface_type": surface.get("surface_type") or summary.get("surface_type"),
            "surface_title": surface.get("title") or summary.get("surface_title"),
            "surface_url": surface.get("url") or summary.get("surface_url"),
            "status": surface.get("status"),
            "scan_state": surface.get("scan_state"),
            "source": surface.get("source"),
            "members_or_subscribers": reach.get("members") or reach.get("members_count") or summary.get("members_or_subscribers"),
            "recommendation": recommendation,
            "selection_status": selection_status,
            "increment_status": increment_status,
            "is_incremental_last_run": _is_counted_increment_status(increment_status),
            "discovered_at": surface.get("discovered_at"),
            "discovered_in_run_id": surface.get("discovered_in_run_id"),
            "discovery_source_context": surface.get("discovery_source_context"),
            "latest_comment_at": summary.get("latest_comment_at"),
            "latest_event_question_at": summary.get("latest_event_question_at"),
            "latest_route_recommendation_at": summary.get("latest_route_recommendation_at"),
            "days_since_latest_activity": summary.get("days_since_latest_activity"),
            "freshness_status": summary.get("freshness_status") or ("unknown_no_profile" if not summary else ""),
            "comments_embedded": summary.get("comments_embedded") or 0,
            "answerable_question_candidates": summary.get("answerable_question_candidates") or 0,
            "route_poi_questions": summary.get("route_poi_questions") or 0,
            "event_questions": summary.get("event_questions") or 0,
            "event_site_search_questions": summary.get("event_site_search_questions") or 0,
            "ask_clarification_contexts": summary.get("ask_clarification_contexts") or 0,
            "summary_ru": summary.get("summary_ru") or "В этом прогоне полезных сигналов не найдено или поверхность не попала в comment retrieval.",
        })
    return sorted(rows, key=lambda r: (
        {"selected": 0, "candidate": 1, "rejected": 2}.get(str(r.get("selection_status")), 3),
        -int(float(r.get("answerable_question_candidates") or 0)),
        str(r.get("platform") or ""),
        str(r.get("surface_title") or r.get("surface_key") or ""),
    ))


def _summary_count_rows(surface_inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for row in surface_inventory:
        key = (str(row.get("platform") or "unknown"), str(row.get("surface_type") or "unknown"))
        status = str(row.get("selection_status") or "candidate")
        grouped[key]["total"] += 1
        grouped[key][status] += 1
        if str(row.get("increment_status") or "") in {"newly_discovered_this_run", "newly_discovered_and_analyzed_this_run"}:
            grouped[key]["newly_discovered_this_run"] += 1
        if _is_counted_increment_status(row.get("increment_status")):
            grouped[key]["increment_touched_this_run"] += 1
        if str(row.get("increment_status") or "") in {"analyzed_comments_this_run", "newly_discovered_and_analyzed_this_run"}:
            grouped[key]["analyzed_comments_this_run"] += 1
    rows: list[dict[str, Any]] = []
    total_counter: Counter[str] = Counter()
    for (platform, surface_type), counter in sorted(grouped.items()):
        total_counter.update(counter)
        rows.append({
            "platform": platform,
            "surface_type": surface_type,
            "total_surfaces": counter.get("total", 0),
            "selected_surfaces": counter.get("selected", 0),
            "candidate_surfaces": counter.get("candidate", 0),
            "rejected_surfaces": counter.get("rejected", 0),
            "newly_discovered_this_run": counter.get("newly_discovered_this_run", 0),
            "increment_touched_this_run": counter.get("increment_touched_this_run", 0),
            "analyzed_comments_this_run": counter.get("analyzed_comments_this_run", 0),
        })
    rows.insert(0, {
        "platform": "ALL",
        "surface_type": "ALL",
        "total_surfaces": total_counter.get("total", 0),
        "selected_surfaces": total_counter.get("selected", 0),
        "candidate_surfaces": total_counter.get("candidate", 0),
        "rejected_surfaces": total_counter.get("rejected", 0),
        "newly_discovered_this_run": total_counter.get("newly_discovered_this_run", 0),
        "increment_touched_this_run": total_counter.get("increment_touched_this_run", 0),
        "analyzed_comments_this_run": total_counter.get("analyzed_comments_this_run", 0),
    })
    return rows


def _has_verified_increment_source(scope: dict[str, Any]) -> bool:
    return str(scope.get("source_run_provenance") or "") in {"kaggle_run_id", "explicit_source_run_id"}


def _dashboard_summary_rows(
    *,
    summary: dict[str, Any] | None,
    surface_summaries: list[dict[str, Any]],
    scope_rows: list[dict[str, Any]],
    surface_inventory: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    summary = summary or {}
    scope = {str(r.get("metric")): r.get("value") for r in scope_rows}
    monitored = [r for r in surface_summaries if str(r.get("recommendation")) in {"both_monitor_replies_and_ask_clarifications", "monitor_for_reply_opportunities", "ask_organizer_clarification_questions", "sample_more"}]
    no_signal = [r for r in (surface_inventory or []) if int(float(r.get("answerable_question_candidates") or 0)) <= 0]
    counts = _summary_count_rows(surface_inventory or [])
    total = counts[0] if counts else {}
    verified_increment_source = _has_verified_increment_source(scope)
    run_status_ru = (
        "да, источник запуска подтверждён" if verified_increment_source
        else "нет доказанного нового запуска: KAGGLE_RUN_ID/ACQ_SOURCE_RUN_ID отсутствует, инкремент не засчитывается"
    )
    return [
        {"section": "Что это", "metric": "Назначение", "value": "Дашборд показывает, в каких TG/VK группах и обсуждениях есть вопросы для аккуратных ответов или места для уточняющих вопросов организаторам."},
        {"section": "Охват", "metric": "Комментариев обработано", "value": summary.get("comments_embedded") or scope.get("comments_after_filter")},
        {"section": "Охват", "metric": "Свежих для мониторинга", "value": scope.get("monitoring_window_comments")},
        {"section": "Охват", "metric": "Исторических для эталонов/контроля", "value": scope.get("historical_calibration_comments")},
        {"section": "Охват", "metric": "Поверхностей с профилем", "value": summary.get("surface_profiles_count") or scope.get("surfaces_profiled")},
        {"section": "Охват", "metric": "Все поверхности в списке", "value": len(surface_inventory or [])},
        {"section": "Итог", "metric": "Выбрано", "value": total.get("selected_surfaces", 0)},
        {"section": "Итог", "metric": "Кандидаты", "value": total.get("candidate_surfaces", 0)},
        {"section": "Итог", "metric": "Отклонено", "value": total.get("rejected_surfaces", 0)},
        {"section": "Инкремент последнего запуска", "metric": "Был ли новый запуск", "value": run_status_ru},
        {"section": "Инкремент последнего запуска", "metric": "Новых ссылок/стен найдено", "value": total.get("newly_discovered_this_run", 0)},
        {"section": "Инкремент последнего запуска", "metric": "Поверхностей затронуто/проанализировано", "value": total.get("increment_touched_this_run", 0)},
        {"section": "Инкремент последнего запуска", "metric": "С комментариями в анализе", "value": total.get("analyzed_comments_this_run", 0)},
        {"section": "Период", "metric": "С даты", "value": scope.get("period_min_created_at")},
        {"section": "Период", "metric": "По дату", "value": scope.get("period_max_created_at")},
        {"section": "Модели", "metric": "Модели смысла", "value": summary.get("models") or scope.get("models")},
        {"section": "Модели", "metric": "Модель для LLM-gate", "value": summary.get("recommended_model") or scope.get("gate_model_for_llm_budget")},
        {"section": "Итог", "metric": "Где есть хотя бы слабый смысл", "value": len(monitored)},
        {"section": "Итог", "metric": "Где ничего не найдено/не попало", "value": len(no_signal)},
        {"section": "Как читать", "metric": "surface_summary", "value": "Главный лист: где мониторить, сколько вопросов и как часто они встречаются."},
        {"section": "Как читать", "metric": "full_surface_list", "value": "Полный список поверхностей из payload/run: видно, где ничего не нашлось или поверхность не анализировалась."},
        {"section": "Как читать", "metric": "summary_counts", "value": "Сводка по каждому типу: всего / выбрано / кандидаты / отклонено."},
        {"section": "Как читать", "metric": "intent_catalog", "value": "Список модельных смыслов, по которым ищем. top_intent_phrase в примерах берётся именно отсюда."},
        {"section": "Ограничения", "metric": "Контекст", "value": "Новые прогоны сохраняют исходный пост/родительский комментарий; в старых артефактах контекст восстановить нельзя."},
        {"section": "Ограничения", "metric": "Не наши темы", "value": "Недвижимость и медицина отсекаются как out_of_scope, если нет явной связи с маршрутом/событием."},
        {"section": "Freshness", "metric": "Активность", "value": f"Для включения в мониторинг учитываются свежие комментарии до {_max_comment_age_days()} дней; поверхности без активности больше {_stale_activity_days()} дней отклоняются сейчас, но исторические строки остаются для эталонных вопросов и контроля смыслов."},
    ]


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


_RU_HEADERS = {
    "label": "Метка",
    "action_class": "Класс действия",
    "is_actionable_reply_opportunity": "Можно отвечать?",
    "false_positive_type": "Тип ошибки",
    "model_disagreement_bucket": "Согласие моделей",
    "model_name": "Модель",
    "intent_set": "Группа смысла",
    "intent_set_ru": "Смысл по-русски",
    "model_phrase": "Модельная фраза",
    "phrase_order": "№",
    "is_negative": "Минус-смысл?",
    "note_ru": "Пояснение",
    "score": "Оценка",
    "rank_global": "Ранг общий",
    "rank_within_surface": "Ранг в группе",
    "surface_key": "ID поверхности",
    "platform": "Платформа",
    "surface_type": "Тип",
    "surface_title": "Название",
    "surface_url": "Ссылка",
    "status": "Статус",
    "selection_status": "Итоговый статус",
    "scan_state": "Сканирование",
    "source": "Источник",
    "increment_status": "Инкремент",
    "is_incremental_last_run": "Инкремент последнего запуска?",
    "discovered_at": "Добавлено/найдено",
    "discovered_in_run_id": "Найдено в run_id",
    "discovery_source_context": "Где найдено",
    "relation": "Тип контекста",
    "is_post": "Это пост?",
    "author_id": "Автор",
    "created_at": "Дата",
    "comment_age_days": "Возраст, дней",
    "candidate_usage_scope": "Роль строки",
    "is_within_monitoring_window": "В окне мониторинга?",
    "context_url": "Ссылка на контекст",
    "text_snapshot": "Комментарий",
    "analysis_context_snapshot": "Контекст поста/родителя",
    "source_post_text_snapshot": "Исходный пост",
    "reply_parent_text_snapshot": "Родительский комментарий",
    "top_intent_phrase": "Ближайшая модельная фраза",
    "positive_score": "Похожесть +",
    "negative_score": "Похожесть −",
    "funnel_bucket": "Воронка",
    "candidate_action_type": "Что можно делать",
    "destination_hint": "Место/направление",
    "transport_hint": "Транспорт",
    "question_signal": "Есть вопрос?",
    "candidate_noise_type": "Диагностика шума",
    "intent_text_supported": "Смысл подтверждён текстом",
    "pre_llm_candidate_eligible": "В LLM-gate?",
    "llm_gate_selection_basis": "Основа отбора в LLM",
    "recommendation": "Рекомендация",
    "members_or_subscribers": "Участники",
    "period_min_created_at": "Период с",
    "period_max_created_at": "Период по",
    "period_days": "Дней",
    "period_label": "Период",
    "latest_comment_at": "Последний комментарий",
    "latest_event_question_at": "Последний event-вопрос",
    "latest_route_recommendation_at": "Последняя route-рекомендация",
    "days_since_latest_activity": "Дней без активности",
    "freshness_status": "Свежесть",
    "analysis_max_comment_age_days": "Окно анализа, дней",
    "stale_activity_days": "Порог неактивности, дней",
    "unique_commenters": "Уникальные авторы",
    "unique_commenters_note": "Авторы: примечание",
    "comments_total": "Всего комментариев",
    "comments_embedded": "В анализе",
    "comments_per_day": "Комм./день",
    "comments_per_week": "Комм./нед.",
    "comments_per_30d": "Комм./30д",
    "comments_per_90d": "Комм./90д",
    "latest_100_comments": "Последние N",
    "latest_100_min_created_at": "Latest-100 с",
    "latest_100_max_created_at": "Latest-100 по",
    "latest_100_period_days": "Latest-100 дней",
    "latest_100_period_label": "Latest-100 период",
    "summary_ru": "Вывод",
    "answerable_question_candidates": "Вопросы для ответа",
    "answerable_questions_per_30d": "Вопросы/30д",
    "answerable_questions_per_90d": "Вопросы/90д",
    "answerable_questions_per_100_comments": "Вопросы/100 комм.",
    "ask_clarification_contexts": "Где спрашивать самим",
    "ask_contexts_per_30d": "Уточнения/30д",
    "eligible_comment_contexts": "Контексты-комментарии",
    "source_post_contexts": "Контексты-посты",
    "route_poi_questions": "Маршруты",
    "event_questions": "События",
    "event_site_search_questions": "Поиск/афиша",
    "organizer_submission_questions": "Организаторам",
    "badge_filter_questions": "Фильтры",
    "filtered_noise_contexts": "Шум/диагн.",
    "relation_counts_json": "Типы контекстов",
    "dominant_detected_interests": "Темы",
    "monitoring_decision_hint": "Решение",
    "monitoring_reason": "Причина",
    "answerable_examples": "Примеры ответов",
    "ask_question_examples": "Примеры уточнений",
    "pattern": "Паттерн",
    "canonical_question_ru": "Эталонный вопрос",
    "count": "Количество",
    "intent_sets": "Группы смыслов",
    "models": "Модели",
    "example_questions": "Примеры вопросов",
    "example_urls": "Ссылки примеров",
    "metric": "Показатель",
    "value": "Значение",
    "section": "Раздел",
    "total_surfaces": "Всего",
    "selected_surfaces": "Выбрано",
    "candidate_surfaces": "Кандидаты",
    "rejected_surfaces": "Отклонено",
    "newly_discovered_this_run": "Новых в запуске",
    "increment_touched_this_run": "Затронуто в запуске",
    "analyzed_comments_this_run": "С анализом комм.",
    "real_question_examples_total": "Реальных вопросов",
    "monitoring_candidate_examples": "Свежих примеров",
    "historical_calibration_examples": "Исторических примеров",
    "surfaces_count": "Поверхностей",
    "source_note_ru": "Пояснение",
}


def _format_date_ru(value: Any) -> Any:
    dt = _parse_created_at(value)
    if dt is None:
        return value
    return dt.strftime("%d.%m.%Y")


def _format_datetime_ru(value: Any) -> Any:
    dt = _parse_created_at(value)
    if dt is None:
        return value
    return dt.strftime("%d.%m.%Y %H:%M")


def _xlsx_value(header: str, value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    if header in {"discovered_at", "report_generated_at", "generated_at"}:
        return _format_datetime_ru(value)
    if header.endswith("_created_at") or header == "created_at" or header.endswith("_at") or header in {"period_min_created_at", "period_max_created_at"}:
        return _format_date_ru(value)
    if isinstance(value, float):
        return round(value, 1)
    if isinstance(value, str):
        stripped = value.strip()
        if header.endswith("_days") or "_per_" in header or header.endswith("_score") or header == "score":
            try:
                return round(float(stripped), 1)
            except Exception:
                return value
    return value


def _append_grouped_header(ws: Any, headers: list[str], groups: dict[str, str] | None = None) -> None:
    from openpyxl.styles import Alignment, Font, PatternFill

    groups = groups or {}
    group_values = [groups.get(h, "") for h in headers]
    ws.append(group_values)
    ws.append([_RU_HEADERS.get(h, h) for h in headers])
    for row_idx, color in [(1, "B7DEE8"), (2, "D9EAF7")]:
        for cell in ws[row_idx]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor=color)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    start = 1
    while start <= len(headers):
        group = group_values[start - 1]
        end = start
        while end < len(headers) and group_values[end] == group:
            end += 1
        if group and end > start:
            ws.merge_cells(start_row=1, start_column=start, end_row=1, end_column=end)
        start = end + 1


def _append_data_rows(
    ws: Any,
    rows: list[dict[str, Any]],
    headers: list[str],
    *,
    hyperlink_field: str | None = None,
    style: str | None = None,
) -> None:
    from openpyxl.styles import PatternFill

    selected_fill = PatternFill("solid", fgColor="C6EFCE")
    increment_fill = PatternFill("solid", fgColor="FFF2CC")
    for row in rows:
        ws.append([_xlsx_value(h, row.get(h)) for h in headers])
        if style == "surface":
            fill = None
            if str(row.get("selection_status") or "") == "selected":
                fill = selected_fill
            elif _is_counted_increment_status(row.get("increment_status")):
                fill = increment_fill
            if fill is not None:
                for cell in ws[ws.max_row]:
                    cell.fill = fill
        elif style == "summary_counts":
            if any(int(float(row.get(h) or 0)) > 0 for h in ["newly_discovered_this_run", "increment_touched_this_run", "analyzed_comments_this_run"] if h in headers):
                for header in ["newly_discovered_this_run", "increment_touched_this_run", "analyzed_comments_this_run"]:
                    if header in headers:
                        ws.cell(ws.max_row, headers.index(header) + 1).fill = increment_fill
        if hyperlink_field and row.get(hyperlink_field):
            url_cell = ws.cell(ws.max_row, headers.index(hyperlink_field) + 1)
            url_cell.hyperlink = str(row.get(hyperlink_field))
            url_cell.style = "Hyperlink"


def _write_xlsx(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    surface_summaries: list[dict[str, Any]] | None = None,
    model_examples: dict[str, list[dict[str, Any]]] | None = None,
    question_patterns: list[dict[str, Any]] | None = None,
    canonical_questions: list[dict[str, Any]] | None = None,
    scope_rows: list[dict[str, Any]] | None = None,
    surface_inventory: list[dict[str, Any]] | None = None,
    summary_counts: list[dict[str, Any]] | None = None,
    dashboard_rows: list[dict[str, Any]] | None = None,
    intent_catalog_rows: list[dict[str, Any]] | None = None,
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
        "relation", "is_post", "author_id", "created_at", "comment_age_days", "candidate_usage_scope", "context_url", "text_snapshot", "analysis_context_snapshot", "top_intent_phrase", "positive_score", "negative_score", "funnel_bucket",
        "destination_hint", "transport_hint", "question_signal", "candidate_noise_type", "intent_text_supported", "pre_llm_candidate_eligible", "llm_gate_selection_basis",
    ]
    manual_groups = {h: "Разметка" for h in headers[:5]} | {h: "Скоринг" for h in headers[5:10]} | {h: "Поверхность" for h in headers[10:17]} | {h: "Текст и контекст" for h in headers[17:22]} | {h: "Решение" for h in headers[22:]}
    _append_grouped_header(ws, headers, manual_groups)
    _append_data_rows(ws, rows, headers, hyperlink_field="context_url")
    for idx, width in enumerate([12, 28, 28, 24, 28, 34, 28, 12, 12, 16, 28, 10, 18, 18, 10, 16, 12, 45, 70, 70, 55, 14, 14, 14, 22, 16, 14, 24, 20, 24], start=1):
        ws.column_dimensions[get_column_letter(idx)].width = width
    ws.freeze_panes = "A3"

    if dashboard_rows:
        dash = wb.create_sheet("summary_ru", 0)
        dash_headers = ["section", "metric", "value"]
        _append_grouped_header(dash, dash_headers, {h: "Краткое объяснение" for h in dash_headers})
        _append_data_rows(dash, dashboard_rows, dash_headers)
        dash.freeze_panes = "A3"
        for idx, width in enumerate([18, 32, 120], start=1):
            dash.column_dimensions[get_column_letter(idx)].width = width

    if surface_summaries:
        surf = wb.create_sheet("surface_summary")
        surf_headers = [
            "recommendation", "selection_status", "surface_key", "platform", "surface_type", "surface_title", "surface_url",
            "members_or_subscribers", "period_min_created_at", "period_max_created_at", "period_days",
            "latest_comment_at", "latest_event_question_at", "latest_route_recommendation_at", "days_since_latest_activity", "freshness_status", "unique_commenters",
            "comments_total", "comments_embedded", "comments_per_day", "comments_per_week", "comments_per_30d", "comments_per_90d",
            "latest_100_comments", "latest_100_min_created_at", "latest_100_max_created_at", "latest_100_period_days",
            "increment_status", "is_incremental_last_run", "discovered_at", "discovered_in_run_id", "discovery_source_context",
            "summary_ru", "answerable_question_candidates", "answerable_questions_per_30d", "answerable_questions_per_90d",
            "answerable_questions_per_100_comments", "ask_clarification_contexts", "ask_contexts_per_30d", "eligible_comment_contexts", "source_post_contexts",
            "route_poi_questions", "event_questions", "event_site_search_questions", "organizer_submission_questions", "badge_filter_questions",
            "filtered_noise_contexts", "relation_counts_json", "unique_commenters_note", "answerable_examples", "ask_question_examples",
        ]
        surf_groups = {
            **{h: "Поверхность" for h in surf_headers[:8]},
            **{h: "Период, свежесть и объём" for h in surf_headers[8:25]},
            **{h: "Потенциал" for h in surf_headers[25:39]},
            **{h: "Пояснения" for h in surf_headers[35:]},
        }
        _append_grouped_header(surf, surf_headers, surf_groups)
        _append_data_rows(surf, surface_summaries, surf_headers, hyperlink_field="surface_url", style="surface")
        surf.freeze_panes = "A3"
        for idx, width in enumerate([34, 30, 10, 18, 30, 42, 16, 22, 22, 12, 18, 14, 14, 14, 14, 14, 14, 14, 22, 22, 14, 70, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 60, 24, 90, 90], start=1):
            surf.column_dimensions[get_column_letter(idx)].width = width

    if surface_inventory:
        inv = wb.create_sheet("full_surface_list")
        inv_headers = [
            "surface_key", "platform", "surface_type", "surface_title", "surface_url", "status", "scan_state", "source",
            "members_or_subscribers", "recommendation", "selection_status", "increment_status", "is_incremental_last_run",
            "discovered_at", "discovered_in_run_id", "latest_comment_at", "latest_event_question_at", "latest_route_recommendation_at", "days_since_latest_activity", "freshness_status",
            "comments_embedded", "answerable_question_candidates",
            "route_poi_questions", "event_questions", "event_site_search_questions", "ask_clarification_contexts", "summary_ru",
        ]
        inv_groups = {h: "Поверхность" for h in inv_headers[:9]} | {h: "Результат анализа" for h in inv_headers[9:]}
        _append_grouped_header(inv, inv_headers, inv_groups)
        _append_data_rows(inv, surface_inventory, inv_headers, hyperlink_field="surface_url", style="surface")
        inv.freeze_panes = "A3"
        for idx, width in enumerate([30, 10, 18, 34, 44, 20, 24, 22, 14, 36, 16, 16, 14, 14, 14, 14, 14, 14, 14, 90], start=1):
            inv.column_dimensions[get_column_letter(idx)].width = width

    if summary_counts:
        counts = wb.create_sheet("summary_counts")
        count_headers = ["platform", "surface_type", "total_surfaces", "selected_surfaces", "candidate_surfaces", "rejected_surfaces", "newly_discovered_this_run", "increment_touched_this_run", "analyzed_comments_this_run"]
        count_groups = {h: "Тип поверхности" for h in count_headers[:2]} | {h: "Итог" for h in count_headers[2:]}
        _append_grouped_header(counts, count_headers, count_groups)
        _append_data_rows(counts, summary_counts, count_headers, style="summary_counts")
        counts.freeze_panes = "A3"
        for idx, width in enumerate([14, 24, 12, 12, 12, 12], start=1):
            counts.column_dimensions[get_column_letter(idx)].width = width

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
            "relation", "is_post", "created_at", "comment_age_days", "candidate_usage_scope", "intent_set", "candidate_action_type", "candidate_noise_type",
            "llm_gate_selection_basis", "context_url", "text_snapshot", "analysis_context_snapshot", "top_intent_phrase", "destination_hint", "transport_hint",
        ]
        ex_groups = {h: "Скоринг" for h in ex_headers[:4]} | {h: "Поверхность" for h in ex_headers[4:13]} | {h: "Комментарий и контекст" for h in ex_headers[13:17]} | {h: "Действие" for h in ex_headers[17:]}
        _append_grouped_header(ex, ex_headers, ex_groups)
        _append_data_rows(ex, example_rows, ex_headers, hyperlink_field="context_url")
        ex.freeze_panes = "A3"
        for idx, width in enumerate([34, 12, 12, 16, 30, 10, 18, 18, 10, 12, 28, 30, 24, 45, 80, 80, 55, 22, 18], start=1):
            ex.column_dimensions[get_column_letter(idx)].width = width

    if question_patterns is not None:
        qp = wb.create_sheet("question_patterns")
        qp_headers = [
            "surface_key", "platform", "surface_type", "pattern", "candidate_action_type",
            "canonical_question_ru", "count", "intent_sets", "models", "example_questions", "example_urls",
        ]
        qp_groups = {h: "Где найдено" for h in qp_headers[:3]} | {h: "Эталон вопроса" for h in qp_headers[3:7]} | {h: "Модели и примеры" for h in qp_headers[7:]}
        _append_grouped_header(qp, qp_headers, qp_groups)
        _append_data_rows(qp, question_patterns, qp_headers)
        qp.freeze_panes = "A3"
        for idx, width in enumerate([30, 10, 18, 28, 34, 70, 10, 45, 45, 90, 70], start=1):
            qp.column_dimensions[get_column_letter(idx)].width = width

    if canonical_questions is not None:
        cq = wb.create_sheet("canonical_questions")
        cq_headers = [
            "pattern", "candidate_action_type", "canonical_question_ru", "real_question_examples_total",
            "monitoring_candidate_examples", "historical_calibration_examples", "surfaces_count",
            "example_questions", "example_urls", "source_note_ru",
        ]
        cq_groups = {h: "Эталон вопроса" for h in cq_headers[:7]} | {h: "Реальные примеры" for h in cq_headers[7:9]} | {"source_note_ru": "Пояснение"}
        _append_grouped_header(cq, cq_headers, cq_groups)
        _append_data_rows(cq, canonical_questions, cq_headers)
        cq.freeze_panes = "A3"
        for idx, width in enumerate([28, 34, 76, 14, 14, 14, 14, 100, 80, 100], start=1):
            cq.column_dimensions[get_column_letter(idx)].width = width

    if intent_catalog_rows:
        cat = wb.create_sheet("intent_catalog")
        cat_headers = ["intent_set", "intent_set_ru", "phrase_order", "model_phrase", "candidate_action_type", "is_negative", "note_ru"]
        cat_groups = {h: "Группа смысла" for h in cat_headers[:3]} | {h: "Модельная фраза" for h in cat_headers[3:]} 
        _append_grouped_header(cat, cat_headers, cat_groups)
        _append_data_rows(cat, intent_catalog_rows, cat_headers)
        cat.freeze_panes = "A3"
        for idx, width in enumerate([32, 38, 8, 80, 34, 12, 90], start=1):
            cat.column_dimensions[get_column_letter(idx)].width = width

    if scope_rows:
        scope = wb.create_sheet("scope")
        _append_grouped_header(scope, ["metric", "value"], {"metric": "Охват запуска", "value": "Охват запуска"})
        _append_data_rows(scope, scope_rows, ["metric", "value"])
        scope.freeze_panes = "A3"
        scope.column_dimensions["A"].width = 36
        scope.column_dimensions["B"].width = 120

    summary = wb.create_sheet("summary")
    _append_grouped_header(summary, ["metric", "value"], {"metric": "Файл", "value": "Файл"})
    _append_data_rows(summary, [
        {"metric": "rows", "value": len(rows)},
        {"metric": "generated_at", "value": datetime.now(timezone.utc).isoformat()},
    ], ["metric", "value"])
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
    source_run_id = _source_run_id_from_env()
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

        comments = [str(r.get("analysis_text") or r.get("text") or "") for r in records]
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
                enriched["target_hint"] = _route_target_hint(str(rec.get("analysis_text") or rec.get("text") or ""), str(row.get("intent_set")))
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
    gate_candidates = [
        c for c in all_candidates
        if c.get("model_name") == gate_model
        and c.get("candidate_usage_scope") == "monitoring_candidate"
    ]
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
            if row.get("candidate_usage_scope") == "monitoring_candidate" and _report_candidate_eligible(row):
                rows_by_surface[str(row.get("surface_key") or "unknown")].append(row)
    for rec in records:
        records_by_surface[str(rec.get("surface_key") or "unknown")].append(rec)
    for surface_key, surface_records in records_by_surface.items():
        profile = _surface_profile(surface_key, surface_records, rows_by_surface.get(surface_key, []), scoring_method=scoring_method)
        if source_run_id:
            profile["last_analyzed_run_id"] = source_run_id
        if surfaces_by_external and surface_key in surfaces_by_external:
            s = surfaces_by_external[surface_key]
            profile["surface_title"] = s.get("title")
            profile["surface_url"] = s.get("url")
            profile["status"] = s.get("status")
            profile["scan_state"] = s.get("scan_state")
            profile["source"] = s.get("source")
            profile["discovered_at"] = s.get("discovered_at")
            profile["discovered_in_run_id"] = s.get("discovered_in_run_id")
            profile["discovery_source_context"] = s.get("discovery_source_context")
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
    # Canonical question/pattern catalogs may use historical calibration rows
    # after the strict report-quality gate, but monitoring/surface decisions
    # above stay limited to fresh monitoring candidates.
    question_patterns = _build_question_patterns(all_candidates)
    canonical_questions = _canonical_question_catalog_rows(all_candidates)
    model_examples = {model_name: _model_example_rows(all_candidates, model_name) for model_name in models}
    surface_inventory = _surface_inventory_rows(surfaces_by_external, surface_summaries)
    summary_counts = _summary_count_rows(surface_inventory)
    intent_catalog = _intent_catalog_rows()
    run_id = source_run_id or "unknown_source_run"
    scope_rows = _scope_rows(
        run_id=run_id,
        records=records,
        profiles=profiles,
        models=models,
        gate_model=gate_model,
        scoring_method=scoring_method,
    )
    dashboard_rows = _dashboard_summary_rows(
        summary={
            "comments_embedded": len(records),
            "surface_profiles_count": len(profiles),
            "models": ", ".join(models),
            "recommended_model": gate_model,
        },
        surface_summaries=surface_summaries,
        scope_rows=scope_rows,
        surface_inventory=surface_inventory,
    )

    artifact_prefix = os.getenv("ACQ_COMMENT_RETRIEVAL_ARTIFACT_PREFIX") or "comment_retrieval"
    candidates_csv = output_path / f"{artifact_prefix}_candidates.csv"
    profiles_csv = output_path / f"{artifact_prefix}_surface_profiles.csv"
    surface_summary_csv = output_path / f"{artifact_prefix}_surface_decision_summary.csv"
    question_patterns_csv = output_path / f"{artifact_prefix}_question_patterns.csv"
    canonical_questions_csv = output_path / f"{artifact_prefix}_canonical_questions.csv"
    distributions_csv = output_path / f"{artifact_prefix}_score_distributions.csv"
    speed_csv = output_path / f"{artifact_prefix}_speed_metrics.csv"
    manual_xlsx = output_path / f"{artifact_prefix}_manual_review_sample.xlsx"
    report_md = output_path / f"{artifact_prefix}_report.md"
    summary_json = output_path / "acq_comment_retrieval_run_summary.json"

    candidate_fields = [
        "run_id", "surface_key", "platform", "surface_type", "relation", "is_post", "author_id", "retrieval", "search_query",
        "context_url", "comment_id", "post_id", "topic_id", "thread_id",
        "created_at", "comment_age_days", "candidate_usage_scope", "is_within_monitoring_window",
        "text_snapshot", "analysis_context_snapshot", "source_post_text_snapshot", "reply_parent_text_snapshot",
        "model_name", "max_length", "batch_size", "intent_set", "score", "positive_score", "negative_score",
        "scoring_method", "raw_score", "question_boost", "noise_penalty", "top_intent_phrase", "top_intent_score", "rank_global", "rank_within_surface",
        "funnel_bucket", "candidate_action_type", "destination_hint", "transport_hint", "question_signal", "candidate_noise_type",
        "intent_text_supported", "pre_llm_candidate_eligible", "llm_gate_selection_basis", "model_disagreement_bucket",
    ]
    _write_csv(candidates_csv, all_candidates, candidate_fields)
    _write_csv(profiles_csv, [{**p, "semantic_presence": json.dumps(p.get("semantic_presence"), ensure_ascii=False), "dominant_detected_interests": ",".join(p.get("dominant_detected_interests") or [])} for p in profiles], [
        "surface_key", "platform", "surface_type", "surface_title", "surface_url", "members_or_subscribers",
        "period_min_created_at", "period_max_created_at", "period_days", "period_label", "latest_comment_at",
        "latest_event_question_at", "latest_route_recommendation_at", "increment_status", "is_incremental_last_run", "discovered_at", "discovered_in_run_id",
        "days_since_latest_activity", "freshness_status", "analysis_max_comment_age_days", "stale_activity_days", "unique_commenters",
        "comments_total", "comments_embedded", "comment_records", "source_post_records",
        "comments_per_day", "comments_per_week", "comments_per_30d", "comments_per_90d",
        "latest_100_comments", "latest_100_min_created_at", "latest_100_max_created_at", "latest_100_period_days", "latest_100_period_label",
        "eligible_question_candidates", "monitoring_decision_hint", "monitoring_reason", "dominant_detected_interests", "semantic_presence",
    ])
    _write_csv(surface_summary_csv, surface_summaries, [
        "surface_key", "platform", "surface_type", "surface_title", "surface_url", "members_or_subscribers", "selection_status",
        "period_min_created_at", "period_max_created_at", "period_days", "period_label", "unique_commenters", "unique_commenters_note",
        "latest_comment_at", "latest_event_question_at", "latest_route_recommendation_at", "increment_status", "is_incremental_last_run", "discovered_at", "discovered_in_run_id",
        "days_since_latest_activity", "freshness_status", "analysis_max_comment_age_days", "stale_activity_days",
        "comments_total", "comments_embedded", "comments_per_day", "comments_per_week", "comments_per_30d", "comments_per_90d",
        "latest_100_comments", "latest_100_min_created_at", "latest_100_max_created_at", "latest_100_period_days", "latest_100_period_label",
        "recommendation", "summary_ru", "answerable_question_candidates", "answerable_questions_per_30d",
        "answerable_questions_per_90d", "answerable_questions_per_100_comments", "ask_clarification_contexts",
        "ask_contexts_per_30d", "eligible_comment_contexts", "source_post_contexts",
        "route_poi_questions", "event_questions", "event_site_search_questions", "organizer_submission_questions", "badge_filter_questions",
        "filtered_noise_contexts", "relation_counts_json", "dominant_detected_interests",
        "monitoring_decision_hint", "monitoring_reason", "answerable_examples", "ask_question_examples",
    ])
    _write_csv(question_patterns_csv, question_patterns, [
        "surface_key", "platform", "surface_type", "pattern", "candidate_action_type", "canonical_question_ru", "count", "intent_sets", "models", "example_questions", "example_urls",
    ])
    _write_csv(canonical_questions_csv, canonical_questions, [
        "pattern", "candidate_action_type", "canonical_question_ru", "real_question_examples_total",
        "monitoring_candidate_examples", "historical_calibration_examples", "surfaces_count",
        "example_questions", "example_urls", "source_note_ru",
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
        canonical_questions=canonical_questions,
        scope_rows=scope_rows,
        surface_inventory=surface_inventory,
        summary_counts=summary_counts,
        dashboard_rows=dashboard_rows,
        intent_catalog_rows=intent_catalog,
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
            "canonical_questions_csv": str(canonical_questions_csv),
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
        "canonical_questions": canonical_questions,
        "candidates": all_candidates,
        "llm_gate_candidates": gate_candidates,
        "artifacts": summary["artifacts"],
    }
