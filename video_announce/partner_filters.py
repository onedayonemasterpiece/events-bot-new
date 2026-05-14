"""Partner-track eligibility filters.

Two filters live here today, one per partner track:

* ``kaliningrad_region_east`` — deterministic geo gate keyed on ``event.city``
  with ``exclude_over_include`` priority. The Event model only stores ``city``
  (no municipality / settlement columns), so this layer matches against the
  spec's included-settlements list and rejects coastal/resort settlements and
  Kaliningrad/Guryevsk.

* ``eco_prirodnaya`` — LLM-first editorial filter on Gemma 4. The classifier
  returns one of ``matched`` / ``manual_review`` / ``exclude``. For the
  partner-track pipeline, ``manual_review`` is admitted with a warning and
  ``exclude`` removes the candidate. Deterministic keyword recall is used only
  as a fast pre-pass to skip events that are not even plausibly in scope, never
  to override an LLM include.
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Iterable

from models import Event

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared decision shape
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FilterDecision:
    event_id: int
    matched: bool
    needs_manual_review: bool
    reason: str
    extra: dict[str, Any]


# ---------------------------------------------------------------------------
# kaliningrad_region_east
# ---------------------------------------------------------------------------


_EAST_INCLUDED_SETTLEMENTS: frozenset[str] = frozenset(
    s.casefold()
    for s in (
        # core cities
        "Советск",
        "Черняховск",
        "Гусев",
        "Гвардейск",
        # small cities
        "Багратионовск",
        "Краснознаменск",
        "Ладушкин",
        "Мамоново",
        "Неман",
        "Нестеров",
        "Озёрск",
        "Озерск",
        "Полесск",
        "Правдинск",
        "Славск",
        # tourist / event settlements
        "Железнодорожный",
        "Знаменск",
        "Талпаки",
        "Большаково",
        "Добровольск",
        "Ясная Поляна",
        "Краснолесье",
        "Чистые Пруды",
        "Ильинское",
        "Ольховатка",
        "Междуречье",
        "Лунино",
        "Ульяново",
        "Маяковское",
        "Крылово",
        "Домново",
        "Дружба",
        "Суворово",
        "Пушкарёво",
        "Пушкарево",
    )
)

# Родники is ambiguous (per spec it appears in both lists). Because the filter
# is exclude_over_include, we treat it as excluded here unless future structured
# geo disambiguation proves otherwise.
_EAST_EXCLUDED_SETTLEMENTS: frozenset[str] = frozenset(
    s.casefold()
    for s in (
        "Калининград",
        "Гурьевск",
        "Большое Исаково",
        "Малое Исаково",
        "Васильково",
        "Храброво",
        "Невское",
        "Кутузово",
        "Родники",
        # coast and resort zone
        "Балтийск",
        "Приморск",
        "Янтарный",
        "Светлогорск",
        "Отрадное",
        "Приморье",
        "Пионерский",
        "Зеленоградск",
        "Лесной",
        "Рыбачий",
        "Морское",
        "Заостровье",
        "Малиновка",
        "Куликово",
        "Романово",
        "Переславское",
        "Светлый",
        "Взморье",
        "Люблино",
    )
)

_LOCATION_PREFIX_RE = re.compile(
    r"^\s*(?:г\.|гор\.|город|пос\.|посёлок|поселок|пгт\.?|п\.|с\.|село|д\.|деревня)\s+",
    flags=re.IGNORECASE,
)


def _normalize_settlement(raw: str | None) -> str:
    if not raw:
        return ""
    cleaned = str(raw).strip()
    cleaned = _LOCATION_PREFIX_RE.sub("", cleaned)
    cleaned = cleaned.replace("ё", "е").replace("Ё", "Е")
    return cleaned.casefold()


def _location_settlement_candidates(event: Event) -> list[str]:
    out: list[str] = []
    for candidate in (
        getattr(event, "city", None),
        getattr(event, "location_address", None),
        getattr(event, "location_name", None),
    ):
        if not candidate:
            continue
        # location_name/address may carry comma-separated parts; the city
        # value is typically the cleanest signal, but we still take the
        # leading chunk as a hint.
        for part in str(candidate).split(","):
            normalized = _normalize_settlement(part)
            if normalized:
                out.append(normalized)
    return out


def _matches_settlement_set(candidates: Iterable[str], reference: frozenset[str]) -> str | None:
    normalized_ref = {s.replace("ё", "е") for s in reference}
    for candidate in candidates:
        if candidate in normalized_ref:
            return candidate
    return None


def classify_event_kaliningrad_region_east(event: Event) -> FilterDecision:
    event_id = int(getattr(event, "id", 0) or 0)
    candidates = _location_settlement_candidates(event)
    if not candidates:
        return FilterDecision(
            event_id=event_id,
            matched=False,
            needs_manual_review=False,
            reason="no_location",
            extra={},
        )
    excluded_hit = _matches_settlement_set(candidates, _EAST_EXCLUDED_SETTLEMENTS)
    if excluded_hit:
        return FilterDecision(
            event_id=event_id,
            matched=False,
            needs_manual_review=False,
            reason=f"exclude:{excluded_hit}",
            extra={"matched_settlement": excluded_hit},
        )
    included_hit = _matches_settlement_set(candidates, _EAST_INCLUDED_SETTLEMENTS)
    if included_hit:
        return FilterDecision(
            event_id=event_id,
            matched=True,
            needs_manual_review=False,
            reason=f"include:{included_hit}",
            extra={"matched_settlement": included_hit},
        )
    return FilterDecision(
        event_id=event_id,
        matched=False,
        needs_manual_review=False,
        reason="not_in_east",
        extra={},
    )


# ---------------------------------------------------------------------------
# eco_prirodnaya — Gemma 4 LLM-first classifier
# ---------------------------------------------------------------------------


# Deterministic recall pre-filter: if NONE of these substrings appear in the
# combined event text, we still pass the event to the LLM (recall-only) since
# the spec mandates LLM-owned final decisions. The keyword list mirrors the
# keyword families documented in the spec.
ECO_KEYWORD_HINTS: tuple[str, ...] = (
    "своп", "обмен вещ", "обмен одежд", "обмен книг", "обмен растен",
    "фримаркет", "барахолк", "гаражная распродаж", "second hand", "секонд-хенд",
    "переработк", "раздельный сбор", "макулатур", "вторсырь", "ресайкл",
    "апсайкл", "ремонт", "zero waste", "экологичн", "устойчив",
    "субботник", "уборк", "эковолонтёр", "эковолонтер", "посадк",
    "озеленен", "восстановлени троп", "помощь природе", "защит природ",
    "природ", "ботаническ", "орнитолог", "флор", "фаун", "ландшафт",
    "куршск", "виштынец", "роминтск", "балтийск ", "дюн", "заказник",
    "заповедник", "национальн парк",
    "краеведен", "история кра", "история регион", "восточн прусс",
    "культурное наследи", "историческое наследи", "усадьб", "кирх",
    "замок", "фортификац", "архитектурное наследи", "немецкое наследи",
    "выставка о природ", "фотовыставк", "ботаническ иллюстрац",
    "экологическое искусств", "пейзаж", "ландшафтн живопис",
    "растени", "садоводств", "огород", "рассад", "семен",
    "животн", "птиц", "приют", "защит животн", "кормушк", "насеком", "экосистем",
    "экопросвещен", "экологическ лекци", "школа экологи", "бережное отношение к природ",
    "музей", "историко-художеств",
)


# JSON schema given to Gemma 4 via the native structured-output path.
ECO_NATIVE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "matched": {"type": "boolean"},
        "score": {"type": "integer"},
        "decision": {
            "type": "string",
            "enum": ["matched", "manual_review", "exclude"],
        },
        "matched_categories": {
            "type": "array",
            "items": {"type": "string"},
        },
        "matched_keywords": {
            "type": "array",
            "items": {"type": "string"},
        },
        "reason": {"type": "string"},
        "needs_manual_review": {"type": "boolean"},
    },
    "required": ["decision", "reason"],
}


ECO_SYSTEM_PROMPT = (
    "Ты строгий редактор партнёрского трека CherryFlash «эко-природная». "
    "Тебе дают одно событие: название, описание, организатор/источник, "
    "место/адрес. Реши, относится ли событие к одной из тем фильтра: "
    "экология, природа, устойчивое потребление, краеведение и культурное "
    "наследие Калининградской области. Локальная история и наследие — "
    "первоклассный путь матча наравне с природой и экологией.\n\n"
    "ВКЛЮЧАЙ:\n"
    "- свопы, барахолки, фримаркеты, второй ход вещей;\n"
    "- переработку, ремонт, апсайклинг, zero waste, осознанное потребление;\n"
    "- субботники, посадки, эковолонтёрство;\n"
    "- прогулки/экскурсии/лекции о природе Калининградской области, флоре, "
    "фауне, орнитологии, ландшафтах, заказниках, нацпарках;\n"
    "- краеведение, история городов и посёлков, усадьбы, кирхи, замки, "
    "фортификация, Восточная Пруссия, культурное наследие;\n"
    "- ботаническое искусство, выставки о природе/крае, природная "
    "фотография, документальное кино о природе;\n"
    "- растения, садоводство, обмен растениями;\n"
    "- животные, птицы, помощь животным, экопросвещение;\n"
    "- зоо-события, где главное — животные/природа/просвещение, а не "
    "развлечение.\n\n"
    "ИСКЛЮЧАЙ:\n"
    "- обычные коммерческие ярмарки/маркеты без re-use или краеведения;\n"
    "- события, где природа лишь фон/место;\n"
    "- обычные концерты/пикники/спорт в парке без эко/краеведческой темы;\n"
    "- развлечения с животными (контактные зоопарки, фото с животными);\n"
    "- концерты/лекции в музее без краеведческой/экологической темы;\n"
    "- охота, трофейная рыбалка, джип-туры, гонки в природе.\n\n"
    "Скоринг ориентир: +3 если тема явно в названии; +2 если описание "
    "ясно раскрывает тему; +2 если организатор — музей/заповедник/эко-"
    "центр; +1 если место — музей/парк/охраняемая природа; -3 за прямое "
    "исключение; -2 если природа только фон. matched при score>=3, "
    "manual_review при score==2, exclude при score<2.\n\n"
    "Ответ — JSON по схеме."
)


def _eco_event_text(event: Event) -> str:
    parts: list[str] = []
    for label, attr in (
        ("Название", "title"),
        ("Описание", "description"),
        ("Дайджест", "search_digest"),
        ("Краткое описание", "short_description"),
        ("Тип события", "event_type"),
        ("Место", "location_name"),
        ("Адрес", "location_address"),
        ("Город", "city"),
        ("Источник", "source_label"),
        ("URL", "source_post_url"),
    ):
        raw = getattr(event, attr, None)
        if raw:
            parts.append(f"{label}: {str(raw).strip()}")
    return "\n".join(parts)


def _has_keyword_hint(text: str) -> bool:
    casefolded = text.casefold()
    return any(hint in casefolded for hint in ECO_KEYWORD_HINTS)


def _parse_eco_response(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}


def _decision_from_payload(payload: dict[str, Any]) -> tuple[str, str, list[str], list[str]]:
    raw_decision = str(payload.get("decision") or "").strip().lower()
    if raw_decision not in {"matched", "manual_review", "exclude"}:
        if payload.get("needs_manual_review"):
            raw_decision = "manual_review"
        elif bool(payload.get("matched")):
            raw_decision = "matched"
        else:
            raw_decision = "exclude"
    reason = str(payload.get("reason") or "").strip()
    cats = [str(x).strip() for x in (payload.get("matched_categories") or []) if str(x).strip()]
    kws = [str(x).strip() for x in (payload.get("matched_keywords") or []) if str(x).strip()]
    return raw_decision, reason, cats, kws


async def classify_event_eco_prirodnaya(
    event: Event,
    *,
    llm_call,
) -> FilterDecision:
    """Classify an event for the eco_prirodnaya partner filter.

    ``llm_call`` is an async callable that receives ``(system_prompt, user_text,
    schema)`` and returns the raw structured response (dict or JSON string).
    The caller wires this to the project Gemma 4 gateway; this module stays
    transport-agnostic so it can be unit-tested with a stub.
    """
    event_id = int(getattr(event, "id", 0) or 0)
    text = _eco_event_text(event)
    if not text.strip():
        return FilterDecision(
            event_id=event_id,
            matched=False,
            needs_manual_review=True,
            reason="empty_event_text",
            extra={"matched_keywords": [], "matched_categories": []},
        )
    has_hint = _has_keyword_hint(text)
    try:
        raw = await llm_call(ECO_SYSTEM_PROMPT, text, ECO_NATIVE_SCHEMA)
    except Exception as exc:  # noqa: BLE001 — classifier is best-effort
        logger.warning(
            "video_announce.partner_filters: eco_prirodnaya LLM call failed event_id=%s err=%s",
            event_id,
            exc,
        )
        return FilterDecision(
            event_id=event_id,
            matched=False,
            needs_manual_review=True,
            reason=f"llm_error:{type(exc).__name__}",
            extra={"keyword_hint": has_hint},
        )
    payload = _parse_eco_response(raw)
    decision, reason, cats, kws = _decision_from_payload(payload)
    matched = decision == "matched"
    needs_review = decision == "manual_review"
    return FilterDecision(
        event_id=event_id,
        matched=matched,
        needs_manual_review=needs_review,
        reason=reason or decision,
        extra={
            "decision": decision,
            "matched_categories": cats,
            "matched_keywords": kws,
            "keyword_hint": has_hint,
            "score": payload.get("score"),
        },
    )


# ---------------------------------------------------------------------------
# Gemma 4 transport adapter
# ---------------------------------------------------------------------------


PARTNER_FILTER_GEMMA_MODEL_ENV = "PARTNER_FILTER_GEMMA_MODEL"
PARTNER_FILTER_GEMMA_MODEL_DEFAULT = "gemma-4-31b-it"


def _strip_code_fence(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    if raw.startswith("```"):
        # ``` or ```json
        first_nl = raw.find("\n")
        if first_nl != -1:
            raw = raw[first_nl + 1 :]
        if raw.endswith("```"):
            raw = raw[: -3]
    return raw.strip()


def _extract_json_object(text_value: str) -> dict[str, Any] | None:
    raw = _strip_code_fence(text_value)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        data = json.loads(raw[start : end + 1])
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def make_eco_gemma_llm_call(gemma_client: Any | None):
    """Return an async callable suitable as ``llm_call`` for the eco classifier.

    The returned coroutine wraps the project Gemma 4 gateway with native JSON
    schema output (Gemma 4 supports ``response_mime_type`` /
    ``response_schema``). Caller passes the resolved
    :class:`google_ai.client.GoogleAIClient` instance, or ``None`` to make
    every classification request fail fast (and the partner filter will mark
    the event ``needs_manual_review``).
    """
    if gemma_client is None:
        async def _missing(*_a, **_kw):  # noqa: ANN001
            raise RuntimeError("Gemma 4 client is not configured")

        return _missing

    model = (os.getenv(PARTNER_FILTER_GEMMA_MODEL_ENV) or "").strip() or PARTNER_FILTER_GEMMA_MODEL_DEFAULT

    async def _call(system_prompt: str, user_text: str, schema: dict[str, Any]) -> dict[str, Any]:
        full_prompt = f"{system_prompt}\n\n---\n\nСобытие:\n{user_text}"
        config: dict[str, Any] = {
            "temperature": 0.0,
            "response_mime_type": "application/json",
            "response_schema": schema,
        }
        raw, _usage = await gemma_client.generate_content_async(
            model=model,
            prompt=full_prompt,
            generation_config=config,
            max_output_tokens=512,
        )
        parsed = _extract_json_object(str(raw or ""))
        if parsed is None:
            raise RuntimeError("Gemma 4 returned non-JSON for eco classifier")
        return parsed

    return _call
