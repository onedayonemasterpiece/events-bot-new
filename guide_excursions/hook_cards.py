"""LLM pipeline that turns digest excursions into VK hook cards.

Stage 1 (generate + select, one GPT-4o call): given the digest occurrences with
their grounded facts, the model writes a single marketing curiosity hook (a
question or intriguing phrase) per promising excursion and returns only the
strongest, most *diverse* ``cap`` of them — exactly the same "curiosity hook"
voice used by the announce rewrite (``ask_4o``), but compressed to one phrase.

Stage 2 (deterministic): sanitize each hook (no emoji / URLs / prices / dates),
assign a distinct high-contrast palette, and hand off to
:mod:`guide_excursions.hook_card_render` for rasterization.

The whole thing is best-effort and additive: callers must treat any failure here
as "no cards" and still publish the digest with its afishas.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Sequence

from .hook_card_render import CardPalette, load_palettes, main_fit_px, render_hook_card
from .parser import collapse_ws

logger = logging.getLogger(__name__)

# Total images in the VK grid we are willing to publish (afishas + cards).
HOOK_CARD_MAX_TOTAL_IMAGES = 9
# Soft cap on how many cards we add even when many slots are free.
HOOK_CARD_MAX_CARDS = 3
# Bound the prompt: only the first N occurrences are offered to the model.
HOOK_CARD_MAX_CANDIDATES = 12

HOOK_MAX_CHARS = 90
HOOK_MIN_WORDS = 3
HOOK_MAX_WORDS = 14
SUBLINE_MAX_CHARS = 32
SUBLINE_MAX_WORDS = 5

_EMOJI_RE = re.compile(
    "["
    "\U0001f300-\U0001faff"
    "\U00002600-\U000027bf"
    "\U0001f000-\U0001f0ff"
    "\U0000fe00-\U0000fe0f"
    "\U00002190-\U000021ff"
    "\U00002b00-\U00002bff"
    "\U0000200d"
    "]",
    flags=re.UNICODE,
)
_URLISH_RE = re.compile(r"(https?://|www\.|t\.me/|vk\.com/|@[A-Za-z0-9_]{3,}|[\w.-]+\.(ru|com|me)\b)", re.IGNORECASE)
_PHONE_RE = re.compile(r"(?:\+7|\b8)\s?[\d\-\s()]{7,}")
_PRICE_RE = re.compile(r"\d[\d\s.]*\s*(?:₽|руб|р\.|рублей|eur|€|\$)", re.IGNORECASE)
_TIME_RE = re.compile(r"\b\d{1,2}[:.]\d{2}\b")
_DATE_RE = re.compile(r"\b\d{1,2}\s*(?:янв|фев|мар|апр|мая|июн|июл|авг|сен|окт|ноя|дек)", re.IGNORECASE)


@dataclass(frozen=True)
class HookCard:
    occurrence_id: int
    main_text: str
    sub_text: str | None
    theme: str
    palette: CardPalette
    main_px: int | None = None  # shared main type size across the publication

    def render_png(self) -> bytes:
        return render_hook_card(
            main_text=self.main_text,
            sub_text=self.sub_text,
            palette=self.palette,
            main_px=self.main_px,
        )


HOOK_CARDS_RESPONSE_FORMAT: dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "GuideHookCards",
        "schema": {
            "type": "object",
            "properties": {
                "cards": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "occurrence_id": {"type": "integer"},
                            "hook": {"type": "string"},
                            "theme": {"type": "string"},
                            "strength": {"type": "integer"},
                        },
                        "required": ["occurrence_id", "hook", "theme", "strength"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["cards"],
            "additionalProperties": False,
        },
    },
}

_SYSTEM_PROMPT = (
    "Ты — редактор VK-канала об экскурсиях и прогулках по Калининграду и области. "
    "Пишешь короткие маркетинговые крючки для карточек-картинок в VK: одна цепляющая фраза, "
    "которая вызывает любопытство и желание открыть дайджест целиком. "
    "Это тот же приём, что и крючок-первая-фраза в рерайте анонса, только ещё короче — в одну фразу. "
    "Только факты из входных данных, без выдумок и без хайпа. Верни только JSON по схеме."
)


def _strip_emoji(text: str) -> str:
    return _EMOJI_RE.sub("", text)


def _normalize_text(value: Any) -> str:
    return collapse_ws("" if value is None else str(value))


def _word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", text))


def sanitize_hook(value: Any) -> str | None:
    """Return a clean card hook, or ``None`` if it violates the card contract."""
    text = _normalize_text(_strip_emoji("" if value is None else str(value)))
    text = text.strip("\"'«»“”").strip()
    if not text:
        return None
    if _URLISH_RE.search(text) or _PHONE_RE.search(text):
        return None
    if _PRICE_RE.search(text) or _TIME_RE.search(text) or _DATE_RE.search(text):
        return None
    # Cards must be a question hook (same principle as the announce rewrite hook):
    # exactly one curiosity question, ending with "?", no statements.
    if not text.endswith("?") or text.count("?") != 1:
        return None
    words = _word_count(text)
    if words < HOOK_MIN_WORDS or words > HOOK_MAX_WORDS:
        return None
    if len(text) > HOOK_MAX_CHARS:
        return None
    return text


def sanitize_subline(value: Any) -> str | None:
    text = _normalize_text(_strip_emoji("" if value is None else str(value))).strip(" .,:;!?\"'«»").strip()
    if not text:
        return None
    if _URLISH_RE.search(text) or _PHONE_RE.search(text) or _PRICE_RE.search(text):
        return None
    if _word_count(text) > SUBLINE_MAX_WORDS or len(text) > SUBLINE_MAX_CHARS:
        return None
    return text


def select_post_palette(seed: int = 0) -> CardPalette:
    """Pick ONE palette for the whole publication, deterministic per ``seed``.

    All cards in a single VK post share this palette; a different ``seed``
    (issue id / day) rotates to a different colour, so different publication
    days look different while one day stays consistent.
    """
    palettes = list(load_palettes())
    return palettes[abs(int(seed)) % len(palettes)]


_RU_MONTH_GEN = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня",
    7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def _short_date(value: Any) -> str | None:
    text = _normalize_text(value)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        month = _RU_MONTH_GEN.get(mo)
        if month:
            return f"{d} {month}"
    return None


def _format_card_subline(row: Mapping[str, Any]) -> str | None:
    """Card footer: date + guide (who leads it), as short as possible."""
    date = _short_date(row.get("date"))
    guides = row.get("guide_names") if isinstance(row.get("guide_names"), Sequence) else []
    guide = next((_normalize_text(g) for g in list(guides) if _normalize_text(g)), None)
    if guide and len(guide) > 24:
        guide = guide.split()[0][:24]
    parts = [p for p in (date, guide) if p]
    return " · ".join(parts) if parts else None


def _card_payload_row(row: Mapping[str, Any]) -> dict[str, Any]:
    fact_pack = row.get("fact_pack") if isinstance(row.get("fact_pack"), Mapping) else {}
    guide_names = row.get("guide_names") if isinstance(row.get("guide_names"), Sequence) else []
    audience = row.get("audience_fit") if isinstance(row.get("audience_fit"), Sequence) else []
    payload = {
        "occurrence_id": int(row.get("occurrence_id") or row.get("id") or 0),
        "title": _normalize_text(row.get("canonical_title")),
        "summary": _normalize_text(row.get("summary_one_liner") or row.get("digest_blurb")),
        "route": _normalize_text(row.get("route_summary")),
        "city": _normalize_text(row.get("city")),
        "guides": [_normalize_text(g) for g in list(guide_names)[:3] if _normalize_text(g)],
        "audience": [_normalize_text(a) for a in list(audience)[:4] if _normalize_text(a)],
        "main_hook": _normalize_text(fact_pack.get("main_hook")) if isinstance(fact_pack, Mapping) else "",
    }
    return {k: v for k, v in payload.items() if v not in ("", [], None)}


def _build_prompt(payload_rows: Sequence[Mapping[str, Any]], *, cap: int) -> str:
    return (
        f"Ниже {len(payload_rows)} экскурсий/прогулок недели, готовых к дайджесту. "
        f"Для самых сильных придумай крючок и верни НЕ БОЛЕЕ {cap} карточек — только самые "
        "цепляющие и РАЗНОТИПНЫЕ (разные темы/форматы, не похожие друг на друга). "
        "Не обязательно охватывать все экскурсии.\n\n"
        "Поле hook:\n"
        "- ОБЯЗАТЕЛЬНО вопрос-крючок, заканчивающийся знаком «?» (как первая фраза-крючок в рерайте "
        "анонса) — сильный маркетинговый вопрос, вызывающий любопытство; НЕ утверждение и НЕ заголовок;\n"
        "- ровно один вопрос, без второго предложения;\n"
        "- 4–12 слов, идеально 5–9; это текст КРУПНО на картинке, поэтому коротко и ёмко;\n"
        "- строго на основе данных экскурсии (особенно полей `main_hook`, `route`, `summary`); "
        "не выдумывай конкретные объекты, проекты, имена или события, которых нет во входных данных;\n"
        "- сохраняй доминирующий термин: прогулка остаётся прогулкой, экскурсия — экскурсией, "
        "джип-тур/выезд не превращай в экскурсию;\n"
        "- без эмодзи, без URL, имён пользователей, телефонов, цен, дат, времени и призывов «записаться/купить билет»;\n"
        "- не повторяй дословно название — заинтригуй.\n"
        "Поле theme: короткий тег темы (история, природа, архитектура, море, индустриальное и т.п.) — для разнообразия.\n"
        "Поле strength: 0..100, насколько крючок цепляет. Сортируй карточки по убыванию strength.\n\n"
        f"Экскурсии (JSON):\n{json.dumps(list(payload_rows), ensure_ascii=False)}"
    )


async def generate_hook_cards(
    rows: Sequence[Mapping[str, Any]],
    *,
    existing_image_count: int,
    max_total: int = HOOK_CARD_MAX_TOTAL_IMAGES,
    max_cards: int = HOOK_CARD_MAX_CARDS,
    seed: int = 0,
    ask_fn: Callable[..., Awaitable[str]] | None = None,
) -> list[HookCard]:
    """Generate up to ``cap`` sanitized, palette-assigned hook cards for a digest.

    ``cap = max(0, min(max_total - existing_image_count, max_cards))`` — cards only
    fill VK-grid slots not already taken by real afishas.
    """
    cap = max(0, min(max_total - max(0, int(existing_image_count)), max_cards))
    if cap <= 0 or not rows:
        logger.info(
            "hook_cards.skip existing_images=%s cap=%s rows=%s",
            existing_image_count,
            cap,
            len(rows),
        )
        return []

    payload_rows = [p for p in (_card_payload_row(r) for r in rows) if p.get("occurrence_id")]
    payload_rows = payload_rows[:HOOK_CARD_MAX_CANDIDATES]
    if not payload_rows:
        logger.info("hook_cards.skip reason=no_payload_rows")
        return []

    if ask_fn is None:
        from main import ask_4o  # type: ignore

        ask_fn = ask_4o

    prompt = _build_prompt(payload_rows, cap=cap)
    try:
        raw = await ask_fn(
            prompt,
            system_prompt=_SYSTEM_PROMPT,
            response_format=HOOK_CARDS_RESPONSE_FORMAT,
            max_tokens=700,
            temperature=0.6,
            meta={"feature": "guide_hook_cards", "candidates": len(payload_rows), "cap": cap},
        )
    except Exception as exc:
        logger.warning("hook_cards.llm_failed: %s", exc)
        return []

    try:
        parsed = json.loads(raw or "{}")
        items = parsed.get("cards") if isinstance(parsed, Mapping) else None
    except Exception as exc:
        logger.warning("hook_cards.parse_failed: %s preview=%s", exc, (raw or "")[:200])
        return []
    if not isinstance(items, list):
        logger.warning("hook_cards.bad_payload cards_not_list")
        return []

    by_id: dict[int, Mapping[str, Any]] = {}
    for r in rows:
        oid = int(r.get("occurrence_id") or r.get("id") or 0)
        if oid:
            by_id[oid] = r
    valid_ids = {int(p["occurrence_id"]) for p in payload_rows}
    items = sorted(
        items,
        key=lambda it: int(it.get("strength") or 0) if isinstance(it, Mapping) else 0,
        reverse=True,
    )

    # One palette per publication; rotates by seed (issue id / day).
    palette = select_post_palette(seed=seed)

    cards: list[HookCard] = []
    seen_occ: set[int] = set()
    seen_hooks: set[str] = set()
    rejected = 0
    for item in items:
        if len(cards) >= cap:
            break
        if not isinstance(item, Mapping):
            continue
        occ_id = int(item.get("occurrence_id") or 0)
        if occ_id not in valid_ids or occ_id in seen_occ:
            continue
        hook = sanitize_hook(item.get("hook"))
        if not hook:
            rejected += 1
            continue
        dedup_key = hook.casefold()
        if dedup_key in seen_hooks:
            continue
        # Footer is deterministic: date + guide (who leads it), not a slogan.
        sub = _format_card_subline(by_id[occ_id])
        theme = _normalize_text(item.get("theme"))[:40]
        seen_occ.add(occ_id)
        seen_hooks.add(dedup_key)
        cards.append(
            HookCard(
                occurrence_id=occ_id,
                main_text=hook,
                sub_text=sub,
                theme=theme,
                palette=palette,
            )
        )

    if not cards:
        logger.info("hook_cards.empty rejected=%s returned=0", rejected)
        return []

    # One shared main type size for the whole post (smallest that fits every
    # card) so adjacent cards don't look like different weights/widths.
    shared_main_px = min(main_fit_px(c.main_text) for c in cards)
    cards = [
        HookCard(
            occurrence_id=c.occurrence_id,
            main_text=c.main_text,
            sub_text=c.sub_text,
            theme=c.theme,
            palette=c.palette,
            main_px=shared_main_px,
        )
        for c in cards
    ]

    logger.info(
        "hook_cards.generated cap=%s returned=%s rejected=%s occ=%s palette=%s main_px=%s",
        cap,
        len(cards),
        rejected,
        [c.occurrence_id for c in cards],
        palette.id,
        shared_main_px,
    )
    return cards
