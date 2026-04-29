from __future__ import annotations

from dataclasses import dataclass
import re
import textwrap
from typing import Any

from . import writer_final_4o_family


LEGACY_CONTRACT_VERSION = "lollipop_legacy.v1"


@dataclass(slots=True)
class LegacyValidationResult:
    errors: list[str]
    warnings: list[str]


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def enhancement_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "lead_hook_fact_indexes": {"type": "ARRAY", "items": {"type": "INTEGER"}},
            "quote_candidates": {"type": "ARRAY", "items": {"type": "STRING"}},
            "extra_facts": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "text": {"type": "STRING"},
                        "source_refs": {"type": "ARRAY", "items": {"type": "STRING"}},
                    },
                    "required": ["text", "source_refs"],
                },
            },
            "writer_notes": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["lead_hook_fact_indexes", "quote_candidates", "extra_facts", "writer_notes"],
    }


def writer_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "title": {"type": "STRING"},
            "description_md": {"type": "STRING"},
            "covered_baseline_fact_indexes": {"type": "ARRAY", "items": {"type": "INTEGER"}},
            "used_extra_fact_indexes": {"type": "ARRAY", "items": {"type": "INTEGER"}},
        },
        "required": ["title", "description_md", "covered_baseline_fact_indexes", "used_extra_fact_indexes"],
    }


def build_enhancement_system_prompt() -> str:
    return textwrap.dedent(
        """
        You do one bounded step: lollipop_legacy.enhance.v1.

        Goal: preserve the baseline fact floor and add only source-grounded lollipop-style strength.

        CONTRACT
        - Input baseline_facts are mandatory factual floor. Do not rewrite or drop them.
        - Find which baseline fact indexes can act as the lead hook: rarity, format, strong action,
          atmosphere, named programme, quote-like official phrasing, or local context.
        - Add extra_facts only when the source excerpts contain grounded attendance-relevant detail
          that is absent from baseline_facts.
        - Do not add logistics as extra_facts: date, time, venue, address, tickets, prices, URLs.
        - Keep extra_facts short, Russian, and directly traceable to source_refs.
        - quote_candidates may contain short exact source phrases, not invented slogans.
        - writer_notes are compact tactical hints for the final writer, not public prose.
        - Return one JSON object only. No markdown. No commentary.
        """
    ).strip()


def build_enhancement_payload(
    *,
    title: str,
    event_type: str,
    baseline_facts: list[str],
    source_excerpt: str,
) -> dict[str, Any]:
    return {
        "title": title,
        "event_type": event_type,
        "baseline_facts": [
            {"index": idx, "text": fact}
            for idx, fact in enumerate(baseline_facts)
            if _clean_text(fact)
        ],
        "source_excerpt": source_excerpt[:9000],
    }


def normalize_enhancement_payload(payload: dict[str, Any], *, baseline_fact_count: int) -> dict[str, Any]:
    valid_indexes = set(range(max(0, baseline_fact_count)))
    lead_hook_fact_indexes: list[int] = []
    for raw in list(payload.get("lead_hook_fact_indexes") or []):
        try:
            idx = int(raw)
        except Exception:
            continue
        if idx in valid_indexes and idx not in lead_hook_fact_indexes:
            lead_hook_fact_indexes.append(idx)

    quote_candidates: list[str] = []
    for raw in list(payload.get("quote_candidates") or []):
        text = _clean_text(raw)
        if text and text not in quote_candidates:
            quote_candidates.append(text[:180])

    extra_facts: list[dict[str, Any]] = []
    for raw in list(payload.get("extra_facts") or []):
        if not isinstance(raw, dict):
            continue
        text = _clean_text(raw.get("text"))
        if not text:
            continue
        if re.search(r"(?iu)\b(?:билет|цена|руб|адрес|ссылка|https?://|\d{1,2}:\d{2})\b", text):
            continue
        refs = []
        for item in list(raw.get("source_refs") or []):
            ref = _clean_text(item)
            if ref and ref not in refs:
                refs.append(ref)
        extra_facts.append({"text": text[:360], "source_refs": refs})

    writer_notes: list[str] = []
    for raw in list(payload.get("writer_notes") or []):
        text = _clean_text(raw)
        if text and text not in writer_notes:
            writer_notes.append(text[:220])

    return {
        "lead_hook_fact_indexes": lead_hook_fact_indexes,
        "quote_candidates": quote_candidates[:5],
        "extra_facts": extra_facts[:8],
        "writer_notes": writer_notes[:8],
    }


def build_writer_system_prompt() -> str:
    return textwrap.dedent(
        f"""
        You do one bounded step: {LEGACY_CONTRACT_VERSION} final writer.

        Return one JSON object only:
        {{
          "title": "string",
          "description_md": "string",
          "covered_baseline_fact_indexes": [0],
          "used_extra_fact_indexes": [0]
        }}

        HARD CONTRACT
        - You write the final public event description in Russian.
        - The baseline_facts list is the factual floor. Semantically cover every baseline fact.
        - If a fact is awkward but important, rewrite it naturally without changing meaning.
        - Use extra_facts only when they strengthen the text without crowding out baseline facts.
        - Do not invent facts, dates, venues, prices, ticket conditions, names, or quotes.
        - Do not put date/time/location/address/ticket logistics in narrative prose.
        - Aim to be shorter than baseline_description_chars, but never drop a fact to save length.
        - If coverage requires comparable length, choose coverage over brevity.
        - The first paragraph should use the strongest grounded hook early: rarity, format,
          quote-like phrase, atmosphere, named programme, local context, or stage action.
        - Avoid report/card/promotional formulas: "посвящено", "характеризуется",
          "программа состоит из", "наполнен", "уникальная возможность", "не упустите",
          "зрители смогут насладиться", "X — это ...".
        - Keep the register: vivid cultural digest, not a dry card and not ad copy.
        - If a baseline fact contains an explicit named list, preserve the names; do not collapse to
          "и другие".
        - For programme/title lists, use markdown bullets only when the facts genuinely contain a
          list of works or items.
        - Mark covered_baseline_fact_indexes honestly. Include an index only if the output text
          semantically covers that fact.
        - Mark used_extra_fact_indexes honestly.
        """
    ).strip()


def build_writer_payload(
    *,
    title: str,
    event_type: str,
    baseline_description: str,
    baseline_facts: list[str],
    enhancement: dict[str, Any],
) -> dict[str, Any]:
    return {
        "title": title,
        "event_type": event_type,
        "baseline_description_chars": len(str(baseline_description or "")),
        "baseline_description": baseline_description,
        "baseline_facts": [
            {"index": idx, "text": fact}
            for idx, fact in enumerate(baseline_facts)
            if _clean_text(fact)
        ],
        "lead_hook_fact_indexes": list(enhancement.get("lead_hook_fact_indexes") or []),
        "quote_candidates": list(enhancement.get("quote_candidates") or []),
        "extra_facts": [
            {"index": idx, **item}
            for idx, item in enumerate(list(enhancement.get("extra_facts") or []))
            if isinstance(item, dict) and _clean_text(item.get("text"))
        ],
        "writer_notes": list(enhancement.get("writer_notes") or []),
    }


def apply_writer_output(*, title: str, output: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": _clean_text(output.get("title")) or title,
        "description_md": str(output.get("description_md") or "").strip(),
    }


def _indexes(value: Any) -> set[int]:
    result: set[int] = set()
    for item in list(value or []):
        try:
            result.add(int(item))
        except Exception:
            continue
    return result


def validate_writer_output(
    *,
    baseline_facts: list[str],
    baseline_description: str,
    enhancement: dict[str, Any],
    output: dict[str, Any],
    max_length_ratio: float = 1.15,
) -> LegacyValidationResult:
    description = str(output.get("description_md") or "")
    errors: list[str] = []
    warnings: list[str] = []

    expected_indexes = set(range(len([fact for fact in baseline_facts if _clean_text(fact)])))
    covered = _indexes(output.get("covered_baseline_fact_indexes"))
    missing = sorted(expected_indexes - covered)
    for idx in missing:
        errors.append(f"baseline_fact.missing:{idx}")
    extra_indexes = set(range(len(list(enhancement.get("extra_facts") or []))))
    used_extra = _indexes(output.get("used_extra_fact_indexes"))
    for idx in sorted(used_extra - extra_indexes):
        errors.append(f"extra_fact.unknown:{idx}")

    if not description.strip():
        errors.append("description.empty")
    if re.search(r"(?iu)\b(?:6|12|16|18)\+\b|возрастн", description):
        errors.append("age.leak")
    if re.search(r"(?iu)\b(?:http|www\.|билет\w*|руб(?:\.|л|лей)?|₽)\b", description):
        warnings.append("logistics_or_ticket_language")

    quality = writer_final_4o_family._describe_text_quality(description)
    for label in quality["report_formula_hits"]:
        errors.append(f"style.report_formula:{label}")
    for label in quality["promo_phrase_hits"]:
        errors.append(f"style.promo_phrase:{label}")
    if quality["lead_meta_opening"]:
        errors.append("lead.meta_opening")
    if re.search(r"(?iu)зрител\w+[^.!?\n]{0,36}(?:смогут|могут)\s+наслад", description):
        errors.append("style.audience_template:will_enjoy")

    baseline_len = len(str(baseline_description or ""))
    if baseline_len and len(description) > int(baseline_len * max_length_ratio):
        warnings.append(f"length.above_baseline_ratio:{len(description)}/{baseline_len}")
    if not quality["lead_hook_signals"]:
        warnings.append("lead_hook_signal.absent")

    return LegacyValidationResult(errors=errors, warnings=warnings)
