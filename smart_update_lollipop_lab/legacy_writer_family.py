from __future__ import annotations

from dataclasses import dataclass
import re
import textwrap
from typing import Any

from . import writer_final_4o_family


LEGACY_CONTRACT_VERSION = "lollipop_legacy.v2"


@dataclass(slots=True)
class LegacyValidationResult:
    errors: list[str]
    warnings: list[str]


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


# Narrator-frame openers: stock copywriter leads that frame the event from
# the narrator's pose ("X plunges you into / introduces you to ...") instead
# of opening with a concrete object, named actor, quote, or event action.
# Anchored to the very start of the lead paragraph.
NARRATOR_FRAME_LEAD_PATTERNS: tuple[str, ...] = (
    r"(?iu)^[\s>«\"`*_-]*Погружение\s+в\b",
    r"(?iu)^[\s>«\"`*_-]*Знакомство\s+с\b",
    r"(?iu)^[\s>«\"`*_-]*Путешестви[ея]\s+в\s+мир\b",
    r"(?iu)^[\s>«\"`*_-]*Прогулк[аи]\s+по\s+миру\b",
    r"(?iu)^[\s>«\"`*_-]*Окуни(?:тесь|сь)\s+в\b",
    r"(?iu)^[\s>«\"`*_-]*Откройте\s+для\s+себя\b",
    r"(?iu)^[\s>«\"`*_-]*Добро\s+пожаловать\s+в\b",
    r"(?iu)^[\s>«\"`*_-]*Приготовьтесь\s+(?:к|погрузиться)\b",
)


def _lead_paragraph(text: str) -> str:
    body = str(text or "").strip()
    if not body:
        return ""
    # Skip a leading blockquote epigraph if it sits as its own paragraph.
    paragraphs = [part for part in re.split(r"\n\s*\n", body) if part.strip()]
    for part in paragraphs:
        stripped = part.strip()
        if stripped.startswith(">"):
            continue
        return stripped
    return paragraphs[0].strip() if paragraphs else ""


def _has_narrator_frame_opening(text: str) -> bool:
    lead = _lead_paragraph(text)
    if not lead:
        return False
    first_line = lead.split("\n", 1)[0]
    return any(re.search(pattern, first_line) for pattern in NARRATOR_FRAME_LEAD_PATTERNS)


def _heading_count(text: str) -> int:
    return len(re.findall(r"(?m)^###\s+\S", str(text or "")))


def _has_blockquote_epigraph(text: str) -> bool:
    body = str(text or "").lstrip()
    if not body:
        return False
    first_line = body.split("\n", 1)[0].strip()
    return first_line.startswith(">")


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


def _text_quality_score(description: str) -> int:
    quality = writer_final_4o_family._describe_text_quality(description)
    score = 0
    if quality["lead_hook_signals"]:
        score += 3
    score -= 4 * len(quality["report_formula_hits"])
    score -= 5 * len(quality["promo_phrase_hits"])
    if quality["lead_meta_opening"]:
        score -= 6
    if quality["poster_leak"]:
        score -= 4
    if quality["age_leak"]:
        score -= 4
    if _has_narrator_frame_opening(description):
        score -= 3
    return score


def compare_to_baseline(
    *,
    baseline_description: str,
    candidate_description: str,
) -> dict[str, Any]:
    """Deterministic quality guard for the benchmark contract.

    It is intentionally narrow: semantic/factual coverage remains LLM-owned via
    the writer schema, while this guard catches measurable public-text regressions.
    """

    baseline = str(baseline_description or "")
    candidate = str(candidate_description or "")
    baseline_quality = writer_final_4o_family._describe_text_quality(baseline)
    candidate_quality = writer_final_4o_family._describe_text_quality(candidate)
    baseline_score = _text_quality_score(baseline)
    candidate_score = _text_quality_score(candidate)
    baseline_len = len(baseline)
    candidate_len = len(candidate)
    length_ratio = round(candidate_len / baseline_len, 4) if baseline_len else None

    regressions: list[str] = []
    improvements: list[str] = []
    warnings: list[str] = []

    baseline_narrator_frame = _has_narrator_frame_opening(baseline)
    candidate_narrator_frame = _has_narrator_frame_opening(candidate)
    baseline_headings = _heading_count(baseline)
    candidate_headings = _heading_count(candidate)
    baseline_epigraph = _has_blockquote_epigraph(baseline)
    candidate_epigraph = _has_blockquote_epigraph(candidate)

    if len(candidate_quality["report_formula_hits"]) > len(baseline_quality["report_formula_hits"]):
        regressions.append("quality.report_formula_regression")
    if len(candidate_quality["promo_phrase_hits"]) > len(baseline_quality["promo_phrase_hits"]):
        regressions.append("quality.promo_phrase_regression")
    if candidate_quality["lead_meta_opening"] and not baseline_quality["lead_meta_opening"]:
        regressions.append("quality.lead_meta_regression")
    if candidate_quality["poster_leak"] and not baseline_quality["poster_leak"]:
        regressions.append("quality.poster_leak_regression")
    if candidate_quality["age_leak"] and not baseline_quality["age_leak"]:
        regressions.append("quality.age_leak_regression")
    if baseline_quality["lead_hook_signals"] and not candidate_quality["lead_hook_signals"]:
        regressions.append("quality.lost_lead_hook")
    if candidate_narrator_frame and not baseline_narrator_frame:
        regressions.append("quality.narrator_frame_opening")
    if baseline_len and candidate_len < int(baseline_len * 0.68):
        regressions.append(f"quality.too_short_vs_baseline:{candidate_len}/{baseline_len}")
    if baseline_len and candidate_len > int(baseline_len * 1.12):
        warnings.append(f"quality.longer_than_baseline:{candidate_len}/{baseline_len}")
    if baseline_headings >= 2 and candidate_headings == 0:
        warnings.append(f"quality.lost_baseline_headings:{baseline_headings}")
    if baseline_epigraph and not candidate_epigraph:
        warnings.append("quality.lost_baseline_epigraph")

    if candidate_score > baseline_score:
        improvements.append("quality.score_improved")
    if len(candidate_quality["lead_hook_signals"]) > len(baseline_quality["lead_hook_signals"]):
        improvements.append("quality.lead_hook_improved")
    if baseline_len and candidate_len < baseline_len and candidate_len >= int(baseline_len * 0.68):
        improvements.append("quality.more_compact")
    if len(candidate_quality["report_formula_hits"]) < len(baseline_quality["report_formula_hits"]):
        improvements.append("quality.report_formula_reduced")
    if len(candidate_quality["promo_phrase_hits"]) < len(baseline_quality["promo_phrase_hits"]):
        improvements.append("quality.promo_phrase_reduced")
    if baseline_narrator_frame and not candidate_narrator_frame:
        improvements.append("quality.narrator_frame_avoided")

    status = "regressed" if regressions else ("improved" if improvements else "no_worse")
    return {
        "status": status,
        "baseline_score": baseline_score,
        "candidate_score": candidate_score,
        "length_ratio": length_ratio,
        "regressions": regressions,
        "improvements": improvements,
        "warnings": warnings,
        "baseline_quality": baseline_quality,
        "candidate_quality": candidate_quality,
    }


def build_enhancement_system_prompt() -> str:
    return textwrap.dedent(
        """
        You do one bounded step: lollipop_legacy.enhance.v1.

        Goal: preserve the full baseline fact floor and add only source-grounded lollipop-style strength.

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
        You do one bounded step: {LEGACY_CONTRACT_VERSION} final public-text editor.

        Return one JSON object only:
        {{
          "title": "string",
          "description_md": "string",
          "covered_baseline_fact_indexes": [0],
          "used_extra_fact_indexes": [0]
        }}

        HARD CONTRACT
        - You write the final public event description in Russian.
        - Treat baseline_description as the draft you are editing, not as disposable context.
          Keep every concrete meaningful claim, named object/person/place/programme, explicit list,
          and useful texture unless it is unsafe, unsupported, repetitive, or violates the style rules.
        - The baseline_facts list is the mandatory public factual floor. Semantically cover every
          baseline fact, but rewrite dry extractor phrasing into natural event-facing Russian.
        - logistics_context contains service facts. Use them only when they are already part of
          baseline_description or when omitting them would make the public text misleading.
        - source_excerpt is available only to verify or add grounded texture. It does not override
          the public factual floor.
        - If a fact is awkward but important, rewrite it naturally without changing meaning.
        - Use extra_facts only when they strengthen the text without crowding out baseline facts.
        - Do not invent facts, dates, venues, prices, ticket conditions, names, or quotes.
        - Do not stuff date/time/address/ticket logistics into narrative prose just to prove coverage.
          If logistics are needed, keep them in one compact natural sentence.
        - Keep the result at least as detailed as baseline_description in meaningful event substance,
          not necessarily in raw character count.
        - Required length window is in the user payload: min_description_chars..max_description_chars.
          Stay inside it unless a hard fact-floor constraint makes that impossible. Prefer the lower
          half of the window when all meaningful facts still fit naturally.
        - Do not compress the baseline into a short summary. Aim for a sharper edited version:
          fewer weak phrases, same or better factual substance.
        - If coverage requires comparable length, choose coverage and texture over arbitrary brevity.
        - The first paragraph should use the strongest grounded hook early: rarity, format,
          quote-like phrase, atmosphere, named programme, local context, or stage action.
        - Avoid report/card/promotional formulas: "посвящено", "характеризуется",
          "программа состоит из", "наполнен", "уникальная возможность", "не упустите",
          "зрители смогут насладиться", "X — это ...".
        - Never open with an em-dash definition like `Название — это ...`; write through action,
          atmosphere, object, or format instead.
        - Never open the lead with a stock narrator-frame: "Погружение в ...",
          "Знакомство с ...", "Путешествие в мир ...", "Прогулка по миру ...",
          "Окунитесь в ...", "Откройте для себя ...", "Добро пожаловать в ...",
          "Приготовьтесь к ...". Open with a concrete object, named actor, exact phrase
          from the source, format texture word, or event action instead.
        - Positive lead patterns to imitate (pick the kind that already exists in the facts):
          - object: "Жостовские подносы, каргопольская и дымковская игрушки..."
          - actor: "Религиовед Алексей Зыгмонт предлагает взглянуть на сакральное..."
          - format texture: "Звуки старого города, где каждый камень хранит отголоски..."
          - event action: "В преддверии Дня Земли откроется персональная выставка..."
          - named programme/quote: "«Космос красного» собирает русские народные промыслы..."
        - Keep the register: vivid cultural digest, not a dry card and not ad copy.
        - If baseline_description already used `### Heading` blocks for distinct thematic
          clusters and the fact floor is rich (4+ public facts), prefer either keeping the
          same heading structure or producing equivalently scannable paragraph breaks.
          Do not collapse a clearly structured baseline into one undifferentiated blob.
        - If a baseline fact contains an explicit named list, preserve the names; do not collapse to
          "и другие".
        - For programme/title lists, use markdown bullets only when the facts genuinely contain a
          list of works or items.
        - Mark covered_baseline_fact_indexes honestly. Include an index only if the output text
          semantically covers that fact.
        - Mark used_extra_fact_indexes honestly.

        QUALITY TARGET
        - The candidate should be objectively no worse than baseline_description:
          stronger or equal hook, no extra report/promotional formulas, no meta lead, and no factual loss.
        - Best outcome: 70-100% of baseline length with all meaningful facts preserved and a cleaner lead.
        - Acceptable outcome: comparable length but clearly cleaner prose.
        - Bad outcome: shorter by dropping concrete facts, or longer because logistics were pasted in.
        """
    ).strip()


def build_writer_payload(
    *,
    title: str,
    event_type: str,
    baseline_description: str,
    baseline_facts: list[str],
    enhancement: dict[str, Any],
    source_excerpt: str = "",
) -> dict[str, Any]:
    return {
        "title": title,
        "event_type": event_type,
        "baseline_description_chars": len(str(baseline_description or "")),
        "min_description_chars": int(len(str(baseline_description or "")) * 0.70),
        "target_description_chars": int(len(str(baseline_description or "")) * 0.86),
        "max_description_chars": int(len(str(baseline_description or "")) * 1.05),
        "hard_validation_gates": {
            "cover_all_baseline_fact_indexes": True,
            "minimum_description_chars": int(len(str(baseline_description or "")) * 0.68),
            "objective_quality_status": "must be no_worse or improved versus baseline_description",
            "speed_profile": "benchmark counts baseline stage plus lollipop stages",
            "forbidden_lead_pattern": "title — это",
            "forbidden_register": ["dry report", "promo CTA", "short summary"],
        },
        "baseline_description": baseline_description,
        "source_excerpt": str(source_excerpt or "")[:5000],
        "baseline_facts": [
            {"index": idx, "text": fact}
            for idx, fact in enumerate(baseline_facts)
            if _clean_text(fact)
        ],
        "logistics_context": list(enhancement.get("logistics_facts") or []),
        "lead_hook_fact_indexes": list(enhancement.get("lead_hook_fact_indexes") or []),
        "quote_candidates": list(enhancement.get("quote_candidates") or []),
        "extra_facts": [
            {"index": idx, **item}
            for idx, item in enumerate(list(enhancement.get("extra_facts") or []))
            if isinstance(item, dict) and _clean_text(item.get("text"))
        ],
        "writer_notes": list(enhancement.get("writer_notes") or []),
    }


def build_writer_repair_payload(
    *,
    title: str,
    event_type: str,
    baseline_description: str,
    baseline_facts: list[str],
    enhancement: dict[str, Any],
    previous_output: dict[str, Any],
    validation_errors: list[str],
    quality_regressions: list[str],
    source_excerpt: str = "",
) -> dict[str, Any]:
    payload = build_writer_payload(
        title=title,
        event_type=event_type,
        baseline_description=baseline_description,
        baseline_facts=baseline_facts,
        enhancement=enhancement,
        source_excerpt=source_excerpt,
    )
    payload["repair_instruction"] = {
        "mode": "repair_previous_writer_output",
        "previous_description_md": str(previous_output.get("description_md") or ""),
        "previous_covered_baseline_fact_indexes": list(previous_output.get("covered_baseline_fact_indexes") or []),
        "validation_errors": list(validation_errors or []),
        "quality_regressions": list(quality_regressions or []),
        "rules": [
            "Fix the listed errors without falling back to a dry card.",
            "If length is too short, restore the concrete baseline texture and programme/list details.",
            "If a hook is missing, rewrite the first paragraph through grounded format, atmosphere, action, or object.",
            "Return a fresh complete JSON object, not a patch.",
        ],
    }
    return payload


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
    min_length_ratio: float = 0.68,
    max_length_ratio: float = 1.12,
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
    duplicate_word = re.search(r"(?iu)\b([а-яёa-z]{4,})\b[\s,;:—-]+\1\b", description)
    if duplicate_word:
        errors.append(f"text.duplicate_word:{duplicate_word.group(1).lower()}")
    if re.search(r",[\s ]*,", description):
        errors.append("text.double_comma")
    duplicate_tail = re.search(r"(?iu)\b[а-яё]*([а-яё]{3,})\1[а-яё]*\b", description)
    if duplicate_tail:
        errors.append(f"text.duplicate_tail:{duplicate_tail.group(1).lower()}")
    baseline_has_ticket_or_price = any(
        re.search(r"(?iu)\b(?:билет\w*|руб(?:\.|л|лей)?|₽)\b", str(fact or ""))
        for fact in baseline_facts
    )
    if re.search(r"(?iu)\b(?:http|www\.)\b", description):
        warnings.append("url.leak")
    if (
        not baseline_has_ticket_or_price
        and re.search(r"(?iu)\b(?:билет\w*|руб(?:\.|л|лей)?|₽)\b", description)
    ):
        errors.append("ticket_or_price.invented_or_unexpected")

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
    if baseline_len and len(description) < int(baseline_len * min_length_ratio):
        errors.append(f"length.below_baseline_ratio:{len(description)}/{baseline_len}")
    if baseline_len and len(description) > int(baseline_len * max_length_ratio):
        warnings.append(f"length.above_baseline_ratio:{len(description)}/{baseline_len}")
    if not quality["lead_hook_signals"]:
        warnings.append("lead_hook_signal.absent")

    return LegacyValidationResult(errors=errors, warnings=warnings)
