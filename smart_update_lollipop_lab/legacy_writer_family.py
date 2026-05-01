"""Legacy lab variant `lollipop_legacy.v14`.

Goal: a baseline-equivalent public-description path on Gemma 4. The contract is
intentionally minimal: extract public/logistics facts from the source, then ask
a single writer (Gemma 4 default, 4o fallback when Gemma 4 fails) to produce the
final Russian Markdown. The benchmark also runs an LLM-first fact-coverage
reviewer to compare Gemma 3 baseline facts against Gemma 4 facts; that reviewer
is benchmark-only and never touches generation.

Hard rules carried from project policy:
- No baseline text/facts/draft enter the legacy generation payload. Baseline is
  only used by the benchmark for length/quality/timing comparison and by the
  fact-coverage reviewer (which is read-only).
- No repair pass. The writer either returns clean output or the variant falls
  back to the 4o final writer (still source-grounded, no baseline leakage).
- No deterministic regex post-processing of the output. Validation reports
  errors but never edits the text.
- No intermediate enrichment/planning stage. The legacy candidate is just
  extract -> write.
- Fact coverage is judged by an LLM-first reviewer, never by string overlap or
  keyword regex. The reviewer is allowed to mark a baseline fact as
  `baseline_ungrounded` so we do not punish Gemma 4 for skipping a baseline
  hallucination.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
import textwrap
from typing import Any

from . import writer_final_4o_family


LEGACY_CONTRACT_VERSION = "lollipop_legacy.v14"


@dataclass(slots=True)
class LegacyValidationResult:
    errors: list[str]
    warnings: list[str]


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


# --- text-quality helpers (shared with the benchmark reporting) -------------

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


def _named_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for match in re.finditer(r"(?u)[«\"']?[A-ZА-ЯЁ][0-9A-ZА-ЯЁа-яёA-Za-z_-]{2,}", str(text or "")):
        token = match.group(0).strip("«»\"'")
        if token:
            tokens.add(token)
    return tokens


def _source_fidelity_delta(
    *,
    source_excerpt: str,
    baseline_description: str,
    candidate_description: str,
) -> dict[str, int]:
    source_tokens = _named_tokens(source_excerpt)
    baseline_tokens = _named_tokens(baseline_description)
    candidate_tokens = _named_tokens(candidate_description)
    return {
        "candidate_source_tokens": len(candidate_tokens & source_tokens),
        "baseline_source_tokens": len(baseline_tokens & source_tokens),
        "candidate_invented_tokens": len(candidate_tokens - source_tokens),
        "baseline_invented_tokens": len(baseline_tokens - source_tokens),
    }


def compare_to_baseline(
    *,
    baseline_description: str,
    candidate_description: str,
    source_excerpt: str = "",
) -> dict[str, Any]:
    """Read-only quality signal for the benchmark.

    Emits regressions/improvements/warnings; never rewrites the candidate. The
    benchmark uses this for reporting only, never to gate a repair pass.
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
    fidelity = _source_fidelity_delta(
        source_excerpt=source_excerpt,
        baseline_description=baseline,
        candidate_description=candidate,
    )

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
        if (
            fidelity["candidate_source_tokens"] >= fidelity["baseline_source_tokens"]
            or fidelity["candidate_invented_tokens"] < fidelity["baseline_invented_tokens"]
        ):
            warnings.append("quality.lost_baseline_lead_hook")
        else:
            regressions.append("quality.lost_lead_hook")
    if candidate_narrator_frame and not baseline_narrator_frame:
        regressions.append("quality.narrator_frame_opening")
    if baseline_len and candidate_len < int(baseline_len * 0.68):
        warnings.append(f"quality.shorter_than_baseline:{candidate_len}/{baseline_len}")
    if baseline_len and candidate_len > int(baseline_len * 1.12):
        warnings.append(f"quality.longer_than_baseline:{candidate_len}/{baseline_len}")
    if baseline_headings >= 2 and candidate_headings == 0:
        warnings.append(f"quality.lost_baseline_headings:{baseline_headings}")
    if baseline_epigraph and not candidate_epigraph:
        warnings.append("quality.lost_baseline_epigraph")
    if (
        fidelity["candidate_source_tokens"] < fidelity["baseline_source_tokens"] - 3
        and fidelity["candidate_source_tokens"] < 2
    ):
        regressions.append(
            "quality.source_named_token_loss:"
            f"{fidelity['candidate_source_tokens']}/{fidelity['baseline_source_tokens']}"
        )

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
    if fidelity["candidate_invented_tokens"] < fidelity["baseline_invented_tokens"]:
        improvements.append("quality.invented_named_tokens_reduced")
    if fidelity["candidate_source_tokens"] > fidelity["baseline_source_tokens"]:
        improvements.append("quality.source_named_tokens_improved")

    status = "regressed" if regressions else ("improved" if improvements else "no_worse")
    return {
        "status": status,
        "baseline_score": baseline_score,
        "candidate_score": candidate_score,
        "length_ratio": length_ratio,
        "regressions": regressions,
        "improvements": improvements,
        "warnings": warnings,
        "source_fidelity": fidelity,
        "baseline_quality": baseline_quality,
        "candidate_quality": candidate_quality,
    }


# --- extraction stage --------------------------------------------------------

def extraction_response_schema() -> dict[str, Any]:
    fact_item = {
        "type": "OBJECT",
        "properties": {
            "text": {"type": "STRING"},
            "source_span": {"type": "STRING"},
            "kind": {"type": "STRING"},
        },
        "required": ["text", "source_span", "kind"],
    }
    return {
        "type": "OBJECT",
        "properties": {
            "public_facts": {"type": "ARRAY", "items": fact_item},
            "logistics_facts": {"type": "ARRAY", "items": fact_item},
            "warnings": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["public_facts", "logistics_facts", "warnings"],
    }


def build_extraction_system_prompt() -> str:
    return textwrap.dedent(
        f"""
        You are the Gemma 4 fact extractor for the {LEGACY_CONTRACT_VERSION} pipeline.
        Return only JSON matching the response schema. No markdown. No analysis.

        Extract concise Russian facts from the provided source text and event metadata
        for one cultural event description. Use the standard public/logistics split:
        - public_facts: format, title/programme, topic, named people and roles,
          route/object/list, source-local atmosphere, format texture, source-grounded
          cultural handle. These are what a public description must convey.
        - logistics_facts: date, time, venue, address, tickets, price, age, URLs,
          registration/instruction service lines.

        Rules:
        - Use only the source text and event metadata. Do not invent venues, dates,
          prices, programme items, names, or interpretations.
        - Preserve named entities exactly. Do not transliterate or paraphrase names.
        - Each fact text is one short Russian phrase. source_span is the shortest
          supporting source phrase. kind is a short lowercase label such as format,
          title, topic, person, route, object, texture, date, time, venue, address,
          tickets, price, age, url, service.
        - Prefer recall over brevity: emit 4-10 public_facts when source supports.
        - For sparse sources (1-2 source sentences) still split format, route/place,
          instruction/map, OCR location, and start mode when present, so the writer
          has enough public substance.
        - Do not put field labels ("text:", "kind:", JSON keys) inside fact text.
        - A street + house number attached to a named venue is logistics/address; a
          separate OCR street that the source presents as the route/object goes to
          public_facts as kind=route.
        - Russian only. No English helper words inside fact texts.
        """
    ).strip()


def normalize_extraction_payload(payload: dict[str, Any]) -> dict[str, Any]:
    def _items(key: str) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        seen: set[str] = set()
        for raw in list(payload.get(key) or []):
            if not isinstance(raw, dict):
                continue
            text = _clean_text(raw.get("text"))
            text = re.sub(r"(?iu)^(?:text|kind|fact)\s*:\s*", "", text).strip()
            if not text:
                continue
            normalized = text.casefold()
            if normalized in seen:
                continue
            seen.add(normalized)
            result.append(
                {
                    "text": text[:420],
                    "source_span": _clean_text(raw.get("source_span"))[:220],
                    "kind": _clean_text(raw.get("kind"))[:40],
                }
            )
        return result

    public_items = _items("public_facts")
    logistics_items = _items("logistics_facts")
    warnings: list[str] = []
    for raw in list(payload.get("warnings") or []):
        text = _clean_text(raw)
        if text and text not in warnings:
            warnings.append(text[:220])
    return {
        "public_facts": public_items,
        "logistics_facts": logistics_items,
        "warnings": warnings,
    }


def merge_extraction_facts(items: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Deduplicate facts across source extractions and assign stable indexes."""
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        text = _clean_text(item.get("text"))
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        merged.append(
            {
                "index": len(merged),
                "text": text[:420],
                "source_span": _clean_text(item.get("source_span"))[:220],
                "kind": _clean_text(item.get("kind"))[:40],
            }
        )
    return merged


# --- writer stage ------------------------------------------------------------

def writer_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "title": {"type": "STRING"},
            "description_md": {"type": "STRING"},
            "covered_public_fact_indexes": {"type": "ARRAY", "items": {"type": "INTEGER"}},
            "used_logistics_fact_indexes": {"type": "ARRAY", "items": {"type": "INTEGER"}},
            "warnings": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": [
            "title",
            "description_md",
            "covered_public_fact_indexes",
            "used_logistics_fact_indexes",
            "warnings",
        ],
    }


def build_writer_system_prompt() -> str:
    return textwrap.dedent(
        f"""
        You are the Gemma 4 / 4o final writer for the {LEGACY_CONTRACT_VERSION} pipeline.
        Return one JSON object matching the response schema. No markdown fences. No analysis.

        Write description_md as the final Russian Markdown for one cultural event card.
        Ground truth lives in public_facts, logistics_facts, source_excerpt, and the event
        metadata. Baseline text/facts are not in the request and must not be assumed.

        Hard rules:
        - description_md is plain Russian Markdown prose. Never JSON fragments, schema words,
          field names, English helper words, or instruction echoes.
        - Cover every meaningful public_fact. Never invent names, works, venues, route
          details, dates, prices, age limits, promises, or claims.
        - Logistics belong only in one optional final block "### Когда и где" when the
          date/time/place facts are useful. Do not stuff date/time/address/ticket lines into
          narrative prose.
        - Russian only. No English words unless the exact English token appears in
          source_excerpt as a name.
        - No URL, phone, age limit, OCR/poster labels, direct address, or ad CTA.
        - Avoid dry/promo formulas: "посвящено", "характеризуется", "программа состоит из",
          "представляет собой", "не упустите", "уникальная возможность", "приглашаем вас",
          "вы сможете", "позволит вам". Avoid stock narrator-frame leads
          ("Погружение в ...", "Знакомство с ...", "Путешествие в мир ...").
        - Open with a concrete grounded hook from a fact: route, object, topic, named
          person, exact title phrase, format texture, or event action. Never open with
          "Название — это ...".

        Shape and length:
        - Obey the length contract: produce a description_md within
          [min_description_chars, max_description_chars]; aim for target_description_chars.
          Shorter than min_description_chars is invalid output.
        - Use 2-4 public paragraphs before the optional logistics block. Add ### headings
          only when they help readability; the only allowed logistics heading is
          "### Когда и где".
        - Each paragraph is one continuous line; blank lines only between paragraphs and
          around a ### heading. Do not hard-wrap sentences.
        - For sparse sources, expand only through grounded interpretation of the existing
          format/topic (audio walk = listening/route/city attention; lecture = unpacking
          the topic; exhibition = looking at named objects). Never add new facts.
        - Each paragraph must add new information. Do not repeat the title or a fact across
          paragraphs.

        Honesty fields:
        - covered_public_fact_indexes lists the public_fact indexes you actually covered.
        - used_logistics_fact_indexes lists the logistics indexes you actually mentioned.
        - warnings is a short list of caveats (empty list if none).
        """
    ).strip()


def build_writer_payload(
    *,
    title: str,
    event_type: str,
    public_facts: list[dict[str, Any]],
    logistics_facts: list[dict[str, Any]],
    source_excerpt: str,
    reference_description_chars: int,
) -> dict[str, Any]:
    """Build the user payload for the writer stage.

    The reference_description_chars value is used only to compute advisory length
    bounds. Baseline description text and baseline facts are not included.
    """
    base_len = max(int(reference_description_chars or 0), 0) or 650
    object_count = sum(
        1
        for item in public_facts
        if str(item.get("kind") or "").casefold() == "object"
    )
    if object_count >= 3 and base_len >= 1000:
        floor_ratio, target_ratio, max_ratio = 0.55, 0.70, 0.95
    elif base_len >= 1000:
        floor_ratio, target_ratio, max_ratio = 0.70, 0.82, 1.05
    else:
        floor_ratio, target_ratio, max_ratio = 0.80, 0.92, 1.10
    min_chars = int(base_len * floor_ratio)
    target_chars = int(base_len * target_ratio)
    max_chars = int(base_len * max_ratio)
    return {
        "title": title,
        "event_type": event_type,
        "public_facts": public_facts,
        "logistics_facts": logistics_facts,
        "source_excerpt": str(source_excerpt or "")[:3000],
        "reference_description_chars": base_len,
        "min_description_chars": min_chars,
        "target_description_chars": target_chars,
        "max_description_chars": max_chars,
        "length_contract": (
            f"description_md must be {min_chars}-{max_chars} characters; "
            f"target about {target_chars}; output below {min_chars} is invalid"
        ),
    }


def apply_writer_output(*, title: str, output: dict[str, Any]) -> dict[str, Any]:
    """Lift the writer JSON into the canonical applied_output shape (title, description_md)."""
    description = str(output.get("description_md") or "").strip()
    return {
        "title": _clean_text(output.get("title")) or title,
        "description_md": description,
    }


# --- validation (read-only signal, never rewrites text) ----------------------

_PROMPT_LEAK_PATTERNS: tuple[str, ...] = (
    "facts_text_clean",
    "epigraph_fact",
    "Self-Correction",
    "schema",
    "prompt",
    "JSON",
    "}\n",
    "],",
)


def validate_writer_output(
    *,
    public_facts: list[dict[str, Any]] | list[str],
    description_md: str,
    baseline_description: str = "",
) -> LegacyValidationResult:
    """Light-weight, deterministic signal for the writer output.

    This validator never edits the candidate. It reports issues so the benchmark
    table is honest; the runtime falls back to 4o only on extraction/writer
    timeouts, exceptions, or empty/invalid output.
    """
    description = str(description_md or "")
    errors: list[str] = []
    warnings: list[str] = []

    if not description.strip():
        errors.append("description.empty")
        return LegacyValidationResult(errors=errors, warnings=warnings)

    public_count = len(list(public_facts or []))
    baseline_len = len(str(baseline_description or ""))
    cand_len = len(description)

    if cand_len < 180 and public_count:
        errors.append(f"length.too_short_absolute:{cand_len}")
    if cand_len > 1600:
        warnings.append(f"length.long:{cand_len}")
    if baseline_len and cand_len < int(baseline_len * 0.55):
        warnings.append(f"length.below_baseline_ratio:{cand_len}/{baseline_len}")

    duplicate_word = re.search(r"(?iu)\b([а-яёa-z]{4,})\b[\s,;:—-]+\1\b", description)
    if duplicate_word:
        errors.append(f"text.duplicate_word:{duplicate_word.group(1).lower()}")
    repeated_cluster = re.search(r"(?iu)([а-яё]{2,5})\1{3,}", description)
    if repeated_cluster:
        errors.append(f"text.repeated_cluster:{repeated_cluster.group(1).lower()}")
    english_word = re.search(r"\b[A-Za-z]{3,}\b", description)
    if english_word and not re.search(r"[«\"]" + re.escape(english_word.group(0)) + r"[»\"]", description):
        warnings.append(f"text.english_word:{english_word.group(0)}")

    lower = description.lower()
    for marker in _PROMPT_LEAK_PATTERNS:
        if marker.lower() in lower and len(marker) >= 5:
            errors.append(f"prompt_leak:{marker}")
            break

    if re.search(r"(?iu)\b(?:6|12|16|18)\+\b|возрастн", description):
        warnings.append("age.leak")
    if re.search(r"(?iu)\b(?:вы\s+сможете|для\s+вас|позволит\s+вам|приглашаем\s+вас)\b", description):
        warnings.append("style.direct_address")

    return LegacyValidationResult(errors=errors, warnings=warnings)


# --- fact-coverage reviewer (benchmark-only LLM judge) ----------------------

FACT_COVERAGE_VERDICTS: tuple[str, ...] = ("accepted", "partial", "rejected", "unknown")
LOSS_SEVERITIES: tuple[str, ...] = ("none", "minor", "major", "critical")
GROUNDED_VALUES: tuple[str, ...] = ("true", "false", "unclear")


def fact_coverage_response_schema() -> dict[str, Any]:
    baseline_review_item = {
        "type": "OBJECT",
        "properties": {
            "baseline_index": {"type": "INTEGER"},
            "baseline_fact": {"type": "STRING"},
            "grounded_in_source": {"type": "STRING"},
            "covered_by_g4": {"type": "BOOLEAN"},
            "matched_g4_fact_indexes": {"type": "ARRAY", "items": {"type": "INTEGER"}},
            "loss_severity": {"type": "STRING"},
            "reason": {"type": "STRING"},
        },
        "required": [
            "baseline_index",
            "baseline_fact",
            "grounded_in_source",
            "covered_by_g4",
            "matched_g4_fact_indexes",
            "loss_severity",
            "reason",
        ],
    }
    g4_review_item = {
        "type": "OBJECT",
        "properties": {
            "g4_index": {"type": "INTEGER"},
            "g4_fact": {"type": "STRING"},
            "fact_kind": {"type": "STRING"},
            "category": {"type": "STRING"},
            "grounded_in_source": {"type": "STRING"},
            "useful_new_fact": {"type": "BOOLEAN"},
            "suspicious_reason": {"type": "STRING"},
        },
        "required": [
            "g4_index",
            "g4_fact",
            "fact_kind",
            "category",
            "grounded_in_source",
            "useful_new_fact",
            "suspicious_reason",
        ],
    }
    return {
        "type": "OBJECT",
        "properties": {
            "baseline_facts_review": {"type": "ARRAY", "items": baseline_review_item},
            "g4_facts_review": {"type": "ARRAY", "items": g4_review_item},
            "coverage_summary": {
                "type": "OBJECT",
                "properties": {
                    "public_coverage_status": {"type": "STRING"},
                    "logistics_coverage_status": {"type": "STRING"},
                    "named_entity_coverage_status": {"type": "STRING"},
                    "format_topic_program_coverage_status": {"type": "STRING"},
                    "overall_verdict": {"type": "STRING"},
                    "verdict_reason": {"type": "STRING"},
                },
                "required": [
                    "public_coverage_status",
                    "logistics_coverage_status",
                    "named_entity_coverage_status",
                    "format_topic_program_coverage_status",
                    "overall_verdict",
                    "verdict_reason",
                ],
            },
        },
        "required": ["baseline_facts_review", "g4_facts_review", "coverage_summary"],
    }


def build_fact_coverage_system_prompt() -> str:
    return textwrap.dedent(
        f"""
        You are an LLM-first fact-coverage reviewer for the {LEGACY_CONTRACT_VERSION} pipeline.
        Return only JSON matching the response schema. No prose outside JSON.

        You judge how well a Gemma 4 fact extraction covers the same event as a
        Gemma 3 baseline extraction. Both extractions came from the same source.

        Method:
        - Match by meaning, not by word overlap. Two facts match when they convey
          the same atomic claim about the event (same name, same date, same role,
          same object, same programme item, same instruction). Paraphrasing is
          allowed; "Лектор — Борис Мегорский" and "Борис Мегорский, лектор" match;
          "Билеты на сайте" and "Билеты в кассе" are two different logistics facts.
        - For each baseline fact, set grounded_in_source to one of: true, false,
          unclear. A baseline fact is grounded only when source_excerpt or event
          metadata supports it. If baseline invented "знаковые места" or
          "интересно всем", mark it false (baseline hallucination); we will not
          punish G4 for skipping it.
        - For each baseline fact, decide covered_by_g4 (true/false). When matched,
          list matched_g4_fact_indexes from g4_facts. When unmatched, set
          loss_severity:
            * none      -> baseline fact is decorative/ungrounded and not needed;
            * minor     -> small phrasing nuance lost, no factual gap;
            * major     -> a real public fact missing (programme item, secondary
                            person, useful texture, named object);
            * critical  -> date/time/venue/address/title/lecturer/author/format
                            missing AND the baseline fact was grounded.
        - For each G4 fact, set grounded_in_source the same way and decide
          useful_new_fact when G4 brings a real source-grounded detail that the
          baseline did not have (e.g. exact start time, age limit, address).
          If a G4 fact is suspicious (ungrounded, fragmented, duplicate, English
          word leak, JSON debris, or label-only like "лекция"), set
          suspicious_reason to a short Russian phrase. Otherwise leave it empty.
        - In coverage_summary set each *_coverage_status to one of:
          accepted | partial | rejected.
            * named_entity covers title, lecturer/author, named work,
              named programme item;
            * format_topic_program covers event type, lecture cycle, programme
              list, route/object list;
            * logistics covers date, time, venue, address, tickets, age, URL;
            * public covers everything that is not strictly logistics.
        - overall_verdict is one of: accepted | partial | rejected.
            * accepted -> no critical or major losses on grounded baseline facts,
                          and named_entity / format_topic_program / logistics are
                          each accepted or partial.
            * partial  -> minor or major losses on a few grounded baseline facts
                          but no critical loss.
            * rejected -> at least one critical loss of a grounded baseline fact,
                          or G4 widely invents non-grounded claims.
        - verdict_reason: one short Russian sentence explaining the verdict.

        Hard rules:
        - Do not invent baseline or G4 facts that are not in the inputs.
        - matched_g4_fact_indexes must reference indexes that actually appear in
          g4_facts. If you cannot match, return an empty list.
        - Do not edit the texts you receive. Echo baseline_fact and g4_fact
          verbatim from the inputs.
        - Russian for free-form fields (reason, suspicious_reason, verdict_reason).
        - Lowercase enum values exactly as listed above.
        """
    ).strip()


def _coerce_grounded(value: Any) -> str:
    text = _clean_text(value).lower()
    if text in {"true", "yes", "истина", "да"}:
        return "true"
    if text in {"false", "no", "ложь", "нет"}:
        return "false"
    return "unclear"


def _coerce_severity(value: Any) -> str:
    text = _clean_text(value).lower()
    if text in {"none", "minor", "major", "critical"}:
        return text
    return "none"


def _coerce_verdict(value: Any, *, allowed: tuple[str, ...] = ("accepted", "partial", "rejected")) -> str:
    text = _clean_text(value).lower()
    if text in allowed:
        return text
    return "unknown"


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    return text in {"true", "yes", "1"}


def _coerce_index_list(value: Any, *, max_index: int) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for raw in list(value or []):
        try:
            idx = int(raw)
        except Exception:
            continue
        if 0 <= idx < max_index and idx not in seen:
            seen.add(idx)
            result.append(idx)
    return result


def build_fact_coverage_payload(
    *,
    title: str,
    event_type: str,
    date: str | None,
    time: str | None,
    location_name: str | None,
    location_address: str | None,
    city: str | None,
    source_excerpt: str,
    baseline_facts: list[str],
    g4_public_facts: list[dict[str, Any]] | list[str],
    g4_logistics_facts: list[dict[str, Any]] | list[str],
    baseline_writer_facts: list[str] | None = None,
    baseline_metadata_facts: list[str] | None = None,
) -> dict[str, Any]:
    """Build the user payload for the fact-coverage reviewer.

    Baseline facts are intentionally allowed in this payload because the reviewer
    is benchmark-only. The variant generation payloads (extractor / writer / 4o
    fallback) must continue to omit baseline facts and baseline text — that
    invariant is asserted by the corresponding regression test.
    """
    def _normalize_g4(items: list[Any], category: str) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for raw in items:
            if isinstance(raw, dict):
                text = _clean_text(raw.get("text"))
                kind = _clean_text(raw.get("kind"))
            else:
                text = _clean_text(raw)
                kind = ""
            if not text:
                continue
            normalized.append(
                {
                    "index": len(normalized),
                    "text": text[:420],
                    "kind": kind[:40],
                    "category": category,
                }
            )
        return normalized

    public_items = _normalize_g4(list(g4_public_facts or []), "public")
    logistics_items = _normalize_g4(list(g4_logistics_facts or []), "logistics")
    public_count = len(public_items)
    flat_g4 = list(public_items)
    for item in logistics_items:
        flat_g4.append({**item, "index": public_count + item["index"]})

    baseline_items = []
    for idx, fact in enumerate(baseline_facts or []):
        text = _clean_text(fact)
        if text:
            baseline_items.append({"index": len(baseline_items), "text": text[:420]})

    writer_items: list[dict[str, Any]] = []
    writer_seen: set[str] = set()
    for fact in list(baseline_writer_facts or []):
        text = _clean_text(fact)
        if text and text not in writer_seen:
            writer_seen.add(text)
            writer_items.append({"index": len(writer_items), "text": text[:420]})

    metadata_items: list[dict[str, Any]] = []
    metadata_seen: set[str] = set()
    for fact in list(baseline_metadata_facts or []):
        text = _clean_text(fact)
        if text and text not in metadata_seen:
            metadata_seen.add(text)
            metadata_items.append({"index": len(metadata_items), "text": text[:420]})

    return {
        "event": {
            "title": title,
            "event_type": event_type,
            "date": date,
            "time": time,
            "location_name": location_name,
            "location_address": location_address,
            "city": city,
        },
        "source_excerpt": str(source_excerpt or "")[:5000],
        "baseline_facts": baseline_items,
        "baseline_fact_surfaces": {
            "raw_extracted_facts": baseline_items,
            "writer_facts_text_clean": writer_items,
            "metadata_anchors": metadata_items,
        },
        "g4_facts": flat_g4,
    }


def normalize_fact_coverage_payload(
    payload: dict[str, Any],
    *,
    baseline_count: int,
    g4_count: int,
    baseline_facts: list[dict[str, Any]] | None = None,
    g4_facts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    baseline_text_by_index: dict[int, str] = {}
    for item in list(baseline_facts or []):
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except Exception:
            continue
        text = _clean_text(item.get("text"))[:420]
        if text:
            baseline_text_by_index[idx] = text

    g4_by_index: dict[int, dict[str, str]] = {}
    for item in list(g4_facts or []):
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except Exception:
            continue
        text = _clean_text(item.get("text"))[:420]
        if text:
            g4_by_index[idx] = {
                "text": text,
                "kind": _clean_text(item.get("kind"))[:40],
                "category": _clean_text(item.get("category")).lower(),
            }

    baseline_review: list[dict[str, Any]] = []
    seen_baseline_idx: set[int] = set()
    for raw in list(payload.get("baseline_facts_review") or []):
        if not isinstance(raw, dict):
            continue
        try:
            idx = int(raw.get("baseline_index"))
        except Exception:
            continue
        if not (0 <= idx < baseline_count) or idx in seen_baseline_idx:
            continue
        seen_baseline_idx.add(idx)
        baseline_fact = baseline_text_by_index.get(idx) or _clean_text(raw.get("baseline_fact"))[:420]
        baseline_review.append(
            {
                "baseline_index": idx,
                "baseline_fact": baseline_fact,
                "grounded_in_source": _coerce_grounded(raw.get("grounded_in_source")),
                "covered_by_g4": _coerce_bool(raw.get("covered_by_g4")),
                "matched_g4_fact_indexes": _coerce_index_list(
                    raw.get("matched_g4_fact_indexes"), max_index=g4_count
                ),
                "loss_severity": _coerce_severity(raw.get("loss_severity")),
                "reason": _clean_text(raw.get("reason"))[:300],
            }
        )

    g4_review: list[dict[str, Any]] = []
    seen_g4_idx: set[int] = set()
    for raw in list(payload.get("g4_facts_review") or []):
        if not isinstance(raw, dict):
            continue
        try:
            idx = int(raw.get("g4_index"))
        except Exception:
            continue
        if not (0 <= idx < g4_count) or idx in seen_g4_idx:
            continue
        seen_g4_idx.add(idx)
        original = g4_by_index.get(idx) or {}
        category = (original.get("category") or _clean_text(raw.get("category"))).lower()
        if category not in {"public", "logistics"}:
            category = ""
        g4_review.append(
            {
                "g4_index": idx,
                "g4_fact": original.get("text") or _clean_text(raw.get("g4_fact"))[:420],
                "fact_kind": original.get("kind") or _clean_text(raw.get("fact_kind"))[:40],
                "category": category,
                "grounded_in_source": _coerce_grounded(raw.get("grounded_in_source")),
                "useful_new_fact": _coerce_bool(raw.get("useful_new_fact")),
                "suspicious_reason": _clean_text(raw.get("suspicious_reason"))[:200],
            }
        )

    summary_raw = payload.get("coverage_summary") or {}
    coverage_summary = {
        "public_coverage_status": _coerce_verdict(summary_raw.get("public_coverage_status")),
        "logistics_coverage_status": _coerce_verdict(summary_raw.get("logistics_coverage_status")),
        "named_entity_coverage_status": _coerce_verdict(summary_raw.get("named_entity_coverage_status")),
        "format_topic_program_coverage_status": _coerce_verdict(
            summary_raw.get("format_topic_program_coverage_status")
        ),
        "overall_verdict": _coerce_verdict(summary_raw.get("overall_verdict")),
        "verdict_reason": _clean_text(summary_raw.get("verdict_reason"))[:300],
    }

    return {
        "baseline_facts_review": baseline_review,
        "g4_facts_review": g4_review,
        "coverage_summary": coverage_summary,
    }


def summarize_fact_coverage(normalized: dict[str, Any]) -> dict[str, Any]:
    """Derive lost / added / suspicious lists and a deterministic verdict floor.

    The reviewer's `overall_verdict` is the LLM judgement; this helper derives a
    second-opinion floor from the per-fact decisions so the benchmark report can
    cross-check. We pick the more conservative of the two for the surfaced
    `verdict` field, so a single critical loss cannot be dismissed as accepted.
    """
    baseline_review = list(normalized.get("baseline_facts_review") or [])
    g4_review = list(normalized.get("g4_facts_review") or [])
    coverage_summary = dict(normalized.get("coverage_summary") or {})

    lost_baseline_facts: list[dict[str, Any]] = []
    grounded_baseline_count = 0
    covered_grounded_baseline_count = 0
    for item in baseline_review:
        grounded = item.get("grounded_in_source") == "true"
        if grounded:
            grounded_baseline_count += 1
        if grounded and item.get("covered_by_g4"):
            covered_grounded_baseline_count += 1
        if grounded and not item.get("covered_by_g4"):
            lost_baseline_facts.append(
                {
                    "baseline_index": item.get("baseline_index"),
                    "baseline_fact": item.get("baseline_fact"),
                    "loss_severity": item.get("loss_severity"),
                    "reason": item.get("reason"),
                }
            )

    added_g4_facts: list[dict[str, Any]] = []
    suspicious_g4_facts: list[dict[str, Any]] = []
    grounded_g4_count = 0
    for item in g4_review:
        grounded = item.get("grounded_in_source") == "true"
        if grounded:
            grounded_g4_count += 1
        if grounded and item.get("useful_new_fact"):
            added_g4_facts.append(
                {
                    "g4_index": item.get("g4_index"),
                    "g4_fact": item.get("g4_fact"),
                    "category": item.get("category"),
                    "fact_kind": item.get("fact_kind"),
                }
            )
        suspicious_reason = (item.get("suspicious_reason") or "").strip()
        if suspicious_reason or item.get("grounded_in_source") in {"false", "unclear"}:
            suspicious_g4_facts.append(
                {
                    "g4_index": item.get("g4_index"),
                    "g4_fact": item.get("g4_fact"),
                    "category": item.get("category"),
                    "grounded_in_source": item.get("grounded_in_source"),
                    "suspicious_reason": suspicious_reason or item.get("grounded_in_source"),
                }
            )

    deterministic_verdict = "accepted"
    has_critical_loss = any(
        item.get("loss_severity") == "critical" for item in lost_baseline_facts
    )
    has_major_loss = any(
        item.get("loss_severity") == "major" for item in lost_baseline_facts
    )
    suspicious_grounded_false_count = sum(
        1
        for item in g4_review
        if item.get("grounded_in_source") == "false"
    )
    if has_critical_loss or suspicious_grounded_false_count >= 3:
        deterministic_verdict = "rejected"
    elif has_major_loss or lost_baseline_facts or suspicious_grounded_false_count >= 1:
        deterministic_verdict = "partial"

    llm_verdict = coverage_summary.get("overall_verdict") or "unknown"
    rank = {"unknown": -1, "rejected": 0, "partial": 1, "accepted": 2}
    final_verdict = min(
        (deterministic_verdict, llm_verdict),
        key=lambda v: rank.get(v, -1),
    )
    if final_verdict == "unknown":
        final_verdict = deterministic_verdict

    return {
        "baseline_fact_count": len(baseline_review),
        "grounded_baseline_fact_count": grounded_baseline_count,
        "covered_grounded_baseline_fact_count": covered_grounded_baseline_count,
        "g4_fact_count": len(g4_review),
        "grounded_g4_fact_count": grounded_g4_count,
        "lost_baseline_facts": lost_baseline_facts,
        "added_g4_facts": added_g4_facts,
        "suspicious_g4_facts": suspicious_g4_facts,
        "llm_overall_verdict": llm_verdict,
        "deterministic_verdict_floor": deterministic_verdict,
        "verdict": final_verdict,
        "coverage_summary": coverage_summary,
    }
