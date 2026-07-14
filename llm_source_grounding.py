"""Narrow source-grounding checks for LLM-produced public claims.

The LLM remains responsible for selecting and wording facts.  This module does
not write or repair prose; it only verifies that an evidence quote is present
in the supplied source and that the proposed claim has enough lexical support
to be safe to publish.  Callers must fail closed or route to an approved LLM
fallback when the contract is not met.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


_TOKEN_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё]+", re.UNICODE)

# Function words and schema labels carry no evidence about an event.  Keeping
# them out of the denominator makes the check about source-bearing content,
# not Russian sentence structure.
_NON_EVIDENCE_TOKENS = {
    "а",
    "без",
    "будет",
    "быть",
    "в",
    "во",
    "для",
    "до",
    "его",
    "ее",
    "её",
    "и",
    "из",
    "или",
    "их",
    "к",
    "как",
    "на",
    "не",
    "но",
    "о",
    "об",
    "от",
    "по",
    "при",
    "про",
    "с",
    "со",
    "что",
    "это",
    "является",
    # Generic schema nouns must not make an unsupported abstraction look
    # grounded merely because the source announces some event.
    "событие",
    "события",
    "мероприятие",
    "мероприятия",
    "формат",
    "цель",
}


def normalize_source_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold().replace("ё", "е")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _token_key(token: str) -> str:
    value = normalize_source_text(token)
    if value.isdigit():
        return value
    # Prefix keys tolerate ordinary Russian inflection without pretending to
    # perform semantic matching.  Short tokens are kept intact.
    if len(value) >= 8:
        return value[:6]
    if len(value) >= 6:
        return value[:5]
    return value


def evidence_tokens(value: str | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in _TOKEN_RE.findall(normalize_source_text(value)):
        if len(raw) < 3 and not raw.isdigit():
            continue
        if raw in _NON_EVIDENCE_TOKENS:
            continue
        key = _token_key(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def source_contains_quote(source: str | None, quote: str | None) -> bool:
    source_norm = normalize_source_text(source)
    quote_norm = normalize_source_text(quote).strip(" \t\r\n\"'«»“”")
    return bool(source_norm and quote_norm and quote_norm in source_norm)


@dataclass(frozen=True)
class GroundingVerdict:
    ok: bool
    matched: int
    claim_tokens: int
    ratio: float
    reason: str


def lexical_grounding_verdict(
    claim: str | None,
    evidence: str | None,
    *,
    min_ratio: float = 0.5,
    min_matches: int = 2,
) -> GroundingVerdict:
    claim_tokens = evidence_tokens(claim)
    evidence_set = set(evidence_tokens(evidence))
    if not claim_tokens or not evidence_set:
        return GroundingVerdict(False, 0, len(claim_tokens), 0.0, "empty_evidence_tokens")

    # Numbers are exact public claims.  An unsupported number always fails,
    # irrespective of aggregate word overlap.
    claim_numbers = {token for token in claim_tokens if token.isdigit()}
    evidence_numbers = {token for token in evidence_set if token.isdigit()}
    if not claim_numbers.issubset(evidence_numbers):
        return GroundingVerdict(False, 0, len(claim_tokens), 0.0, "unsupported_number")

    matched = sum(1 for token in claim_tokens if token in evidence_set)
    ratio = matched / max(1, len(claim_tokens))
    required_matches = 1 if len(claim_tokens) <= 2 else max(1, int(min_matches))
    ok = matched >= required_matches and ratio >= float(min_ratio)
    return GroundingVerdict(
        ok,
        matched,
        len(claim_tokens),
        ratio,
        "grounded" if ok else "insufficient_lexical_support",
    )


def claim_is_grounded(
    claim: str | None,
    source: str | None,
    *,
    evidence_quote: str | None = None,
    min_ratio: float = 0.5,
    min_matches: int = 2,
) -> GroundingVerdict:
    evidence = evidence_quote if str(evidence_quote or "").strip() else source
    if evidence_quote and not source_contains_quote(source, evidence_quote):
        return GroundingVerdict(False, 0, len(evidence_tokens(claim)), 0.0, "quote_not_in_source")
    return lexical_grounding_verdict(
        claim,
        evidence,
        min_ratio=min_ratio,
        min_matches=min_matches,
    )
