"""Pure, dependency-light source-parse contradiction fact collector.

The collector only decides whether the bounded semantic verifier must inspect a
source.  It never changes a source disposition, assigns a no-event reason, or
otherwise makes a product verdict.  Keeping this module free of provider, ORM,
and runtime imports lets the Telegram notebook stage and execute the exact same
implementation as the application.
"""

from __future__ import annotations

from datetime import date, datetime
import re
from typing import Any, Mapping, Sequence

from source_parse_contract import (
    ContradictionFact,
    EvidenceManifest,
    SourceDisposition,
    SourceParseDecision,
    VerificationReason,
)


_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}
_MONTH_PATTERN = "|".join(_MONTHS)
_NUMERIC_DATE_RE = re.compile(
    r"(?<!\d)(?P<day>\d{1,2})[./-](?P<month>\d{1,2})(?:[./-](?P<year>\d{2,4}))?(?!\d)"
)
_ISO_DATE_RE = re.compile(r"(?<!\d)(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})(?!\d)")
_WORD_DATE_RE = re.compile(
    rf"(?<!\d)(?P<day>\d{{1,2}})\s+(?P<month>{_MONTH_PATTERN})(?:\s+(?P<year>\d{{4}}))?",
    re.IGNORECASE,
)
_TIME_RE = re.compile(r"(?<!\d)(?P<hour>\d{1,2}):(?P<minute>\d{2})(?!\d)")

_INVITATION_RE = re.compile(
    r"\b(?:приходите|приглаша(?:ем|ет|ют)|жд[её]м\s+вас|регистрац(?:ия|ии)|"
    r"записывайтесь|купить\s+билет|билеты?\s+(?:в\s+продаже|по\s+ссылке)|"
    r"join\s+us|register|tickets?\s+(?:available|on\s+sale))\b",
    re.IGNORECASE,
)
_EVENT_RE = re.compile(
    r"\b(?:концерт|выставк\w*|лекци\w*|спектакл\w*|фестивал\w*|экскурси\w*|"
    r"мастер[- ]класс\w*|встреч\w*|показ\w*|сеанс\w*|воркшоп\w*|"
    r"concert|exhibition|lecture|festival|workshop|screening|session)\b",
    re.IGNORECASE,
)
_ATTENDANCE_RE = re.compile(
    r"\b(?:билет\w*|регистрац\w*|запис\w*|вход\b|место\s+проведения|адрес\b|"
    r"tickets?|registration|venue|address|admission)\b",
    re.IGNORECASE,
)
_LIFECYCLE_RE = re.compile(
    r"\b(?:отмен(?:а|[её]н[аоы]?)|перенос(?:ится|им|а)?|перенес[её]н|"
    r"новая\s+дата|изменени[ея]\s+(?:времени|места)|cancelled|canceled|"
    r"postponed|rescheduled)\b",
    re.IGNORECASE,
)
_RANGE_OR_RECURRENCE_RE = re.compile(
    rf"\b(?:с\s+\d{{1,2}}\s+по\s+\d{{1,2}}|"
    rf"\d{{1,2}}\s*[-–—]\s*\d{{1,2}}\s+(?:{_MONTH_PATTERN})|"
    r"\d{4}-\d{2}-\d{2}\s*\.\.\s*\d{4}-\d{2}-\d{2}|"
    r"кажд\w+|ежедневно|еженедельно|"
    r"по\s+(?:понедельникам|вторникам|средам|четвергам|пятницам|субботам|воскресеньям)|"
    r"daily|weekly|every\s+(?:day|week))\b",
    re.IGNORECASE,
)
_GENERIC_TITLE_RE = re.compile(
    r"^(?:мероприятие|событие|концерт|выставка|лекция|спектакль|фестиваль|"
    r"экскурсия|мастер[- ]класс|афиша|анонс|event|concert|exhibition)(?:\s*[—–-]\s*[^—–-]+)?$",
    re.IGNORECASE,
)


def _text_parts(source_text: str, all_ocr_blocks: Sequence[str] | None) -> tuple[str, ...]:
    return tuple(
        part.strip()
        for part in (str(source_text or ""), *(str(item or "") for item in (all_ocr_blocks or ())))
        if part.strip()
    )


def _parse_year(raw: str | None, default_year: int) -> int:
    if not raw:
        return default_year
    year = int(raw)
    return year + 2000 if year < 100 else year


def _date_tokens(text: str, *, default_year: int) -> tuple[tuple[int, int, int], ...]:
    iso_matches = tuple(_ISO_DATE_RE.finditer(text))
    values: list[tuple[int, int, int]] = [
        (
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        )
        for match in iso_matches
    ]
    for match in _NUMERIC_DATE_RE.finditer(text):
        if any(
            match.start() < iso.end() and iso.start() < match.end()
            for iso in iso_matches
        ):
            continue
        values.append(
            (
                _parse_year(match.group("year"), default_year),
                int(match.group("month")),
                int(match.group("day")),
            )
        )
    for match in _WORD_DATE_RE.finditer(text):
        values.append(
            (
                _parse_year(match.group("year"), default_year),
                _MONTHS[match.group("month").casefold()],
                int(match.group("day")),
            )
        )
    return tuple(dict.fromkeys(values))


def _times(text: str) -> tuple[tuple[int, int], ...]:
    return tuple(
        dict.fromkeys(
            (int(match.group("hour")), int(match.group("minute")))
            for match in _TIME_RE.finditer(text)
        )
    )


def _metadata_year(source_metadata: Mapping[str, Any] | None) -> int:
    for key in ("today", "published_at", "source_date"):
        raw = (source_metadata or {}).get(key)
        if not raw:
            continue
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).year
        except ValueError:
            continue
    # Production callers always pass ``today``.  A stable fallback keeps this
    # pure helper deterministic for malformed/test metadata.
    return 2000


def _metadata_today(source_metadata: Mapping[str, Any] | None) -> date | None:
    raw = (source_metadata or {}).get("today")
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _valid_calendar_token(value: tuple[int, int, int]) -> bool:
    try:
        date(*value)
    except ValueError:
        return False
    return True


def _event_date_tokens(
    events: Sequence[Mapping[str, Any]], *, default_year: int
) -> tuple[tuple[int, int, int], ...]:
    found: list[tuple[int, int, int]] = []
    for event in events:
        for key in ("date", "start_date", "start", "end_date", "end"):
            value = event.get(key)
            if isinstance(value, str):
                iso = _ISO_DATE_RE.search(value)
                if iso:
                    found.append(
                        (
                            int(iso.group("year")),
                            int(iso.group("month")),
                            int(iso.group("day")),
                        )
                    )
                else:
                    found.extend(_date_tokens(value, default_year=default_year))
    return tuple(dict.fromkeys(found))


def _event_time_tokens(events: Sequence[Mapping[str, Any]]) -> tuple[tuple[int, int], ...]:
    found: list[tuple[int, int]] = []
    for event in events:
        for key in ("time", "start_time", "end_time", "start", "end"):
            value = event.get(key)
            if isinstance(value, str):
                found.extend(_times(value))
    return tuple(dict.fromkeys(found))


def _manifest_incomplete(evidence_manifest: EvidenceManifest | Mapping[str, Any] | None) -> bool:
    if evidence_manifest is None:
        return True
    if isinstance(evidence_manifest, EvidenceManifest):
        return not evidence_manifest.evidence_complete
    if not isinstance(evidence_manifest, Mapping):
        return True
    return not bool(evidence_manifest.get("evidence_complete", False))


def _source_signal_classes(text: str, dates: Sequence[tuple[int, int, int]], times: Sequence[tuple[int, int]]) -> set[str]:
    signals: set[str] = set()
    if _INVITATION_RE.search(text):
        signals.add("invitation")
    if _EVENT_RE.search(text):
        signals.add("event_type")
    if _ATTENDANCE_RE.search(text):
        signals.add("attendance")
    if dates:
        signals.add("date")
    if times:
        signals.add("time")
    return signals


def _concrete_card_occurrences(parts: Sequence[str], *, default_year: int) -> int:
    """Count cards that independently contain an event and a schedule anchor."""

    count = 0
    for block in parts:
        dates = _date_tokens(block, default_year=default_year)
        times = _times(block)
        if _EVENT_RE.search(block) and (dates or times):
            count += 1
    return count


def _impossible_event_range(events: Sequence[Mapping[str, Any]]) -> bool:
    for event in events:
        start = event.get("start_time") or event.get("time")
        end = event.get("end_time")
        if isinstance(start, str) and isinstance(end, str):
            start_values, end_values = _times(start), _times(end)
            if start_values and end_values and start_values[0] > end_values[0]:
                return True
        start_date = event.get("start_date") or event.get("date")
        end_date = event.get("end_date")
        if isinstance(start_date, str) and isinstance(end_date, str):
            try:
                if date.fromisoformat(end_date[:10]) < date.fromisoformat(start_date[:10]):
                    return True
            except ValueError:
                return True
    return False


def _impossible_typed_value(events: Sequence[Mapping[str, Any]]) -> bool:
    for event in events:
        for key in ("ticket_price_min", "ticket_price_max"):
            value = event.get(key)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0
            ):
                return True
        minimum, maximum = event.get("ticket_price_min"), event.get("ticket_price_max")
        if isinstance(minimum, (int, float)) and isinstance(maximum, (int, float)) and maximum < minimum:
            return True
        for key in ("is_free", "pushkin_card"):
            if key in event and event.get(key) is not None and not isinstance(event.get(key), bool):
                return True
    return False


def derive_source_contradiction_facts(
    source_text: str,
    all_ocr_blocks: Sequence[str] | None,
    source_metadata: Mapping[str, Any] | None,
    primary_decision: SourceParseDecision,
    evidence_manifest: EvidenceManifest | Mapping[str, Any] | None,
) -> tuple[ContradictionFact, ...]:
    """Return closed verifier triggers for a single primary decision.

    The signature is deliberately positional and serialization-friendly so the
    Telegram service can stage this module without an application dependency.
    """

    parts = _text_parts(source_text, all_ocr_blocks)
    combined = "\n".join(parts)
    year = _metadata_year(source_metadata)
    source_dates = _date_tokens(combined, default_year=year)
    source_times = _times(combined)
    events = tuple(item for item in primary_decision if isinstance(item, Mapping))
    event_dates = _event_date_tokens(events, default_year=year)
    event_times = _event_time_tokens(events)
    signals = _source_signal_classes(combined, source_dates, source_times)
    facts: list[ContradictionFact] = []

    if (
        primary_decision.disposition is SourceDisposition.CONFIRMED_NO_EVENT
        and "invitation" in signals
        and "event_type" in signals
        and "attendance" in signals
        and bool({"date", "time"} & signals)
        and len(signals) >= 4
    ):
        facts.append(
            ContradictionFact(
                VerificationReason.NO_EVENT_WITH_STRONG_SIGNALS,
                "Complete no-event verdict conflicts with multiple independent invitation signals.",
                tuple(sorted(signals)),
            )
        )

    valid_source_dates = tuple(item for item in source_dates if _valid_calendar_token(item))
    valid_event_dates = tuple(item for item in event_dates if _valid_calendar_token(item))
    if len(valid_source_dates) == 1 and len(valid_event_dates) == 1 and valid_source_dates != valid_event_dates:
        facts.append(
            ContradictionFact(
                VerificationReason.EVENT_DATE_CONFLICT,
                "The only unambiguous source/OCR date conflicts with the only parsed event date.",
                (str(valid_source_dates[0]), str(valid_event_dates[0])),
            )
        )

    session_time_count = (
        len(source_times)
        if len(source_times) >= 3
        and re.search(r"\b(?:сеанс\w*|сесси\w*|начала?\s+в)\b", combined, re.IGNORECASE)
        else 0
    )
    occurrence_count = max(
        len(valid_source_dates),
        session_time_count,
        _concrete_card_occurrences(parts[1:], default_year=year),
    )
    metadata_occurrences = (source_metadata or {}).get("occurrence_count")
    if isinstance(metadata_occurrences, int) and not isinstance(metadata_occurrences, bool):
        occurrence_count = max(occurrence_count, metadata_occurrences)
    metadata_anchors = (source_metadata or {}).get("occurrence_anchors")
    if isinstance(metadata_anchors, Sequence) and not isinstance(metadata_anchors, (str, bytes)):
        occurrence_count = max(
            occurrence_count,
            len({str(item).strip() for item in metadata_anchors if str(item).strip()}),
        )
    if (
        occurrence_count >= 2
        and len(events) < occurrence_count
        and not _RANGE_OR_RECURRENCE_RE.search(combined)
        and primary_decision.disposition in {SourceDisposition.EVENTS_FOUND, SourceDisposition.MIXED}
    ):
        facts.append(
            ContradictionFact(
                VerificationReason.MULTIPLE_OCCURRENCES_COLLAPSED,
                "Distinct source occurrence anchors exceed parsed event children.",
                (f"occurrences={occurrence_count}", f"events={len(events)}"),
            )
        )

    generic_titles = tuple(
        str(event.get("title") or "").strip()
        for event in events
        if not str(event.get("title") or "").strip()
        or _GENERIC_TITLE_RE.fullmatch(str(event.get("title") or "").strip())
    )
    if generic_titles:
        facts.append(
            ContradictionFact(
                VerificationReason.GENERIC_UNGROUNDED_TITLE,
                "A positive child has an empty, placeholder, or bare type/venue title.",
                generic_titles,
            )
        )

    today = _metadata_today(source_metadata)
    has_future_date = bool(
        today
        and any(date(*value) >= today for value in valid_source_dates)
    )
    if (
        primary_decision.disposition is SourceDisposition.LIFECYCLE_ONLY
        and _LIFECYCLE_RE.search(combined)
        and "invitation" in signals
        and "event_type" in signals
        and bool({"date", "time"} & signals)
        and has_future_date
        and len(signals) >= 4
    ):
        facts.append(
            ContradictionFact(
                VerificationReason.LIFECYCLE_MIXED_CONTENT_CONFLICT,
                "Lifecycle-only verdict coexists with independent new-event invitation evidence.",
                tuple(sorted(signals)),
            )
        )

    impossible_dates = tuple(item for item in (*source_dates, *event_dates) if not _valid_calendar_token(item))
    impossible_times = tuple(item for item in (*source_times, *event_times) if item[0] > 23 or item[1] > 59)
    if (
        impossible_dates
        or impossible_times
        or _impossible_event_range(events)
        or _impossible_typed_value(events)
    ):
        facts.append(
            ContradictionFact(
                VerificationReason.IMPOSSIBLE_SCHEMA_VALUE,
                "Source or parsed event contains an objectively impossible date, time, or range.",
                tuple(str(item) for item in (*impossible_dates, *impossible_times)),
            )
        )

    if _manifest_incomplete(evidence_manifest):
        facts.append(
            ContradictionFact(
                VerificationReason.INCOMPLETE_EVIDENCE,
                "Evidence manifest is absent, incomplete, truncated, or has an OCR cardinality gap.",
            )
        )

    by_reason = {fact.reason: fact for fact in facts}
    return tuple(by_reason[reason] for reason in VerificationReason if reason in by_reason)


__all__ = ["derive_source_contradiction_facts"]
