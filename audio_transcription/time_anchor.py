from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .contracts import AnchorSource, RecordingAnchor

_QUICKTIME_KEYS = (
    "com.apple.quicktime.creationdate",
    "format.com.apple.quicktime.creationdate",
    "stream.com.apple.quicktime.creationdate",
)
_FORMAT_CREATION_KEYS = (
    "format.creation_time",
    "format.date",
    "creation_time",
    "date",
)
_STREAM_CREATION_KEYS = (
    "stream.creation_time",
    "stream.date",
)


_FILENAME_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"(?<!\d)(?P<y>20\d{2})(?P<m>0[1-9]|1[0-2])(?P<d>0[1-9]|[12]\d|3[01])"
            r"[-_ T]?(?P<h>[01]\d|2[0-3])(?P<mi>[0-5]\d)(?P<s>[0-5]\d)(?!\d)"
        ),
        "compact",
    ),
    (
        re.compile(
            r"(?<!\d)(?P<y>20\d{2})[-_.](?P<m>0[1-9]|1[0-2])[-_.](?P<d>0[1-9]|[12]\d|3[01])"
            r"[ T_-]+(?P<h>[01]\d|2[0-3])[-_.:](?P<mi>[0-5]\d)(?:[-_.:](?P<s>[0-5]\d))?(?!\d)"
        ),
        "iso-like",
    ),
    (
        re.compile(
            r"(?<!\d)(?P<d>0[1-9]|[12]\d|3[01])[-_.](?P<m>0[1-9]|1[0-2])[-_.](?P<y>20\d{2})"
            r"[ T_-]+(?P<h>[01]\d|2[0-3])[-_.:](?P<mi>[0-5]\d)(?:[-_.:](?P<s>[0-5]\d))?(?!\d)"
        ),
        "ru-like",
    ),
)


def _timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone: {name}") from exc


def parse_aware_datetime(value: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("timestamp is empty")
    normalized = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    # QuickTime commonly emits +0200 rather than +02:00.
    if re.search(r"[+-]\d{4}$", normalized):
        normalized = normalized[:-5] + normalized[-5:-2] + ":" + normalized[-2:]
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("timestamp must be RFC3339/ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include UTC offset")
    return parsed


def _metadata_anchor(tags: Mapping[str, str]) -> RecordingAnchor | None:
    normalized = {str(key).casefold(): str(value).strip() for key, value in tags.items()}
    groups = (
        (_QUICKTIME_KEYS, AnchorSource.QUICKTIME_CREATION_TIME),
        (_FORMAT_CREATION_KEYS, AnchorSource.FORMAT_CREATION_TIME),
        (_STREAM_CREATION_KEYS, AnchorSource.STREAM_CREATION_TIME),
    )
    for keys, source in groups:
        for key in keys:
            raw = normalized.get(key)
            if not raw:
                continue
            try:
                parsed = parse_aware_datetime(raw)
            except ValueError:
                continue
            return RecordingAnchor(
                started_at=parsed,
                source=source,
                uncertainty_ms=1000,
                raw_value=raw,
            )
    return None


def _filename_anchor(file_name: str, timezone_name: str) -> RecordingAnchor | None:
    stem = Path(file_name).stem
    tz = _timezone(timezone_name)
    for pattern, _label in _FILENAME_PATTERNS:
        match = pattern.search(stem)
        if match is None:
            continue
        parts = match.groupdict(default="0")
        try:
            parsed = datetime(
                int(parts["y"]),
                int(parts["m"]),
                int(parts["d"]),
                int(parts["h"]),
                int(parts["mi"]),
                int(parts.get("s") or 0),
                tzinfo=tz,
            )
        except ValueError:
            continue
        return RecordingAnchor(
            started_at=parsed,
            source=AnchorSource.FILENAME,
            uncertainty_ms=1000,
            raw_value=match.group(0),
        )
    return None


def resolve_recording_anchor(
    *,
    explicit_started_at: str | None,
    tags: Mapping[str, str],
    file_name: str,
    timezone_name: str,
) -> RecordingAnchor:
    """Resolve one truthful start-time anchor in strict confidence order."""

    _timezone(timezone_name)  # validate even when an explicit offset is supplied
    if explicit_started_at:
        return RecordingAnchor(
            started_at=parse_aware_datetime(explicit_started_at),
            source=AnchorSource.EXPLICIT,
            uncertainty_ms=0,
            raw_value=explicit_started_at,
        )
    metadata = _metadata_anchor(tags)
    if metadata is not None:
        return metadata
    filename = _filename_anchor(file_name, timezone_name)
    if filename is not None:
        return filename
    return RecordingAnchor(
        started_at=None,
        source=AnchorSource.MISSING,
        uncertainty_ms=None,
        raw_value=None,
    )


def absolute_at(anchor: RecordingAnchor, offset_ms: int) -> datetime | None:
    if offset_ms < 0:
        raise ValueError("offset cannot be negative")
    if anchor.started_at is None:
        return None
    return anchor.started_at + timedelta(milliseconds=offset_ms)


def to_utc(value: datetime | None) -> datetime | None:
    return value.astimezone(timezone.utc) if value is not None else None
