from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

SCHEMA_VERSION = "events-bot.audio-transcription.v1"


class Precision(StrEnum):
    """How aggressively the source is split before Telegram transcription."""

    SEGMENT = "segment"
    PHRASE = "phrase"


class JobState(StrEnum):
    QUEUED = "queued"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    COLLECTING = "collecting"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {self.COMPLETE, self.FAILED, self.CANCELLED}


class AnchorSource(StrEnum):
    EXPLICIT = "explicit"
    QUICKTIME_CREATION_TIME = "quicktime.creation_time"
    FORMAT_CREATION_TIME = "format.creation_time"
    STREAM_CREATION_TIME = "stream.creation_time"
    FILENAME = "filename"
    MISSING = "missing"


@dataclass(frozen=True, slots=True)
class RecordingAnchor:
    started_at: datetime | None
    source: AnchorSource
    uncertainty_ms: int | None = None
    raw_value: str | None = None

    def __post_init__(self) -> None:
        if self.started_at is not None and self.started_at.tzinfo is None:
            raise ValueError("recording anchor must be timezone-aware")
        if self.source is AnchorSource.MISSING and self.started_at is not None:
            raise ValueError("missing anchor cannot contain started_at")
        if self.source is not AnchorSource.MISSING and self.started_at is None:
            raise ValueError("non-missing anchor requires started_at")
        if self.uncertainty_ms is not None and self.uncertainty_ms < 0:
            raise ValueError("anchor uncertainty cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "source": self.source.value,
            "uncertainty_ms": self.uncertainty_ms,
            "raw_value": self.raw_value,
        }


@dataclass(frozen=True, slots=True)
class AudioProbe:
    duration_ms: int
    format_name: str
    codec_name: str | None
    sample_rate: int | None
    channels: int | None
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.duration_ms <= 0:
            raise ValueError("audio duration must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "duration_ms": self.duration_ms,
            "format_name": self.format_name,
            "codec_name": self.codec_name,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "tags": dict(self.tags),
        }


@dataclass(frozen=True, slots=True)
class AudioChunk:
    index: int
    start_ms: int
    end_ms: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError("chunk index cannot be negative")
        if self.start_ms < 0 or self.end_ms <= self.start_ms:
            raise ValueError("invalid chunk range")

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms

    def to_dict(self) -> dict[str, int]:
        return {
            "index": self.index,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "duration_ms": self.duration_ms,
        }


@dataclass(frozen=True, slots=True)
class TranscriptSegment:
    id: str
    source_start_ms: int
    source_end_ms: int
    text: str
    absolute_start: datetime | None = None
    absolute_end: datetime | None = None
    chunk_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.source_start_ms < 0 or self.source_end_ms <= self.source_start_ms:
            raise ValueError("invalid transcript segment range")
        if not self.text.strip():
            raise ValueError("transcript segment text cannot be empty")
        for value in (self.absolute_start, self.absolute_end):
            if value is not None and value.tzinfo is None:
                raise ValueError("absolute segment timestamps must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_start_ms": self.source_start_ms,
            "source_end_ms": self.source_end_ms,
            "absolute_start": self.absolute_start.isoformat() if self.absolute_start else None,
            "absolute_end": self.absolute_end.isoformat() if self.absolute_end else None,
            "text": self.text.strip(),
            "chunk_sha256": self.chunk_sha256,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TranscriptSegment":
        def parse_dt(raw: Any) -> datetime | None:
            if not raw:
                return None
            text = str(raw)
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                raise ValueError("absolute timestamp must include a timezone")
            return parsed

        return cls(
            id=str(value["id"]),
            source_start_ms=int(value["source_start_ms"]),
            source_end_ms=int(value["source_end_ms"]),
            text=str(value["text"]),
            absolute_start=parse_dt(value.get("absolute_start")),
            absolute_end=parse_dt(value.get("absolute_end")),
            chunk_sha256=(str(value["chunk_sha256"]) if value.get("chunk_sha256") else None),
        )


@dataclass(frozen=True, slots=True)
class TranscriptResult:
    job_ref: str
    source_sha256: str
    source_name: str
    probe: AudioProbe
    anchor: RecordingAnchor
    precision: Precision
    segments: tuple[TranscriptSegment, ...]
    created_at: datetime
    engine: str = "telegram-native"
    language: str = "ru"
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        previous_end = -1
        for segment in self.segments:
            if segment.source_start_ms < previous_end:
                raise ValueError("transcript segments must be ordered and non-overlapping")
            previous_end = segment.source_end_ms

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "job_ref": self.job_ref,
            "source": {
                "sha256": self.source_sha256,
                "name": self.source_name,
                **self.probe.to_dict(),
            },
            "recording_anchor": self.anchor.to_dict(),
            "engine": self.engine,
            "language": self.language,
            "precision": self.precision.value,
            "created_at": self.created_at.isoformat(),
            "segments": [segment.to_dict() for segment in self.segments],
        }
