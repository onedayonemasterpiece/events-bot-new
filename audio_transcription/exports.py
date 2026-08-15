from __future__ import annotations

import json
from pathlib import Path
from zoneinfo import ZoneInfo

from .contracts import TranscriptResult, TranscriptSegment


def _elapsed(ms: int, *, separator: str = ".") -> str:
    if ms < 0:
        raise ValueError("elapsed time cannot be negative")
    hours, remainder = divmod(ms, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}{separator}{millis:03d}"


def _srt_time(ms: int) -> str:
    return _elapsed(ms, separator=",")


def _clean_text(value: str) -> str:
    return " ".join(str(value).strip().split())


def render_plain(result: TranscriptResult) -> str:
    return "\n\n".join(_clean_text(segment.text) for segment in result.segments).strip() + "\n"


def render_timeline(result: TranscriptResult, *, timezone_name: str) -> str:
    tz = ZoneInfo(timezone_name)
    lines: list[str] = []
    for segment in result.segments:
        text = _clean_text(segment.text)
        if segment.absolute_start is not None:
            local = segment.absolute_start.astimezone(tz)
            label = local.strftime("%d.%m.%y %H:%M:%S")
        else:
            label = _elapsed(segment.source_start_ms)
        lines.append(f"[{label}] {text}")
    return "\n\n".join(lines).strip() + "\n"


def render_srt(result: TranscriptResult) -> str:
    blocks: list[str] = []
    for index, segment in enumerate(result.segments, start=1):
        blocks.append(
            f"{index}\n{_srt_time(segment.source_start_ms)} --> "
            f"{_srt_time(segment.source_end_ms)}\n{_clean_text(segment.text)}"
        )
    return "\n\n".join(blocks).strip() + "\n"


def render_vtt(result: TranscriptResult) -> str:
    blocks = ["WEBVTT"]
    for segment in result.segments:
        blocks.append(
            f"{_elapsed(segment.source_start_ms)} --> {_elapsed(segment.source_end_ms)}\n"
            f"{_clean_text(segment.text)}"
        )
    return "\n\n".join(blocks).strip() + "\n"


def render_json(result: TranscriptResult) -> str:
    return json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_exports(
    result: TranscriptResult,
    output_dir: str | Path,
    *,
    timezone_name: str,
) -> dict[str, dict[str, str | int]]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "plain": ("transcript.txt", render_plain(result)),
        "timeline": (
            "transcript.timeline.txt",
            render_timeline(result, timezone_name=timezone_name),
        ),
        "json": ("transcript.json", render_json(result)),
        "srt": ("transcript.srt", render_srt(result)),
        "vtt": ("transcript.vtt", render_vtt(result)),
    }
    manifest: dict[str, dict[str, str | int]] = {}
    import hashlib

    for key, (file_name, content) in payloads.items():
        encoded = content.encode("utf-8")
        path = root / file_name
        path.write_bytes(encoded)
        manifest[key] = {
            "file_name": file_name,
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "byte_length": len(encoded),
        }
    return manifest


def paginate_segments(
    segments: tuple[TranscriptSegment, ...],
    *,
    offset: int,
    limit: int,
) -> tuple[tuple[TranscriptSegment, ...], int | None]:
    if offset < 0:
        raise ValueError("offset cannot be negative")
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    page = segments[offset : offset + limit]
    next_offset = offset + len(page)
    return page, (next_offset if next_offset < len(segments) else None)
