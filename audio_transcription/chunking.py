from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .contracts import AudioChunk, Precision


@dataclass(frozen=True, slots=True)
class ChunkingProfile:
    target_ms: int
    hard_max_ms: int
    min_ms: int
    search_before_ms: int
    search_after_ms: int


_PROFILES = {
    Precision.SEGMENT: ChunkingProfile(
        target_ms=150_000,
        hard_max_ms=240_000,
        min_ms=30_000,
        search_before_ms=60_000,
        search_after_ms=45_000,
    ),
    Precision.PHRASE: ChunkingProfile(
        target_ms=45_000,
        hard_max_ms=90_000,
        min_ms=8_000,
        search_before_ms=20_000,
        search_after_ms=20_000,
    ),
}


def normalize_breakpoints(points_ms: Iterable[int], *, duration_ms: int) -> tuple[int, ...]:
    if duration_ms <= 0:
        raise ValueError("duration must be positive")
    return tuple(
        sorted(
            {
                int(point)
                for point in points_ms
                if 0 < int(point) < duration_ms
            }
        )
    )


def _choose_boundary(
    *,
    start_ms: int,
    duration_ms: int,
    breakpoints: tuple[int, ...],
    profile: ChunkingProfile,
) -> int:
    target = min(duration_ms, start_ms + profile.target_ms)
    hard_max = min(duration_ms, start_ms + profile.hard_max_ms)
    minimum = min(hard_max, start_ms + profile.min_ms)
    remaining = duration_ms - start_ms
    if remaining <= profile.target_ms + profile.search_after_ms:
        return duration_ms
    candidates = [
        point
        for point in breakpoints
        if minimum <= point <= hard_max
        and duration_ms - point >= profile.min_ms
        and target - profile.search_before_ms <= point <= target + profile.search_after_ms
    ]
    if candidates:
        # Prefer a pause just after target when distance is tied: it reduces the
        # number of Telegram requests without violating the hard cap.
        return min(candidates, key=lambda point: (abs(point - target), point < target, point))
    fallback = [point for point in breakpoints if minimum <= point <= hard_max]
    if fallback:
        return max(fallback)
    return hard_max


def plan_chunks(
    duration_ms: int,
    silence_end_points_ms: Iterable[int],
    *,
    precision: Precision,
) -> tuple[AudioChunk, ...]:
    if duration_ms <= 0:
        raise ValueError("duration must be positive")
    profile = _PROFILES[Precision(precision)]
    breakpoints = normalize_breakpoints(silence_end_points_ms, duration_ms=duration_ms)
    chunks: list[AudioChunk] = []
    start = 0
    while start < duration_ms:
        end = _choose_boundary(
            start_ms=start,
            duration_ms=duration_ms,
            breakpoints=breakpoints,
            profile=profile,
        )
        if end <= start:
            raise RuntimeError("chunk planner made no progress")
        chunks.append(AudioChunk(index=len(chunks), start_ms=start, end_ms=end))
        start = end
    return tuple(chunks)


def split_chunk_near_middle(
    chunk: AudioChunk,
    silence_end_points_ms: Iterable[int],
    *,
    min_part_ms: int = 5_000,
) -> tuple[AudioChunk, AudioChunk]:
    """Split a server-rejected voice chunk while preserving the source timeline."""

    if chunk.duration_ms < min_part_ms * 2:
        raise ValueError("chunk is too short to split safely")
    points = normalize_breakpoints(
        silence_end_points_ms,
        duration_ms=chunk.end_ms + 1,
    )
    midpoint = chunk.start_ms + chunk.duration_ms // 2
    candidates = [
        point
        for point in points
        if chunk.start_ms + min_part_ms <= point <= chunk.end_ms - min_part_ms
    ]
    boundary = min(candidates, key=lambda point: abs(point - midpoint)) if candidates else midpoint
    return (
        AudioChunk(index=chunk.index, start_ms=chunk.start_ms, end_ms=boundary),
        AudioChunk(index=chunk.index + 1, start_ms=boundary, end_ms=chunk.end_ms),
    )


def reindex_chunks(chunks: Iterable[AudioChunk]) -> tuple[AudioChunk, ...]:
    return tuple(
        AudioChunk(index=index, start_ms=chunk.start_ms, end_ms=chunk.end_ms)
        for index, chunk in enumerate(sorted(chunks, key=lambda item: item.start_ms))
    )
