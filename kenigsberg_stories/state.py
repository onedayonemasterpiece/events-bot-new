from __future__ import annotations

import json
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

KENIGSBERG_PROFILE_KEY = "kenigsberg_story"
STATE_SETTING_KEY = "kenigsberg_stories_state"
THOUGHTS_PATH = Path("docs/features/kenigsberg-stories/thoughts.md")

_RANGE_TOKEN_RE = re.compile(r"(?<!\d)(\d+(?:[.,]\d+)?)(?:\s*[-–—]\s*(\d+(?:[.,]\d+)?))?(?!\d)")


@dataclass(frozen=True)
class SecondRange:
    start: float
    end: float

    def as_dict(self) -> dict[str, float]:
        return {"start": self.start, "end": self.end}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_state() -> dict[str, Any]:
    return {
        "version": 1,
        "next_issue": 1,
        "used_thought_ids": [],
        "recent_music": [],
        "recent_sources": [],
        "issues": {},
        "source_bans": [],
    }


def parse_second_ranges(value: str) -> list[SecondRange]:
    """Parse human range text like ``1-3, 7, 16-17`` into normalized ranges."""
    text = (value or "").strip().replace(";", ",")
    if not text:
        return []
    ranges: list[SecondRange] = []
    for match in _RANGE_TOKEN_RE.finditer(text):
        raw_start = match.group(1).replace(",", ".")
        raw_end = (match.group(2) or match.group(1)).replace(",", ".")
        start = float(raw_start)
        end = float(raw_end)
        if end < start:
            start, end = end, start
        if end == start:
            end = start + 1.0
        ranges.append(SecondRange(round(start, 3), round(end, 3)))
    return _merge_ranges(ranges)


def _merge_ranges(ranges: list[SecondRange]) -> list[SecondRange]:
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda item: (item.start, item.end))
    merged: list[SecondRange] = [ordered[0]]
    for item in ordered[1:]:
        prev = merged[-1]
        if item.start <= prev.end:
            merged[-1] = SecondRange(prev.start, max(prev.end, item.end))
        else:
            merged.append(item)
    return merged


async def load_state(db: Any) -> dict[str, Any]:
    async with db.raw_conn() as conn:
        cursor = await conn.execute("SELECT value FROM setting WHERE key=?", (STATE_SETTING_KEY,))
        row = await cursor.fetchone()
    if not row or not row[0]:
        return _empty_state()
    try:
        data = json.loads(row[0])
    except Exception:
        return _empty_state()
    if not isinstance(data, dict):
        return _empty_state()
    state = _empty_state()
    state.update(data)
    state["issues"] = state.get("issues") if isinstance(state.get("issues"), dict) else {}
    state["source_bans"] = state.get("source_bans") if isinstance(state.get("source_bans"), list) else []
    return state


async def save_state(db: Any, state: dict[str, Any]) -> None:
    payload = json.dumps(state, ensure_ascii=False, sort_keys=True)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO setting(key, value) VALUES(?, ?)",
            (STATE_SETTING_KEY, payload),
        )
        await conn.commit()


def _intersect(a: SecondRange, b_start: float, b_end: float) -> SecondRange | None:
    start = max(a.start, float(b_start))
    end = min(a.end, float(b_end))
    if end <= start:
        return None
    return SecondRange(round(start, 3), round(end, 3))


def map_generated_range_to_source(
    issue: dict[str, Any],
    generated_range: SecondRange,
) -> list[dict[str, Any]]:
    """Map generated-video seconds to the one dominant source-video segment.

    Operator ranges are entered in whole seconds while scene cuts can happen on
    fractional beat boundaries. Treat small edge overlaps as input imprecision:
    one requested range should create one source ban, chosen by maximum overlap.
    """
    candidates: list[tuple[float, float, dict[str, Any]]] = []
    for segment in issue.get("segments") or []:
        if not isinstance(segment, dict):
            continue
        timeline_start = float(segment.get("timeline_start") or 0.0)
        timeline_end = float(segment.get("timeline_end") or 0.0)
        overlap = _intersect(generated_range, timeline_start, timeline_end)
        if overlap is None:
            continue
        source_start = float(segment.get("source_start") or 0.0)
        overlap_len = overlap.end - overlap.start
        requested_midpoint = (generated_range.start + generated_range.end) / 2.0
        midpoint_inside = 1.0 if timeline_start <= requested_midpoint < timeline_end else 0.0
        candidates.append(
            (
                overlap_len,
                midpoint_inside,
                {
                    "dataset": str(segment.get("dataset") or issue.get("dataset") or "").strip(),
                    "source_file": str(segment.get("source_file") or "").strip(),
                    "source_start": round(source_start + (overlap.start - timeline_start), 3),
                    "source_end": round(source_start + (overlap.end - timeline_start), 3),
                    "issue_number": int(issue.get("issue_number") or 0),
                    "generated_start": overlap.start,
                    "generated_end": overlap.end,
                    "requested_generated_start": generated_range.start,
                    "requested_generated_end": generated_range.end,
                    "created_at": _utc_now_iso(),
                },
            )
        )
    if not candidates:
        return []
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    item = candidates[0][2]
    return [item] if item["dataset"] and item["source_file"] else []


async def apply_generated_timeline_bans(
    db: Any,
    *,
    issue_number: int,
    ranges: list[SecondRange],
) -> tuple[list[dict[str, Any]], str]:
    state = await load_state(db)
    issue_key = str(int(issue_number))
    issue = state.get("issues", {}).get(issue_key)
    if not isinstance(issue, dict):
        return [], f"Выпуск kenigsberg #{issue_number} не найден в истории генераций."
    mapped: list[dict[str, Any]] = []
    for item in ranges:
        mapped.extend(map_generated_range_to_source(issue, item))
    if not mapped:
        return [], (
            f"В выпуске kenigsberg #{issue_number} не нашёл исходные сегменты "
            "для указанных секунд."
        )
    source_bans = state.setdefault("source_bans", [])
    source_bans.extend(mapped)
    await save_state(db, state)
    return mapped, f"Добавлено банов: {len(mapped)}"


async def reset_bans(db: Any) -> None:
    state = await load_state(db)
    state["source_bans"] = []
    await save_state(db, state)


def format_bans_report(state: dict[str, Any]) -> str:
    bans = state.get("source_bans") or []
    if not bans:
        return "Банов для Kenigsberg Stories пока нет."
    lines = [f"Kenigsberg bans: {len(bans)}"]
    for idx, item in enumerate(bans[-30:], start=max(1, len(bans) - 29)):
        lines.append(
            "#{idx}: issue #{issue} {dataset} {source} {start:.2f}-{end:.2f}s".format(
                idx=idx,
                issue=item.get("issue_number") or "?",
                dataset=item.get("dataset") or "-",
                source=item.get("source_file") or "-",
                start=float(item.get("source_start") or 0.0),
                end=float(item.get("source_end") or 0.0),
            )
        )
    if len(bans) > 30:
        lines.append(f"... показаны последние 30 из {len(bans)}")
    return "\n".join(lines)


def load_thoughts(path: Path = THOUGHTS_PATH) -> list[dict[str, str]]:
    if not path.exists():
        return []
    thoughts: list[dict[str, str]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^\s*(\d+)\.\s+(.+?)\s*$", raw_line)
        if not match:
            continue
        thoughts.append({"id": match.group(1), "text": match.group(2)})
    return thoughts


async def reserve_issue_number(db: Any) -> int:
    state = await load_state(db)
    issue_number = int(state.get("next_issue") or 1)
    state["next_issue"] = issue_number + 1
    await save_state(db, state)
    return issue_number


async def choose_next_thought(db: Any, *, thoughts_path: Path = THOUGHTS_PATH) -> dict[str, str]:
    thoughts = load_thoughts(thoughts_path)
    if not thoughts:
        return {"id": "", "text": ""}
    state = await load_state(db)
    used = {str(item) for item in (state.get("used_thought_ids") or [])}
    available = [item for item in thoughts if str(item["id"]) not in used]
    if not available:
        used = set()
        available = list(thoughts)
    index = secrets.randbelow(len(available))
    chosen = available[index]
    state["used_thought_ids"] = [*sorted(used, key=lambda x: int(x) if x.isdigit() else x), chosen["id"]]
    await save_state(db, state)
    return chosen


def recent_source_exclusions(
    state: dict[str, Any],
    *,
    max_age_days: int = 28,
    max_segments: int = 250,
) -> list[dict[str, Any]]:
    issues = state.get("issues") if isinstance(state.get("issues"), dict) else {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(max_age_days)))
    items: list[tuple[datetime, dict[str, Any]]] = []
    for issue in issues.values():
        if not isinstance(issue, dict):
            continue
        raw_registered = str(issue.get("registered_at") or "").strip()
        try:
            registered_at = datetime.fromisoformat(raw_registered)
        except Exception:
            registered_at = datetime.now(timezone.utc)
        if registered_at.tzinfo is None:
            registered_at = registered_at.replace(tzinfo=timezone.utc)
        if registered_at < cutoff:
            continue
        issue_number = int(issue.get("issue_number") or 0)
        for segment in issue.get("segments") or []:
            if not isinstance(segment, dict):
                continue
            dataset = str(segment.get("dataset") or issue.get("dataset") or "").strip()
            source_file = str(segment.get("source_file") or "").strip()
            if not dataset or not source_file:
                continue
            source_start = float(segment.get("source_start") or 0.0)
            source_end = float(segment.get("source_end") or 0.0)
            if source_end <= source_start:
                continue
            items.append(
                (
                    registered_at,
                    {
                        "dataset": dataset,
                        "source_file": source_file,
                        "source_start": round(source_start, 3),
                        "source_end": round(source_end, 3),
                        "issue_number": issue_number,
                        "created_at": registered_at.isoformat(),
                        "reason": "recent_generation",
                    },
                )
            )
    items.sort(key=lambda item: item[0], reverse=True)
    return [item for _registered_at, item in items[:max_segments]]


async def register_issue_manifest(db: Any, manifest: dict[str, Any]) -> None:
    issue_number = int(manifest.get("issue_number") or 0)
    if issue_number <= 0:
        return
    state = await load_state(db)
    issues = state.setdefault("issues", {})
    issues[str(issue_number)] = {
        **manifest,
        "registered_at": _utc_now_iso(),
    }
    await save_state(db, state)
