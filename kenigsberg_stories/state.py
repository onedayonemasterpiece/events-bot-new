from __future__ import annotations

import asyncio
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
POEMS_PATH = Path("docs/features/kenigsberg-stories/poems.md")

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
        "used_poem_ids": [],
        "pending_poem_id": None,
        "last_poetry_success_at": None,
        "recent_music": [],
        "recent_sources": [],
        "recent_usage_reset_at": None,
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
    state["used_poem_ids"] = state.get("used_poem_ids") if isinstance(state.get("used_poem_ids"), list) else []
    return state


async def save_state(db: Any, state: dict[str, Any]) -> None:
    payload = json.dumps(state, ensure_ascii=False, sort_keys=True)
    last_exc: Exception | None = None
    for attempt in range(1, 6):
        try:
            async with db.raw_conn() as conn:
                await conn.execute(
                    "INSERT OR REPLACE INTO setting(key, value) VALUES(?, ?)",
                    (STATE_SETTING_KEY, payload),
                )
                await conn.commit()
            return
        except Exception as exc:
            last_exc = exc
            if "database is locked" not in str(exc).casefold() or attempt >= 5:
                raise
            await asyncio.sleep(0.25 * attempt)
    if last_exc:
        raise last_exc


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


async def reset_recent_usage_windows(db: Any) -> dict[str, Any]:
    state = await load_state(db)
    state["recent_music"] = []
    state["recent_sources"] = []
    state["recent_usage_reset_at"] = _utc_now_iso()
    await save_state(db, state)
    return state


def _recent_usage_reset_at(state: dict[str, Any]) -> datetime | None:
    raw = str(state.get("recent_usage_reset_at") or "").strip()
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(raw)
    except Exception:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


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


def _parse_poem_block(block: list[str]) -> dict[str, Any] | None:
    if not block:
        return None
    header = block[0].strip()
    match = re.match(r"^##\s+([A-Za-z0-9_-]+)\s*$", header)
    if not match:
        return None
    poem_id = match.group(1).strip()
    meta: dict[str, str] = {}
    body_lines: list[str] = []
    in_body = False
    for raw_line in block[1:]:
        line = raw_line.rstrip()
        if line.strip() == "```poem":
            in_body = True
            continue
        if line.strip() == "```" and in_body:
            in_body = False
            continue
        if in_body:
            body_lines.append(line)
            continue
        key_match = re.match(r"^([a-zA-Z_]+):\s*(.*?)\s*$", line)
        if key_match:
            meta[key_match.group(1).casefold()] = key_match.group(2)
    while body_lines and not body_lines[0].strip():
        body_lines.pop(0)
    while body_lines and not body_lines[-1].strip():
        body_lines.pop()
    if not body_lines:
        return None
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in body_lines:
        clean = line.strip()
        if not clean:
            if current:
                blocks.append(current)
                current = []
            continue
        current.append(clean)
    if current:
        blocks.append(current)
    body_text = "\n".join(body_lines).strip()
    return {
        "id": poem_id,
        "title": meta.get("title", ""),
        "author": meta.get("author", ""),
        "author_note": meta.get("author_note", ""),
        "handle": meta.get("handle", ""),
        "audio": meta.get("audio", poem_id),
        "body": body_text,
        "blocks": blocks,
    }


def load_poems(path: Path = POEMS_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    poems: list[dict[str, Any]] = []
    block: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if raw_line.startswith("## "):
            poem = _parse_poem_block(block)
            if poem:
                poems.append(poem)
            block = [raw_line]
        elif block:
            block.append(raw_line)
    poem = _parse_poem_block(block)
    if poem:
        poems.append(poem)
    return poems


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
        available = list(thoughts)
    index = secrets.randbelow(len(available))
    return available[index]


def _find_poem_by_id(poems: list[dict[str, Any]], poem_id: str | None) -> dict[str, Any] | None:
    target = str(poem_id or "").strip()
    if not target:
        return None
    for poem in poems:
        if str(poem.get("id") or "").strip() == target:
            return poem
    return None


async def choose_next_poem(db: Any, *, poems_path: Path = POEMS_PATH) -> dict[str, Any] | None:
    poems = load_poems(poems_path)
    if not poems:
        return None
    state = await load_state(db)
    pending = _find_poem_by_id(poems, str(state.get("pending_poem_id") or ""))
    if pending:
        return pending
    used = {str(item) for item in (state.get("used_poem_ids") or [])}
    available = [item for item in poems if str(item.get("id") or "") not in used]
    if not available:
        available = list(poems)
        used = set()
    index = secrets.randbelow(len(available))
    selected = available[index]
    state["pending_poem_id"] = selected.get("id")
    if not available or not used:
        state["used_poem_ids"] = [item for item in state.get("used_poem_ids", []) if item in used]
    await save_state(db, state)
    return selected


def poetry_due(state: dict[str, Any], *, now: datetime | None = None, interval_days: int = 3) -> bool:
    if state.get("pending_poem_id"):
        return True
    raw = str(state.get("last_poetry_success_at") or "").strip()
    if not raw:
        return True
    try:
        last = datetime.fromisoformat(raw)
    except Exception:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current - last >= timedelta(days=max(1, int(interval_days)))


def recent_source_exclusions(
    state: dict[str, Any],
    *,
    max_age_days: int = 28,
    max_segments: int = 250,
) -> list[dict[str, Any]]:
    issues = state.get("issues") if isinstance(state.get("issues"), dict) else {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(max_age_days)))
    reset_at = _recent_usage_reset_at(state)
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
        if reset_at is not None and registered_at <= reset_at:
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


def recent_music_exclusions(
    state: dict[str, Any],
    *,
    max_age_days: int = 28,
    max_items: int = 80,
) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(max_age_days)))
    reset_at = _recent_usage_reset_at(state)
    items: list[tuple[datetime, dict[str, Any]]] = []

    def add_item(raw: dict[str, Any], *, issue_number: int = 0, registered_at: datetime | None = None) -> None:
        file_name = str(raw.get("file") or raw.get("music_file") or "").strip()
        if not file_name:
            return
        try:
            start = float(raw.get("start") if raw.get("start") is not None else raw.get("music_start") or 0.0)
            end = float(raw.get("end") if raw.get("end") is not None else raw.get("music_end") or 0.0)
        except Exception:
            return
        if end <= start:
            return
        created = registered_at
        raw_created = str(raw.get("created_at") or "").strip()
        if created is None and raw_created:
            try:
                created = datetime.fromisoformat(raw_created)
            except Exception:
                created = None
        if created is None:
            created = datetime.now(timezone.utc)
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if created < cutoff:
            return
        if reset_at is not None and created <= reset_at:
            return
        items.append(
            (
                created,
                {
                    "file": file_name,
                    "start": round(start, 3),
                    "end": round(end, 3),
                    "issue_number": int(raw.get("issue_number") or issue_number or 0),
                    "created_at": created.isoformat(),
                },
            )
        )

    for raw in state.get("recent_music") or []:
        if isinstance(raw, dict):
            add_item(raw)

    issues = state.get("issues") if isinstance(state.get("issues"), dict) else {}
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
        selected = issue.get("selected_music")
        if isinstance(selected, dict):
            add_item(selected, issue_number=int(issue.get("issue_number") or 0), registered_at=registered_at)
        else:
            add_item(issue, issue_number=int(issue.get("issue_number") or 0), registered_at=registered_at)

    dedup: dict[tuple[str, float, float], tuple[datetime, dict[str, Any]]] = {}
    for created, item in items:
        key = (str(item.get("file") or ""), float(item.get("start") or 0.0), float(item.get("end") or 0.0))
        if key not in dedup or created > dedup[key][0]:
            dedup[key] = (created, item)
    ordered = sorted(dedup.values(), key=lambda item: item[0], reverse=True)
    return [item for _created, item in ordered[:max_items]]


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
    thought_id = str(manifest.get("thought_id") or "").strip()
    if thought_id:
        used = [str(item) for item in (state.get("used_thought_ids") or []) if str(item).strip()]
        if thought_id not in set(used):
            state["used_thought_ids"] = [
                *sorted(used, key=lambda x: int(x) if x.isdigit() else x),
                thought_id,
            ]
    if (
        str(manifest.get("content_mode") or "").strip() == "poetry"
        and str(manifest.get("poetry_mode") or "").strip() != "test"
    ):
        poem_id = str(manifest.get("poem_id") or "").strip()
        if poem_id:
            used_poems = [str(item) for item in (state.get("used_poem_ids") or []) if str(item).strip()]
            if poem_id not in set(used_poems):
                state["used_poem_ids"] = [
                    *sorted(used_poems, key=lambda x: int(x.rsplit("-", 1)[-1]) if x.rsplit("-", 1)[-1].isdigit() else x),
                    poem_id,
                ]
            if str(state.get("pending_poem_id") or "") == poem_id:
                state["pending_poem_id"] = None
            state["last_poetry_success_at"] = _utc_now_iso()
    music_file = str(manifest.get("music_file") or "").strip()
    try:
        music_start = float(manifest.get("music_start") or 0.0)
        music_end = float(manifest.get("music_end") or 0.0)
    except Exception:
        music_start = music_end = 0.0
    if music_file and music_end > music_start:
        recent_music = [item for item in (state.get("recent_music") or []) if isinstance(item, dict)]
        recent_music.insert(
            0,
            {
                "file": music_file,
                "start": round(music_start, 3),
                "end": round(music_end, 3),
                "issue_number": issue_number,
                "created_at": _utc_now_iso(),
            },
        )
        state["recent_music"] = recent_music[:80]
    await save_state(db, state)
