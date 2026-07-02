#!/usr/bin/env python3
"""Scan public/canonical exhibition rows for high-confidence duplicate cards.

This is an operational acceptance helper for the Smart Update Vector Identity Gate:
after `SMART_UPDATE_IDENTITY_GATE=enforce`, the daily/static-site job can run this
against the production SQLite snapshot and fail if `/vystavki/` would expose two
canonical-looking rows for the same long-running exhibition identity.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

_TOKEN_RE = re.compile(r"[\wа-яё]+", re.IGNORECASE)
_ISO_DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}")
_EXHIBITION_RE = re.compile(r"(?iu)\b(выставк\w*|экспозиц\w*|ярмарк\w*|exhibition|fair)\b")
_STOPWORDS = {
    "выставка",
    "выставки",
    "экспозиция",
    "экспозиции",
    "ярмарка",
    "музей",
    "галерея",
    "калининград",
    "для",
    "при",
    "про",
    "или",
    "это",
}


@dataclass(frozen=True)
class PublicExhibition:
    id: int
    title: str
    date: str
    end_date: str | None
    location_name: str | None
    city: str | None
    event_type: str | None
    source_post_url: str | None = None
    source_vk_post_url: str | None = None
    ticket_link: str | None = None
    added_at: str | None = None


@dataclass(frozen=True)
class DuplicatePair:
    left_id: int
    right_id: int
    confidence: float
    reason: str
    left_title: str
    right_title: str
    venue: str | None
    left_added_at: str | None = None
    right_added_at: str | None = None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except Exception:
        return set()


def _norm(value: str | None) -> str:
    return " ".join((value or "").replace("ё", "е").lower().split())


def _tokens(value: str | None) -> set[str]:
    return {t for t in _TOKEN_RE.findall(_norm(value)) if len(t) > 2 and t not in _STOPWORDS}


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        match = _ISO_DATE_RE.search(str(value))
        if not match:
            return None
        return date.fromisoformat(match.group(0))
    except Exception:
        return None


def _overlaps(left: PublicExhibition, right: PublicExhibition) -> bool:
    ls = _parse_date(left.date)
    rs = _parse_date(right.date)
    if not ls or not rs:
        return False
    le = _parse_date(left.end_date) or ls
    re_ = _parse_date(right.end_date) or rs
    return ls <= re_ and rs <= le


def _active_on_or_after(ev: PublicExhibition, current: date) -> bool:
    start = _parse_date(ev.date)
    if not start:
        return False
    end = _parse_date(ev.end_date) or start
    return end >= current


def _same_venue(left: PublicExhibition, right: PublicExhibition) -> bool:
    lbits = _norm(" ".join(x for x in (left.location_name, left.city) if x))
    rbits = _norm(" ".join(x for x in (right.location_name, right.city) if x))
    return bool(lbits and rbits and (lbits == rbits or lbits in rbits or rbits in lbits))


def _title_similarity(left: str, right: str) -> float:
    lt = _tokens(left)
    rt = _tokens(right)
    if not lt or not rt:
        return 0.0
    return len(lt & rt) / max(len(lt), len(rt))


def _same_strong_link(left: PublicExhibition, right: PublicExhibition) -> bool:
    for attr in ("source_post_url", "source_vk_post_url", "ticket_link"):
        lv = _norm(getattr(left, attr))
        rv = _norm(getattr(right, attr))
        if lv and rv and lv == rv:
            return True
    return False


def _is_exhibition(row: PublicExhibition) -> bool:
    hay = "\n".join([row.title or "", row.event_type or ""])
    return bool(_EXHIBITION_RE.search(hay) or (row.end_date and row.end_date != row.date))


def load_public_exhibitions(conn: sqlite3.Connection, current: date) -> list[PublicExhibition]:
    cols = _columns(conn, "event")
    optional = {
        name: (name if name in cols else f"NULL AS {name}")
        for name in (
            "end_date",
            "location_name",
            "city",
            "event_type",
            "source_post_url",
            "source_vk_post_url",
            "ticket_link",
            "identity_status",
            "merged_into_event_id",
            "lifecycle_status",
            "added_at",
        )
    }
    sql = f"""
        SELECT id, title, date,
               {optional['end_date']}, {optional['location_name']}, {optional['city']},
               {optional['event_type']}, {optional['source_post_url']},
               {optional['source_vk_post_url']}, {optional['ticket_link']},
               {optional['identity_status']}, {optional['merged_into_event_id']},
               {optional['lifecycle_status']}, {optional['added_at']}
        FROM event
        WHERE date GLOB '20??-??-??'
    """
    out: list[PublicExhibition] = []
    for row in conn.execute(sql):
        lifecycle = (row[12] or "active").lower() if len(row) > 12 else "active"
        identity_status = (row[10] or "canonical").lower() if len(row) > 10 else "canonical"
        merged_into = row[11] if len(row) > 11 else None
        if lifecycle not in {"", "active"}:
            continue
        if identity_status != "canonical" or merged_into not in {None, "", 0}:
            continue
        ev = PublicExhibition(
            id=int(row[0]),
            title=str(row[1] or ""),
            date=str(row[2] or ""),
            end_date=row[3],
            location_name=row[4],
            city=row[5],
            event_type=row[6],
            source_post_url=row[7],
            source_vk_post_url=row[8],
            ticket_link=row[9],
            added_at=row[13] if len(row) > 13 else None,
        )
        if _is_exhibition(ev) and _active_on_or_after(ev, current):
            out.append(ev)
    return out


def find_high_confidence_duplicates(events: Iterable[PublicExhibition]) -> list[DuplicatePair]:
    rows = list(events)
    pairs: list[DuplicatePair] = []
    for i, left in enumerate(rows):
        for right in rows[i + 1 :]:
            if not _overlaps(left, right):
                continue
            same_venue = _same_venue(left, right)
            title_sim = _title_similarity(left.title, right.title)
            strong_link = _same_strong_link(left, right)
            if same_venue and (title_sim >= 0.5 or strong_link):
                confidence = max(0.9, min(0.99, 0.78 + title_sim * 0.22 + (0.08 if strong_link else 0.0)))
                pairs.append(
                    DuplicatePair(
                        left_id=left.id,
                        right_id=right.id,
                        confidence=round(confidence, 3),
                        reason="same_venue_overlapping_period_title_or_link",
                        left_title=left.title,
                        right_title=right.title,
                        venue=left.location_name or right.location_name,
                        left_added_at=left.added_at,
                        right_added_at=right.added_at,
                    )
                )
    return pairs


def _prometheus(payload: dict) -> str:
    lines = [
        f"events_public_exhibition_rows_total {payload['public_exhibition_count']}",
        f'events_public_exhibition_duplicate_pairs_total{{confidence="high"}} {payload["high_confidence_duplicate_total_count"]}',
        f'events_public_exhibition_duplicate_clusters_total{{confidence="high"}} {payload["high_confidence_duplicate_total_cluster_count"]}',
        f'events_public_exhibition_duplicate_pairs_since_total{{confidence="high",window_days="{payload["since_days"]}"}} {payload["high_confidence_duplicate_count"]}',
        f'events_public_exhibition_duplicate_clusters_since_total{{confidence="high",window_days="{payload["since_days"]}"}} {payload["high_confidence_duplicate_cluster_count"]}',
    ]
    for reason, count in sorted(payload.get("gate_suppressed", {}).items()):
        lines.append(f'events_public_exhibition_gate_suppressed_total{{reason="{reason}"}} {count}')
    return "\n".join(lines) + "\n"


def _cluster_count(duplicates: list[DuplicatePair]) -> int:
    parent: dict[int, int] = {}

    def find(x: int) -> int:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for pair in duplicates:
        union(pair.left_id, pair.right_id)
    return len({find(x) for x in parent})


def _pair_touches_since_window(pair: DuplicatePair, since: date) -> bool:
    left_added = _parse_date(pair.left_added_at)
    right_added = _parse_date(pair.right_added_at)
    if left_added is None and right_added is None:
        # Older snapshots/test fixtures may not have event.added_at. Fail closed:
        # count the pair in the rollout window instead of silently hiding it.
        return True
    return bool((left_added and left_added >= since) or (right_added and right_added >= since))


def build_audit_payload(
    db_path: Path | str,
    *,
    current: date | None = None,
    since_days: int = 14,
) -> dict:
    """Return the schema-adaptive duplicate-audit payload for a SQLite DB.

    The connection is opened read-only so scheduled/CI acceptance checks cannot
    mutate the production DB or a downloaded production snapshot.
    """

    current = current or date.today()
    since_days = max(1, int(since_days or 14))
    since = current - timedelta(days=since_days)
    conn = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    try:
        rows = load_public_exhibitions(conn, current)
        all_duplicates = find_high_confidence_duplicates(rows)
    finally:
        conn.close()

    window_duplicates = [pair for pair in all_duplicates if _pair_touches_since_window(pair, since)]
    return {
        "current_date": current.isoformat(),
        "since_days": since_days,
        "since_date": since.isoformat(),
        "public_exhibition_count": len(rows),
        "high_confidence_duplicate_count": len(window_duplicates),
        "high_confidence_duplicate_cluster_count": _cluster_count(window_duplicates),
        "high_confidence_duplicate_total_count": len(all_duplicates),
        "high_confidence_duplicate_total_cluster_count": _cluster_count(all_duplicates),
        "gate_suppressed": {},
        "duplicates": [asdict(pair) for pair in window_duplicates],
        "all_duplicates": [asdict(pair) for pair in all_duplicates],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--current-date", default=date.today().isoformat())
    parser.add_argument("--since-days", type=int, default=14)
    parser.add_argument("--format", choices=("text", "json", "prometheus", "both"), default="text")
    parser.add_argument("--json", action="store_true", help="Deprecated alias for --format json")
    parser.add_argument("--fail-on-high-confidence", "--fail-on-duplicates", action="store_true")
    args = parser.parse_args()

    try:
        current = date.fromisoformat(args.current_date)
        payload = build_audit_payload(args.db, current=current, since_days=args.since_days)
        duplicates = [DuplicatePair(**pair) for pair in payload["duplicates"]]
    except Exception as exc:
        print(f"audit_public_exhibition_duplicates failed: {exc}", file=sys.stderr)
        return 3

    output_format = "json" if args.json else args.format
    if output_format in {"json", "both"}:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    if output_format in {"prometheus", "both"}:
        print(_prometheus(payload), end="")
    if output_format == "text":
        print(
            f"public_exhibitions={payload['public_exhibition_count']} "
            f"high_confidence_duplicates_since={payload['high_confidence_duplicate_count']} "
            f"high_confidence_duplicates_total={payload['high_confidence_duplicate_total_count']}"
        )
        for pair in duplicates:
            print(f"{pair.left_id} {pair.right_id} {pair.confidence:.3f} {pair.reason}")
    return 2 if args.fail_on_high_confidence and duplicates else 0


if __name__ == "__main__":
    raise SystemExit(main())
