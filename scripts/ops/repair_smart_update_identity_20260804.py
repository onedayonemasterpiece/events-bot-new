#!/usr/bin/env python3
"""One-shot, fail-closed repair for INC-2026-08-04 Smart Update identity drift.

The program is intentionally incident-specific.  It never prints source text or
media URLs.  Exact rows needed for rollback are retained inside the target
SQLite database in a restricted incident backup table; stdout/output JSON only
contains ids, field names, counts, hashes, and public identity anchors.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


INCIDENT = "INC-2026-08-04-smart-update-identity-source-replay-corruption"
BACKUP_TABLE = "incident_20260804_smart_identity_before"
META_TABLE = "incident_20260804_smart_identity_meta"

PIANISSIMO_URL = "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46315/2026-08-07/20:00:00"
KAGUYA_URL = "https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/48636/2026-08-09/17:00:00"
WOMEN_SEA_URL = "https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/48801/2026-08-09/14:00:00"
URBAN_CONTEXT_URL = "https://t.me/urban_literature/15"

EVENT_IDS = frozenset({3216, 3864, 4827, 4828, 4830, 4831, 5269, 5294, 5296, 5297, 5298, 5316, 7024, 7244, 7435})
NEGATIVE_CONTROL_EVENT_ID = 7151

MOVE_SOURCE = {713970: 3216}
DUPLICATE_SOURCES = {
    1826515: 4827,
    1826516: 4828,
    1826518: 4830,
    1826519: 4831,
    2144807: 5316,
    2371505: 5294,
    2377807: 5269,
    2377808: 5296,
    2377809: 5297,
    2377810: 5298,
}
QUARANTINE_DELETE_SOURCES = frozenset({1318151, 1331928, 1826514, 1826517, 1826520, 2377806, 2377811})
WRONG_BINDING_SOURCES = frozenset({9404562})
DELETE_SOURCES = frozenset((*DUPLICATE_SOURCES, *QUARANTINE_DELETE_SOURCES, *WRONG_BINDING_SOURCES))

IDENTITY_SOURCES = {
    713970: 3216,
    2377812: 3864,
    9404476: 7024,
    9573730: 7244,
    10949059: 7435,
    10971966: 7435,
}
CONTEXT_SOURCES = {
    10903283: 3864,
    10903286: 7024,
    10903285: 7244,
    10971967: 7435,
}
TOUCHED_SOURCE_IDS = frozenset({*MOVE_SOURCE, *DELETE_SOURCES, *IDENTITY_SOURCES, *CONTEXT_SOURCES})

MOVE_POSTERS = {6583: 3216}
REJECT_POSTERS_3864 = frozenset({*range(6584, 6589), *range(14164, 14181), 18395, 18396})
DELETE_POSTERS = frozenset({15555, 15915, 16554, 17132, 18588})
TOUCHED_POSTER_IDS = frozenset({*MOVE_POSTERS, *REJECT_POSTERS_3864, *DELETE_POSTERS})
PRICE_FACT_IDS = frozenset({142751, 142754, 146855})

TRACKING_KEYS = {"_openstat", "fbclid", "from", "gclid", "igshid", "mc_cid", "mc_eid", "ref", "source", "yclid"}


class RepairBlocked(RuntimeError):
    pass


def _sha(value: str | bytes) -> str:
    data = value if isinstance(value, bytes) else value.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _canonical_url(value: str | None) -> str:
    raw = str(value or "").strip().strip("<>\"'")
    if not raw:
        return ""
    if "://" not in raw:
        return raw
    parts = urlsplit(raw)
    host = (parts.hostname or "").lower()
    if host in {"telegram.me", "www.telegram.me", "www.t.me"}:
        host = "t.me"
    if host in {"m.vk.com", "m.vk.ru", "vk.ru", "www.vk.com", "www.vk.ru"}:
        host = "vk.com"
    path = re.sub(r"/+", "/", parts.path or "/")
    if host == "t.me":
        bits = [x for x in path.split("/") if x]
        if bits and bits[0].lower() == "s":
            bits = bits[1:]
        if bits:
            bits[0] = bits[0].lower()
        path = "/" + "/".join(bits)
    elif host == "vk.com":
        path = path.lower()
    if path != "/":
        path = path.rstrip("/")
    query = []
    for key, val in parse_qsl(parts.query, keep_blank_values=True):
        low = key.lower()
        if low.startswith("utm_") or low in TRACKING_KEYS or (host == "t.me" and low == "single"):
            continue
        query.append((key, val))
    query.sort()
    fragment = (parts.fragment or "").strip()
    normalized_fragment = ""
    route = fragment.lstrip("/")
    if re.match(r"(?i)^buy/event/[^/]+/[^/]+/[^/]+$", route):
        normalized_fragment = "/" + route
    netloc = host
    if parts.port:
        netloc += f":{parts.port}"
    return urlunsplit(((parts.scheme or "https").lower(), netloc, path, urlencode(query), normalized_fragment))


def _table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")]


def _require_schema(con: sqlite3.Connection) -> None:
    required = {
        "event": {"id", "title", "date", "time", "source_text", "source_texts", "telegraph_url", "telegraph_path"},
        "event_source": {"id", "event_id", "source_url", "source_text", "canonical_source_url", "source_role", "source_fingerprint"},
        "event_source_fact": {"id", "event_id", "source_id", "fact"},
        "eventposter": {"id", "event_id", "review_status", "review_reason"},
        "joboutbox": {"id", "event_id", "task", "status"},
        "event_identity_decision_log": {"id", "event_id"},
    }
    for table, columns in required.items():
        existing = set(_table_columns(con, table))
        missing = sorted(columns - existing)
        if missing:
            raise RepairBlocked(f"required_schema_missing:{table}:{','.join(missing)}")


def _rows(con: sqlite3.Connection, table: str, where: str, params: Iterable[Any]) -> list[dict[str, Any]]:
    cur = con.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY id", tuple(params))
    return [dict(row) for row in cur.fetchall()]


def _rowset_hash(rows: Iterable[dict[str, Any]]) -> str:
    return _sha(_json(list(rows)))


def _graph_hash(con: sqlite3.Connection, event_id: int) -> str:
    payload: dict[str, Any] = {}
    for table in ("event", "event_source", "event_source_fact", "eventposter", "joboutbox", "event_identity_decision_log"):
        payload[table] = _rows(con, table, "event_id=?" if table != "event" else "id=?", (event_id,))
    return _sha(_json(payload))


def _one(con: sqlite3.Connection, sql: str, params: Iterable[Any]) -> sqlite3.Row:
    row = con.execute(sql, tuple(params)).fetchone()
    if row is None:
        raise RepairBlocked("required_row_missing")
    return row


def _source(con: sqlite3.Connection, source_id: int) -> sqlite3.Row | None:
    return con.execute("SELECT * FROM event_source WHERE id=?", (source_id,)).fetchone()


def _event(con: sqlite3.Connection, event_id: int) -> sqlite3.Row:
    return _one(con, "SELECT * FROM event WHERE id=?", (event_id,))


def _assert_anchor(event: sqlite3.Row, *, title: str, date: str, time: str, ticket_token: str | None = None) -> None:
    if str(event["date"] or "") != date or str(event["time"] or "") != time:
        raise RepairBlocked(f"event_anchor_mismatch:{event['id']}:date_time")
    if title.casefold() not in str(event["title"] or "").casefold():
        raise RepairBlocked(f"event_anchor_mismatch:{event['id']}:title")
    if ticket_token and ticket_token not in str(event["ticket_link"] or ""):
        raise RepairBlocked(f"event_anchor_mismatch:{event['id']}:ticket")


def _is_final(con: sqlite3.Connection) -> bool:
    if any(_source(con, sid) is not None for sid in DELETE_SOURCES):
        return False
    moved = _source(con, 713970)
    if moved is None or int(moved["event_id"]) != 3216:
        return False
    for sid, event_id in IDENTITY_SOURCES.items():
        row = _source(con, sid)
        if row is None or int(row["event_id"]) != event_id or row["source_role"] != "identity_bearing":
            return False
        if not str(row["canonical_source_url"] or ""):
            return False
    for sid, event_id in CONTEXT_SOURCES.items():
        row = _source(con, sid)
        if row is None or int(row["event_id"]) != event_id or row["source_role"] != "context_only":
            return False
    e3864, e7024, e7244 = (_event(con, eid) for eid in (3864, 7024, 7244))
    if str(e3864["title"]) != "Фестиваль Pianissimo: Константин Хачикян":
        return False
    if e3864["telegraph_url"] is not None or e3864["telegraph_path"] is not None:
        return False
    if str(e7024["age_restriction"] or "") != "16+" or "48636" not in str(e7024["age_restriction_source_url"] or ""):
        return False
    if int(e7244["is_free"] or 0) != 1 or e7244["ticket_price_min"] is not None or e7244["ticket_price_max"] is not None:
        return False
    if str(e7244["age_restriction"] or "") != "12+" or "48801" not in str(e7244["age_restriction_source_url"] or ""):
        return False
    if con.execute("SELECT COUNT(*) FROM event_source_fact WHERE id IN (142751,142754,146855)").fetchone()[0]:
        return False
    if any(con.execute("SELECT 1 FROM eventposter WHERE id=?", (pid,)).fetchone() for pid in DELETE_POSTERS):
        return False
    if int(_one(con, "SELECT event_id FROM eventposter WHERE id=6583", ())[0]) != 3216:
        return False
    return True


def _assert_initial(con: sqlite3.Connection) -> None:
    e3864 = _event(con, 3864)
    e7024 = _event(con, 7024)
    e7244 = _event(con, 7244)
    _assert_anchor(e3864, title="Хачикян", date="2026-08-07", time="20:00", ticket_token="46315")
    _assert_anchor(e7024, title="Кагуя", date="2026-08-09", time="17:00", ticket_token="48636")
    _assert_anchor(e7244, title="Право женщин на море", date="2026-08-09", time="14:00", ticket_token="48801")
    if str(e3864["telegraph_path"] or "") != "Velikie-uchitelya-04-13":
        raise RepairBlocked("event_anchor_mismatch:3864:telegraph")
    if str(e7024["age_restriction_status"] or "") != "conflict" or "48801" not in str(e7024["age_restriction_source_url"] or ""):
        raise RepairBlocked("event_anchor_mismatch:7024:age_conflict")
    if int(e7244["is_free"] or 0) != 1 or (e7244["ticket_price_min"], e7244["ticket_price_max"]) != (1500, 2000):
        raise RepairBlocked("event_anchor_mismatch:7244:free_price_model")
    _assert_anchor(_event(con, 7435), title="Хочу машину", date="2026-08-08", time="16:00")
    _event(con, 3216)
    _event(con, NEGATIVE_CONTROL_EVENT_ID)

    initial_source_events = {
        **{sid: 3864 for sid in DUPLICATE_SOURCES},
        **{sid: 3864 for sid in QUARANTINE_DELETE_SOURCES},
        9404562: 7024,
        **IDENTITY_SOURCES,
        **CONTEXT_SOURCES,
    }
    initial_source_events[713970] = 3864
    for sid, event_id in initial_source_events.items():
        row = _source(con, sid)
        if row is None or int(row["event_id"]) != event_id:
            raise RepairBlocked(f"source_precondition_mismatch:{sid}")

    for source_id, destination in DUPLICATE_SOURCES.items():
        source = _source(con, source_id)
        assert source is not None
        duplicate = con.execute(
            "SELECT id FROM event_source WHERE event_id=? AND source_url=? AND id<>? LIMIT 1",
            (destination, source["source_url"], source_id),
        ).fetchone()
        if duplicate is None:
            raise RepairBlocked(f"destination_source_missing:{source_id}:{destination}")
    wrong = _source(con, 9404562)
    right = _source(con, 9573730)
    if wrong is None or right is None or _canonical_url(wrong["source_url"]) != _canonical_url(right["source_url"]):
        raise RepairBlocked("wrong_48801_binding_precondition")
    if int(con.execute("SELECT COUNT(*) FROM event_source_fact WHERE source_id=713970").fetchone()[0]) != 16:
        raise RepairBlocked("source_fact_precondition:713970")
    if int(con.execute("SELECT COUNT(*) FROM event_source_fact WHERE source_id=9404562").fetchone()[0]) != 9:
        raise RepairBlocked("source_fact_precondition:9404562")
    if int(con.execute("SELECT COUNT(*) FROM event_source_fact WHERE source_id=10971967").fetchone()[0]) != 0:
        raise RepairBlocked("source_fact_precondition:10971967")

    poster_expect = {**{pid: 3864 for pid in MOVE_POSTERS}, **{pid: 3864 for pid in REJECT_POSTERS_3864}, 15555: 7024, 15915: 7024, 16554: 7244, 17132: 7244, 18588: 7244}
    for poster_id, event_id in poster_expect.items():
        row = con.execute("SELECT event_id FROM eventposter WHERE id=?", (poster_id,)).fetchone()
        if row is None or int(row[0]) != event_id:
            raise RepairBlocked(f"poster_precondition_mismatch:{poster_id}")
    actual_price_facts = {int(row[0]) for row in con.execute("SELECT id FROM event_source_fact WHERE event_id=7244 AND id IN (142751,142754,146855)")}
    if actual_price_facts != set(PRICE_FACT_IDS):
        raise RepairBlocked("price_fact_precondition_mismatch")
    running = con.execute(
        f"SELECT COUNT(*) FROM joboutbox WHERE event_id IN ({','.join('?' for _ in EVENT_IDS)}) AND status='running'",
        tuple(sorted(EVENT_IDS)),
    ).fetchone()[0]
    if running:
        raise RepairBlocked(f"affected_job_running:{running}")


def _sanitized_snapshot(con: sqlite3.Connection) -> dict[str, Any]:
    events = []
    for event_id in (3216, 3864, 7024, 7244, 7435, NEGATIVE_CONTROL_EVENT_ID):
        row = _event(con, event_id)
        events.append({
            "event_id": event_id,
            "row_sha256": _rowset_hash([dict(row)]),
            "date": row["date"],
            "time": row["time"],
            "changed_surface_present": {
                "telegraph": bool(row["telegraph_url"]),
                "ics": bool(row["ics_url"]),
            },
        })
    sources = []
    for source_id in sorted(TOUCHED_SOURCE_IDS):
        row = _source(con, source_id)
        sources.append({
            "source_id": source_id,
            "event_id": int(row["event_id"]) if row else None,
            "present": row is not None,
            "source_role": row["source_role"] if row else None,
            "source_url_sha256": _sha(str(row["source_url"])) if row else None,
            "fact_count": int(con.execute("SELECT COUNT(*) FROM event_source_fact WHERE source_id=?", (source_id,)).fetchone()[0]),
        })
    return {
        "events": events,
        "sources": sources,
        "poster_ids": sorted(int(row[0]) for row in con.execute(f"SELECT id FROM eventposter WHERE id IN ({','.join('?' for _ in TOUCHED_POSTER_IDS)})", tuple(sorted(TOUCHED_POSTER_IDS)))),
        "negative_control_graph_sha256": _graph_hash(con, NEGATIVE_CONTROL_EVENT_ID),
    }


def _create_backup(con: sqlite3.Connection, before: dict[str, Any]) -> None:
    con.execute(f"CREATE TABLE IF NOT EXISTS {BACKUP_TABLE} (table_name TEXT NOT NULL, row_id INTEGER NOT NULL, row_json TEXT NOT NULL, row_sha256 TEXT NOT NULL, PRIMARY KEY(table_name,row_id))")
    con.execute(f"CREATE TABLE IF NOT EXISTS {META_TABLE} (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    selections = {
        "event": ("id IN (3864,7024,7244)", ()),
        "event_source": (f"id IN ({','.join('?' for _ in TOUCHED_SOURCE_IDS)})", tuple(sorted(TOUCHED_SOURCE_IDS))),
        "event_source_fact": (f"source_id IN ({','.join('?' for _ in TOUCHED_SOURCE_IDS)}) OR id IN (142751,142754,146855)", tuple(sorted(TOUCHED_SOURCE_IDS))),
        "eventposter": (f"id IN ({','.join('?' for _ in TOUCHED_POSTER_IDS)})", tuple(sorted(TOUCHED_POSTER_IDS))),
    }
    for table, (where, params) in selections.items():
        for row in _rows(con, table, where, params):
            raw = _json(row)
            con.execute(
                f"INSERT OR IGNORE INTO {BACKUP_TABLE}(table_name,row_id,row_json,row_sha256) VALUES(?,?,?,?)",
                (table, int(row["id"]), raw, _sha(raw)),
            )
    con.execute(f"INSERT OR IGNORE INTO {META_TABLE}(key,value) VALUES('negative_control_graph_sha256',?)", (before["negative_control_graph_sha256"],))
    baseline_orphans = int(con.execute("SELECT COUNT(*) FROM event_source_fact f LEFT JOIN event_source s ON s.id=f.source_id WHERE s.id IS NULL").fetchone()[0])
    con.execute(f"INSERT OR IGNORE INTO {META_TABLE}(key,value) VALUES('baseline_orphan_fact_count',?)", (str(baseline_orphans),))
    baseline_fk = len(con.execute("PRAGMA foreign_key_check").fetchall())
    con.execute(f"INSERT OR IGNORE INTO {META_TABLE}(key,value) VALUES('baseline_foreign_key_violation_count',?)", (str(baseline_fk),))
    con.execute(f"INSERT OR IGNORE INTO {META_TABLE}(key,value) VALUES('incident',?)", (INCIDENT,))


def _set_source_identity(con: sqlite3.Connection, source_id: int, event_id: int, role: str) -> None:
    row = _source(con, source_id)
    if row is None or int(row["event_id"]) != event_id:
        raise RepairBlocked(f"source_write_precondition:{source_id}")
    canonical = _canonical_url(row["source_url"])
    if not canonical:
        raise RepairBlocked(f"source_canonical_empty:{source_id}")
    cur = con.execute(
        "UPDATE event_source SET canonical_source_url=?, source_role=?, source_fingerprint=NULL WHERE id=? AND event_id=?",
        (canonical, role, source_id, event_id),
    )
    if cur.rowcount != 1:
        raise RepairBlocked(f"source_write_count:{source_id}:{cur.rowcount}")


def _direct_text(con: sqlite3.Connection, source_id: int) -> str:
    row = _source(con, source_id)
    if row is None or not str(row["source_text"] or "").strip():
        raise RepairBlocked(f"direct_source_text_missing:{source_id}")
    return str(row["source_text"])


def _apply(con: sqlite3.Connection) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    # Move the confirmed exhibition source/facts/media as one identity graph.
    if con.execute("UPDATE event_source SET event_id=? WHERE id=? AND event_id=3864", (3216, 713970)).rowcount != 1:
        raise RepairBlocked("move_source_713970_failed")
    con.execute("UPDATE event_source_fact SET event_id=3216 WHERE source_id=713970 AND event_id=3864")
    if con.execute("UPDATE eventposter SET event_id=3216 WHERE id=6583 AND event_id=3864").rowcount != 1:
        raise RepairBlocked("move_poster_6583_failed")
    changes.append({"entity": "event_source", "id": 713970, "fields": ["event_id"], "from_event_id": 3864, "to_event_id": 3216})

    # Duplicate and unresolved contaminating bindings are backed up first, then
    # removed with their facts. No raw source payload is printed.
    if DELETE_SOURCES:
        placeholders = ",".join("?" for _ in DELETE_SOURCES)
        con.execute(f"DELETE FROM event_source_fact WHERE source_id IN ({placeholders})", tuple(sorted(DELETE_SOURCES)))
        cur = con.execute(f"DELETE FROM event_source WHERE id IN ({placeholders})", tuple(sorted(DELETE_SOURCES)))
        if cur.rowcount != len(DELETE_SOURCES):
            raise RepairBlocked(f"delete_source_count:{cur.rowcount}")
        changes.append({"entity": "event_source", "ids": sorted(DELETE_SOURCES), "fields": ["deleted_after_restricted_backup"]})

    for source_id, event_id in IDENTITY_SOURCES.items():
        _set_source_identity(con, source_id, event_id, "identity_bearing")
    for source_id, event_id in CONTEXT_SOURCES.items():
        _set_source_identity(con, source_id, event_id, "context_only")
    changes.append({"entity": "event_source", "ids": sorted((*IDENTITY_SOURCES, *CONTEXT_SOURCES)), "fields": ["canonical_source_url", "source_role", "source_fingerprint"]})

    # Rejected candidates remain review evidence but are ineligible for public
    # selection; known cross-event duplicates are removed after backup.
    if REJECT_POSTERS_3864:
        placeholders = ",".join("?" for _ in REJECT_POSTERS_3864)
        cur = con.execute(
            f"UPDATE eventposter SET review_status='rejected', review_reason=? WHERE id IN ({placeholders}) AND event_id=3864",
            (INCIDENT, *sorted(REJECT_POSTERS_3864)),
        )
        if cur.rowcount != len(REJECT_POSTERS_3864):
            raise RepairBlocked(f"reject_poster_count:{cur.rowcount}")
    if DELETE_POSTERS:
        placeholders = ",".join("?" for _ in DELETE_POSTERS)
        cur = con.execute(f"DELETE FROM eventposter WHERE id IN ({placeholders})", tuple(sorted(DELETE_POSTERS)))
        if cur.rowcount != len(DELETE_POSTERS):
            raise RepairBlocked(f"delete_poster_count:{cur.rowcount}")
    changes.append({"entity": "eventposter", "ids": sorted(TOUCHED_POSTER_IDS), "fields": ["event_id", "review_status", "review_reason", "deleted_after_restricted_backup"]})

    text3864 = _direct_text(con, 2377812)
    text7024 = _direct_text(con, 9404476)
    text7244 = _direct_text(con, 9573730)
    con.execute(
        "UPDATE event SET title=?, source_text=?, source_texts=?, telegraph_url=NULL, telegraph_path=NULL WHERE id=3864",
        ("Фестиваль Pianissimo: Константин Хачикян", text3864, _json([text3864])),
    )
    con.execute(
        "UPDATE event SET source_text=?, source_texts=?, age_restriction='16+', age_restriction_status='declared', age_restriction_provenance='official_direct_source', age_restriction_source_url=?, age_restriction_confidence=1.0 WHERE id=7024",
        (text7024, _json([text7024]), KAGUYA_URL),
    )
    con.execute(
        "UPDATE event SET source_text=?, source_texts=?, is_free=1, ticket_price_min=NULL, ticket_price_max=NULL, age_restriction='12+', age_restriction_status='declared', age_restriction_provenance='official_direct_source', age_restriction_source_url=?, age_restriction_confidence=1.0 WHERE id=7244",
        (text7244, _json([text7244]), WOMEN_SEA_URL),
    )
    con.execute("DELETE FROM event_source_fact WHERE id IN (142751,142754,146855) AND event_id=7244")
    changes.extend([
        {"entity": "event", "id": 3864, "fields": ["title", "source_text_sha256", "source_texts_sha256", "telegraph_url", "telegraph_path"]},
        {"entity": "event", "id": 7024, "fields": ["source_text_sha256", "source_texts_sha256", "age_restriction", "age_restriction_status", "age_restriction_provenance", "age_restriction_source_url", "age_restriction_confidence"]},
        {"entity": "event", "id": 7244, "fields": ["source_text_sha256", "source_texts_sha256", "is_free", "ticket_price_min", "ticket_price_max", "age_restriction", "age_restriction_status", "age_restriction_provenance", "age_restriction_source_url", "age_restriction_confidence"]},
        {"entity": "event_source_fact", "ids": sorted(PRICE_FACT_IDS), "fields": ["deleted_after_restricted_backup"]},
    ])

    # The authoritative concurrency invariant covers every classified nonblank
    # source. Legacy unclassified rows remain NULL and outside this incident.
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_event_source_event_canonical ON event_source(event_id,canonical_source_url) WHERE canonical_source_url IS NOT NULL AND canonical_source_url<>''")
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_event_source_identity_canonical ON event_source(canonical_source_url) WHERE source_role='identity_bearing' AND canonical_source_url IS NOT NULL AND canonical_source_url<>''")
    return changes


def _verify(con: sqlite3.Connection) -> dict[str, Any]:
    if not _is_final(con):
        raise RepairBlocked("final_state_mismatch")
    placeholders = ",".join("?" for _ in TOUCHED_SOURCE_IDS)
    touched_orphan_facts = int(con.execute(
        f"SELECT COUNT(*) FROM event_source_fact f LEFT JOIN event_source s ON s.id=f.source_id WHERE f.source_id IN ({placeholders}) AND s.id IS NULL",
        tuple(sorted(TOUCHED_SOURCE_IDS)),
    ).fetchone()[0])
    if touched_orphan_facts:
        raise RepairBlocked(f"touched_orphan_facts:{touched_orphan_facts}")
    global_orphan_facts = int(con.execute("SELECT COUNT(*) FROM event_source_fact f LEFT JOIN event_source s ON s.id=f.source_id WHERE s.id IS NULL").fetchone()[0])
    fk_count = len(con.execute("PRAGMA foreign_key_check").fetchall())
    quick = str(con.execute("PRAGMA quick_check").fetchone()[0])
    if quick.lower() != "ok":
        raise RepairBlocked("quick_check_failed")
    indexes = {str(row[0]) for row in con.execute("SELECT name FROM sqlite_master WHERE type='index' AND name IN ('ux_event_source_event_canonical','ux_event_source_identity_canonical')")}
    if indexes != {"ux_event_source_event_canonical", "ux_event_source_identity_canonical"}:
        raise RepairBlocked("source_identity_indexes_missing")
    negative_hash = _graph_hash(con, NEGATIVE_CONTROL_EVENT_ID)
    stored = con.execute(f"SELECT value FROM {META_TABLE} WHERE key='negative_control_graph_sha256'").fetchone() if META_TABLE in {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")} else None
    if stored is not None and str(stored[0]) != negative_hash:
        raise RepairBlocked("negative_control_7151_changed")
    stored_orphans = con.execute(f"SELECT value FROM {META_TABLE} WHERE key='baseline_orphan_fact_count'").fetchone() if stored is not None else None
    if stored_orphans is not None and global_orphan_facts > int(stored_orphans[0]):
        raise RepairBlocked("global_orphan_fact_count_increased")
    stored_fk = con.execute(f"SELECT value FROM {META_TABLE} WHERE key='baseline_foreign_key_violation_count'").fetchone() if stored is not None else None
    if stored_fk is not None and fk_count > int(stored_fk[0]):
        raise RepairBlocked("foreign_key_violation_count_increased")
    return {
        "quick_check": quick,
        "foreign_key_violation_count": fk_count,
        "foreign_key_violation_baseline": int(stored_fk[0]) if stored_fk is not None else None,
        "touched_orphan_facts": touched_orphan_facts,
        "global_orphan_fact_count": global_orphan_facts,
        "global_orphan_fact_baseline": int(stored_orphans[0]) if stored_orphans is not None else None,
        "indexes": sorted(indexes),
        "negative_control_event_id": NEGATIVE_CONTROL_EVENT_ID,
        "negative_control_graph_sha256": negative_hash,
    }


def run(db_path: str, mode: str, *, confirm_7244_free: bool = False) -> dict[str, Any]:
    uri = f"file:{Path(db_path).resolve()}?mode={'ro' if mode in {'dry-run', 'verify'} else 'rw'}"
    con = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA foreign_keys=ON")
    try:
        _require_schema(con)
        before = _sanitized_snapshot(con)
        if mode == "verify":
            verification = _verify(con)
            return {"incident": INCIDENT, "mode": mode, "status": "verified", "changed": False, "verification": verification, "snapshot": before}

        final = _is_final(con)
        if not final:
            _assert_initial(con)
            if mode == "apply" and not confirm_7244_free:
                raise RepairBlocked("manual_confirmation_required:7244_official_free_model")
        planned = [
            {"event_id": 3864, "decision": "retain_pianissimo_split_exhibition", "changed_fields": ["title", "sources", "facts", "posters", "source_texts", "telegraph_binding"]},
            {"event_id": 7024, "decision": "detach_48801_keep_48636", "changed_fields": ["sources", "facts", "posters", "source_texts", "age"]},
            {"event_id": 7244, "decision": "retain_48801_free_model", "changed_fields": ["price", "facts", "posters", "source_texts", "age"]},
            {"event_id": 7435, "decision": "standalone_context_source", "changed_fields": ["source_role"]},
        ]
        if mode == "dry-run":
            return {"incident": INCIDENT, "mode": mode, "status": "noop" if final else "ready", "changed": False, "planned": [] if final else planned, "snapshot": before}

        con.execute("BEGIN IMMEDIATE")
        try:
            before = _sanitized_snapshot(con)
            if _is_final(con):
                con.rollback()
                return {"incident": INCIDENT, "mode": mode, "status": "noop", "changed": False, "diff": [], "snapshot": before, "verification": _verify(con)}
            _assert_initial(con)
            negative_before = _graph_hash(con, NEGATIVE_CONTROL_EVENT_ID)
            _create_backup(con, before)
            diff = _apply(con)
            verification = _verify(con)
            if verification["negative_control_graph_sha256"] != negative_before:
                raise RepairBlocked("negative_control_7151_changed_in_transaction")
            con.commit()
        except Exception:
            con.rollback()
            raise
        return {"incident": INCIDENT, "mode": mode, "status": "applied", "changed": True, "diff": diff, "snapshot": before, "verification": verification}
    finally:
        con.close()


def _write_result(result: dict[str, Any], output: str) -> None:
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if output == "-":
        sys.stdout.write(rendered)
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")
    sys.stdout.write(json.dumps({"status": result.get("status"), "changed": result.get("changed"), "output": str(path)}, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    group.add_argument("--verify", action="store_true")
    parser.add_argument(
        "--confirm-7244-free",
        action="store_true",
        help="Confirm the operator rechecked the official 48801 source and accepted the free/no-price model",
    )
    parser.add_argument("--db", default=os.getenv("DB_PATH", "/data/db.sqlite"))
    parser.add_argument("--output", default="-")
    args = parser.parse_args()
    mode = "apply" if args.apply else "verify" if args.verify else "dry-run"
    try:
        result = run(str(args.db), mode, confirm_7244_free=bool(args.confirm_7244_free))
        _write_result(result, str(args.output))
        return 0
    except RepairBlocked as exc:
        sys.stderr.write(json.dumps({"incident": INCIDENT, "mode": mode, "status": "blocked", "reason": str(exc)}, sort_keys=True) + "\n")
        return 2
    except (sqlite3.DatabaseError, OSError, ValueError) as exc:
        # Only the exception class is exposed; SQLite messages can contain
        # values from a failing constraint and therefore remain private.
        sys.stderr.write(json.dumps({"incident": INCIDENT, "mode": mode, "status": "error", "exception_class": type(exc).__name__}, sort_keys=True) + "\n")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
