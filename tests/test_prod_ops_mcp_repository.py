from __future__ import annotations

import sqlite3

import pytest

from prod_ops_mcp.repository import ReadOnlyOperationsRepository


def build_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE event(
          id INTEGER PRIMARY KEY,
          title TEXT,
          description TEXT,
          short_description TEXT,
          search_digest TEXT,
          date TEXT,
          time TEXT,
          city TEXT,
          location_name TEXT,
          identity_status TEXT,
          lifecycle_status TEXT,
          telegraph_url TEXT,
          source_text TEXT,
          added_at TEXT
        );
        CREATE TABLE event_source(
          id INTEGER PRIMARY KEY,
          event_id INTEGER,
          source_type TEXT,
          source_url TEXT,
          canonical_source_url TEXT,
          source_role TEXT,
          source_text TEXT,
          imported_at TEXT
        );
        CREATE TABLE event_identity_decision_log(
          id INTEGER PRIMARY KEY,
          event_id INTEGER,
          candidate_event_id INTEGER,
          decision TEXT,
          decision_reason TEXT,
          created_at TEXT
        );
        CREATE TABLE joboutbox(
          id INTEGER PRIMARY KEY,
          event_id INTEGER,
          task TEXT,
          status TEXT,
          attempts INTEGER,
          last_error TEXT,
          updated_at TEXT
        );
        """
    )
    conn.execute(
        """INSERT INTO event(
          id,title,description,short_description,search_digest,date,time,city,
          location_name,identity_status,lifecycle_status,telegraph_url,source_text,added_at
        ) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            "Лекция о море",
            "private long description",
            "Коротко",
            "искусство у моря",
            "2026-08-10",
            "18:00",
            "Калининград",
            "Музей",
            "canonical",
            "active",
            "https://telegra.ph/example",
            "private source body",
            "2026-08-08T00:00:00Z",
        ),
    )
    conn.execute(
        "INSERT INTO event_source VALUES(1,1,'telegram','https://t.me/source/1','https://t.me/source/1','canonical','secret body','2026-08-08')"
    )
    conn.execute(
        "INSERT INTO event_identity_decision_log VALUES(1,1,NULL,'same_event','grounded','2026-08-08')"
    )
    conn.execute(
        "INSERT INTO joboutbox VALUES(1,1,'telegraph_build','done',1,NULL,'2026-08-08')"
    )
    conn.commit()
    conn.close()


@pytest.mark.asyncio
async def test_find_and_explain_never_return_full_private_text(tmp_path):
    db_path = tmp_path / "db.sqlite"
    build_db(db_path)
    repo = ReadOnlyOperationsRepository(str(db_path), query_timeout_ms=500)
    found = await repo.events_find({"query": "море", "limit": 5})
    assert found["count"] == 1
    assert "description" not in found["items"][0]
    assert "source_text" not in found["items"][0]

    explained = await repo.event_explain(1)
    serialized = str(explained)
    assert "private long description" not in serialized
    assert "private source body" not in serialized
    assert explained["sources"][0]["source_url"] == "https://t.me/source/1"
    assert explained["identity_decisions"][0]["decision"] == "same_event"
