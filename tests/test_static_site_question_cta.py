from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "site/scripts/export-production-preview-data.py"
SPEC = importlib.util.spec_from_file_location("question_cta_exporter", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


def question_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(
        '''
        create table event(id integer primary key, creator_id integer, source_post_url text);
        create table "user"(user_id integer primary key, is_partner integer, organization text);
        create table organization(name text primary key, vk_source_group_ids text);
        create table event_source(
          id integer primary key, event_id integer, source_type text, source_url text,
          source_chat_id integer, source_message_id integer, imported_at text
        );
        create table event_publication(
          id integer primary key, event_id integer, platform text, target text,
          status text, stored_url text, live_url text, resolved_at text
        );
        insert into organization values('Partner Org', '[30777579]');
        insert into "user" values(100, 1, 'Partner Org');
        insert into "user" values(200, 0, null);
        '''
    )
    return con


def event_row(con: sqlite3.Connection, event_id: int, creator_id: int, source_url: str | None = None):
    con.execute("insert into event values(?,?,?)", (event_id, creator_id, source_url))
    return con.execute("select * from event where id=?", (event_id,)).fetchone()


def add_live_partner_source(
    con: sqlite3.Connection,
    event_id: int,
    post_id: int,
    *,
    owner_id: int = 30777579,
    imported_at: str | None = "2026-08-03T11:00:00Z",
    source_chat_id: int | None = None,
    source_message_id: int | None = None,
) -> None:
    con.execute(
        """
        insert into event_source(
          event_id,source_type,source_url,source_chat_id,source_message_id,imported_at
        ) values(?,?,?,?,?,?)
        """,
        (
            event_id,
            "vk",
            f"https://vk.com/wall-{owner_id}_{post_id}",
            owner_id if source_chat_id is None else source_chat_id,
            post_id if source_message_id is None else source_message_id,
            imported_at,
        ),
    )


def test_partner_published_post_wins_over_managed_afisha(monkeypatch) -> None:
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    con = question_db()
    row = event_row(con, 1, 100, "https://vk.com/wall-30777579_44")
    add_live_partner_source(con, 1, 44)
    con.execute(
        "insert into event_publication values(1,1,'vk','klgdevents','published',null,?,?)",
        ("https://vk.com/wall-231920894_77", "2026-08-03T12:00:00Z"),
    )

    assert EXPORTER.event_question_cta(con, 1, row) == {
        "provider": "vk",
        "url": "https://vk.com/wall-30777579_44",
        "source": "partner_post",
    }


def test_partner_source_must_belong_to_declared_partner_community(monkeypatch) -> None:
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    con = question_db()
    row = event_row(con, 2, 100, "https://vk.com/wall-999_2")
    add_live_partner_source(con, 2, 45)

    assert EXPORTER.event_question_cta(con, 2, row) == {
        "provider": "vk",
        "url": "https://vk.com/wall-30777579_45",
        "source": "partner_post",
    }


def test_non_partner_uses_only_published_live_managed_post(monkeypatch) -> None:
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    con = question_db()
    row = event_row(con, 3, 200, "https://vk.com/wall-999_3")
    con.execute(
        "insert into event_publication values(3,3,'vk','klgdevents','published',?, ?, ?)",
        (
            "https://vk.com/wall-231920894_30",
            "https://vk.com/wall-231920894_31",
            "2026-08-03T12:00:00Z",
        ),
    )

    assert EXPORTER.event_question_cta(con, 3, row) == {
        "provider": "vk",
        "url": "https://vk.com/wall-231920894_31",
        "source": "managed_afisha_post",
    }


def test_scheduled_stored_or_wrong_group_managed_urls_fail_closed(monkeypatch) -> None:
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    con = question_db()
    scheduled = event_row(con, 4, 200, "https://example.test/arbitrary")
    con.execute(
        "insert into event_publication values(4,4,'vk','klgdevents','scheduled',?,null,?)",
        ("https://vk.com/wall-231920894_40", "2026-08-03T12:00:00Z"),
    )
    wrong_group = event_row(con, 5, 200)
    con.execute(
        "insert into event_publication values(5,5,'vk','klgdevents','published',null,?,?)",
        ("https://vk.com/wall-999_50", "2026-08-03T12:00:00Z"),
    )

    assert EXPORTER.event_question_cta(con, 4, scheduled) is None
    assert EXPORTER.event_question_cta(con, 5, wrong_group) is None


def test_partner_looking_stored_or_unproven_url_falls_back_to_managed(monkeypatch) -> None:
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    con = question_db()
    row = event_row(con, 6, 100, "https://vk.com/wall-30777579_60")
    # A partner-looking stored/scheduled value is explicitly not live evidence.
    con.execute(
        "insert into event_publication values(6,6,'vk','partner_source','scheduled',?,null,?)",
        ("https://vk.com/wall-30777579_60", "2026-08-03T11:00:00Z"),
    )
    con.execute(
        "insert into event_publication values(7,6,'vk','klgdevents','published',null,?,?)",
        ("https://vk.com/wall-231920894_61", "2026-08-03T12:00:00Z"),
    )

    assert EXPORTER.event_question_cta(con, 6, row) == {
        "provider": "vk",
        "url": "https://vk.com/wall-231920894_61",
        "source": "managed_afisha_post",
    }


def test_partner_source_without_exact_live_import_provenance_fails_closed() -> None:
    con = question_db()
    missing_timestamp = event_row(con, 7, 100, "https://vk.com/wall-30777579_70")
    add_live_partner_source(con, 7, 70, imported_at=None)
    mismatched_ids = event_row(con, 8, 100, "https://vk.com/wall-30777579_80")
    add_live_partner_source(con, 8, 80, source_message_id=81)
    malformed_timestamp = event_row(con, 9, 100, "https://vk.com/wall-30777579_90")
    add_live_partner_source(con, 9, 90, imported_at="not-a-timestamp")

    assert EXPORTER.event_question_cta(con, 7, missing_timestamp) is None
    assert EXPORTER.event_question_cta(con, 8, mismatched_ids) is None
    assert EXPORTER.event_question_cta(con, 9, malformed_timestamp) is None


def test_old_snapshot_without_publication_contract_fails_closed() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("create table event(id integer primary key, creator_id integer, source_post_url text)")
    con.execute("insert into event values(1,null,'https://vk.com/wall-1_2')")
    row = con.execute("select * from event").fetchone()

    assert EXPORTER.event_question_cta(con, 1, row) is None
