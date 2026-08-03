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
        create table event_source(id integer primary key, event_id integer, source_type text, source_url text);
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


def test_partner_published_post_wins_over_managed_afisha(monkeypatch) -> None:
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    con = question_db()
    row = event_row(con, 1, 100, "https://vk.com/wall-30777579_44")
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
    con.execute(
        "insert into event_source(event_id,source_type,source_url) values(2,'vk',?)",
        ("https://vk.com/wall-30777579_45",),
    )

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


def test_old_snapshot_without_publication_contract_fails_closed() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("create table event(id integer primary key, creator_id integer, source_post_url text)")
    con.execute("insert into event values(1,null,'https://vk.com/wall-1_2')")
    row = con.execute("select * from event").fetchone()

    assert EXPORTER.event_question_cta(con, 1, row) is None
