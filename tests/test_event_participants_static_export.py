from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "site"
    / "scripts"
    / "export-production-preview-data.py"
)
SPEC = importlib.util.spec_from_file_location("event_participants_preview_exporter", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


def _connection() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        CREATE TABLE artist_registry_entity(
            artist_id TEXT PRIMARY KEY,
            entity_type TEXT NOT NULL,
            display_name TEXT NOT NULL,
            verification_status TEXT NOT NULL,
            photo_url TEXT,
            photo_rights_status TEXT NOT NULL,
            photo_rights_evidence_json TEXT NOT NULL
        );
        CREATE TABLE event_artist_appearance(
            event_id INTEGER NOT NULL,
            artist_id TEXT NOT NULL,
            role TEXT NOT NULL,
            status TEXT NOT NULL,
            physical_visit_status TEXT NOT NULL,
            participant_evidence_json TEXT NOT NULL,
            eligibility_status TEXT NOT NULL,
            cancelled_at TEXT,
            media_identity_status TEXT NOT NULL
        );
        """
    )
    return con


def _insert_participant(
    con: sqlite3.Connection,
    *,
    artist_id: str = "kgd80:levchenkov-andrey",
    name: str = "Андрей Левченков",
    role: str = "speaker",
    status: str = "confirmed",
    verification_status: str = "verified",
    rights_status: str = "event_artist_verified",
    media_identity_status: str = "verified",
    cancelled_at: str | None = None,
) -> None:
    con.execute(
        "INSERT INTO artist_registry_entity VALUES(?,?,?,?,?,?,?)",
        (
            artist_id,
            "person",
            name,
            verification_status,
            "/assets/participants/levchenkov-andrey.webp",
            rights_status,
            json.dumps(
                [
                    {
                        "source_url": "https://kgd80.ru/",
                        "credit_text": "Фотография проекта «80 историй о главном»",
                    }
                ],
                ensure_ascii=False,
            ),
        ),
    )
    con.execute(
        "INSERT INTO event_artist_appearance VALUES(?,?,?,?,?,?,?,?,?)",
        (
            6172,
            artist_id,
            role,
            status,
            "confirmed",
            json.dumps(
                [{"source_url": "https://kgd80.ru/program/levchenkov"}],
                ensure_ascii=False,
            ),
            "eligible",
            cancelled_at,
            media_identity_status,
        ),
    )


def test_old_snapshot_without_registry_tables_fails_closed() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    assert EXPORTER.event_participants(con, 6172) == []


def test_verified_participant_projects_name_role_avatar_and_evidence() -> None:
    con = _connection()
    _insert_participant(con)

    assert EXPORTER.event_participants(con, 6172) == [
        {
            "id": "kgd80:levchenkov-andrey",
            "name": "Андрей Левченков",
            "role": "Спикер",
            "entity_kind": "person",
            "is_headliner": False,
            "avatar_url": "/assets/participants/levchenkov-andrey.webp",
            "avatar_alt": "Андрей Левченков — Спикер",
            "likes_count": 0,
            "profile_url": None,
            "credit_text": "Фотография проекта «80 историй о главном»",
            "credit_url": "https://kgd80.ru/",
            "evidence_url": "https://kgd80.ru/program/levchenkov",
        }
    ]


def test_headliner_is_ordered_first_and_unreviewed_photo_uses_initials_fallback() -> None:
    con = _connection()
    _insert_participant(con)
    _insert_participant(
        con,
        artist_id="guest:pupo",
        name="Pupo",
        role="headliner",
        rights_status="review",
    )

    projected = EXPORTER.event_participants(con, 6172)
    assert [item["name"] for item in projected] == ["Pupo", "Андрей Левченков"]
    assert projected[0]["is_headliner"] is True
    assert projected[0]["avatar_url"] is None
    assert projected[0]["credit_url"] is None


def test_cancelled_or_unverified_participants_are_not_public() -> None:
    con = _connection()
    _insert_participant(con, cancelled_at="2026-07-27T12:00:00Z")
    _insert_participant(
        con,
        artist_id="review:person",
        name="На проверке",
        verification_status="review",
    )

    assert EXPORTER.event_participants(con, 6172) == []
