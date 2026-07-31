from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "site"
    / "scripts"
    / "export-production-preview-data.py"
)
SPEC = importlib.util.spec_from_file_location("event_video_preview_exporter", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


def _connection(*, with_optional_columns: bool = True) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    optional = (
        """
        , cdn_path TEXT
        , mime_type TEXT
        , duration_seconds REAL
        , aesthetic_score REAL
        , technical_score REAL
        , showcase_score REAL
        , description TEXT
        , search_text TEXT
        """
        if with_optional_columns
        else ""
    )
    relevance = ", event_relevance_score REAL, ranking_score REAL" if with_optional_columns else ""
    con.executescript(
        f"""
        CREATE TABLE video_asset(
            id INTEGER PRIMARY KEY,
            sha256 TEXT NOT NULL,
            cdn_url TEXT,
            width INTEGER,
            height INTEGER
            {optional}
        );
        CREATE TABLE event_video_link(
            event_id INTEGER NOT NULL,
            video_asset_id INTEGER NOT NULL
            {relevance}
        );
        """
    )
    return con


def _insert_asset(
    con: sqlite3.Connection,
    *,
    asset_id: int,
    byte: str,
    width: int = 1080,
    height: int = 1920,
    cdn_url: str | None = None,
    showcase: float = 80,
) -> str:
    sha = byte * 64
    con.execute(
        """
        INSERT INTO video_asset(
            id, sha256, cdn_url, width, height, cdn_path, mime_type,
            duration_seconds, aesthetic_score, technical_score, showcase_score,
            description, search_text
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            asset_id,
            sha,
            cdn_url or f"https://static.kenigevents.ru/v/{sha}.mp4",
            width,
            height,
            f"v/{sha}.mp4",
            "video/mp4",
            14.25,
            showcase - 2,
            showcase - 1,
            showcase,
            f"Описание {asset_id}",
            f"Поисковый текст {asset_id}",
        ),
    )
    return sha


def test_snapshot_without_video_tables_fails_closed() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    assert EXPORTER.event_video_assets_for_events(con, [7, 8]) == {7: [], 8: []}


def test_partial_snapshot_without_optional_columns_remains_readable() -> None:
    con = _connection(with_optional_columns=False)
    sha = "a" * 64
    con.execute(
        "INSERT INTO video_asset(id,sha256,cdn_url,width,height) VALUES(1,?,?,?,?)",
        (sha, "https://static.kenigevents.ru/v/a.mp4", 720, 1280),
    )
    con.execute("INSERT INTO event_video_link(event_id,video_asset_id) VALUES(7,1)")

    [video] = EXPORTER.event_video_assets_for_events(con, [7])[7]
    assert video["asset_key"] == sha
    assert video["showcase_score"] is None
    assert video["event_relevance_score"] is None
    assert video["ranking_score"] is None


def test_multiple_assets_are_ranked_by_link_score_then_showcase_and_relevance() -> None:
    con = _connection()
    first_sha = _insert_asset(con, asset_id=1, byte="a", showcase=91)
    second_sha = _insert_asset(con, asset_id=2, byte="b", showcase=91)
    third_sha = _insert_asset(con, asset_id=3, byte="c", showcase=75)
    con.executemany(
        "INSERT INTO event_video_link(event_id,video_asset_id,event_relevance_score,ranking_score) VALUES(?,?,?,?)",
        [(7, 1, 20, 73.25), (7, 2, 95, 92), (7, 3, 100, 81.25)],
    )

    videos = EXPORTER.event_video_assets_for_events(con, [7])[7]

    assert [video["asset_key"] for video in videos] == [second_sha, third_sha, first_sha]
    assert videos[0] == {
        "src": f"https://static.kenigevents.ru/v/{second_sha}.mp4",
        "asset_key": second_sha,
        "cdn_path": f"v/{second_sha}.mp4",
        "mime_type": "video/mp4",
        "width": 1080,
        "height": 1920,
        "duration_seconds": 14.25,
        "aesthetic_score": 89.0,
        "technical_score": 90.0,
        "event_relevance_score": 95.0,
        "ranking_score": 92.0,
        "showcase_score": 91.0,
        "description": "Описание 2",
        "search_text": "Поисковый текст 2",
    }


def test_same_content_addressed_asset_projects_to_two_events() -> None:
    con = _connection()
    sha = _insert_asset(con, asset_id=1, byte="d")
    con.executemany(
        "INSERT INTO event_video_link(event_id,video_asset_id,event_relevance_score,ranking_score) VALUES(?,?,?,?)",
        [(7, 1, 96, 84), (8, 1, 84, 81)],
    )

    projected = EXPORTER.event_video_assets_for_events(con, [7, 8])

    assert projected[7][0]["asset_key"] == projected[8][0]["asset_key"] == sha
    assert projected[7][0]["event_relevance_score"] == 96.0
    assert projected[8][0]["event_relevance_score"] == 84.0


def test_empty_url_nonvertical_and_malformed_hash_rows_are_filtered() -> None:
    con = _connection()
    _insert_asset(con, asset_id=1, byte="e", cdn_url=" ")
    _insert_asset(con, asset_id=2, byte="f", width=1920, height=1080)
    _insert_asset(con, asset_id=3, byte="1")
    con.execute("UPDATE video_asset SET sha256='not-a-sha' WHERE id=3")
    valid_sha = _insert_asset(con, asset_id=4, byte="2")
    con.executemany(
        "INSERT INTO event_video_link(event_id,video_asset_id,event_relevance_score,ranking_score) VALUES(7,?,90,82.5)",
        [(1,), (2,), (3,), (4,)],
    )

    videos = EXPORTER.event_video_assets_for_events(con, [7])[7]

    assert [video["asset_key"] for video in videos] == [valid_sha]
