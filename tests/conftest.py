import hashlib
import os
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(__file__))
from _helpers.no_network import no_network  # noqa: F401

import main
import poster_ocr
from models import PosterOcrCache
from private_events_mcp.config import PrivateEventsMCPConfig


@pytest.fixture(autouse=True)
def _reset_run_due_jobs_lock():
    main._reset_run_due_jobs_locks()


@pytest.fixture(autouse=True)
def _skip_default_vk_source_seed(monkeypatch):
    # Most unit tests build tiny vk_source fixtures and assert exact crawl/list
    # counts. Production keeps this seed enabled by default; tests opt out unless
    # a specific case explicitly clears the env var.
    monkeypatch.setenv("DB_INIT_SKIP_VK_SOURCES_SEED", "1")


@pytest.fixture(autouse=True)
def _mock_telegraph(monkeypatch, request):
    if "get_telegraph_token" not in request.node.nodeid:
        monkeypatch.setattr(main, "get_telegraph_token", lambda: "t")

    async def fake_create_page(tg, *args, **kwargs):
        return {"path": "test", "url": "https://t.me/test"}

    # Skip for new test that needs real implementation to verify calls
    if "test_split_month_requires_many_pages" not in request.node.nodeid:
        monkeypatch.setattr(main, "telegraph_create_page", fake_create_page)

    async def fake_update(event_id, db_obj, bot_obj):
        async with db_obj.get_session() as session:
            ev = await session.get(main.Event, event_id)
        if not ev:
            return None
        res = await main.create_source_page(
            ev.title or "Event",
            ev.source_text,
            ev.source_post_url,
            db=db_obj,
        )
        if res:
            url, path, *_ = res
            async with db_obj.get_session() as session:
                obj = await session.get(main.Event, event_id)
                if obj:
                    obj.telegraph_url = url
                    obj.telegraph_path = path
                    session.add(obj)
                    await session.commit()
            return url
        return None

    monkeypatch.setattr(main, "update_telegraph_event_page", fake_update)
    monkeypatch.setattr(main, "update_source_post_keyboard", lambda *a, **k: None)


@pytest.fixture(autouse=True)
def _mock_poster_ocr(monkeypatch, request):
    if "test_poster_ocr" in str(getattr(request.node, "fspath", "")):
        return

    async def fake_recognize(
        db,
        items,
        detail="auto",
        *,
        count_usage=True,
        log_context=None,
        **kwargs,
    ):
        results = []
        for item in items:
            if isinstance(item, tuple) and item:
                data = item[0]
            elif isinstance(item, (bytes, bytearray, memoryview)):
                data = bytes(item)
            else:
                data = getattr(item, "data", b"")
            digest = hashlib.sha256(bytes(data)).hexdigest()
            results.append(
                PosterOcrCache(
                    hash=digest,
                    detail=detail,
                    model="mock",
                    text="",
                    prompt_tokens=0,
                    completion_tokens=0,
                    total_tokens=0,
                )
            )
        return results, 0, poster_ocr.DAILY_TOKEN_LIMIT

    monkeypatch.setattr(poster_ocr, "recognize_posters", fake_recognize)


@pytest.fixture
def repo_root(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    incidents = root / "docs" / "reports" / "incidents"
    incidents.mkdir(parents=True)
    (incidents / "INC-2026-08-01-test.md").write_text(
        """# INC-2026-08-01 Test publication incident\n\n"
        "Status: resolved\nSeverity: high\nDate: 2026-08-01\n\n"
        "A publication job failed after a source identity conflict. "
        "The operator reviewed the evidence and retried it safely.\n""",
        encoding="utf-8",
    )
    (root / ".static-site-repo-sha").write_text("a" * 40 + "\n", encoding="utf-8")
    return root


@pytest.fixture
def event_db(tmp_path: Path) -> Path:
    path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE event (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            short_description TEXT,
            festival TEXT,
            date TEXT NOT NULL,
            end_date TEXT,
            time TEXT,
            location_name TEXT,
            location_address TEXT,
            city TEXT,
            ticket_link TEXT,
            event_type TEXT,
            is_free INTEGER,
            pushkin_card INTEGER,
            lifecycle_status TEXT,
            source_text TEXT,
            search_digest TEXT,
            topics TEXT,
            telegraph_url TEXT,
            source_post_url TEXT,
            photo_count INTEGER,
            added_at TEXT,
            content_hash TEXT
        );
        CREATE TABLE event_source (
            id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            source_type TEXT,
            source_url TEXT,
            source_chat_username TEXT,
            source_text TEXT,
            imported_at TEXT,
            trust_level TEXT
        );
        CREATE TABLE event_source_fact (
            id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            source_id INTEGER,
            fact_type TEXT,
            fact_key TEXT,
            fact_value TEXT,
            confidence REAL,
            status TEXT,
            provenance_json TEXT,
            updated_at TEXT
        );
        CREATE TABLE eventposter (
            id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            catbox_url TEXT,
            poster_hash TEXT,
            ocr_title TEXT,
            ocr_text TEXT,
            updated_at TEXT
        );
        CREATE TABLE joboutbox (
            id INTEGER PRIMARY KEY,
            event_id INTEGER,
            task TEXT,
            status TEXT,
            attempts INTEGER,
            last_error TEXT,
            last_result TEXT,
            updated_at TEXT,
            next_run_at TEXT,
            payload TEXT
        );
        CREATE TABLE ops_run (
            run_id TEXT PRIMARY KEY,
            operation TEXT,
            status TEXT,
            event_id INTEGER,
            started_at TEXT,
            finished_at TEXT,
            error_json TEXT,
            result_json TEXT,
            details_json TEXT,
            correlation_id TEXT
        );
        CREATE TABLE smart_update_review (
            id INTEGER PRIMARY KEY,
            event_id INTEGER,
            status TEXT,
            review_reason TEXT,
            evidence_json TEXT,
            decision_json TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE telegram_scanned_message (
            id INTEGER PRIMARY KEY,
            status TEXT
        );
        CREATE TABLE telegram_monitoring_on_demand_queue (
            id INTEGER PRIMARY KEY,
            status TEXT
        );
        CREATE TABLE vk_inbox (
            id INTEGER PRIMARY KEY,
            status TEXT
        );
        """
    )
    conn.execute(
        """INSERT INTO event VALUES (
            42, 'Лекция об архитектуре Кёнигсберга',
            'Разговор о городской архитектуре и источниках.',
            'История городской архитектуры.', 'Городской лекторий',
            '2026-08-11', NULL, '19:00', 'Библиотека', 'ул. Мира, 1',
            'Калининград', 'https://tickets.example/42', 'лекция', 0, 0,
            'published', 'Исходный публичный анонс лекции.',
            'архитектура кёнигсберг лекция', '["LECTURES","URBANISM"]',
            'https://telegra.ph/event-42', 'https://t.me/source/42', 1,
            '2026-08-01T10:00:00Z', 'eventhash42'
        )"""
    )
    conn.execute(
        """INSERT INTO event_source VALUES (
            1, 42, 'telegram', 'https://t.me/source/42', 'source',
            'Публичный пост организатора.', '2026-08-01T10:01:00Z', 'primary'
        )"""
    )
    conn.execute(
        """INSERT INTO event_source_fact VALUES (
            1, 42, 1, 'schedule', 'time', '19:00', 0.99, 'accepted',
            '{"source":"telegram"}', '2026-08-01T10:02:00Z'
        )"""
    )
    conn.execute(
        """INSERT INTO eventposter VALUES (
            1, 42, 'https://files.example/poster.jpg', 'posterhash',
            'Лекция', '11 августа, 19:00', '2026-08-01T10:03:00Z'
        )"""
    )
    conn.execute(
        """INSERT INTO joboutbox VALUES (
            7, 42, 'telegraph_build', 'error', 2, 'temporary timeout', NULL,
            '2026-08-01T10:04:00Z', '2026-08-01T10:05:00Z', '{"event_id":42}'
        )"""
    )
    conn.execute(
        """INSERT INTO ops_run VALUES (
            'run-test-1', 'smart_update', 'failed', 42,
            '2026-08-01T10:00:00Z', '2026-08-01T10:05:00Z',
            '{"message":"identity conflict"}', NULL,
            '{"review_required":true}', 'corr-test-1'
        )"""
    )
    conn.execute(
        """INSERT INTO smart_update_review VALUES (
            2, 42, 'resolved', 'identity conflict',
            '{"source_ids":[1]}', '{"action":"retry"}',
            '2026-08-01T10:06:00Z', '2026-08-01T10:07:00Z'
        )"""
    )
    conn.execute("INSERT INTO telegram_scanned_message VALUES (1, 'imported')")
    conn.execute("INSERT INTO telegram_monitoring_on_demand_queue VALUES (1, 'done')")
    conn.execute("INSERT INTO vk_inbox VALUES (1, 'pending')")
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def config(tmp_path: Path, repo_root: Path, event_db: Path) -> PrivateEventsMCPConfig:
    return PrivateEventsMCPConfig(
        enabled=True,
        public_base_url="https://events-bot-new.fly.dev",
        path_secret="mcp_" + "p" * 44,
        database_path=str(event_db),
        auth_database_path=str(tmp_path / "oauth.sqlite"),
        oauth_client_id="chatgpt-events-test-client",
        oauth_client_secret="client_" + "s" * 48,
        codex_oauth_client_id="codex-events-test-client",
        opencode_oauth_client_id="opencode-events-test-client",
        operator_token="operator_" + "o" * 48,
        signing_key="signing_" + "k" * 64,
        repository_root=str(repo_root),
        repository_slug="onedayonemasterpiece/events-bot-new",
        repository_sha_file=str(repo_root / ".static-site-repo-sha"),
        query_timeout_ms=1000,
        max_rows=25,
        max_concurrency=2,
        cache_ttl_seconds=0,
    )


@pytest.fixture
def event_db_digest(event_db: Path) -> str:
    return hashlib.sha256(event_db.read_bytes()).hexdigest()
