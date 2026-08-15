from __future__ import annotations

import sqlite3
import inspect

import aiosqlite
import pytest

import smart_event_update as seu
import scheduling
from db import Database, _migrate_smart_update_terminal_contract
from smart_update_state import ProductExclusionReason, RetryReason, SmartUpdateTerminalOutcome


def test_background_smart_update_retry_worker_is_default_off() -> None:
    source = inspect.getsource(scheduling.startup)
    assert '_env_enabled("SMART_UPDATE_RETRY_WORKER_ENABLED", default=False)' in source


@pytest.mark.asyncio
async def test_old_sqlite_checks_migrate_idempotently_and_preserve_foreign_keys(tmp_path):
    path = tmp_path / "old-contract.sqlite"
    async with aiosqlite.connect(path) as conn:
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("CREATE TABLE event(id INTEGER PRIMARY KEY)")
        await conn.execute(
            """
            CREATE TABLE smart_update_candidate_state(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_key TEXT NOT NULL UNIQUE,
                occurrence_key TEXT NOT NULL,
                canonical_source_url TEXT,
                source_type TEXT NOT NULL,
                intent TEXT NOT NULL CHECK(intent IN ('UPSERT_EVENT','ATTACH_CONTEXT')),
                source_fingerprint TEXT NOT NULL,
                candidate_payload JSON NOT NULL DEFAULT '{}',
                current_outcome TEXT NOT NULL DEFAULT 'RETRY_SCHEDULED'
                    CHECK(current_outcome IN ('CREATED','MERGED','NOOP_EXACT_REPLAY','REJECTED_PRODUCT_POLICY','RETRY_SCHEDULED')),
                accepted_event_id INTEGER,
                diagnostic_event_id INTEGER,
                reason TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                retry_attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                retry_exhausted INTEGER NOT NULL DEFAULT 0,
                next_retry_at TIMESTAMP,
                claimed_by TEXT,
                claim_expires_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY(accepted_event_id) REFERENCES event(id) ON DELETE SET NULL,
                FOREIGN KEY(diagnostic_event_id) REFERENCES event(id) ON DELETE SET NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE smart_update_attempt(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_state_id INTEGER NOT NULL,
                attempt_no INTEGER NOT NULL,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP,
                terminal_outcome TEXT NOT NULL DEFAULT 'RETRY_SCHEDULED'
                    CHECK(terminal_outcome IN ('CREATED','MERGED','NOOP_EXACT_REPLAY','REJECTED_PRODUCT_POLICY','RETRY_SCHEDULED')),
                accepted_event_id INTEGER,
                diagnostic_event_id INTEGER,
                reason TEXT,
                UNIQUE(candidate_state_id, attempt_no),
                FOREIGN KEY(candidate_state_id) REFERENCES smart_update_candidate_state(id) ON DELETE CASCADE,
                FOREIGN KEY(accepted_event_id) REFERENCES event(id) ON DELETE SET NULL,
                FOREIGN KEY(diagnostic_event_id) REFERENCES event(id) ON DELETE SET NULL
            )
            """
        )
        await conn.execute(
            "CREATE TABLE event_source(id INTEGER PRIMARY KEY,event_id INTEGER NOT NULL,"
            "smart_update_candidate_id INTEGER REFERENCES smart_update_candidate_state(id))"
        )
        await conn.execute(
            "INSERT INTO smart_update_candidate_state(id,candidate_key,occurrence_key,source_type,intent,"
            "source_fingerprint,candidate_payload) VALUES(7,'legacy','ordinal:0','vk','UPSERT_EVENT','fp','{}')"
        )
        await conn.execute(
            "INSERT INTO smart_update_attempt(id,candidate_state_id,attempt_no) VALUES(9,7,1)"
        )
        await conn.execute("INSERT INTO event(id) VALUES(1)")
        await conn.execute("INSERT INTO event_source(id,event_id,smart_update_candidate_id) VALUES(3,1,7)")
        await conn.commit()

        await _migrate_smart_update_terminal_contract(conn)
        await _migrate_smart_update_terminal_contract(conn)
        await conn.execute(
            "UPDATE smart_update_attempt SET terminal_outcome='FAILED_TECHNICAL' WHERE id=9"
        )
        await conn.execute(
            "UPDATE smart_update_candidate_state SET current_outcome='FAILED_TECHNICAL' WHERE id=7"
        )
        await conn.commit()
        assert await (await conn.execute("PRAGMA foreign_key_check")).fetchall() == []
        assert await (await conn.execute(
            "SELECT id,current_outcome FROM smart_update_candidate_state"
        )).fetchall() == [(7, "FAILED_TECHNICAL")]


@pytest.mark.asyncio
async def test_database_init_twice_keeps_linear_terminal_schema(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    db = Database(str(tmp_path / "init-twice.sqlite"))
    await db.init()
    await db.init()
    with sqlite3.connect(db.path) as conn:
        schemas = "\n".join(
            row[0]
            for row in conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' "
                "AND name IN ('smart_update_candidate_state','smart_update_attempt') ORDER BY name"
            )
        )
        assert schemas.count("FAILED_TECHNICAL") == 2
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    await db.close()


@pytest.mark.asyncio
async def test_unchanged_semantic_fingerprint_never_schedules_retry(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    db = Database(str(tmp_path / "linear.sqlite"))
    await db.init()
    candidate = seu.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-32547811_11187",
        source_text="18 августа в Чеховке, Московский проспект, 39",
        title="Презентация экологического маршрута",
        date="2026-08-18",
        time="15:00",
        location_name="Библиотека Чехова",
        location_address="Московский проспект, 39",
        city="Калининград",
    )

    async def semantic_veto(*_args, **_kwargs):
        return seu.SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="location_grounding_review:llm_reject_missing_location",
            retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
        )

    monkeypatch.setattr(seu, "_smart_event_update_impl", semantic_veto)
    first = await seu.smart_event_update(db, candidate, schedule_tasks=False)
    second = await seu.smart_event_update(db, candidate, schedule_tasks=False)
    assert first.outcome is second.outcome is SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY
    assert first.product_exclusion_reason is ProductExclusionReason.MISSING_LOCATION
    with sqlite3.connect(db.path) as conn:
        assert conn.execute(
            "SELECT current_outcome,next_retry_at,claimed_by FROM smart_update_candidate_state"
        ).fetchone() == ("REJECTED_PRODUCT_POLICY", None, None)
        assert conn.execute(
            "SELECT COUNT(*) FROM smart_update_attempt WHERE terminal_outcome='RETRY_SCHEDULED'"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM smart_update_attempt").fetchone()[0] == 2
    await db.close()


@pytest.mark.asyncio
async def test_technical_result_is_visible_terminal_not_background_work(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    db = Database(str(tmp_path / "technical.sqlite"))
    await db.init()
    candidate = seu.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_2",
        source_text="Event at Venue",
        title="Event",
        date="2026-08-20",
        location_name="Venue",
    )

    async def unavailable(*_args, **_kwargs):
        return seu.SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="provider_unavailable",
            retry_reason=RetryReason.SOURCE_VERIFICATION_TECHNICAL_FAILURE,
        )

    monkeypatch.setattr(seu, "_smart_event_update_impl", unavailable)
    result = await seu.smart_event_update(db, candidate, schedule_tasks=False)
    assert result.outcome is SmartUpdateTerminalOutcome.FAILED_TECHNICAL
    with sqlite3.connect(db.path) as conn:
        assert conn.execute(
            "SELECT current_outcome,retry_exhausted,next_retry_at FROM smart_update_candidate_state"
        ).fetchone() == ("FAILED_TECHNICAL", 1, None)
    await db.close()
