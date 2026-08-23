from __future__ import annotations

import json
from datetime import date

import pytest

import scheduling
from db import Database
from exhibition_duplicate_audit import run_exhibition_duplicate_audit_scheduler


class _Bot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str, **_kwargs) -> None:
        self.messages.append((chat_id, text))


async def _insert_user(db: Database, user_id: int = 1001) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            'INSERT INTO "user"(user_id, username, is_superadmin, blocked) VALUES(?, ?, 1, 0)',
            (user_id, "admin"),
        )
        await conn.commit()


async def _insert_event(
    db: Database,
    event_id: int,
    title: str,
    start: str,
    end: str | None,
    venue: str,
    *,
    identity_status: str = "canonical",
    merged_into_event_id: int | None = None,
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO event(
                id, title, description, date, time, end_date, location_name, city, event_type,
                lifecycle_status, identity_status, merged_into_event_id, source_text
            )
            VALUES(?, ?, '', ?, '', ?, ?, 'Калининград', 'выставка', 'active', ?, ?, '')
            """,
            (event_id, title, start, end, venue, identity_status, merged_into_event_id),
        )
        await conn.commit()


async def _insert_identity_decision(
    db: Database,
    *,
    decision: str = "veto_create",
    reason: str = "vector_identity_match",
    created_at: str = "2026-07-02 10:00:00",
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO event_identity_decision_log(
                event_id, source_type, source_url, decision, decision_reason,
                confidence, decided_by, decision_payload, created_at
            )
            VALUES(1, 'telegram', 'https://t.me/example/identity', ?, ?, 0.95,
                   'smart_update.identity_gate', ?, ?)
            """,
            (decision, reason, json.dumps({"mode": "enforce", "vector": {"available": True}}), created_at),
        )
        await conn.commit()


async def _insert_pair_review(
    db: Database,
    left_id: int,
    right_id: int,
    *,
    decision: str,
    relation: str,
    evidence: list[str],
    conflicts: list[str],
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO event_identity_decision_log(
                event_id, candidate_event_id, source_type, source_url, decision,
                decision_reason, confidence, decided_by, decision_payload
            ) VALUES(?, ?, 'manual', 'artifact://terra-review', ?, 'reviewed_pair',
                     0.99, 'terra.manual-review', ?)
            """,
            (left_id, right_id, decision, json.dumps({
                "stage": "manual_pair_review_v1", "action": decision,
                "relation": relation, "evidence": evidence,
                "blocking_conflicts": conflicts,
            }, ensure_ascii=False)),
        )
        await conn.commit()


@pytest.mark.asyncio
async def test_exhibition_duplicate_audit_scheduler_records_success(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    try:
        await db.init()
        await _insert_event(db, 1, "Розовый натюрморт", "2026-07-01", "2026-08-01", "Музей")
        await _insert_event(db, 2, "Совсем другая выставка", "2026-07-02", "2026-08-02", "Другая галерея")
        await _insert_identity_decision(db)

        payload = await run_exhibition_duplicate_audit_scheduler(
            db,
            None,
            run_id="audit-ok",
            current_date=date(2026, 7, 2),
        )

        assert payload["high_confidence_duplicate_count"] == 0
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                "SELECT kind, trigger, status, metrics_json, details_json FROM ops_run ORDER BY id DESC LIMIT 1"
            )
            row = await cur.fetchone()
        assert row is not None
        kind, trigger, status, metrics_raw, details_raw = row
        assert kind == "exhibition_duplicate_audit"
        assert trigger == "scheduled"
        assert status == "success"
        metrics = json.loads(metrics_raw)
        assert metrics["high_confidence_duplicate_count"] == 0
        assert metrics["identity_gate_decision_count"] == 1
        assert metrics["identity_gate_veto_create_count"] == 1
        assert "identity_gate_env_ready" in metrics
        details = json.loads(details_raw)
        assert details["scheduler_run_id"] == "audit-ok"
        assert details["identity_gate"]["decision_count"] == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_exhibition_duplicate_audit_scheduler_records_failure_and_alerts(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    try:
        await db.init()
        await _insert_user(db, 4242)
        await _insert_event(db, 1, "Розовый натюрморт", "2026-07-01", "2026-08-01", "Музей")
        await _insert_event(db, 2, "Розовый натюрморт", "2026-07-02", "2026-08-02", "Музей")
        await _insert_event(
            db,
            3,
            "Розовый натюрморт",
            "2026-07-02",
            "2026-08-02",
            "Музей",
            identity_status="merged",
            merged_into_event_id=1,
        )
        await _insert_pair_review(
            db, 1, 2, decision="CONFIRMED_DUPLICATE", relation="same_event",
            evidence=["same title, range, venue and source-defined exhibition"],
            conflicts=[],
        )
        bot = _Bot()

        payload = await run_exhibition_duplicate_audit_scheduler(
            db,
            bot,
            run_id="audit-dup",
            current_date=date(2026, 7, 2),
            raise_on_duplicates=False,
        )

        assert payload["high_confidence_duplicate_count"] == 1
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                "SELECT status, metrics_json, details_json FROM ops_run WHERE kind='exhibition_duplicate_audit' ORDER BY id DESC LIMIT 1"
            )
            row = await cur.fetchone()
        assert row is not None
        status, metrics_raw, details_raw = row
        assert status == "failed"
        assert json.loads(metrics_raw)["high_confidence_duplicate_count"] == 1
        details = json.loads(details_raw)
        assert details["duplicates"][0]["left_id"] == 1
        assert details["duplicates"][0]["right_id"] == 2
        assert bot.messages and bot.messages[0][0] == 4242
        assert payload["confirmed_duplicate_count"] == 1
        assert payload["unresolved_count"] == 0
        assert "confirmed=1" in bot.messages[0][1]
        assert "unresolved=0" in bot.messages[0][1]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_exhibition_duplicate_audit_scheduler_records_failed_before_raise(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    try:
        await db.init()
        await _insert_event(db, 1, "Розовый натюрморт", "2026-07-01", "2026-08-01", "Музей")
        await _insert_event(db, 2, "Розовый натюрморт", "2026-07-02", "2026-08-02", "Музей")

        with pytest.raises(RuntimeError, match="public exhibition duplicates require action"):
            await run_exhibition_duplicate_audit_scheduler(
                db,
                None,
                run_id="audit-raise",
                current_date=date(2026, 7, 2),
            )

        async with db.raw_conn() as conn:
            cur = await conn.execute(
                "SELECT status, metrics_json FROM ops_run WHERE kind='exhibition_duplicate_audit' ORDER BY id DESC LIMIT 1"
            )
            row = await cur.fetchone()
        assert row is not None
        status, metrics_raw = row
        assert status == "failed"
        assert json.loads(metrics_raw)["high_confidence_duplicate_count"] == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_exhibition_duplicate_audit_succeeds_for_grounded_keep_distinct(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    try:
        await db.init()
        await _insert_event(db, 1, "Выставка Точка и линия", "2026-07-01", "2026-09-01", "Музей")
        await _insert_event(db, 2, "Экскурсия по выставке Точка и линия", "2026-07-02", "2026-09-01", "Музей")
        await _insert_pair_review(
            db, 1, 2, decision="FINAL_DISTINCT", relation="distinct_event",
            evidence=["source identifies a separately bookable excursion"],
            conflicts=["event_type: exhibition vs excursion"],
        )

        payload = await run_exhibition_duplicate_audit_scheduler(
            db, None, run_id="audit-distinct", current_date=date(2026, 7, 2)
        )
        assert payload["candidate_pair_count"] == 1
        assert payload["keep_distinct_count"] == 1
        assert payload["confirmed_duplicate_count"] == 0
        assert payload["unresolved_count"] == 0
        async with db.raw_conn() as conn:
            row = await (await conn.execute(
                "SELECT status, metrics_json, details_json FROM ops_run ORDER BY id DESC LIMIT 1"
            )).fetchone()
        assert row is not None and row[0] == "success"
        metrics = json.loads(row[1]); details = json.loads(row[2])
        assert metrics["candidate_pair_count"] == 1
        assert metrics["keep_distinct_count"] == 1
        assert details["keep_distinct_pairs"][0]["disposition"] == "KEEP_DISTINCT"
    finally:
        await db.close()


def test_startup_registers_exhibition_duplicate_audit(monkeypatch):
    class DummyExecutor:
        pass

    class DummyJob:
        def __init__(self, job_id: str) -> None:
            self.id = job_id
            self.next_run_time = None

    class DummyScheduler:
        def __init__(self, executors=None, timezone=None):
            self.jobs: dict[str, DummyJob] = {}

        def configure(self, job_defaults=None):
            self.job_defaults = job_defaults

        def add_job(self, func, trigger, id, args=None, **kwargs):
            self.jobs[id] = DummyJob(id)
            self.jobs[id].trigger = trigger
            self.jobs[id].kwargs = kwargs
            return self.jobs[id]

        def get_job(self, job_id):
            return self.jobs.get(job_id)

        def add_listener(self, listener, mask):
            self.listener = (listener, mask)

        def start(self):
            self.started = True

        def shutdown(self, wait=False):
            self.started = False

    monkeypatch.setenv("ENABLE_EXHIBITION_DUPLICATE_AUDIT", "1")
    monkeypatch.setenv("EXHIBITION_DUPLICATE_AUDIT_TIME_LOCAL", "07:45")
    monkeypatch.setenv("EXHIBITION_DUPLICATE_AUDIT_TZ", "Europe/Kaliningrad")
    monkeypatch.setattr(scheduling, "AsyncIOExecutor", lambda: DummyExecutor())
    monkeypatch.setattr(scheduling, "AsyncIOScheduler", DummyScheduler)
    monkeypatch.setattr(scheduling, "_scheduler", None)

    try:
        scheduler = scheduling.startup(
            db=None,
            bot=None,
            vk_scheduler=lambda *a, **k: None,
            vk_poll_scheduler=lambda *a, **k: None,
            vk_crawl_cron=lambda *a, **k: None,
            cleanup_scheduler=lambda *a, **k: None,
            partner_notification_scheduler=lambda *a, **k: None,
            nightly_page_sync=lambda *a, **k: None,
            rebuild_fest_nav_if_changed=lambda *a, **k: None,
        )
        assert "exhibition_duplicate_audit" in scheduler.jobs
        job = scheduler.jobs["exhibition_duplicate_audit"]
        assert job.trigger == "cron"
        assert job.kwargs["misfire_grace_time"] == 1800
    finally:
        scheduling.cleanup()
