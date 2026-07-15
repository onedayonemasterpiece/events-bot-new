from __future__ import annotations

import json
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from db import Database
from kaggle_status import (
    create_kaggle_run_config,
    create_kaggle_status_dataset,
    format_kaggle_status_label,
    make_kaggle_run_event_handler,
    record_kaggle_run_event,
)


def _load_status_client_class():
    module_path = Path(__file__).resolve().parents[1] / "kaggle" / "kaggle_status_client.py"
    spec = importlib.util.spec_from_file_location("events_bot_kaggle_status_client", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.KaggleStatusClient


def test_create_kaggle_status_dataset_uses_kaggle_valid_title():
    titles: list[str] = []

    class FakeClient:
        def create_dataset(self, folder):  # noqa: ANN001
            metadata = json.loads((Path(folder) / "dataset-metadata.json").read_text(encoding="utf-8"))
            titles.append(metadata["title"])

        def dataset_status(self, dataset):  # noqa: ANN001
            return "ready"

        def dataset_list_files(self, dataset, page_size=50):  # noqa: ANN001,ARG002
            return [{"name": "kaggle_run.json"}, {"name": "kaggle_status_client.py"}]

    dataset = create_kaggle_status_dataset(
        FakeClient(),
        username="zigomaro",
        slug_prefix="status-ParseTheatres",
        run_id="status-smoke-20260613223955",
        config={"run_id": "status-smoke-20260613223955"},
    )

    assert dataset.startswith("zigomaro/status-parsetheatres-status-sm-")
    assert len(dataset.rsplit("-", 1)[-1]) == 8
    assert titles
    assert 6 <= len(titles[0]) <= 50


def test_create_kaggle_status_dataset_keeps_long_run_ids_unique():
    datasets: list[str] = []

    class FakeClient:
        def create_dataset(self, folder):  # noqa: ANN001
            metadata = json.loads((Path(folder) / "dataset-metadata.json").read_text(encoding="utf-8"))
            datasets.append(metadata["id"])

        def dataset_status(self, dataset):  # noqa: ANN001,ARG002
            return "ready"

        def dataset_list_files(self, dataset, page_size=50):  # noqa: ANN001,ARG002
            return [{"name": "kaggle_run.json"}, {"name": "kaggle_status_client.py"}]

    for run_id in (
        "event-age-bge-20260715T181855Z",
        "event-age-bge-20260715T183407Z",
    ):
        create_kaggle_status_dataset(
            FakeClient(),
            username="zigomaro",
            slug_prefix="status-EventAgeBgeAssessment",
            run_id=run_id,
            config={"run_id": run_id},
        )

    assert len(datasets) == 2
    assert datasets[0] != datasets[1]
    assert all(len(dataset) <= 80 for dataset in datasets)


def test_create_kaggle_status_dataset_does_not_version_after_validation_error():
    class FakeClient:
        def create_dataset(self, folder):  # noqa: ANN001,ARG002
            raise ValueError("The dataset title must be between 6 and 50 characters")

        def create_dataset_version(self, *args, **kwargs):  # noqa: ANN002,ANN003
            raise AssertionError("validation errors must not fall back to dataset version")

    with pytest.raises(ValueError, match="dataset title"):
        create_kaggle_status_dataset(
            FakeClient(),
            username="zigomaro",
            slug_prefix="status-ParseTheatres",
            run_id="status-smoke-20260613223955",
            config={"run_id": "status-smoke-20260613223955"},
        )


def test_create_kaggle_status_dataset_versions_existing_dataset():
    calls: list[str] = []

    class FakeClient:
        def create_dataset(self, folder):  # noqa: ANN001,ARG002
            calls.append("create")
            raise RuntimeError("Dataset already exists")

        def create_dataset_version(self, folder, **kwargs):  # noqa: ANN001,ANN003
            calls.append("version")

        def dataset_status(self, dataset):  # noqa: ANN001
            return "ready"

        def dataset_list_files(self, dataset, page_size=50):  # noqa: ANN001,ARG002
            return [{"name": "kaggle_run.json"}, {"name": "kaggle_status_client.py"}]

    create_kaggle_status_dataset(
        FakeClient(),
        username="zigomaro",
        slug_prefix="status-ParseTheatres",
        run_id="status-smoke-20260613223955",
        config={"run_id": "status-smoke-20260613223955"},
    )

    assert calls == ["create", "version"]


def test_kaggle_status_label_includes_percent_and_business_progress():
    assert (
        format_kaggle_status_label(
            {
                "status": "RUNNING",
                "progress": {
                    "sources_done": 5,
                    "sources_total": 17,
                    "progress_label": "каналы 5/17",
                },
            }
        )
        == "RUNNING 29% · каналы 5/17"
    )


@pytest.mark.asyncio
async def test_kaggle_run_config_and_alive_callback(tmp_path, monkeypatch):
    monkeypatch.setenv("KAGGLE_STATUS_CALLBACK_URL", "https://example.test/internal/kaggle/run-event")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    config = await create_kaggle_run_config(
        db,
        run_id="videoannounce:664",
        session_id=664,
        kind="cherryflash",
        notebook="CherryFlash",
        kernel_ref="zigomaro/cherryflash",
        dataset_ref="zigomaro/cherryflash-session-664",
        resource_leases=["telegram_session:s22"],
    )

    assert config is not None
    assert config["callback_url"] == "https://example.test/internal/kaggle/run-event"
    assert config["token"]
    assert config["resource_leases"] == ["telegram_session:s22"]

    status, body = await record_kaggle_run_event(
        db,
        {
            "run_id": config["run_id"],
            "token": config["token"],
            "event": "alive",
            "event_uid": "alive:1",
            "phase": "render",
            "status": "alive",
            "progress": {"rendered_frames": 120, "total_frames": 300},
        },
    )

    assert status == 200
    assert body["ok"] is True
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, phase, progress_json, last_heartbeat_at FROM kaggle_run_ledger WHERE run_id=?",
            ("videoannounce:664",),
        )
        row = await cur.fetchone()
        await cur.close()
    assert row[0] == "alive"
    assert row[1] == "render"
    assert json.loads(row[2]) == {
        "rendered_frames": 120,
        "total_frames": 300,
        "progress_percent": 60,
    }
    assert row[3]

    status, duplicate_body = await record_kaggle_run_event(
        db,
        {
            "run_id": config["run_id"],
            "token": config["token"],
            "event": "alive",
            "event_uid": "alive:1",
            "phase": "render",
            "status": "alive",
            "progress": {"rendered_frames": 120, "total_frames": 300},
        },
    )
    assert status == 200
    assert duplicate_body["duplicate"] is True
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM kaggle_run_event WHERE run_id=?",
            ("videoannounce:664",),
        )
        count_row = await cur.fetchone()
        await cur.close()
    assert count_row[0] == 1


@pytest.mark.asyncio
async def test_kaggle_run_event_http_handler_records_event(tmp_path, monkeypatch):
    monkeypatch.setenv("KAGGLE_STATUS_CALLBACK_URL", "https://example.test/internal/kaggle/run-event")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    config = await create_kaggle_run_config(
        db,
        run_id="parser:ParseQtickets:test",
        session_id=None,
        kind="parser_kernel",
        notebook="ParseQtickets",
    )
    assert config

    app = web.Application()
    app.router.add_post("/internal/kaggle/run-event", make_kaggle_run_event_handler(db))
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            "/internal/kaggle/run-event",
            json={
                "run_id": config["run_id"],
                "token": config["token"],
                "event": "kernel_started",
                "event_uid": "kernel_started:1",
                "phase": "preflight",
                "status": "running",
                "progress": {"cell_index": 0},
            },
        )
        assert response.status == 200
        body = await response.json()
        assert body["ok"] is True
    finally:
        await client.close()

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT event_name, phase FROM kaggle_run_event WHERE run_id=?",
            (config["run_id"],),
        )
        row = await cur.fetchone()
        await cur.close()
    assert row == ("kernel_started", "preflight")


@pytest.mark.asyncio
async def test_publish_target_events_are_recorded_as_status_history(tmp_path, monkeypatch):
    monkeypatch.setenv("KAGGLE_STATUS_CALLBACK_URL", "https://example.test/internal/kaggle/run-event")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    config = await create_kaggle_run_config(
        db,
        run_id="videoannounce:662",
        session_id=662,
        kind="cherryflash",
        notebook="CherryFlash",
    )
    assert config

    for event, status_value in (
        ("publish_target_started", "running"),
        ("publish_target_done", "done"),
    ):
        status, body = await record_kaggle_run_event(
            db,
            {
                "run_id": config["run_id"],
                "token": config["token"],
                "event": event,
                "event_uid": event,
                "status": status_value,
                "phase": "publish",
                "progress": {"target_label": "kenigevents", "external_url": "https://t.me/kenigevents/3997"},
            },
        )
        assert status == 200
        assert body["ok"] is True

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT event_name, status, progress_json FROM kaggle_run_event WHERE run_id=? ORDER BY seq",
            (config["run_id"],),
        )
        rows = await cur.fetchall()
        await cur.close()
    assert [row[0] for row in rows] == ["publish_target_started", "publish_target_done"]
    assert [row[1] for row in rows] == ["running", "done"]
    assert json.loads(rows[1][2])["external_url"] == "https://t.me/kenigevents/3997"


@pytest.mark.asyncio
async def test_resource_lease_blocks_parallel_holder(tmp_path, monkeypatch):
    monkeypatch.setenv("KAGGLE_STATUS_CALLBACK_URL", "https://example.test/internal/kaggle/run-event")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    first = await create_kaggle_run_config(
        db,
        run_id="guide-monitor:1",
        session_id=None,
        kind="guide_excursions_monitor",
        notebook="GuideExcursionsMonitor",
    )
    second = await create_kaggle_run_config(
        db,
        run_id="telegram-monitor:2",
        session_id=None,
        kind="telegram_monitor",
        notebook="TelegramMonitor",
    )
    assert first and second

    status, body = await record_kaggle_run_event(
        db,
        {
            "run_id": first["run_id"],
            "token": first["token"],
            "event": "resource_acquire",
            "resource": {"key": "telegram_session:s22", "action": "acquire", "ttl_seconds": 600},
        },
    )
    assert status == 200
    assert body["resource_action"] == "acquired"

    status, body = await record_kaggle_run_event(
        db,
        {
            "run_id": second["run_id"],
            "token": second["token"],
            "event": "resource_acquire",
            "resource": {"key": "telegram_session:s22", "action": "acquire", "ttl_seconds": 600},
        },
    )
    assert status == 200
    assert body["resource_action"] == "blocked"
    assert body["holder_run_id"] == first["run_id"]


@pytest.mark.asyncio
async def test_alive_renews_resource_lease_and_coalesces_history(tmp_path, monkeypatch):
    monkeypatch.setenv("KAGGLE_STATUS_CALLBACK_URL", "https://example.test/internal/kaggle/run-event")
    monkeypatch.setenv("KAGGLE_STATUS_ALIVE_EVENT_MIN_INTERVAL_SECONDS", "300")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    config = await create_kaggle_run_config(
        db,
        run_id="videoannounce:700",
        session_id=700,
        kind="cherryflash",
        notebook="CherryFlash",
    )
    assert config
    now = datetime.now(timezone.utc)
    old_expires = (now + timedelta(minutes=10)).isoformat()
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO kaggle_resource_lease(
                resource_key, run_id, holder_kind, status, acquired_at, expires_at, updated_at
            )
            VALUES (?, ?, 'kaggle', 'active', ?, ?, ?)
            """,
            (
                "telegram_session:env:TELEGRAM_AUTH_BUNDLE_STORY",
                config["run_id"],
                now.isoformat(),
                old_expires,
                now.isoformat(),
            ),
        )
        await conn.commit()

    first_status, first_body = await record_kaggle_run_event(
        db,
        {
            "run_id": config["run_id"],
            "token": config["token"],
            "event": "alive",
            "event_uid": "alive:1",
            "phase": "render",
            "status": "alive",
            "progress": {"progress_label": "кадры 10/100"},
        },
    )
    second_status, second_body = await record_kaggle_run_event(
        db,
        {
            "run_id": config["run_id"],
            "token": config["token"],
            "event": "alive",
            "event_uid": "alive:2",
            "phase": "render",
            "status": "alive",
            "progress": {"progress_label": "кадры 11/100"},
        },
    )

    assert first_status == 200
    assert second_status == 200
    assert first_body["resource_action"] == "renewed"
    assert second_body.get("coalesced") is True
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM kaggle_run_event WHERE run_id=? AND event_name='alive'",
            (config["run_id"],),
        )
        event_count = (await cur.fetchone())[0]
        await cur.close()
        cur = await conn.execute(
            "SELECT expires_at FROM kaggle_resource_lease WHERE resource_key=?",
            ("telegram_session:env:TELEGRAM_AUTH_BUNDLE_STORY",),
        )
        renewed_expires = (await cur.fetchone())[0]
        await cur.close()
        cur = await conn.execute(
            "SELECT last_heartbeat_at, progress_json FROM kaggle_run_ledger WHERE run_id=?",
            (config["run_id"],),
        )
        ledger = await cur.fetchone()
        await cur.close()
    assert event_count == 1
    assert renewed_expires > old_expires
    assert ledger[0]
    assert json.loads(ledger[1])["progress_label"] == "кадры 11/100"


@pytest.mark.asyncio
async def test_expired_resource_lease_is_marked_on_next_run_config(tmp_path, monkeypatch):
    monkeypatch.setenv("KAGGLE_STATUS_CALLBACK_URL", "https://example.test/internal/kaggle/run-event")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO kaggle_resource_lease(
                resource_key, run_id, holder_kind, status, acquired_at, expires_at, updated_at
            )
            VALUES (
                'telegram_session:s22',
                'videoannounce:669',
                'kaggle',
                'active',
                '2026-06-13T17:05:01+00:00',
                '2026-06-13T20:05:01+00:00',
                '2026-06-13T17:05:01+00:00'
            )
            """,
        )
        await conn.commit()

    config = await create_kaggle_run_config(
        db,
        run_id="guide-monitor:next",
        session_id=None,
        kind="guide_excursions_monitor",
        notebook="GuideExcursionsMonitor",
    )
    assert config

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, released_at FROM kaggle_resource_lease WHERE resource_key=?",
            ("telegram_session:s22",),
        )
        row = await cur.fetchone()
        await cur.close()
    assert row[0] == "expired"
    assert row[1]


def test_kaggle_status_client_redacts_token_in_local_jsonl(tmp_path):
    KaggleStatusClient = _load_status_client_class()
    client = KaggleStatusClient(
        {
            "run_id": "parser:test",
            "session_id": None,
            "kind": "parser_kernel",
            "notebook": "ParseQtickets",
            "callback_url": "http://127.0.0.1:9/unreachable",
            "token": "secret-token",
        },
        output_dir=tmp_path,
        log=lambda _message: None,
    )

    client.event("alive", progress={"phase": "parse"}, timeout=0.01)

    rows = [
        json.loads(line)
        for line in (tmp_path / "kaggle_status_events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert rows[0]["payload"]["token"] == "<redacted>"
    assert "secret-token" not in json.dumps(rows, ensure_ascii=False)
