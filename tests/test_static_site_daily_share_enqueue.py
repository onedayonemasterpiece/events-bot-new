from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from db import Database
from models import JobOutbox, JobStatus, JobTask


@pytest.mark.asyncio
async def test_daily_share_enqueue_is_durable_per_kaliningrad_day(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import main

    db = Database(str(tmp_path / "daily-share.sqlite"))
    await db.init()
    monkeypatch.setenv("ENABLE_STATIC_SITE_KAGGLE_BUILDER", "1")
    monkeypatch.setenv(
        "STATIC_SITE_CURRENT_DATETIME",
        "2026-07-27T00:05:00+02:00",
    )

    first = await main.enqueue_static_site_build_request(
        db,
        reason="Europe/Kaliningrad local date rollover",
        trigger="startup_catchup",
    )
    assert first == "new"
    async with db.get_session() as session:
        row = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build)
            )
        ).scalar_one()
        first_payload = dict(row.payload)
        first_fingerprint = first_payload["daily_share_refresh"]["force_fingerprint"]
        assert first_payload["daily_share_refresh"] == {
            "schema_version": "static_site_daily_share_refresh_v1",
            "local_date": "2026-07-27",
            "time_zone": "Europe/Kaliningrad",
            "force_fingerprint": (
                "service-share-daily:Europe/Kaliningrad:2026-07-27"
            ),
        }
        assert first_payload["force_rebuild"] is False
        assert first_payload["correlation_ids"] == [
            "static-site-daily-share-2026-07-27"
        ]
        # Simulate a terminal build: the payload remains the durable daily
        # marker even after the active queue entry has completed.
        row.status = JobStatus.done
        row.updated_at = datetime.now(timezone.utc)
        session.add(row)
        await session.commit()

    repeated = await main.enqueue_static_site_build_request(
        db,
        reason="Europe/Kaliningrad local date rollover",
        trigger="calendar_rollover",
    )
    assert repeated == "daily-already-requested"
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build)
            )
        ).scalars().all()
        assert len(rows) == 1
        assert rows[0].status == JobStatus.done
        assert rows[0].payload == first_payload

    monkeypatch.setenv(
        "STATIC_SITE_CURRENT_DATETIME",
        "2026-07-28T00:01:00+02:00",
    )
    next_day = await main.enqueue_static_site_build_request(
        db,
        reason="Europe/Kaliningrad local date rollover",
        trigger="calendar_rollover",
    )
    assert next_day == "requeued"
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build)
            )
        ).scalars().all()
        assert len(rows) == 1
        assert rows[0].status == JobStatus.pending
        marker = rows[0].payload["daily_share_refresh"]
        assert marker["local_date"] == "2026-07-28"
        assert marker["force_fingerprint"] != first_fingerprint
        assert rows[0].payload["target_watermark"] != first_payload["target_watermark"]
        assert rows[0].payload["effective_build_date"] == "2026-07-28"
        assert rows[0].payload["force_rebuild"] is False
    await db.close()


@pytest.mark.asyncio
async def test_daily_share_enqueue_is_disabled_with_builder(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import main

    db = Database(str(tmp_path / "daily-share-disabled.sqlite"))
    await db.init()
    monkeypatch.setenv("ENABLE_STATIC_SITE_KAGGLE_BUILDER", "0")
    monkeypatch.setenv(
        "STATIC_SITE_CURRENT_DATETIME",
        "2026-07-27T00:05:00+02:00",
    )

    result = await main.enqueue_static_site_build_request(
        db,
        reason="Europe/Kaliningrad local date rollover",
        trigger="startup_catchup",
    )
    assert result == "disabled"
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build)
            )
        ).scalars().all()
        assert rows == []
    await db.close()


@pytest.mark.asyncio
async def test_same_day_smart_update_covers_daily_share_without_suppressing_updates(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import main

    db = Database(str(tmp_path / "daily-share-smart-update.sqlite"))
    await db.init()
    monkeypatch.setenv("ENABLE_STATIC_SITE_KAGGLE_BUILDER", "1")
    monkeypatch.setenv(
        "STATIC_SITE_CURRENT_DATETIME",
        "2026-07-27T13:00:00+02:00",
    )

    first = await main.enqueue_static_site_build_request(
        db,
        reason="smart update event 11",
        event_ids=[11],
        trigger="smart_update",
    )
    second = await main.enqueue_static_site_build_request(
        db,
        reason="smart update event 12",
        event_ids=[12],
        trigger="smart_update",
    )
    assert first == "new"
    assert second in {"merged", "merged-rearmed"}

    daily = await main.enqueue_static_site_build_request(
        db,
        reason="Europe/Kaliningrad local date rollover",
        trigger="calendar_rollover",
    )
    assert daily == "daily-already-requested"
    async with db.get_session() as session:
        row = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build)
            )
        ).scalar_one()
        assert row.payload["event_ids"] == [11, 12]
        assert row.payload["daily_share_refresh"]["local_date"] == "2026-07-27"
        assert row.payload["daily_share_idempotent"] is False
    await db.close()
