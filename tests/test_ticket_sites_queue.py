from types import SimpleNamespace

import pytest
from sqlalchemy import select

from db import Database
from models import TicketSiteQueueItem
from smart_event_update import SmartUpdateResult, SmartUpdateTerminalOutcome


@pytest.mark.asyncio
async def test_ticket_site_smart_update_schedules_public_fanout(monkeypatch):
    import smart_event_update as seu
    import ticket_sites_queue

    captured = {}

    async def fake_smart_event_update(db, candidate, *, check_source_url, schedule_kwargs):
        captured["candidate"] = candidate
        captured["check_source_url"] = check_source_url
        captured["schedule_kwargs"] = schedule_kwargs
        return seu.SmartUpdateResult(
            outcome=seu.SmartUpdateTerminalOutcome.CREATED,
            event_id=42,
        )

    monkeypatch.setattr(seu, "smart_event_update", fake_smart_event_update)

    result = await ticket_sites_queue._smart_update_from_theatre_event(
        db=object(),
        site_kind="qtickets",
        url="https://kaliningrad.qtickets.events/240069-test",
        title="Тестовый концерт",
        date_iso="2026-07-04",
        time_str="19:00",
        location="Зал",
        description="Описание",
        ticket_price_min=1000,
        ticket_price_max=2000,
        ticket_status="available",
        photos=["https://cdn.example/poster.jpg"],
    )

    assert result.outcome is seu.SmartUpdateTerminalOutcome.CREATED
    assert result.event_id == 42
    assert captured["check_source_url"] is False
    assert captured["schedule_kwargs"] == {"skip_vk_sync": False}
    candidate = captured["candidate"]
    assert candidate.source_type == "parser:qtickets"
    assert candidate.ticket_link == "https://kaliningrad.qtickets.events/240069-test"


@pytest.mark.asyncio
async def test_ticket_queue_treats_exact_noop_as_success(tmp_path, monkeypatch):
    import source_parsing.qtickets as qtickets
    import ticket_sites_queue

    db = Database(str(tmp_path / "ticket-noop.sqlite"))
    await db.init()
    url = "https://kaliningrad.qtickets.events/240069-test"
    try:
        async with db.get_session() as session:
            session.add(
                TicketSiteQueueItem(
                    site_kind="qtickets",
                    url=url,
                    status="active",
                )
            )
            await session.commit()

        async def _kernel(*_args, **_kwargs):
            return "complete", ["fixture.json"], 0.01

        def _parse(_paths):
            return [
                SimpleNamespace(
                    url=url,
                    title="Тестовый концерт",
                    parsed_date="2099-07-04",
                    parsed_time="19:00",
                    location="Зал",
                    description="Описание",
                    ticket_price_min=1000,
                    ticket_price_max=2000,
                    ticket_status="available",
                    age_restriction=None,
                    photos=[],
                )
            ]

        async def _noop(*_args, **_kwargs):
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY,
                event_id=42,
            )

        monkeypatch.setattr(qtickets, "run_qtickets_kaggle_kernel", _kernel)
        monkeypatch.setattr(qtickets, "parse_qtickets_output", _parse)
        monkeypatch.setattr(ticket_sites_queue, "_smart_update_from_theatre_event", _noop)

        report = await ticket_sites_queue.process_ticket_sites_queue(
            db,
            site_kind="qtickets",
            only_url=url,
            trigger="test",
        )
        assert report.processed == 1
        assert report.success == 1
        assert report.failed == 0
        async with db.get_session() as session:
            row = (
                await session.execute(
                    select(TicketSiteQueueItem)
                )
            ).scalar_one()
        assert row.status == "active"
        assert row.event_id == 42
        assert row.last_error is None
        assert row.last_result_json["smart_update_outcome"] == "NOOP_EXACT_REPLAY"
    finally:
        await db.close()
