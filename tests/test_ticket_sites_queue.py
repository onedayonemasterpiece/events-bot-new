from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_ticket_site_smart_update_schedules_public_fanout(monkeypatch):
    import smart_event_update as seu
    import ticket_sites_queue

    captured = {}

    async def fake_smart_event_update(db, candidate, *, check_source_url, schedule_kwargs):
        captured["candidate"] = candidate
        captured["check_source_url"] = check_source_url
        captured["schedule_kwargs"] = schedule_kwargs
        return SimpleNamespace(status="created", event_id=42)

    monkeypatch.setattr(seu, "smart_event_update", fake_smart_event_update)

    status, event_id = await ticket_sites_queue._smart_update_from_theatre_event(
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

    assert (status, event_id) == ("created", 42)
    assert captured["check_source_url"] is False
    assert captured["schedule_kwargs"] == {"skip_vk_sync": False}
    candidate = captured["candidate"]
    assert candidate.source_type == "parser:qtickets"
    assert candidate.ticket_link == "https://kaliningrad.qtickets.events/240069-test"
