from __future__ import annotations

from models import Event
from smart_event_update import _apply_ticket_fields


def test_apply_ticket_fields_refines_broad_landing_page_link() -> None:
    event = Event(
        id=5786,
        title="Лекция",
        description="",
        date="2026-06-11",
        time="18:30",
        location_name="Научная библиотека",
        city="Калининград",
        source_text="source",
        ticket_link="https://kgd80.ru",
        ticket_trust_level="high",
        vk_ticket_short_url="https://vk.cc/old",
        vk_ticket_short_key="old",
    )

    updated = _apply_ticket_fields(
        event,
        ticket_link="https://kgd80.ru/sobytiya/mirnaya-zhizn-samoy-zapadnoy-tochki-rossii-baltiyskoy-kosy/?register=1",
        ticket_price_min=None,
        ticket_price_max=None,
        ticket_status=None,
        candidate_trust="high",
    )

    assert updated == ["ticket_link"]
    assert event.ticket_link.endswith("?register=1")
    assert event.vk_ticket_short_url is None
    assert event.vk_ticket_short_key is None


def test_apply_ticket_fields_keeps_unrelated_existing_link_at_same_trust() -> None:
    event = Event(
        id=1,
        title="Лекция",
        description="",
        date="2026-06-11",
        time="18:30",
        location_name="Научная библиотека",
        city="Калининград",
        source_text="source",
        ticket_link="https://tickets.example/a",
        ticket_trust_level="high",
    )

    updated = _apply_ticket_fields(
        event,
        ticket_link="https://other.example/register",
        ticket_price_min=None,
        ticket_price_max=None,
        ticket_status=None,
        candidate_trust="high",
    )

    assert updated == []
    assert event.ticket_link == "https://tickets.example/a"
