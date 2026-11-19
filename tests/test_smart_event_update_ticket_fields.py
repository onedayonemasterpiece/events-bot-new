from __future__ import annotations

import json

import pytest

from models import Event
import smart_event_update as su
from smart_event_update import _apply_ticket_fields


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "legacy_free,decision,link,terms,expected",
    [
        (False, None, None, "", None),
        (False, None, "https://tickets.example/fair", "", None),
        (False, "unknown", None, "", None),
        (False, None, None, "Вход платный.", None),
        (False, "confirmed_paid", None, "Вход платный.", False),
        (False, "confirmed_free", None, "Вход свободный.", True),
        (True, None, None, "Вход свободный. Мастер-класс оплачивается отдельно.", True),
    ],
)
async def test_full_rewrite_only_passes_confirmed_admission(
    monkeypatch, legacy_free, decision, link, terms, expected
):
    # Telegram tastes_of_vistynets/1429, event 11105: sale of local produce.
    source = (
        "Ярмарка «Вкусов Виштынецкой возвышенности». В программе: "
        "луковой пирог от Натальи Добровольской, пос. Дмитриевка; "
        "манник с иргой, тыквенные вафли, лисичкины пряники и чайные сердечки от Елены Уфимцевой."
    )
    source = f"{source} {terms}".strip()
    event = Event(
        title="Ярмарка", date="2026-11-21", source_text=source, is_free=legacy_free,
        ticket_link=link,
        collection_decisions={"admission_decision": {"value": decision}} if decision else None,
    )
    candidate = su.EventCandidate(
        source_type="telegram", source_url="https://t.me/tastes_of_vistynets/1429", source_text=source
    )
    prompts = []

    async def capture(prompt, **_kwargs):
        prompts.append(prompt)
        return None

    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "_ask_gemma_text", capture)
    await su._rewrite_description_full_from_sources(event, candidate)

    (prompt,) = prompts
    data = json.loads(prompt.split("Данные:\n", 1)[1])
    assert data["is_free"] is expected
    assert data["source_text"] == source
    assert "не подтверждают обязательный входной билет" in prompt


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


def test_apply_ticket_fields_does_not_replace_real_link_with_vk_shorter() -> None:
    event = Event(
        id=5333,
        title="Концерт классической музыки «Зимний путь»",
        description="",
        date="2026-06-13",
        time="21:00",
        location_name="Филиал Третьяковской галереи",
        city="Калининград",
        source_text="source",
        ticket_link="https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/46430/2026-06-13/21:00:00",
        ticket_trust_level="medium",
    )

    updated = _apply_ticket_fields(
        event,
        ticket_link="https://vk.cc/cYaxjc",
        ticket_price_min=None,
        ticket_price_max=None,
        ticket_status=None,
        candidate_trust="high",
    )

    assert updated == []
    assert "tretyakovgallery.ru/tickets/" in event.ticket_link


def test_apply_ticket_fields_replaces_vk_shorter_with_real_link() -> None:
    event = Event(
        id=5333,
        title="Концерт классической музыки «Зимний путь»",
        description="",
        date="2026-06-13",
        time="21:00",
        location_name="Филиал Третьяковской галереи",
        city="Калининград",
        source_text="source",
        ticket_link="https://vk.cc/cYaxjc",
        ticket_trust_level="high",
        vk_ticket_short_url="https://vk.cc/cYaxjc",
        vk_ticket_short_key="cYaxjc",
    )

    updated = _apply_ticket_fields(
        event,
        ticket_link="https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/46430/2026-06-13/21:00:00",
        ticket_price_min=None,
        ticket_price_max=None,
        ticket_status=None,
        candidate_trust="high",
    )

    assert updated == ["ticket_link"]
    assert "tretyakovgallery.ru/tickets/" in event.ticket_link
    assert event.vk_ticket_short_url is None
    assert event.vk_ticket_short_key is None
