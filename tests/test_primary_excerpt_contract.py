from types import SimpleNamespace

import pytest

import main
import smart_event_update as su
import source_parsing.handlers as handlers
import vk_intake
from source_parsing.parser import TheatreEvent


SOURCE = "Аудиоспектакль в арт-пространстве, 12+."
GENERATED = "Историческое путешествие о культурном наследии региона."


@pytest.mark.asyncio
@pytest.mark.parametrize("producer", ["parser", "vk"])
async def test_generated_draft_is_not_primary_excerpt(monkeypatch, producer):
    captured = []
    draft = vk_intake.EventDraft(
        title="Дорога ЭПОХ", date="2099-11-22", time="18:00",
        source_text=SOURCE, description=GENERATED,
    )

    async def capture(_db, candidate, *_args, **_kwargs):
        captured.append(candidate)
        return SimpleNamespace(
            is_accepted=False, outcome=su.SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
            reason="offline_capture", diagnostic_event_id=None,
        )

    if producer == "parser":
        async def fake_drafts(*_args, **_kwargs):
            return [draft], None

        monkeypatch.setattr(vk_intake, "build_event_drafts_from_vk", fake_drafts)
        monkeypatch.setattr(handlers, "_smart_event_update_with_lock_retry", capture)
        event = TheatreEvent(
            title=draft.title, description=SOURCE, date_raw="22.11.2099 18:00",
            parsed_date=draft.date, parsed_time=draft.time, source_type="yantarhall",
            ticket_status="available",
            url="https://example.test/ticket", photos=["https://example.test/poster.jpg"],
        )
        await handlers.add_new_event_via_queue(None, None, event, 1, 1)
    else:
        monkeypatch.setattr(su, "smart_event_update", capture)
        await vk_intake.persist_event_and_pages(
            draft, [], None, source_post_url="https://vk.com/wall-1_2",
        )

    assert len(captured) == 1
    candidate = captured[0]
    assert SOURCE in candidate.source_text
    assert GENERATED not in candidate.source_text
    assert GENERATED not in (candidate.raw_excerpt or "")
    if producer == "parser":
        assert candidate.raw_excerpt == SOURCE


@pytest.mark.asyncio
async def test_genuine_excerpt_only_remains_rich_fact_evidence(monkeypatch):
    excerpt = "В семейном мюзикле звучат музыка и песни на стихи Пушкина."

    async def fake_json(prompt, *_args, **_kwargs):
        assert excerpt in prompt
        return {"public_core_facts": [excerpt]}

    monkeypatch.setattr(su, "_ask_gemma_json", fake_json)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    candidate = su.EventCandidate(
        source_type="parser:pyramida", source_url="https://example.test/ticket", source_text="",
        raw_excerpt=excerpt,
    )
    assert excerpt in await su._llm_extract_candidate_facts(candidate)
