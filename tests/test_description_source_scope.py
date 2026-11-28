from types import SimpleNamespace

import pytest

import smart_event_update as su


SCOPE = "28 ноября\n14:30 — кормление дикобразов"
PROGRAMME = (
    "28 ноября\n11:30 — кормление колобусов\n14:30 — кормление дикобразов\n"
    "29 ноября\n11:00 — экскурсия о животных в легендах народов России"
)


def source_pair(scope=SCOPE):
    candidate = su.EventCandidate(
        title="Кормление дикобразов", date="2026-11-28", time="14:30",
        source_type="vk", source_url="https://vk.com/wall-48383763_43077",
        source_text=PROGRAMME, raw_excerpt=PROGRAMME,
        occurrence_scope_text=scope,
        posters=[su.PosterCandidate(ocr_text=PROGRAMME)],
    )
    event = SimpleNamespace(
        title=candidate.title, date=candidate.date, end_date=None,
        source_text=SCOPE, source_texts=[SCOPE],
    )
    return event, candidate


@pytest.mark.asyncio
async def test_selected_child_scope_reaches_description_shrink(monkeypatch):
    event, candidate = source_pair()
    captured = []

    async def capture(prompt, **kwargs):
        captured.append(prompt)
        return "Посетители наблюдают за кормлением дикобразов."

    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "_ask_gemma_text", capture)
    source = su._pick_richest_source_text_for_description(event, candidate)
    await su._llm_shrink_description_to_budget(
        source_type="vk", source_url=None, description="Кормление дикобразов",
        source_text=source, facts=[], max_chars=300,
    )
    assert source == SCOPE
    assert "колобусов" not in captured[0]
    assert "экскурсия" not in captured[0]
    assert candidate.source_text == PROGRAMME


def test_selected_scope_preserves_accepted_event_source():
    event, candidate = source_pair()
    accepted = "Дикобразов кормят овощами. Посетители наблюдают за животными снаружи вольера."
    event.source_text = accepted
    event.source_texts = [accepted]
    assert su._pick_richest_source_text_for_description(event, candidate) == accepted


def test_unscoped_candidate_keeps_richest_source():
    event, candidate = source_pair(scope=None)
    assert "колобусов" in su._pick_richest_source_text_for_description(event, candidate)
