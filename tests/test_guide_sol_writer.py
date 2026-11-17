import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest

from guide_excursions import digest_writer as writer
from guide_excursions import sol_writer
from guide_excursions import sol_service


PAYLOAD = {"excursions": [{"occurrence_id": 1, "title": "Прогулка", "guides": ["Гид"]}]}
STORY = {"lead": "У ручья.", "lead_occurrence_id": 1, "outro": "",
         "paragraphs": [{"details": ["ручей"], "lead_anchor": "ручья", "occurrence_ids": [1], "text": "У ручья растёт дуб."}]}


@pytest.mark.asyncio
async def test_primary_writes_before_google_is_initialized(monkeypatch):
    primary = AsyncMock(return_value=json.dumps(STORY))
    google = Mock(side_effect=AssertionError("Google should not be initialized"))
    monkeypatch.setattr(writer, "generate_sol_narrative", primary)
    monkeypatch.setattr(writer, "_get_digest_writer_runtime", google)
    review = {}
    assert await writer.ask_vk_narrative(PAYLOAD, issue_id=5, review=review) == STORY
    google.assert_not_called()
    assert review["model"] == "gpt-6.1-sol" and review["provider"] == "codex"
    assert json.loads(review["raw_response"]) == STORY
    assert review["attempts"][0]["outcome"] == "written"


@pytest.mark.asyncio
@pytest.mark.parametrize("primary", [FileNotFoundError(), TimeoutError(), "{}", "invalid",
                                     json.dumps(dict(STORY, lead_occurrence_id=99)),
                                     json.dumps(dict(STORY, lead="У [ручья]"))])
async def test_primary_failure_uses_existing_limited_google_chain(monkeypatch, primary):
    generate = AsyncMock(side_effect=primary) if isinstance(primary, Exception) else AsyncMock(return_value=primary)
    monkeypatch.setattr(writer, "generate_sol_narrative", generate)
    usage = Mock(provider_model_version="gemini-fallback-002", model="gemini-fallback")
    client = Mock(generate_content_async=AsyncMock(return_value=(json.dumps(STORY), usage)))
    monkeypatch.setattr(writer, "_get_digest_writer_runtime", lambda: (client, ["test-key-id"]))
    review = {}
    assert await writer.ask_vk_narrative(PAYLOAD, issue_id=5, review=review) == STORY
    assert review["provider"] == "google" and review["model"] == "gemini-fallback-002"
    assert len(review["attempts"]) == 2 and review["attempts"][0]["outcome"] != "written"
    assert client.generate_content_async.await_args.kwargs["candidate_key_ids"] == ["test-key-id"]


@pytest.mark.asyncio
async def test_both_rejected_returns_list_sentinel(monkeypatch):
    broken = dict(STORY)
    del broken["lead_occurrence_id"]
    monkeypatch.setattr(writer, "generate_sol_narrative", AsyncMock(return_value=json.dumps(broken)))
    client = Mock(generate_content_async=AsyncMock(return_value=(json.dumps(broken), Mock(model="gemini", provider_model_version=None))))
    monkeypatch.setattr(writer, "_get_digest_writer_runtime", lambda: (client, None))
    review = {}
    assert await writer.ask_vk_narrative(PAYLOAD, issue_id=5, review=review) is None
    assert review["provider"] is review["model"] is None
    assert [a["outcome"] for a in review["attempts"]] == ["rejected", "rejected"]


@pytest.mark.asyncio
async def test_native_cli_accepts_only_completed_text_turn(monkeypatch, tmp_path):
    fake = tmp_path / "codex"
    fake.write_text("#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' "
                    """'{"type":"item.completed","item":{"type":"agent_message","text":"{}"}}' """
                    """'{"type":"turn.completed"}'\n""")
    fake.chmod(0o700)
    monkeypatch.setenv("GUIDE_DIGEST_CODEX_BIN", str(fake))
    assert await sol_writer.generate_sol_narrative("тест", {}) == "{}"
    fake.write_text("#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' "
                    """'{"type":"turn.failed"}'\n""")
    with pytest.raises(sol_writer.SolWriterError):
        await sol_writer.generate_sol_narrative("тест", {})


def test_lead_covers_each_distinct_tour_and_repeats_its_first_subject():
    rows = {
        1: {"title": "Городской парк", "guides": ["Анна"]},
        2: {"title": "Прогулка с ботаником", "guides": ["Ольга"]},
        3: {"title": "Третий маршрут", "guides": ["Юлия"]},
        4: {"title": "Городской парк", "guides": ["Анна"]},
    }
    story = {"lead_occurrence_id": 1, "lead": "Где растёт дуб? Другие прогулки ведут в сад и на маяк.",
             "outro": "", "paragraphs": [
                 {"occurrence_ids": [1, 4], "lead_anchor": "дуб", "details": ["дуб"], "text": "Дуб растёт у воды."},
                 {"occurrence_ids": [2], "lead_anchor": "сад", "details": ["сад"], "text": "Посетим сад у озера."},
                 {"occurrence_ids": [3], "lead_anchor": "маяк", "details": ["маяк"], "text": "Маяк открыт."},
             ]}
    def validate():
        return writer._validate_vk_narrative(story, rows, issue_id=17)

    assert validate()
    story["lead"] = "Где растёт дуб? В другом маршруте — сад."
    assert validate() is None
    story["lead"] += " Маяк."
    story["paragraphs"][2]["text"] = "Маяковский писал стихи."
    assert validate() is None
    story["paragraphs"][2]["text"] = "Маяк открыт."
    story["paragraphs"][2]["lead_anchor"] = "сад"
    assert validate() is None
    story["paragraphs"][2]["lead_anchor"] = "маяк"
    story["paragraphs"][0]["text"] = "Пройдём по дорожке. Дуб растёт у воды."
    assert validate() is None


def test_sparse_tour_can_use_its_confirmed_title_in_the_lead():
    rows = {1: {"title": "У реки", "guides": []},
            2: {"title": "«История одной поездки от города до моря»", "guides": ["Анна"]}}
    data = {"lead": "Где растёт клён? Ещё в выпуске «История одной поездки от города до моря».",
            "lead_occurrence_id": 1, "outro": "", "paragraphs": [
                {"occurrence_ids": [1], "lead_anchor": "клен", "details": ["клён"], "text": "Клён растёт у воды."},
                {"occurrence_ids": [2], "lead_anchor": "История одной поездки от города до моря",
                 "details": ["Анна"], "text": "Эту экскурсию ведёт Анна."}]}
    assert writer._validate_vk_narrative(data, rows, issue_id=18)


@pytest.mark.asyncio
async def test_private_writer_rebuilds_the_native_prompt_and_runs_the_normal_validation(monkeypatch, tmp_path):
    native = AsyncMock(return_value=json.dumps(STORY))
    monkeypatch.setattr(sol_service, "generate_sol_narrative", native)
    monkeypatch.setattr(writer, "generate_sol_narrative", Mock(side_effect=AssertionError("unexpected local login")))
    socket_path = str(tmp_path / "sol.sock")
    monkeypatch.setenv("GUIDE_DIGEST_SOL_SOCKET", socket_path)

    async def handle(reader, stream):
        raw = await sol_service.write_edition(await reader.readline())
        stream.write(json.dumps({"text": raw}).encode() + b"\n")
        await stream.drain()
        stream.close()
        await stream.wait_closed()

    async with await asyncio.start_unix_server(handle, path=socket_path):
        review = {}
        assert await writer.ask_vk_narrative(PAYLOAD, issue_id=5, review=review) == STORY
    native.assert_awaited_once_with(writer._vk_narrative_prompt(PAYLOAD, issue_id=5), writer._VK_NARRATIVE_SOL_SCHEMA)
    assert (review["provider"], review["model"]) == ("codex", "gpt-6.1-sol")


@pytest.mark.asyncio
async def test_private_writer_rejects_prompt_or_model_overrides(monkeypatch):
    native = AsyncMock()
    monkeypatch.setattr(sol_service, "generate_sol_narrative", native)
    for request in ({"issue_id": 5, "payload": PAYLOAD, "prompt": "other"},
                    {"issue_id": 5, "payload": PAYLOAD, "model": "other"},
                    {"issue_id": 5, "payload": {"excursions": []}}):
        with pytest.raises(ValueError):
            await sol_service.write_edition(json.dumps(request).encode() + b"\n")
    native.assert_not_called()
