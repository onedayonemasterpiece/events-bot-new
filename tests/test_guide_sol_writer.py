import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest

from guide_excursions import digest_writer as writer
from guide_excursions import sol_writer


PAYLOAD = {"excursions": [{"occurrence_id": 1, "title": "Прогулка", "guides": ["Гид"]}]}
STORY = {"lead": "У ручья.", "lead_occurrence_id": 1, "outro": "",
         "paragraphs": [{"details": ["ручей"], "occurrence_ids": [1], "text": "У ручья растёт дуб."}]}


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
