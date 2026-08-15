from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import source_parsing.commands as commands
from source_parsing.commands import MAX_TG_MESSAGE_LEN, _send_markdown_chunks


class _Bot:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, str | None]] = []

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        parse_mode: str | None = None,
    ) -> None:
        self.calls.append((chat_id, text, parse_mode))


def test_long_parser_report_is_chunked_within_telegram_limit() -> None:
    bot = _Bot()
    report = "\n".join(f"line-{index:03d} " + ("x" * 180) for index in range(60))

    asyncio.run(_send_markdown_chunks(bot, 42, report))

    assert len(bot.calls) > 1
    assert all(chat_id == 42 for chat_id, _text, _mode in bot.calls)
    assert all(mode == "Markdown" for _chat_id, _text, mode in bot.calls)
    assert all(len(text) <= MAX_TG_MESSAGE_LEN for _chat_id, text, _mode in bot.calls)
    assert "\n".join(text for _chat_id, text, _mode in bot.calls) == report


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stored_signatures", "expected_only_sources"),
    [
        ({"sobor": "old", "dramteatr": "old"}, None),
        ({"sobor": "new", "dramteatr": "new"}, ["sobor"]),
    ],
)
async def test_day_parser_recovery_does_not_starve_changed_sources(
    monkeypatch,
    stored_signatures,
    expected_only_sources,
):
    signatures = {"sobor": "new", "dramteatr": "new"}
    calls = []

    async def claim(_db):
        return ["sobor"]

    async def collect():
        return signatures

    async def run(_db, _bot, **kwargs):
        calls.append(kwargs.get("only_sources"))
        return SimpleNamespace(total_events=0, errors=[], stats_by_source={})

    async def noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(commands, "_claim_source_parser_recovery_requests", claim)
    monkeypatch.setattr(commands, "_collect_source_parsing_signatures", collect)
    monkeypatch.setattr(
        commands, "_load_source_parsing_guard", lambda: {"signatures": stored_signatures}
    )
    monkeypatch.setattr(commands, "run_source_parsing", run)
    monkeypatch.setattr(commands, "_settle_source_parser_recovery_requests", noop)
    monkeypatch.setattr(commands, "_update_source_parsing_guard", noop)
    monkeypatch.setattr(commands, "resolve_superadmin_chat_id", noop)

    await commands.source_parsing_scheduler_if_changed(object(), object())

    assert calls == [expected_only_sources]
