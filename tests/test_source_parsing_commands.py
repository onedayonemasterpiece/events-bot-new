from __future__ import annotations

import asyncio

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
