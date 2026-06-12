from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import poll_to_forward as pf
from db import Database
from models import Event


class DummyPollBot:
    def __init__(self):
        self.sent_polls = []
        self.stopped = []
        self.messages = []
        self.forwarded = []
        self.stop_poll_result = SimpleNamespace(
            total_voter_count=0,
            options=[],
        )

    async def send_poll(self, **kwargs):
        self.sent_polls.append(kwargs)
        return SimpleNamespace(
            message_id=101,
            poll=SimpleNamespace(id="poll-101"),
        )

    async def stop_poll(self, **kwargs):
        self.stopped.append(kwargs)
        return self.stop_poll_result

    async def send_message(self, **kwargs):
        self.messages.append(kwargs)
        return SimpleNamespace(message_id=201)

    async def forward_message(self, **kwargs):
        self.forwarded.append(kwargs)
        return SimpleNamespace(message_id=301)


def _event(event_id: int, *, title: str, post_id: int, event_type: str = "концерт") -> Event:
    return Event(
        id=event_id,
        title=title,
        description=f"Описание {title}",
        date="2026-06-13",
        time="19:00",
        location_name="Дом искусств",
        city="Калининград",
        source_text=f"source {title}",
        event_type=event_type,
        tg_event_post_id=post_id,
        tg_event_post_url=f"https://t.me/kldevents/{post_id}",
        telegraph_url=f"https://telegra.ph/event-{event_id}",
        lifecycle_status="active",
        silent=False,
    )


async def _seed_events(db: Database) -> None:
    async with db.get_session() as session:
        session.add(_event(101, title="Камерный концерт", post_id=501, event_type="концерт"))
        session.add(_event(102, title="Лекция о море", post_id=502, event_type="лекция"))
        session.add(_event(103, title="Семейная мастерская", post_id=503, event_type="мастер-класс"))
        await session.commit()


@pytest.mark.asyncio
async def test_debug_create_skips_without_llm_plan(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_events(db)
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")
    monkeypatch.setenv("POLL_TO_FORWARD_LLM_ENABLED", "1")

    async def fake_llm(**_kwargs):
        return None

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)
    bot = DummyPollBot()

    result = await pf.create_debug_poll_if_due(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
    )

    assert result["reason"] == "topic_underfill"
    assert bot.sent_polls == []
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status FROM poll_repost_run")
        assert (await cur.fetchone())[0] == pf.STATUS_SKIPPED_TOPIC_UNDERFILL
    await db.close()


@pytest.mark.asyncio
async def test_debug_create_uses_llm_plan_and_sends_poll(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_events(db)
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")

    async def fake_llm(**_kwargs):
        return {
            "question_text": "Что выбрать на завтра?",
            "options": [
                {"key": "music", "text": "Вечер с музыкой", "candidate_event_ids": [101]},
                {"key": "learn", "text": "Узнать новое", "candidate_event_ids": [102]},
                {"key": "kids", "text": "С детьми", "candidate_event_ids": [103]},
            ],
        }

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)
    bot = DummyPollBot()

    result = await pf.create_debug_poll_if_due(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
    )

    assert result["created"] is True
    assert bot.sent_polls[0]["chat_id"] == "@keniggpt"
    assert bot.sent_polls[0]["question"] == pf.DEFAULT_POLL_QUESTION_TEXT
    assert bot.sent_polls[0]["options"] == ["Вечер с музыкой", "Узнать новое", "С детьми"]
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, poll_message_id FROM poll_repost_run")
        assert await cur.fetchone() == (pf.STATUS_OPEN, 101)
    await db.close()


@pytest.mark.asyncio
async def test_debug_resolve_replies_and_forwards_llm_choice(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_events(db)
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")

    async def fake_llm(**kwargs):
        prompt = kwargs.get("prompt", "")
        if "winner_key" in prompt:
            return {"winner_key": "music", "event_id": 101, "reason": "самый сильный концерт"}
        return {
            "question_text": "Что выбрать на завтра?",
            "options": [
                {"key": "music", "text": "Вечер с музыкой", "candidate_event_ids": [101]},
                {"key": "learn", "text": "Узнать новое", "candidate_event_ids": [102]},
                {"key": "kids", "text": "С детьми", "candidate_event_ids": [103]},
            ],
        }

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)
    bot = DummyPollBot()
    await pf.create_debug_poll_if_due(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
    )
    bot.stop_poll_result = SimpleNamespace(
        total_voter_count=1,
        options=[
            SimpleNamespace(voter_count=1),
            SimpleNamespace(voter_count=0),
            SimpleNamespace(voter_count=0),
        ],
    )

    result = await pf.resolve_due_debug_polls(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 8, 31, tzinfo=timezone.utc),
    )

    assert result["resolved"] == 1
    assert bot.sent_polls[0]["question"] == pf.DEFAULT_POLL_QUESTION_TEXT
    assert bot.messages[0]["reply_to_message_id"] == 101
    assert bot.messages[0]["text"] == (
        "Вы выбрали: Вечер с музыкой. Спасибо за голос — показываю анонс, "
        "который лучше всего совпал с этим выбором.\n"
        "самый сильный концерт.\n"
        "Подробнее: https://telegra.ph/event-101\n"
        "Сейчас перешлю анонс 👇"
    )
    assert bot.forwarded[0] == {
        "chat_id": "@keniggpt",
        "from_chat_id": "@kldevents",
        "message_id": 501,
    }
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, winner_option_id, chosen_event_id, kldevents_message_id, forwarded_message_id FROM poll_repost_run"
        )
        assert await cur.fetchone() == (pf.STATUS_FORWARDED, "music", 101, 501, 301)
    await db.close()
