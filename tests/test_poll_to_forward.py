from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import poll_to_forward as pf
from db import Database
from models import Event


def test_repost_intro_handles_tied_topics_without_preview_link_copy():
    text = pf._repost_intro_text(
        "Выставки",
        "как раз открывается выставка, и это хорошо попадает в голосование",
        event_title="Точка и линия",
        telegraph_url="https://telegra.ph/vystavka",
        tied_texts=["Выставки", "Экскурсии и прогулки"],
    )

    assert text == (
        "Спасибо за голоса: голоса разделились поровну между «Выставки» и «Экскурсии и прогулки».\n"
        'Я бы предложил <a href="https://telegra.ph/vystavka">Точка и линия</a> — '
        "как раз открывается выставка, и это хорошо попадает в голосование.\n"
        "Поставьте 👍, если рекомендация попала, или 👎, если нет.\n"
        "Сейчас перешлю анонс 👇"
    )
    assert "Подробнее" not in text


class DummyPollBot:
    def __init__(self):
        self.sent_polls = []
        self.stopped = []
        self.messages = []
        self.forwarded = []
        self.stop_poll_results = {}
        self._next_poll_message_id = 101
        self._next_reply_message_id = 201
        self._next_forward_message_id = 301
        self.stop_poll_result = SimpleNamespace(
            total_voter_count=0,
            options=[],
        )

    async def send_poll(self, **kwargs):
        message_id = self._next_poll_message_id
        self._next_poll_message_id += 1
        record = dict(kwargs)
        record["_message_id"] = message_id
        self.sent_polls.append(record)
        return SimpleNamespace(
            message_id=message_id,
            poll=SimpleNamespace(id=f"poll-{message_id}"),
        )

    async def stop_poll(self, **kwargs):
        self.stopped.append(kwargs)
        message_id = int(kwargs.get("message_id") or 0)
        return self.stop_poll_results.get(message_id, self.stop_poll_result)

    async def send_message(self, **kwargs):
        self.messages.append(kwargs)
        message_id = self._next_reply_message_id
        self._next_reply_message_id += 1
        return SimpleNamespace(message_id=message_id)

    async def forward_message(self, **kwargs):
        self.forwarded.append(kwargs)
        message_id = self._next_forward_message_id
        self._next_forward_message_id += 1
        return SimpleNamespace(message_id=message_id)


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


async def _seed_many_events(db: Database) -> None:
    event_specs = [
        (101, "Камерный концерт", "концерт", 501),
        (102, "Джазовый вечер", "концерт", 502),
        (103, "Хоровая программа", "концерт", 503),
        (104, "Фортепианный вечер", "концерт", 504),
        (105, "Музыка у моря", "концерт", 505),
        (201, "Лекция о городе", "лекция", 601),
        (202, "Экскурсия по музею", "экскурсия", 602),
        (203, "Историческая прогулка", "экскурсия", 603),
        (204, "Открытая лекция", "лекция", 604),
        (205, "Архитектурный маршрут", "экскурсия", 605),
        (301, "Семейный спектакль", "детям", 701),
        (302, "Детская мастерская", "мастер-класс", 702),
        (303, "Праздник во дворе", "детям", 703),
        (304, "Сказки в библиотеке", "детям", 704),
        (305, "Семейная экскурсия", "детям", 705),
    ]
    async with db.get_session() as session:
        for event_id, title, event_type, post_id in event_specs:
            session.add(_event(event_id, title=title, post_id=post_id, event_type=event_type))
        await session.commit()


def test_default_question_variants_frame_real_tomorrow_plan():
    assert len(pf.DEFAULT_POLL_QUESTION_VARIANTS) >= 6
    banned = (
        "план звучит",
        "звучит лучше",
        "общему выбору",
        "лучшие события",
        "алгоритм",
        "за какую тему",
        "выберите тему",
        "анонс",
        "перешлю",
    )
    for text in pf.DEFAULT_POLL_QUESTION_VARIANTS:
        lowered = text.lower()
        assert not any(fragment in lowered for fragment in banned)
        assert "сегодня вечером" in lowered
        assert "завтра" in lowered
        assert any(
            marker in lowered
            for marker in (
                "куда",
                "пойти",
                "сходить",
                "выбраться",
                "план",
                "настро",
                "провести",
                "направление",
                "выбрать",
                "темати",
                "тип события",
            )
        )
        assert any(marker in lowered for marker in ("рекоменд", "посовет", "подбер", "найду", "покажу"))


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
    assert bot.sent_polls[0]["question"] in pf.DEFAULT_POLL_QUESTION_VARIANTS
    assert bot.sent_polls[0]["options"] == ["Вечер с музыкой", "Узнать новое", "С детьми"]
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, poll_message_id FROM poll_repost_run")
        assert await cur.fetchone() == (pf.STATUS_OPEN, 101)
    await db.close()


@pytest.mark.asyncio
async def test_five_isolated_cycles_keep_recommendation_inside_voted_theme(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_many_events(db)
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")
    used_event_ids: set[int] = set()
    option_plan = [
        {"key": "music", "text": "Послушать музыку вечером", "candidate_event_ids": [101, 102, 103, 104, 105]},
        {"key": "learn", "text": "Узнать что-то новое", "candidate_event_ids": [201, 202, 203, 204, 205]},
        {"key": "family", "text": "Провести время с семьёй", "candidate_event_ids": [301, 302, 303, 304, 305]},
    ]

    async def fake_llm(**kwargs):
        prompt = kwargs.get("prompt", "")
        if "winner_key" not in prompt:
            return {"question_text": "", "options": option_plan}
        match = re.search(
            r"Опции-победители/ничья:\n(?P<options>.*?)\n\nСобытия-кандидаты:",
            prompt,
            flags=re.DOTALL,
        )
        tied_options = json.loads(match.group("options")) if match else []
        selected = tied_options[0]
        event_id = next(
            event_id
            for event_id in selected["candidate_event_ids"]
            if event_id not in used_event_ids
        )
        used_event_ids.add(event_id)
        return {
            "winner_key": selected["key"],
            "event_id": event_id,
            "reason": f"это аккуратно ложится в выбранную тему «{selected['text']}»",
        }

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)
    bot = DummyPollBot()
    slots = [
        datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
        datetime(2026, 6, 12, 9, 0, tzinfo=timezone.utc),
        datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 6, 12, 11, 0, tzinfo=timezone.utc),
        datetime(2026, 6, 12, 12, 0, tzinfo=timezone.utc),
    ]
    vote_patterns = [
        [1, 0, 0],
        [0, 1, 0],
        [0, 0, 1],
        [1, 1, 0],
        [0, 2, 0],
    ]
    for now_utc, votes in zip(slots, vote_patterns, strict=True):
        result = await pf.create_debug_poll_if_due(db, bot, now_utc=now_utc)
        assert result["created"] is True
        poll_message_id = bot.sent_polls[-1]["_message_id"]
        bot.stop_poll_results[poll_message_id] = SimpleNamespace(
            total_voter_count=sum(votes),
            options=[SimpleNamespace(voter_count=count) for count in votes],
        )

    result = await pf.resolve_due_debug_polls(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 13, 31, tzinfo=timezone.utc),
    )

    assert result["resolved"] == 5
    assert len(bot.messages) == 5
    assert len(bot.forwarded) == 5
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, winner_option_id, winner_text, chosen_event_id, options_json, result_json
            FROM poll_repost_run
            ORDER BY id
            """
        )
        rows = await cur.fetchall()
    assert len(rows) == 5
    for index, row in enumerate(rows):
        status, winner_option_id, winner_text, chosen_event_id, options_json, result_json = row
        assert status == pf.STATUS_FORWARDED
        options = json.loads(options_json)
        result_options = json.loads(result_json)["options"]
        winner = next(option for option in options if option["key"] == winner_option_id)
        assert int(chosen_event_id) in winner["candidate_event_ids"]
        max_votes = max(item["voter_count"] for item in result_options)
        tied_keys = {
            item["key"]
            for item in result_options
            if item["voter_count"] == max_votes
        }
        assert winner_option_id in tied_keys
        text = bot.messages[index]["text"]
        assert winner_text in text
        assert "Подробнее" not in text
        assert "Поставьте 👍" in text
        assert "Сейчас перешлю анонс 👇" in text
        assert bot.messages[index]["parse_mode"] == "HTML"
        assert bot.messages[index]["disable_web_page_preview"] is True
    assert "голоса разделились поровну" in bot.messages[3]["text"]
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
    assert bot.sent_polls[0]["question"] in pf.DEFAULT_POLL_QUESTION_VARIANTS
    assert bot.messages[0]["reply_to_message_id"] == 101
    assert bot.messages[0]["text"] == (
        "Спасибо за голоса: вы выбрали «Вечер с музыкой».\n"
        'Я бы предложил <a href="https://telegra.ph/event-101">Камерный концерт</a> — самый сильный концерт.\n'
        "Поставьте 👍, если рекомендация попала, или 👎, если нет.\n"
        "Сейчас перешлю анонс 👇"
    )
    assert bot.messages[0]["parse_mode"] == "HTML"
    assert bot.messages[0]["disable_web_page_preview"] is True
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
