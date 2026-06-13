from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
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
        "Голоса разделились поровну между «Выставки» и «Экскурсии и прогулки». "
        "Беру один из этих вариантов.\n\n"
        'Беру в рекомендацию один анонс из этих тем — <a href="https://telegra.ph/vystavka">Точка и линия</a> — '
        "как раз открывается выставка, и это хорошо попадает в голосование.\n\n"
        "Если рекомендация зашла — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.\n\n"
        "Сейчас перешлю анонс 👇"
    )
    assert "Подробнее" not in text
    assert text.count("\n\n") == 3


def test_repost_intro_compacts_long_reason():
    text = pf._repost_intro_text(
        "Провести время с семьёй",
        (
            "Холмогорье завтра как раз устраивают встречу со сказочными героями — "
            "хороший вариант, чтобы выбраться на природу и занять детей мастер-классами"
        ),
        event_title="Встреча со сказочными героями в Холмогорье",
        telegraph_url="https://telegra.ph/demo",
    )

    recommendation_line = next(line for line in text.splitlines() if line.startswith("Беру в рекомендацию"))
    reason = recommendation_line.split(" — ", 1)[1]
    assert len(reason) <= 103
    assert " — " not in reason
    assert reason.endswith("...")
    assert text.count("\n\n") == 3


def test_repost_intro_softens_marketing_reason_lead():
    text = pf._repost_intro_text(
        "Вкусно провести время",
        "Отличный вариант для тех, кто проголосовал за гастро-отдых — на ферме как раз праздник",
        event_title="Фермерский праздник",
        telegraph_url="https://telegra.ph/farm",
    )

    assert "Отличный вариант" not in text
    assert (
        'Беру в рекомендацию анонс <a href="https://telegra.ph/farm">Фермерский праздник</a> — '
        "для тех, кто проголосовал за гастро-отдых, на ферме как раз праздник."
    ) in text


def test_repost_intro_renders_llm_reply_template_with_safe_event_link():
    text = pf._repost_intro_text(
        "Узнать новое на лекции или встрече",
        "для тех, кто голосовал за лекции",
        event_title="Лекция «Моне / Мане: погружение в мир импрессионизма»",
        telegraph_url="https://telegra.ph/lecture",
        reply_template=(
            "Спасибо за голоса — сегодня берём тему «Узнать новое на лекции или встрече».\n\n"
            "Для этой темы подходит лекция {{EVENT_LINK}}: в субботу можно спокойно погрузиться в историю импрессионизма.\n\n"
            "Если попал с рекомендацией — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.\n\n"
            "Сейчас перешлю анонс 👇"
        ),
    )

    assert "я бы предложил Лекция" not in text
    assert (
        'подходит лекция <a href="https://telegra.ph/lecture">Лекция «Моне / Мане: '
        'погружение в мир импрессионизма»</a>:'
    ) in text
    assert "Если попал с рекомендацией" in text
    assert "Сейчас перешлю анонс 👇" in text


def test_repost_intro_uses_llm_generated_event_link_text_not_raw_caps_title():
    text = pf._repost_intro_text(
        "Пообщение и смех",
        "подходит под выбранную тему",
        event_title="ОТКРЫТЫЙ МИКРОФОН",
        telegraph_url="https://telegra.ph/open-mic",
        event_link_text="стендап «Открытый микрофон»",
        reply_template=(
            "Спасибо всем, кто проголосовал.\n\n"
            "Вы выбрали тему про общение и смех, поэтому сегодня взял для вас {{EVENT_LINK}}.\n\n"
            "Ставьте 👍, если такой формат вам близок, или 👎, если хотелось чего-то другого.\n\n"
            "Сейчас перешлю анонс 👇"
        ),
    )

    assert 'href="https://telegra.ph/open-mic">стендап «Открытый микрофон»</a>' in text
    assert "ОТКРЫТЫЙ МИКРОФОН" not in text


def test_repost_intro_falls_back_when_llm_reply_has_no_event_placeholder():
    text = pf._repost_intro_text(
        "Вечер с музыкой",
        "камерный концерт подходит под выбранную тему",
        event_title="Камерный концерт",
        telegraph_url="https://telegra.ph/music",
        reply_template="Спасибо за голоса. Сейчас перешлю анонс 👇",
    )

    assert "Спасибо за голоса — берём тему" in text
    assert '<a href="https://telegra.ph/music">Камерный концерт</a>' in text


def test_repost_intro_separates_forward_line_in_llm_reply():
    text = pf._repost_intro_text(
        "Вечер с музыкой",
        "камерный концерт подходит под выбранную тему",
        event_title="Камерный концерт",
        telegraph_url="https://telegra.ph/music",
        reply_template=(
            "Спасибо за голоса — берём музыку.\n\n"
            "Для этой темы беру анонс {{EVENT_LINK}}. Подходит для спокойного вечера.\n\n"
            "Если попал — 👍, если нет — 👎.\n"
            "Сейчас перешлю анонс 👇"
        ),
    )

    assert "👎.\n\nСейчас перешлю анонс 👇" in text


def test_repost_intro_rejects_unsupported_outdoor_claim():
    text = pf._repost_intro_text(
        "Послушать музыку или сходить на фестиваль",
        "фестиваль продолжается в эти дни",
        event_title="Фестиваль Кантата",
        telegraph_url="https://telegra.ph/kantata",
        fact_context="Фестиваль Кантата\nконцерт\nсмешанная программа",
        reply_template=(
            "Спасибо, что проголосовали.\n\n"
            "Сегодня рекомендация такая: {{EVENT_LINK}}. Формат выступлений под открытым небом хорошо подходит на субботу.\n\n"
            "Если зашло — 👍, если нет — 👎.\n\n"
            "Сейчас перешлю анонс 👇"
        ),
    )

    assert "под открытым небом" not in text
    assert "Беру в рекомендацию анонс" in text


def test_repost_intro_rejects_event_link_after_colon_label():
    text = pf._repost_intro_text(
        "Бесплатно или спокойно отдохнуть",
        "турнир в баре подходит под выбранные темы",
        event_title="Шахматы в Краны и стаканы",
        telegraph_url="https://telegra.ph/chess",
        reply_template=(
            "Спасибо всем, кто проголосовал.\n\n"
            "Сегодня рекомендация такая: {{EVENT_LINK}}.\n\n"
            "Если зашло — 👍, если нет — 👎.\n\n"
            "Сейчас перешлю анонс 👇"
        ),
    )

    assert "Сегодня рекомендация такая" not in text
    assert "Беру в рекомендацию анонс" in text


def test_repost_intro_rejects_on_this_placeholder_pattern():
    text = pf._repost_intro_text(
        "Музыка",
        "подходит под тему",
        event_title="Фестиваль Кантата",
        telegraph_url="https://telegra.ph/kantata",
        reply_template=(
            "Спасибо, что проголосовали.\n\n"
            "Большинство выбрало музыку, поэтому остановимся на этом: {{EVENT_LINK}}.\n\n"
            "Если зашло — 👍, если нет — 👎.\n\n"
            "Сейчас перешлю анонс 👇"
        ),
    )

    assert "остановимся на этом" not in text
    assert "Беру в рекомендацию анонс" in text


def test_repost_intro_rejects_fake_request_merging_for_single_winner():
    text = pf._repost_intro_text(
        "Послушать музыку или узнать новое",
        "концерт подходит под выбранный вариант",
        event_title='Литературно-музыкальная программа «Россия в сердце, песня в душе»',
        telegraph_url="https://telegra.ph/music",
        tied_texts=["Послушать музыку или узнать новое"],
        reply_template=(
            "Спасибо всем, кто проголосовал! Большинство из вас захотело и послушать музыку, "
            "и узнать что-то новое, поэтому я решил объединить эти запросы.\n\n"
            "В качестве подходящего варианта выбрал концерт {{EVENT_LINK}}.\n\n"
            "Ставьте 👍, если вам по душе такой досуг, или 👎, если не зашло.\n\n"
            "Сейчас перешлю анонс 👇"
        ),
    )

    assert "объединить эти запросы" not in text
    assert "Большинство из вас захотело и послушать музыку" not in text
    assert "Спасибо за голоса — берём тему" in text


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


def _event_for_date(
    event_id: int,
    *,
    title: str,
    post_id: int,
    date: str,
    end_date: str | None = None,
    event_type: str = "концерт",
) -> Event:
    event = _event(event_id, title=title, post_id=post_id, event_type=event_type)
    event.date = date
    event.end_date = end_date
    return event


def _candidate(
    event_id: int,
    *,
    title: str | None = None,
    event_type: str = "концерт",
    is_free: bool = False,
) -> pf.CandidateEvent:
    return pf.CandidateEvent(
        id=event_id,
        title=title or f"Событие {event_id}",
        date="2026-06-13",
        end_date=None,
        time="12:00",
        event_type=event_type,
        festival=None,
        city="Калининград",
        location_name="Площадка",
        is_free=is_free,
        tg_event_post_id=500 + event_id,
        tg_event_post_url=f"https://t.me/kldevents/{500 + event_id}",
        telegraph_url=f"https://telegra.ph/event-{event_id}",
        summary="Короткое описание.",
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
    assert "Сегодня вечером подберу рекомендацию на завтра. Давайте выберем тип события вместе." in pf.DEFAULT_POLL_QUESTION_VARIANTS
    banned = (
        "план звучит",
        "звучит лучше",
        "общему выбору",
        "лучшие события",
        "алгоритм",
        "найду",
        "искать",
        "по настроению",
        "куда тянет",
        "что завтра порекомендовать",
        "завтра сделать рекомендацию",
    )
    for text in pf.DEFAULT_POLL_QUESTION_VARIANTS:
        lowered = text.lower()
        assert not any(fragment in lowered for fragment in banned)
        assert "вечером" in lowered
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
                "выбира",
                "тема",
                "тему",
                "темы",
                "темати",
                "тип события",
            )
        )
        assert any(
            marker in lowered
            for marker in ("рекоменд", "посовет", "подбер", "покажу", "выбер", "выбира", "подсвет")
        )


def test_poll_question_rotation_does_not_repeat_adjacent_debug_slots():
    previous = None
    for hour in range(24):
        text = pf._poll_question_text(f"debug:2026-06-12T{hour:02d}")
        if previous is not None:
            assert text != previous
        previous = text


def test_poll_question_reacts_to_previous_feedback_other():
    text = pf._poll_question_with_feedback_hint(
        "Сегодня вечером подберу рекомендацию на завтра.",
        previous_feedback_other=True,
    )

    lowered = text.casefold()
    assert "в прошлый раз темы не попали" in lowered
    assert "другое" in lowered


def test_production_min_vote_threshold_grows_weekly(monkeypatch):
    monkeypatch.delenv("POLL_TO_FORWARD_PROD_MIN_VOTES_BASE", raising=False)
    monkeypatch.delenv("POLL_TO_FORWARD_PROD_MIN_VOTES_START_DATE", raising=False)

    assert pf.production_min_vote_threshold(pf.PROD_MIN_VOTES_START_DATE) == 10
    assert pf.production_min_vote_threshold(pf.PROD_MIN_VOTES_START_DATE.replace(day=18)) == 10
    assert pf.production_min_vote_threshold(pf.PROD_MIN_VOTES_START_DATE.replace(day=19)) == 11
    assert pf.production_min_vote_threshold(pf.PROD_MIN_VOTES_START_DATE.replace(day=26)) == 12
    assert pf.min_vote_threshold_for_profile(pf.PROFILE_PROD, pf.PROD_MIN_VOTES_START_DATE.replace(day=26)) == 12
    assert pf.min_vote_threshold_for_profile(pf.PROFILE_DEBUG, pf.PROD_MIN_VOTES_START_DATE.replace(day=26)) == 1


@pytest.mark.asyncio
async def test_load_eligible_events_excludes_ongoing_start_date_posts(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(
            _event_for_date(
                101,
                title="Фестиваль со старым стартовым постом",
                post_id=501,
                date="2026-06-12",
                end_date="2026-06-16",
                event_type="фестиваль",
            )
        )
        session.add(
            _event_for_date(
                102,
                title="Завтрашний концерт",
                post_id=502,
                date="2026-06-13",
                event_type="концерт",
            )
        )
        await session.commit()

    events = await pf.load_eligible_events(
        db,
        target_date=datetime(2026, 6, 13, tzinfo=timezone.utc).date(),
        now_utc=datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
    )

    assert [event.id for event in events] == [102]
    await db.close()


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
    assert any(bot.sent_polls[0]["question"].startswith(text) for text in pf.DEFAULT_POLL_QUESTION_VARIANTS)
    assert "Другое" in bot.sent_polls[0]["question"]
    assert bot.sent_polls[0]["options"] == [
        "Вечер с музыкой",
        "Узнать новое",
        "С детьми",
        pf.FEEDBACK_OPTION_TEXT,
    ]
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, poll_message_id, resolve_after FROM poll_repost_run")
        status, poll_message_id, resolve_after = await cur.fetchone()
        assert (status, poll_message_id) == (pf.STATUS_OPEN, 101)
        assert resolve_after.endswith("08:30:00+00:00")
    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "previous_status",
    [pf.STATUS_OPEN, pf.STATUS_SKIPPED_NO_CANDIDATE, pf.STATUS_FAILED],
)
async def test_debug_create_waits_for_previous_visible_poll_result(tmp_path, monkeypatch, previous_status):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_events(db)
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")
    option = pf.PollOptionPlan(key="music", text="Вечер с музыкой", candidate_event_ids=(101,))
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_DEBUG,
        run_key="debug:2026-06-12T09",
        status=previous_status,
        target_event_date=datetime(2026, 6, 13, tzinfo=timezone.utc).date(),
        question_text="Предыдущий опрос",
        options=[option],
        poll_chat_id="@keniggpt",
        poll_message_id=99,
        poll_id="poll-99",
        resolve_after=datetime(2026, 6, 12, 7, 30, tzinfo=timezone.utc),
        error={"reason": "llm_unavailable"},
    )

    async def fail_llm(**_kwargs):
        raise AssertionError("new poll should not call LLM while previous visible poll has no result")

    monkeypatch.setattr(pf, "_google_generate_json", fail_llm)
    bot = DummyPollBot()

    result = await pf.create_debug_poll_if_due(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
    )

    assert result == {
        "created": False,
        "reason": "previous_poll_without_result",
        "run_key": "debug:2026-06-12T10",
        "previous_run_id": 1,
        "previous_run_key": "debug:2026-06-12T09",
        "previous_status": previous_status,
    }
    assert bot.sent_polls == []
    await db.close()


@pytest.mark.asyncio
async def test_debug_due_loader_tolerates_scheduler_milliseconds(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    option = pf.PollOptionPlan(key="music", text="Вечер с музыкой", candidate_event_ids=(101,))
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_DEBUG,
        run_key="debug:2026-06-12T10",
        status=pf.STATUS_OPEN,
        target_event_date=datetime(2026, 6, 13, tzinfo=timezone.utc).date(),
        question_text="Опрос",
        options=[option],
        poll_chat_id="@keniggpt",
        poll_message_id=101,
        poll_id="poll-101",
        resolve_after=datetime(2026, 6, 12, 8, 30, 0, 20_000, tzinfo=timezone.utc),
    )

    runs = await pf._load_open_due_runs(
        db,
        now_utc=datetime(2026, 6, 12, 8, 30, 0, tzinfo=timezone.utc),
    )

    assert [run["run_key"] for run in runs] == ["debug:2026-06-12T10"]
    await db.close()


@pytest.mark.asyncio
async def test_topic_planner_prompt_requests_free_option_when_possible(monkeypatch):
    events = [
        _candidate(101, title="Бесплатная лекция", event_type="лекция", is_free=True),
        _candidate(102, title="Бесплатная экскурсия", event_type="экскурсия", is_free=True),
        _candidate(103, title="Концерт", event_type="концерт"),
        _candidate(104, title="Мастер-класс", event_type="мастер-класс"),
        _candidate(105, title="Кино", event_type="кинопоказ"),
        _candidate(106, title="Выставка", event_type="выставка"),
    ]
    captured = {}

    async def fake_llm(**kwargs):
        captured["prompt"] = kwargs.get("prompt", "")
        return {"question_text": "", "options": []}

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)

    await pf._call_llm_topic_planner(events)

    assert "бесплат" in captured["prompt"].lower()
    assert "is_free=true" in captured["prompt"]
    assert "куда угодно, только бесплатно" in captured["prompt"]
    assert "минимум 6" in captured["prompt"]
    assert "не сжимай опрос до 5" in captured["prompt"]
    assert "Нужно 6-8 опций" in captured["prompt"]


@pytest.mark.asyncio
async def test_build_poll_plan_rejects_five_options_when_free_axis_is_possible(monkeypatch):
    events = [
        _candidate(101, is_free=True, event_type="лекция"),
        _candidate(102, is_free=True, event_type="экскурсия"),
        _candidate(103, event_type="концерт"),
        _candidate(104, event_type="мастер-класс"),
        _candidate(105, event_type="кинопоказ"),
        _candidate(106, event_type="выставка"),
    ]

    async def fake_llm(**_kwargs):
        return {
            "question_text": "",
            "options": [
                {"key": "free", "text": "Куда угодно, только бесплатно", "candidate_event_ids": [101, 102]},
                {"key": "music", "text": "Послушать музыку", "candidate_event_ids": [103]},
                {"key": "learn", "text": "Узнать что-то новое", "candidate_event_ids": [101, 104]},
                {"key": "cinema", "text": "Сходить в кино", "candidate_event_ids": [105]},
                {"key": "art", "text": "Посмотреть выставку", "candidate_event_ids": [106]},
            ],
        }

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)

    _question, options, strategy = await pf.build_poll_plan(events, min_options=3, run_key="debug:2026-06-12T10")

    assert options == []
    assert strategy == "llm_underfilled"


@pytest.mark.asyncio
async def test_debug_create_sends_six_options_when_free_axis_is_possible(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        free_lecture = _event(101, title="Бесплатная лекция", post_id=501, event_type="лекция")
        free_walk = _event(102, title="Бесплатная прогулка", post_id=502, event_type="экскурсия")
        free_lecture.is_free = True
        free_walk.is_free = True
        session.add_all(
            [
                free_lecture,
                free_walk,
                _event(103, title="Камерный концерт", post_id=503, event_type="концерт"),
                _event(104, title="Семейный праздник", post_id=504, event_type="детям"),
                _event(105, title="Кинопоказ", post_id=505, event_type="кинопоказ"),
                _event(106, title="Выставка", post_id=506, event_type="выставка"),
            ]
        )
        await session.commit()
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")

    async def fake_llm(**kwargs):
        prompt = kwargs.get("prompt", "")
        assert "Нужно 6-8 опций" in prompt
        return {
            "question_text": "",
            "options": [
                {"key": "free", "text": "Куда угодно, только бесплатно", "candidate_event_ids": [101, 102]},
                {"key": "music", "text": "Послушать музыку", "candidate_event_ids": [103]},
                {"key": "family", "text": "Выбраться с семьёй", "candidate_event_ids": [104]},
                {"key": "cinema", "text": "Сходить в кино", "candidate_event_ids": [105]},
                {"key": "art", "text": "Посмотреть выставку", "candidate_event_ids": [106]},
                {"key": "learn", "text": "Узнать что-то новое", "candidate_event_ids": [101]},
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
    assert len(bot.sent_polls[0]["options"]) == 7
    assert "Куда угодно, только бесплатно" in bot.sent_polls[0]["options"]
    assert pf.FEEDBACK_OPTION_TEXT in bot.sent_polls[0]["options"]
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT options_json, error_json FROM poll_repost_run")
        options_json, error_json = await cur.fetchone()
    assert len(json.loads(options_json)) == 7
    assert json.loads(error_json)["eligible_events"] == 6
    await db.close()


@pytest.mark.asyncio
async def test_topic_planner_reframes_after_feedback_other(monkeypatch):
    events = [_candidate(101), _candidate(102), _candidate(103)]
    captured = {}

    async def fake_llm(**kwargs):
        captured["prompt"] = kwargs["prompt"]
        return {
            "question_text": "",
            "options": [
                {"key": "one", "text": "Попробовать камерный вечер", "candidate_event_ids": [101]},
                {"key": "two", "text": "Выбрать что-то познавательное", "candidate_event_ids": [102]},
                {"key": "three", "text": "Сходить на живую программу", "candidate_event_ids": [103]},
            ],
        }

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)

    question, options, strategy = await pf.build_poll_plan(
        events,
        min_options=3,
        run_key="debug:2026-06-12T11",
        previous_feedback={
            "run_id": 1,
            "run_key": "debug:2026-06-12T10",
            "option_texts": ["Послушать музыку", "Узнать новое"],
        },
    )

    assert strategy == "llm"
    assert len(options) == 4
    assert "В прошлый раз темы не попали" in question
    assert "аудитория выбрала «Другое»" in captured["prompt"]
    assert "Послушать музыку" in captured["prompt"]
    assert "Узнать новое" in captured["prompt"]


@pytest.mark.asyncio
async def test_latest_feedback_other_context_reads_options_json(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_DEBUG,
        run_key="debug:2026-06-12T10",
        status=pf.STATUS_SKIPPED_FEEDBACK_OTHER,
        target_event_date=datetime(2026, 6, 13, tzinfo=timezone.utc).date(),
        options=[
            pf.PollOptionPlan(key="music", text="Послушать музыку", candidate_event_ids=(101, 102)),
            pf.PollOptionPlan(key="learn", text="Узнать новое", candidate_event_ids=(201,)),
            pf.PollOptionPlan(key=pf.FEEDBACK_OPTION_KEY, text=pf.FEEDBACK_OPTION_TEXT, candidate_event_ids=()),
        ],
    )

    context = await pf._latest_debug_feedback_other_context(db)

    assert context["run_key"] == "debug:2026-06-12T10"
    assert context["option_texts"] == ["Послушать музыку", "Узнать новое"]
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
        if "Ты пишешь публичный комментарий" in prompt:
            match = re.search(r'"winner_topic":\s*"(?P<topic>[^"]+)"', prompt)
            topic = match.group("topic") if match else "выбранную тему"
            if '"is_tie": true' in prompt:
                return {
                    "reply_text": (
                        f"Голоса разделились поровну, поэтому беру один анонс из темы «{topic}».\n\n"
                        "{{EVENT_LINK}} хорошо ложится в выбранную тему: можно выбраться завтра без ощущения случайного совета.\n\n"
                        "Если попал с рекомендацией — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.\n\n"
                        "Сейчас перешлю анонс 👇"
                    ),
                    "event_link_text": "этот анонс",
                }
            return {
                "reply_text": (
                    f"Спасибо за голоса — берём тему «{topic}».\n\n"
                    "{{EVENT_LINK}} хорошо ложится в выбранную тему: можно выбраться завтра без ощущения случайного совета.\n\n"
                    "Если попал с рекомендацией — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.\n\n"
                    "Сейчас перешлю анонс 👇"
                ),
                "event_link_text": "этот анонс",
            }
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
        resolved = await pf.resolve_due_debug_polls(
            db,
            bot,
            now_utc=now_utc + timedelta(minutes=31),
        )
        assert resolved["resolved"] == 1

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
        assert "Если попал с рекомендацией" in text
        assert "Сейчас перешлю анонс 👇" in text
        assert text.count("\n\n") == 3
        assert bot.messages[index]["parse_mode"] == "HTML"
        assert bot.messages[index]["disable_web_page_preview"] is True
    assert "Голоса разделились поровну" in bot.messages[3]["text"]
    await db.close()


@pytest.mark.asyncio
async def test_debug_resolve_replies_and_forwards_llm_choice(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_events(db)
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")

    async def fake_llm(**kwargs):
        prompt = kwargs.get("prompt", "")
        if "Ты пишешь публичный комментарий" in prompt:
            return {
                "reply_text": (
                    "Спасибо за голоса — сегодня берём «Вечер с музыкой».\n\n"
                    "Для этой темы выбрал {{EVENT_LINK}}: камерная музыка хорошо попадает в запрос на спокойный вечер.\n\n"
                    "Если попал с рекомендацией — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.\n\n"
                    "Сейчас перешлю анонс 👇"
                ),
                "event_link_text": "камерный концерт",
            }
        if "winner_key" in prompt:
            return {
                "winner_key": "music",
                "event_id": 101,
                "reason": "самый сильный концерт",
            }
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
    assert any(bot.sent_polls[0]["question"].startswith(text) for text in pf.DEFAULT_POLL_QUESTION_VARIANTS)
    assert "Другое" in bot.sent_polls[0]["question"]
    assert bot.messages[0]["reply_to_message_id"] == 101
    assert bot.messages[0]["text"] == (
        "Спасибо за голоса — сегодня берём «Вечер с музыкой».\n\n"
        'Для этой темы выбрал <a href="https://telegra.ph/event-101">камерный концерт</a>: '
        "камерная музыка хорошо попадает в запрос на спокойный вечер.\n\n"
        "Если попал с рекомендацией — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.\n\n"
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


@pytest.mark.asyncio
async def test_debug_resolve_feedback_other_winner_replies_without_forward(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_events(db)
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")

    async def fake_llm(**kwargs):
        prompt = kwargs.get("prompt", "")
        if "winner_key" in prompt or "Ты пишешь публичный комментарий" in prompt:
            raise AssertionError("feedback-only winner should not call winner/repost LLM")
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
    feedback_index = bot.sent_polls[0]["options"].index(pf.FEEDBACK_OPTION_TEXT)
    votes = [0] * len(bot.sent_polls[0]["options"])
    votes[feedback_index] = 2
    bot.stop_poll_result = SimpleNamespace(
        total_voter_count=2,
        options=[SimpleNamespace(voter_count=count) for count in votes],
    )

    result = await pf.resolve_due_debug_polls(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 8, 31, tzinfo=timezone.utc),
    )

    assert result["resolved"] == 1
    assert bot.forwarded == []
    assert bot.messages[0]["text"] == pf._feedback_other_reply_text()
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, winner_option_id, winner_text, chosen_event_id, reply_message_id, forwarded_message_id
            FROM poll_repost_run
            """
        )
        assert await cur.fetchone() == (
            pf.STATUS_SKIPPED_FEEDBACK_OTHER,
            pf.FEEDBACK_OPTION_KEY,
            pf.FEEDBACK_OPTION_TEXT,
            None,
            201,
            None,
        )

    # A feedback-only terminal result must not block the next hourly debug poll.
    async def second_llm(**kwargs):
        if "winner_key" in kwargs.get("prompt", ""):
            raise AssertionError("not resolving second poll here")
        return {
            "question_text": "Что выбрать на завтра?",
            "options": [
                {"key": "music", "text": "Вечер с музыкой", "candidate_event_ids": [101]},
                {"key": "learn", "text": "Узнать новое", "candidate_event_ids": [102]},
                {"key": "kids", "text": "С детьми", "candidate_event_ids": [103]},
            ],
        }

    monkeypatch.setattr(pf, "_google_generate_json", second_llm)
    second = await pf.create_debug_poll_if_due(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 9, 0, tzinfo=timezone.utc),
    )
    assert second["created"] is True
    await db.close()


@pytest.mark.asyncio
async def test_popularity_inventory_filters_single_candidate_options(monkeypatch):
    events = [
        pf.CandidateEvent(
            id=101,
            title="Концерт 1",
            date="2026-06-14",
            end_date=None,
            time="19:00",
            event_type="концерт",
            festival=None,
            city="Калининград",
            location_name="Зал",
            is_free=False,
            tg_event_post_id=501,
            tg_event_post_url="https://t.me/kldevents/501",
            telegraph_url="https://telegra.ph/101",
            summary="",
            popularity_score=10.0,
            popularity_group_key="kld:1",
        ),
        pf.CandidateEvent(
            id=102,
            title="Концерт 2",
            date="2026-06-14",
            end_date=None,
            time="20:00",
            event_type="концерт",
            festival=None,
            city="Калининград",
            location_name="Зал",
            is_free=False,
            tg_event_post_id=502,
            tg_event_post_url="https://t.me/kldevents/502",
            telegraph_url="https://telegra.ph/102",
            summary="",
            popularity_score=8.0,
            popularity_group_key="kld:2",
        ),
        pf.CandidateEvent(
            id=201,
            title="Единственная лекция",
            date="2026-06-14",
            end_date=None,
            time="18:00",
            event_type="лекция",
            festival=None,
            city="Калининград",
            location_name="Библиотека",
            is_free=True,
            tg_event_post_id=601,
            tg_event_post_url="https://t.me/kldevents/601",
            telegraph_url="https://telegra.ph/201",
            summary="",
            popularity_score=9.0,
            popularity_group_key="kld:3",
        ),
    ]

    async def fake_llm(**_kwargs):
        return {
            "question_text": "",
            "options": [
                {"key": "music", "text": "Послушать музыку", "candidate_event_ids": [101, 102]},
                {"key": "learn", "text": "Узнать новое", "candidate_event_ids": [201]},
                {"key": "free", "text": "Куда угодно, только бесплатно", "candidate_event_ids": [201]},
            ],
        }

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)

    _question, options, strategy = await pf.build_poll_plan(
        events,
        min_options=2,
        run_key="debug:2026-06-13T14",
    )

    assert strategy == "llm_underfilled"
    assert options == []


@pytest.mark.asyncio
async def test_popularity_winner_selection_stays_inside_winning_option_top3(monkeypatch):
    events = [
        pf.CandidateEvent(
            id=event_id,
            title=f"Событие {event_id}",
            date="2026-06-14",
            end_date=None,
            time="19:00",
            event_type="концерт",
            festival=None,
            city="Калининград",
            location_name="Зал",
            is_free=False,
            tg_event_post_id=500 + event_id,
            tg_event_post_url=f"https://t.me/kldevents/{500 + event_id}",
            telegraph_url=f"https://telegra.ph/{event_id}",
            summary="",
            popularity_score=score,
            popularity_group_key=f"kld:{event_id}",
            popularity_trace={
                "best_signal": {
                    "source": "kldevents_vk",
                    "above": ["views", "comments"],
                }
            },
        )
        for event_id, score in ((101, 12.0), (102, 10.0), (103, 8.0), (104, 1.0), (201, 20.0))
    ]
    tied_options = [
        pf.PollOptionPlan(key="music", text="Послушать музыку", candidate_event_ids=(101, 102, 103, 104)),
    ]

    async def fail_llm(**_kwargs):
        raise AssertionError("popularity path should not call LLM winner selection")

    monkeypatch.setattr(pf, "_google_generate_json", fail_llm)

    winner_option, chosen_id, reason = await pf._choose_winner_with_llm(
        tied_options=tied_options,
        events=events,
        target_date=datetime(2026, 6, 14, tzinfo=timezone.utc).date(),
    )

    assert winner_option == tied_options[0]
    assert chosen_id in {101, 102, 103}
    assert chosen_id != 104
    assert chosen_id != 201
    assert "kldevents" in reason


@pytest.mark.asyncio
async def test_popularity_top3_pick_varies_by_run_seed(monkeypatch):
    events = [
        pf.CandidateEvent(
            id=event_id,
            title=f"Событие {event_id}",
            date="2026-06-14",
            end_date=None,
            time="19:00",
            event_type="концерт",
            festival=None,
            city="Калининград",
            location_name="Зал",
            is_free=False,
            tg_event_post_id=500 + event_id,
            tg_event_post_url=f"https://t.me/kldevents/{500 + event_id}",
            telegraph_url=f"https://telegra.ph/{event_id}",
            summary="",
            popularity_score=score,
            popularity_group_key=f"kld:{event_id}",
            popularity_trace={
                "best_signal": {
                    "source": "kldevents_vk",
                    "above": ["likes"],
                }
            },
        )
        for event_id, score in ((101, 12.0), (102, 10.0), (103, 8.0), (104, 1.0))
    ]
    tied_options = [
        pf.PollOptionPlan(key="music", text="Послушать музыку", candidate_event_ids=(101, 102, 103, 104)),
    ]

    async def fail_llm(**_kwargs):
        raise AssertionError("popularity path should not call LLM winner selection")

    monkeypatch.setattr(pf, "_google_generate_json", fail_llm)

    chosen_ids = set()
    for idx in range(20):
        _option, chosen_id, _reason = await pf._choose_winner_with_llm(
            tied_options=tied_options,
            events=events,
            target_date=datetime(2026, 6, 14, tzinfo=timezone.utc).date(),
            selection_seed=f"run:{idx}",
        )
        chosen_ids.add(chosen_id)

    assert chosen_ids <= {101, 102, 103}
    assert 104 not in chosen_ids
    assert len(chosen_ids) >= 2
