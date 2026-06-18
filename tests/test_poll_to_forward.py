from __future__ import annotations

import json
import re
from dataclasses import replace
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
    time: str = "12:00",
    popularity_score: float = 0.0,
    popularity_group_key: str | None = None,
) -> pf.CandidateEvent:
    return pf.CandidateEvent(
        id=event_id,
        title=title or f"Событие {event_id}",
        date="2026-06-13",
        end_date=None,
        time=time,
        event_type=event_type,
        festival=None,
        city="Калининград",
        location_name="Площадка",
        is_free=is_free,
        tg_event_post_id=500 + event_id,
        tg_event_post_url=f"https://t.me/kldevents/{500 + event_id}",
        telegraph_url=f"https://telegra.ph/event-{event_id}",
        summary="Короткое описание.",
        popularity_score=popularity_score,
        popularity_group_key=popularity_group_key,
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
    assert (
        "Сегодня выбираем категорию событий, куда можно сходить завтра. "
        "Вечером возьму один анонс из варианта, за который будет больше голосов."
    ) in pf.DEFAULT_POLL_QUESTION_VARIANTS
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
        "что завтра",
        "завтра подсветить",
        "завтра показать",
        "что завтра порекомендовать",
        "завтра сделать рекомендацию",
        "завтра будет рекомендация",
        "завтра будет один",
        "завтрашней рекомендации",
        "для завтрашней рекомендации",
        "рекомендации на завтра",
        "рекомендацию на завтра",
        "разберусь с выбором",
    )
    for text in pf.DEFAULT_POLL_QUESTION_VARIANTS:
        lowered = text.lower()
        assert not any(fragment in lowered for fragment in banned)
        assert "вечером" in lowered
        assert "завтра" in lowered
        assert "событ" in lowered
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
            for marker in ("анонс", "покажу", "выбер", "выбира", "возьму")
        )
        assert pf._question_guard_rejection_reason(text) is None


def test_question_guard_rejects_current_ambiguous_prod_phrase():
    assert pf._question_guard_rejection_reason(
        "Голосуем за тему события на завтра. Если варианты не те — выбирайте «Другое», вечером разберусь с выбором."
    )
    assert pf._question_guard_rejection_reason(
        "Куда отправимся завтра? Голосуйте за категорию, а вечером я найду для вас одно классное мероприятие."
    )
    assert pf._question_guard_rejection_reason(
        "Голосуйте за категорию событий, а вечером я выберу самое крутое событие из лидирующей темы."
    )


@pytest.mark.asyncio
async def test_validated_question_regenerates_until_llm_reviewer_accepts(monkeypatch):
    monkeypatch.setenv("POLL_TO_FORWARD_QUESTION_LLM_REVIEW_ENABLED", "1")
    monkeypatch.setenv("POLL_TO_FORWARD_QUESTION_LLM_REVIEW_ATTEMPTS", "3")
    writer_calls = 0
    reviewer_questions = []

    async def fake_llm(**kwargs):
        nonlocal writer_calls
        prompt = kwargs.get("prompt", "")
        if "Ты пишешь вопрос для Telegram-опроса" in prompt:
            writer_calls += 1
            if writer_calls == 1:
                return {
                    "question_text": (
                        "Голосуем за тему события на завтра. Если варианты не те — "
                        "выбирайте «Другое», вечером разберусь с выбором."
                    )
                }
            assert "ambiguous_or_banned_phrase" in prompt
            return {
                "question_text": (
                    "Сегодня выбираем категорию событий, куда можно сходить завтра. "
                    "Вечером покажу один анонс из темы, за которую будет больше голосов. "
                    "Если темы не те — выбирайте «Другое»."
                )
            }
        if "Ты проверяешь вопрос Telegram-опроса" in prompt:
            reviewer_questions.append(prompt)
            return {"accepted": True, "reason": "понятно"}
        raise AssertionError(f"unexpected prompt: {prompt[:120]}")

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)

    question = await pf._validated_poll_question(run_key="prod:2026-06-15")

    assert writer_calls == 2
    assert len(reviewer_questions) == 1
    assert "куда можно сходить завтра" in question
    assert "Другое" in question
    assert "разберусь с выбором" not in question


def test_poll_question_rotation_does_not_repeat_adjacent_debug_slots():
    previous = None
    for hour in range(24):
        text = pf._poll_question_text(f"debug:2026-06-12T{hour:02d}")
        if previous is not None:
            assert text != previous
        previous = text


def test_popular_rich_inventory_requires_more_topic_options():
    events = []
    for idx in range(12):
        group_idx = min(idx, 9)
        events.append(
            pf.CandidateEvent(
                id=100 + idx,
                title=f"Событие {idx}",
                date="2026-06-14",
                end_date=None,
                time="12:00",
                event_type="концерт",
                festival=None,
                city="Калининград",
                location_name="Площадка",
                is_free=False,
                tg_event_post_id=500 + idx,
                tg_event_post_url=f"https://t.me/kldevents/{500 + idx}",
                telegraph_url=f"https://telegra.ph/{idx}",
                summary="",
                popularity_score=5.0,
                popularity_group_key=f"kld:{group_idx}",
            )
        )

    assert pf._effective_min_options(events, 3) == 5


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
        profile_key=pf.PROFILE_DEBUG,
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
        _candidate(107, title="Встреча во дворе", event_type="встреча"),
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
async def test_build_poll_plan_keeps_fallback_conservative_on_fragmented_inventory(monkeypatch):
    events = [
        _candidate(101, is_free=True, event_type="лекция"),
        _candidate(102, is_free=True, event_type="экскурсия"),
        _candidate(103, event_type="концерт"),
        _candidate(104, event_type="мастер-класс"),
        _candidate(105, event_type="кинопоказ"),
        _candidate(106, event_type="выставка"),
        _candidate(107, event_type="встреча"),
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

    assert strategy == "llm_underfilled"
    assert options == []


@pytest.mark.asyncio
async def test_build_poll_plan_skips_instead_of_overmerging_sparse_popular_inventory(monkeypatch):
    events = [
        _candidate(
            5749,
            title="Заключительный гала-концерт фестиваля «Кантата»",
            event_type="концерт",
            popularity_score=9.6,
            popularity_group_key="kld:5749",
        ),
        _candidate(
            6041,
            title="Лекция «Кант о нравственности, вере и религии»",
            event_type="лекция",
            is_free=True,
            time="18:30",
            popularity_score=1.0,
            popularity_group_key="src:6041",
        ),
        _candidate(
            5843,
            title='Концерт "Музыка Времён"',
            event_type="кинопоказ",
            time="19:00",
            popularity_score=8.0,
            popularity_group_key="kld:5843",
        ),
        _candidate(
            6023,
            title="Открытая встреча Ассоциации сообществ КлубОК",
            event_type="встреча",
            time="19:00",
            popularity_score=8.0,
            popularity_group_key="kld:6023",
        ),
        _candidate(
            6031,
            title="Заключительная игра интеллектуального клуба",
            event_type="встреча",
            time="19:00",
            popularity_score=2.0,
            popularity_group_key="src:6031",
        ),
    ]

    async def fake_llm(**kwargs):
        if "Ты пишешь вопрос для Telegram-опроса" in kwargs.get("prompt", ""):
            return {
                "question_text": (
                    "Сегодня выбираем категорию событий, куда можно сходить завтра. "
                    "Вечером возьму один анонс из варианта, за который будет больше голосов."
                )
            }
        if "Ты проверяешь вопрос Telegram-опроса" in kwargs.get("prompt", ""):
            return {"accepted": True, "reason": "понятно"}
        return {"question_text": "", "options": []}

    monkeypatch.setenv("POLL_TO_FORWARD_QUESTION_LLM_REVIEW_ENABLED", "1")
    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)

    question, options, strategy = await pf.build_poll_plan(
        events,
        min_options=3,
        run_key="debug:2026-06-15T20",
    )

    assert strategy == "llm_underfilled"
    assert "куда можно сходить завтра" in question
    assert options == []


@pytest.mark.asyncio
async def test_build_poll_plan_uses_fallback_only_for_coherent_multi_candidate_topics(monkeypatch):
    events = [
        _candidate(101, title="Камерный концерт", event_type="концерт", popularity_score=8, popularity_group_key="a"),
        _candidate(102, title="Джазовый концерт", event_type="концерт", popularity_score=7, popularity_group_key="b"),
        _candidate(103, title="Выставка графики", event_type="выставка", popularity_score=6, popularity_group_key="c"),
        _candidate(104, title="Выставка живописи", event_type="выставка", popularity_score=5, popularity_group_key="d"),
        _candidate(105, title="Лекция о Канте", event_type="лекция", popularity_score=4, popularity_group_key="e"),
        _candidate(106, title="Встреча книжного клуба", event_type="встреча", popularity_score=3, popularity_group_key="f"),
    ]

    async def fake_llm(**kwargs):
        if "Ты пишешь вопрос для Telegram-опроса" in kwargs.get("prompt", ""):
            return {
                "question_text": (
                    "Сегодня выбираем категорию событий, куда можно сходить завтра. "
                    "Вечером возьму один анонс из варианта, за который будет больше голосов."
                )
            }
        if "Ты проверяешь вопрос Telegram-опроса" in kwargs.get("prompt", ""):
            return {"accepted": True, "reason": "понятно"}
        return {"question_text": "", "options": []}

    monkeypatch.setenv("POLL_TO_FORWARD_QUESTION_LLM_REVIEW_ENABLED", "1")
    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)

    question, options, strategy = await pf.build_poll_plan(events, min_options=3, run_key="debug:coherent")

    assert strategy == "fallback_topics"
    assert "куда можно сходить завтра" in question
    content_options = [option for option in options if option.key != pf.FEEDBACK_OPTION_KEY]
    assert [option.key for option in content_options] == ["music_concert", "art_exhibition", "lecture_meeting"]
    assert all(len(option.candidate_event_ids) >= 2 for option in content_options)
    assert pf.FEEDBACK_OPTION_TEXT in [option.text for option in options]


@pytest.mark.asyncio
async def test_create_poll_relaxes_popularity_filter_when_raw_inventory_is_sufficient(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        for idx, event_type in enumerate(("концерт", "лекция", "встреча", "кинопоказ", "экскурсия"), start=101):
            session.add(_event(idx, title=f"Событие {idx}", post_id=500 + idx, event_type=event_type))
        await session.commit()

    async def fake_popularity(_db, events, **_kwargs):
        popular = [
            replace(events[0], popularity_score=10.0, popularity_group_key="kld:101"),
            replace(events[1], popularity_score=8.0, popularity_group_key="kld:102"),
        ]
        return popular, {"source_metric_rows": 2, "popular_events": 2, "eligible_before_popularity": len(events)}

    async def fake_llm(**kwargs):
        prompt = kwargs.get("prompt", "")
        if "Ты пишешь вопрос для Telegram-опроса" in prompt:
            return {
                "question_text": (
                    "Сегодня выбираем категорию событий, куда можно сходить завтра. "
                    "Вечером возьму один анонс из варианта, за который будет больше голосов."
                )
            }
        if "Ты проверяешь вопрос Telegram-опроса" in prompt:
            return {"accepted": True, "reason": "понятно"}
        return {
            "question_text": "",
            "options": [
                {"key": "music", "text": "Послушать музыку", "candidate_event_ids": [101, 103]},
                {"key": "learn", "text": "Узнать новое", "candidate_event_ids": [102, 105]},
                {"key": "walk", "text": "Выбраться на прогулку", "candidate_event_ids": [104, 105]},
            ],
        }

    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")
    monkeypatch.setenv("POLL_TO_FORWARD_QUESTION_LLM_REVIEW_ENABLED", "1")
    monkeypatch.setattr(pf, "_apply_popularity_preflight", fake_popularity)
    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)
    bot = DummyPollBot()

    result = await pf.create_debug_poll_if_due(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
    )

    assert result["created"] is True
    assert bot.sent_polls
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT error_json FROM poll_repost_run")
        error = json.loads((await cur.fetchone())[0])
    assert error["popularity"]["popularity_filter_relaxed"] is True
    assert error["strategy"] == "llm"
    await db.close()


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
                _event(107, title="Встреча во дворе", post_id=507, event_type="встреча"),
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
    assert json.loads(error_json)["eligible_events"] == 7
    await db.close()


@pytest.mark.asyncio
async def test_load_eligible_events_skips_started_or_near_start_events(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        too_soon = _event(101, title="Уже скоро", post_id=501)
        too_soon.date = "2026-06-13"
        too_soon.time = "20:00"
        future = _event(102, title="Завтра нормально", post_id=502)
        future.date = "2026-06-14"
        future.time = "10:00"
        session.add_all([too_soon, future])
        await session.commit()

    events = await pf.load_eligible_events(
        db,
        target_date=datetime(2026, 6, 13, tzinfo=timezone.utc).date(),
        now_utc=datetime(2026, 6, 13, 17, 30, tzinfo=timezone.utc),
    )

    assert [event.id for event in events] == []

    tomorrow_events = await pf.load_eligible_events(
        db,
        target_date=datetime(2026, 6, 14, tzinfo=timezone.utc).date(),
        now_utc=datetime(2026, 6, 13, 17, 30, tzinfo=timezone.utc),
    )

    assert [event.id for event in tomorrow_events] == [102]
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

    context = await pf._latest_feedback_other_context(db, profile_key=pf.PROFILE_DEBUG)

    assert context["run_key"] == "debug:2026-06-12T10"
    assert context["option_texts"] == ["Послушать музыку", "Узнать новое"]
    await db.close()


@pytest.mark.asyncio
async def test_latest_feedback_other_context_reads_significant_prod_other_votes(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.delenv("POLL_TO_FORWARD_FEEDBACK_SIGNAL_MIN_VOTES", raising=False)
    monkeypatch.delenv("POLL_TO_FORWARD_FEEDBACK_SIGNAL_MIN_SHARE", raising=False)
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_PROD,
        run_key="prod:2026-06-18",
        status=pf.STATUS_FORWARDED,
        target_event_date=datetime(2026, 6, 18, tzinfo=timezone.utc).date(),
        poll_chat_id="@kenigevents",
        poll_message_id=4052,
        options=[
            pf.PollOptionPlan(key="music", text="Послушать музыку", candidate_event_ids=(101, 102)),
            pf.PollOptionPlan(key="family", text="Выбраться всей семьёй", candidate_event_ids=(201, 202)),
            pf.PollOptionPlan(key=pf.FEEDBACK_OPTION_KEY, text=pf.FEEDBACK_OPTION_TEXT, candidate_event_ids=()),
        ],
    )
    await pf._update_run(
        db,
        1,
        status=pf.STATUS_FORWARDED,
        result={
            "total_voter_count": 11,
            "options": [
                {"index": 0, "key": "music", "text": "Послушать музыку", "voter_count": 6},
                {"index": 1, "key": "family", "text": "Выбраться всей семьёй", "voter_count": 3},
                {
                    "index": 2,
                    "key": pf.FEEDBACK_OPTION_KEY,
                    "text": pf.FEEDBACK_OPTION_TEXT,
                    "voter_count": 2,
                },
            ],
        },
        forwarded_message_id=4055,
    )

    context = await pf._latest_feedback_other_context(db, profile_key=pf.PROFILE_PROD)

    assert context["run_key"] == "prod:2026-06-18"
    assert context["strength"] == "partial"
    assert context["feedback_votes"] == 2
    assert context["feedback_share"] == pytest.approx(0.1818, abs=0.001)
    assert context["option_texts"] == ["Послушать музыку", "Выбраться всей семьёй"]
    await db.close()


@pytest.mark.asyncio
async def test_create_prod_poll_passes_previous_other_feedback_to_planner(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_PROD,
        run_key="prod:2026-06-18",
        status=pf.STATUS_FORWARDED,
        target_event_date=datetime(2026, 6, 18, tzinfo=timezone.utc).date(),
        poll_chat_id="@kenigevents",
        poll_message_id=4052,
        options=[
            pf.PollOptionPlan(key="music", text="Послушать музыку", candidate_event_ids=(101, 102)),
            pf.PollOptionPlan(key=pf.FEEDBACK_OPTION_KEY, text=pf.FEEDBACK_OPTION_TEXT, candidate_event_ids=()),
        ],
    )
    await pf._update_run(
        db,
        1,
        status=pf.STATUS_FORWARDED,
        result={
            "total_voter_count": 10,
            "options": [
                {"index": 0, "key": "music", "text": "Послушать музыку", "voter_count": 8},
                {
                    "index": 1,
                    "key": pf.FEEDBACK_OPTION_KEY,
                    "text": pf.FEEDBACK_OPTION_TEXT,
                    "voter_count": 2,
                },
            ],
        },
        forwarded_message_id=4055,
    )
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_PROD", "1")
    monkeypatch.setenv("POLL_TO_FORWARD_PROD_POLL_TIME_LOCAL", "16:00")
    captured = {}

    async def fake_create_poll_if_due(*args, **kwargs):
        captured["previous_feedback"] = kwargs.get("previous_feedback")
        return {"created": False, "reason": "captured"}

    monkeypatch.setattr(pf, "_create_poll_if_due", fake_create_poll_if_due)

    result = await pf.create_prod_poll_if_due(
        db,
        DummyPollBot(),
        now_utc=datetime(2026, 6, 18, 14, 0, tzinfo=timezone.utc),
    )

    assert result["reason"] == "captured"
    assert captured["previous_feedback"]["profile_key"] == pf.PROFILE_PROD
    assert captured["previous_feedback"]["strength"] == "partial"
    assert captured["previous_feedback"]["feedback_votes"] == 2
    await db.close()


@pytest.mark.asyncio
async def test_operator_invalidated_visible_poll_does_not_block_next_slot(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_DEBUG,
        run_key="debug:2026-06-15T20-hotfix-a943e5af",
        status=pf.STATUS_SKIPPED_TOPIC_UNDERFILL,
        target_event_date=datetime(2026, 6, 16, tzinfo=timezone.utc).date(),
        poll_chat_id="@keniggpt",
        poll_message_id=2333,
        options=[
            pf.PollOptionPlan(key="music", text="Послушать музыку", candidate_event_ids=(101, 102)),
            pf.PollOptionPlan(key=pf.FEEDBACK_OPTION_KEY, text=pf.FEEDBACK_OPTION_TEXT, candidate_event_ids=()),
        ],
        error={"invalidated_reason": "overmerged_fallback_topics_after_product_review"},
    )

    assert await pf._latest_visible_poll_without_result(db, profile_key=pf.PROFILE_DEBUG) is None
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
async def test_feedback_other_tie_is_signal_but_not_candidate_for_winner_llm(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_events(db)
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_DEBUG", "1")
    captured_tied_keys = []

    async def fake_llm(**kwargs):
        prompt = kwargs.get("prompt", "")
        if "Ты пишешь публичный комментарий" in prompt:
            return {
                "reply_text": (
                    "Спасибо за голоса — беру один анонс из выбранных тем.\n\n"
                    "Для этой темы подходит {{EVENT_LINK}}.\n\n"
                    "Если попал с рекомендацией — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.\n\n"
                    "Сейчас перешлю анонс 👇"
                ),
                "event_link_text": "камерный концерт",
            }
        if "winner_key" in prompt:
            match = re.search(
                r"Опции-победители/ничья:\n(?P<options>.*?)\n\nСобытия-кандидаты:",
                prompt,
                flags=re.DOTALL,
            )
            tied_options = json.loads(match.group("options")) if match else []
            captured_tied_keys.extend(option["key"] for option in tied_options)
            assert pf.FEEDBACK_OPTION_KEY not in captured_tied_keys
            return {
                "winner_key": tied_options[0]["key"],
                "event_id": tied_options[0]["candidate_event_ids"][0],
                "reason": "выбрана реальная тема, а «Другое» учтём как сигнал",
            }
        return {
            "question_text": "",
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
    votes[0] = 2
    votes[1] = 2
    votes[feedback_index] = 2
    bot.stop_poll_result = SimpleNamespace(
        total_voter_count=sum(votes),
        options=[SimpleNamespace(voter_count=count) for count in votes],
    )

    result = await pf.resolve_due_debug_polls(
        db,
        bot,
        now_utc=datetime(2026, 6, 12, 8, 31, tzinfo=timezone.utc),
    )

    assert result["resolved"] == 1
    assert pf.FEEDBACK_OPTION_KEY not in captured_tied_keys
    assert bot.forwarded
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, result_json FROM poll_repost_run")
        status, result_json = await cur.fetchone()
    assert status == pf.STATUS_FORWARDED
    feedback_signal = json.loads(result_json)["feedback_other_signal"]
    assert feedback_signal["significant"] is True
    assert feedback_signal["tied_top"] is True
    assert feedback_signal["reason"] == "feedback_other_tied_top"
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


@pytest.mark.asyncio
async def test_prod_create_uses_kenigevents_and_1955_result_slot(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        for idx, (event_type, title) in enumerate(
            [
                ("концерт", "Концерт у моря"),
                ("лекция", "Лекция о городе"),
                ("мастер-класс", "Керамическая мастерская"),
                ("экскурсия", "Прогулка по району"),
                ("выставка", "Новая выставка"),
            ],
            start=1,
        ):
            session.add(
                _event_for_date(
                    900 + idx,
                    title=title,
                    post_id=1900 + idx,
                    date="2026-06-15",
                    event_type=event_type,
                )
            )
        await session.commit()
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_PROD", "1")
    monkeypatch.setenv("POLL_TO_FORWARD_PROD_TARGET_CHAT", "@kenigevents")
    monkeypatch.setenv("POLL_TO_FORWARD_PROD_POLL_TIME_LOCAL", "16:00")
    monkeypatch.setenv("POLL_TO_FORWARD_PROD_RESULT_TIME_LOCAL", "19:55")
    monkeypatch.setenv("POLL_TO_FORWARD_PROD_MIN_ELIGIBLE_EVENTS", "5")
    monkeypatch.setenv("POLL_TO_FORWARD_PROD_MIN_OPTIONS", "4")

    async def fake_llm(**kwargs):
        if "winner_key" in kwargs.get("prompt", ""):
            raise AssertionError("only creating poll")
        return {
            "question_text": "",
            "options": [
                {"key": "music", "text": "Послушать музыку", "candidate_event_ids": [901]},
                {"key": "learn", "text": "Узнать новое", "candidate_event_ids": [902]},
                {"key": "hands", "text": "Сделать что-то руками", "candidate_event_ids": [903]},
                {"key": "walk", "text": "Погулять и посмотреть город", "candidate_event_ids": [904]},
            ],
        }

    monkeypatch.setattr(pf, "_google_generate_json", fake_llm)
    bot = DummyPollBot()

    result = await pf.create_prod_poll_if_due(
        db,
        bot,
        now_utc=datetime(2026, 6, 14, 14, 0, tzinfo=timezone.utc),
    )

    assert result["created"] is True
    assert result["profile_key"] == pf.PROFILE_PROD
    assert bot.sent_polls[0]["chat_id"] == "@kenigevents"
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT profile_key, run_key, target_event_date, poll_chat_id, resolve_after FROM poll_repost_run"
        )
        row = await cur.fetchone()
    assert row == (
        pf.PROFILE_PROD,
        "prod:2026-06-15",
        "2026-06-15",
        "@kenigevents",
        "2026-06-14T17:55:00+00:00",
    )
    await db.close()


@pytest.mark.asyncio
async def test_prod_resolve_low_votes_posts_public_result_without_forward(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("ENABLE_POLL_TO_FORWARD_PROD", "1")
    monkeypatch.setenv("POLL_TO_FORWARD_PROD_MIN_VOTES_BASE", "10")
    option = pf.PollOptionPlan(key="music", text="Послушать музыку", candidate_event_ids=(101,))
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_PROD,
        run_key="prod:2026-06-15",
        status=pf.STATUS_OPEN,
        target_event_date=datetime(2026, 6, 15, tzinfo=timezone.utc).date(),
        question_text="Опрос",
        options=[option],
        poll_chat_id="@kenigevents",
        poll_message_id=101,
        poll_id="poll-101",
        resolve_after=datetime(2026, 6, 14, 17, 55, tzinfo=timezone.utc),
    )
    bot = DummyPollBot()
    bot.stop_poll_result = SimpleNamespace(
        total_voter_count=3,
        options=[SimpleNamespace(voter_count=3)],
    )

    result = await pf.resolve_due_prod_polls(
        db,
        bot,
        now_utc=datetime(2026, 6, 14, 17, 55, tzinfo=timezone.utc),
    )

    assert result["resolved"] == 1
    assert bot.forwarded == []
    assert bot.messages[0]["chat_id"] == "@kenigevents"
    assert bot.messages[0]["reply_to_message_id"] == 101
    assert "голос" in bot.messages[0]["text"]
    assert "без анонса" in bot.messages[0]["text"]
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, reply_message_id, forwarded_message_id FROM poll_repost_run")
        row = await cur.fetchone()
    assert row == (pf.STATUS_SKIPPED_NO_VOTES, 201, None)
    await db.close()


@pytest.mark.asyncio
async def test_due_loader_keeps_debug_and_prod_profiles_isolated(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    option = pf.PollOptionPlan(key="music", text="Послушать музыку", candidate_event_ids=(101,))
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_DEBUG,
        run_key="debug:2026-06-14T16",
        status=pf.STATUS_OPEN,
        target_event_date=datetime(2026, 6, 15, tzinfo=timezone.utc).date(),
        options=[option],
        poll_chat_id="@keniggpt",
        poll_message_id=101,
        resolve_after=datetime(2026, 6, 14, 14, 30, tzinfo=timezone.utc),
    )
    await pf._insert_run(
        db,
        profile_key=pf.PROFILE_PROD,
        run_key="prod:2026-06-15",
        status=pf.STATUS_OPEN,
        target_event_date=datetime(2026, 6, 15, tzinfo=timezone.utc).date(),
        options=[option],
        poll_chat_id="@kenigevents",
        poll_message_id=201,
        resolve_after=datetime(2026, 6, 14, 17, 55, tzinfo=timezone.utc),
    )

    debug_runs = await pf._load_open_due_runs(
        db,
        now_utc=datetime(2026, 6, 14, 18, 0, tzinfo=timezone.utc),
        profile_key=pf.PROFILE_DEBUG,
    )
    prod_runs = await pf._load_open_due_runs(
        db,
        now_utc=datetime(2026, 6, 14, 18, 0, tzinfo=timezone.utc),
        profile_key=pf.PROFILE_PROD,
    )

    assert [run["run_key"] for run in debug_runs] == ["debug:2026-06-14T16"]
    assert [run["run_key"] for run in prod_runs] == ["prod:2026-06-15"]
    await db.close()
