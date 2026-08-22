from types import SimpleNamespace
import json
import io

import pytest
from PIL import Image
from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession

import main
from models import EventSource, PromoActivity, PromoCampaign, PromoTarget
from promo import PROMO_SURFACE_TG_BUTTON_HIGHLIGHT, PROMO_TG_BUTTON_HIGHLIGHT_PROFILE


class DummyTgBot:
    def __init__(self):
        self.messages = []
        self.photos = []
        self.media_groups = []
        self.rich_messages = []
        self.deleted = []
        self.reply_markup_edits = []
        self._next_id = 100

    async def send_message(self, chat_id, text, **kwargs):
        self.messages.append((chat_id, text, kwargs))
        self._next_id += 1
        return SimpleNamespace(
            message_id=self._next_id,
            chat=SimpleNamespace(id=-1001234567890),
        )

    async def send_photo(self, chat_id, photo, **kwargs):
        self.photos.append((chat_id, photo, kwargs))
        self._next_id += 1
        return SimpleNamespace(
            message_id=self._next_id,
            chat=SimpleNamespace(id=-1001234567890),
        )

    async def send_media_group(self, chat_id, media):
        self.media_groups.append((chat_id, media))
        sent = []
        for _item in media:
            self._next_id += 1
            sent.append(
                SimpleNamespace(
                    message_id=self._next_id,
                    chat=SimpleNamespace(id=-1001234567890),
                )
            )
        return sent

    async def send_rich_message(self, chat_id, rich_message, **kwargs):
        self.rich_messages.append((chat_id, rich_message, kwargs))
        self._next_id += 1
        return SimpleNamespace(
            message_id=self._next_id,
            chat=SimpleNamespace(id=-1001234567890),
        )

    async def edit_message_text(self, **_kwargs):
        raise AssertionError("edit should not be called in first publish")

    async def edit_message_caption(self, **_kwargs):
        raise AssertionError("edit should not be called in first publish")

    async def edit_message_reply_markup(self, *args, **kwargs):
        self.reply_markup_edits.append((args, kwargs))

    async def delete_message(self, **kwargs):
        self.deleted.append(kwargs)


def _patch_media_materializer(monkeypatch):
    calls = []

    async def fake_materialize(url, index):
        calls.append((url, index))
        return main.types.BufferedInputFile(
            f"image-bytes-{index}".encode(),
            filename=f"{index}.jpg",
        )

    monkeypatch.setattr(main, "materialize_tg_event_media_for_upload", fake_materialize)
    return calls


def _event(**kwargs) -> main.Event:
    data = {
        "id": 42,
        "title": "Камерный концерт",
        "description": "",
        "date": "2026-06-20",
        "time": "19:00",
        # Keep the default fixture outside the curated medallion catalog; tests
        # that exercise RichMessage medallions opt into a concrete identity.
        "location_name": "Концертный зал",
        "location_address": "Ленинский проспект 155",
        "city": "Калининград",
        "source_text": "source",
        "ticket_link": "https://example.com/tickets",
        "ticket_price_min": 500,
        "telegraph_url": "https://telegra.ph/event",
        "photo_urls": [],
    }
    data.update(kwargs)
    return main.Event(**data)


def test_build_tg_event_announcement_formats_links_hashtags_and_footer():
    event = _event(festival="Фестиваль света")

    text = main.build_tg_event_announcement(
        event,
        "Небольшое описание с **важной деталью**.",
    )

    assert text.startswith("<b>Камерный концерт</b>")
    assert "КАМЕРНЫЙ КОНЦЕРТ" not in text
    assert '🎫 <a href="https://example.com/tickets">Билеты</a> 💰 500' in text
    assert "руб." not in text
    assert "Калининград" in text
    assert "Ленинский проспект 155, #Калининград" in text
    assert text.count("#Калининград") == 1
    assert "#афишакалининград" in text
    assert "#концерт" in text
    assert "#20июня" in text
    assert "#Фестивальсвета" in text
    assert "#анонс" not in text
    assert (
        '<a href="https://telegra.ph/event">🔎 Подробнее</a>            '
        '<a href="https://max.ru/channel_kenigevents">Max</a> · '
        '<a href="https://vk.ru/im/channels/-239844596">Вконтакте</a>'
    ) in text
    assert "Подписаться" not in text


def test_build_tg_event_announcement_does_not_repeat_city_as_venue() -> None:
    event = _event(
        location_name="Янтарный",
        location_address=None,
        city="Янтарный",
    )

    text = main.build_tg_event_announcement(event, "Описание.")

    assert "📍 Янтарный" in text
    assert "📍 Янтарный, #Янтарный" not in text


def test_tg_event_hashtag_line_drops_long_title_like_festival_slug():
    event = _event(
        festival="По одёжке встречают: Народный костюм, традиции и смыслы",
        is_free=True,
        ticket_link=None,
        ticket_price_min=None,
        ticket_price_max=None,
        date="2026-07-01",
        title="Лекция «Чулки и носки»",
        description="Лекция и встреча о народном костюме.",
    )

    hashtag_line = main._tg_event_hashtag_line(event)

    assert "#1июля" in hashtag_line
    assert "#1_июля" in hashtag_line
    assert "#афишакалининград" in hashtag_line
    assert "#лекция" in hashtag_line
    assert "#встреча" in hashtag_line
    assert "#бесплатно" in hashtag_line
    assert "#ПоодёжкевстречаютНародныйкостюмтрадицииисмыслы" not in hashtag_line
    assert all(len(tag.lstrip("#")) <= main.TG_EVENT_HASHTAG_MAX_TAG_CHARS for tag in hashtag_line.split())
    assert len(hashtag_line.split()) <= main.TG_EVENT_HASHTAG_MAX_TAGS


def test_build_tg_event_announcement_formats_multiday_range():
    event = _event(
        date="2026-06-12",
        end_date="2026-06-15",
        time="",
        title="Фестиваль Кантата",
        ics_url="https://example.com/kantata.ics",
        ics_post_url="https://t.me/c/asset/42",
    )

    text = main.build_tg_event_announcement(event, "Описание.")
    markup = main.build_tg_event_reply_markup(event)

    assert "📅 12–15 июня" in text
    assert "📅 12 июня" not in text
    assert markup.inline_keyboard[0][0].text == "📅 12–15 июня · Добавить в календарь"
    assert markup.inline_keyboard[0][0].url == "https://example.com/kantata.ics"


def test_build_tg_event_announcement_formats_multiday_cross_month_range():
    event = _event(date="2026-06-30", end_date="2026-07-02", time="")

    text = main.build_tg_event_announcement(event, "Описание.")

    assert "📅 30 июня–2 июля" in text


def test_build_tg_event_announcement_marks_rock_concert_with_horns_icon():
    event = _event(
        title="Концерт группы «Крематорий»",
        emoji="🎸",
        description="Концерт рок-группы «Крематорий».",
        search_digest="Легендарная рок-группа играет юбилейную программу.",
    )

    text = main.build_tg_event_announcement(event, "Описание.")

    assert text.startswith("<b>🤘 Концерт группы «Крематорий»</b>")





def test_build_tg_event_announcement_free_event_keeps_search_hashtag():
    event = _event(ticket_link=None, ticket_price_min=None, ticket_price_max=None, is_free=True)

    text = main.build_tg_event_announcement(event, "Описание.")

    assert "🟡 Бесплатно" in text
    assert "#бесплатно" in text


@pytest.mark.asyncio
async def test_tg_event_publish_schedules_premium_editor_after_send(monkeypatch):
    event = _event(ticket_link=None, ticket_price_min=None, ticket_price_max=None, is_free=True)
    scheduled = []

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        return "Описание события."

    def fake_schedule(targets, *, context, medallion_html_block=None):
        scheduled.append((targets, context, medallion_html_block))

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    monkeypatch.setattr(main, "_schedule_tg_premium_emoji_editor", fake_schedule)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        "source text",
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "text"
    assert source_hash
    assert scheduled == [([("@kldevents", 101)], "tg_event_publish_send", None)]
    message_text = bot.messages[0][1]
    assert "🟡 Бесплатно" in message_text
    assert "#бесплатно" in message_text


def test_build_tg_event_announcement_uses_original_ticket_link_not_vk_short():
    event = _event(
        ticket_link="https://example.com/register",
        vk_ticket_short_url="https://vk.cc/cYdmeE",
    )

    text = main.build_tg_event_announcement(event, "Описание.")

    assert 'href="https://example.com/register"' in text
    assert "vk.cc" not in text


def test_build_tg_event_announcement_links_phone_ticket_line():
    event = _event(
        ticket_link="tel:+74012463635",
        ticket_price_min=None,
        ticket_price_max=None,
        is_free=True,
    )

    text = main.build_tg_event_announcement(event, "Описание.")

    assert 'href="tel:+74012463635"' in text
    assert "🟡 Бесплатно, запись: " in text
    assert "+7 (4012) 46-36-35" in text


def test_build_tg_event_announcement_linkifies_phone_in_body():
    event = _event(ticket_link=None, ticket_price_min=None, is_free=True)

    text = main.build_tg_event_announcement(
        event,
        "Запись по телефону +7 (4012) 46-36-35.",
    )

    assert 'Запись по телефону <a href="tel:+74012463635">+7 (4012) 46-36-35</a>.' in text


def _entity_type(entity) -> str:
    return str(getattr(getattr(entity, "type", ""), "value", getattr(entity, "type", "")))


def test_telegram_event_html_to_text_entities_emits_phone_entity_for_tel_link():
    event = _event(
        title="🎓 Лекция",
        ticket_link="tel:+74012463635",
        ticket_price_min=None,
        ticket_price_max=None,
        is_free=True,
    )

    html_text = main.build_tg_event_announcement(event, "Описание.")
    plain, entities = main.telegram_event_html_to_text_entities(html_text)

    assert "<a" not in plain
    assert "+74012463635" in plain
    assert any(_entity_type(ent) == "bold" for ent in entities)
    assert any(_entity_type(ent) == "text_link" and ent.url == "https://telegra.ph/event" for ent in entities)
    assert any(_entity_type(ent) == "phone_number" for ent in entities)


def test_telegram_event_html_to_text_entities_linkifies_plain_body_phone():
    event = _event(ticket_link=None, ticket_price_min=None, is_free=True)
    html_text = main.build_tg_event_announcement(
        event,
        "Запись по телефону +7 (4012) 46-36-35.",
    )

    plain, entities = main.telegram_event_html_to_text_entities(html_text)

    assert "Запись по телефону +74012463635." in plain
    assert sum(1 for ent in entities if _entity_type(ent) == "phone_number") == 1


def test_tg_event_source_hash_includes_prompt_version(monkeypatch):
    event = _event()
    base_hash = main.build_tg_event_source_hash(event, "source text")

    monkeypatch.setattr(main, "TG_EVENT_REWRITE_PROMPT_VERSION", "tg-event-hook-test")

    assert main.build_tg_event_source_hash(event, "source text") != base_hash


def test_tg_event_source_hash_includes_media_signature():
    event = _event()
    base_hash = main.build_tg_event_source_hash(event, "source text")

    event.photo_urls = ["https://img.example/poster.webp"]

    assert main.build_tg_event_source_hash(event, "source text") != base_hash


def test_tg_event_source_hash_includes_promo_highlight():
    event = _event()
    base_hash = main.build_tg_event_source_hash(event, "source text")

    assert (
        main.build_tg_event_source_hash(event, "source text", promo_highlight=True)
        != base_hash
    )


def test_tg_event_text_for_publish_uses_source_when_utility_description_conflicts():
    source = (
        "Собранное сырье направят на переработку. От одного физического лица "
        "принимается не более 4 шин. Временная точка приема: Правая набережная, 25."
    )
    event = _event(
        title="Прием шин",
        description=(
            "Приглашаем на уникальное мероприятие с музыкальными номерами, "
            "театральными постановками и входными билетами."
        ),
        search_digest="На ярмарке вас ждет насыщенная программа.",
        source_text=source,
        event_type="ярмарка",
        is_free=True,
        ticket_link=None,
    )

    assert main.select_tg_event_text_for_publish(event) == source


def test_tg_event_promo_text_for_publish_combines_context_sources():
    event = _event(
        description="Описание с главной программой.",
        short_description="Коротко о формате.",
        search_digest="Суть события для поиска.",
        source_text="Исходный текст с деталями участников.",
    )

    text = main.select_tg_event_text_for_publish(event, promo_highlight=True)

    assert "Описание: Описание с главной программой." in text
    assert "Коротко: Коротко о формате." in text
    assert "Суть: Суть события для поиска." in text
    assert "Исходный текст: Исходный текст с деталями участников." in text


@pytest.mark.asyncio
async def test_tg_event_button_highlight_requires_enabled_activity(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = main.datetime(2026, 6, 20, 10, 0, tzinfo=main.timezone.utc)

    event = _event(date="2026-07-20")
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        campaign = PromoCampaign(
            title="Любая промо-кампания",
            status="active",
            starts_at=now,
            ends_at=main.datetime(2026, 7, 30, 23, 59, tzinfo=main.timezone.utc),
        )
        session.add(campaign)
        await session.commit()
        await session.refresh(campaign)
        session.add(
            PromoTarget(
                campaign_id=int(campaign.id),
                target_type="event",
                event_id=int(event.id),
            )
        )
        await session.commit()
        event_id = int(event.id)
        campaign_id = int(campaign.id)

    async with db.get_session() as session:
        stored = await session.get(main.Event, event_id)

    assert stored is not None
    assert await main.resolve_tg_event_promo_highlight(stored, db) is True
    assert await main.resolve_tg_event_button_highlight(stored, db) is False

    async with db.get_session() as session:
        session.add(
            PromoActivity(
                campaign_id=campaign_id,
                surface=PROMO_SURFACE_TG_BUTTON_HIGHLIGHT,
                profile_key=PROMO_TG_BUTTON_HIGHLIGHT_PROFILE,
                enabled=True,
            )
        )
        await session.commit()
    assert await main.resolve_tg_event_button_highlight(stored, db) is True

    async with db.get_session() as session:
        activity = (
            await session.execute(
                main.select(PromoActivity).where(
                    PromoActivity.campaign_id == campaign_id,
                    PromoActivity.surface == PROMO_SURFACE_TG_BUTTON_HIGHLIGHT,
                )
            )
        ).scalars().one()
        activity.enabled = False
        await session.commit()
    assert await main.resolve_tg_event_button_highlight(stored, db) is False
    await db.close()


def test_tg_event_utility_hashtags_ignore_conflicting_description():
    source = (
        "Собранное сырье направят на переработку. От одного физического лица "
        "принимается не более 4 шин. Временная точка приема: Правая набережная, 25."
    )
    event = _event(
        title="Прием шин",
        description=(
            "Приглашаем на уникальное мероприятие с музыкальными номерами, "
            "театральными постановками и входными билетами."
        ),
        source_text=source,
        event_type="ярмарка",
        is_free=True,
        ticket_link=None,
    )

    text = main.build_tg_event_announcement(event, "Можно сдать шины на переработку.")

    assert "#спектакль" not in text
    assert "#концерт" not in text


@pytest.mark.asyncio
async def test_tg_event_hook_rewrite_keeps_useful_non_question(monkeypatch):
    import google_ai

    source = (
        "Собранное сырье будет направлено на перерабатывающее предприятие, "
        "где из шин изготовят новые полезные продукты. От одного физического "
        "лица принимается не более 4 шин."
    )
    event = _event(title="Прием шин", source_text=source)
    clients = []

    class FakeGoogleAIClient:
        def __init__(self, *args, **kwargs):
            self.fallback_models = ["gemini-3.1-flash-lite"]
            self.max_retries = 3
            clients.append(self)

        async def generate_content_async(self, **kwargs):
            prompt = kwargs["prompt"]
            assert kwargs["model"] == "gemini-3.1-flash-lite"
            assert kwargs["max_output_tokens"] == 768
            assert "Вопрос не обязателен" in prompt
            assert "какую пользу это даёт" in prompt
            assert "Любое обещание посетителю" in prompt
            assert "Не переноси детали прошедшего события" in prompt
            assert "без домысливания программы и активностей" in prompt
            assert "evidence_quote" in prompt
            assert kwargs["generation_config"]["response_mime_type"] == "application/json"
            assert "response_json_schema" in kwargs["generation_config"]
            assert "response_schema" not in kwargs["generation_config"]
            assert self.fallback_models == []
            assert self.max_retries == 1
            return (
                json.dumps(
                    {
                        "sentences": [
                            {
                                "text": "Принимается не более 4 шин от одного человека; собранное сырьё направят на переработку.",
                                "evidence_quote": (
                                    "Собранное сырье будет направлено на перерабатывающее предприятие, "
                                    "где из шин изготовят новые полезные продукты. От одного физического "
                                    "лица принимается не более 4 шин."
                                ),
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                {},
            )

    monkeypatch.setattr(google_ai, "GoogleAIClient", FakeGoogleAIClient)

    hook = await main.build_tg_event_hook_text(event, source)

    assert len(clients) == 1
    assert hook.startswith("Принимается не более 4 шин")
    assert "Что здесь стоит увидеть?" not in hook


def test_tg_event_hook_schema_bounds_grounded_sentence_array() -> None:
    sentences = main._TG_EVENT_HOOK_RESPONSE_SCHEMA["properties"]["sentences"]

    assert sentences["minItems"] == 1
    assert sentences["maxItems"] == 3


def test_tg_event_hook_schema_constrains_quotes_to_exact_source_fragments() -> None:
    evidence = (
        "Первая точная фраза организатора о программе. "
        "Вторая точная фраза организатора о дегустации и маршруте."
    )

    schema = main._tg_event_hook_response_schema(evidence)
    quote_schema = schema["properties"]["sentences"]["items"]["properties"][
        "evidence_quote"
    ]

    assert quote_schema["enum"] == [
        "Первая точная фраза организатора о программе.",
        "Вторая точная фраза организатора о дегустации и маршруте.",
    ]
    assert all(quote in evidence for quote in quote_schema["enum"])


def test_tg_event_quote_enum_splits_long_source_without_inventing_text() -> None:
    evidence = " ".join(
        f"дословный фрагмент организатора номер {index}" for index in range(30)
    )

    candidates = main._tg_event_evidence_quote_candidates(evidence)

    assert 1 < len(candidates) <= 6
    assert all(8 <= len(quote) <= 160 for quote in candidates)
    assert sum(map(len, candidates)) <= 960
    assert all(quote in evidence for quote in candidates)


def test_tg_event_quote_enum_keeps_multiline_heading_with_dotted_title() -> None:
    source = (
        "[ТАКТИЧЕСКАЯ ИГРА-КОЛОДОСТРОЙ]\n"
        "Дюна. Империя + дополнение «Бессмертие»\n"
        "Далёкое будущее. Человечество распространилось во Вселенной."
    )
    event = _event(source_text=source)

    evidence = main._tg_event_source_evidence(event)
    candidates = main._tg_event_evidence_quote_candidates(evidence)
    expected_quote = (
        "[ТАКТИЧЕСКАЯ ИГРА-КОЛОДОСТРОЙ] "
        "Дюна. Империя + дополнение «Бессмертие»"
    )

    assert "\n" in evidence
    assert any(candidate.startswith(expected_quote) for candidate in candidates)
    verdict = main.claim_is_grounded(
        "Тактическая игра-колодострой «Дюна. Империя» с дополнением «Бессмертие».",
        evidence,
        evidence_quote=next(
            candidate for candidate in candidates if candidate.startswith(expected_quote)
        ),
        min_ratio=0.45,
        min_matches=2,
    )
    assert verdict.ok
    assert len(candidates) <= 6
    assert all(len(candidate) <= 160 for candidate in candidates)


def test_tg_event_quote_enum_never_packs_across_source_boundary() -> None:
    evidence = "Заголовок первого источника\n\nНазвание второго источника"

    candidates = main._tg_event_evidence_quote_candidates(evidence)

    assert "Заголовок первого источника Название второго источника" not in candidates
    assert candidates == [
        "Заголовок первого источника",
        "Название второго источника",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("promo_highlight", "expected_max_tokens"),
    [(False, 768), (True, 1024)],
)
async def test_tg_event_hook_lite_failure_uses_strict_budgeted_4o(
    monkeypatch,
    promo_highlight,
    expected_max_tokens,
):
    import google_ai

    source = "Музыканты исполнят редкую камерную программу."
    event = _event(id=81, title="Камерный концерт", source_text=source)

    class FailingLiteClient:
        def __init__(self, *args, **kwargs):
            self.fallback_models = ["gemma-4-31b-it"]
            self.max_retries = 3

        async def generate_content_async(self, **kwargs):
            assert kwargs["model"] == "gemini-3.1-flash-lite"
            assert self.fallback_models == []
            assert self.max_retries == 1
            raise RuntimeError("lite unavailable")

    monkeypatch.setattr(google_ai, "GoogleAIClient", FailingLiteClient)

    async def fake_reserve(db, *, event_id):
        assert db == "db"
        assert event_id == 81
        return 7

    calls = []

    async def fake_ask_4o(prompt, **kwargs):
        calls.append((prompt, kwargs))
        return json.dumps(
            {
                "sentences": [
                    {
                        "text": "Музыканты исполнят редкую камерную программу.",
                        "evidence_quote": "Музыканты исполнят редкую камерную программу.",
                    }
                ]
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(main, "_reserve_tg_event_4o_fallback", fake_reserve)
    monkeypatch.setattr(main, "ask_4o", fake_ask_4o)

    hook = await main.build_tg_event_hook_text(
        event,
        source,
        db="db",
        promo_highlight=promo_highlight,
    )

    assert hook.startswith("Музыканты исполнят")
    assert len(calls) == 1
    assert calls[0][1]["model"] == "gpt-4o"
    assert calls[0][1]["allow_model_fallback"] is False
    assert calls[0][1]["max_tokens"] == expected_max_tokens
    assert calls[0][1]["meta"]["consumer"] == "tg_event_publish_fallback"
    assert calls[0][1]["meta"]["daily_request_no"] == 7


@pytest.mark.asyncio
async def test_tg_event_hook_has_no_deterministic_fallback_when_budget_exhausted(monkeypatch):
    import google_ai

    event = _event(id=82, title="Камерный концерт", source_text="Текст события")

    class InvalidLiteClient:
        def __init__(self, *args, **kwargs):
            self.fallback_models = []
            self.max_retries = 1

        async def generate_content_async(self, **kwargs):
            return "Хотите камерный вечер?", {}

    monkeypatch.setattr(google_ai, "GoogleAIClient", InvalidLiteClient)
    monkeypatch.setattr(
        main,
        "_reserve_tg_event_4o_fallback",
        lambda *args, **kwargs: main.asyncio.sleep(0, result=None),
    )

    async def unexpected_4o(*args, **kwargs):
        raise AssertionError("4o must not run without a reservation")

    monkeypatch.setattr(main, "ask_4o", unexpected_4o)

    with pytest.raises(main.TelegramEventPublicWriterUnavailable):
        await main.build_tg_event_hook_text(event, event.source_text, db="db")


@pytest.mark.asyncio
async def test_tg_event_4o_fallback_budget_is_persisted_and_hard_capped(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "TG_EVENT_4O_FALLBACK_DAILY_LIMIT", 2)

    assert await main._reserve_tg_event_4o_fallback(db, event_id=1) == 1
    assert await main._reserve_tg_event_4o_fallback(db, event_id=2) == 2
    assert await main._reserve_tg_event_4o_fallback(db, event_id=3) is None
    rows = await db.exec_driver_sql(
        "SELECT used, limit_value FROM llm_daily_request_budget "
        "WHERE budget_key = ?",
        (main.TG_EVENT_4O_FALLBACK_BUDGET_KEY,),
    )
    assert rows == [(2, 2)]
    await db.close()


@pytest.mark.asyncio
async def test_tg_event_promo_rewrite_uses_richer_prompt(monkeypatch):
    import google_ai

    event = _event(title="Камерный концерт", source_text="Музыканты играют редкую программу.")

    class FakeGoogleAIClient:
        def __init__(self, *args, **kwargs):
            pass

        async def generate_content_async(self, **kwargs):
            prompt = kwargs["prompt"]
            assert "до 500 символов" in prompt
            assert "Это промо-событие" in prompt
            assert "2-3 самые сильные конкретные причины" in prompt
            assert kwargs["max_output_tokens"] == 1024
            return (
                json.dumps(
                    {
                        "sentences": [
                            {
                                "text": "Музыканты играют редкую камерную программу.",
                                "evidence_quote": "Музыканты играют редкую программу.",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                {},
            )

    monkeypatch.setattr(google_ai, "GoogleAIClient", FakeGoogleAIClient)

    hook = await main.build_tg_event_hook_text(
        event,
        event.source_text,
        promo_highlight=True,
    )

    assert hook.startswith("Музыканты играют")


def test_tg_event_hook_payload_rejects_unrelated_tire_claim() -> None:
    source = (
        "На Летнем Экодворе будет специалистка Центра защиты леса. "
        "Также собирают чистые материалы для повторного использования."
    )
    raw = json.dumps(
        {
            "sentences": [
                {
                    "text": "Можно сдать до 4 шин на переработку.",
                    "evidence_quote": "чистые материалы для повторного использования",
                }
            ]
        },
        ensure_ascii=False,
    )

    assert main._parse_tg_event_hook_payload(raw, evidence_corpus=source) is None


def test_managed_vk_projection_is_not_event_source_evidence(monkeypatch) -> None:
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")

    assert main._event_source_url_is_managed_output(
        "https://vk.com/wall-231920894_7008"
    ) is True
    assert main._event_source_url_is_managed_output(
        "https://vk.com/wall-132625599_17342"
    ) is False


def test_build_tg_promo_event_publication_formats_markdown_body():
    event = _event(
        title="🗣️ Творческая встреча",
        emoji="🗣️",
        description=(
            "Вступительный абзац про образовательную программу.\n\n"
            "### О спикере\n"
            "Гость — **Иван Никифорчин**.\n\n"
            "### Формат и темы\n"
            "* Роль дирижёра как регулятора эмоционального поля.\n"
            "* Специфика взаимодействия внутри оркестра."
        ),
        is_free=True,
    )

    text = main.build_tg_promo_event_publication_message(event)

    assert text.startswith("<b>🗣️ Творческая встреча</b>")
    assert "###" not in text
    assert "**" not in text
    assert "\n* " not in text
    assert "<b>О спикере</b>" in text
    assert "<b>Формат и темы</b>" in text
    assert "• Роль дирижёра как регулятора эмоционального поля." in text
    assert "Гость — Иван Никифорчин." in text
    assert '🟡 Бесплатно, <a href="https://example.com/tickets">по регистрации</a>' in text


def test_build_tg_promo_media_caption_prefers_media_over_bullet_dump():
    event = _event(
        title="Народные художественные промыслы",
        description=(
            "Событие представляет собой показ документального сериала-путешествия "
            "«Народные художественные промыслы».\n\n"
            "### Что важно\n"
            + "\n".join(f"- Факт программы номер {idx}" for idx in range(1, 40))
        ),
        is_free=True,
        photo_urls=["https://example.com/poster.webp"],
    )

    full = main.build_tg_promo_event_publication_message(event)
    caption = main.build_tg_promo_event_publication_media_caption(event)

    assert len(full) > 1024
    assert len(caption) <= 1024
    assert "Событие представляет собой показ" in caption
    assert "Полный текст — по кнопке" in caption
    assert "<b>Что важно</b>" not in caption
    assert "Факт программы номер 25" not in caption


@pytest.mark.asyncio
async def test_tg_promo_event_publish_sends_media_when_full_text_exceeds_caption_limit(monkeypatch):
    scheduled = []
    monkeypatch.setattr(
        main,
        "_schedule_tg_premium_emoji_editor",
        lambda targets, *, context, **_kwargs: scheduled.append((targets, context)),
    )
    event = _event(
        title="Длинный промо-пост",
        description=(
            "Короткое вступление для подписи.\n\n"
            "### Что важно\n"
            + "\n".join(f"- Подробность номер {idx}" for idx in range(1, 80))
        ),
        photo_urls=[
            "https://example.com/one.webp",
            "https://example.com/two.webp",
        ],
        telegraph_url="https://telegra.ph/full",
    )
    bot = DummyTgBot()

    url = await main.publish_tg_promo_event_publication(
        event,
        db=None,
        bot=bot,
        target_chat="@kldevents",
    )

    assert url == "https://t.me/kldevents/101"
    assert bot.messages == []
    assert bot.photos == []
    assert len(bot.media_groups) == 1
    chat_id, media = bot.media_groups[0]
    assert chat_id == "@kldevents"
    assert len(media) == 2
    assert len(media[0].caption) <= 1024
    assert "Короткое вступление для подписи." in media[0].caption
    assert "Подробность номер 60" not in media[0].caption
    assert bot.reply_markup_edits
    assert bot.reply_markup_edits[0][1]["chat_id"] == "@kldevents"
    assert bot.reply_markup_edits[0][1]["message_id"] == 101
    assert scheduled == [
        ([("@kldevents", 101), ("@kldevents", 102)], "tg_promo_event_publish")
    ]


def test_tg_event_promo_details_move_to_inline_button():
    event = _event(
        ics_url="https://example.com/event.ics",
        ics_post_url="https://t.me/c/asset/42",
    )

    normal = main.build_tg_event_announcement(
        event,
        "Описание.",
        promo_highlight=False,
    )
    normal_markup = main.build_tg_event_reply_markup(event, promo_highlight=False)
    promo = main.build_tg_event_announcement(
        event,
        "Описание.",
        promo_highlight=True,
    )
    promo_markup = main.build_tg_event_reply_markup(event, promo_highlight=True)

    assert "🔎 Подробнее" in normal
    assert normal_markup.inline_keyboard[0][0].text == "📅 20 июня 19:00 · Добавить в календарь"
    assert "Подробнее" not in promo
    assert "Max" not in promo
    assert "Вконтакте" not in promo
    assert promo_markup.inline_keyboard[0][0].text == "✨ Подробнее"
    assert promo_markup.inline_keyboard[0][0].url == "https://telegra.ph/event"
    assert promo_markup.inline_keyboard[1][0].text == "📅 20 июня 19:00 · Добавить в календарь"
    assert promo_markup.inline_keyboard[1][0].url == "https://example.com/event.ics"


def test_tg_event_promo_intro_without_button_keeps_social_footer():
    event = _event(ics_url="https://example.com/event.ics")

    text = main.build_tg_event_announcement(
        event,
        "Описание.",
        promo_highlight=True,
        details_button_highlight=False,
    )
    markup = main.build_tg_event_reply_markup(
        event,
        promo_highlight=True,
        details_button_highlight=False,
    )

    assert "🔎 Подробнее" in text
    assert '<a href="https://max.ru/channel_kenigevents">Max</a>' in text
    assert '<a href="https://vk.ru/im/channels/-239844596">Вконтакте</a>' in text
    assert markup.inline_keyboard[0][0].text == "📅 20 июня 19:00 · Добавить в календарь"


def test_tg_event_button_highlight_suppresses_social_footer_even_without_promo_intro():
    event = _event()

    text = main.build_tg_event_announcement(
        event,
        "Описание.",
        promo_highlight=False,
        details_button_highlight=True,
    )
    markup = main.build_tg_event_reply_markup(
        event,
        promo_highlight=False,
        details_button_highlight=True,
    )

    assert "Подробнее" not in text
    assert "Max" not in text
    assert "Вконтакте" not in text
    assert markup.inline_keyboard[0][0].text == "✨ Подробнее"
    assert markup.inline_keyboard[0][0].url == "https://telegra.ph/event"


def test_tg_event_calendar_url_keeps_public_telegram_post_url():
    event = _event(
        ics_url="https://example.com/event.ics",
        ics_post_url="https://t.me/kenigeventscalendar/42",
    )

    markup = main.build_tg_event_reply_markup(event)

    assert markup.inline_keyboard[0][0].url == "https://t.me/kenigeventscalendar/42"


@pytest.mark.asyncio
async def test_same_day_linked_events_use_one_publish_anchor(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    first = _event(id=None, title="Аудиоспектакль «Дорога эпох»", date="2026-06-20", time="14:00")
    second = _event(id=None, title="Аудиоспектакль «Дорога эпох»", date="2026-06-20", time="16:00")
    async with db.get_session() as session:
        session.add(first)
        session.add(second)
        await session.commit()
        await session.refresh(first)
        await session.refresh(second)
        first.linked_event_ids = [int(second.id)]
        second.linked_event_ids = [int(first.id)]
        session.add(first)
        session.add(second)
        await session.commit()
        second_id = int(second.id)

    async with db.get_session() as session:
        source = await session.get(main.Event, second_id)

    assert source is not None
    anchor, group = await main._prepare_same_day_linked_publish_event(db, source)

    assert int(anchor.id) == int(first.id)
    assert anchor.time == "14:00 и 16:00"
    assert [int(item.id) for item in group] == [int(first.id), int(second.id)]

    published = {}

    async def fake_publish(event, text, db_arg, bot, *, promo_highlight=False, details_button_highlight=False):
        published["event_id"] = int(event.id)
        published["time"] = event.time
        return "https://t.me/kldevents/999", 999, "text", "same-day-hash"

    monkeypatch.setattr(main, "publish_tg_event_announcement", fake_publish)

    assert await main.job_publish_tg_event_post(second_id, db, bot=object()) is True

    assert published == {"event_id": int(first.id), "time": "14:00 и 16:00"}
    async with db.get_session() as session:
        stored_first = await session.get(main.Event, int(first.id))
        stored_second = await session.get(main.Event, int(second.id))

    assert stored_first.tg_event_post_url == "https://t.me/kldevents/999"
    assert stored_second.tg_event_post_url == "https://t.me/kldevents/999"
    assert stored_first.tg_event_source_hash == "same-day-hash"
    assert stored_second.tg_event_source_hash == "same-day-hash"
    await db.close()


@pytest.mark.asyncio
async def test_same_source_feeding_series_uses_one_schedule_publish_anchor(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    shared = {
        "date": "2026-06-20",
        "location_name": "Калининградский зоопарк",
        "location_address": "пр-т Мира 26",
        "city": "Калининград",
        "source_post_url": "https://t.me/kldzoo/7521",
        "photo_urls": ["https://img.test/a.webp", "https://img.test/b.webp"],
        "lifecycle_status": "active",
    }
    first = _event(id=None, title="Кормление колобусов", time="11:30", **shared)
    second = _event(id=None, title="Кормление бурого медведя Фимы", time="13:30", **shared)
    third = _event(id=None, title="кормление рыб в Тропическом доме", time="14:00", **shared)
    async with db.get_session() as session:
        session.add_all([first, second, third])
        await session.commit()
        await session.refresh(first)
        await session.refresh(second)
        await session.refresh(third)
        ids = [int(first.id), int(second.id), int(third.id)]
        third_id = int(third.id)

    async with db.get_session() as session:
        source = await session.get(main.Event, third_id)

    assert source is not None
    anchor, group = await main._prepare_same_day_linked_publish_event(db, source)

    assert int(anchor.id) == ids[0]
    assert [int(item.id) for item in group] == ids
    assert anchor.title == "Кормления животных: Калининградский зоопарк"
    assert anchor.time == "11:30, 13:30 и 14:00"
    assert "• 11:30 — колобусов" in anchor.description
    assert "• 13:30 — бурого медведя Фимы" in anchor.description
    assert "• 14:00 — рыб в Тропическом доме" in anchor.description

    published = {}

    async def fake_publish(event, text, db_arg, bot, *, promo_highlight=False, details_button_highlight=False):
        published["event_id"] = int(event.id)
        published["title"] = event.title
        published["text"] = text
        return "https://t.me/kldevents/1000", 1000, "text", "feeding-series-hash"

    monkeypatch.setattr(main, "publish_tg_event_announcement", fake_publish)

    assert await main.job_publish_tg_event_post(third_id, db, bot=object()) is True

    assert published["event_id"] == ids[0]
    assert published["title"] == "Кормления животных: Калининградский зоопарк"
    assert "• 13:30 — бурого медведя Фимы" in published["text"]
    async with db.get_session() as session:
        rows = [await session.get(main.Event, event_id) for event_id in ids]

    assert [row.tg_event_post_url for row in rows] == ["https://t.me/kldevents/1000"] * 3
    assert [row.tg_event_source_hash for row in rows] == ["feeding-series-hash"] * 3
    await db.close()


@pytest.mark.asyncio
async def test_same_source_feeding_series_replaces_vk_text_instead_of_appending(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    shared = {
        "date": "2026-06-20",
        "location_name": "Калининградский зоопарк",
        "location_address": "пр-т Мира 26",
        "city": "Калининград",
        "source_post_url": "https://t.me/kldzoo/7521",
        "photo_urls": ["https://img.test/a.webp", "https://img.test/b.webp"],
        "lifecycle_status": "active",
    }
    events = [
        _event(id=None, title="Кормление колобусов", time="11:30", **shared),
        _event(id=None, title="Кормление бурого медведя Фимы", time="13:30", **shared),
        _event(id=None, title="кормление рыб в Тропическом доме", time="14:00", **shared),
    ]
    async with db.get_session() as session:
        session.add_all(events)
        await session.commit()
        for item in events:
            await session.refresh(item)
        third_id = int(events[-1].id)

    calls = []

    async def fake_sync(event, text, db_arg, bot, **kwargs):
        calls.append(
            {
                "event_id": int(event.id),
                "title": event.title,
                "text": text,
                "append_text": kwargs.get("append_text"),
            }
        )
        return "https://vk.com/wall-231920894_999"

    monkeypatch.setattr(main, "sync_vk_source_post", fake_sync)

    await main.job_sync_vk_source_post(third_id, db, bot=None)

    assert len(calls) == 1
    assert calls[0]["event_id"] == int(events[0].id)
    assert calls[0]["title"] == "Кормления животных: Калининградский зоопарк"
    assert calls[0]["append_text"] is False
    assert "Расписание кормлений:" in calls[0]["text"]
    assert "• 13:30 — бурого медведя Фимы" in calls[0]["text"]
    async with db.get_session() as session:
        rows = [await session.get(main.Event, int(item.id)) for item in events]

    assert [row.source_vk_post_url for row in rows] == ["https://vk.com/wall-231920894_999"] * 3
    await db.close()


def test_unique_tg_media_urls_only_applies_exact_url_guard():
    base = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/0c/"
        "0c9a2cd38cb24672c27010e8004d884b84434062a0d264522cc29896985624c0.webp"
    )
    near_duplicate = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/0c/"
        "0c9a2cd38cb24672c27010f8004d884b84234062a0d264522cc298b6985624c0.webp"
    )
    distinct = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/89/"
        "892b6145103c3c3e2c0aae2a2b0b608921893489108c18281d241c380c5000d0.webp"
    )

    assert main._unique_tg_media_urls([base, near_duplicate, distinct, base]) == [
        base,
        near_duplicate,
        distinct,
    ]


def test_unique_tg_media_urls_does_not_make_borderline_perceptual_decision():
    base = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/8c/"
        "8c22d06b5b245a25046594720972196274e373193230d963c70b520848490504.webp"
    )
    near_duplicate = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/89/"
        "8922d26b59245825046514720972196274e373193230d963c70b180048490504.webp"
    )

    assert main._unique_tg_media_urls([base, near_duplicate]) == [base, near_duplicate]


@pytest.mark.asyncio
async def test_tg_event_publish_does_not_override_canonical_media_gate(monkeypatch):
    managed = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/80/"
        "8001c001000c9c09430561ac78e858358b0706a338e534c498c0d06819000800.webp"
    )
    vk_cdn = "https://sun9-78.userapi.com/s/v1/ig2/source-copy.jpg?cs=1080x0"
    event = _event(photo_urls=[managed, vk_cdn])

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        assert event_arg is event
        return "Короткий анонс события."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    materialize_calls = _patch_media_materializer(monkeypatch)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        "source",
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "album_caption"
    assert source_hash
    assert materialize_calls == [(managed, 0), (vk_cdn, 1)]
    assert not bot.photos
    assert len(bot.media_groups) == 1


@pytest.mark.asyncio
async def test_tg_event_publish_sends_single_photo_caption_with_calendar_button(monkeypatch):
    event = _event(
        photo_urls=["https://img.example/0.jpg"],
        ics_url="https://example.com/event.ics",
        ics_post_url="https://t.me/c/asset/42",
    )
    long_text = " ".join(["Очень длинное описание события."] * 120)

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        assert event_arg is event
        assert source_text == long_text
        return "Что делает этот вечер особенным? Музыка прозвучит в камерном формате."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    materialize_calls = _patch_media_materializer(monkeypatch)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        long_text,
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "photo_caption"
    assert source_hash
    assert materialize_calls == [("https://img.example/0.jpg", 0)]
    assert not bot.messages
    assert len(bot.photos) == 1
    assert not isinstance(bot.photos[0][1], str)
    message_text = bot.photos[0][2]["caption"]
    message_kwargs = bot.photos[0][2]
    assert "Что делает этот вечер особенным?" in message_text
    assert main._tg_html_visible_len(message_text) <= 1000
    assert "Подробнее" in message_text
    assert "Подписаться" not in message_text
    assert "Вконтакте" in message_text
    assert "#20июня" in message_text
    assert "#Калининград" in message_text
    assert "#афишакалининград" in message_text
    assert "#анонс" not in message_text
    button = message_kwargs["reply_markup"].inline_keyboard[0][0]
    assert button.text == "📅 20 июня 19:00 · Добавить в календарь"
    assert button.url == "https://example.com/event.ics"
    assert not bot.media_groups


@pytest.mark.asyncio
async def test_tg_event_publish_replaces_old_text_when_media_appears(monkeypatch):
    event = _event(
        photo_urls=["https://img.example/0.jpg"],
        tg_event_post_id=77,
        tg_event_post_mode="text",
        tg_event_post_url="https://t.me/c/1234567890/77",
        tg_event_source_hash="old",
    )
    long_text = " ".join(["Описание события."] * 20)

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        return "Что делает этот вечер особенным? Камерный формат."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    materialize_calls = _patch_media_materializer(monkeypatch)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        long_text,
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "photo_caption"
    assert source_hash and source_hash != "old"
    assert materialize_calls == [("https://img.example/0.jpg", 0)]
    assert len(bot.photos) == 1
    assert bot.deleted == [{"chat_id": "@kldevents", "message_id": 77}]


@pytest.mark.asyncio
async def test_tg_event_publish_sends_album_caption_for_multiple_media(monkeypatch):
    event = _event(
        photo_urls=[f"https://img.example/{idx}.jpg" for idx in range(12)],
        ics_post_url="https://t.me/c/asset/42",
    )
    long_text = " ".join(["Очень длинное описание события."] * 120)

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        assert event_arg is event
        assert source_text == long_text
        return "Что делает этот вечер особенным? Музыка прозвучит в камерном формате."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    materialize_calls = _patch_media_materializer(monkeypatch)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        long_text,
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "album_caption"
    assert source_hash
    assert not bot.messages
    assert not bot.photos
    assert len(bot.media_groups) == 1
    assert len(bot.media_groups[0][1]) == 9
    assert materialize_calls == [(f"https://img.example/{idx}.jpg", idx) for idx in range(9)]
    assert not isinstance(bot.media_groups[0][1][0].media, str)
    assert "Что делает этот вечер особенным?" in bot.media_groups[0][1][0].caption
    assert "Добавить в календарь" not in bot.media_groups[0][1][0].caption
    assert "🔎 Подробнее" in bot.media_groups[0][1][0].caption


@pytest.mark.asyncio
async def test_tg_event_publish_fails_closed_when_materialized_media_unavailable(monkeypatch):
    event = _event(
        photo_urls=[f"https://img.example/{idx}.jpg" for idx in range(2)],
        ics_post_url="https://t.me/c/asset/42",
    )
    long_text = " ".join(["Описание события."] * 40)

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        assert event_arg is event
        assert source_text == long_text
        return "Что делает этот вечер особенным? Водная программа и активности."

    async def fail_materialize(url, index):
        raise RuntimeError("materialized media unavailable")

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    monkeypatch.setattr(main, "materialize_tg_event_media_for_upload", fail_materialize)
    bot = DummyTgBot()

    with pytest.raises(RuntimeError, match="materialized media unavailable"):
        await main.publish_tg_event_announcement(event, long_text, None, bot)

    assert not bot.messages
    assert not bot.photos
    assert not bot.media_groups


@pytest.mark.asyncio
async def test_tg_event_publish_never_downgrades_to_text_in_strict_cdn_mode(
    monkeypatch,
):
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    event = _event(
        photo_urls=[],
        tg_event_post_id=77,
        tg_event_post_mode="photo_caption",
        tg_event_post_url="https://t.me/c/1234567890/77",
    )
    bot = DummyTgBot()

    with pytest.raises(RuntimeError, match="missing_approved_cdn_media"):
        await main.publish_tg_event_announcement(event, "Описание", None, bot)

    assert not bot.messages
    assert not bot.photos
    assert not bot.deleted


@pytest.mark.asyncio
async def test_strict_cdn_schedule_puts_media_repair_before_public_fanout(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls = []

    async def fake_enqueue_job(_db, eid, task, **kwargs):
        calls.append((task, kwargs.get("depends_on")))
        return f"{task.value}:{eid}"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)
    event = _event(id=None, date="2027-08-20", photo_urls=[])
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert (main.JobTask.event_media_review, None) in calls
    assert (
        main.JobTask.telegraph_build,
        [f"event_media_review:{event.id}"],
    ) in calls
    tg_calls = [item for item in calls if item[0] == main.JobTask.tg_event_publish]
    assert tg_calls and f"telegraph_build:{event.id}" in tg_calls[0][1]


@pytest.mark.asyncio
async def test_tg_event_publish_suppresses_retry_on_uncertain_send_timeout(monkeypatch):
    event = _event(photo_urls=["https://img.example/0.jpg"])
    bot = DummyTgBot()

    _patch_media_materializer(monkeypatch)
    async def fake_announcement_for_publish(*args, **kwargs):
        return "<b>Камерный концерт</b>\n\nОписание", False

    monkeypatch.setattr(
        main,
        "build_tg_event_announcement_for_publish",
        fake_announcement_for_publish,
    )

    async def timeout_send_photo(*args, **kwargs):
        bot.photos.append((args, kwargs))
        raise RuntimeError("Telegram server says - Request timeout error")

    bot.send_photo = timeout_send_photo

    with pytest.raises(main.TelegramEventPublishUncertainSendError):
        await main.publish_tg_event_announcement(event, "Описание", None, bot)

    assert len(bot.photos) == 1


@pytest.mark.asyncio
async def test_tg_event_publish_does_not_keep_text_mode_when_media_exists(monkeypatch):
    long_text = " ".join(["Описание события."] * 20)
    event = _event(
        photo_urls=["https://img.example/0.jpg"],
        tg_event_post_id=77,
        tg_event_post_mode="text",
        tg_event_post_url="https://t.me/c/1234567890/77",
    )
    event.tg_event_source_hash = main.build_tg_event_source_hash(event, long_text)

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        return "Что делает этот вечер особенным? Камерный формат."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    materialize_calls = _patch_media_materializer(monkeypatch)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        long_text,
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "photo_caption"
    assert source_hash == event.tg_event_source_hash
    assert materialize_calls == [("https://img.example/0.jpg", 0)]
    assert len(bot.photos) == 1
    assert bot.deleted == [{"chat_id": "@kldevents", "message_id": 77}]


@pytest.mark.asyncio
async def test_tg_event_publish_preserves_legacy_album_without_complete_id_ledger(monkeypatch):
    long_text = " ".join(["Описание события."] * 20)
    event = _event(
        location_name="Калининградская областная научная библиотека",
        photo_urls=[
            "https://img.example/0.jpg",
            "https://img.example/1.jpg",
        ],
        tg_event_post_id=77,
        tg_event_post_mode="album_caption",
        tg_event_post_url="https://t.me/c/1234567890/77",
        tg_event_source_hash="old",
    )

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        return "Что делает этот вечер особенным? Камерный формат."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    materialize_calls = _patch_media_materializer(monkeypatch)

    class AlbumEditBot(DummyTgBot):
        def __init__(self):
            super().__init__()
            self.caption_edits = []

        async def edit_message_caption(self, **kwargs):
            self.caption_edits.append(kwargs)
            return True

    bot = AlbumEditBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        long_text,
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/77"
    assert post_id == 77
    assert mode == "album_caption"
    assert source_hash and source_hash != "old"
    assert len(bot.caption_edits) == 1
    assert bot.caption_edits[0]["message_id"] == 77
    assert materialize_calls == []
    assert bot.rich_messages == []
    assert bot.deleted == []


@pytest.mark.asyncio
async def test_tg_event_publish_legacy_album_edit_failure_retries_without_duplicate(monkeypatch):
    event = _event(
        location_name="Калининградская областная научная библиотека",
        photo_urls=[
            "https://img.example/0.jpg",
            "https://img.example/1.jpg",
        ],
        tg_event_post_id=77,
        tg_event_post_mode="album_caption",
        tg_event_post_url="https://t.me/c/1234567890/77",
        tg_event_source_hash="old",
    )

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        return "Что делает этот вечер особенным? Камерный формат."

    class FailingAlbumEditBot(DummyTgBot):
        async def edit_message_caption(self, **kwargs):
            raise RuntimeError("telegram edit failed")

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    materialize_calls = _patch_media_materializer(monkeypatch)
    bot = FailingAlbumEditBot()

    with pytest.raises(RuntimeError, match="refusing duplicate fallback"):
        await main.publish_tg_event_announcement(
            event,
            "Описание события.",
            None,
            bot,
        )

    assert materialize_calls == []
    assert bot.media_groups == []
    assert bot.rich_messages == []
    assert bot.deleted == []


@pytest.mark.asyncio
async def test_tg_event_publish_migrates_single_photo_to_rich_after_success(monkeypatch):
    event = _event(
        location_name="Калининградская областная научная библиотека",
        photo_urls=["https://img.example/0.jpg"],
        tg_event_post_id=77,
        tg_event_post_mode="photo_caption",
        tg_event_post_url="https://t.me/c/1234567890/77",
        tg_event_source_hash="old",
    )

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        return "Что делает этот вечер особенным? Камерный формат."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    _patch_media_materializer(monkeypatch)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        "Описание события.",
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "rich_message"
    assert source_hash and source_hash != "old"
    assert len(bot.rich_messages) == 1
    assert bot.deleted == [{"chat_id": "@kldevents", "message_id": 77}]


@pytest.mark.asyncio
async def test_tg_event_publish_deletes_old_post_when_edit_falls_back_to_send(monkeypatch):
    event = _event(
        photo_urls=[],
        tg_event_post_id=77,
        tg_event_post_mode="text",
        tg_event_post_url="https://t.me/c/1234567890/77",
        tg_event_source_hash="old",
    )

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        return "Что делает этот вечер особенным? Камерный формат."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        "Описание события.",
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "text"
    assert source_hash and source_hash != "old"
    assert len(bot.messages) == 1
    assert bot.deleted == [{"chat_id": "@kldevents", "message_id": 77}]


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_enqueues_tg_publish(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append((task, kwargs.get("depends_on"), kwargs.get("next_run_at")))
        return f"{task.value}:job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    deferred_at = main.datetime(2026, 6, 20, 5, 0, tzinfo=main.timezone.utc)
    monkeypatch.setattr(
        main,
        "next_tg_event_publish_run_at",
        lambda db_obj, **_kwargs: main.asyncio.sleep(0, result=deferred_at),
    )
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(id=None)
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert (
        main.JobTask.tg_event_publish,
        [
            f"telegraph_build:{event.id}",
            f"tg_ics_post:{event.id}",
        ],
        deferred_at,
    ) in tasks
    assert (main.JobTask.vk_sync, None, None) in tasks


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_skips_calendar_dependency_without_time(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append((task, kwargs.get("depends_on"), kwargs.get("next_run_at")))
        return f"{task.value}:job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    deferred_at = main.datetime(2026, 6, 20, 5, 0, tzinfo=main.timezone.utc)
    monkeypatch.setattr(
        main,
        "next_tg_event_publish_run_at",
        lambda db_obj, **_kwargs: main.asyncio.sleep(0, result=deferred_at),
    )
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(id=None, time="")
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert not any(task == main.JobTask.tg_ics_post for task, _, _ in tasks)
    assert (
        main.JobTask.tg_event_publish,
        [
            f"telegraph_build:{event.id}",
        ],
        deferred_at,
    ) in tasks


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_skips_calendar_dependency_for_bad_time(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append((task, kwargs.get("depends_on"), kwargs.get("next_run_at")))
        return f"{task.value}:job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    deferred_at = main.datetime(2026, 6, 20, 5, 0, tzinfo=main.timezone.utc)
    monkeypatch.setattr(
        main,
        "next_tg_event_publish_run_at",
        lambda db_obj, **_kwargs: main.asyncio.sleep(0, result=deferred_at),
    )
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(id=None, time="по расписанию")
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert not any(task == main.JobTask.ics_publish for task, _, _ in tasks)
    assert not any(task == main.JobTask.tg_ics_post for task, _, _ in tasks)
    assert (
        main.JobTask.tg_event_publish,
        [
            f"telegraph_build:{event.id}",
        ],
        deferred_at,
    ) in tasks


@pytest.mark.asyncio
async def test_enqueue_tg_publish_replaces_stale_calendar_dependency(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    event = _event(id=None, time="")
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)
        session.add(
            main.JobOutbox(
                event_id=event_id,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.pending,
                depends_on=f"telegraph_build:{event_id},tg_ics_post:{event_id},vk_sync:{event_id}",
                next_run_at=main.datetime(2026, 6, 20, 5, 0, tzinfo=main.timezone.utc),
                updated_at=main.datetime(2026, 6, 19, 5, 0, tzinfo=main.timezone.utc),
            )
        )
        await session.commit()

    result = await main.enqueue_job(
        db,
        event_id,
        main.JobTask.tg_event_publish,
        depends_on=[f"telegraph_build:{event_id}"],
        replace_depends_on=True,
        next_run_at=main.datetime(2026, 6, 20, 5, 10, tzinfo=main.timezone.utc),
    )

    async with db.get_session() as session:
        job = (
            await session.execute(
                main.select(main.JobOutbox).where(
                    main.JobOutbox.event_id == event_id,
                    main.JobOutbox.task == main.JobTask.tg_event_publish,
                )
            )
        ).scalar_one()

    assert result == "merged-rearmed"
    assert job.depends_on == f"telegraph_build:{event_id}"


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_requeues_deleted_managed_vk_post(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")

    async def fake_vk_api(method, **kwargs):
        assert method == "wall.getById"
        assert kwargs["posts"] == "-231920894_2432"
        return {"response": {"items": []}}

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append((task, kwargs.get("depends_on"), kwargs.get("next_run_at")))
        return f"{task.value}:job"

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(
        id=None,
        date=(main.datetime.now(main.LOCAL_TZ).date() + main.timedelta(days=10)).isoformat(),
        source_vk_post_url="https://vk.com/wall-231920894_2432",
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert any(task == main.JobTask.vk_sync for task, _, _ in tasks)
    tg_publish = [item for item in tasks if item[0] == main.JobTask.tg_event_publish][0]
    # Telegram publishing is deliberately independent from VK retries/captcha.
    assert f"vk_sync:{event.id}" not in tg_publish[1]


@pytest.mark.asyncio
async def test_managed_vk_post_with_missing_expected_photo_is_incomplete(monkeypatch):
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")

    async def fake_vk_api(method, **kwargs):
        assert method == "wall.getById"
        return {"response": {"items": [{"id": 2432, "attachments": []}]}}

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    event = _event(
        source_vk_post_url="https://vk.com/wall-231920894_2432",
        photo_urls=["https://static.kenigevents.ru/poster.webp"],
    )

    assert await main._managed_vk_post_state(event) == (True, False)
    assert not await main._event_has_existing_managed_vk_post(event)


@pytest.mark.asyncio
async def test_recover_managed_vk_live_url_persists_unique_title_date_match(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")

    event = _event(
        id=None,
        source_vk_post_url="https://vk.com/wall-231920894_7266",
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)
        session.add(
            EventSource(
                event_id=event_id,
                source_type="vk",
                source_url="https://vk.com/wall-231920894_7266",
            )
        )
        await session.commit()

    async def fake_state(ev):
        return False, False

    async def fake_find(ev, *, owner_id, db, bot):
        assert owner_id == -231920894
        return {"id": 7269, "text": "Камерный концерт\n\n\U0001f4c5 20 июня 19:00"}

    monkeypatch.setattr(main, "_managed_vk_post_state", fake_state)
    monkeypatch.setattr(main, "_find_unique_live_managed_vk_item_for_event", fake_find)

    assert await main._recover_managed_vk_live_url(db, event, bot=None)
    assert event.source_vk_post_url == "https://vk.com/wall-231920894_7269"
    async with db.get_session() as session:
        saved = await session.get(main.Event, event_id)
        source_urls = list(
            (
                await session.execute(
                    main.select(EventSource.source_url).where(EventSource.event_id == event_id)
                )
            ).scalars()
        )
    assert saved.source_vk_post_url == "https://vk.com/wall-231920894_7269"
    assert source_urls == ["https://vk.com/wall-231920894_7269"]
    await db.close()


@pytest.mark.asyncio
async def test_live_managed_vk_item_recovery_fails_closed_when_ambiguous(monkeypatch):
    monkeypatch.setattr(
        main,
        "_vk_wall_get_actors",
        lambda owner_id: [SimpleNamespace(kind="user", token="user-token", label="user")],
    )
    expected = "Камерный концерт\n\n\U0001f4c5 20 июня 19:00\n\U0001f4cd Концертный зал"

    async def fake_vk_api(method, params, *args, **kwargs):
        assert method == "wall.get"
        return {"response": {"items": [{"id": 7268, "text": expected}, {"id": 7269, "text": expected}]}}

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    item = await main._find_unique_live_managed_vk_item_for_event(
        _event(), owner_id=-231920894, db=None, bot=None
    )
    assert item is None


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_defers_night_and_spaces_jobs(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "7")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")

    night_utc = main.datetime(2026, 6, 7, 0, 30, tzinfo=main.timezone.utc)
    first = await main.next_tg_event_publish_run_at(db, now=night_utc)
    assert first.astimezone(main.LOCAL_TZ).hour == 7
    assert first.astimezone(main.LOCAL_TZ).minute == 0

    async with db.get_session() as session:
        session.add(
            main.JobOutbox(
                event_id=42,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.pending,
                updated_at=night_utc,
                next_run_at=first,
            )
        )
        await session.commit()

    second = await main.next_tg_event_publish_run_at(db, now=night_utc)
    assert second == first + main.timedelta(minutes=10)


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_spreads_same_source_afisha(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "0")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    monkeypatch.setenv("SAME_SOURCE_EVENT_PUBLISH_INTERVAL_HOURS", "12")
    original_tz = main.LOCAL_TZ
    main.LOCAL_TZ = main.timezone.utc
    try:
        now = main.datetime(2026, 6, 14, 8, 0, tzinfo=main.timezone.utc)
        source_url = "https://vk.com/wall-194927034_4698"
        existing = _event(id=None, source_post_url=source_url)
        async with db.get_session() as session:
            session.add(existing)
            await session.commit()
            await session.refresh(existing)
            session.add(
                main.JobOutbox(
                    event_id=int(existing.id),
                    task=main.JobTask.tg_event_publish,
                    status=main.JobStatus.pending,
                    updated_at=now,
                    next_run_at=now,
                )
            )
            await session.commit()

        scheduled = await main.next_tg_event_publish_run_at(
            db,
            now=now,
            source_url=source_url,
        )

        assert scheduled == now + main.timedelta(hours=12)
    finally:
        main.LOCAL_TZ = original_tz


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_ignores_far_future_cancelled_backlog(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "7")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    monkeypatch.setenv("TG_EVENT_PUBLISH_SPACING_HORIZON_HOURS", "24")

    now = main.datetime(2026, 6, 8, 10, 45, tzinfo=main.timezone.utc)
    far_future = now + main.timedelta(days=3650)
    async with db.get_session() as session:
        session.add(
            main.JobOutbox(
                event_id=42,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.error,
                last_error="cancelled_bad_dependency_key_after_fanout_fix",
                updated_at=now,
                next_run_at=far_future,
            )
        )
        session.add(
            main.JobOutbox(
                event_id=43,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.pending,
                updated_at=now,
                next_run_at=far_future,
            )
        )
        await session.commit()

    scheduled = await main.next_tg_event_publish_run_at(db, now=now)
    assert scheduled == main._normalize_tg_event_publish_run_at(now)


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_ignores_next_day_pending_anchor_when_window_open(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "7")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")

    now = main.datetime(2026, 6, 8, 17, 5, tzinfo=main.timezone.utc)
    tomorrow_anchor = now + main.timedelta(hours=23)
    async with db.get_session() as session:
        session.add(
            main.JobOutbox(
                event_id=5787,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.pending,
                updated_at=now,
                next_run_at=tomorrow_anchor,
            )
        )
        await session.commit()

    scheduled = await main.next_tg_event_publish_run_at(db, now=now)

    assert scheduled == main._normalize_tg_event_publish_run_at(now)


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_ignores_late_next_day_backlog_after_window(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    original_tz = main.LOCAL_TZ
    main.LOCAL_TZ = main.timezone(main.timedelta(hours=2))
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "7")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    try:
        now = main.datetime(2026, 6, 8, 21, 17, tzinfo=main.timezone.utc)
        morning = main._normalize_tg_event_publish_run_at(now)
        evening_backlog = main.datetime(2026, 6, 9, 16, 0, tzinfo=main.timezone.utc)
        async with db.get_session() as session:
            session.add(
                main.JobOutbox(
                    event_id=5787,
                    task=main.JobTask.tg_event_publish,
                    status=main.JobStatus.pending,
                    updated_at=now,
                    next_run_at=evening_backlog,
                )
            )
            await session.commit()

        scheduled = await main.next_tg_event_publish_run_at(db, now=now)

        assert scheduled == morning
        assert scheduled.astimezone(main.LOCAL_TZ).hour == 7
    finally:
        main.LOCAL_TZ = original_tz


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_uses_open_gap_before_late_same_day_backlog(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    original_tz = main.LOCAL_TZ
    main.LOCAL_TZ = main.timezone(main.timedelta(hours=2))
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "7")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    try:
        now = main.datetime(2026, 6, 9, 6, 42, tzinfo=main.timezone.utc)
        async with db.get_session() as session:
            session.add(
                main.JobOutbox(
                    event_id=5682,
                    task=main.JobTask.tg_event_publish,
                    status=main.JobStatus.done,
                    updated_at=main.datetime(2026, 6, 9, 6, 40, tzinfo=main.timezone.utc),
                    next_run_at=main.datetime(2026, 6, 9, 6, 40, tzinfo=main.timezone.utc),
                )
            )
            session.add(
                main.JobOutbox(
                    event_id=5692,
                    task=main.JobTask.tg_event_publish,
                    status=main.JobStatus.pending,
                    updated_at=main.datetime(2026, 6, 8, 15, 59, tzinfo=main.timezone.utc),
                    next_run_at=main.datetime(2026, 6, 9, 16, 0, tzinfo=main.timezone.utc),
                )
            )
            session.add(
                main.JobOutbox(
                    event_id=5806,
                    task=main.JobTask.tg_event_publish,
                    status=main.JobStatus.pending,
                    updated_at=main.datetime(2026, 6, 9, 0, 15, tzinfo=main.timezone.utc),
                    next_run_at=main.datetime(2026, 6, 9, 20, 50, tzinfo=main.timezone.utc),
                )
            )
            await session.commit()

        scheduled = await main.next_tg_event_publish_run_at(db, now=now)

        assert scheduled == main.datetime(2026, 6, 9, 6, 50, tzinfo=main.timezone.utc)
    finally:
        main.LOCAL_TZ = original_tz


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_fresh_import_ignores_old_same_day_backlog(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    original_tz = main.LOCAL_TZ
    main.LOCAL_TZ = main.timezone.utc
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "7")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    monkeypatch.setenv("TG_EVENT_PUBLISH_FRESH_QUEUE_HOURS", "3")
    try:
        now = main.datetime(2026, 6, 29, 17, 3, tzinfo=main.timezone.utc)
        async with db.get_session() as session:
            session.add(
                main.JobOutbox(
                    event_id=1000,
                    task=main.JobTask.tg_event_publish,
                    status=main.JobStatus.done,
                    updated_at=main.datetime(2026, 6, 29, 16, 57, tzinfo=main.timezone.utc),
                    next_run_at=main.datetime(2026, 6, 29, 16, 57, tzinfo=main.timezone.utc),
                )
            )
            for offset_min in range(7, 247, 10):
                event = _event(id=None, added_at=now - main.timedelta(days=14))
                session.add(event)
                await session.flush()
                session.add(
                    main.JobOutbox(
                        event_id=int(event.id),
                        task=main.JobTask.tg_event_publish,
                        status=main.JobStatus.pending,
                        updated_at=now - main.timedelta(hours=1),
                        next_run_at=now + main.timedelta(minutes=offset_min),
                    )
                )
            await session.commit()

        scheduled = await main.next_tg_event_publish_run_at(
            db,
            now=now,
            prefer_fresh=True,
        )

        assert scheduled == main.datetime(2026, 6, 29, 17, 7, tzinfo=main.timezone.utc)
    finally:
        main.LOCAL_TZ = original_tz


@pytest.mark.asyncio
async def test_enqueue_tg_publish_rearm_replaces_stale_next_day_slot(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    event = _event(id=None)
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)
        session.add(
            main.JobOutbox(
                event_id=event_id,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.pending,
                depends_on=f"telegraph_build:{event_id},vk_sync:{event_id}",
                next_run_at=main.datetime(2026, 6, 9, 16, 40, tzinfo=main.timezone.utc),
                updated_at=main.datetime(2026, 6, 8, 17, 0, tzinfo=main.timezone.utc),
            )
        )
        await session.commit()

    current_cycle_slot = main.datetime(2026, 6, 8, 17, 20, tzinfo=main.timezone.utc)
    result = await main.enqueue_job(
        db,
        event_id,
        main.JobTask.tg_event_publish,
        depends_on=[f"telegraph_build:{event_id}"],
        replace_depends_on=True,
        next_run_at=current_cycle_slot,
    )

    async with db.get_session() as session:
        job = (
            await session.execute(
                main.select(main.JobOutbox).where(
                    main.JobOutbox.event_id == event_id,
                    main.JobOutbox.task == main.JobTask.tg_event_publish,
                )
            )
        ).scalar_one()

    assert result == "merged-rearmed"
    assert main._ensure_utc(job.next_run_at) == current_cycle_slot


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_skips_tg_publish_for_past(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(
        id=None,
        date=(main.datetime.now(main.LOCAL_TZ).date() - main.timedelta(days=1)).isoformat(),
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert main.JobTask.tg_event_publish not in tasks


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_skips_same_day_started_event(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    local_now = main.datetime.now(main.LOCAL_TZ)
    started_at = (local_now - main.timedelta(hours=1)).strftime("%H:%M")
    event = _event(id=None, date=local_now.date().isoformat(), time=started_at)
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert main.JobTask.vk_sync not in tasks
    assert main.JobTask.tg_event_publish not in tasks


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_skips_started_event_with_inferred_end_date(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    local_now = main.datetime.now(main.LOCAL_TZ)
    started_at = (local_now - main.timedelta(hours=1)).strftime("%H:%M")
    event = _event(
        id=None,
        date=local_now.date().isoformat(),
        time=started_at,
        end_date=(local_now.date() + main.timedelta(days=30)).isoformat(),
        end_date_is_inferred=True,
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert main.JobTask.vk_sync not in tasks
    assert main.JobTask.tg_event_publish not in tasks


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_allows_started_event_with_explicit_end_date(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    local_now = main.datetime.now(main.LOCAL_TZ)
    started_at = (local_now - main.timedelta(hours=1)).strftime("%H:%M")
    event = _event(
        id=None,
        date=local_now.date().isoformat(),
        time=started_at,
        end_date=(local_now.date() + main.timedelta(days=30)).isoformat(),
        end_date_is_inferred=False,
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert main.JobTask.vk_sync in tasks
    assert main.JobTask.tg_event_publish in tasks


@pytest.mark.asyncio
async def test_post_to_vk_skips_reserved_slot_after_start_deadline(monkeypatch):
    async def fake_reserve(*args, **kwargs):
        return 200

    async def fail_vk_api(*args, **kwargs):  # pragma: no cover - must not be called
        raise AssertionError("wall.post should not be called for stale scheduled slot")

    monkeypatch.setattr(main, "VK_POSTPONED_ENABLED", True)
    monkeypatch.setattr(
        main,
        "choose_vk_actor",
        lambda owner_id, permission: [
            SimpleNamespace(kind="group", token="token", label="test")
        ],
    )
    monkeypatch.setattr(main, "_reserve_vk_postponed_publish_date", fake_reserve)
    monkeypatch.setattr(main, "_vk_api", fail_vk_api)

    url = await main.post_to_vk(
        "231920894",
        "message",
        source_event_id=6346,
        latest_publish_ts=100,
    )

    assert url is None


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_skips_ticket_giveaway_when_alternative_exists(
    tmp_path,
    monkeypatch,
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    alternative = _event(
        id=None,
        title="Обычный концерт",
        source_text="Анонс концерта без розыгрыша.",
        tg_event_post_url="https://t.me/kldevents/500",
    )
    giveaway = _event(
        id=None,
        title="Розыгрыш билетов",
        source_text="Разыгрываем два билета: подпишитесь и отметьте друга.",
    )
    async with db.get_session() as session:
        session.add(alternative)
        session.add(giveaway)
        await session.commit()
        await session.refresh(giveaway)

    await main.schedule_event_update_tasks(db, giveaway, skip_vk_sync=False)

    assert main.JobTask.telegraph_build in tasks
    assert main.JobTask.vk_sync not in tasks
    assert main.JobTask.tg_event_publish not in tasks
    await db.close()


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_publishes_cleaned_event_with_giveaway_source(
    tmp_path,
    monkeypatch,
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    async def fake_has_managed_vk_post(event):
        return False

    monkeypatch.setattr(
        main,
        "_event_has_existing_managed_vk_post",
        fake_has_managed_vk_post,
    )

    alternative = _event(
        id=None,
        title="Обычный концерт",
        source_text="Анонс концерта без розыгрыша.",
        tg_event_post_url="https://t.me/kldevents/500",
    )
    fair = _event(
        id=None,
        title="Путешествие в сказку в деревне Холмогорье",
        event_type="ярмарка",
        source_post_url="https://vk.com/wall-146688375_7432",
        source_vk_post_url="https://vk.com/wall-146688375_7432",
        source_text=(
            "Поздравляем победителей розыгрыша. Каждый билет на 4 человека. "
            "13 июня сказочная деревня Холмогорье снова распахнет двери."
        ),
        short_description=(
            "Ярмарка в Холмогорье с конкурсами костюмов и рисунков, "
            "выступлением ходулистов, клоуном Мультиком и запуском "
            "воздушных змеев для всей семьи."
        ),
        description=(
            "Деревня Холмогорье превращается в пространство для тех, кто верит "
            "в чудеса и стремится провести время в кругу семьи.\n\n"
            "В рамках ярмарки запланированы конкурс костюмов, конкурс рисунков, "
            "выступление ходулистов, анимация с клоуном Мультиком и запуск "
            "воздушных змеев.\n\n"
            "Итоги розыгрыша: победители получили билеты."
        ),
    )
    async with db.get_session() as session:
        session.add(alternative)
        session.add(fair)
        await session.commit()
        await session.refresh(fair)

    await main.schedule_event_update_tasks(db, fair, skip_vk_sync=False)

    assert main.JobTask.vk_sync in tasks
    assert main.JobTask.tg_event_publish in tasks
    await db.close()


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_allows_ticket_giveaway_without_alternative(
    tmp_path,
    monkeypatch,
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    async def fake_has_managed_vk_post(event):
        return False

    monkeypatch.setattr(
        main,
        "_event_has_existing_managed_vk_post",
        fake_has_managed_vk_post,
    )

    giveaway = _event(
        id=None,
        title="Розыгрыш билетов",
        source_text="Разыгрываем два билета: подпишитесь и отметьте друга.",
    )
    async with db.get_session() as session:
        session.add(giveaway)
        await session.commit()
        await session.refresh(giveaway)

    await main.schedule_event_update_tasks(db, giveaway, skip_vk_sync=False)

    assert main.JobTask.vk_sync in tasks
    assert main.JobTask.tg_event_publish in tasks
    await db.close()


def _medallion_test_config():
    def ids(prefix, *, rows=4, cols=4):
        return [[f"{prefix}{r}{c}" for c in range(1, cols + 1)] for r in range(1, rows + 1)]
    return {
        "rows": 4,
        "cols": 4,
        "max_medallions": 3,
        "items": {
            "pushkin-card": {
                "label": "Пушкинская карта",
                "priority": 1,
                "aliases": ["Пушкинская карта"],
                "emoji_ids": ids("p"),
            },
            "kgd80": {
                "label": "80 историй",
                "priority": 10,
                "aliases": ["80 историй о главном"],
                "emoji_ids": ids("k"),
            },
            "kgd80-znanie": {
                "label": "80 историй + Знание",
                "priority": 10,
                "aliases": ["80 историй о главном", "kgd80.ru"],
                "rows": 4,
                "cols": 7,
                "emoji_ids": ids("x", cols=7),
            },
            "znanie-russia": {
                "label": "Знание",
                "priority": 20,
                "aliases": ["Знание"],
                "emoji_ids": ids("z"),
            },
            "history-art-museum": {
                "label": "КОИХМ",
                "priority": 30,
                "aliases": ["Историко-художественный музей"],
                "emoji_ids": ids("h"),
            },

            "yantar-hall": {
                "label": "Янтарь холл",
                "priority": 11,
                "aliases": ["Янтарь холл"],
                "emoji_ids": ids("y"),
            },
            "kant-island": {
                "label": "Остров Канта",
                "priority": 21,
                "aliases": ["Остров Канта", "Кафедральный собор"],
                "emoji_ids": ids("o"),
            },
            "tretyakovka-kaliningrad": {
                "label": "Третьяковка",
                "priority": 31,
                "aliases": ["Филиал Третьяковской галереи", "Третьяковка"],
                "emoji_ids": ids("t"),
            },
            "simfoniya-vetra": {
                "label": "Симфония ветра",
                "priority": 91,
                "aliases": ["Симфония ветра"],
                "emoji_ids": ids("s"),
            },
            "world-ocean-museum": {
                "label": "ММО",
                "priority": 40,
                "aliases": ["Музей Мирового океана", "ММО", "World Ocean Museum"],
                "emoji_ids": ids("m"),
            },
        },
    }



def test_tg_event_announcement_places_medallions_before_details_footer(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="Калининградский морской торговый порт",
            festival="80 историй о главном",
            location_name="Историко-художественный музей",
        )
        html_text = main.build_tg_event_announcement(event, "Описание.")
    finally:
        tg_medallions.reset_medallion_config_cache()

    assert html_text.count('<tg-emoji emoji-id=') == 44
    assert "⠀\n\n🔎 Подробнее" not in html_text
    assert '⠀\n<a href="https://telegra.ph/event">🔎 Подробнее</a>' in html_text
    medallion_pos = html_text.index('<tg-emoji emoji-id=')
    details_pos = html_text.index("🔎 Подробнее")
    assert medallion_pos < details_pos


def test_tg_medallions_do_not_match_short_acronym_inside_words(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="Показ фильма «Переселенцы. История Первых»",
            description="В рамках программы состоится творческая встреча с Эммой Басовой.",
            search_digest="Показ фильма и встреча с Эммой Басовой.",
            location_name="Дом молодёжи",
        )
        slugs = [item["slug"] for item in tg_medallions.resolve_event_medallions(event)]
    finally:
        tg_medallions.reset_medallion_config_cache()

    assert "world-ocean-museum" not in slugs


def test_tg_medallions_match_short_acronym_as_standalone_token(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="Лекция ММО",
            description="Встреча в ММО.",
            location_name="Музей Мирового океана",
        )
        slugs = [item["slug"] for item in tg_medallions.resolve_event_medallions(event)]
    finally:
        tg_medallions.reset_medallion_config_cache()

    assert "world-ocean-museum" in slugs



def test_tg_medallions_match_venue_aliases_only_against_location_fields(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="Таланты и покойники",
            description=(
                "Фильм рассказывает про остров Канта, Кафедральный собор "
                "и другие достопримечательности региона."
            ),
            search_digest="Культурное наследие, остров Канта и Кафедральный собор.",
            location_name="Филиал Третьяковской галереи",
            location_address="Парадная наб. 3",
        )
        slugs = [item["slug"] for item in tg_medallions.resolve_event_medallions(event)]
    finally:
        tg_medallions.reset_medallion_config_cache()

    assert slugs == ["tretyakovka-kaliningrad"]


def test_tg_medallions_do_not_add_festival_badge_as_location_match(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="ROCK N ROLL CITY с программой Встречая закат",
            description="Концерт проходит в рамках фестиваля искусств Симфония ветра.",
            festival="Симфония ветра",
            location_name="Янтарь холл",
            location_address="Ленина 11",
        )
        slugs = [item["slug"] for item in tg_medallions.resolve_event_medallions(event)]
    finally:
        tg_medallions.reset_medallion_config_cache()

    assert slugs == ["yantar-hall"]

def test_tg_event_album_footer_keeps_deliberate_social_gap():
    event = _event()

    text = main.build_tg_event_announcement(
        event,
        "Описание.",
        include_calendar_link=True,
    )

    assert '<a href="https://telegra.ph/event">🔎 Подробнее</a>            <a href="https://max.ru/channel_kenigevents">Max</a>' in text


def test_tg_graphic_medallions_resolve_konb_kgd80_and_znanie_for_event_6811():
    event = _event(
        id=6811,
        title="Встреча в библиотеке",
        location_name="Калининградская областная научная библиотека",
        location_address="проспект Мира 9",
        festival="80 историй о главном",
        pushkin_card=False,
    )

    medallions = main.resolve_event_graphic_medallions(event)

    assert [item["slug"] for item in medallions] == [
        "konb",
        "kgd80-80-stories",
        "znanie-russia",
    ]
    strip = main.render_event_graphic_medallion_strip(medallions)
    with Image.open(io.BytesIO(strip)) as rendered:
        assert rendered.size == (1300, 330)


def test_tg_graphic_medallions_do_not_duplicate_explicit_znanie_for_kgd80():
    event = _event(
        title="Лекторий Российского общества Знание",
        location_name="Калининградская областная научная библиотека",
        festival="80 историй о главном",
        tg_source_author="Российское общество Знание",
    )

    slugs = [item["slug"] for item in main.resolve_event_graphic_medallions(event)]

    assert slugs.count("znanie-russia") == 1
    assert slugs == ["konb", "kgd80-80-stories", "znanie-russia"]


def test_tg_graphic_medallions_never_add_source_channel_badges():
    event = _event(
        title="Kino",
        location_name="Видеосалон / Плохой охотник",
        source_post_url="https://t.me/meowafisha/7951",
        source_vk_post_url="https://vk.com/wall-231920894_7640",
        source_urls=["https://t.me/meowafisha/7951"],
    )

    assert main.resolve_event_graphic_medallions(event) == []


def test_tg_graphic_medallions_keep_real_venue_badge_for_meow_source():
    event = _event(
        title="Встреча в библиотеке",
        location_name="Калининградская областная научная библиотека",
        source_post_url="https://t.me/meowafisha/7951",
        source_vk_post_url="https://vk.com/wall-231920894_7640",
    )

    slugs = [item["slug"] for item in main.resolve_event_graphic_medallions(event)]

    assert slugs == ["konb"]


@pytest.mark.asyncio
async def test_tg_event_publish_replaces_source_only_rich_message_after_send(monkeypatch):
    event = _event(
        id=6931,
        title="Kino",
        location_name="Видеосалон / Плохой охотник",
        source_post_url="https://t.me/meowafisha/7951",
        source_vk_post_url="https://vk.com/wall-231920894_7640",
        photo_urls=["https://img.example/poster.jpg"],
        tg_event_post_id=2556,
        tg_event_post_mode="rich_message",
        tg_event_post_url="https://t.me/c/3954607218/2556",
        tg_event_source_hash="source-medallion-hash",
    )
    materialize_calls = _patch_media_materializer(monkeypatch)
    scheduled = []

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        assert event_arg is event
        assert source_text == "Описание."
        return "Описание."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    monkeypatch.setattr(
        main,
        "_schedule_tg_premium_emoji_editor",
        lambda *args, **kwargs: scheduled.append((args, kwargs)),
    )
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        "Описание.",
        None,
        bot,
    )

    assert (url, post_id, mode) == (
        "https://t.me/c/1234567890/101",
        101,
        "photo_caption",
    )
    assert source_hash and source_hash != "source-medallion-hash"
    assert materialize_calls == [("https://img.example/poster.jpg", 0)]
    assert len(bot.photos) == 1
    assert bot.rich_messages == []
    assert bot.deleted == [{"chat_id": "@kldevents", "message_id": 2556}]
    assert scheduled and scheduled[0][0] == ([("@kldevents", 101)],)


def test_tg_rich_message_footer_uses_non_collapsing_semantic_gap():
    event = _event()
    ordinary = main.build_tg_event_announcement(event, "Описание.", include_medallions=False)

    rich_html = main.build_tg_event_rich_message_html(ordinary, event_media_count=1)

    assert rich_html.count("&nbsp;") == main.TG_EVENT_SOCIAL_LINK_GAP_SPACES
    assert 'tg://photo?id=medallions' in rich_html
    assert rich_html.index('tg://photo?id=medallions') < rich_html.index("🔎 Подробнее")


def test_aiogram_serializes_extra_rich_media_as_multipart_attachments():
    rich_message = main.build_tg_event_input_rich_message(
        "<b>Событие</b>\n\n🔎 Подробнее            Max · Вконтакте",
        [
            main.types.BufferedInputFile(b"poster-one", filename="one.jpg"),
            main.types.BufferedInputFile(b"poster-two", filename="two.jpg"),
        ],
        b"medallion-strip",
    )
    bot = Bot("123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789")
    session = AiohttpSession()
    files = {}

    serialized = session.prepare_value(rich_message, bot=bot, files=files)
    payload = json.loads(serialized)

    assert [item["id"] for item in payload["media"]] == [
        "event-0",
        "event-1",
        "medallions",
    ]
    references = [item["media"]["media"] for item in payload["media"]]
    assert all(reference.startswith("attach://") for reference in references)
    assert {reference.removeprefix("attach://") for reference in references} == set(files)


@pytest.mark.asyncio
async def test_tg_event_publish_sends_graphical_medallions_as_rich_message(monkeypatch):
    event = _event(
        id=6811,
        title="Встреча в библиотеке",
        location_name="Калининградская областная научная библиотека",
        festival="80 историй о главном",
        photo_urls=[
            "https://img.example/poster.jpg",
            "https://img.example/program.jpg",
        ],
        pushkin_card=False,
    )
    materialize_calls = _patch_media_materializer(monkeypatch)
    scheduled = []

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        assert event_arg is event
        assert source_text == "Описание."
        return "Описание."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    monkeypatch.setattr(
        main,
        "_schedule_tg_premium_emoji_editor",
        lambda *args, **kwargs: scheduled.append((args, kwargs)),
    )
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        "Описание.",
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "rich_message"
    assert source_hash
    assert materialize_calls == [
        ("https://img.example/poster.jpg", 0),
        ("https://img.example/program.jpg", 1),
    ]
    assert len(bot.rich_messages) == 1
    rich_message = bot.rich_messages[0][1]
    assert '<tg-emoji' not in rich_message.html
    assert rich_message.html.count("&nbsp;") == 12
    assert [item["id"] for item in rich_message.media] == [
        "event-0",
        "event-1",
        "medallions",
    ]
    assert not bot.photos
    assert not bot.media_groups
    assert scheduled == []


@pytest.mark.asyncio
async def test_tg_event_publish_edits_existing_rich_message_in_place(monkeypatch):
    event = _event(
        id=6811,
        location_name="Калининградская областная научная библиотека",
        festival="80 историй о главном",
        photo_urls=["https://img.example/poster.jpg"],
        tg_event_post_id=77,
        tg_event_post_mode="rich_message",
        tg_event_post_url="https://t.me/c/1234567890/77",
        tg_event_source_hash="old",
    )

    async def fake_hook_text(event_arg, source_text, **_kwargs):
        return "Обновлённое описание."

    class RichEditBot(DummyTgBot):
        def __init__(self):
            super().__init__()
            self.rich_edits = []

        async def edit_message_text(self, **kwargs):
            self.rich_edits.append(kwargs)
            return True

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    _patch_media_materializer(monkeypatch)
    bot = RichEditBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        "source",
        None,
        bot,
    )

    assert (url, post_id, mode) == (
        "https://t.me/c/1234567890/77",
        77,
        "rich_message",
    )
    assert source_hash and source_hash != "old"
    assert len(bot.rich_edits) == 1
    assert bot.rich_edits[0]["message_id"] == 77
    assert bot.rich_edits[0]["text"] is None
    assert bot.rich_edits[0]["rich_message"].media[-1]["id"] == "medallions"
    assert bot.rich_messages == []
    assert bot.deleted == []


@pytest.mark.asyncio
async def test_tg_event_publish_job_does_not_enqueue_premium_editor_for_rich_message(
    tmp_path,
    monkeypatch,
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event = _event(
        id=None,
        date="2027-07-19",
        time="18:30",
        location_name="Калининградская областная научная библиотека",
        festival="80 историй о главном",
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)

    async def fake_publish(*args, **kwargs):
        return "https://t.me/kldevents/3000", 3000, "rich_message", "rich-hash"

    premium_enqueues = []

    async def fake_enqueue(*args, **kwargs):
        premium_enqueues.append((args, kwargs))
        return "unexpected"

    monkeypatch.setattr(main, "publish_tg_event_announcement", fake_publish)
    monkeypatch.setattr(main, "enqueue_tg_event_premium_emoji_edit_job", fake_enqueue)

    assert await main.job_publish_tg_event_post(event_id, db, bot=object()) is True
    assert premium_enqueues == []
    async with db.get_session() as session:
        stored = await session.get(main.Event, event_id)
    assert stored.tg_event_post_mode == "rich_message"
    assert stored.tg_event_post_id == 3000
    await db.close()


@pytest.mark.asyncio
async def test_stale_premium_editor_job_skips_current_rich_message(tmp_path, monkeypatch):
    import tg_premium_emojis

    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event = _event(
        id=None,
        date="2027-07-19",
        tg_event_post_id=3000,
        tg_event_post_mode="rich_message",
        tg_event_post_url="https://t.me/kldevents/3000",
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)

    calls = []

    async def fake_edit(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("legacy editor must not touch RichMessage")

    monkeypatch.setattr(tg_premium_emojis, "edit_messages_with_env", fake_edit)
    monkeypatch.setattr(tg_premium_emojis, "premium_emoji_editor_enabled", lambda: True)

    assert await main.job_edit_tg_event_premium_emoji(event_id, db, bot=None) is False
    assert calls == []
    await db.close()


def test_tg_promo_medallion_block_uses_custom_emoji_entities(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="Калининград корабельный",
            festival="80 историй о главном",
            location_name="Историко-художественный музей",
        )
        html_text = main.build_tg_promo_event_publication_message(event)
        text, entities = main.telegram_event_html_to_text_entities(html_text)
    finally:
        tg_medallions.reset_medallion_config_cache()

    custom = [entity for entity in entities if getattr(entity, "type", None) == "custom_emoji"]
    assert len(custom) == 44  # combined 7x4 KGD80+Znanie + one 4x4 location medallion
    assert {entity.custom_emoji_id[0] for entity in custom} == {"x", "h"}
    assert text.count("🟧") == 44


def test_tg_promo_medallion_block_prioritizes_pushkin_and_limits_two(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="Лекция по Пушкинской карте",
            festival="80 историй о главном",
            location_name="Историко-художественный музей",
            pushkin_card=True,
        )
        html_text = main.build_tg_promo_event_publication_message(event)
        _, entities = main.telegram_event_html_to_text_entities(html_text)
    finally:
        tg_medallions.reset_medallion_config_cache()

    custom = [entity for entity in entities if getattr(entity, "type", None) == "custom_emoji"]
    assert len(custom) == 44
    # Pushkin is mandatory; max two means only the combined festival/partner signal remains.
    assert {entity.custom_emoji_id[0] for entity in custom} == {"p", "x"}


def test_tg_event_announcement_can_omit_medallions_for_bot_channel_send(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="Калининградский морской торговый порт",
            festival="80 историй о главном",
            location_name="Историко-художественный музей",
        )
        html_text = main.build_tg_event_announcement(
            event,
            "Описание.",
            include_medallions=False,
        )
    finally:
        tg_medallions.reset_medallion_config_cache()

    assert '<tg-emoji emoji-id=' not in html_text
    assert "🟧" not in html_text
    assert "🔎 Подробнее" in html_text
    assert '\n\n<a href="https://telegra.ph/event">🔎 Подробнее</a>' in html_text


def test_tg_medallion_selection_caps_at_two_for_telegram(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            festival="80 историй о главном",
            location_name="Историко-художественный музей",
        )
        html_text = main.build_tg_event_announcement(event, "Описание.")
    finally:
        tg_medallions.reset_medallion_config_cache()

    medallion_lines = [line for line in html_text.splitlines() if "<tg-emoji" in line]
    assert [line.count("<tg-emoji") for line in medallion_lines] == [11, 11, 11, 11]
    assert "\u200a" in medallion_lines[0]


def test_tg_medallion_keeps_standalone_znanie_outside_kgd80(monkeypatch):
    import json
    import tg_medallions

    monkeypatch.setenv("TG_MEDALLION_CUSTOM_EMOJI_JSON", json.dumps(_medallion_test_config()))
    tg_medallions.reset_medallion_config_cache()
    try:
        event = _event(
            title="Лекторий общества Знание",
            description="Встреча Российского общества «Знание».",
        )
        html_text = main.build_tg_event_announcement(event, "Описание.")
        _, entities = main.telegram_event_html_to_text_entities(html_text)
    finally:
        tg_medallions.reset_medallion_config_cache()

    custom = [entity for entity in entities if getattr(entity, "type", None) == "custom_emoji"]
    assert len(custom) == 16
    assert {entity.custom_emoji_id[0] for entity in custom} == {"z"}
