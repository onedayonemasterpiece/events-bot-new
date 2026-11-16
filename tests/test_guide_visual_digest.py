from __future__ import annotations

from datetime import date

import pytest

from guide_excursions import visual_digest as vd

from guide_excursions.visual_digest import (
    _BRAND_LOCKUP_FILE,
    _avatar_key,
    _brand_lockup,
    _source_name,
    _visual_start_iso,
    _visual_item_state,
    _visual_selection_reason,
    build_visual_digest_telegram_text,
    build_visual_digest_vk_text,
    render_visual_digest_story_image,
    render_visual_digest_cards,
    render_visual_digest_card,
)


SAMPLE_ROWS = [
    {
        "id": 1,
        "canonical_title": "Самые красивые виллы Амалиенау",
        "date": "2026-07-02",
        "time": "18:30",
        "city": "Калининград",
        "guide_names": ["Таня Бурдужан"],
        "route_summary": "Амалиенау",
        "seats_text": "осталось 3 места",
        "booking_url": "https://vk.com/wall-1_2",
        "source_post_url": "https://vk.com/wall-1_2",
    },
    {
        "id": 2,
        "canonical_title": "Вармийское кольцо",
        "date": "2026-07-03",
        "time": "09:00",
        "city": "Калининградская область",
        "guide_names": ["Татьяна Удовенко"],
        "route_summary": "Ушаково → Ладушкин → Багратионовск",
        "booking_url": "https://t.me/example/10",
        "source_post_url": "https://t.me/source/20",
    },
]


@pytest.mark.asyncio
async def test_vk_upload_keeps_full_card_as_png(monkeypatch):
    import io
    from PIL import Image
    from guide_excursions import visual_digest as vd

    rendered = io.BytesIO()
    Image.new("RGB", (1080, 1350), "white").save(rendered, format="JPEG")

    async def issue(*_args, **_kwargs):
        return {"items": SAMPLE_ROWS}

    async def group(**_kwargs):
        return 238875824

    async def caption(*_args, **_kwargs):
        return "Дайджест экскурсий"

    monkeypatch.setattr(vd, "load_visual_digest_issue", issue)
    monkeypatch.setattr(vd, "_resolve_vk_group_id", group)
    monkeypatch.setattr(vd, "build_visual_digest_vk_text", caption)
    monkeypatch.setattr(vd, "render_visual_digest_cards", lambda *_a, **_kw: [rendered.getvalue()])
    monkeypatch.setattr(vd, "VISUAL_DIGEST_STORIES_ENABLED", False)

    async def upload(_group, payload, *_args, filename, **_kwargs):
        with Image.open(io.BytesIO(payload)) as image:
            assert image.format == "PNG"
            assert image.size == (1080, 1350)
        assert filename.endswith(".png")
        return "photo868977531_457264569"

    class ReachedWallPost(Exception):
        pass

    async def post(*_args, **kwargs):
        assert kwargs["attachments"] == ["photo868977531_457264569"]
        raise ReachedWallPost

    with pytest.raises(ReachedWallPost):
        await vd._publish_visual_digest_to_vk_once(
            object(), issue_id=328, vk_api_fn=object(),
            post_to_vk_fn=post, upload_vk_photo_bytes_fn=upload,
        )


@pytest.fixture(autouse=True)
def _no_story_writer(monkeypatch):
    async def writer(payload, *, issue_id):
        return None

    monkeypatch.setattr(vd, "ask_vk_narrative", writer)


@pytest.mark.asyncio
async def test_visual_digest_vk_text_uses_vk_clickable_and_shortens_external_links():
    calls = []

    async def fake_vk(method, params, db=None, bot=None, **kwargs):
        calls.append((method, params["url"]))
        return {"response": {"short_url": f"https://vk.cc/{len(calls)}"}}

    text = await build_visual_digest_vk_text(SAMPLE_ROWS, issue_id=42, vk_api_fn=fake_vk)

    assert text.splitlines()[0] == "Дайджест экскурсий №42"
    assert "1. [https://vk.com/wall-1_2|Самые красивые виллы Амалиенау]" in text
    assert "2. Вармийское кольцо — vk.cc/1" in text
    assert calls == [("utils.getShortLink", "https://t.me/example/10")]


@pytest.mark.asyncio
async def test_visual_digest_telegram_text_uses_title_links_without_shortener():
    text = await build_visual_digest_telegram_text(SAMPLE_ROWS, issue_id=42)

    assert text.splitlines()[0] == "<b>Дайджест экскурсий №42</b>"
    assert '<a href="https://vk.com/wall-1_2">Самые красивые виллы Амалиенау</a>' in text
    assert '<a href="https://t.me/example/10">Вармийское кольцо</a>' in text
    assert "vk.cc" not in text
    assert "#Калининград" in text
    assert "#Амалиенау" in text
    assert '<a href="https://t.me/wheretogo39">Подписаться</a>' in text
    assert '<a href="https://max.ru/channel_uh_kaliningrad">Max</a>' in text
    assert '<a href="https://vk.ru/uhtykaliningrad">Вконтакте</a>' in text
    assert '#Амалиенау\n\n<a href="https://t.me/wheretogo39">Подписаться</a>' in text


@pytest.mark.asyncio
async def test_visual_digest_telegram_footer_uses_target_channel_subscribe_link():
    text = await build_visual_digest_telegram_text(SAMPLE_ROWS, issue_id=42, target_chat="@youwillsee39")

    assert '<a href="https://t.me/youwillsee39">Подписаться</a>' in text
    assert '<a href="https://vk.ru/uhtykaliningrad">Вконтакте</a>' in text


@pytest.mark.asyncio
async def test_visual_digest_telegram_text_uses_source_link_for_phone_contacts():
    rows = [
        {
            "id": 3,
            "canonical_title": "Школа юного альпаковеда",
            "date": "2026-07-04",
            "booking_text": "+7 (962) 255-54-91",
            "source_post_url": "https://t.me/excursions_profitour/943",
        },
        {
            "id": 4,
            "canonical_title": "Домашняя прогулка",
            "date": "2026-07-05",
            "booking_url": "https://vk.com/natakkaz",
        },
    ]

    text = await build_visual_digest_telegram_text(rows, issue_id=44)

    assert (
        '1. <a href="https://t.me/excursions_profitour/943">Школа юного альпаковеда</a>'
        in text
    )
    assert '+79622555491' not in text
    assert '<a href="https://vk.com/natakkaz">Домашняя прогулка</a>' in text


@pytest.mark.asyncio
async def test_visual_digest_text_demotes_stale_vk_links_from_preliminary_schedule():
    row = {
        "id": 400,
        "canonical_title": "Путешествие по следам советского кино",
        "date": "2026-07-08",
        "post_kind": "announce_multi",
        "booking_url": "https://vk.ru/wall-190663987_8037",
        "booking_text": "not specified",
        "source_post_url": "https://vk.com/wall-190663987_9010",
        "channel_url": "https://vk.com/wall-190663987_9010",
        "dedup_source_text": (
            "Афиша прогулок «Хранителей руин» в июле! "
            "Делимся предварительным расписанием благотворительных прогулок. "
            "8 июля — путешествие по следам советского кино. "
            "Подробные анонсы прогулок будут появляться каждую неделю."
        ),
    }

    tg_text = await build_visual_digest_telegram_text([row], issue_id=148)
    vk_text = await build_visual_digest_vk_text([row], issue_id=148)

    assert "wall-190663987_8037" not in tg_text
    assert "wall-190663987_8037" not in vk_text
    assert '<a href="https://vk.com/wall-190663987_9010">Путешествие по следам советского кино</a>' in tg_text
    assert "[https://vk.com/wall-190663987_9010|Путешествие по следам советского кино]" in vk_text
    assert _visual_item_state(row).get("booking_url") in {"", None}


@pytest.mark.asyncio
async def test_visual_digest_vk_text_keeps_phone_without_shortener():
    calls = []

    async def fake_vk(method, params, db=None, bot=None, **kwargs):  # pragma: no cover - must not be called
        calls.append((method, params))
        return {"response": {"short_url": "https://vk.cc/nope"}}

    rows = [
        {
            "id": 3,
            "canonical_title": "Прогулка по Понарту",
            "date": "2026-07-04",
            "booking_text": "+7 (999) 111-22-33",
        }
    ]
    text = await build_visual_digest_vk_text(rows, issue_id=43, vk_api_fn=fake_vk)
    assert "Прогулка по Понарту — +7 (999) 111-22-33" in text
    assert calls == []


def test_render_visual_digest_card_smoke():
    payload = render_visual_digest_card(SAMPLE_ROWS, issue_id=42, all_rows=SAMPLE_ROWS)
    assert payload.startswith(b"\xff\xd8")
    assert len(payload) > 30_000


def test_render_visual_digest_story_image_keeps_full_width_card():
    payload = render_visual_digest_card(SAMPLE_ROWS, issue_id=42, all_rows=SAMPLE_ROWS)
    story = render_visual_digest_story_image(payload)
    assert story.startswith(b"\xff\xd8")
    from PIL import Image
    import io

    img = Image.open(io.BytesIO(story))
    assert img.size == (1080, 1920)


def test_brand_lockup_uses_committed_v18_asset():
    logo = _brand_lockup()
    assert _BRAND_LOCKUP_FILE.is_file()
    assert logo.size == (358, 141)


def test_visual_digest_candidate_window_starts_tomorrow():
    assert _visual_start_iso(date(2026, 7, 1)) == "2026-07-02"


def test_render_visual_digest_cards_chunks_by_five():
    rows = []
    for idx in range(7):
        item = dict(SAMPLE_ROWS[idx % 2])
        item["id"] = idx + 1
        item["date"] = f"2026-07-{idx+2:02d}"
        item["canonical_title"] = f"Экскурсия {idx+1}"
        rows.append(item)
    cards = render_visual_digest_cards(rows, issue_id=44)
    assert len(cards) == 2
    assert all(card.startswith(b"\xff\xd8") for card in cards)


def test_visual_digest_repeat_policy_requires_serious_change():
    published = {
        **SAMPLE_ROWS[0],
        "published_visual_digest_issue_id": 10,
    }
    state = _visual_item_state(published)

    unchanged = {**published, "updated_at": "2026-07-01 12:00:00", "published_visual_digest_state": state}
    assert _visual_selection_reason(unchanged) == ""

    last_seats = {
        **published,
        "seats_text": "осталось 2 места",
        "published_visual_digest_state": {**state, "seats_text": "12 мест", "seats_count": 12},
    }
    assert _visual_selection_reason(last_seats) == "low_seats"

    moved = {**published, "date": "2026-07-05", "published_visual_digest_state": state}
    assert _visual_selection_reason(moved) == "changed_date"


def test_visual_digest_katya_identity_uses_surname_and_avatar():
    row = {
        "guide_names": ["Катя"],
        "organizer_names": ["ПРОгулки с Катей"],
        "source_username": "progulki_s_katey",
        "booking_url": "https://t.me/katerinakostiugova",
    }
    assert _source_name(row) == "Катя Костюгова"
    assert _avatar_key(row) == "katya_kostyugova"


EDITION_349 = [
    {"id": 745, "canonical_title": "Зелёные истории", "date": "2026-11-17", "time": "14:00",
     "guide_names": ["Ольга Рубцова"], "organizer_names": ["80 лет единства"], "price_text": "500 руб.",
     "booking_url": "https://docs.google.com/forms/d/e/form/viewform", "source_post_url": "https://vk.com/wall-211429559_3892",
     "dedup_source_text": "Программа «80 лет единства»: расписание ноября",
     "participant_medallions": [{"name": "Ольга Рубцова", "roles": ["Гид"], "owner": 41501067},
                                {"name": "80 лет единства", "roles": ["Организатор"], "owner": -237389053}]},
    {"id": 746, "canonical_title": "Рассвет над Балтикой", "date": "2026-11-20", "time": "14:00",
     "guide_names": ["Ноза Усманова"], "price_text": "500 руб.",
     "booking_url": "https://docs.google.com/forms/d/e/form/viewform", "source_post_url": "https://vk.com/wall-211429559_3892",
     "dedup_source_text": "Программа «80 лет единства»: расписание ноября",
     "participant_medallions": [{"name": "Ноза Усманова", "roles": ["Гид"], "owner": 408521495}]},
    {"id": 747, "canonical_title": "Зелёные истории", "date": "2026-11-21", "time": "11:00",
     "guide_names": ["Ольга Рубцова"], "price_text": "500 руб.",
     "booking_url": "https://docs.google.com/forms/d/e/form/viewform", "source_post_url": "https://vk.com/wall-211429559_3892",
     "dedup_source_text": "Программа «80 лет единства»: расписание ноября",
     "participant_medallions": [{"name": "Ольга Рубцова", "roles": ["Гид"], "owner": 41501067}]},
    {"id": 763, "canonical_title": "Секреты чёрного золота: экскурсия на Осетровую ферму", "date": "2026-11-22",
     "guide_names": ["Мария Морозова"], "price_text": "не указано", "booking_text": "8 (911) 858-58-49",
     "source_post_url": "https://vk.com/wall1746008_13815", "dedup_source_text": "Осетровая ферма: 15, 22 и 29 ноября",
     "participant_medallions": [{"name": "Olga Ox", "roles": ["Источник"], "owner": 1746008},
                                {"name": "Мария Морозова", "roles": ["Гид"], "owner": None}]},
]
NARRATIVE_349 = {
    "lead": "Где начинается рассвет в два часа дня?",
    "paragraphs": [
        {"occurrence_ids": [745, 747], "text": "Вдоль ручья пройдёмся с [[1|Ольгой Рубцовой]]."},
        {"occurrence_ids": [746], "text": "У [[3|Нозы Усмановой]] другая отправная точка."},
        {"occurrence_ids": [763], "text": "А в воскресенье можно побывать на осетровой ферме с [[4|Марией Морозовой]]."},
    ],
    "outro": "Пешком или на ферму?",
}


async def _fake_shortener(method, params, db=None, bot=None, **kwargs):
    return {"response": {"short_url": "https://vk.cc/d3XqaE"}}


@pytest.mark.asyncio
async def test_vk_text_wraps_written_story_with_exact_dates_contacts_and_plain_names(monkeypatch):
    seen = {}

    async def writer(payload, *, issue_id):
        seen.update(payload)
        return NARRATIVE_349

    monkeypatch.setattr(vd, "ask_vk_narrative", writer)
    text = await build_visual_digest_vk_text(EDITION_349, issue_id=349, vk_api_fn=_fake_shortener)

    # The writer sees every excursion, one shared programme text and people without the wall owner.
    assert [x["occurrence_id"] for x in seen["excursions"]] == [745, 746, 747, 763]
    assert seen["sources"][0]["excursions"] == [745, 746, 747]
    assert [p["name"] for p in seen["people"]] == ["Ольга Рубцова", "80 лет единства", "Ноза Усманова", "Мария Морозова"]
    assert seen["excursions"][3]["movement"] == []

    assert text.startswith("ЭКСКУРСИИ 17–22 НОЯБРЯ\nГде начинается рассвет в два часа дня?\n\n")
    assert "дайджест экскурсий по Калининградской области от «Ух ты, Калининград!»" in text
    assert ("ЗЕЛЁНЫЕ ИСТОРИИ\nВдоль ручья пройдёмся с Ольгой Рубцовой.\n"
            "17 ноября (вт), 14:00 · 21 ноября (сб), 11:00\n💳 500 руб. · 🔗 Запись: vk.cc/d3XqaE") in text
    assert "РАССВЕТ НАД БАЛТИКОЙ\nУ Нозы Усмановой другая отправная точка.\n20 ноября (пт), 14:00\n" in text
    # Unknown time and price stay absent; the phone is the booking contact.
    assert ("с Марией Морозовой.\n22 ноября (вс)\n☎ Запись по телефону: +7 (911) 858-58-49") in text
    assert "[" not in text and "docs.google.com" not in text
    assert text.rstrip().endswith("#экскурсии #Калининград #УхтыКалининград")


@pytest.mark.asyncio
async def test_vk_text_mentions_only_guides_with_personal_vk_id_when_enabled(monkeypatch):
    async def writer(payload, *, issue_id):
        return dict(NARRATIVE_349, lead="[[1|Ольга Рубцова]] ведёт прогулку.",
                    outro="[[1|Ольга Рубина]] тоже здесь?")

    monkeypatch.setattr(vd, "ask_vk_narrative", writer)
    monkeypatch.setattr(vd, "VISUAL_DIGEST_VK_MENTIONS_ENABLED", True)
    text = await build_visual_digest_vk_text(EDITION_349, issue_id=349, vk_api_fn=_fake_shortener)

    assert "[id41501067|Ольга Рубцова] ведёт" in text
    assert "с Ольгой Рубцовой." in text
    assert "У Нозы Усмановой другая" in text
    assert "с Марией Морозовой." in text  # no confirmed personal id: plain name
    assert "Ольга Рубина тоже здесь?" in text
    assert text.count("[id") == 1


@pytest.mark.asyncio
async def test_vk_text_keeps_compact_list_when_story_is_missing_or_has_stray_markup(monkeypatch):
    broken = dict(NARRATIVE_349, lead="Смотрите [id1|кто-то]")
    for result in (None, broken):
        async def writer(payload, *, issue_id, result=result):
            return result

        monkeypatch.setattr(vd, "ask_vk_narrative", writer)
        text = await build_visual_digest_vk_text(EDITION_349, issue_id=349, vk_api_fn=_fake_shortener)
        assert text.splitlines()[0] == "Дайджест экскурсий №349"
        assert "4. Секреты чёрного золота: экскурсия на Осетровую ферму — +7 (911) 858-58-49" in text


@pytest.mark.asyncio
async def test_writer_rejects_story_that_drops_or_invents_an_excursion(monkeypatch):
    from guide_excursions import digest_writer as dw

    payload, _ = vd._vk_narrative_payload(EDITION_349[:2])
    answers = iter([
        {"lead": "Хук", "paragraphs": [{"occurrence_ids": [745], "text": "Один абзац."}]},
        {"lead": "Хук", "paragraphs": [{"occurrence_ids": [745, 746, 999], "text": "Лишний id."}]},
        {"lead": "Хук", "paragraphs": [{"occurrence_ids": [745, 746], "text": "Оба на месте."}]},
        {"lead": "Хук", "paragraphs": [{"occurrence_ids": [745], "text": "Один."},
                                       {"occurrence_ids": [746], "text": "Другой."},
                                       {"occurrence_ids": [], "text": ""}]},
    ])

    async def generate(*_args, **_kwargs):
        return next(answers)

    monkeypatch.setattr(dw, "_get_digest_writer_runtime", lambda: (object(), None))
    monkeypatch.setattr(dw, "_generate_json", generate)
    assert await dw.ask_vk_narrative(payload, issue_id=1) is None
    assert await dw.ask_vk_narrative(payload, issue_id=1) is None
    assert await dw.ask_vk_narrative(payload, issue_id=1) is None
    assert len((await dw.ask_vk_narrative(payload, issue_id=1))["paragraphs"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("injection", ["@id41501067(Гид)", "*varja39", "vk.cc/example"])
async def test_writer_rejects_active_vk_text_from_model(monkeypatch, injection):
    from guide_excursions import digest_writer as dw

    async def generate(*args, **kwargs):
        return dict(NARRATIVE_349, outro=injection)

    monkeypatch.setattr(dw, "_get_digest_writer_runtime", lambda: (object(), None))
    monkeypatch.setattr(dw, "_generate_json", generate)
    payload, _ = vd._vk_narrative_payload(EDITION_349)
    assert await dw.ask_vk_narrative(payload, issue_id=349) is None


def test_writer_uses_same_roles_as_visual_and_sees_time():
    row = dict(EDITION_349[0], guide_names=["Старое имя"])
    payload, _ = vd._vk_narrative_payload([row])
    assert payload["excursions"][0]["guides"] == ["Ольга Рубцова"]
    assert payload["excursions"][0]["time"] == "14:00"
    row.pop("participant_medallions")
    row["fact_pack"] = {"guide_names": []}
    payload, _ = vd._vk_narrative_payload([row])
    assert payload["excursions"][0]["guides"] == []


@pytest.mark.asyncio
async def test_test_publication_uses_real_writer_and_card_but_only_notifies_test_recipient(monkeypatch):
    async def writer(payload, *, issue_id):
        return dict(NARRATIVE_349, lead="[[1|Ольга Рубцова]] ведёт прогулку.",
                    outro="[[3|Ноза Усманова]] тоже проводит экскурсию.")

    async def issue(*args):
        return {"items": EDITION_349}

    async def upload(group, payload, *args, **kwargs):
        assert group == "243343951" and payload.startswith(b"\x89PNG")
        return "photo-243343951_1"

    async def post(group, text, *args, **kwargs):
        assert group == "243343951"
        assert "[id1134828279|Ольга Рубцова] ведёт прогулку" in text
        assert "Ноза Усманова тоже проводит экскурсию." in text
        assert re.findall(r"\[id(\d+)\|", text) == ["1134828279"]
        assert kwargs["attachments"] == ["photo-243343951_1"]
        return "https://vk.ru/wall-243343951_123"

    import re
    monkeypatch.setattr(vd, "ask_vk_narrative", writer)
    monkeypatch.setattr(vd, "load_visual_digest_issue", issue)
    monkeypatch.setattr(vd, "VISUAL_DIGEST_VK_MENTIONS_ENABLED", True)
    result = await vd.publish_visual_digest_to_vk(
        object(), issue_id=349, group_id=243343951, test_mention=1134828279, publish_date=0,
        vk_api_fn=_fake_shortener, upload_vk_photo_bytes_fn=upload, post_to_vk_fn=post)
    assert result["url"].endswith("_123")


@pytest.mark.asyncio
async def test_review_marks_test_recipient_when_the_story_has_no_confirmed_name(monkeypatch):
    async def writer(*args, **kwargs):
        return NARRATIVE_349

    monkeypatch.setattr(vd, "ask_vk_narrative", writer)
    text = await build_visual_digest_vk_text(EDITION_349, issue_id=349, test_mention=858363572,
                                             vk_api_fn=_fake_shortener)
    assert text.endswith("Проверка уведомления: [id858363572|тестовый получатель]")
    assert text.count("[id") == 1


def test_writer_receives_all_linked_articles_with_their_own_source_and_paragraphs():
    first = {"url": "https://vk.ru/wall-10_20", "text": "Предисловие\n\n" + "Текст полного анонса. " * 200}
    second = {"url": "https://vk.ru/wall-10_21", "text": "Другая статья\n\nМаршрут по парку."}
    rows = [dict(EDITION_349[0], linked_posts=[first, first, second]),
            dict(EDITION_349[1], linked_posts=[first])]
    payload, _ = vd._vk_narrative_payload(rows)
    assert payload["sources"] == [
        {"url": first["url"], "text": first["text"].strip(), "excursions": [745, 746]},
        {"url": second["url"], "text": second["text"], "excursions": [745]},
    ]


def test_same_trip_prefers_complete_announcement_and_keeps_conflicting_sessions():
    date_row = dict(date="2026-11-28", city="Калининградская область", guide_names=["Эмма Басова"],
                    organizer_names=["Хранители руин"])
    brief = dict(date_row, id=705, canonical_title="Большое путешествие по Самбийскому кольцу",
                 route_summary="Самбийское кольцо", time=None, price_text=None, transport={"modes": ["bus"]},
                 linked_posts=[{"url": "https://vk.ru/wall-10_10", "text": "Сводная афиша"}])
    full = dict(date_row, id=740, canonical_title="Самбийское кольцо", route_summary="Музей Вайденен, усадьба Грюнхоф",
                time="09:00-18:00", price_text="4000 рублей с человека",
                linked_posts=[{"url": "https://t.me/ruin_keepers/6490", "text": "Полный маршрут"}])
    together = dict(full, id=741, time="09:00", price_text="4000 рублей")
    # The previous issue may refer to the sparse schedule row only.
    single, = vd._select_documented_departures([brief], [brief, full, together])
    states = vd._visual_publication_states([single])
    assert set(states) == {"705", "740", "741"}
    assert states["705"] == vd._visual_item_state(brief)
    alias = dict(brief, published_visual_digest_issue_id=350, published_visual_digest_state=states["705"],
                 updated_at="2026-11-18")
    assert vd._visual_selection_reason(alias) == ""
    assert vd._visual_selection_reason(dict(alias, time="11:00")) == "changed_time"
    assert single["id"] == 740 and single["covered_visual_ids"] == [705, 740, 741]
    assert single["time"] == "09:00-18:00"
    assert single["transport"]["modes"] == ["bus"]
    assert {p["text"] for p in single["linked_posts"]} == {"Сводная афиша", "Полный маршрут"}
    for other in (dict(together, time="11:00"), dict(together, time="09:00-17:00"),
                  dict(together, price_text="8000 рублей"), dict(together, guide_names=["Другой гид"])):
        selection = vd._select_documented_departures([brief, full, other], [brief, full, other])
        assert len(selection) >= 2
        assert not any(740 in r["covered_visual_ids"] and 741 in r["covered_visual_ids"] for r in selection)


def test_vk_narrative_pairs_source_excerpt_with_selected_post_url():
    original = EDITION_349[0]
    selected_url = "https://vk.com/wall-10_12"
    rows = [dict(original, dedup_source_url=selected_url),
            dict(original, id=746),
            dict(original, id=747, dedup_source_url=None)]
    payload, _ = vd._vk_narrative_payload(rows)
    assert [(s["url"], s["excursions"]) for s in payload["sources"]] == [
        (selected_url, [745]), (original["source_post_url"], [746]), ("", [747]),
    ]


@pytest.mark.asyncio
async def test_vk_same_departure_has_one_booking_and_keeps_full_fare(monkeypatch):
    url = "https://vk.com/wall-123_45"
    rows = [dict(row, canonical_title="Кольцо", city="Черняховск", date="2026-11-28",
                 guide_names=["Эмма"], booking_url=url) for row in EDITION_349[:2]]
    rows[0].update(id=740, time="09:00-18:00", price_text="4000 рублей с человека; обед 600", source_post_url=url)
    rows[1].update(id=741, time="09:00", price_text="4000 рублей")
    async def writer(*args, **kwargs):
        return {"lead": "Чем примечателен #Черняховск?", "paragraphs": [
            {"occurrence_ids": [740, 741], "text": "Поедем к кирхе."}], "outro": ""}
    monkeypatch.setattr(vd, "ask_vk_narrative", writer)
    text = await build_visual_digest_vk_text(rows, issue_id=350, test_mention=858363572)
    assert text.count("28 ноября (сб), 09:00") == text.count("4000 рублей") == text.count(url) == 1
    assert "09:00-18:00" in text and "обед 600" in text and f"[{url}|Запись]" in text
    assert "Чем примечателен Черняховск?" in text.splitlines()[1]
    assert "#" not in text.splitlines()[1] and "#Черняховск" in text.splitlines()[-3]
    assert text.index("КОЛЬЦО") < text.index("от «Ух ты, Калининград!»")
    # A second departure or a conflicting tariff remains visible.
    rows[1]["time"] = "11:00"
    text = await build_visual_digest_vk_text(rows, issue_id=350, test_mention=858363572)
    assert "11:00" in text and "09:00-18:00" in text


@pytest.mark.asyncio
async def test_vk_writer_heading_is_removed_only_when_it_is_a_separate_matching_line(monkeypatch):
    from guide_excursions import digest_writer as dw

    paragraphs = [
        {"occurrence_ids": [745, 747], "text": "ЗЕЛЁНЫЕ ИСТОРИИ\nПройдёмся от Зубров вдоль ручья."},
        {"occurrence_ids": [746], "text": "РАССВЕТ НАД БАЛТИКОЙ проходит в городе."},
        {"occurrence_ids": [763], "text": "ДРУГОЙ ЗАГОЛОВОК\nНа ферме покажут осетров."},
    ]

    async def generated(*args, **kwargs):
        return dict(NARRATIVE_349, paragraphs=paragraphs)

    monkeypatch.setattr(dw, "_get_digest_writer_runtime", lambda: (object(), None))
    monkeypatch.setattr(dw, "_generate_json", generated)
    monkeypatch.setattr(vd, "ask_vk_narrative", dw.ask_vk_narrative)  # undo the autouse stub
    text = await build_visual_digest_vk_text(EDITION_349, issue_id=349, vk_api_fn=_fake_shortener)
    assert "ЗЕЛЁНЫЕ ИСТОРИИ\nПройдёмся от Зубров вдоль ручья." in text
    assert text.count("ЗЕЛЁНЫЕ ИСТОРИИ") == 1
    assert "РАССВЕТ НАД БАЛТИКОЙ\nРАССВЕТ НАД БАЛТИКОЙ проходит в городе." in text
    assert "ДРУГОЙ ЗАГОЛОВОК На ферме покажут осетров." in text


def test_vk_card_keeps_complete_hook_and_skips_blurb_fragments():
    hook = "Почему у платанов отслаивается кора?"
    row = {"canonical_title": "Прогулка", "fact_pack": {"main_hook": hook},
           "digest_blurb": "Приглашаем на прогулку с гидом."}
    assert vd._card_description(row) == hook
    assert vd._card_description(dict(row, fact_pack={})) == ""


@pytest.mark.asyncio
async def test_sparse_excursion_keeps_grounded_writer_transition(monkeypatch):
    row = dict(EDITION_349[1])
    async def writer(*args, **kwargs):
        return {"lead": "Что меняется в пятницу?", "paragraphs": [
            {"occurrence_ids": [row["id"]], "text": "А в пятницу прогулку ведёт Ноза Усманова."}], "outro": ""}
    monkeypatch.setattr(vd, "ask_vk_narrative", writer)
    text = await build_visual_digest_vk_text([row], issue_id=349, vk_api_fn=_fake_shortener)
    assert "А в пятницу прогулку ведёт Ноза Усманова." in text


def test_extracted_empty_roles_do_not_present_source_owner_as_guide():
    row = {"date": "2026-11-22", "canonical_title": "Осетровая ферма", "guide_names": ["Мария"],
           "source_kind": "guide_personal", "source_title": "Мария", "guide_profile_summary": "Гид Мария",
           "participant_medallions": [{"name": "Мария", "owner": 123, "roles": ["Гид"]}],
           "fact_pack": {"guide_names": ["Мария"]}}
    posts = [{"text": "На осетровой ферме покажут осетров.", "raw_facts_json": [
        {"canonical_title": "Осетровая ферма", "date": "2026-11-22", "guide_names": [], "organizer_names": []}]}]
    vd._apply_published_facts(row, posts)
    payload, mentions = vd._vk_narrative_payload([dict(row, id=1)])
    assert row["participant_medallions"] == [] and vd._source_name(row) == ""
    assert not payload["excursions"][0]["guides"] and not payload["excursions"][0]["source_profile"] and not mentions


def test_price_stays_with_its_dated_outing_and_deposit_policy_is_not_a_fare():
    row = {"date": "2026-11-21", "canonical_title": "Под сенью ангелов", "price_text": "по запросу"}
    post = {"post_date": "2026-11-01", "text": "8 ноября Дюны\nСтоимость 4400 руб\n"
            "21 ноября — Под сенью ангелов\nСтоимость 4800 руб (завтрак и обед)\n"
            "5 декабря — Лес\nЦена 2000 руб"}
    vd._apply_published_facts(row, [post])
    assert row["price_text"] == "4800 руб (завтрак и обед)"
    vd._apply_published_facts(row := dict(row, fact_pack={}, price_text="Предоплату мы не берём"), [])
    assert not row["price_text"]


def test_visual_booking_skips_dotted_telegram_usernames_and_keeps_valid_links():
    source = "https://t.me/amber_fringilla/6737"
    row = {"id": 693, "source_post_url": source, "booking_text": "свяжитесь с организатором"}
    for invalid in ("https://t.me/vladimir.tour", "telegram.me/vladimir.tour/?profile"):
        assert vd._primary_link(dict(row, booking_url=invalid)) == (source, False)
    for valid in ("https://t.me/Yulia_Grishanova", "https://telegram.me/amber_fringilla/6737",
                  "https://t.me/+invite", "https://t.me/c/123/4", "https://t.me/test_bot?start=a.b",
                  "https://t.me/share/url?url=https://example.com", "tg://resolve?domain=test_bot",
                  "https://example.com/vladimir.tour"):
        assert vd._primary_link(dict(row, booking_url=valid)) == (valid, False)
