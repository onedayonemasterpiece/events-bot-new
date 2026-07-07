from dataclasses import dataclass

import vk_intake


@dataclass
class DummyPoster:
    catbox_url: str | None = None
    supabase_url: str | None = None
    sha256: str | None = None
    phash: str | None = None
    ocr_text: str | None = None
    ocr_title: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


def test_vk_contact_ticket_link_is_public_vk_link_for_vk_source():
    assert (
        vk_intake._sanitize_vk_ticket_link_for_source(
            "tg://user?id=9648720",
            "https://vk.com/wall-211363505_1941",
        )
        == "https://vk.com/id9648720"
    )
    assert (
        vk_intake._sanitize_vk_ticket_link_for_source(
            "tg://user?id=9648720",
            "https://t.me/source/10",
        )
        == "tg://user?id=9648720"
    )


def test_vk_location_guard_clears_unsupported_known_venue_without_choosing_replacement():
    source_text = "Одни и те же слова могут вдохновить. Запись: [id9648720|Анна Астраханцева]"
    assert vk_intake._vk_location_value_ungrounded(
        "Калининград Сити Джаз Клуб",
        source_text=source_text,
        source_name="Мой театр",
    )
    assert not vk_intake._vk_location_value_ungrounded(
        "Мой театр",
        source_text=source_text,
        source_name="Мой театр",
    )
    assert not vk_intake._vk_location_value_ungrounded(
        "Бар Ельцин",
        source_text="🔥10-11.07 | Ельцину 10 лет\n📍Ельцин, Гаражная 2б",
        source_name="мяу | Калининград",
    )


def test_vk_schedule_fragment_title_is_low_confidence():
    assert vk_intake._vk_title_is_schedule_fragment("пятница 22:00")
    assert vk_intake._vk_title_is_schedule_fragment("суббота 14:00 - 02:00")
    assert not vk_intake._vk_title_is_schedule_fragment("Ельцину 10 лет: Amazing Sex People")


def test_vk_structured_footer_datetime_anchor_extracted_for_conflict_guard():
    assert vk_intake._extract_vk_structured_footer_datetime(
        "Все эти имена напомнят о главном вечером 10 июля.\n\n📅 31 июля, начало в 21:00 (двери в 20:00)",
        anchor_year=2026,
    ) == ("2026-07-31", "21:00")


def test_multi_event_without_confident_media_assignment_does_not_use_raw_gallery():
    draft = vk_intake.EventDraft(title="Спектакль «Ужин в музее»")
    draft.allow_raw_photo_fallback = False
    assert (
        vk_intake._build_smart_update_posters(
            draft,
            photos=["https://vk.example/photo1.jpg", "https://vk.example/photo2.jpg"],
            poster_cls=DummyPoster,
        )
        == []
    )
