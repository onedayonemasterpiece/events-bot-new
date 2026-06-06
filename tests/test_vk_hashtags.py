from datetime import date
from types import SimpleNamespace

from vk_hashtags import (
    build_vk_announce_hashtags,
    build_vk_event_hashtags,
    build_vk_video_announce_caption,
    normalize_vk_hashtag,
    vk_date_hashtags,
)


def test_vk_date_hashtags_use_two_required_forms():
    assert vk_date_hashtags(date(2026, 5, 17)) == ["#17мая", "#17_мая"]
    assert vk_date_hashtags("2026-06-03") == ["#3июня", "#3_июня"]


def test_build_vk_announce_hashtags_adds_base_city_and_dates():
    tags = build_vk_announce_hashtags(
        cities=["Калининград", "Светлогорск", "Калининград"],
        dates=["2026-05-17", "2026-05-17"],
    )

    assert tags == [
        "#анонс",
        "#анонс39",
        "#кудапойтиКалининград",
        "#афишаКалининград",
        "#Калининград",
        "#Светлогорск",
        "#17мая",
        "#17_мая",
    ]


def test_build_vk_event_hashtags_adds_festival_name_without_spaces():
    event = SimpleNamespace(
        city="Калининград",
        date="2026-06-06",
        festival="80 историй о главном",
    )

    tags = build_vk_event_hashtags(event)

    assert tags[-1] == "#80историйоглавном"


def test_build_vk_event_hashtags_prefers_canonical_festival_name():
    event = SimpleNamespace(
        city="Калининград",
        date="2026-06-06",
        festival="Кантаты",
    )

    tags = build_vk_event_hashtags(event, festival_name="Кантата")

    assert "#Кантата" in tags
    assert "#Кантаты" not in tags


def test_normalize_vk_hashtag_compacts_city_names():
    assert normalize_vk_hashtag("Гусевский городской округ") == "#Гусевский_городской_округ"


def test_build_vk_video_announce_caption():
    caption = build_vk_video_announce_caption(
        cities=["Калининград"],
        dates=["2026-05-24"],
    )

    assert caption.startswith("Видеоанонс\n\n")
    assert "#анонс #анонс39" in caption
    assert "#Калининград" in caption
    assert "#24мая" in caption
    assert "#24_мая" in caption
