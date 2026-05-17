from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from video_announce.partner_tracks import (
    PARTNER_ECO_NATURE,
    PARTNER_KONB_LIBRARY,
    PARTNER_REGION_EAST,
    PARTNER_TRACKS,
    get_partner_track,
    get_partner_track_by_action,
    get_partner_track_by_profile_key,
)
from video_announce.partner_filters import (
    FilterDecision,
    classify_event_eco_prirodnaya,
    classify_event_kaliningrad_region_east,
    classify_event_konb_library,
)


def _make_event(**kwargs):
    defaults = dict(
        id=1,
        title="",
        description="",
        search_digest="",
        short_description="",
        event_type="",
        location_name="",
        location_address="",
        city="",
        source_post_url="",
        source_label="",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# registry
# ---------------------------------------------------------------------------


def test_partner_tracks_registry_lookups():
    assert PARTNER_ECO_NATURE.track_id == "partner_eco_nature_001"
    assert PARTNER_REGION_EAST.track_id == "partner_region_east_001"
    assert PARTNER_KONB_LIBRARY.track_id == "partner_konb_library_001"
    assert get_partner_track(PARTNER_ECO_NATURE.track_id) is PARTNER_ECO_NATURE
    assert get_partner_track("unknown") is None
    assert get_partner_track_by_action("eco") is PARTNER_ECO_NATURE
    assert get_partner_track_by_action("east") is PARTNER_REGION_EAST
    assert get_partner_track_by_action("konb") is PARTNER_KONB_LIBRARY
    assert get_partner_track_by_profile_key(PARTNER_ECO_NATURE.profile_key) is PARTNER_ECO_NATURE
    assert get_partner_track_by_profile_key(PARTNER_KONB_LIBRARY.profile_key) is PARTNER_KONB_LIBRARY


def test_partner_tracks_have_distinct_profile_keys():
    keys = {t.profile_key for t in PARTNER_TRACKS}
    assert len(keys) == len(PARTNER_TRACKS)
    assert "popular_review" not in keys  # base CherryFlash key reserved


def test_partner_tracks_have_intro_kicker_and_screen_top():
    for track in PARTNER_TRACKS:
        assert track.intro_kicker.strip()
        assert track.intro_screen_top.strip()
        if track.default_publish_mode == "business":
            assert track.business_selector_setting_key.startswith("partner_track_")


def test_partner_tracks_carry_default_business_selector():
    # Ensures the pipeline never has to fall back to operator-only setup.
    for track in PARTNER_TRACKS:
        if track.default_publish_mode == "business":
            assert track.default_business_selector.startswith("@"), track.track_id


def test_konb_track_defaults_to_test_story_target():
    assert PARTNER_KONB_LIBRARY.default_publish_mode == "test"
    assert PARTNER_KONB_LIBRARY.test_story_targets[0]["peer"] == "@keniggpt"
    assert PARTNER_KONB_LIBRARY.prod_story_targets[0]["peer"] == "@kaliningradlibrary"
    assert PARTNER_KONB_LIBRARY.prod_story_targets[1]["transport"] == "vk_story"
    assert PARTNER_KONB_LIBRARY.outro["strip_color"] == "#780000"


# ---------------------------------------------------------------------------
# kaliningrad_region_east — deterministic geo filter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "city,expect_matched",
    [
        ("Советск", True),
        ("советск", True),
        ("г. Гусев", True),
        ("Озерск", True),  # ё/е normalization
        ("Озёрск", True),
        ("Калининград", False),
        ("Гурьевск", False),
        ("Зеленоградск", False),
        ("Светлогорск", False),
        ("Родники", False),  # spec exclude_over_include
        ("Москва", False),
    ],
)
def test_east_filter_city_matches(city, expect_matched):
    event = _make_event(id=10, city=city)
    decision = classify_event_kaliningrad_region_east(event)
    assert decision.matched is expect_matched


def test_east_filter_no_location_returns_not_matched():
    event = _make_event(id=11)
    decision = classify_event_kaliningrad_region_east(event)
    assert decision.matched is False
    assert decision.reason == "no_location"


def test_east_filter_exclude_overrides_include_in_compound_address():
    event = _make_event(id=12, city="Калининград", location_name="Гусев")
    decision = classify_event_kaliningrad_region_east(event)
    assert decision.matched is False
    assert decision.reason.startswith("exclude:")


# ---------------------------------------------------------------------------
# konb_library — deterministic source/venue ownership filter
# ---------------------------------------------------------------------------


def test_konb_filter_matches_scientific_library_location():
    event = _make_event(
        id=30,
        location_name="Научная библиотека, Мира 9, Калининград",
    )
    decision = classify_event_konb_library(event)
    assert decision.matched is True
    assert decision.reason.startswith("location:")


def test_konb_filter_matches_public_source():
    event = _make_event(
        id=31,
        source_post_url="https://vk.com/wall-30777579_15208",
        location_name="Калининград",
    )
    decision = classify_event_konb_library(event)
    assert decision.matched is True
    assert decision.reason.startswith("source:")


def test_konb_filter_rejects_other_library():
    event = _make_event(
        id=32,
        location_name="Библиотека им. Лунина, Калинина 4, Черняховск",
    )
    decision = classify_event_konb_library(event)
    assert decision.matched is False
    assert decision.reason == "not_konb_library"


# ---------------------------------------------------------------------------
# eco_prirodnaya — LLM-first classifier
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eco_filter_uses_llm_decision_matched():
    captured = {}

    async def fake_llm(system_prompt, user_text, schema):
        captured["called"] = True
        return {"decision": "matched", "reason": "природа", "matched_keywords": ["природа"]}

    event = _make_event(id=20, title="Прогулка по Куршской косе", description="Орнитология")
    decision = await classify_event_eco_prirodnaya(event, llm_call=fake_llm)
    assert decision.matched is True
    assert decision.needs_manual_review is False
    assert decision.extra.get("decision") == "matched"
    assert captured.get("called") is True


@pytest.mark.asyncio
async def test_eco_filter_admits_manual_review_marker():
    async def fake_llm(*_a, **_kw):
        return {"decision": "manual_review", "reason": "ambiguous"}

    event = _make_event(id=21, title="Музейная ночь", description="Концерт в музее")
    decision = await classify_event_eco_prirodnaya(event, llm_call=fake_llm)
    assert decision.matched is False
    assert decision.needs_manual_review is True


@pytest.mark.asyncio
async def test_eco_filter_excludes_when_llm_says_exclude():
    async def fake_llm(*_a, **_kw):
        return {"decision": "exclude", "reason": "obviously commercial"}

    event = _make_event(id=22, title="Ярмарка вакансий")
    decision = await classify_event_eco_prirodnaya(event, llm_call=fake_llm)
    assert decision.matched is False
    assert decision.needs_manual_review is False


@pytest.mark.asyncio
async def test_eco_filter_llm_error_marks_manual_review():
    async def fake_llm(*_a, **_kw):
        raise RuntimeError("provider down")

    event = _make_event(id=23, title="Любая тема")
    decision = await classify_event_eco_prirodnaya(event, llm_call=fake_llm)
    assert decision.matched is False
    assert decision.needs_manual_review is True
    assert decision.reason.startswith("llm_error:")


@pytest.mark.asyncio
async def test_eco_filter_retries_and_uses_fallback(monkeypatch):
    monkeypatch.setenv("PARTNER_FILTER_GEMMA_ATTEMPTS", "2")
    calls = {"gemma": 0, "fallback": 0}

    async def fake_llm(*_a, **_kw):
        calls["gemma"] += 1
        raise RuntimeError("provider down")

    async def fake_fallback(*_a, **_kw):
        calls["fallback"] += 1
        return {"decision": "matched", "reason": "fallback accepted"}

    event = _make_event(id=26, title="Эко-лекция о птицах")
    decision = await classify_event_eco_prirodnaya(
        event,
        llm_call=fake_llm,
        fallback_llm_call=fake_fallback,
    )

    assert calls == {"gemma": 2, "fallback": 1}
    assert decision.matched is True
    assert decision.reason == "fallback accepted"


@pytest.mark.asyncio
async def test_eco_filter_empty_event_returns_manual_review():
    async def fake_llm(*_a, **_kw):
        pytest.fail("LLM should not be called for empty event")

    event = _make_event(id=24)
    decision = await classify_event_eco_prirodnaya(event, llm_call=fake_llm)
    assert decision.matched is False
    assert decision.needs_manual_review is True
    assert decision.reason == "empty_event_text"


@pytest.mark.asyncio
async def test_eco_filter_returns_filterdecision_instance():
    async def fake_llm(*_a, **_kw):
        return {"decision": "matched", "reason": ""}

    event = _make_event(id=25, title="Эко-урок")
    decision = await classify_event_eco_prirodnaya(event, llm_call=fake_llm)
    assert isinstance(decision, FilterDecision)
