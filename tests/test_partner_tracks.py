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
    assert get_partner_track(PARTNER_ECO_NATURE.track_id) is PARTNER_ECO_NATURE
    assert get_partner_track("unknown") is None
    assert get_partner_track_by_action("eco") is PARTNER_ECO_NATURE
    assert get_partner_track_by_action("east") is PARTNER_REGION_EAST
    assert get_partner_track_by_profile_key(PARTNER_ECO_NATURE.profile_key) is PARTNER_ECO_NATURE


def test_partner_tracks_have_distinct_profile_keys():
    keys = {t.profile_key for t in PARTNER_TRACKS}
    assert len(keys) == len(PARTNER_TRACKS)
    assert "popular_review" not in keys  # base CherryFlash key reserved


def test_partner_tracks_have_intro_kicker_and_screen_top():
    for track in PARTNER_TRACKS:
        assert track.intro_kicker.strip()
        assert track.intro_screen_top.strip()
        assert track.business_selector_setting_key.startswith("partner_track_")


def test_partner_tracks_carry_default_business_selector():
    # Ensures the pipeline never has to fall back to operator-only setup.
    for track in PARTNER_TRACKS:
        assert track.default_business_selector.startswith("@"), track.track_id


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
