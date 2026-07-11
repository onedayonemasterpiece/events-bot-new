"""Regression coverage: a broken partner Business target must not affect the
other partner track. Tests rely on the synchronous behaviour of
``video_announce.story_publish._business_targets_setting_raw`` /
``_business_targets_allowed_for_mode`` and a stubbed cache loader.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from video_announce import story_publish as story_publish_module
from video_announce.partner_tracks import (
    PARTNER_ECO_NATURE,
    PARTNER_KONB_LIBRARY,
    PARTNER_REGION_EAST,
)
from video_announce.popular_review import POPULAR_REVIEW_PROFILE


@pytest.fixture
def cached_connections(monkeypatch):
    """Only @yasonneolga is cached with story rights; @natakkaz is missing."""
    fixture = [
        {
            "connection_hash": "hash_eco",
            "user_hash": "uhash_eco",
            "username_hash": "uhhash_eco",
            "username": "yasonneolga",
            "connection_id": "conn_eco",
            "is_enabled": True,
            "can_manage_stories": True,
        },
    ]
    monkeypatch.setattr(
        story_publish_module,
        "load_business_story_targets",
        lambda *, path=None, selector_raw=None: _filter(fixture, selector_raw),
    )
    return fixture


def _filter(fixture, selector_raw):
    if not selector_raw:
        return []
    selectors = {s.strip().lstrip("@").casefold() for s in str(selector_raw).split(",") if s.strip()}
    if not selectors:
        return []
    out = []
    for item in fixture:
        if item["username"].casefold() in selectors:
            out.append(item)
    return out


def test_eco_resolves_when_only_eco_cached(cached_connections):
    params = {
        "mode": POPULAR_REVIEW_PROFILE,
        "story_business_targets": PARTNER_ECO_NATURE.default_business_selector,
    }
    targets = asyncio.run(story_publish_module._business_story_targets(None, params))
    assert len(targets) == 1
    assert targets[0].business_connection_hash == "hash_eco"


def test_east_resolves_to_empty_when_not_cached(cached_connections):
    params = {
        "mode": POPULAR_REVIEW_PROFILE,
        "story_business_targets": PARTNER_REGION_EAST.default_business_selector,
    }
    targets = asyncio.run(story_publish_module._business_story_targets(None, params))
    assert targets == []


def test_eco_resolution_is_independent_of_east_cache_state(cached_connections):
    """An empty east cache must not contaminate eco resolution — the per-session
    `story_business_targets` selector scopes resolution to one track."""
    east_targets = asyncio.run(
        story_publish_module._business_story_targets(
            None,
            {
                "mode": POPULAR_REVIEW_PROFILE,
                "story_business_targets": PARTNER_REGION_EAST.default_business_selector,
            },
        )
    )
    eco_targets = asyncio.run(
        story_publish_module._business_story_targets(
            None,
            {
                "mode": POPULAR_REVIEW_PROFILE,
                "story_business_targets": PARTNER_ECO_NATURE.default_business_selector,
            },
        )
    )
    assert east_targets == []
    assert len(eco_targets) == 1
    assert eco_targets[0].business_connection_hash == "hash_eco"


def test_partner_mode_is_popular_review_so_business_resolution_runs():
    """Regression: if `mode` slid to `popular_review_eco` the
    `_business_targets_allowed_for_mode` gate would block Business
    resolution. The partner pipeline must keep `mode=popular_review`."""
    from video_announce import scenario as scenario_module

    class _Stub:
        db = None
        _partner_track_story_targets = scenario_module.VideoAnnounceScenario._partner_track_story_targets

    params = scenario_module.VideoAnnounceScenario._partner_track_selection_params(
        _Stub(),
        PARTNER_ECO_NATURE,
        business_selector=PARTNER_ECO_NATURE.default_business_selector,
    )
    assert params["mode"] == POPULAR_REVIEW_PROFILE
    assert params["partner_track_id"] == PARTNER_ECO_NATURE.track_id
    assert params["partner_profile_key"] == PARTNER_ECO_NATURE.profile_key
    assert params["allow_empty_ocr"] is True
    assert story_publish_module._business_targets_allowed_for_mode(params) is True


def test_konb_selection_params_use_test_story_target_not_business():
    from video_announce import scenario as scenario_module

    class _Stub:
        db = None
        _partner_track_story_targets = scenario_module.VideoAnnounceScenario._partner_track_story_targets

    params = scenario_module.VideoAnnounceScenario._partner_track_selection_params(
        _Stub(),
        PARTNER_KONB_LIBRARY,
        publish_mode="test",
    )
    assert params["mode"] == POPULAR_REVIEW_PROFILE
    assert params["partner_track_id"] == PARTNER_KONB_LIBRARY.track_id
    assert params["allow_empty_ocr"] is False
    assert params["partner_publish_mode"] == "test"
    assert params["story_targets_override"][0]["peer"] == "@keniggpt"
    assert params["story_targets_override"][0]["transport"] == "telegram_chat"
    assert params["story_business_targets"] == ""


def test_partner_modes_are_in_default_business_mode_whitelist():
    """Defence in depth: even if a future caller passes the partner profile_key
    as `mode`, business resolution must still be allowed."""
    for partner_profile_key in (
        PARTNER_ECO_NATURE.profile_key,
        PARTNER_KONB_LIBRARY.profile_key,
        PARTNER_REGION_EAST.profile_key,
    ):
        assert story_publish_module._business_targets_allowed_for_mode(
            {"mode": partner_profile_key}
        ) is True
