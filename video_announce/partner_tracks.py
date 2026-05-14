"""Partner-specific CherryFlash story tracks (see docs/features/cherryflash/partner-story-tracks.md).

This module exposes a stable registry of partner tracks plus helpers to derive
the `selection_params` overrides each track injects into the popular_review
pipeline.

Privacy: this file MUST NOT contain raw Telegram handles or `business_connection_id`
values. Every track references its Business target through a `Setting` row whose
value is operator-supplied at runtime; the bot resolves the selector against the
existing encrypted Business connection cache via
`telegram_business.load_business_story_targets`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .popular_review import POPULAR_REVIEW_PROFILE


@dataclass(frozen=True, slots=True)
class PartnerTrack:
    track_id: str
    profile_key: str
    display_name: str
    button_emoji: str
    button_label: str
    callback_action: str
    content_filter_id: str
    geo_filter_id: str | None
    intro_kicker: str
    intro_screen_top: str
    business_selector_setting_key: str
    # Default selector used when the operator has not overridden the Setting
    # row. Matched by ``telegram_business._selector_match`` against cached
    # Business connections — accepts a ``@username`` form and is compared
    # case-insensitively. Operator can still override at runtime by writing the
    # corresponding Setting row.
    default_business_selector: str = ""


PARTNER_ECO_NATURE = PartnerTrack(
    track_id="partner_eco_nature_001",
    profile_key="popular_review_eco",
    display_name="Природа и экология",
    button_emoji="🍃",
    button_label="Природа и экология",
    callback_action="eco",
    content_filter_id="eco_prirodnaya",
    geo_filter_id=None,
    intro_kicker="ПРИРОДА И ЭКОЛОГИЯ",
    intro_screen_top="природа и экология",
    business_selector_setting_key="partner_track_eco_business_selector",
    default_business_selector="@yasonneolga",
)


PARTNER_REGION_EAST = PartnerTrack(
    track_id="partner_region_east_001",
    profile_key="popular_review_east",
    display_name="Восток Калининградской области",
    button_emoji="🌾",
    button_label="Восток области",
    callback_action="east",
    content_filter_id="popularity_review_default",
    geo_filter_id="kaliningrad_region_east",
    intro_kicker="ВОСТОК\nКАЛИНИНГРАДСКОЙ\nОБЛАСТИ",
    intro_screen_top="восток калининградской области",
    business_selector_setting_key="partner_track_east_business_selector",
    default_business_selector="@natakkaz",
)


PARTNER_TRACKS: tuple[PartnerTrack, ...] = (PARTNER_ECO_NATURE, PARTNER_REGION_EAST)


_BY_TRACK_ID: dict[str, PartnerTrack] = {t.track_id: t for t in PARTNER_TRACKS}
_BY_ACTION: dict[str, PartnerTrack] = {t.callback_action: t for t in PARTNER_TRACKS}
_BY_PROFILE_KEY: dict[str, PartnerTrack] = {t.profile_key: t for t in PARTNER_TRACKS}


CHERRYFLASH_FAMILY_PROFILE_KEYS: frozenset[str] = frozenset(
    {POPULAR_REVIEW_PROFILE, *(t.profile_key for t in PARTNER_TRACKS)}
)


def all_partner_tracks() -> Iterable[PartnerTrack]:
    return PARTNER_TRACKS


def get_partner_track(track_id: str) -> PartnerTrack | None:
    return _BY_TRACK_ID.get(track_id)


def get_partner_track_by_action(action: str) -> PartnerTrack | None:
    return _BY_ACTION.get(action)


def get_partner_track_by_profile_key(profile_key: str) -> PartnerTrack | None:
    return _BY_PROFILE_KEY.get(profile_key)
