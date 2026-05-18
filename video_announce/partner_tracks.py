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
from typing import Any, Iterable

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
    publish_mode_setting_key: str = ""
    default_publish_mode: str = "business"
    test_story_targets: tuple[dict[str, Any], ...] = ()
    prod_story_targets: tuple[dict[str, Any], ...] = ()
    selection_policy_id: str = ""
    outro: dict[str, Any] | None = None


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


PARTNER_KONB_LIBRARY = PartnerTrack(
    track_id="partner_konb_library_001",
    profile_key="popular_review_konb",
    display_name="Калининградская областная научная библиотека",
    button_emoji="📚",
    button_label="КОНБ",
    callback_action="konb",
    content_filter_id="konb_library",
    geo_filter_id=None,
    intro_kicker="НАУЧНАЯ\nБИБЛИОТЕКА",
    intro_screen_top="Калининградская областная\nнаучная библиотека",
    business_selector_setting_key="",
    default_business_selector="",
    publish_mode_setting_key="partner_track_konb_publish_mode",
    default_publish_mode="prod",
    test_story_targets=(
        {
            "peer": "@keniggpt",
            "label": "tg:@keniggpt:test-post",
            "delay_seconds": 0,
            "mode": "upload",
            "transport": "telegram_chat",
            "blocking": True,
            "required": True,
            "caption": "Видеоанонс КОНБ",
        },
    ),
    prod_story_targets=(
        # Round-3 prod contract (2026-05-17): both targets are independent
        # best-effort — neither is `required`. Success in one does not gate
        # the other, and failure of one does not fail the overall publish.
        # Operator brief: «публикация в сторис телеграм и в сторис вк
        # сообщества, вне зависимости от успеха публикации в какой-либо
        # из них».
        {
            "peer": "@kaliningradlibrary",
            "label": "tg:@kaliningradlibrary:story",
            "delay_seconds": 0,
            "mode": "upload",
            "blocking": False,
            "required": False,
        },
        {
            "peer": "konb39",
            "label": "vk:konb39:story",
            "delay_seconds": 0,
            "mode": "upload",
            "transport": "vk_story",
            "blocking": False,
            "required": False,
        },
    ),
    selection_policy_id="konb_library",
    outro={
        "strip_color": "#780000",
        "text_color": "#FFFFFF",
        # Round-2 (2026-05-17) sizing: every stripe + font + base gap is
        # rendered 30% smaller end-to-end via `global_scale=0.7`. Per-line
        # `scale` keeps the same ratios as round-1 (1.0 / 0.42 / 0.8).
        "global_scale": 0.7,
        # Library name: one word per stripe at full scale.
        # Sponsor caption «при поддержке»: a single small stripe with 3×
        # empty space above AND below — the «при поддержке» row sets
        # `extra_gap_before=2.0` (so the gap ABOVE it triples: base + 2×base
        # = 3×base), and the next «Полюбить» row also sets
        # `extra_gap_before=2.0` (so the gap BELOW «при поддержке» triples
        # too). Round-3 operator request (2026-05-17): «между "при
        # поддержке" сверху и снизу добавь дополнительный интервал, в 3
        # раза больше».
        # Channel signature «Полюбить Калининград Анонсы»: one word per
        # stripe at scale 0.80 (20% smaller than the library name lines).
        "lines": [
            {"text": "Калининградская", "scale": 1.0, "side": "left", "delay": 0.0},
            {"text": "областная", "scale": 1.0, "side": "right", "delay": 0.18},
            {"text": "научная", "scale": 1.0, "side": "left", "delay": 0.36},
            {"text": "библиотека", "scale": 1.0, "side": "right", "delay": 0.54},
            {
                "text": "при поддержке",
                "scale": 0.42,
                "side": "left",
                "delay": 0.78,
                "extra_gap_before": 2.0,
            },
            {
                "text": "Полюбить",
                "scale": 0.8,
                "side": "right",
                "delay": 0.96,
                "extra_gap_before": 2.0,
            },
            {"text": "Калининград", "scale": 0.8, "side": "left", "delay": 1.14},
            {"text": "Анонсы", "scale": 0.8, "side": "right", "delay": 1.32},
        ],
    },
)


PARTNER_TRACKS: tuple[PartnerTrack, ...] = (
    PARTNER_ECO_NATURE,
    PARTNER_REGION_EAST,
    PARTNER_KONB_LIBRARY,
)


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
