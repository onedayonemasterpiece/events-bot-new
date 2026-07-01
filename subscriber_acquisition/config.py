from __future__ import annotations

import os
from dataclasses import dataclass

_TRUE = {"1", "true", "yes", "on"}


def _bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in _TRUE


def _int(name: str, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except Exception:
        value = int(default)
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _float(name: str, default: float, *, min_value: float | None = None, max_value: float | None = None) -> float:
    raw = (os.getenv(name) or "").strip()
    try:
        value = float(raw) if raw else float(default)
    except Exception:
        value = float(default)
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


@dataclass(frozen=True)
class AcqConfig:
    enabled: bool = False
    shadow_mode: bool = True
    review_chat_id: int | None = None
    review_thread_id: int | None = None
    review_group_max_cards_per_run: int = 20
    max_surfaces_per_run: int = 30
    max_messages_per_surface: int = 200
    max_threads_per_surface: int = 20
    max_opportunities_per_run: int = 50
    max_opportunities_per_surface_per_run: int = 3
    surface_rescan_cooldown_h: int = 24
    context_dedup_days: int = 14
    approved_surface_reshow_cooldown_h: int = 72
    rejected_surface_cooldown_days: int = 30
    paused_surface_days: int = 7
    opportunity_expires_h: int = 48
    tg_comment_readthrough_factor: float = 0.02
    vk_comment_readthrough_factor: float = 0.01
    reach_unknown_group_low: int = 5
    reach_age_decay_half_life_h: int = 24
    pka_channel_url: str = "https://t.me/kenigevents"
    pka_afisha_channel_url: str = ""
    default_link_target_url: str = "https://t.me/kenigevents"
    discovery_results_path: str = ""
    fixture_path: str = ""
    use_sample_fixture: bool = False


def load_config() -> AcqConfig:
    review_chat_raw = (os.getenv("ACQ_REVIEW_CHAT_ID") or "").strip()
    thread_raw = (os.getenv("ACQ_REVIEW_THREAD_ID") or "").strip()
    return AcqConfig(
        enabled=_bool("ACQ_DISCOVERY_ENABLED", False),
        shadow_mode=_bool("ACQ_DISCOVERY_SHADOW_MODE", True),
        review_chat_id=int(review_chat_raw) if review_chat_raw.lstrip("-").isdigit() else None,
        review_thread_id=int(thread_raw) if thread_raw.isdigit() else None,
        review_group_max_cards_per_run=_int("ACQ_REVIEW_GROUP_MAX_CARDS_PER_RUN", 20, min_value=0, max_value=20),
        max_surfaces_per_run=_int("ACQ_MAX_SURFACES_PER_RUN", 30, min_value=1),
        max_messages_per_surface=_int("ACQ_MAX_MESSAGES_PER_SURFACE", 200, min_value=1),
        max_threads_per_surface=_int("ACQ_MAX_THREADS_PER_SURFACE", 20, min_value=1),
        max_opportunities_per_run=_int("ACQ_MAX_OPPORTUNITIES_PER_RUN", 50, min_value=1),
        max_opportunities_per_surface_per_run=_int("ACQ_MAX_OPPORTUNITIES_PER_SURFACE_PER_RUN", 3, min_value=1),
        surface_rescan_cooldown_h=_int("ACQ_SURFACE_RESCAN_COOLDOWN_H", 24, min_value=1),
        context_dedup_days=_int("ACQ_CONTEXT_DEDUP_DAYS", 14, min_value=1),
        approved_surface_reshow_cooldown_h=_int("ACQ_APPROVED_SURFACE_RESHOW_COOLDOWN_H", 72, min_value=1),
        rejected_surface_cooldown_days=_int("ACQ_REJECTED_SURFACE_COOLDOWN_DAYS", 30, min_value=1),
        paused_surface_days=_int("ACQ_PAUSED_SURFACE_DAYS", 7, min_value=1),
        opportunity_expires_h=_int("ACQ_OPPORTUNITY_EXPIRES_H", 48, min_value=1),
        tg_comment_readthrough_factor=_float("ACQ_TG_COMMENT_READTHROUGH_FACTOR", 0.02, min_value=0.0, max_value=1.0),
        vk_comment_readthrough_factor=_float("ACQ_VK_COMMENT_READTHROUGH_FACTOR", 0.01, min_value=0.0, max_value=1.0),
        reach_unknown_group_low=_int("ACQ_REACH_UNKNOWN_GROUP_LOW", 5, min_value=1),
        reach_age_decay_half_life_h=_int("ACQ_REACH_AGE_DECAY_HALF_LIFE_H", 24, min_value=1),
        pka_channel_url=(os.getenv("ACQ_PKA_CHANNEL_URL") or "https://t.me/kenigevents").strip(),
        pka_afisha_channel_url=(os.getenv("ACQ_PKA_AFISHA_CHANNEL_URL") or "").strip(),
        default_link_target_url=(os.getenv("ACQ_DEFAULT_LINK_TARGET_URL") or "https://t.me/kenigevents").strip(),
        discovery_results_path=(os.getenv("ACQ_DISCOVERY_RESULTS_PATH") or "").strip(),
        fixture_path=(os.getenv("ACQ_DISCOVERY_FIXTURE_PATH") or "").strip(),
        use_sample_fixture=_bool("ACQ_DISCOVERY_USE_SAMPLE", False),
    )
