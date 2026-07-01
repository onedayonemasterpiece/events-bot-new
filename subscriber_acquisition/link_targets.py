from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import AcqConfig, load_config

VALID_LINK_TARGET_KINDS = {
    "event_site", "pka_channel", "pka_afisha_event", "event_telegraph", "event_source",
    "topic_landing", "sticker_pack", "other", "none",
}


@dataclass(frozen=True)
class LinkTargetChoice:
    kind: str
    url: str | None
    label: str | None
    reason: str | None
    fallback_url: str | None = None


def _event_url(event: Any, *names: str) -> str | None:
    for name in names:
        value = str(getattr(event, name, "") or "").strip()
        if value.startswith(("http://", "https://")):
            return value
    return None


def select_link_target(
    *,
    topic_cluster: str | None = None,
    candidate_events: list[Any] | None = None,
    context_kind: str | None = None,
    sticker_fit: str | None = None,
    config: AcqConfig | None = None,
) -> LinkTargetChoice:
    cfg = config or load_config()
    events = list(candidate_events or [])
    single = events[0] if len(events) == 1 else None
    if sticker_fit in {"strong", "possible"} and context_kind == "sticker":
        return LinkTargetChoice("sticker_pack", None, f"Стикерпак: {topic_cluster or 'события'}", "в контексте потенциально уместнее стикер", cfg.default_link_target_url)
    if single is not None:
        site = _event_url(single, "ticket_link", "site_url", "event_url")
        if site:
            return LinkTargetChoice("event_site", site, "страница конкретного события", "вопрос про конкретное событие и есть сайт/билеты", cfg.default_link_target_url)
        pka = _event_url(single, "source_tg_post_url", "pka_afisha_url")
        if pka and cfg.pka_afisha_channel_url and cfg.pka_afisha_channel_url in pka:
            return LinkTargetChoice("pka_afisha_event", pka, "пост в «Полюбить Калининград Афиша»", "есть нативный пост Афиши", cfg.default_link_target_url)
        telegraph = _event_url(single, "telegraph_url")
        if telegraph:
            return LinkTargetChoice("event_telegraph", telegraph, "Telegraph-страница события", "есть подробная страница события", cfg.default_link_target_url)
        source = _event_url(single, "source_post_url", "source_vk_post_url")
        if source:
            return LinkTargetChoice("event_source", source, "исходный пост организатора", "есть публичный source", cfg.default_link_target_url)
    if topic_cluster:
        return LinkTargetChoice("topic_landing", cfg.default_link_target_url, topic_cluster, "вопрос про направление, а не одно событие", cfg.default_link_target_url)
    if cfg.pka_channel_url:
        return LinkTargetChoice("pka_channel", cfg.pka_channel_url, "Полюбить Калининград Анонсы", "широкий запрос без уверенного события", cfg.default_link_target_url)
    return LinkTargetChoice("none", None, None, "нет безопасного target", None)
