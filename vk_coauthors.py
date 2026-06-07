from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Protocol


@dataclass(frozen=True, slots=True)
class VkCoauthorCandidate:
    screen_name: str
    url: str
    label: str


class CoauthorEvent(Protocol):
    title: str | None
    description: str | None
    source_text: str | None
    location_name: str | None
    source_post_url: str | None


_COAUTHOR_RULES: tuple[tuple[VkCoauthorCandidate, tuple[str, ...]], ...] = (
    (
        VkCoauthorCandidate(
            screen_name="prodetstvosu",
            url="https://vk.com/prodetstvosu",
            label="Музей Советского детства БФУ",
        ),
        (
            "prodetstvosu",
            "музей советского детства",
            "советского детства бфу",
        ),
    ),
    (
        VkCoauthorCandidate(
            screen_name="konb39",
            url="https://vk.com/konb39",
            label="Калининградская областная научная библиотека",
        ),
        (
            "konb39",
            "калининградская областная научная библиотека",
            "областная научная библиотека",
            "научная библиотека",
            "конб",
        ),
    ),
    (
        VkCoauthorCandidate(
            screen_name="oldlunetbaltkosa",
            url="https://vk.com/oldlunetbaltkosa",
            label="Старый Люнет",
        ),
        (
            "oldlunetbaltkosa",
            "старый люнет",
        ),
    ),
    (
        VkCoauthorCandidate(
            screen_name="filarmonia39",
            url="https://vk.com/filarmonia39",
            label="Калининградская филармония",
        ),
        (
            "filarmonia39",
            "калининградская филармония",
            "филармония",
        ),
    ),
)


def _norm(value: str | None) -> str:
    text = str(value or "").casefold()
    text = re.sub(r"https?://", " ", text)
    text = re.sub(r"[^a-zа-яё0-9_]+", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def select_vk_coauthor_candidate(
    event: CoauthorEvent,
    *,
    source_urls: Iterable[str | None] = (),
) -> VkCoauthorCandidate | None:
    haystack = _norm(
        " ".join(
            [
                str(getattr(event, "title", "") or ""),
                str(getattr(event, "description", "") or ""),
                str(getattr(event, "source_text", "") or ""),
                str(getattr(event, "location_name", "") or ""),
                str(getattr(event, "source_post_url", "") or ""),
                *[str(url or "") for url in source_urls],
            ]
        )
    )
    if not haystack:
        return None
    padded = f" {haystack} "
    for candidate, needles in _COAUTHOR_RULES:
        for needle in needles:
            n = _norm(needle)
            if n and f" {n} " in padded:
                return candidate
    return None

