from __future__ import annotations

from typing import Any

from . import playwright_discovery as _base
from .source_config import DobroSourceConfig


async def _visible_exact_region_candidate(page: Any, region_name: str) -> Any | None:
    """Return the visible exact region option from Dobro.ru's duplicated DOM.

    Dobro.ru currently keeps hidden and visible copies of picker options. A
    locator's `.first` can therefore point at a hidden node even when a visible
    exact option exists. Enumerating a bounded set of semantically eligible
    nodes keeps the selector evidence-based and prevents a city option such as
    `Калининградская обл, г Советск` from being accepted for the region row.
    """

    expected = " ".join(region_name.split()).casefold()
    selectors = (
        "button:visible",
        "[role='option']:visible",
        "[role='radio']:visible",
        "[role='checkbox']:visible",
        "label:visible",
    )
    for selector in selectors:
        locator = page.locator(selector)
        try:
            count = min(await locator.count(), 100)
        except Exception:
            continue
        for index in range(count):
            node = locator.nth(index)
            try:
                text = " ".join((await node.inner_text()).split()).casefold()
            except Exception:
                continue
            if text == expected:
                return node
    return None


async def discover_event_urls(config: DobroSourceConfig):
    # Install the live-DOM compatibility resolver only at this adapter boundary;
    # fixtures and the rest of the source parser remain deterministic.
    original = _base._region_candidate
    _base._region_candidate = _visible_exact_region_candidate
    try:
        return await _base.discover_event_urls(config)
    finally:
        _base._region_candidate = original
