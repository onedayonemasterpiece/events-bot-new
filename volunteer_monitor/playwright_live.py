from __future__ import annotations

from typing import Any

from . import playwright_discovery as _base
from .source_config import DobroSourceConfig


async def _scan_visible_exact_region_candidate(page: Any, region_name: str) -> Any | None:
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


async def _visible_exact_region_candidate(page: Any, region_name: str) -> Any | None:
    """Wait for Dobro.ru's visible exact region option in a duplicated DOM.

    Search suggestions are populated asynchronously after the input value is
    committed. Failure evidence can therefore contain the option even though a
    one-shot lookup immediately after `fill()` missed it. Polling remains
    bounded to five seconds and accepts only the exact region-level label, not
    city rows such as `Калининградская обл, г Советск`.
    """

    for _ in range(20):
        candidate = await _scan_visible_exact_region_candidate(page, region_name)
        if candidate is not None:
            return candidate
        await page.wait_for_timeout(250)
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
