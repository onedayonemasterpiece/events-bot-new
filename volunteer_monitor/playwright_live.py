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


async def _activate_current_vacancy_surface(page: Any) -> bool:
    """Activate Dobro.ru's current vacancy-only search surface.

    The former `С доступными вакансиями` checkbox is no longer rendered in the
    current search UI. Its product role is now an explicit Radix tab named
    `Вакансии`. We require the tab to become active and still let every detail
    page independently decide OPEN/CLOSED/EXPIRED, so this compatibility step
    cannot turn a stale application into a public OPEN result.
    """

    tab = page.get_by_role("tab", name="Вакансии", exact=True)
    try:
        if not await tab.count():
            raise _base.DiscoveryError("cannot find Dobro.ru vacancies tab")
        # As with the region picker, duplicate hidden tab trees can exist.
        visible = None
        for index in range(min(await tab.count(), 10)):
            candidate = tab.nth(index)
            if await candidate.is_visible():
                visible = candidate
                break
        if visible is None:
            raise _base.DiscoveryError("Dobro.ru vacancies tab is not visible")
        if (
            (await visible.get_attribute("aria-selected") or "").casefold() != "true"
            and (await visible.get_attribute("data-state") or "").casefold() != "active"
        ):
            await visible.click()
        for _ in range(30):
            aria_selected = (await visible.get_attribute("aria-selected") or "").casefold()
            data_state = (await visible.get_attribute("data-state") or "").casefold()
            if aria_selected == "true" or data_state == "active":
                # Allow the tab panel to finish replacing all-search cards.
                await page.wait_for_timeout(750)
                return True
            await page.wait_for_timeout(200)
    except _base.DiscoveryError:
        raise
    except Exception as exc:
        raise _base.DiscoveryError("cannot activate Dobro.ru vacancies tab") from exc
    raise _base.DiscoveryError("Dobro.ru vacancies tab did not become active")


async def discover_event_urls(config: DobroSourceConfig):
    # Install live-DOM compatibility resolvers only at this adapter boundary;
    # fixtures and the source parser remain deterministic.
    original_region = _base._region_candidate
    original_available = _base._enable_available_vacancies
    _base._region_candidate = _visible_exact_region_candidate
    _base._enable_available_vacancies = _activate_current_vacancy_surface
    try:
        return await _base.discover_event_urls(config)
    finally:
        _base._region_candidate = original_region
        _base._enable_available_vacancies = original_available
