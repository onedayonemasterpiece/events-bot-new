from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from . import playwright_discovery as _base
from .dobro_adapter import VacancyTarget, extract_event_urls, extract_vacancy_targets
from .source_config import DobroSourceConfig


@dataclass(slots=True, frozen=True)
class LiveDiscoveryResult:
    urls: tuple[str, ...]
    vacancies: tuple[VacancyTarget, ...]
    region_proven: bool
    available_filter_proven: bool
    explicit_zero_supply: bool
    load_more_clicks: int


async def _first_visible(locator: Any, *, limit: int = 20) -> Any | None:
    try:
        count = min(await locator.count(), limit)
    except Exception:
        return None
    for index in range(count):
        candidate = locator.nth(index)
        try:
            if await candidate.is_visible():
                return candidate
        except Exception:
            continue
    return None


def _current_location_title(page: Any) -> str:
    try:
        values = parse_qs(urlsplit(page.url).query).get("location[title]", [])
        return unquote(str(values[0])).strip() if values else ""
    except Exception:
        return ""


async def _visible_location_search_input(page: Any) -> Any | None:
    placeholders = (
        "Введите название",
        "Поиск населённого пункта",
        "Поиск региона",
        "Город или регион",
        "В каком месте?",
    )
    for placeholder in placeholders:
        candidate = await _first_visible(
            page.get_by_placeholder(placeholder, exact=True), limit=10
        )
        if candidate is not None:
            return candidate
    return None


async def _open_current_location_filter(page: Any) -> bool:
    """Open either observed Dobro.ru location-entry state.

    A cold search page has two valid source states:

    1. a geolocation confirmation popover with `Изменить`;
    2. an already resolved location, encoded in the URL and rendered as a
       `button[data-slot=popover-anchor]` such as `Москва`.

    The monitor accepts only those source-backed controls and waits boundedly
    for the actual location search input after the click.
    """

    for _ in range(80):
        if await _visible_location_search_input(page) is not None:
            return True

        current_title = _current_location_title(page)
        locators = [
            page.get_by_role("button", name="Изменить", exact=True),
            page.get_by_text("Изменить", exact=True),
        ]
        if current_title:
            locators.extend(
                [
                    page.get_by_role("button", name=current_title, exact=True),
                    page.get_by_text(current_title, exact=True),
                ]
            )
        locators.append(page.locator("button[data-slot='popover-anchor']"))

        clicked = False
        for locator in locators:
            candidate = await _first_visible(locator, limit=20)
            if candidate is None:
                continue
            try:
                if current_title and locator == locators[-1]:
                    text = " ".join((await candidate.inner_text()).split())
                    if text.casefold() != current_title.casefold():
                        continue
                await candidate.click()
                clicked = True
                break
            except Exception:
                continue
        if clicked:
            for _ in range(20):
                if await _visible_location_search_input(page) is not None:
                    return True
                await page.wait_for_timeout(150)
        await page.wait_for_timeout(250)
    return False


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
    """Wait for Dobro.ru's visible exact region option in a duplicated DOM."""

    for _ in range(24):
        candidate = await _scan_visible_exact_region_candidate(page, region_name)
        if candidate is not None:
            return candidate
        await page.wait_for_timeout(250)
    return None


async def _select_region_live(page: Any, region_name: str) -> bool:
    """Select the configured region across both observed cold-start flows."""

    target = " ".join(region_name.split()).casefold()
    if _current_location_title(page).casefold() == target:
        return True

    await _base._dismiss_cookie_banner(page)
    if not await _open_current_location_filter(page):
        raise _base.DiscoveryError("cannot open Dobro.ru location filter")

    candidate = await _visible_exact_region_candidate(page, region_name)
    if candidate is None:
        field = await _visible_location_search_input(page)
        if field is None:
            raise _base.DiscoveryError("cannot find Dobro.ru location search input")
        try:
            await field.fill(region_name)
        except Exception as exc:
            raise _base.DiscoveryError("cannot fill Dobro.ru location search input") from exc
        candidate = await _visible_exact_region_candidate(page, region_name)
    if candidate is None:
        raise _base.DiscoveryError(f"cannot select Dobro.ru region: {region_name}")
    try:
        await candidate.click()
    except Exception as exc:
        raise _base.DiscoveryError(f"cannot click Dobro.ru region: {region_name}") from exc

    for label in ("Выбрать", "Сохранить", "Применить", "Найти", "Показать"):
        confirm = await _first_visible(
            page.get_by_role("button", name=label, exact=True), limit=10
        )
        if confirm is None:
            continue
        try:
            await confirm.click()
            break
        except Exception:
            continue

    for _ in range(48):
        if _current_location_title(page).casefold() == target:
            return True
        selected = await _base._selected_text(page)
        if target in selected:
            return True
        await page.wait_for_timeout(250)
    raise _base.DiscoveryError("region selection was not reflected in source state")


async def _vacancy_surface_ready(page: Any) -> bool:
    """Wait until vacancy cards or an explicit zero state replace the tab shell."""

    application_links = page.locator("a[href*='__target_path'][href*='vacancy']")
    for _ in range(48):
        try:
            if await application_links.count() > 0:
                return True
            body_text = await page.locator("body").inner_text()
            if _base._explicit_zero_supply(body_text):
                return True
        except Exception:
            pass
        await page.wait_for_timeout(250)
    return False


async def _activate_current_vacancy_surface(page: Any) -> bool:
    """Activate Dobro.ru's current vacancy-only search surface."""

    tab = page.get_by_role("tab", name="Вакансии", exact=True)
    try:
        visible = await _first_visible(tab, limit=10)
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
                if await _vacancy_surface_ready(page):
                    return True
                raise _base.DiscoveryError(
                    "Dobro.ru vacancies tab became active but its result panel did not settle"
                )
            await page.wait_for_timeout(200)
    except _base.DiscoveryError:
        raise
    except Exception as exc:
        raise _base.DiscoveryError("cannot activate Dobro.ru vacancies tab") from exc
    raise _base.DiscoveryError("Dobro.ru vacancies tab did not become active")


def _write_enriched_receipt(config: DobroSourceConfig, result: LiveDiscoveryResult) -> None:
    if config.evidence_dir is None:
        return
    root = Path(config.evidence_dir)
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "urls": list(result.urls),
        "vacancy_count": len(result.vacancies),
        "vacancies": [target.to_dict() for target in result.vacancies],
        "region_proven": result.region_proven,
        "available_filter_proven": result.available_filter_proven,
        "explicit_zero_supply": result.explicit_zero_supply,
        "load_more_clicks": result.load_more_clicks,
    }
    (root / "discovery-receipt.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


async def discover_event_urls(config: DobroSourceConfig) -> LiveDiscoveryResult:
    """Run base navigation while retaining the current vacancy-grain inventory."""

    latest_vacancies: list[VacancyTarget] = []

    def capture_urls(html: str, *, base_url: str = "https://dobro.ru") -> list[str]:
        nonlocal latest_vacancies
        latest_vacancies = extract_vacancy_targets(html, base_url=base_url)
        return extract_event_urls(html, base_url=base_url)

    original_select = _base._select_region
    original_region = _base._region_candidate
    original_available = _base._enable_available_vacancies
    original_extract = _base.extract_event_urls
    _base._select_region = _select_region_live
    _base._region_candidate = _visible_exact_region_candidate
    _base._enable_available_vacancies = _activate_current_vacancy_surface
    _base.extract_event_urls = capture_urls
    try:
        base_result = await _base.discover_event_urls(config)
        result = LiveDiscoveryResult(
            urls=base_result.urls,
            vacancies=tuple(latest_vacancies),
            region_proven=base_result.region_proven,
            available_filter_proven=base_result.available_filter_proven,
            explicit_zero_supply=base_result.explicit_zero_supply,
            load_more_clicks=base_result.load_more_clicks,
        )
        _write_enriched_receipt(config, result)
        return result
    finally:
        _base._select_region = original_select
        _base._region_candidate = original_region
        _base._enable_available_vacancies = original_available
        _base.extract_event_urls = original_extract
