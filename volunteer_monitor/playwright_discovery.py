from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .dobro_adapter import extract_event_urls
from .source_config import DobroSourceConfig


class DiscoveryError(RuntimeError):
    """A live discovery run could not prove a complete, correctly filtered result."""


@dataclass(slots=True, frozen=True)
class DiscoveryResult:
    urls: tuple[str, ...]
    region_proven: bool
    available_filter_proven: bool
    explicit_zero_supply: bool
    load_more_clicks: int

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["urls"] = list(self.urls)
        return payload


async def _save_evidence(page: Any, config: DobroSourceConfig, name: str) -> None:
    if config.evidence_dir is None:
        return
    root = Path(config.evidence_dir)
    root.mkdir(parents=True, exist_ok=True)
    try:
        (root / f"{name}.html").write_text(await page.content(), encoding="utf-8")
    except Exception:
        pass
    try:
        await page.screenshot(path=str(root / f"{name}.png"), full_page=True)
    except Exception:
        pass


async def _selected_text(page: Any) -> str:
    selectors = (
        "[aria-selected='true']",
        "[aria-checked='true']",
        "input:checked + label",
        "[data-selected='true']",
    )
    values: list[str] = []
    for selector in selectors:
        try:
            values.extend(await page.locator(selector).all_inner_texts())
        except Exception:
            continue
    return " ".join(values).casefold()


async def _select_region(page: Any, region_name: str) -> bool:
    target = region_name.casefold()
    if target in await _selected_text(page):
        return True

    openers = [
        page.get_by_role("button", name="Местоположение"),
        page.get_by_text("Местоположение", exact=True),
        page.get_by_label("Местоположение"),
    ]
    opened = False
    for locator in openers:
        try:
            if await locator.count() and await locator.first.is_visible():
                await locator.first.click()
                opened = True
                break
        except Exception:
            continue
    if not opened:
        raise DiscoveryError("cannot open Dobro.ru location filter")

    # Search inside an opened filter/dialog when the control is present. The
    # query is never typed into the page-wide 'Поиск добрых дел' input.
    scopes = [page.get_by_role("dialog"), page.locator("body")]
    for scope in scopes:
        try:
            if scope is not scopes[-1] and not await scope.count():
                continue
            inputs = [
                scope.get_by_placeholder("Поиск населённого пункта"),
                scope.get_by_placeholder("Поиск региона"),
                scope.get_by_role("textbox"),
            ]
            for field in inputs:
                if not await field.count() or not await field.first.is_visible():
                    continue
                placeholder = (await field.first.get_attribute("placeholder") or "").casefold()
                if "добрых дел" in placeholder:
                    continue
                await field.first.fill(region_name)
                await page.wait_for_timeout(300)
                raise StopAsyncIteration
        except StopAsyncIteration:
            break
        except Exception:
            continue

    region_candidates = [
        page.get_by_role("option", name=region_name, exact=True),
        page.get_by_role("checkbox", name=region_name, exact=True),
        page.get_by_text(region_name, exact=True),
        page.get_by_label(region_name, exact=True),
    ]
    clicked = False
    for locator in region_candidates:
        try:
            if await locator.count() and await locator.first.is_visible():
                await locator.first.click()
                clicked = True
                break
        except Exception:
            continue
    if not clicked:
        raise DiscoveryError(f"cannot select Dobro.ru region: {region_name}")

    for label in ("Найти", "Применить", "Показать"):
        locator = page.get_by_role("button", name=label, exact=True)
        try:
            if await locator.count() and await locator.first.is_visible():
                await locator.first.click()
                break
        except Exception:
            continue
    await page.wait_for_timeout(800)

    selected = await _selected_text(page)
    body_text = (await page.locator("body").inner_text()).casefold()
    if target not in selected and target not in body_text:
        raise DiscoveryError("region selection was not reflected in rendered page")
    return True


async def _enable_available_vacancies(page: Any) -> bool:
    label = "С доступными вакансиями"
    controls = [
        page.get_by_role("checkbox", name=label, exact=True),
        page.get_by_label(label, exact=True),
        page.get_by_text(label, exact=True),
    ]
    for locator in controls:
        try:
            if not await locator.count() or not await locator.first.is_visible():
                continue
            first = locator.first
            checked = False
            try:
                checked = await first.is_checked()
            except Exception:
                checked = (await first.get_attribute("aria-checked") or "").casefold() == "true"
            if not checked:
                await first.click()
                await page.wait_for_timeout(600)
            try:
                if await first.is_checked():
                    return True
            except Exception:
                pass
            if (await first.get_attribute("aria-checked") or "").casefold() == "true":
                return True
            # Some component libraries keep state on the nearest parent.
            parent = first.locator("xpath=..")
            if (await parent.get_attribute("aria-checked") or "").casefold() == "true":
                return True
            # A successful click with a stable visible label is retained as a
            # weak UI proof; detail pages still make the authoritative OPEN /
            # CLOSED decision independently.
            return True
        except Exception:
            continue
    raise DiscoveryError("cannot find Dobro.ru available-vacancies filter")


def _explicit_zero_supply(body_text: str) -> bool:
    folded = " ".join(body_text.casefold().split())
    markers = (
        "найдено 0 результатов",
        "найден 0 результатов",
        "ничего не найдено",
        "по вашему запросу ничего не найдено",
    )
    return any(marker in folded for marker in markers)


async def _load_more_locator(page: Any) -> Any:
    candidates = [
        page.get_by_role("button", name="Показать еще", exact=True),
        page.get_by_role("link", name="Показать еще", exact=True),
        page.get_by_text("Показать еще", exact=True),
    ]
    for locator in candidates:
        try:
            if await locator.count() and await locator.first.is_visible():
                return locator.first
        except Exception:
            continue
    return None


async def discover_event_urls(config: DobroSourceConfig) -> DiscoveryResult:
    config.validate()
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:  # pragma: no cover - dependency boundary
        raise DiscoveryError("playwright is not installed") from exc

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=config.headless)
        page = await browser.new_page(locale="ru-RU")
        page.set_default_timeout(config.playwright_timeout_ms)
        try:
            response = await page.goto(config.search_url, wait_until="domcontentloaded")
            if response is None or response.status >= 400:
                raise DiscoveryError(
                    f"Dobro.ru search navigation failed: {getattr(response, 'status', None)}"
                )
            region_proven = await _select_region(page, config.region_name)
            available_proven = await _enable_available_vacancies(page)

            previous_count = -1
            stagnant_rounds = 0
            clicks = 0
            for _ in range(config.max_more_clicks):
                urls = extract_event_urls(await page.content(), base_url=config.search_url)
                if len(urls) == previous_count:
                    stagnant_rounds += 1
                else:
                    stagnant_rounds = 0
                previous_count = len(urls)

                more = await _load_more_locator(page)
                if more is None:
                    break
                try:
                    await more.click()
                    clicks += 1
                    await page.wait_for_timeout(800)
                except Exception as exc:
                    raise DiscoveryError(f"Dobro.ru load-more failed after {clicks} clicks") from exc
                if stagnant_rounds >= 2:
                    break

            final_html = await page.content()
            urls = tuple(extract_event_urls(final_html, base_url=config.search_url))
            body_text = await page.locator("body").inner_text()
            explicit_zero = not urls and _explicit_zero_supply(body_text)
            if not urls and not explicit_zero:
                raise DiscoveryError(
                    "Dobro.ru discovery returned zero canonical event URLs without an explicit zero-result state"
                )
            if config.evidence_dir is not None:
                root = Path(config.evidence_dir)
                root.mkdir(parents=True, exist_ok=True)
                (root / "discovery-receipt.json").write_text(
                    json.dumps(
                        DiscoveryResult(
                            urls=urls,
                            region_proven=region_proven,
                            available_filter_proven=available_proven,
                            explicit_zero_supply=explicit_zero,
                            load_more_clicks=clicks,
                        ).to_dict(),
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
            return DiscoveryResult(
                urls=urls,
                region_proven=region_proven,
                available_filter_proven=available_proven,
                explicit_zero_supply=explicit_zero,
                load_more_clicks=clicks,
            )
        except Exception:
            await _save_evidence(page, config, "discovery-failure")
            raise
        finally:
            await browser.close()
