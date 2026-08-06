from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping

import requests

from .dobro_adapter import VacancyTarget, extract_event_urls, is_in_target_region, parse_event_page
from .playwright_live import discover_event_urls
from .source_config import DobroSourceConfig
from .types import (
    AvailabilityStatus,
    MonitorResult,
    MonitorRunStatus,
    VolunteerOpportunity,
    canonical_json_hash,
)


class MonitorTransportError(RuntimeError):
    pass


def run_fixture_monitor(
    *,
    search_html: str,
    event_html_by_url: Mapping[str, str],
    config: DobroSourceConfig | None = None,
    checked_at: datetime | None = None,
    today: date | None = None,
) -> MonitorResult:
    effective_config = config or DobroSourceConfig()
    effective_config.validate()
    checked = checked_at or datetime.now(timezone.utc)
    urls = extract_event_urls(search_html)
    if not urls:
        raise MonitorTransportError("fixture search contains no canonical event URLs")
    if len(urls) > effective_config.max_items:
        raise MonitorTransportError("fixture search exceeds configured max_items")

    opportunities: list[VolunteerOpportunity] = []
    warnings: list[str] = []
    outside_region = 0
    for url in urls:
        html = event_html_by_url.get(url)
        if html is None:
            raise MonitorTransportError(f"fixture event body is missing: {url}")
        item = parse_event_page(html, source_url=url, checked_at=checked, today=today)
        if not is_in_target_region(item, effective_config.region_name):
            outside_region += 1
            warnings.append(f"outside_target_region:{url}")
            continue
        opportunities.append(item)

    if not opportunities and outside_region:
        raise MonitorTransportError(
            "all fixture rows are outside the configured region; refusing a false regional success"
        )
    status = MonitorRunStatus.PARTIAL if warnings else MonitorRunStatus.PASS
    return MonitorResult(
        mode="fixture",
        generated_at=checked,
        source_pages_seen=len(urls),
        opportunities=tuple(opportunities),
        run_status=status,
        warnings=tuple(warnings),
        outside_region_count=outside_region,
    )


def _fetch(config: DobroSourceConfig, url: str) -> str:
    response = requests.get(
        url,
        timeout=config.detail_timeout_seconds,
        headers={
            "User-Agent": (
                "KenigEventsVolunteerMonitor/0.1 "
                "(+https://kenigevents.ru; read-only approved monitoring)"
            )
        },
    )
    response.raise_for_status()
    if len(response.content) > config.max_response_bytes:
        raise MonitorTransportError(f"unbounded Dobro.ru response: {url}")
    content_type = (response.headers.get("content-type") or "").casefold()
    if content_type and "html" not in content_type:
        raise MonitorTransportError(f"unexpected Dobro.ru content type: {content_type}")
    return response.text


def _card_proves_region(card_text: str, region_name: str) -> bool:
    target = " ".join(region_name.casefold().replace("ё", "е").split())
    target_root = target.replace("область", "").replace("обл.", "").replace("обл", "").strip()
    haystack = " ".join(card_text.casefold().replace("ё", "е").split())
    return any(value and value in haystack for value in (target, target_root))


def _apply_vacancy_inventory(
    item: VolunteerOpportunity,
    targets: tuple[VacancyTarget, ...],
) -> VolunteerOpportunity:
    if not targets:
        return item
    vacancy_ids = tuple(target.vacancy_id for target in targets)
    application_urls = tuple(target.application_url for target in targets)
    card_texts = tuple(target.card_text for target in targets if target.card_text)
    semantic_hash = canonical_json_hash(
        {
            "parent_semantic_hash": item.semantic_hash,
            "vacancy_ids": vacancy_ids,
            "application_urls": application_urls,
            "vacancy_card_texts": card_texts,
        }
    )
    availability_hash = canonical_json_hash(
        {
            "parent_event_id": item.source_external_id,
            "vacancy_ids": vacancy_ids,
            "application_urls": application_urls,
            "availability_status": AvailabilityStatus.OPEN.value,
            "availability_reason": "active_vacancy_inventory",
        }
    )
    return replace(
        item,
        vacancy_ids=vacancy_ids,
        application_urls=application_urls,
        vacancy_card_texts=card_texts,
        availability_status=AvailabilityStatus.OPEN,
        availability_reason="active_vacancy_inventory",
        semantic_hash=semantic_hash,
        availability_hash=availability_hash,
    )


async def run_live_monitor(
    *,
    config: DobroSourceConfig,
    checked_at: datetime | None = None,
) -> MonitorResult:
    config.validate()
    checked = checked_at or datetime.now(timezone.utc)
    discovery = await discover_event_urls(config)
    if discovery.explicit_zero_supply:
        return MonitorResult(
            mode="live_canary",
            generated_at=checked,
            source_pages_seen=0,
            opportunities=(),
            run_status=MonitorRunStatus.WARN_NO_LIVE_SUPPLY,
            warnings=("WARN_NO_LIVE_SUPPLY",),
        )

    selected = discovery.urls[: config.max_items]
    selected_set = set(selected)
    targets_by_event: dict[str, list[VacancyTarget]] = {}
    for target in discovery.vacancies:
        if target.event_url in selected_set:
            targets_by_event.setdefault(target.event_url, []).append(target)

    opportunities: list[VolunteerOpportunity] = []
    warnings: list[str] = []
    outside_region = 0
    for url in selected:
        targets = tuple(targets_by_event.get(url, ()))
        try:
            html = await asyncio.to_thread(_fetch, config, url)
            item = parse_event_page(html, source_url=url, checked_at=checked)
            region_proven = is_in_target_region(item, config.region_name) or any(
                _card_proves_region(target.card_text, config.region_name) for target in targets
            )
            if not region_proven:
                outside_region += 1
                warnings.append(f"outside_target_region:{url}")
                continue
            opportunities.append(_apply_vacancy_inventory(item, targets))
        except Exception as exc:
            warnings.append(f"{url}: {type(exc).__name__}: {exc}")

    if not opportunities:
        raise MonitorTransportError(
            "all discovered Dobro.ru rows failed extraction or target-region proof; refusing empty success"
        )
    status = MonitorRunStatus.PARTIAL if warnings else MonitorRunStatus.PASS
    return MonitorResult(
        mode="live_canary",
        generated_at=checked,
        source_pages_seen=len(selected),
        opportunities=tuple(opportunities),
        run_status=status,
        warnings=tuple(warnings),
        outside_region_count=outside_region,
    )


def read_event_map(path: Path) -> dict[str, str]:
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("event map must be a JSON object")
    result: dict[str, str] = {}
    for url, value in payload.items():
        item_path = Path(str(value))
        if not item_path.is_absolute():
            item_path = path.parent / item_path
        result[str(url)] = item_path.read_text(encoding="utf-8")
    return result
