"""HTTP parser for the current filarmonia39.ru event catalog."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger("PhilharmoniaParser")

BASE_URL = "https://filarmonia39.ru"
LISTING_URL = f"{BASE_URL}/afisha/"
REQUEST_TIMEOUT_SECONDS = 45
OUTPUT_PATH = Path("/kaggle/working/philharmonia_results.json")
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def _clean_text(node: Tag | None) -> str:
    if node is None:
        return ""
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()


def _absolute_url(value: str | None) -> str:
    return urljoin(BASE_URL, str(value or "").strip())


def _price_bounds(text: str) -> tuple[int | None, int | None]:
    values = [int(raw.replace(" ", "")) for raw in re.findall(r"\d[\d ]*", text or "")]
    if not values:
        return None, None
    return min(values), max(values)


def parse_listing_html(
    html: str,
    *,
    today: date | None = None,
) -> list[dict[str, Any]]:
    """Extract future listing cards without depending on fragile month URLs."""

    current_day = today or date.today()
    soup = BeautifulSoup(html, "html.parser")
    events: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for card in soup.select("article.entry[data-date-iso]"):
        iso_date = str(card.get("data-date-iso") or "").strip()
        try:
            event_day = date.fromisoformat(iso_date)
        except ValueError:
            logger.warning("philharmonia: invalid listing date=%r", iso_date)
            continue
        if event_day < current_day:
            continue

        detail_link = card.select_one("a.production_detail_link[href]")
        title = (
            str(detail_link.get("aria-label") or "").strip()
            if detail_link is not None
            else ""
        )
        if not title:
            title = _clean_text(card.select_one("h2.heading"))
        detail_url = _absolute_url(
            str(detail_link.get("href") or "") if detail_link is not None else ""
        )
        time_text = _clean_text(card.select_one(".session .hour")) or "00:00"
        key = (detail_url, iso_date, time_text)
        if not title or not detail_url or key in seen:
            continue
        seen.add(key)

        image = card.select_one("img.production_image[src]")
        image_url = _absolute_url(str(image.get("src") or "")) if image else ""
        buy_link = card.select_one("a.buy_tickets[href]")
        events.append(
            {
                "title": title,
                "url": detail_url,
                "ticket_url": _absolute_url(
                    str(buy_link.get("href") or "") if buy_link else ""
                ),
                "date_text": str(card.get("data-human-date") or iso_date).strip(),
                "normalized_date": iso_date,
                "time": time_text,
                "age_restriction": str(card.get("data-age") or "").strip(),
                "image_url": image_url,
                "description": "",
                "listing_text": str(card.get("data-search") or "").strip(),
                "price_min": None,
                "price_max": None,
                "ticket_status": "available" if buy_link else "unavailable",
                "pushkin_card": bool(card.select_one(".list_logo_pushkin_card")),
                "scene": str(card.get("data-venue") or "").strip(),
            }
        )
    return events


def enrich_event_from_detail_html(event: dict[str, Any], html: str) -> dict[str, Any]:
    """Attach source-grounded detail text, price and live ticket status."""

    enriched = dict(event)
    soup = BeautifulSoup(html, "html.parser")
    description = _clean_text(
        soup.select_one(".production_description .text_container")
    )
    if not description:
        raise ValueError(f"detail description missing for {event.get('url')}")
    enriched["description"] = description

    price_min, price_max = _price_bounds(
        _clean_text(soup.select_one(".price_block .value"))
    )
    enriched["price_min"] = price_min
    enriched["price_max"] = price_max

    live_session = soup.select_one(
        ".session_entry[data-expired='false'][data-can-buy-tickets='true']"
    )
    buy_link = soup.select_one("a.buy_button[href], .production_sticky_buy_button a[href]")
    enriched["ticket_status"] = (
        "available" if live_session is not None or buy_link is not None else "unavailable"
    )
    if buy_link is not None:
        enriched["ticket_url"] = _absolute_url(str(buy_link.get("href") or ""))
    return enriched


def fetch_philharmonia_events(
    *,
    today: date | None = None,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch the live catalog and every future event detail page."""

    client = session or requests.Session()
    client.headers.setdefault("User-Agent", USER_AGENT)
    response = client.get(LISTING_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    events = parse_listing_html(response.text, today=today)
    if not events:
        raise RuntimeError("philharmonia listing returned zero future events")

    results: list[dict[str, Any]] = []
    for index, event in enumerate(events, 1):
        detail = client.get(event["url"], timeout=REQUEST_TIMEOUT_SECONDS)
        detail.raise_for_status()
        results.append(enrich_event_from_detail_html(event, detail.text))
        logger.info(
            "philharmonia: parsed %d/%d %s %s %s",
            index,
            len(events),
            event["normalized_date"],
            event["time"],
            event["title"],
        )
    return results


def _load_status_client() -> Any | None:
    """Load the optional callback client from an attached private status dataset."""

    try:
        from kaggle_status_client import load_status_client

        return load_status_client(log=lambda message: logger.info(message))
    except Exception as exc:
        logger.warning("philharmonia: status client unavailable: %s", exc)
        return None


def main() -> None:
    """Kaggle script entry point; keep the production parser self-contained."""

    status_client = _load_status_client()
    if status_client is not None and status_client.enabled:
        status_client.event(
            "kernel_started",
            phase="parse",
            status="running",
            progress={"progress_percent": 10, "progress_label": "загрузка афиши"},
        )
    try:
        events = fetch_philharmonia_events()
        output_path = OUTPUT_PATH
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(events, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("philharmonia: wrote %d events to %s", len(events), output_path)
        if status_client is not None and status_client.enabled:
            status_client.event(
                "report_written",
                phase="report",
                status="done",
                progress={
                    "events_done": len(events),
                    "events_total": len(events),
                    "progress_percent": 100,
                    "progress_label": f"события {len(events)}/{len(events)}",
                },
            )
    except Exception as exc:
        if status_client is not None and status_client.enabled:
            status_client.event(
                "kernel_failed",
                phase="parse",
                status="failed",
                message=str(exc),
            )
        raise
    finally:
        if status_client is not None:
            status_client.stop_alive()


if __name__ == "__main__":
    main()
