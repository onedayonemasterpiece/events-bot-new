from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("Europe/Kaliningrad")


def _normalize(value: Any) -> str:
    return re.sub(r"[^0-9a-zа-я]+", " ", str(value or "").lower().replace("ё", "е")).strip()


def _event_datetime(event: dict[str, Any], prefix: str) -> datetime | None:
    value = event.get(f"{prefix}_datetime")
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.astimezone(TIMEZONE) if parsed.tzinfo else parsed.replace(tzinfo=TIMEZONE)
        except ValueError:
            return None
    date_value = event.get("start_date" if prefix == "start" else "end_date") or event.get("start_date")
    time_value = event.get("start_time" if prefix == "start" else "end_time") or event.get("time_range_end")
    if not date_value or not time_value:
        return None
    try:
        return datetime.fromisoformat(f"{date_value}T{str(time_value)[:5]}:00").replace(tzinfo=TIMEZONE)
    except ValueError:
        return None


def is_transport_eligible_event(event: dict[str, Any]) -> bool:
    """Fail closed: only named, located events outside Kaliningrad are eligible."""

    city = _normalize(event.get("city"))
    venue = _normalize(event.get("venue_name") or event.get("location_name"))
    if not city or city in {"калининград", "город калининград", "г калининград"} or not venue:
        return False
    try:
        lat = float(event.get("venue_lat"))
        lon = float(event.get("venue_lon"))
    except (TypeError, ValueError):
        return False
    return -90 <= lat <= 90 and -180 <= lon <= 180 and _event_datetime(event, "start") is not None


def select_event_transport(
    event: dict[str, Any],
    combined_manifest: dict[str, Any],
    *,
    max_options_per_direction: int = 2,
) -> dict[str, list[dict[str, Any]]]:
    if not is_transport_eligible_event(event):
        return {"outbound": [], "return": []}
    start = _event_datetime(event, "start")
    end = _event_datetime(event, "end")
    assert start is not None
    city = _normalize(event.get("city"))
    venue = _normalize(event.get("venue_name") or event.get("location_name"))
    event_id = str(event.get("id") or "")
    matches: dict[str, list[dict[str, Any]]] = {"outbound": [], "return": []}
    for service in combined_manifest.get("services") or []:
        if str(service.get("service_date")) != start.date().isoformat():
            continue
        binding = service.get("binding") or {}
        bound_venue = _normalize((binding.get("venue") or {}).get("name"))
        aliases = {_normalize(item) for item in (binding.get("venue_aliases") or [])}
        bound_ids = {str(item) for item in (binding.get("event_ids") or [])}
        if _normalize(binding.get("city")) != city:
            continue
        if event_id and bound_ids and event_id not in bound_ids:
            continue
        if venue != bound_venue and venue not in aliases:
            continue
        try:
            departure = datetime.fromisoformat(str(service["departure"]["time"]).replace("Z", "+00:00"))
            arrival = datetime.fromisoformat(str(service["arrival"]["time"]).replace("Z", "+00:00"))
        except (KeyError, ValueError, TypeError):
            continue
        direction = service.get("direction")
        if direction == "outbound" and arrival <= start:
            enriched = dict(service)
            enriched["minutes_before_event"] = int((start - arrival).total_seconds() // 60)
            matches["outbound"].append(enriched)
        elif direction == "return" and end is not None and departure >= end:
            enriched = dict(service)
            enriched["minutes_after_event"] = int((departure - end).total_seconds() // 60)
            matches["return"].append(enriched)
    matches["outbound"].sort(key=lambda item: (item["minutes_before_event"], item["arrival"]["time"]))
    matches["return"].sort(key=lambda item: (item["minutes_after_event"], item["departure"]["time"]))
    matches["outbound"] = matches["outbound"][:max_options_per_direction]
    # A return is never promised when the event has no explicit end.
    matches["return"] = matches["return"][:max_options_per_direction] if end is not None else []
    return matches
