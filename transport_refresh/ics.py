from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


def _escape(value: Any) -> str:
    return str(value or "").replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")


def _utc_stamp(value: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("ICS datetime must be timezone-aware")
    return parsed.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _calendar(uid: str, summary: str, start: str, end: str, description: str, location: str, url: str, *, alarm: bool) -> str:
    lines = [
        "BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//KenigEvents//Transport v1//RU", "CALSCALE:GREGORIAN",
        "BEGIN:VEVENT", f"UID:{_escape(uid)}", f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        f"DTSTART:{_utc_stamp(start)}", f"DTEND:{_utc_stamp(end)}", f"SUMMARY:{_escape(summary)}",
        f"DESCRIPTION:{_escape(description)}", f"LOCATION:{_escape(location)}", f"URL:{_escape(url)}",
    ]
    if alarm:
        lines.extend(["BEGIN:VALARM", "TRIGGER:-PT30M", "ACTION:DISPLAY", f"DESCRIPTION:{_escape(summary)}", "END:VALARM"])
    lines.extend(["END:VEVENT", "END:VCALENDAR", ""])
    return "\r\n".join(lines)


def build_event_ics(event: dict[str, Any], *, public_url: str) -> str:
    start = event.get("start_datetime")
    end = event.get("end_datetime")
    if not start or not end:
        raise ValueError("event ICS requires explicit occurrence start_datetime and end_datetime")
    uid = f"event-{event['id']}-{str(start)[:10]}@kenigevents.ru"
    return _calendar(uid, str(event.get("title") or "Событие"), str(start), str(end), str(event.get("description") or ""), str(event.get("venue_name") or ""), public_url, alarm=False)


def build_transport_ics(event: dict[str, Any], service: dict[str, Any], *, public_url: str) -> str:
    departure = str(service["departure"]["time"])
    arrival = str(service["arrival"]["time"])
    route = service.get("route") or {}
    stops = {str(stop.get("id")): stop for stop in service.get("stops") or []}
    dep_stop = stops.get(str(service["departure"]["stop_id"])) or {}
    arr_stop = stops.get(str(service["arrival"]["stop_id"])) or {}
    stable = f"{service.get('provider')}|{service.get('trip_id')}|{service.get('service_date')}|{departure}|{event.get('id')}"
    uid = f"transport-{hashlib.sha256(stable.encode()).hexdigest()[:24]}@kenigevents.ru"
    summary = f"{dep_stop.get('name')} → {arr_stop.get('name')}"
    description = f"{route.get('name')} · {service.get('provider')} · событие: {event.get('title') or ''}"
    return _calendar(uid, summary, departure, arrival, description, str(dep_stop.get("name") or ""), public_url, alarm=True)
