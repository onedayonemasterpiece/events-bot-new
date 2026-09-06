"""Read-only canonical event tokens; no publication or inferred media readiness.

RouteReadinessEvidence is supplied only by a trusted internal verifier, never by
model output or request JSON. It asserts current public route/revision inclusion;
a queued job, build success, or secret candidate receipt is insufficient.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
import re
import sqlite3
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from static_site_release import event_public_revision

KALININGRAD = ZoneInfo("Europe/Kaliningrad")


class EventResolutionError(ValueError):
    pass


@dataclass(frozen=True)
class RouteReadinessEvidence:
    event_id: int
    event_revision: str
    slug: str
    href: str
    verified_at: datetime
    expires_at: datetime


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise EventResolutionError("timezone_required")
    return value.astimezone(timezone.utc)


def _deadline(event: SimpleNamespace) -> datetime:
    # Reuse canonical publication span/end semantics, but never main.LOCAL_TZ:
    # it is mutable runtime configuration and initially UTC, not Hero's timezone.
    from main import _event_has_trusted_publish_span, _event_vk_publish_end_date, parse_iso_date

    start = parse_iso_date(str(event.date or "").split("..", 1)[0].strip())
    end = _event_vk_publish_end_date(event)
    if start is None or end is None or end < start:
        raise EventResolutionError("event_date_invalid")
    explicit_end = str(getattr(event, "end_date", "") or "").strip()
    if explicit_end and parse_iso_date(explicit_end.split("..", 1)[-1].strip()) is None:
        raise EventResolutionError("event_date_invalid")
    if ".." in str(event.date) and parse_iso_date(str(event.date).split("..", 1)[1].strip()) is None:
        raise EventResolutionError("event_date_invalid")
    if not _event_has_trusted_publish_span(event):
        # Exact time grammar used by _event_publication_start_deadline_ts.
        match = re.match(r"^\s*(\d{1,2})[:.](\d{2})", str(event.time or "").strip())
        if match:
            hour, minute = map(int, match.groups())
            if hour > 23 or minute > 59:
                raise EventResolutionError("event_time_invalid")
            return datetime.combine(start, time(hour, minute), KALININGRAD).astimezone(timezone.utc)
        # A guessed end date cannot extend a date-only event's eligibility.
        end = start
    return datetime.combine(end + timedelta(days=1), time.min, KALININGRAD).astimezone(timezone.utc)


def resolve_event_packet(database_path: str | Path, event_id: int, *, now: datetime,
                         route_evidence: RouteReadinessEvidence | None = None) -> dict:
    """Resolve one event snapshot. Missing evidence leaves links/media unresolved.

    The caller must re-resolve before activation/use; this is not a durable permit.
    Media remains empty until an authoritative object/pixel/rights verifier exists.
    """
    now = _aware(now)
    if type(event_id) is not int or event_id < 1:
        raise EventResolutionError("event_id_invalid")
    path = Path(database_path).resolve()
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN")
        row = connection.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise EventResolutionError("event_not_found")
        values = dict(row)
        if not {"identity_status", "merged_into_event_id", "lifecycle_status", "silent"}.issubset(values):
            raise EventResolutionError("canonical_identity_unavailable")
        if (values["identity_status"] != "canonical" or values["merged_into_event_id"] is not None
                or values["lifecycle_status"] != "active" or values["silent"] != 0):
            raise EventResolutionError("event_ineligible")
        # Match ORM JSON values consumed by event_public_revision, not serialized
        # SQLite strings (which would yield a different revision for the same row).
        import json
        for field in ("linked_event_ids", "photo_urls", "topics"):
            if isinstance(values.get(field), str):
                try:
                    values[field] = json.loads(values[field])
                except ValueError:
                    raise EventResolutionError("canonical_json_invalid") from None
        for field in ("silent", "time_is_default", "is_free", "pushkin_card"):
            if values.get(field) is not None:
                values[field] = bool(values[field])
        event = SimpleNamespace(**values)
        deadline = _deadline(event)
        if deadline <= now:
            raise EventResolutionError("event_expired")
        revision = event_public_revision(event)
    finally:
        connection.close()
    ref = f"event:{event_id}"
    facts = {}
    for field in ("title", "date", "time", "location_name", "location_address", "city"):
        value = values.get(field)
        if isinstance(value, str) and value.strip():
            facts[f"{ref}:{field}"] = {"dependency_ref": ref, "text": value.strip()}
    links = {}
    evidence = route_evidence
    if isinstance(evidence, RouteReadinessEvidence):
        verified, expires = _aware(evidence.verified_at), _aware(evidence.expires_at)
        if (type(evidence.event_id) is int and evidence.event_id == event_id
                and evidence.event_revision == revision and verified <= now < expires
                and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,240}", evidence.slug)
                and evidence.href == f"/sobytiya/{evidence.slug}/"):
            deadline = min(deadline, expires)
            links[f"{ref}:route"] = {"dependency_ref": ref, "href": evidence.href, "ready": True}
    return {"dependencies": {ref: {"revision": revision, "eligible": True,
            "eligible_until": deadline.isoformat()}}, "facts": facts, "links": links, "media": {}}
