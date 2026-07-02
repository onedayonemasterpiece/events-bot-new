from __future__ import annotations

import hashlib
import html
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

EventEmailKind = Literal["calendar_confirmation", "event_reminder_24h", "event_rescheduled", "event_cancelled"]

CANCELLATION_DISCLAIMER = (
    "Полюбить Калининград Анонсы не является организатором события; "
    "информация об отмене получена из публичных источников/сообщений организатора."
)
TRACKED_CHANGE_FIELDS = (
    "start_date",
    "start_time",
    "display_time",
    "starts_at",
    "venue_name",
    "location_name",
    "location_address",
    "address",
    "city",
    "ticket_link",
    "ticket_href",
)
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: datetime | str | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


def normalize_email(email: str | None) -> str:
    return str(email or "").strip().lower()


def validate_email(email: str | None) -> bool:
    return bool(EMAIL_RE.match(normalize_email(email)))


def email_hash(email: str, *, salt: str = "") -> str:
    return hashlib.sha256(f"{salt}:{normalize_email(email)}".encode()).hexdigest()


@dataclass(frozen=True)
class EventSnapshot:
    event_id: int
    title: str
    event_url: str
    starts_at: datetime | str | None = None
    start_date: str | None = None
    start_time: str | None = None
    display_time: str | None = None
    venue_name: str | None = None
    location_name: str | None = None
    location_address: str | None = None
    address: str | None = None
    city: str | None = None
    ticket_link: str | None = None
    ticket_href: str | None = None
    source_url: str | None = None
    lifecycle_status: str = "active"
    lifecycle_note: str | None = None
    cancellation_note: str | None = None

    @property
    def start_dt(self) -> datetime | None:
        return parse_dt(self.starts_at)

    @property
    def place_label(self) -> str:
        return self.venue_name or self.location_name or self.city or "место уточняется"

    @property
    def address_label(self) -> str:
        return self.location_address or self.address or ""

    @property
    def ticket_url(self) -> str:
        return self.ticket_link or self.ticket_href or ""

    def payload(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "title": self.title,
            "event_url": self.event_url,
            "starts_at": iso(self.start_dt) if self.start_dt else self.starts_at,
            "start_date": self.start_date,
            "start_time": self.start_time,
            "display_time": self.display_time,
            "venue_name": self.venue_name,
            "location_name": self.location_name,
            "location_address": self.location_address,
            "address": self.address,
            "city": self.city,
            "ticket_link": self.ticket_url or None,
            "source_url": self.source_url,
            "lifecycle_status": self.lifecycle_status,
            "lifecycle_note": self.lifecycle_note,
            "cancellation_note": self.cancellation_note,
        }


@dataclass(frozen=True)
class EventFollower:
    user_id: str
    email: str
    consent_at: datetime | str
    unsubscribed_at: datetime | str | None = None

    @property
    def active(self) -> bool:
        return validate_email(self.email) and bool(self.consent_at) and not self.unsubscribed_at


@dataclass(frozen=True)
class EmailDeliveryEvent:
    kind: str
    event_id: int | None
    user_id: str | None
    status: str
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class EmailOutboxItem:
    kind: EventEmailKind
    event_id: int
    user_id: str
    recipient_email: str
    payload: dict[str, Any]
    next_run_at: datetime
    idempotency_key: str
    recipient_email_hash: str
    status: str = "pending"
    attempts: int = 0


def _date_line(event: EventSnapshot) -> str:
    return " · ".join(p for p in [event.start_date or "дата уточняется", event.display_time or event.start_time or ""] if p)


def _common_lines(event: EventSnapshot) -> list[str]:
    lines = [f"Событие: {event.title}", f"Когда: {_date_line(event)}", f"Где: {event.place_label}"]
    if event.address_label:
        lines.append(f"Адрес: {event.address_label}")
    lines.append(f"Страница события: {event.event_url}")
    if event.source_url:
        lines.append(f"Источник/организатор: {event.source_url}")
    if event.ticket_url:
        lines.append(f"Билеты/регистрация: {event.ticket_url}")
    return lines


def render_email(kind: EventEmailKind, event: EventSnapshot, *, changes: dict[str, dict[str, Any]] | None = None) -> dict[str, str]:
    common = _common_lines(event)
    changes = changes or {}
    if kind == "calendar_confirmation":
        subject, intro = f"Событие добавлено в календарь: {event.title}", "Вы добавили событие в календарь и согласились получать уведомления по нему."
    elif kind == "event_reminder_24h":
        subject, intro = f"Напоминание: завтра {event.title}", "Напоминаем о событии примерно за 24 часа до начала."
    elif kind == "event_rescheduled":
        subject, intro = f"Изменения в событии: {event.title}", "У события, которое вы добавили в календарь, изменились важные детали."
        if changes:
            common.append("Изменения:")
            common.extend(f"{field}: {diff.get('before') or '—'} → {diff.get('after') or '—'}" for field, diff in changes.items())
    elif kind == "event_cancelled":
        subject, intro = f"Отмена события: {event.title}", "Событие, которое вы добавили в календарь, отменено."
        note = event.cancellation_note or event.lifecycle_note
        if note:
            common.append(f"Комментарий источника: {note}")
        common.append(CANCELLATION_DISCLAIMER)
    else:
        raise ValueError(f"unsupported email kind: {kind}")
    footer = "Управлять уведомлениями можно на странице события. Это транзакционное письмо по событию, которое вы добавили в календарь."
    text = "\n\n".join([intro, *common, footer])
    html_body = f"<p>{html.escape(intro)}</p><ul>{''.join(f'<li>{html.escape(line)}</li>' for line in common)}</ul><p>{html.escape(footer)}</p>"
    return {"subject": subject, "text": text, "html": html_body}


def _version_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()[:20]


def _outbox_item(kind: EventEmailKind, follower: EventFollower, event: EventSnapshot, *, now: datetime, next_run_at: datetime | None = None, changes: dict[str, dict[str, Any]] | None = None, version: str | None = None) -> EmailOutboxItem:
    payload = {"kind": kind, "event": event.payload(), "email": render_email(kind, event, changes=changes), "changes": changes or {}, "queued_at": iso(now)}
    ver = version or _version_hash(payload)
    return EmailOutboxItem(kind, event.event_id, follower.user_id, normalize_email(follower.email), payload, next_run_at or now, f"{kind}:{follower.user_id}:{event.event_id}:{ver}", email_hash(follower.email))


def build_follow_outbox(follower: EventFollower, event: EventSnapshot, *, now: datetime | None = None) -> tuple[list[EmailOutboxItem], list[EmailDeliveryEvent]]:
    now = now or utc_now()
    if not follower.active:
        return [], [EmailDeliveryEvent("event_follow", event.event_id, follower.user_id, "skipped", "missing_consent_or_email")]
    items = [_outbox_item("calendar_confirmation", follower, event, now=now, version="calendar-follow-v1")]
    delivery_events: list[EmailDeliveryEvent] = []
    if not event.start_dt:
        delivery_events.append(EmailDeliveryEvent("event_reminder_24h", event.event_id, follower.user_id, "skipped", "missing_start_time"))
    elif event.start_dt - now < timedelta(hours=24):
        delivery_events.append(EmailDeliveryEvent("event_reminder_24h", event.event_id, follower.user_id, "skipped", "starts_in_less_than_24h"))
    else:
        items.append(_outbox_item("event_reminder_24h", follower, event, now=now, next_run_at=event.start_dt - timedelta(hours=24), version=f"reminder-24h:{iso(event.start_dt)}"))
    return items, delivery_events


def _field_value(event: EventSnapshot, field: str) -> str:
    if field == "location_name":
        return event.location_name or event.venue_name or ""
    if field == "location_address":
        return event.location_address or event.address or ""
    if field == "ticket_link":
        return event.ticket_url
    value = getattr(event, field, "")
    return iso(value) if isinstance(value, datetime) else str(value or "").strip()


def detect_event_changes(before: EventSnapshot, after: EventSnapshot) -> dict[str, dict[str, str]]:
    changes: dict[str, dict[str, str]] = {}
    for field in TRACKED_CHANGE_FIELDS:
        old, new = _field_value(before, field), _field_value(after, field)
        if old != new:
            canonical = {"venue_name": "location_name", "address": "location_address", "ticket_href": "ticket_link"}.get(field, field)
            changes[canonical] = {"before": old, "after": new}
    return changes


def build_lifecycle_outbox(followers: list[EventFollower], before: EventSnapshot, after: EventSnapshot, *, now: datetime | None = None) -> tuple[list[EmailOutboxItem], list[EmailDeliveryEvent]]:
    now = now or utc_now()
    active = [f for f in followers if f.active]
    skipped = [EmailDeliveryEvent("event_lifecycle", after.event_id, f.user_id, "skipped", "missing_consent_or_email") for f in followers if not f.active]
    if after.lifecycle_status == "cancelled" and before.lifecycle_status != "cancelled":
        version = f"cancelled:{_version_hash(after.payload())}"
        return [_outbox_item("event_cancelled", f, after, now=now, version=version) for f in active], skipped
    changes = detect_event_changes(before, after)
    if changes:
        version = f"rescheduled:{_version_hash(changes)}"
        return [_outbox_item("event_rescheduled", f, after, now=now, changes=changes, version=version) for f in active], skipped
    return [], skipped
