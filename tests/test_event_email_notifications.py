from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from email_notifications import (
    CANCELLATION_DISCLAIMER,
    EmailRateLimitConfig,
    EmailRateLimiter,
    EmailStatsEvent,
    EventFollower,
    EventSnapshot,
    PostboxConfig,
    PostboxSmtpSender,
    RateLimitCounters,
    RecordingYDBStatsSink,
    YDBStatsConfig,
    YDBStatsSink,
    build_follow_outbox,
    build_lifecycle_outbox,
    detect_event_changes,
    render_email,
)


def future_event(hours: int = 72) -> EventSnapshot:
    starts_at = datetime(2026, 7, 10, 16, 0, tzinfo=timezone.utc) + timedelta(hours=hours - 72)
    return EventSnapshot(
        event_id=5988,
        title="Александр Буйнов: Лучшие песни",
        event_url="https://kenigevents.ru/sobytiya/buynov/",
        starts_at=starts_at,
        start_date="2026-07-14",
        display_time="19:00",
        venue_name="Янтарь-холл",
        location_address="Светлогорск",
        city="Светлогорск",
        source_url="https://vk.com/wall-100137391_165125",
        ticket_link="https://example.com/tickets",
    )


def test_follow_creates_confirmation_and_24h_reminder() -> None:
    now = datetime(2026, 7, 7, 16, 0, tzinfo=timezone.utc)
    follower = EventFollower("user-1", "USER@example.COM", now)
    items, events = build_follow_outbox(follower, future_event(), now=now)
    assert [item.kind for item in items] == ["calendar_confirmation", "event_reminder_24h"]
    assert items[0].recipient_email == "user@example.com"
    assert items[0].idempotency_key == "calendar_confirmation:user-1:5988:calendar-follow-v1"
    assert items[1].next_run_at == future_event().start_dt - timedelta(hours=24)
    assert events == []


def test_reminder_skipped_when_event_starts_in_less_than_24h() -> None:
    now = datetime(2026, 7, 10, 1, 0, tzinfo=timezone.utc)
    follower = EventFollower("user-1", "user@example.com", now)
    items, events = build_follow_outbox(follower, future_event(hours=10), now=now)
    assert [item.kind for item in items] == ["calendar_confirmation"]
    assert events[0].kind == "event_reminder_24h"
    assert events[0].status == "skipped"
    assert events[0].reason == "starts_in_less_than_24h"


def test_missing_email_blocks_notifications() -> None:
    items, events = build_follow_outbox(EventFollower("user-1", "", datetime.now(timezone.utc)), future_event())
    assert items == []
    assert events[0].reason == "missing_consent_or_email"


def test_cancellation_email_has_disclaimer_and_idempotency() -> None:
    before = future_event()
    after = EventSnapshot(**{**before.__dict__, "lifecycle_status": "cancelled", "cancellation_note": "Организатор сообщил об отмене."})
    follower = EventFollower("user-1", "user@example.com", datetime(2026, 7, 1, tzinfo=timezone.utc))
    items, _ = build_lifecycle_outbox([follower], before, after, now=datetime(2026, 7, 2, tzinfo=timezone.utc))
    assert len(items) == 1
    assert items[0].kind == "event_cancelled"
    assert items[0].idempotency_key.startswith("event_cancelled:user-1:5988:cancelled:")
    assert CANCELLATION_DISCLAIMER in items[0].payload["email"]["text"]
    assert "Организатор сообщил об отмене" in items[0].payload["email"]["text"]


def test_reschedule_diff_contains_changed_fields_only() -> None:
    before = future_event()
    after = EventSnapshot(**{**before.__dict__, "display_time": "20:00", "venue_name": "Новая площадка"})
    changes = detect_event_changes(before, after)
    assert set(changes) == {"display_time", "location_name"}
    follower = EventFollower("user-1", "user@example.com", datetime(2026, 7, 1, tzinfo=timezone.utc))
    items, _ = build_lifecycle_outbox([follower], before, after, now=datetime(2026, 7, 2, tzinfo=timezone.utc))
    assert items[0].kind == "event_rescheduled"
    assert "display_time" in items[0].payload["email"]["text"]
    assert "city" not in items[0].payload["changes"]


def test_postbox_dry_run_does_not_require_credentials_or_network() -> None:
    sender = PostboxSmtpSender(PostboxConfig(enabled=False, dry_run=True, from_email="info@kenigevents.ru"))
    result = sender.send(to_email="user@example.com", subject="Test", text="Body", html="<p>Body</p>")
    assert result.dry_run is True
    assert result.provider_message_id.startswith("dry-run:")


def test_rate_limiter_defer_reasons() -> None:
    limiter = EmailRateLimiter(EmailRateLimitConfig(max_per_hour=1, cancel_batch_per_minute=1))
    assert limiter.check(kind="calendar_confirmation", counters=RateLimitCounters()).allowed is True
    assert limiter.check(kind="calendar_confirmation", counters=RateLimitCounters(sender_hour=1)).reason == "sender_hour_limit"
    assert limiter.check(kind="event_cancelled", counters=RateLimitCounters(cancel_minute=1)).reason == "cancel_batch_minute_limit"


def test_ydb_stats_projection_and_disabled_sink_is_visible() -> None:
    sink = RecordingYDBStatsSink()
    event = EmailStatsEvent(event_type="delivery", kind="calendar_confirmation", status="queued", event_id=5988, recipient_email_hash="abc")
    sink.record(event)
    assert sink.events[0].as_dict()["event_id"] == 5988
    with pytest.raises(RuntimeError, match="YDB stats sink is disabled"):
        YDBStatsSink(YDBStatsConfig(endpoint="", database="", table_path="", enabled=False)).record(event)


def test_render_email_html_escapes_values() -> None:
    event = EventSnapshot(event_id=1, title="<script>", event_url="https://kenigevents.ru/e/1")
    rendered = render_email("calendar_confirmation", event)
    assert "<script>" not in rendered["html"]
    assert "&lt;script&gt;" in rendered["html"]
