from __future__ import annotations

import json

from email_control.scheduler import (
    PostboxAlertSnapshot,
    PostboxAlertStateStore,
    decide_postbox_notification,
)


def _health(dlq: int) -> dict[str, int]:
    return {"dlq_visible_count": dlq, "dlq_inflight_count": 0}


def test_static_dlq_backlog_notifies_once_then_waits_for_long_reminder() -> None:
    alarms = [("alarm", "postbox_dlq_nonempty")]
    first = decide_postbox_notification(
        PostboxAlertSnapshot(),
        alarms,
        _health(162),
        now_epoch=1_000.0,
        static_reminder_seconds=21_600,
    )
    assert first.kind == "alert"
    assert first.dlq_delta == 162

    acknowledged = first.notified(1_000.0).snapshot
    unchanged = decide_postbox_notification(
        acknowledged,
        alarms,
        _health(162),
        now_epoch=2_000.0,
        static_reminder_seconds=21_600,
    )
    assert unchanged.kind == "none"
    assert unchanged.dlq_delta == 0

    reminder = decide_postbox_notification(
        unchanged.snapshot,
        alarms,
        _health(162),
        now_epoch=22_601.0,
        static_reminder_seconds=21_600,
    )
    assert reminder.kind == "alert"
    assert reminder.dlq_delta == 0


def test_dlq_growth_and_alarm_shape_change_notify_immediately() -> None:
    previous = PostboxAlertSnapshot(
        initialized=True,
        dlq_total=162,
        codes=("postbox_dlq_nonempty",),
        last_notified_at=10_000.0,
        observed_at=10_000.0,
    )
    growth = decide_postbox_notification(
        previous,
        [("alarm", "postbox_dlq_nonempty")],
        _health(165),
        now_epoch=10_060.0,
        static_reminder_seconds=21_600,
    )
    assert growth.kind == "alert"
    assert growth.dlq_delta == 3

    changed = decide_postbox_notification(
        growth.notified(10_060.0).snapshot,
        [
            ("alarm", "postbox_dlq_nonempty"),
            ("alarm", "postbox_correlation_missing"),
        ],
        _health(165),
        now_epoch=10_120.0,
        static_reminder_seconds=21_600,
    )
    assert changed.kind == "alert"
    assert changed.codes[-1] == "postbox_correlation_missing"


def test_cleared_alarm_emits_one_recovery() -> None:
    previous = PostboxAlertSnapshot(
        initialized=True,
        dlq_total=162,
        codes=("postbox_dlq_nonempty",),
        last_notified_at=1_000.0,
        observed_at=1_000.0,
    )
    recovered = decide_postbox_notification(
        previous,
        [],
        _health(0),
        now_epoch=2_000.0,
        static_reminder_seconds=21_600,
    )
    assert recovered.kind == "recovery"
    assert recovered.dlq_delta == -162

    settled = decide_postbox_notification(
        recovered.notified(2_000.0).snapshot,
        [],
        _health(0),
        now_epoch=2_300.0,
        static_reminder_seconds=21_600,
    )
    assert settled.kind == "none"


def test_state_store_roundtrip_is_pii_free_and_fail_closed(tmp_path) -> None:
    path = tmp_path / "monitor-state.json"
    store = PostboxAlertStateStore(str(path))
    snapshot = PostboxAlertSnapshot(
        initialized=True,
        dlq_total=162,
        codes=("postbox_dlq_nonempty",),
        last_notified_at=1_000.0,
        observed_at=1_100.0,
    )
    store.save(snapshot)
    assert store.load() == snapshot
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert set(payload) == {
        "schema",
        "initialized",
        "dlq_total",
        "codes",
        "last_notified_at",
        "observed_at",
    }

    path.write_text("not-json", encoding="utf-8")
    assert store.load() == PostboxAlertSnapshot()
