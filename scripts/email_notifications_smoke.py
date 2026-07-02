#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from email_notifications import EventFollower, EventSnapshot, PostboxConfig, PostboxSmtpSender, build_follow_outbox


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run event email notification rendering/Postbox smoke.")
    parser.add_argument("--to", default="operator@example.invalid")
    parser.add_argument("--event-id", type=int, default=5988)
    args = parser.parse_args()

    starts_at = datetime.now(timezone.utc) + timedelta(days=3)
    event = EventSnapshot(
        event_id=args.event_id,
        title="Тестовое событие KenigEvents",
        event_url=f"https://kenigevents.ru/sobytiya/test-{args.event_id}/",
        starts_at=starts_at,
        start_date=starts_at.date().isoformat(),
        display_time="19:00",
        venue_name="Тестовая площадка",
        city="Калининград",
        source_url="https://example.invalid/source",
    )
    follower = EventFollower(user_id="smoke-user", email=args.to, consent_at=datetime.now(timezone.utc))
    items, delivery_events = build_follow_outbox(follower, event)
    sender = PostboxSmtpSender(PostboxConfig.from_env())
    out: dict[str, object] = {"outbox": [], "delivery_events": [event.__dict__ for event in delivery_events]}
    for item in items:
        email = item.payload["email"]
        result = sender.send(to_email=item.recipient_email, subject=email["subject"], text=email["text"], html=email["html"])
        out["outbox"].append(
            {
                "kind": item.kind,
                "idempotency_key": item.idempotency_key,
                "next_run_at": item.next_run_at.isoformat(),
                "provider_message_id": result.provider_message_id,
                "dry_run": result.dry_run,
            }
        )
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
