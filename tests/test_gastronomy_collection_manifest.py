from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "site" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import gastronomy_collection_manifest as subject


def event(event_id: int, start_date: str) -> dict:
    return {"id": event_id, "start_date": start_date, "title": f"prose must not matter {event_id}"}


def decision(event_id: int, family: str) -> dict:
    return {"event_id": event_id, "family_id": family, "role": "core"}


def build(events, decisions, **overrides):
    return subject.build_manifest(
        events,
        decisions,
        current_date="2026-08-03",
        catalog_hash="catalog-a",
        audit_complete=True,
        generated_at="2026-08-03T10:00:00Z",
        **overrides,
    )


def test_three_or_more_future_families_are_active():
    events = [event(i, f"2026-08-{10 + i:02d}") for i in range(1, 4)]
    manifest = build(events, [decision(i, f"family-{i}") for i in range(1, 4)])
    assert manifest["lifecycle"] == "active"
    assert manifest["publication_status"] == "ready"
    assert manifest["provider_calls"] == 0


def test_one_or_two_future_families_are_low_supply():
    events = [event(1, "2026-08-10"), event(2, "2026-09-10")]
    assert build(events, [decision(1, "a"), decision(2, "b")])["lifecycle"] == "low_supply"


def test_zero_future_with_recent_and_without_recent_are_distinct():
    recent = build([event(1, "2026-07-01")], [decision(1, "a")])
    dormant = build([event(1, "2025-01-01")], [decision(1, "a")])
    assert recent["lifecycle"] == "recent_empty"
    assert dormant["lifecycle"] == "dormant"
    assert dormant["publication_status"] == "shadow"


def test_occurrence_family_dedupe_prefers_future_then_earliest_future():
    events = [event(1, "2026-07-01"), event(2, "2026-08-20"), event(3, "2026-08-10")]
    manifest = build(events, [decision(1, "same"), decision(2, "same"), decision(3, "same")])
    assert [item["event_id"] for item in manifest["accepted"]] == [3]


def test_incomplete_or_provider_backed_run_fails_closed_and_keeps_last_good():
    events = [event(1, "2026-08-10")]
    last_good = build(events, [decision(1, "a")])
    failed = subject.build_manifest(
        events,
        [],
        current_date="2026-08-03",
        catalog_hash="catalog-a",
        audit_complete=False,
        generated_at="2026-08-03T11:00:00Z",
        previous_manifest=last_good,
        provider_calls=1,
    )
    assert failed["lifecycle"] == "last_good"
    assert failed["publication_status"] == "blocked"
    assert failed["accepted"] == []
    assert failed["last_good"]["status"] == "available"


def test_last_good_with_wrong_catalog_is_rejected():
    events = [event(1, "2026-08-10")]
    last_good = build(events, [decision(1, "a")])
    failed = subject.build_manifest(
        events,
        [],
        current_date="2026-08-03",
        catalog_hash="catalog-b",
        audit_complete=False,
        previous_manifest=last_good,
    )
    assert failed["lifecycle"] == "blocked"
    assert failed["last_good"]["status"] == "absent"
