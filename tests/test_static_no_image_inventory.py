from __future__ import annotations

import importlib.util
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "site" / "scripts" / "audit-no-image-inventory.py"
SPEC = importlib.util.spec_from_file_location("audit_no_image_inventory", SCRIPT)
assert SPEC and SPEC.loader
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


def test_inventory_classifies_all_read_only_failure_reasons(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            """
            create table event (id integer primary key, updated_at text);
            create table eventposter (
              id integer primary key, event_id integer, review_status text, supabase_url text
            );
            insert into event values
              (1, '2026-07-18T08:00:00Z'), (2, '2026-07-18T08:00:00Z'),
              (3, '2026-07-18T08:00:00Z'), (4, '2026-07-18T08:00:00Z');
            insert into eventposter(event_id, review_status, supabase_url) values
              (2, 'pending_review', null),
              (3, 'approved', 'https://example.org/source.jpg'),
              (4, 'approved', 'https://static.kenigevents.ru/p/ok.webp');
            """
        )
    preview = {
        "build": {"generated_at": "2026-07-18T09:00:00Z"},
        "events": [
            {"id": 1, "title": "A", "event_type": "Концерт", "image_url": None, "image_assets": []},
            {"id": 2, "title": "B", "event_type": "концерт", "image_url": None, "image_assets": []},
            {"id": 3, "title": "C", "event_type": "Встреча", "image_url": None, "image_assets": []},
            {"id": 4, "title": "D", "event_type": "Встреча", "image_url": None, "image_assets": []},
            {"id": 5, "title": "Has media", "event_type": "лекция", "image_url": "https://static.kenigevents.ru/p/5.webp", "image_assets": []},
        ],
    }
    with AUDIT.open_read_only(db_path) as connection:
        report = AUDIT.build_inventory(
            connection,
            preview,
            db_path=db_path,
            as_of=datetime(2026, 7, 18, 12, tzinfo=timezone.utc),
            asset_base_url="https://static.kenigevents.ru",
        )

    assert report["summary"]["by_reason"] == {
        "no_ledger": 1,
        "no_approved": 1,
        "approved_non_cdn": 1,
        "projection_mismatch": 1,
    }
    assert report["summary"]["by_event_type"] == {"встреча": 2, "концерт": 2}
    assert report["summary"]["past_or_inactive_projection_count"] == 0
    assert report["snapshot"]["preview_generated_at"] == "2026-07-18T09:00:00Z"
    assert report["snapshot"]["age_hours"] == 3.0
    assert [event["reason"] for event in report["events"]] == list(AUDIT.REASONS)


def test_cli_does_not_mutate_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute("create table event (id integer primary key, updated_at text)")
    before = db_path.read_bytes()
    preview_path = tmp_path / "preview.json"
    preview_path.write_text(json.dumps({"build": {}, "events": []}), encoding="utf-8")
    with AUDIT.open_read_only(db_path) as connection:
        AUDIT.build_inventory(
            connection,
            json.loads(preview_path.read_text()),
            db_path=db_path,
            as_of=datetime(2026, 7, 18, tzinfo=timezone.utc),
            asset_base_url="https://static.kenigevents.ru",
        )
    assert db_path.read_bytes() == before
