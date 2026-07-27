from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "kaggle" / "StaticSiteBuilder" / "service_share_card.py"


def load_module():
    spec = importlib.util.spec_from_file_location("static_service_share_card_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def events() -> list[dict]:
    return [
        {
            "id": event_id,
            "title": f"Необычное событие {event_id}",
            "start_date": "2026-07-28",
            "start_time": f"{10 + event_id:02d}:00",
            "city": "Калининград" if event_id % 2 else "Зеленоградск",
            "venue_name": f"Площадка {event_id}",
            "lifecycle_status": "active",
            "source_views_count": event_id * 100,
            "source_likes_count": event_id,
        }
        for event_id in range(1, 11)
    ]


def test_selector_is_stable_per_day_and_rotates_stable_bucket():
    module = load_module()
    first = module.select_daily_events(events(), local_date="2026-07-27")
    repeat = module.select_daily_events(list(reversed(events())), local_date="2026-07-27")
    tomorrow = module.select_daily_events(events(), local_date="2026-07-28")

    assert first["selection_hash"] == repeat["selection_hash"]
    assert first["event_ids"] == repeat["event_ids"]
    assert first["event_ids"][:3] == [10, 9, 8]
    assert first["event_ids"][3:] != tomorrow["event_ids"][3:]
    assert first["events_floor"] <= first["eligible_event_count"]


def test_daily_renderer_writes_immutable_assets_and_atomic_current_pointer(tmp_path: Path):
    module = load_module()
    kwargs = {
        "events": events(),
        "public_root": tmp_path,
        "build_id": "production-test",
        "measured_at": "2026-07-27T00:00:00+02:00",
        "source_snapshot_id": "snapshot-test",
        "source_snapshot_hash": "a" * 64,
    }
    first = module.build_daily_service_share(**kwargs)
    second = module.build_daily_service_share(**kwargs)

    assert first == second
    assert first["assets"]["png"]["width"] == 1080
    assert first["assets"]["png"]["height"] == 1350
    assert first["manifest_payload_hash"] == second["manifest_payload_hash"]
    current_path = tmp_path / "service-share" / "current" / "manifest.json"
    current = json.loads(current_path.read_text(encoding="utf-8"))
    version_dir = tmp_path / "service-share" / "versions" / first["asset_version"]
    assert current == first
    assert json.loads((version_dir / "manifest.json").read_text(encoding="utf-8")) == first
    for asset in first["assets"].values():
        path = version_dir / asset["filename"]
        assert path.is_file()
        assert Image.open(path).size == (1080, 1350)
    measured = datetime.fromisoformat(first["measured_at"].replace("Z", "+00:00"))
    fresh_until = datetime.fromisoformat(first["fresh_until"].replace("Z", "+00:00"))
    assert (fresh_until - measured).total_seconds() == 86400
