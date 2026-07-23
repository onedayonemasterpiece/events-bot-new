from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
SPEC = importlib.util.spec_from_file_location("export_production_preview_data", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)
PREVIEW_EVENTS = SCRIPT.parents[1] / "src" / "data" / "preview-events.json"
ESTIMATES = SCRIPT.parents[1] / "src" / "data" / "event-duration-estimates.json"


def test_extracts_only_explicitly_labeled_event_duration() -> None:
    assert EXPORTER.explicit_event_duration_minutes("Продолжительность: 1 час 10 мин. Билеты") == 70
    assert EXPORTER.explicit_event_duration_minutes("Продолжительность: 45 минут") == 45
    assert EXPORTER.explicit_event_duration_minutes("Два часа живого звука") is None


def test_derives_transport_safe_end_time_from_explicit_duration() -> None:
    assert EXPORTER.event_end_from_duration("2026-07-12", "17:00", 70) == ("2026-07-12", "18:10")
    assert EXPORTER.event_end_from_duration("2026-07-12", "23:30", 90) == ("2026-07-13", "01:00")


def test_event_3103_retains_explicit_duration_regression() -> None:
    preview = json.loads(PREVIEW_EVENTS.read_text(encoding="utf-8"))
    event = next(item for item in preview["events"] if item["id"] == 3103)
    estimates = json.loads(ESTIMATES.read_text(encoding="utf-8"))

    assert "Продолжительность спектакля – 1 час 40 минут" in event["description_html"]
    assert all(item["event_id"] != 3103 for item in estimates["estimates"])
    assert EXPORTER.event_end_from_duration(event["start_date"], event["start_time"], 100) == (
        "2026-08-15",
        "19:40",
    )


def test_event_6529_estimate_is_preview_only_and_not_a_canonical_end() -> None:
    preview = json.loads(PREVIEW_EVENTS.read_text(encoding="utf-8"))
    event = next(item for item in preview["events"] if item["id"] == 6529)
    estimates = json.loads(ESTIMATES.read_text(encoding="utf-8"))
    estimate = next(item for item in estimates["estimates"] if item["event_id"] == 6529)

    assert event["time_range_end"] is None
    assert EXPORTER.explicit_event_duration_minutes(event["description_html"]) is None
    assert estimates["scope"] == "preview_only"
    assert estimate["source_status"] == "llm_estimated"
    assert estimate["canonical_end"] is False
    assert estimate["model"]["id"] == "gemini-3.1-pro-low"
    assert estimate["provenance"].endswith("/gemini-duration-6529/response.json")
    assert estimate["estimated_at"] == "2026-07-23T06:35:15Z"
    assert estimate["most_likely_minutes"] == 120
    assert (estimate["plausible_min_minutes"], estimate["plausible_max_minutes"]) == (90, 180)
    assert estimate["confidence"] == "medium"
    assert estimate["conservative_routing_minutes"] == 150
