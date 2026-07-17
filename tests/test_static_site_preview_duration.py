from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
SPEC = importlib.util.spec_from_file_location("export_production_preview_data", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


def test_extracts_only_explicitly_labeled_event_duration() -> None:
    assert EXPORTER.explicit_event_duration_minutes("Продолжительность: 1 час 10 мин. Билеты") == 70
    assert EXPORTER.explicit_event_duration_minutes("Продолжительность: 45 минут") == 45
    assert EXPORTER.explicit_event_duration_minutes("Два часа живого звука") is None


def test_derives_transport_safe_end_time_from_explicit_duration() -> None:
    assert EXPORTER.event_end_from_duration("2026-07-12", "17:00", 70) == ("2026-07-12", "18:10")
    assert EXPORTER.event_end_from_duration("2026-07-12", "23:30", 90) == ("2026-07-13", "01:00")
