from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess


SCRIPT = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
SPEC = importlib.util.spec_from_file_location("export_production_preview_data", SCRIPT)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)
TRANSPORT_HELPER = (
    Path(__file__).resolve().parents[1] / "site" / "src" / "lib" / "desktopEventTransport.ts"
)


def test_extracts_only_explicitly_labeled_event_duration() -> None:
    assert EXPORTER.explicit_event_duration_minutes("Продолжительность: 1 час 10 мин. Билеты") == 70
    assert EXPORTER.explicit_event_duration_minutes("Продолжительность: 45 минут") == 45
    assert EXPORTER.explicit_event_duration_minutes("Два часа живого звука") is None


def test_derives_transport_safe_end_time_from_explicit_duration() -> None:
    assert EXPORTER.event_end_from_duration("2026-07-12", "17:00", 70) == ("2026-07-12", "18:10")
    assert EXPORTER.event_end_from_duration("2026-07-12", "23:30", 90) == ("2026-07-13", "01:00")


def test_exports_only_valid_persisted_duration_forecast() -> None:
    assert EXPORTER.forecast_event_duration_minutes(95) == 95
    assert EXPORTER.forecast_event_duration_minutes("120") == 120
    assert EXPORTER.forecast_event_duration_minutes(None) is None
    assert EXPORTER.forecast_event_duration_minutes(0) is None
    assert EXPORTER.forecast_event_duration_minutes(721) is None


def test_transport_end_uses_explicit_then_forecast_then_safe_null_fallback() -> None:
    script = f"""
import {{ desktopEventWithExplicitEnd }} from {json.dumps(TRANSPORT_HELPER.as_uri())};
const base = {{ start_time: '17:00', time_range_end: null, description_html: '' }};
const explicit = desktopEventWithExplicitEnd({{
  ...base,
  description_html: '<p>Продолжительность: 45 минут</p>',
  duration_forecast_minutes: 120,
}});
const forecast = desktopEventWithExplicitEnd({{
  ...base,
  duration_forecast_minutes: 120,
}});
const fallback = desktopEventWithExplicitEnd({{ ...base }});
const extractedEnd = desktopEventWithExplicitEnd({{
  ...base,
  time_range_end: '18:20',
  duration_forecast_minutes: 120,
}});
process.stdout.write(JSON.stringify({{
  explicit: explicit.time_range_end,
  forecast: forecast.time_range_end,
  fallback: fallback.time_range_end,
  extractedEnd: extractedEnd.time_range_end,
}}));
"""
    completed = subprocess.run(
        ["node", "--experimental-strip-types", "--input-type=module", "-e", script],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(completed.stdout) == {
        "explicit": "17:45",
        "forecast": "19:00",
        "fallback": None,
        "extractedEnd": "18:20",
    }
