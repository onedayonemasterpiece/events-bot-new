from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "backfill_catbox_posters_to_yandex.py"
    spec = importlib.util.spec_from_file_location("backfill_catbox_posters_to_yandex", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_event_in_date_window_matches_start_or_end_date():
    mod = _load_module()

    assert mod._event_in_date_window(
        SimpleNamespace(date="2026-04-13", end_date=None),
        from_date="2026-04-13",
    )
    assert mod._event_in_date_window(
        SimpleNamespace(date="2026-03-01", end_date="2026-04-13"),
        from_date="2026-04-13",
    )
    assert not mod._event_in_date_window(
        SimpleNamespace(date="2026-03-01", end_date="2026-04-12"),
        from_date="2026-04-13",
    )


def test_event_in_date_window_ignores_invalid_or_missing_filter():
    mod = _load_module()

    assert mod._event_in_date_window(
        SimpleNamespace(date="2026-01-01", end_date=None),
        from_date=None,
    )
    assert mod._event_in_date_window(
        SimpleNamespace(date="2026-01-01", end_date=None),
        from_date="invalid",
    )
