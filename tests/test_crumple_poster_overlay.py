from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
POSTER_OVERLAY_PATH = ROOT / "kaggle" / "CrumpleVideo" / "poster_overlay.py"


def test_apply_poster_overlay_accepts_highlight_title():
    source = POSTER_OVERLAY_PATH.read_text(encoding="utf-8")

    assert "highlight_title: bool | None = None" in source
    assert "fill = title_fill if highlight_title and idx < title_line_count else default_fill" in source
