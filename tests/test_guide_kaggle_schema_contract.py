from __future__ import annotations

from pathlib import Path


def test_guide_kaggle_single_occurrence_schema_avoids_anyof() -> None:
    source = Path("kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py").read_text(
        encoding="utf-8"
    )
    start = source.index("def _single_occurrence_wrapper_schema")
    end = source.index("\n\nasync def ask_gemma", start)
    helper_source = source[start:end]

    assert "anyOf" not in helper_source
    assert '"type": "null"' not in helper_source
