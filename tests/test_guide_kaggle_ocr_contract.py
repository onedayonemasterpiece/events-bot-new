from __future__ import annotations

from pathlib import Path


def test_guide_kaggle_runtime_threads_ocr_chunks_into_llm_inputs() -> None:
    source = Path("kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py").read_text(
        encoding="utf-8"
    )

    assert "collect_post_ocr_chunks(" in source
    assert '"ocr_chunks": compact_ocr_chunks' in source
    assert '"ocr_chunks": ocr_chunks' in source
    assert "ocr_chunks=ocr_chunks" in source


def test_guide_kaggle_ocr_uses_google_ai_multimodal_parts() -> None:
    source = Path("kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py").read_text(
        encoding="utf-8"
    )

    assert '"inline_data"' in source
    assert 'consumer="guide_scout_ocr"' in source
    assert "prompt: Any" in source
