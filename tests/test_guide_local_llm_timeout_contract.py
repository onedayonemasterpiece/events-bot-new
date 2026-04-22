from __future__ import annotations

from pathlib import Path


def test_guide_digest_preview_gemma_stages_are_timeout_bounded() -> None:
    expected = {
        "guide_excursions/enrich.py": "GUIDE_OCCURRENCE_ENRICH_LLM_TIMEOUT_SEC",
        "guide_excursions/dedup.py": "GUIDE_EXCURSIONS_DEDUP_LLM_TIMEOUT_SEC",
        "guide_excursions/digest_writer.py": "GUIDE_DIGEST_WRITER_LLM_TIMEOUT_SEC",
    }
    for path, env_name in expected.items():
        source = Path(path).read_text(encoding="utf-8")
        assert env_name in source
        assert "asyncio.wait_for(" in source
        assert "generate_content_async(" in source
