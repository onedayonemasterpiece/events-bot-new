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
    dedup_source = Path("guide_excursions/dedup.py").read_text(encoding="utf-8")
    assert "GUIDE_EXCURSIONS_DEDUP_TOTAL_TIMEOUT_SEC" in dedup_source
    assert "total budget exhausted" in dedup_source
    writer_source = Path("guide_excursions/digest_writer.py").read_text(encoding="utf-8")
    assert "GUIDE_DIGEST_WRITER_TOTAL_TIMEOUT_SEC" in writer_source
    assert "total budget failed open" in writer_source


def test_guide_public_identity_resolution_is_timeout_bounded() -> None:
    source = Path("guide_excursions/public_identity.py").read_text(encoding="utf-8")

    assert "GUIDE_PUBLIC_IDENTITY_TIMEOUT_SEC" in source
    assert "asyncio.wait_for(" in source
    assert "create_telethon_runtime_client()" in source
    assert "client.get_entity(username)" in source
