from __future__ import annotations

from pathlib import Path


SOURCE = Path("kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py")


def _source() -> str:
    return SOURCE.read_text(encoding="utf-8")


def test_guide_screen_prompt_keeps_non_excursion_events_out_of_scope() -> None:
    source = _source()
    start = source.index("async def screen_post")
    end = source.index("\n\nasync def _extract_announce_post_tier1", start)
    prompt_source = source[start:end]

    assert "do not treat a dated event as an excursion just because it is posted by a guide source" in prompt_source
    assert "volunteer cleanups" in prompt_source
    assert "subbotniks" in prompt_source
    assert "digest_eligible_default must be no or mixed" in prompt_source


def test_guide_extract_prompt_preserves_multi_date_public_schedule() -> None:
    source = _source()
    start = source.index("async def _extract_announce_post_tier1")
    end = source.index("\n\nasync def _extract_status_post", start)
    prompt_source = source[start:end]

    assert "one occurrence per dated line" in prompt_source
    assert "status=available" in prompt_source
    assert "availability_mode=scheduled_public" in prompt_source
    assert "digest_eligible=true" in prompt_source
    assert "use post.schedule_blocks as the complete schedule index" in prompt_source
    assert "tentative_or_free_date" in prompt_source
    assert "do not output an extra template/no-date occurrence" in prompt_source
    assert "title_normalized must be a short stable route identity core" in prompt_source
    assert "3️⃣ мая means 3 мая" in prompt_source
    assert "1️⃣3️⃣ мая means 13 мая" in prompt_source


def test_guide_block_and_enrich_prompts_do_not_downgrade_grounded_future_dates() -> None:
    source = _source()
    start = source.index("async def _extract_occurrence_block")
    end = source.index("\n\nasync def extract_post", start)
    prompt_source = source[start:end]

    assert "compact_post_for_block" in prompt_source
    assert "do not materialize volunteer cleanups" in prompt_source
    assert "do not downgrade a seed with concrete future date/time/booking/meeting facts" in prompt_source
    assert "set status=available, availability_mode=scheduled_public, digest_eligible=true" in prompt_source
    assert "tentative_or_free_date" in prompt_source
    assert "schedule_anchor_text" in prompt_source
    assert "post_context_excerpt" in prompt_source
    assert "shared facts that clearly apply to all schedule blocks" in prompt_source
    assert "do not borrow title/date/time/route facts from a different dated block" in prompt_source


def test_guide_block_splitter_recognizes_keycap_emoji_dates_as_schedule_anchors() -> None:
    source = _source()
    assert "KEYCAP_DIGIT_RE" in source
    assert "def _normalize_keycap_digit_dates" in source
    assert "def _looks_decorative_line" in source
    assert "schedule_text = _normalize_keycap_digit_dates(text)" in source
    assert "scan_line = _normalize_keycap_digit_dates(line)" in source
    assert 'block_payload["schedule_anchor_text"]' in source
    assert 'payload["schedule_blocks"] = _compact_occurrence_blocks(post.text, limit=8)' in source


def test_guide_multi_block_extraction_fails_open_per_block() -> None:
    source = _source()
    assert "async def _extract_occurrence_block_failopen" in source
    assert "[guide:block_extract:warning]" in source
    assert "async def _extract_announce_post_tier1_failopen_for_block_rescue" in source
    assert "ANNOUNCE_MULTI_FULL_TIMEOUT_SECONDS" in source
    assert "GUIDE_MONITORING_ANNOUNCE_MULTI_FULL_TIMEOUT_SEC" in source
    assert "timeout_retries=0" in source
    assert "[guide:announce_extract:warning]" in source
    assert "items = await _extract_announce_post_tier1_failopen_for_block_rescue" in source
    assert "rescued = await _extract_occurrence_block_failopen" in source
    assert "return cleaned\n\n    if extract_mode == \"status\":" not in source


def test_guide_enrichment_fails_open_after_tier1_seed() -> None:
    source = _source()
    assert "async def _extract_occurrence_semantics_failopen" in source
    assert "[guide:enrich:warning]" in source
    assert "return {}" in source
    assert "semantic_patch = await _extract_occurrence_semantics_failopen" in source


def test_guide_ocr_media_hashing_has_runtime_import() -> None:
    source = _source()
    imports = source[: source.index("INPUT_ROOT =")]
    assert "import hashlib" in imports
    assert '"sha256": hashlib.sha256(payload).hexdigest()' in source


def test_guide_ocr_logs_source_post_and_media_context() -> None:
    source = _source()
    assert "log_context=(" in source
    assert "[guide:ocr:ok]" in source
    assert "[guide:ocr:empty]" in source
    assert "source=@{username}" in source
    assert "media_message_id=" in source
    assert "image_index=" in source
