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
    assert "do not output an extra template/no-date occurrence" in prompt_source
    assert "title_normalized must be a short stable route identity core" in prompt_source


def test_guide_block_and_enrich_prompts_do_not_downgrade_grounded_future_dates() -> None:
    source = _source()
    start = source.index("async def _extract_occurrence_block")
    end = source.index("\n\nasync def extract_post", start)
    prompt_source = source[start:end]

    assert "compact_post_for_block" in prompt_source
    assert "do not materialize volunteer cleanups" in prompt_source
    assert "do not downgrade a seed with concrete future date/time/booking/meeting facts" in prompt_source
    assert "set status=available, availability_mode=scheduled_public, digest_eligible=true" in prompt_source
