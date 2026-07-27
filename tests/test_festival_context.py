from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import select

import main
import festival_queue
from festival_queue import detect_festival_context
from models import Festival, FestivalQueueItem
from source_parsing.festival_parser import run_festival_parser_kernel
from smart_event_update import EventCandidate, _should_skip_festival_post_candidate


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_single_event_in_festival_source_is_not_queued_as_whole_festival() -> None:
    decision = detect_festival_context(
        parsed_events=[
            {
                "title": "Мастер-класс «Индустриальный пейзаж»",
                "date": "2026-05-02",
                "time": "12:00",
                "location_name": "Историко-художественный музей",
                "event_type": "мастер-класс",
                "festival_context": "festival_post",
            }
        ],
        festival_payload={"name": "Калининград художественный", "festival_context": "festival_post"},
        source_text=(
            "Продолжаем цикл художественных мастер-классов «Калининград художественный: "
            "80 лет в красках и образах» и приглашаем вас создать индустриальный пейзаж. "
            "2 мая | 12:00. Стоимость билета: 500 рублей. "
            "- Техника: скетч смешанными материалами.\n"
            "- Все материалы предоставляются.\n"
            "- Занятие подходит для любого уровня подготовки.\n"
            "Программа проходит в рамках празднования 80-летия Калининградской области."
        ),
        source_is_festival=True,
        source_series="Калининград художественный",
    )

    assert decision.context == "event_with_festival"
    assert decision.festival == "Калининград художественный"


def test_source_festival_program_without_single_event_still_goes_to_queue() -> None:
    decision = detect_festival_context(
        parsed_events=[],
        festival_payload={"name": "Калининград художественный", "festival_context": "festival_post"},
        source_text=(
            "Полная афиша цикла «Калининград художественный»: программа выставок, "
            "мастер-классов и встреч на май."
        ),
        source_is_festival=True,
        source_series="Калининград художественный",
    )

    assert decision.context == "festival_post"


def test_parser_occurrence_is_not_dropped_as_whole_festival_post() -> None:
    common = {
        "source_url": "https://filarmonia39.ru/afisha/ave-mariya/",
        "source_text": "«Аве Мария». Закрытие фестиваля «Бахослужение».",
        "title": "Концерт «Аве Мария»",
        "date": "2026-07-28",
        "time": "19:00",
        "festival": "Бахослужение",
        "festival_context": "festival_post",
        "location_name": "Филармония им. Светланова",
    }

    parser_candidate = EventCandidate(
        source_type="parser:philharmonia",
        organizer_names=["Калининградская областная филармония"],
        **common,
    )
    social_candidate = EventCandidate(source_type="tg", **common)

    assert not _should_skip_festival_post_candidate(parser_candidate)
    assert parser_candidate.organizer_names == [
        "Калининградская областная филармония"
    ]
    assert _should_skip_festival_post_candidate(social_candidate)


@pytest.mark.asyncio
async def test_process_festival_queue_uses_safety_cap(tmp_path: Path, monkeypatch) -> None:
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setenv("FESTIVAL_QUEUE_MAX_ITEMS_PER_RUN", "2")

        now = datetime.now(timezone.utc)
        async with db.get_session() as session:
            for idx in range(4):
                name = f"Festival {idx}"
                session.add(Festival(name=name, telegraph_url=f"https://telegra.ph/fest-{idx}"))
                session.add(
                    FestivalQueueItem(
                        status="pending",
                        source_kind="vk",
                        source_url=f"https://vk.com/wall-1_{idx}",
                        source_text=f"Program text {idx}",
                        festival_context="event_with_festival",
                        festival_name=name,
                        next_run_at=now,
                    )
                )
            await session.commit()

        async def fake_process_vk_item(db, item):  # noqa: ANN001
            return {"festival_name": item.festival_name, "mode": "test"}

        async def fake_sync_festival_page(db, festival_name):  # noqa: ANN001
            return None

        async def fake_sync_festivals_index_page(db):  # noqa: ANN001
            await main.set_setting_value(db, "fest_index_url", "https://telegra.ph/festivals")

        async def fake_try_set_fest_cover_from_program(db, fest_obj):  # noqa: ANN001
            return None

        monkeypatch.setattr(festival_queue, "_process_vk_item", fake_process_vk_item)
        monkeypatch.setattr(main, "sync_festival_page", fake_sync_festival_page)
        monkeypatch.setattr(main, "sync_festivals_index_page", fake_sync_festivals_index_page)
        monkeypatch.setattr(main, "try_set_fest_cover_from_program", fake_try_set_fest_cover_from_program)

        report = await festival_queue.process_festival_queue(db, trigger="test")

        assert report.processed == 2
        assert report.success == 2
        async with db.get_session() as session:
            items = (await session.execute(select(FestivalQueueItem))).scalars().all()
        statuses = [item.status for item in items]
        assert statuses.count("done") == 2
        assert statuses.count("pending") == 2
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_festival_parser_runner_passes_guardrails(monkeypatch) -> None:
    captured: dict = {}

    async def fake_run_kaggle_kernel(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return "complete", ["uds.json"], 1.0

    import source_parsing.kaggle_runner as kaggle_runner

    monkeypatch.setattr(kaggle_runner, "run_kaggle_kernel", fake_run_kaggle_kernel)
    monkeypatch.setenv("FESTIVAL_PARSER_NO_LLM", "1")
    monkeypatch.setenv("FESTIVAL_PARSER_MAX_LLM_CALLS", "1")
    monkeypatch.setenv("FESTIVAL_PARSER_MAX_ESTIMATED_TOKENS_PER_CALL", "5000")
    monkeypatch.setenv("FESTIVAL_PARSER_TIMEOUT_MS", "9000")
    monkeypatch.setenv("FESTIVAL_PARSER_LLM_MODEL", "gemma-test")

    result = await run_festival_parser_kernel(
        url="https://example.com/fest",
        run_id="run-1",
        dataset_sources=[],
        timeout_minutes=7,
    )

    assert result[0] == "complete"
    assert captured["timeout_minutes"] == 7
    assert captured["run_config"]["no_llm"] is True
    assert captured["run_config"]["max_llm_calls"] == 1
    assert captured["run_config"]["max_estimated_tokens_per_call"] == 5000
    assert captured["run_config"]["timeout_ms"] == 9000
    assert captured["run_config"]["llm_model"] == "gemma-test"


@pytest.mark.asyncio
async def test_festival_parser_rate_limiter_rejects_oversized_call() -> None:
    path = PROJECT_ROOT / "kaggle" / "UniversalFestivalParser" / "src" / "rate_limit.py"
    spec = importlib.util.spec_from_file_location("festival_parser_rate_limit_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    limiter = module.GemmaRateLimiter()
    with pytest.raises(module.FestivalParserRateLimitError):
        await limiter.wait_if_needed(limiter.config.effective_tpm + 1)
