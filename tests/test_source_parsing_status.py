import asyncio

import pytest

import source_parsing.handlers as handlers
import source_parsing.philharmonia as philharmonia
from source_parsing.handlers import (
    SourceParsingResult,
    SourceParsingStats,
    _smart_event_update_with_lock_retry,
    _source_parsing_terminal_status,
)
from source_parsing.commands import SOURCE_PARSING_GUARD_URLS


def test_philharmonia_daytime_change_guard_watches_current_catalog():
    assert SOURCE_PARSING_GUARD_URLS["philharmonia"] == (
        "https://filarmonia39.ru/afisha/"
    )


def test_source_loss_cannot_finish_green():
    no_survivors = SourceParsingResult(errors=["Philharmonia kernel failed"])
    assert _source_parsing_terminal_status(no_survivors) == "error"

    partial = SourceParsingResult(
        stats_by_source={
            "dramteatr": SourceParsingStats(
                source="dramteatr",
                total_received=4,
                already_exists=4,
            )
        },
        errors=["Qtickets kernel failed"],
    )
    assert _source_parsing_terminal_status(partial) == "partial"


def test_failed_items_are_partial_but_clean_sources_are_success():
    failed_item = SourceParsingResult(
        stats_by_source={
            "philharmonia": SourceParsingStats(
                source="philharmonia",
                total_received=2,
                new_added=1,
                failed=1,
            )
        }
    )
    assert _source_parsing_terminal_status(failed_item) == "partial"

    clean = SourceParsingResult(
        stats_by_source={
            "philharmonia": SourceParsingStats(
                source="philharmonia",
                total_received=2,
                new_added=2,
            )
        }
    )
    assert _source_parsing_terminal_status(clean) == "success"


def test_smart_update_retries_transient_sqlite_writer_lock():
    calls = 0

    async def flaky_update(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("database is locked")
        return "ok"

    result = asyncio.run(
        _smart_event_update_with_lock_retry(
            object(),
            object(),
            flaky_update,
            attempts=3,
            base_delay_seconds=0,
        )
    )

    assert result == "ok"
    assert calls == 3


def test_smart_update_does_not_retry_non_lock_failure():
    calls = 0

    async def broken_update(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("validation failed")

    with pytest.raises(RuntimeError, match="validation failed"):
        asyncio.run(
            _smart_event_update_with_lock_retry(
                object(),
                object(),
                broken_update,
                attempts=3,
                base_delay_seconds=0,
            )
        )

    assert calls == 1


def test_cancelled_parse_finishes_ops_run_fail_closed(monkeypatch, tmp_path):
    runner_started = asyncio.Event()
    finished: dict[str, object] = {}

    async def fake_start_ops_run(*_args, **_kwargs):
        return 123

    async def fake_finish_ops_run(*_args, **kwargs):
        finished.update(kwargs)

    async def slow_philharmonia_runner(*_args, **_kwargs):
        runner_started.set()
        await asyncio.sleep(3600)

    monkeypatch.setenv("SOURCE_PARSING_DEBUG_DIR", str(tmp_path))
    monkeypatch.setattr(handlers, "start_ops_run", fake_start_ops_run)
    monkeypatch.setattr(handlers, "finish_ops_run", fake_finish_ops_run)
    monkeypatch.setattr(
        philharmonia,
        "run_philharmonia_kaggle_kernel",
        slow_philharmonia_runner,
    )

    async def scenario():
        task = asyncio.create_task(
            handlers.run_source_parsing(object(), only_sources=["philharmonia"])
        )
        await runner_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert finished["run_id"] == 123
    assert finished["status"] == "error"
    assert finished["details"]["fatal_error"] == "run did not reach terminal status"


def test_completed_parse_clears_fail_closed_sentinel(monkeypatch, tmp_path):
    finished: dict[str, object] = {}

    async def fake_start_ops_run(*_args, **_kwargs):
        return 124

    async def fake_finish_ops_run(*_args, **kwargs):
        finished.update(kwargs)

    monkeypatch.setenv("SOURCE_PARSING_DEBUG_DIR", str(tmp_path))
    monkeypatch.setattr(handlers, "start_ops_run", fake_start_ops_run)
    monkeypatch.setattr(handlers, "finish_ops_run", fake_finish_ops_run)

    result = asyncio.run(
        handlers.run_source_parsing(object(), only_sources=["unknown_source"])
    )

    assert result.errors == []
    assert finished["run_id"] == 124
    assert finished["status"] == "success"
    assert finished["details"]["fatal_error"] is None
