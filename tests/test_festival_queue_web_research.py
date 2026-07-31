from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from db import Database
from festival_queue import process_festival_queue
from festival_web_research.coordinator import ResearchResult
from models import FestivalQueueItem


class FakeResearchService:
    def __init__(self) -> None:
        self.calls = []

    async def collect(self, **kwargs):
        self.calls.append(kwargs)
        return ResearchResult(
            run_id=41,
            run_uid="00000000-0000-0000-0000-000000000041",
            state="review",
            review_status="pending",
            candidate={"festival": {"name": kwargs["name_hint"]}, "classification": {}},
            quality={"independent_agreement": True},
            lanes=[],
        )


@pytest.mark.asyncio
async def test_enabled_url_contour_stops_at_review_without_legacy_apply(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_FESTIVAL_WEB_RESEARCH", "1")
    monkeypatch.setitem(sys.modules, "main", types.ModuleType("main"))
    monkeypatch.setattr("festival_web_research.sources.system_resolver", lambda host, port: ("8.8.8.8",))
    db = Database(str(tmp_path / "queue.sqlite"))
    await db.init()
    service = FakeResearchService()
    try:
        async with db.get_session() as session:
            session.add(FestivalQueueItem(
                status="pending", source_kind="url", source_url="https://uhana.ru/",
                festival_name="Балтийская Ухана", festival_full="2026",
                signals_json={"period": {"start_date": "2026-08-07", "end_date": "2026-08-09"}},
                next_run_at=datetime.now(timezone.utc),
            ))
            await session.commit()
        report = await process_festival_queue(
            db, source_kind="url", limit=1, web_research_service=service, trigger="test"
        )
        assert report.success == 1
        assert len(service.calls) == 1
        async with db.get_session() as session:
            item = (await session.execute(select(FestivalQueueItem))).scalar_one()
        assert item.status == "review"
        assert item.result_json["public_apply"] is False
        assert item.result_json["research_run_id"] == 41
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_enabled_url_contour_fails_closed_without_injected_service(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_FESTIVAL_WEB_RESEARCH", "1")
    monkeypatch.setitem(sys.modules, "main", types.ModuleType("main"))
    monkeypatch.setattr("festival_web_research.sources.system_resolver", lambda host, port: ("8.8.8.8",))
    db = Database(str(tmp_path / "queue.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            session.add(FestivalQueueItem(
                status="pending", source_kind="url", source_url="https://example.org/",
                festival_name="Test", next_run_at=datetime.now(timezone.utc),
                signals_json={"period": {"date": "2026-08-07"}},
            ))
            await session.commit()
        report = await process_festival_queue(db, source_kind="url", limit=1, trigger="test")
        assert report.failed == 1
        async with db.get_session() as session:
            item = (await session.execute(select(FestivalQueueItem))).scalar_one()
        assert item.status == "error"
        assert "no strict-limiter service" in (item.last_error or "")
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_stale_url_moves_to_review_without_spending_provider_call(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_FESTIVAL_WEB_RESEARCH", "1")
    monkeypatch.setitem(sys.modules, "main", types.ModuleType("main"))
    monkeypatch.setattr("festival_web_research.sources.system_resolver", lambda host, port: ("8.8.8.8",))
    db = Database(str(tmp_path / "queue.sqlite"))
    await db.init()
    service = FakeResearchService()
    try:
        async with db.get_session() as session:
            session.add(FestivalQueueItem(
                status="pending", source_kind="url", source_url="https://example.org/archive",
                festival_name="Archive", next_run_at=datetime.now(timezone.utc),
                signals_json={"period": {"start_date": "2026-01-01", "end_date": "2026-01-02"}},
            ))
            await session.commit()
        report = await process_festival_queue(
            db, source_kind="url", limit=1, web_research_service=service, trigger="test"
        )
        assert report.skipped == 1
        assert service.calls == []
        async with db.get_session() as session:
            item = (await session.execute(select(FestivalQueueItem))).scalar_one()
        assert item.status == "review"
        assert item.result_json["reason"] == "stale_explicit_period"
    finally:
        await db.close()
