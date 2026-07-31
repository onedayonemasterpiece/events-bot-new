from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from db import Database
from festival_web_research.coordinator import ResearchResult
from festival_web_research.repository import FestivalResearchRepository
from festival_web_research.service import FestivalWebResearchService


class FakeCoordinator:
    taxonomy_sha256 = "a" * 64

    async def collect(self, *, run_id, run_uid, target, allow_c):
        return ResearchResult(
            run_id=run_id, run_uid=run_uid, state="review", review_status="pending",
            candidate={"festival": {"name": target.name_hint}, "classification": {}},
            quality={"independent_agreement": True}, lanes=[],
        )


@pytest.mark.asyncio
async def test_service_persists_collect_only_run_and_approval_does_not_apply(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        repository = FestivalResearchRepository(db)
        service = FestivalWebResearchService(repository=repository, coordinator=FakeCoordinator())
        result = await service.collect(
            name_hint="Балтийская Ухана", edition_hint="2026", urls=["https://uhana.ru/"], allow_c=False
        )
        assert result.state == "review"
        # Fake coordinator does not persist the final candidate, so mirror the real coordinator boundary.
        await repository.update_run(
            result.run_id,
            state="review",
            candidate_json=result.candidate,
            quality_json={"independent_agreement": True, "unresolved_inventory_count": 0},
        )
        approved = await service.approve(result.run_id, operator="test")
        assert approved.review_status == "approved"
        assert approved.mode == "collect_only"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_service_is_idempotent_by_input_fingerprint(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        repository = FestivalResearchRepository(db)
        service = FestivalWebResearchService(repository=repository, coordinator=FakeCoordinator())
        first = await service.collect(name_hint="X", edition_hint=None, urls=["https://example.org/"])
        await repository.update_run(first.run_id, state="review", candidate_json=first.candidate, quality_json=first.quality)
        second = await service.collect(name_hint="X", edition_hint=None, urls=["https://example.org/"])
        assert second.run_id == first.run_id
        assert second.lanes == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_approval_rejects_unresolved_inventory(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        repository = FestivalResearchRepository(db)
        service = FestivalWebResearchService(repository=repository, coordinator=FakeCoordinator())
        result = await service.collect(name_hint="X", edition_hint=None, urls=["https://example.org/"])
        await repository.update_run(
            result.run_id,
            state="review",
            candidate_json=result.candidate,
            quality_json={"independent_agreement": True, "unresolved_inventory_count": 1},
        )
        with pytest.raises(ValueError, match="unresolved programme inventory"):
            await service.approve(result.run_id, operator="test")
    finally:
        await db.close()
