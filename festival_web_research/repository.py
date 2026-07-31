"""Operational persistence for approval-gated festival web research."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from db import Database
from models import (
    FestivalQueueItem,
    FestivalWebResearchItem,
    FestivalWebResearchLaneRun,
    FestivalWebResearchRun,
    FestivalWebResearchSource,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class FestivalResearchRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def get_run(self, run_id: int) -> FestivalWebResearchRun | None:
        async with self.db.get_session() as session:
            return await session.get(FestivalWebResearchRun, run_id)

    async def get_run_by_fingerprint(self, fingerprint: str) -> FestivalWebResearchRun | None:
        async with self.db.get_session() as session:
            result = await session.execute(
                select(FestivalWebResearchRun).where(
                    FestivalWebResearchRun.input_fingerprint == fingerprint
                )
            )
            return result.scalar_one_or_none()

    async def create_run(self, **values: Any) -> FestivalWebResearchRun:
        row = FestivalWebResearchRun(**values)
        async with self.db.get_session() as session:
            session.add(row)
            await session.commit()
            await session.refresh(row)
        return row

    async def next_attempt(self, run_id: int, lane: str) -> int:
        async with self.db.get_session() as session:
            result = await session.execute(
                select(FestivalWebResearchLaneRun.attempt_no)
                .where(
                    FestivalWebResearchLaneRun.run_id == run_id,
                    FestivalWebResearchLaneRun.lane == lane,
                )
                .order_by(FestivalWebResearchLaneRun.attempt_no.desc())
                .limit(1)
            )
            current = result.scalar_one_or_none()
            return int(current or 0) + 1

    async def create_lane(self, **values: Any) -> FestivalWebResearchLaneRun:
        row = FestivalWebResearchLaneRun(**values)
        async with self.db.get_session() as session:
            session.add(row)
            await session.commit()
            await session.refresh(row)
        return row

    async def get_lane(self, lane_id: int) -> FestivalWebResearchLaneRun | None:
        async with self.db.get_session() as session:
            return await session.get(FestivalWebResearchLaneRun, lane_id)

    async def attach_queue_items(
        self,
        run_id: int,
        *,
        queue_item_ids: Any,
        original_status: str,
    ) -> None:
        async with self.db.get_session() as session:
            for queue_id in queue_item_ids:
                result = await session.execute(
                    select(FestivalWebResearchItem).where(
                        FestivalWebResearchItem.run_id == run_id,
                        FestivalWebResearchItem.queue_item_id == int(queue_id),
                    )
                )
                if result.scalar_one_or_none() is None:
                    session.add(FestivalWebResearchItem(
                        run_id=run_id,
                        queue_item_id=int(queue_id),
                        original_status=original_status,
                        source_role="other",
                    ))
            await session.commit()

    async def add_sources(self, lane_id: int, sources: list[Any]) -> None:
        async with self.db.get_session() as session:
            for source in sources:
                existing = await session.execute(
                    select(FestivalWebResearchSource).where(
                        FestivalWebResearchSource.lane_run_id == lane_id,
                        FestivalWebResearchSource.source_id == source.source_id,
                    )
                )
                if existing.scalar_one_or_none() is not None:
                    continue
                session.add(FestivalWebResearchSource(
                    lane_run_id=lane_id,
                    source_id=source.source_id,
                    requested_url=source.requested_url,
                    resolved_url=source.resolved_url,
                    canonical_url=source.canonical_url,
                    source_role=source.source_role.value,
                    edition_status=source.edition_status.value,
                    content_sha256=source.content_sha256,
                    snapshot_ref=source.snapshot_ref,
                    normalizer_version=source.normalizer_version,
                    fetched_at=source.retrieved_at_utc,
                    decision="accepted" if source.edition_status.value == "accepted" else "excluded",
                    exclusion_reason=None if source.edition_status.value == "accepted" else source.edition_status.value,
                ))
            await session.commit()

    async def update_lane(self, lane_id: int, **values: Any) -> FestivalWebResearchLaneRun:
        async with self.db.get_session() as session:
            row = await session.get(FestivalWebResearchLaneRun, lane_id)
            if row is None:
                raise LookupError(f"festival research lane {lane_id} not found")
            for name, value in values.items():
                setattr(row, name, value)
            row.updated_at = _now()
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row

    async def update_run(self, run_id: int, **values: Any) -> FestivalWebResearchRun:
        async with self.db.get_session() as session:
            row = await session.get(FestivalWebResearchRun, run_id)
            if row is None:
                raise LookupError(f"festival research run {run_id} not found")
            for name, value in values.items():
                setattr(row, name, value)
            row.updated_at = _now()
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row

    async def review(self, run_id: int, *, decision: str, operator: str, reason: str | None) -> FestivalWebResearchRun:
        if decision not in {"approved", "rejected", "pending"}:
            raise ValueError("unsupported review decision")
        run = await self.update_run(
            run_id,
            review_status=decision,
            reviewed_by=operator,
            reviewed_at=_now(),
            review_reason=reason,
        )
        queue_status = {"approved": "approved", "rejected": "rejected", "pending": "review"}[decision]
        async with self.db.get_session() as session:
            result = await session.execute(
                select(FestivalWebResearchItem).where(FestivalWebResearchItem.run_id == run_id)
            )
            items = list(result.scalars())
            for item in items:
                item.decision = decision
                item.decision_reason = reason
                item.updated_at = _now()
                queue = await session.get(FestivalQueueItem, item.queue_item_id)
                if queue is not None:
                    queue.status = queue_status
                    queue.updated_at = _now()
                    session.add(queue)
                session.add(item)
            await session.commit()
        return run
