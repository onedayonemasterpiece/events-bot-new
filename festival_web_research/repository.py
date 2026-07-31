"""Operational persistence for approval-gated festival web research."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from db import Database
from models import FestivalWebResearchLaneRun, FestivalWebResearchRun


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
        return await self.update_run(
            run_id,
            review_status=decision,
            reviewed_by=operator,
            reviewed_at=_now(),
            review_reason=reason,
        )
