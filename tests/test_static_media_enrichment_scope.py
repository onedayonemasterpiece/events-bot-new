from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select

from db import Database
from models import Event
from scripts.enqueue_static_event_media_enrichment import static_event_eligibility
from static_site_public_projection import public_occurrence_gate_reason


def _event(
    title: str,
    *,
    date: str = "2026-07-20",
    end_date: str | None = None,
    silent: bool = False,
    lifecycle_status: str = "active",
    time: str = "18:00",
    identity_status: str = "canonical",
    merged_into_event_id: int | None = None,
    location_name: str = "Площадка",
) -> Event:
    return Event(
        title=title,
        description="Описание",
        date=date,
        end_date=end_date,
        time=time,
        location_name=location_name,
        source_text="Источник",
        silent=silent,
        lifecycle_status=lifecycle_status,
        identity_status=identity_status,
        merged_into_event_id=merged_into_event_id,
    )


@pytest.mark.asyncio
async def test_static_media_enrichment_excludes_non_public_rows(tmp_path) -> None:
    db = Database(str(tmp_path / "scope.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add_all(
            [
                _event("future active"),
                _event(
                    "ongoing active",
                    date="2026-06-01",
                    end_date="2026-08-01",
                ),
                _event("silent tombstone", silent=True),
                _event("cancelled", lifecycle_status="cancelled"),
                _event("postponed", lifecycle_status="postponed"),
                _event("already ended", date="2026-06-01", end_date="2026-07-14"),
            ]
        )
        await session.commit()

        titles = (
            await session.execute(
                select(Event.title)
                .where(static_event_eligibility("2026-07-15"))
                .order_by(Event.title)
            )
        ).scalars().all()

    assert titles == ["future active", "ongoing active"]


def test_shared_public_occurrence_gate_rejects_noncanonical_elapsed_and_leaking_rows() -> None:
    valid = _event("Обычное событие", date="2026-07-15", time="19:00")
    assert public_occurrence_gate_reason(valid, "2026-07-15", "18:30") is None
    assert public_occurrence_gate_reason(_event("alias", identity_status="alias"), "2026-07-15") == "identity_status:not_canonical"
    assert public_occurrence_gate_reason(_event("merged", merged_into_event_id=7), "2026-07-15") == "merged_into_event_id"
    assert public_occurrence_gate_reason(_event("invalid", date="2026-99-99"), "2026-07-15") == "date:invalid_iso"
    assert public_occurrence_gate_reason(_event("elapsed", date="2026-07-15", time="17:00"), "2026-07-15", "18:30") == "occurrence:elapsed"
    assert public_occurrence_gate_reason(_event("ongoing", date="2026-07-14", end_date="2026-07-15", time="10:00"), "2026-07-15", "18:30") is None
    assert public_occurrence_gate_reason(_event("Вот обновленный текст: концерт"), "2026-07-15") == "title:leakage"
    assert public_occurrence_gate_reason(_event("Лекция", location_name="В программе — лекция и обсуждение"), "2026-07-15") == "location_name:leakage"


def test_event_media_worker_rechecks_public_gate_before_any_media_work() -> None:
    source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
    start = source.index("async def job_event_media_review(")
    end = source.index("\n\nJOB_HANDLERS =", start)
    body = source[start:end]

    gate = body.index("public_occurrence_gate_reason(")
    rehydrate = body.index("_rehydrate_missing_event_source_posters_for_telegraph(")
    review = body.index("review_next_event_media_pair")
    assert gate < rehydrate < review
