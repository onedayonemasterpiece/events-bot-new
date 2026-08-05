from __future__ import annotations

import time

import pytest
from sqlalchemy import select

import vk_review
from db import Database
from models import SmartUpdateReview
from smart_event_update import (
    EventCandidate,
    SmartUpdateResult,
    persist_smart_update_review,
)


@pytest.mark.asyncio
async def test_identity_outcome_has_small_durable_review_projection(tmp_path) -> None:
    db = Database(str(tmp_path / "review.sqlite"))
    await db.init()
    try:
        candidate = EventCandidate(
            source_type="parser:fixture",
            source_url="https://example.test/event/1",
            source_text="must not be copied to review projection",
        )
        result = SmartUpdateResult(
            status="review_required",
            event_id=None,
            reason="source_binding_conflict",
        )
        review_id = await persist_smart_update_review(
            db,
            result=result,
            candidate=candidate,
            pipeline="parser",
            carrier_ref="parser:fixture:1",
        )
        assert review_id is not None
        async with db.get_session() as session:
            row = await session.get(SmartUpdateReview, review_id)
        assert row is not None
        assert row.state == "pending_review"
        assert row.attempts == 1
        assert row.source_url == candidate.source_url
        assert not hasattr(row, "source_text")
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_vk_identity_review_is_operator_visible_but_not_auto_requeued(tmp_path) -> None:
    db = Database(str(tmp_path / "vk-review.sqlite"))
    await db.init()
    try:
        async with db.raw_conn() as conn:
            await conn.execute(
                """
                INSERT INTO vk_inbox(group_id, post_id, date, text, has_date, status)
                VALUES (?, ?, ?, ?, ?, 'pending')
                """,
                (1, 2, int(time.time()), "identity review packet", 1),
            )
            row = await (await conn.execute("SELECT id FROM vk_inbox")).fetchone()
            await conn.commit()
        inbox_id = int(row[0])
        await vk_review.mark_review_required(
            db,
            inbox_id,
            reason_code="source_binding_conflict",
            diagnostic_event_id=None,
            identity_decision_log_id=None,
        )

        assert await vk_review.pick_next(
            db, 10, "auto", requeue_skipped=False
        ) is None
        post = await vk_review.pick_next(
            db,
            10,
            "operator",
            requeue_skipped=False,
            include_identity_reviews=True,
        )
        assert post is not None
        assert post.id == inbox_id
        await vk_review.mark_pending(db, inbox_id)
        async with db.raw_conn() as conn:
            row = await (
                await conn.execute(
                    "SELECT status, review_reason_code FROM vk_inbox WHERE id=?",
                    (inbox_id,),
                )
            ).fetchone()
        assert tuple(row) == ("pending", "source_binding_conflict")
    finally:
        await db.close()
