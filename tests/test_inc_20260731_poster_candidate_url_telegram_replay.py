from pathlib import Path

import pytest
from sqlalchemy import select

from db import Database
from models import Event, EventVideoLink, VideoAsset
import smart_event_update as smart_update_module
from smart_event_update import PosterCandidate
from source_parsing.telegram import handlers as tg_handlers


INCIDENT_ID = "INC-2026-07-31-poster-candidate-url"
REPLAY = Path(__file__).parent / "replays" / INCIDENT_ID / "telegram_results.json"
VIDEO_SHA256 = "40d9b68ce40918fba9a1ab432cb00550d7da5bfe6b4c4a19807f13271ce5a60c"


async def _seed_source(db: Database) -> None:
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT id FROM telegram_source WHERE username='meowafisha' LIMIT 1"
            )
        ).fetchone()
        if row:
            await conn.execute(
                "UPDATE telegram_source SET title=?, enabled=1 WHERE id=?",
                ("MEOW Афиша | Калининград", int(row[0])),
            )
        else:
            await conn.execute(
                "INSERT INTO telegram_source(username, title, enabled) VALUES(?, ?, 1)",
                ("meowafisha", "MEOW Афиша | Калининград"),
            )
        await conn.commit()


@pytest.mark.asyncio
async def test_exact_replay_crosses_telegram_import_and_real_smart_update_with_video(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "replay.sqlite"))
    await db.init()
    await _seed_source(db)

    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(
        tg_handlers,
        "_should_skip_past_event_candidate",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        tg_handlers,
        "_event_payload_can_still_be_imported",
        lambda *_args, **_kwargs: True,
    )

    async def fallback_poster(*_args, **_kwargs):
        # Production reached Smart Update with a fallback PosterCandidate.  The
        # missing compatibility URL raised before any event/video write.
        return [
            PosterCandidate(
                catbox_url="https://source.example/poster.jpg",
                ocr_text="Практика · Bus Station · 31 July · 22:00",
            )
        ]

    monkeypatch.setattr(
        tg_handlers,
        "_fallback_fetch_posters_from_public_tg_page",
        fallback_poster,
    )
    real_smart_update = smart_update_module.smart_event_update

    async def real_smart_update_without_outbox(db_arg, candidate, **kwargs):
        kwargs["schedule_tasks"] = False
        return await real_smart_update(db_arg, candidate, **kwargs)

    monkeypatch.setattr(
        tg_handlers,
        "smart_event_update",
        real_smart_update_without_outbox,
    )

    try:
        report = await tg_handlers.process_telegram_results(REPLAY, db)

        assert report.events_created == 1
        assert report.events_merged == 0
        assert report.events_errored == 0
        assert report.errors == []

        async with db.get_session() as session:
            event = (
                await session.execute(
                    select(Event).where(
                        Event.source_post_url == "https://t.me/meowafisha/8101"
                    )
                )
            ).scalar_one()
            asset = (
                await session.execute(
                    select(VideoAsset).where(VideoAsset.sha256 == VIDEO_SHA256)
                )
            ).scalar_one()
            link = (
                await session.execute(
                    select(EventVideoLink).where(
                        EventVideoLink.event_id == int(event.id),
                        EventVideoLink.video_asset_id == int(asset.id),
                    )
                )
            ).scalar_one()

        assert event.title == "Практика"
        assert asset.analysis_status == "accepted"
        assert asset.cdn_url and asset.cdn_url.endswith(f"/{VIDEO_SHA256}.mp4")
        assert asset.showcase_score == pytest.approx(82.75)
        assert link.event_relevance_score == pytest.approx(100.0)
        assert link.ranking_score == pytest.approx(87.0)
        assert link.relation_confidence == pytest.approx(1.0)
        assert link.source_url == "https://t.me/meowafisha/8101"
    finally:
        await db.close()
