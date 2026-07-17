import argparse
import json

import pytest
from sqlmodel import select

from db import Database
from event_media import IMAGE_GEOMETRY_PROMPT_VERSION, image_geometry_model
from models import Event, EventImageGeometry, EventPoster
from scripts.backfill_event_image_geometry import _import


async def _seed(db: Database) -> tuple[int, int]:
    async with db.get_session() as session:
        event = Event(
            title="Geometry import",
            description="test",
            date="2030-01-01",
            time="12:00",
            location_name="test",
            source_text="test",
        )
        session.add(event)
        await session.flush()
        poster = EventPoster(
            event_id=int(event.id),
            poster_hash="poster-v1",
            catbox_url="https://example.test/poster.jpg",
            review_status="approved",
        )
        session.add(poster)
        await session.commit()
        return int(event.id), int(poster.id)


def _args(tmp_path, db_path, input_path, *, apply: bool):
    return argparse.Namespace(
        db=str(db_path),
        input=str(input_path),
        apply=apply,
        backup_out=str(tmp_path / "backup.json"),
    )


@pytest.mark.asyncio
async def test_backfill_import_is_fingerprint_guarded_and_dry_run_safe(tmp_path):
    db_path = tmp_path / "db.sqlite"
    db = Database(str(db_path))
    await db.init()
    event_id, poster_id = await _seed(db)
    await db.engine.dispose()

    valid = {
        "status": "classified",
        "poster_id": poster_id,
        "event_id": event_id,
        "poster_hash": "poster-v1",
        "pixel_sha256": "a" * 64,
        "model": image_geometry_model(),
        "prompt_version": IMAGE_GEOMETRY_PROMPT_VERSION,
        "source_width": 1200,
        "source_height": 800,
        "face_boxes_yxyx": [[0.1, 0.2, 0.3, 0.4]],
        "valuable_region_yxyx": [0.05, 0.1, 0.9, 0.8],
        "valuable_region_confidence": 0.8,
        "reason_code": "main_subject",
    }
    stale = {**valid, "poster_hash": "old-poster"}
    malformed = {**valid, "face_boxes_yxyx": [[0.4, 0.2, 0.3, 0.5]]}
    input_path = tmp_path / "results.jsonl"
    input_path.write_text(
        "\n".join(json.dumps(row) for row in (valid, stale, malformed)) + "\n",
        encoding="utf-8",
    )

    assert await _import(_args(tmp_path, db_path, input_path, apply=False)) == 0
    check = Database(str(db_path))
    await check.init()
    async with check.get_session() as session:
        poster = await session.get(EventPoster, poster_id)
        geometries = (await session.execute(select(EventImageGeometry))).scalars().all()
        assert poster.image_geometry_id is None
        assert geometries == []
    await check.engine.dispose()

    assert await _import(_args(tmp_path, db_path, input_path, apply=True)) == 0
    check = Database(str(db_path))
    await check.init()
    async with check.get_session() as session:
        poster = await session.get(EventPoster, poster_id)
        geometry = await session.get(EventImageGeometry, poster.image_geometry_id)
        assert poster.pixel_sha256 == "a" * 64
        assert poster.width == 1200
        assert poster.height == 800
        assert geometry.face_boxes_yxyx_json == [[0.1, 0.2, 0.3, 0.4]]
        assert geometry.valuable_region_yxyx_json == [0.05, 0.1, 0.9, 0.8]
    await check.engine.dispose()

    backup = json.loads((tmp_path / "backup.json").read_text(encoding="utf-8"))
    assert backup == [
        {
            "poster_id": poster_id,
            "event_id": event_id,
            "poster_hash": "poster-v1",
            "pixel_sha256": None,
            "image_geometry_id": None,
        }
    ]
