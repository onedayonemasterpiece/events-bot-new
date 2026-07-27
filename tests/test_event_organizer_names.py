from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import text

import smart_event_update as su
import vk_intake
import tg_graphic_medallions as tg_graphics
from db import Database
from models import Event


ROOT = Path(__file__).resolve().parents[1]
EXPORTER_PATH = ROOT / "site/scripts/export-production-preview-data.py"


def _load_exporter():
    spec = importlib.util.spec_from_file_location("organizer_preview_exporter", EXPORTER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_grounded_organizers_are_bounded_and_union_without_erasure() -> None:
    source = (
        "Организатор фестиваля — сообщество «Хранители руин». "
        "Площадка — Железнодорожные ворота."
    )
    extracted = su._grounded_organizer_names_from_payload(
        {
            "organizer_names": [
                {
                    "name": "Хранители руин",
                    "evidence_quote": "сообщество «Хранители руин»",
                },
                {
                    "name": "Железнодорожные ворота",
                    "evidence_quote": "Площадка — Железнодорожные ворота",
                },
                {
                    "name": "Выдуманный организатор",
                    "evidence_quote": "Организатор фестиваля",
                },
            ]
        },
        source_corpus=source,
    )

    # The helper proves lexical grounding; the prompt/schema owns the semantic
    # organizer-vs-venue classification.
    assert extracted == ["Хранители руин", "Железнодорожные ворота"]
    assert su._bounded_organizer_names(["Профи-тур"], extracted, ["профи-тур"]) == [
        "Профи-тур",
        "Хранители руин",
        "Железнодорожные ворота",
    ]


def test_vk_organizer_binding_is_explicit_and_unknown_publishers_fail_closed() -> None:
    assert vk_intake._curated_vk_event_organizers(12286984) == ["Профи-тур"]
    assert vk_intake._curated_vk_event_organizers(190663987) == ["Хранители руин"]
    assert vk_intake._curated_vk_event_organizers(231920894) == []
    assert vk_intake._curated_vk_event_organizers(999999999) == []


def test_static_export_preserves_bounded_organizer_names() -> None:
    exporter = _load_exporter()
    assert exporter.event_organizer_names(
        '["Профи-тур", "профи-тур", "Хранители руин"]'
    ) == ["Профи-тур", "Хранители руин"]
    assert exporter.event_organizer_names(None) == []


def test_telegram_graphic_resolver_uses_organizer_field_not_shared_venue() -> None:
    tg_graphics.reset_graphic_medallion_catalog_cache()
    vorotnik = SimpleNamespace(
        organizer_names=["Хранители руин"],
        location_name="Железнодорожные ворота",
        festival="Воротник",
        source_post_url=None,
        source_vk_post_url=None,
        source_urls=[],
        pushkin_card=False,
    )
    ecodvor = SimpleNamespace(
        **{**vars(vorotnik), "organizer_names": [], "festival": None}
    )
    assert [item["slug"] for item in tg_graphics.resolve_event_graphic_medallions(vorotnik)] == [
        "ruin-keepers"
    ]
    assert tg_graphics.resolve_event_graphic_medallions(ecodvor) == []


@pytest.mark.asyncio
async def test_smart_update_create_and_db_persist_organizer_names(tmp_path, monkeypatch) -> None:
    async def no_topics(*_args, **_kwargs):
        return None

    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "_classify_topics", no_topics)
    db = Database(str(tmp_path / "organizers.sqlite"))
    await db.init()
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-12286984_4597",
        source_text="Экскурсия на завод 28 июля в 12:00.",
        title="Экскурсия на судостроительный завод «Янтарь»",
        date="2026-07-28",
        time="12:00",
        location_name="Судостроительный завод «Янтарь»",
        city="Калининград",
        event_type="экскурсия",
        trust_level="high",
        organizer_names=["Профи-тур"],
    )

    result = await su.smart_event_update(
        db,
        candidate,
        check_source_url=False,
        schedule_tasks=False,
    )

    assert result.status == "created"
    async with db.get_session() as session:
        stored = await session.get(Event, result.event_id)
        assert stored is not None
        assert stored.organizer_names == ["Профи-тур"]
    async with db.engine.connect() as conn:
        columns = {
            row[1]: row
            for row in (await conn.execute(text("pragma table_info(event)"))).fetchall()
        }
        assert "organizer_names" in columns
    await db.close()


@pytest.mark.asyncio
async def test_smart_update_merge_unions_organizers_without_erasure(tmp_path, monkeypatch) -> None:
    async def no_topics(*_args, **_kwargs):
        return None

    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "_classify_topics", no_topics)
    db = Database(str(tmp_path / "organizer-merge.sqlite"))
    await db.init()
    base = dict(
        source_type="vk",
        source_text="Городской фестиваль 1 августа в 12:00.",
        title="Воротник",
        date="2026-08-01",
        time="12:00",
        location_name="Железнодорожные ворота",
        city="Калининград",
        event_type="фестиваль",
        trust_level="high",
    )
    first = await su.smart_event_update(
        db,
        su.EventCandidate(
            **base,
            source_url="https://vk.com/wall-190663987_9066",
            organizer_names=["Хранители руин"],
        ),
        check_source_url=False,
        schedule_tasks=False,
    )
    second = await su.smart_event_update(
        db,
        su.EventCandidate(
            **base,
            source_url="https://t.me/partner/100",
            organizer_names=["Партнёрская организация"],
        ),
        check_source_url=False,
        schedule_tasks=False,
    )

    assert first.status == "created"
    assert second.event_id == first.event_id
    async with db.get_session() as session:
        stored = await session.get(Event, first.event_id)
        assert stored is not None
        assert stored.organizer_names == ["Хранители руин", "Партнёрская организация"]
    await db.close()
