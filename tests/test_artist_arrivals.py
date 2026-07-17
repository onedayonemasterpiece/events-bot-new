from __future__ import annotations

import io
import importlib.util
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from PIL import Image
from sqlalchemy import func, select

from artist_arrivals.publisher import publish_artist_arrival_issue, reconcile_artist_arrival_delivery
from artist_arrivals.rendering import (
    build_telegram_slideshow_html,
    render_artist_arrival_card,
)
from artist_arrivals.service import (
    build_artist_arrival_issue,
    ensure_artist_arrivals_promo_campaign,
    ensure_curated_artist_data,
    ensure_curated_artist_media_candidates,
    event_artist_source_revision,
    photo_publication_metadata,
    prune_artist_arrival_shadow_issues,
    public_artist_arrival_projection,
)
from db import Database
from models import (
    ArtistDigestIssue,
    ArtistMediaAsset,
    ArtistMediaProvenance,
    ArtistPublicationLedger,
    ArtistRegistryEntity,
    Event,
    EventArtistAppearance,
    PromoActivity,
    PromoCampaign,
)

NOW = datetime(2026, 7, 17, 8, tzinfo=timezone.utc)


def _event(event_id: int, *, day: int, title: str = "Концерт") -> Event:
    return Event(
        id=event_id,
        title=title,
        description="Описание",
        date=f"2026-07-{day:02d}",
        time="19:00",
        location_name="Площадка",
        city="Калининград",
        source_text="Источник",
        source_post_url=f"https://example.test/events/{event_id}",
    )


async def _db(tmp_path) -> Database:
    db = Database(str(tmp_path / "artist-arrivals.sqlite"))
    await db.init()
    return db


async def _appearance(
    db: Database,
    *,
    event_id: int,
    artist_id: str,
    name: str,
    day: int,
    project: str,
    locality: str = "non_local_ru_verified",
    media_identity_status: str = "unverified",
    photo_url: str | None = None,
    photo_rights_status: str = "none",
) -> None:
    project_key = project.casefold().replace(" ", "-")
    async with db.get_session() as session:
        event = await session.get(Event, event_id)
        if event is None:
            event = _event(event_id, day=day, title=project)
            session.add(event)
        if await session.get(ArtistRegistryEntity, artist_id) is None:
            session.add(
                ArtistRegistryEntity(
                    artist_id=artist_id,
                    display_name=name,
                    canonical_name=name,
                    locality_status=locality,
                    evidence_json=[{"source_url": "https://artist.test"}],
                    verification_status="verified",
                    valid_until=datetime(2027, 1, 1),  # exercise naive SQLite datetime
                    photo_url=photo_url,
                    photo_rights_status=photo_rights_status,
                    photo_rights_evidence_json=(
                        [{
                            "source_url": "https://artist.test/press-kit",
                            "license": "press-kit",
                            "service": "Official site",
                            "account_handle": "artist",
                        }]
                        if photo_url
                        else []
                    ),
                )
            )
        await session.flush()
        session.add(
            EventArtistAppearance(
                event_id=event_id,
                artist_id=artist_id,
                project_title=project,
                project_key=project_key,
                visit_cluster_key=f"{artist_id}:{project_key}",
                status="confirmed",
                physical_visit_status="confirmed",
                participant_evidence_json=[{"source_url": "https://event.test"}],
                appearance_input_hash=f"hash-{event_id}-{artist_id}",
                source_revision=event_artist_source_revision(event),
                eligibility_status="eligible",
                media_identity_status=media_identity_status,
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_db_init_and_curated_seed_are_idempotent(tmp_path):
    db = await _db(tmp_path)
    try:
        from artist_arrivals.service import load_curated_artist_data

        payload = load_curated_artist_data()
        async with db.get_session() as session:
            for index, event_id in enumerate(sorted({int(item["event_id"]) for item in payload["appearances"]})):
                session.add(_event(event_id, day=18 + index % 10))
            await session.commit()
        first = await ensure_curated_artist_data(db)
        held_profile_id = str(payload["profiles"][0]["artist_id"])
        held_appearance_event = int(payload["appearances"][0]["event_id"])
        async with db.get_session() as session:
            held_profile = await session.get(ArtistRegistryEntity, held_profile_id)
            held_profile.verification_status = "review"
            held_profile.locality_status = "unknown"
            held_profile.photo_url = "https://images.test/reviewed.jpg"
            held_profile.photo_rights_status = "press_kit_verified"
            held_profile.photo_rights_evidence_json = [{"source_url": "https://images.test/rights"}]
            held_appearance = (
                await session.execute(
                    select(EventArtistAppearance)
                    .where(EventArtistAppearance.event_id == held_appearance_event)
                    .where(EventArtistAppearance.artist_id == held_profile_id)
                )
            ).scalars().one()
            held_appearance.status = "cancelled"
            held_appearance.eligibility_status = "ineligible"
            held_appearance.exclusion_reason = "operator_hold"
            held_appearance.cancelled_at = NOW
            held_appearance.media_identity_status = "verified"
            held_appearance.media_rights_status = "press_kit_verified"
            await session.commit()
        second = await ensure_curated_artist_data(db)
        async with db.get_session() as session:
            profile_count = await session.scalar(select(func.count()).select_from(ArtistRegistryEntity))
            appearance_count = await session.scalar(select(func.count()).select_from(EventArtistAppearance))
            held_profile = await session.get(ArtistRegistryEntity, held_profile_id)
            held_appearance = (
                await session.execute(
                    select(EventArtistAppearance)
                    .where(EventArtistAppearance.event_id == held_appearance_event)
                    .where(EventArtistAppearance.artist_id == held_profile_id)
                )
            ).scalars().one()
        assert first["missing_events"] == second["missing_events"] == 0
        assert profile_count == len(payload["profiles"])
        assert appearance_count == len(payload["appearances"])
        assert held_profile.verification_status == "review"
        assert held_profile.locality_status == "unknown"
        assert held_profile.photo_url == "https://images.test/reviewed.jpg"
        assert held_profile.photo_rights_evidence_json
        assert held_appearance.status == "cancelled"
        assert held_appearance.eligibility_status == "ineligible"
        assert held_appearance.cancelled_at is not None
        assert held_appearance.media_identity_status == "verified"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_curated_artist_media_seed_is_link_only_and_idempotent(tmp_path):
    db = await _db(tmp_path)
    try:
        await ensure_curated_artist_data(db)
        first = await ensure_curated_artist_media_candidates(db)
        second = await ensure_curated_artist_media_candidates(db)
        async with db.get_session() as session:
            assets = (await session.execute(select(ArtistMediaAsset))).scalars().all()
            provenance = (
                await session.execute(select(ArtistMediaProvenance))
            ).scalars().all()
        assert first == second == {"assets": 7, "provenance": 7, "missing_artists": 0}
        assert len(assets) == len(provenance) == 7
        assert all(item.storage_status == "remote_candidate" for item in assets)
        assert all(item.cdn_url is None and item.object_path is None for item in assets)
        assert all(item.service == "Pinterest" for item in provenance)
        assert all(item.account_handle and item.source_page_url for item in provenance)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_managed_event_artist_media_precedes_legacy_profile_photo(tmp_path):
    db = await _db(tmp_path)
    try:
        await _appearance(
            db,
            event_id=176,
            artist_id="media-artist",
            name="Media Artist",
            day=18,
            project="Media Project",
            media_identity_status="verified",
            photo_url="https://legacy.example/photo.jpg",
            photo_rights_status="press_kit_verified",
        )
        async with db.get_session() as session:
            asset = ArtistMediaAsset(
                candidate_key="eventposter:176:1",
                artist_id="media-artist",
                media_role="event_identity_photo",
                lifecycle_status="ready",
                identity_status="verified",
                quality_status="approved",
                rights_status="event_artist_verified",
                storage_status="ready",
                cdn_url="https://static.kenigevents.ru/artist/v1/sha256/aa/test.webp",
                preferred=True,
                priority=20,
            )
            session.add(asset)
            await session.flush()
            session.add(
                ArtistMediaProvenance(
                    asset_id=int(asset.id),
                    event_id=176,
                    source_kind="event_announcement",
                    service="Telegram",
                    account_handle="event_organizer",
                    account_name="Event Organizer",
                    account_url="https://t.me/event_organizer",
                    source_page_url="https://t.me/event_organizer/176",
                    source_media_url="https://static.kenigevents.ru/artist/v1/sha256/aa/test.webp",
                    credit_text="Telegram · @event_organizer",
                    review_status="approved",
                    observation_key="eventposter:176:1",
                )
            )
            appearance = (
                await session.execute(
                    select(EventArtistAppearance).where(
                        EventArtistAppearance.event_id == 176
                    )
                )
            ).scalars().one()
            appearance.selected_artist_media_asset_id = int(asset.id)
            await session.commit()
        issue = await build_artist_arrival_issue(
            db, now_utc=NOW, min_artists=1, persist=False
        )
        item = issue.items_json[0]
        assert item["photo_url"].startswith("https://static.kenigevents.ru/")
        assert item["photo_source_service"] == "Telegram"
        assert item["photo_source_account"] == "event_organizer"
        assert item["photo_credit_text"] == "Telegram · @event_organizer"
        assert item["artist_media_asset_id"] == asset.id
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("artists", "projects", "expected"),
    [(2, 2, False), (3, 1, False), (3, 2, True), (4, 2, True)],
)
async def test_digest_threshold_matrix(tmp_path, artists, projects, expected):
    db = await _db(tmp_path)
    try:
        for index in range(artists):
            await _appearance(
                db,
                event_id=100 + index,
                artist_id=f"artist-{index}",
                name=f"Артист {index}",
                day=18 + index,
                project=f"Проект {index % projects}",
            )
        issue = await build_artist_arrival_issue(db, now_utc=NOW, persist=False)
        assert issue.unique_artist_count == artists
        assert issue.unique_project_count == projects
        assert issue.meets_threshold is expected
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_local_and_unknown_artists_are_suppressed(tmp_path):
    db = await _db(tmp_path)
    try:
        await _appearance(db, event_id=201, artist_id="local", name="Местный", day=18, project="A", locality="local_verified")
        await _appearance(db, event_id=202, artist_id="unknown", name="Неизвестный", day=19, project="B", locality="unknown")
        await _appearance(db, event_id=203, artist_id="visitor", name="Гость", day=20, project="C")
        issue = await build_artist_arrival_issue(db, now_utc=NOW, min_artists=1, persist=False)
        assert [item["artist_id"] for item in issue.items_json] == ["visitor"]
        assert issue.excluded_counts_json == {"local_artist": 1, "locality_not_verified": 1}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_nonlocal_artist_still_requires_verified_profile(tmp_path):
    db = await _db(tmp_path)
    try:
        await _appearance(db, event_id=251, artist_id="held", name="Held", day=18, project="A")
        async with db.get_session() as session:
            profile = await session.get(ArtistRegistryEntity, "held")
            profile.verification_status = "review"
            await session.commit()
        issue = await build_artist_arrival_issue(db, now_utc=NOW, min_artists=1, persist=False)
        assert issue.items_json == []
        assert issue.excluded_counts_json == {"artist_not_verified": 1}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_same_artist_project_groups_dates_but_other_project_survives(tmp_path):
    db = await _db(tmp_path)
    try:
        await _appearance(db, event_id=301, artist_id="artist", name="Артист", day=18, project="Шоу")
        await _appearance(db, event_id=302, artist_id="artist", name="Артист", day=19, project="Шоу")
        await _appearance(db, event_id=303, artist_id="artist", name="Артист", day=20, project="Другой проект")
        issue = await build_artist_arrival_issue(db, now_utc=NOW, min_artists=1, min_projects=1, persist=False)
        assert len(issue.items_json) == 2
        grouped = next(item for item in issue.items_json if item["project_title"] == "Шоу")
        assert grouped["event_ids"] == [301, 302]
        assert grouped["dates"] == ["2026-07-18", "2026-07-19"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_default_horizon_includes_appearance_six_months_ahead(tmp_path, monkeypatch):
    monkeypatch.delenv("ARTIST_ARRIVALS_HORIZON_DAYS", raising=False)
    db = await _db(tmp_path)
    try:
        await _appearance(db, event_id=350, artist_id="favorite", name="Любимый", day=18, project="Большой тур")
        async with db.get_session() as session:
            event = await session.get(Event, 350)
            event.date = "2027-01-17"
            appearance = (
                await session.execute(
                    select(EventArtistAppearance).where(EventArtistAppearance.event_id == 350)
                )
            ).scalars().one()
            appearance.source_revision = event_artist_source_revision(event)
            await session.commit()
        all_future = await build_artist_arrival_issue(
            db, now_utc=NOW, min_artists=1, min_projects=1, persist=False
        )
        bounded = await build_artist_arrival_issue(
            db,
            now_utc=NOW,
            horizon_days=14,
            min_artists=1,
            min_projects=1,
            persist=False,
        )
        assert [item["artist_id"] for item in all_future.items_json] == ["favorite"]
        assert all_future.threshold_json["horizon_mode"] == "all_future_catalogue"
        assert all_future.window_end == "2027-01-17"
        assert bounded.items_json == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_publication_ledger_suppresses_only_after_both_social_targets(tmp_path, monkeypatch):
    monkeypatch.setenv("ARTIST_ARRIVALS_TG_TARGET", "@test")
    monkeypatch.setenv("ARTIST_ARRIVALS_VK_GROUP_ID", "1")
    db = await _db(tmp_path)
    try:
        for index in range(3):
            await _appearance(db, event_id=401 + index, artist_id=f"a{index}", name=f"A {index}", day=18 + index, project=f"P {index}")
        first = await build_artist_arrival_issue(db, now_utc=NOW)
        async with db.get_session() as session:
            item = first.items_json[0]
            session.add(
                ArtistPublicationLedger(
                    issue_id=first.id,
                    artist_id=item["artist_id"],
                    project_key=item["project_key"],
                    surface="artist_arrival_digest:telegram",
                    target_key="@test",
                    dedupe_key=item["dedupe_key"],
                    content_hash="hash",
                    publish_status="tg_published",
                )
            )
            await session.commit()
        second = await build_artist_arrival_issue(db, now_utc=NOW, persist=False)
        assert len([value for value in second.items_json if value["social_selected"]]) == 3
        async with db.get_session() as session:
            session.add(
                ArtistPublicationLedger(
                    issue_id=first.id,
                    artist_id=item["artist_id"],
                    project_key=item["project_key"],
                    surface="artist_arrival_digest:vk",
                    target_key="1",
                    dedupe_key=item["dedupe_key"],
                    content_hash="hash",
                    publish_status="published",
                )
            )
            await session.commit()
        third = await build_artist_arrival_issue(db, now_utc=NOW, persist=False)
        selected = [value for value in third.items_json if value["social_selected"]]
        assert len(third.items_json) == 3
        assert len(selected) == 2
        assert item["dedupe_key"] not in {value["dedupe_key"] for value in selected}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_changed_event_source_revision_invalidates_reviewed_appearance(tmp_path):
    db = await _db(tmp_path)
    try:
        await _appearance(db, event_id=451, artist_id="reviewed", name="Reviewed", day=18, project="Project")
        async with db.get_session() as session:
            event = await session.get(Event, 451)
            event.date = "2026-07-19"
            await session.commit()
        issue = await build_artist_arrival_issue(db, now_utc=NOW, min_artists=1, persist=False)
        assert issue.items_json == []
        assert issue.excluded_counts_json == {"source_revision_changed": 1}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_changed_event_description_invalidates_reviewed_appearance(tmp_path):
    db = await _db(tmp_path)
    try:
        await _appearance(db, event_id=452, artist_id="reviewed-description", name="Reviewed", day=18, project="Project")
        async with db.get_session() as session:
            event = await session.get(Event, 452)
            event.description = "Теперь заявлен другой состав"
            await session.commit()
        issue = await build_artist_arrival_issue(db, now_utc=NOW, min_artists=1, persist=False)
        assert issue.items_json == []
        assert issue.excluded_counts_json == {"source_revision_changed": 1}
    finally:
        await db.close()


def test_rich_slideshow_and_photo_rights_gate():
    items = [
        {
            "item_key": f"a{i}:p{i}",
            "artist_name": f"Артист {i}",
            "arrival_kind": "russia",
            "project_title": f"Проект {i}",
            "dates": [f"2026-07-{18+i:02d}"],
            "event_url": f"https://example.test/{i}",
            "venues": ["Зал"],
            "municipalities": ["Калининград"],
        }
        for i in range(3)
    ]
    rich = build_telegram_slideshow_html(items)
    assert rich.count("<tg-slideshow>") == 1
    assert rich.count('src="tg://photo?id=artist-') == 3
    with pytest.raises(ValueError):
        build_telegram_slideshow_html(items[:2])

    image = Image.new("RGB", (500, 700), "#123456")
    payload = io.BytesIO()
    image.save(payload, format="JPEG")
    unverified = render_artist_arrival_card(items[0] | {"media_identity_status": "unverified", "photo_rights_status": "press_kit_verified"}, source_image=payload.getvalue())
    verified = render_artist_arrival_card(items[0] | {"media_identity_status": "verified", "photo_rights_status": "press_kit_verified", "photo_rights_evidence_ids": ["rights"]}, source_image=payload.getvalue())
    assert unverified.used_photo is False
    assert verified.used_photo is True


def test_informational_photo_requires_reviewed_provenance_and_exports_credit():
    incomplete = photo_publication_metadata(
        "informational_citation_reviewed",
        [{"source_url": "https://www.pinterest.com/pin/123", "source_name": "Pinterest"}],
    )
    approved = photo_publication_metadata(
        "informational_citation_reviewed",
        [
            {
                "source_url": "https://artist.example/news/tour-photo",
                "discovery_url": "https://www.pinterest.com/pin/123",
                "author_or_rightsholder": "Официальный аккаунт артиста",
                "service": "Official site",
                "account_handle": "artist",
                "lawfully_published_confirmed": True,
                "review_status": "approved",
                "basis": "gc_rf_1274_informational_citation",
                "purpose": "artist_arrival_information",
                "reviewed_by": "editor@example.test",
                "reviewed_at": "2026-07-17T12:00:00Z",
            }
        ],
    )
    assert incomplete is None
    assert approved == {
        "credit_text": "Официальный аккаунт артиста",
        "source_url": "https://artist.example/news/tour-photo",
        "source_service": "Official site",
        "source_account": "artist",
    }


@pytest.mark.asyncio
async def test_promo_activities_are_draft_and_shadow_is_network_safe(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    try:
        stats = await ensure_artist_arrivals_promo_campaign(db)
        again = await ensure_artist_arrivals_promo_campaign(db)
        assert stats == again
        async with db.get_session() as session:
            activities = (await session.execute(select(PromoActivity).where(PromoActivity.campaign_id == stats["campaign_id"]))).scalars().all()
        assert {item.surface for item in activities} == {"artist_arrival_digest", "artist_arrival_hero"}
        assert not any(item.enabled for item in activities)

        for index in range(3):
            await _appearance(db, event_id=501 + index, artist_id=f"s{index}", name=f"S {index}", day=18 + index, project=f"Q {index}")
        issue = await build_artist_arrival_issue(db, now_utc=NOW)
        called = False

        async def forbidden_sender(**_kwargs):
            nonlocal called
            called = True

        result = await publish_artist_arrival_issue(
            db,
            issue,
            publication_mode="shadow",
            telegram_target="@test",
            vk_group_id="1",
            telegram_sender=forbidden_sender,
        )
        assert result.ready is False
        assert "publication_mode_is_shadow" in result.blockers
        assert called is False
        async with db.get_session() as session:
            refreshed = await session.get(ArtistDigestIssue, issue.id)
        assert refreshed.status == "shadow_ready"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_auto_publish_uses_one_frozen_issue_and_writes_cross_surface_ledger(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    try:
        campaign = await ensure_artist_arrivals_promo_campaign(db)
        async with db.get_session() as session:
            campaign_row = await session.get(PromoCampaign, campaign["campaign_id"])
            campaign_row.status = "active"
            activity = (
                await session.execute(
                    select(PromoActivity)
                    .where(PromoActivity.campaign_id == campaign["campaign_id"])
                    .where(PromoActivity.surface == "artist_arrival_digest")
                )
            ).scalars().one()
            activity.enabled = True
            await session.commit()
        for index in range(3):
            await _appearance(
                db,
                event_id=601 + index,
                artist_id=f"photo-{index}",
                name=f"Photo {index}",
                day=18 + index,
                project=f"Project {index}",
                media_identity_status="verified",
                photo_url=f"https://static.kenigevents.ru/artist/test/{index}.jpg",
                photo_rights_status="press_kit_verified",
            )
        issue = await build_artist_arrival_issue(db, now_utc=NOW)
        monkeypatch.setenv("ARTIST_ARRIVALS_ALLOW_PUBLICATION", "1")
        calls = {"tg": 0, "upload": 0, "vk": 0}

        async def photo_fetcher(_url):
            image = Image.new("RGB", (500, 700), "#345678")
            payload = io.BytesIO()
            image.save(payload, format="JPEG")
            return payload.getvalue()

        async def tg_sender(**kwargs):
            calls["tg"] += 1
            assert kwargs["chat_id"] == "@test"
            assert len(kwargs["rich_message"].media) == 3
            return SimpleNamespace(message_id=77, chat=SimpleNamespace(username="test"))

        async def vk_uploader(_group, _jpeg, _db, _bot, *, filename):
            calls["upload"] += 1
            return f"photo-1_{calls['upload']}"

        async def vk_sender(_group, _message, _db, _bot, *, attachments, carousel):
            calls["vk"] += 1
            assert len(attachments) == 3
            assert carousel is True
            return "https://vk.com/wall-1_2"

        first = await publish_artist_arrival_issue(
            db,
            issue,
            publication_mode="auto",
            telegram_target="@test",
            vk_group_id="1",
            telegram_sender=tg_sender,
            vk_uploader=vk_uploader,
            vk_sender=vk_sender,
            photo_fetcher=photo_fetcher,
        )
        assert first.ready is True
        assert calls == {"tg": 1, "upload": 3, "vk": 1}
        second = await publish_artist_arrival_issue(
            db,
            issue,
            publication_mode="auto",
            telegram_target="@test",
            vk_group_id="1",
            telegram_sender=tg_sender,
            vk_uploader=vk_uploader,
            vk_sender=vk_sender,
            photo_fetcher=photo_fetcher,
        )
        assert second.ready is True
        assert calls == {"tg": 1, "upload": 3, "vk": 1}
        async with db.get_session() as session:
            ledger_count = await session.scalar(select(func.count()).select_from(ArtistPublicationLedger))
        assert ledger_count == 6
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_operator_can_reconcile_ambiguous_sending_reservations(tmp_path):
    db = await _db(tmp_path)
    try:
        await _appearance(db, event_id=651, artist_id="a", name="A", day=18, project="P")
        issue = await build_artist_arrival_issue(db, now_utc=NOW, min_artists=1, min_projects=1)
        item = issue.items_json[0]
        async with db.get_session() as session:
            session.add(
                ArtistPublicationLedger(
                    issue_id=issue.id,
                    artist_id=item["artist_id"],
                    project_key=item["project_key"],
                    surface="artist_arrival_digest:telegram",
                    target_key="@test",
                    dedupe_key=item["dedupe_key"],
                    content_hash="hash",
                    publish_status="sending",
                )
            )
            await session.commit()
        count = await reconcile_artist_arrival_delivery(
            db,
            surface="artist_arrival_digest:telegram",
            target="@test",
            dedupe_keys=[item["dedupe_key"]],
            outcome="not_published",
        )
        assert count == 1
        async with db.get_session() as session:
            assert await session.scalar(select(func.count()).select_from(ArtistPublicationLedger)) == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_identical_content_still_creates_a_new_daily_issue(tmp_path):
    db = await _db(tmp_path)
    try:
        await _appearance(db, event_id=652, artist_id="daily", name="Daily", day=22, project="P")
        first = await build_artist_arrival_issue(db, now_utc=NOW, min_artists=1, min_projects=1)
        next_day = NOW.replace(day=18)
        second = await build_artist_arrival_issue(db, now_utc=next_day, min_artists=1, min_projects=1)
        assert first.id != second.id
        assert first.manifest_hash != second.manifest_hash
        assert first.build_date == "2026-07-17"
        assert second.build_date == "2026-07-18"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_shadow_retention_deletes_only_unpublished_unreferenced_issues(tmp_path):
    db = await _db(tmp_path)
    try:
        old = NOW - timedelta(days=60)
        async with db.get_session() as session:
            deletable = ArtistDigestIssue(
                manifest_hash="delete-me",
                build_date="2026-05-18",
                window_start="2026-05-18",
                window_end="2026-05-18",
                created_at=old,
            )
            published = ArtistDigestIssue(
                manifest_hash="published",
                build_date="2026-05-18",
                window_start="2026-05-18",
                window_end="2026-05-18",
                created_at=old,
                published_at=old,
            )
            referenced = ArtistDigestIssue(
                manifest_hash="sending",
                build_date="2026-05-18",
                window_start="2026-05-18",
                window_end="2026-05-18",
                created_at=old,
            )
            session.add_all([deletable, published, referenced])
            await session.flush()
            session.add(
                ArtistPublicationLedger(
                    issue_id=referenced.id,
                    artist_id="artist",
                    project_key="project",
                    surface="artist_arrival_digest:telegram",
                    target_key="@test",
                    dedupe_key="artist:project",
                    content_hash="hash",
                    publish_status="sending",
                )
            )
            await session.commit()
        result = await prune_artist_arrival_shadow_issues(
            db, now_utc=NOW, retention_days=45
        )
        async with db.get_session() as session:
            hashes = set(
                (await session.execute(select(ArtistDigestIssue.manifest_hash))).scalars()
            )
        assert result == {"deleted": 1, "protected": 2, "retention_days": 45}
        assert hashes == {"published", "sending"}
    finally:
        await db.close()


def test_public_projection_does_not_leak_evidence():
    issue = ArtistDigestIssue(
        manifest_hash="abc",
        build_date="2026-07-17",
        window_start="2026-07-17",
        window_end="2026-07-31",
        meets_threshold=True,
        items_json=[
            {
                "item_key": "a:p",
                "artist_name": "Артист",
                "arrival_kind": "russia",
                "locality_status": "non_local_ru_verified",
                "project_title": "Проект",
                "dates": ["2026-07-20"],
                "participant_evidence_ids": ["secret"],
                "source_revisions": ["secret"],
            }
        ],
    )
    projection = public_artist_arrival_projection(issue)
    assert projection["eligible"] is False
    assert projection["shadow_eligible"] is True
    assert "participant_evidence_ids" not in projection["items"][0]
    assert "source_revisions" not in projection["items"][0]


def test_curated_project_keys_are_stable_families_not_visit_editions():
    from artist_arrivals.service import load_curated_artist_data

    for appearance in load_curated_artist_data()["appearances"]:
        project_key = appearance["project_key"]
        assert "2026" not in project_key
        assert "july" not in project_key


def test_static_projection_is_expiry_and_hero_activity_gated(tmp_path):
    script = "site/scripts/export-production-preview-data.py"
    spec = importlib.util.spec_from_file_location("static_export", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    con = sqlite3.connect(tmp_path / "projection.sqlite")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        CREATE TABLE artist_digest_issue(
          id INTEGER PRIMARY KEY, manifest_hash TEXT, window_end TEXT,
          meets_threshold BOOLEAN, items_json TEXT, created_at TEXT
        );
        CREATE TABLE promo_campaign(id INTEGER PRIMARY KEY, status TEXT);
        CREATE TABLE promo_activity(id INTEGER PRIMARY KEY, campaign_id INTEGER, surface TEXT, enabled BOOLEAN);
        INSERT INTO promo_campaign(id, status) VALUES(1, 'draft');
        INSERT INTO promo_activity(id, campaign_id, surface, enabled)
        VALUES(1, 1, 'artist_arrival_hero', 0);
        """
    )
    items = [
        {
            "item_key": "a:p",
            "artist_name": "Artist",
            "arrival_kind": "international",
            "locality_status": "non_local_international_verified",
            "project_title": "Project",
            "dates": ["2026-07-20"],
            "event_ids": [1],
            "venues": [],
            "municipalities": [],
            "photo_credit_text": "Official artist account",
            "photo_source_url": "https://artist.example/photo",
            "media_ready": False,
        }
    ]
    con.execute(
        "INSERT INTO artist_digest_issue VALUES(1, 'hash', '2026-07-31', 0, ?, '2026-07-17T00:00:00Z')",
        (json.dumps(items),),
    )
    con.commit()
    shadow = module.export_artist_arrivals_projection(con, current_date="2026-07-17")
    assert shadow["shadow_eligible"] is True
    assert shadow["eligible"] is False
    assert shadow["items"][0]["photo_credit_text"] == "Official artist account"
    assert shadow["items"][0]["photo_source_url"] == "https://artist.example/photo"
    con.execute("UPDATE promo_campaign SET status='active'")
    con.execute("UPDATE promo_activity SET enabled=1")
    con.commit()
    active = module.export_artist_arrivals_projection(con, current_date="2026-07-17")
    assert active["eligible"] is True
    expired = module.export_artist_arrivals_projection(con, current_date="2026-08-01")
    assert expired["items"] == []
    assert expired["eligible"] is False
    con.close()
