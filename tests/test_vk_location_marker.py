import pytest
from sqlalchemy import text

import main


@pytest.mark.asyncio
async def test_post_to_vk_sends_location_marker_and_retries_without_on_invalid(monkeypatch):
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", "group-token")
    monkeypatch.setattr(main, "VK_USER_TOKEN", "")

    async def fake_reserve(*_args, **_kwargs):
        return None

    monkeypatch.setattr(main, "_reserve_vk_postponed_publish_date", fake_reserve)

    calls: list[dict[str, object]] = []

    async def fake_vk_api(method, params=None, *_, **__):
        if method == "wall.post":
            snapshot = dict(params or {})
            calls.append(snapshot)
            if "lat" in snapshot:
                raise main.VKAPIError(100, "invalid location")
            return {"response": {"post_id": 42}}
        return {"response": {}}

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await main.post_to_vk(
        "231920894",
        "Message",
        location_marker={"lat": "54.710426", "long": "20.452214"},
    )

    assert url == "https://vk.com/wall-231920894_42"
    assert calls[0]["lat"] == "54.710426"
    assert calls[0]["long"] == "20.452214"
    assert "lat" not in calls[1]
    assert "long" not in calls[1]


@pytest.mark.asyncio
async def test_sync_vk_source_post_applies_kaliningrad_location_marker(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")

    event = main.Event(
        id=101,
        title="Title",
        description="",
        date="2026-07-01",
        time="12:00",
        location_name="Музей",
        city="Калининград",
        source_text="Text",
    )

    posted: dict[str, object] = {}

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, **kwargs):
        posted["group_id"] = group_id
        posted["location_marker"] = kwargs.get("location_marker")
        return "https://vk.com/wall-231920894_55"

    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)

    url = await main.sync_vk_source_post(event, "Text", db, None)

    assert url == "https://vk.com/wall-231920894_55"
    assert posted["location_marker"] == {"lat": "54.710426", "long": "20.452214"}

    async with db.get_session() as session:
        row = (
            await session.execute(
                text(
                    "SELECT status, is_kaliningrad_oblast, lat, long, provenance "
                    "FROM vk_location_marker_cache WHERE query_norm='калининград'"
                )
            )
        ).first()
    assert row is not None
    assert row[0] == "applied"
    assert bool(row[1]) is True
    assert round(float(row[2]), 6) == 54.710426
    assert round(float(row[3]), 6) == 20.452214
    assert "static_directory" in row[4]


@pytest.mark.asyncio
async def test_sync_vk_source_post_skips_cached_out_of_region_city(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")

    async with db.get_session() as session:
        await session.execute(
            text(
                "INSERT INTO geo_city_region_cache("
                "city_norm, is_kaliningrad_oblast, region_name, source, updated_at"
                ") VALUES('москва', 0, 'Москва', 'test', CURRENT_TIMESTAMP)"
            )
        )
        await session.commit()

    event = main.Event(
        id=102,
        title="Title",
        description="",
        date="2026-07-01",
        time="12:00",
        location_name="Venue",
        city="Москва",
        source_text="Text",
    )

    posted: dict[str, object] = {}

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, **kwargs):
        posted["location_marker"] = kwargs.get("location_marker")
        return "https://vk.com/wall-231920894_56"

    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)

    url = await main.sync_vk_source_post(event, "Text", db, None)

    assert url == "https://vk.com/wall-231920894_56"
    assert posted["location_marker"] is None

    async with db.get_session() as session:
        row = (
            await session.execute(
                text("SELECT status, is_kaliningrad_oblast FROM vk_location_marker_cache WHERE query_norm='москва'")
            )
        ).first()
    assert row is not None
    assert row[0] == "skipped_not_region"
    assert bool(row[1]) is False


@pytest.mark.asyncio
async def test_resolver_skips_ambiguous_city_without_supporting_context(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event = main.Event(city="Лесной", location_name="Дом культуры", location_address="")

    decision = await main.resolve_vk_location_marker_for_event(event, db)

    assert decision.status == "skipped_low_confidence"
    assert not decision.applied
