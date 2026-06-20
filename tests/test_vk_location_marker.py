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


@pytest.mark.asyncio
async def test_post_to_vk_prefers_user_actor_for_location_marker(monkeypatch):
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", "group-token")
    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")

    async def fake_reserve(owner_id, actors, db, bot, **kwargs):
        assert [actor.kind for actor in actors] == ["user", "group"]
        return None

    wall_calls: list[dict[str, object]] = []

    async def fake_vk_api(method, params=None, *_, token=None, token_kind="group", **__):
        if method == "wall.post":
            wall_calls.append(
                {"params": dict(params or {}), "token": token, "token_kind": token_kind}
            )
            return {"response": {"post_id": 77}}
        return {"response": {}}

    monkeypatch.setattr(main, "_reserve_vk_postponed_publish_date", fake_reserve)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await main.post_to_vk(
        "231920894",
        "Message",
        location_marker={"lat": 54.710426, "long": 20.452214},
    )

    assert url == "https://vk.com/wall-231920894_77"
    assert wall_calls[0]["token_kind"] == "user"
    assert wall_calls[0]["token"] == "user-token"
    assert wall_calls[0]["params"]["lat"] == "54.710426"
    assert wall_calls[0]["params"]["long"] == "20.452214"


@pytest.mark.asyncio
async def test_edit_vk_post_applies_location_marker_even_when_text_unchanged(monkeypatch):
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setenv("VK_USER_TOKEN", "user-token")
    monkeypatch.setattr(main, "VK_POST_MAX_EDIT_AGE", main.timedelta(days=3650))

    calls: list[tuple[str, dict[str, object], str | None]] = []

    async def fake_vk_api(method, params=None, *_, token=None, **__):
        calls.append((method, dict(params or {}), token))
        if method == "wall.getById":
            return {
                "response": [
                    {
                        "id": 4018,
                        "date": int(main.datetime.now(main.timezone.utc).timestamp()),
                        "can_edit": 1,
                        "text": "Same text",
                        "attachments": [
                            {"type": "photo", "photo": {"owner_id": -231920894, "id": 1}}
                        ],
                    }
                ]
            }
        if method == "wall.edit":
            return {"response": {"post_id": 4018}}
        raise AssertionError(method)

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    changed = await main.edit_vk_post(
        "https://vk.com/wall-231920894_4018",
        "Same text",
        None,
        None,
        location_marker={"lat": 54.710426, "long": 20.452214},
    )

    assert changed is True
    edit_calls = [params for method, params, _token in calls if method == "wall.edit"]
    assert edit_calls
    assert edit_calls[0]["lat"] == "54.710426"
    assert edit_calls[0]["long"] == "20.452214"
