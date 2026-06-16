"""Tests for auto-deletion of past-event klgdevents VK posts.

See ``docs/features/vk-publishing/autodeletevkposts.md`` for the safety contract.
"""

import pytest

import main
from main import Database, Event


GROUP_ID = "211443239"
OWNER_ID = -int(GROUP_ID)
PAST_DATE = "2020-01-01"
FUTURE_DATE = "2099-01-01"


def _wall_url(post_id: int, owner_id: int = OWNER_ID) -> str:
    return f"https://vk.com/wall{owner_id}_{post_id}"


async def _add_event(db: Database, **kwargs) -> int:
    defaults = dict(
        title="Title",
        description="Desc",
        festival=None,
        date=PAST_DATE,
        time="18:00",
        location_name="Club",
        location_address=None,
        city="Калининград",
        source_text="Source",
    )
    defaults.update(kwargs)
    async with db.get_session() as session:
        event = Event(**defaults)
        session.add(event)
        await session.commit()
        await session.refresh(event)
        return event.id


def _patch_vk(
    monkeypatch,
    *,
    posts_by_id: dict[int, dict],
    deletes: list,
    recent_items: list[dict] | None = None,
):
    """Stub VK reads (wall.getById) and writes (wall.delete)."""

    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", GROUP_ID, raising=False)
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", GROUP_ID, raising=False)

    async def fake_vk_api(method, **params):
        if method == "wall.get":
            return {"items": list(recent_items or [])}
        assert method == "wall.getById"
        ids = params["posts"].split(",")
        items = []
        for ref in ids:
            _owner, _pid = ref.split("_")
            pid = int(_pid)
            if pid in posts_by_id:
                item = dict(posts_by_id[pid])
                item.setdefault("id", pid)
                items.append(item)
        return items

    async def fake_vk_api_internal(method, params, db=None, bot=None, **kwargs):
        assert method == "wall.delete"
        deletes.append((params["owner_id"], params["post_id"]))
        return {"response": 1}

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api_internal)


@pytest.mark.asyncio
async def test_prune_deletes_past_zero_repost(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event_id = await _add_event(db, source_vk_post_url=_wall_url(101))

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={101: {"reposts": {"count": 0}, "comments": {"count": 0}}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["candidates"] == 1
    assert stats["deleted"] == 1
    assert deletes == [(OWNER_ID, 101)]
    # URL is cleared after a successful delete so the backlog can drain.
    async with db.get_session() as session:
        stored = await session.get(Event, event_id)
    assert stored.source_vk_post_url is None


@pytest.mark.asyncio
async def test_prune_keeps_reposted_post(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(db, source_vk_post_url=_wall_url(102))

    deletes: list = []
    _patch_vk(monkeypatch, posts_by_id={102: {"reposts": {"count": 3}}}, deletes=deletes)

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["deleted"] == 0
    assert stats["kept_reposts"] == 1
    assert deletes == []


@pytest.mark.asyncio
async def test_prune_keeps_commented_post(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(db, source_vk_post_url=_wall_url(112))

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={112: {"reposts": {"count": 0}, "comments": {"count": 2}}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["deleted"] == 0
    assert stats["kept_comments"] == 1
    assert deletes == []


@pytest.mark.asyncio
async def test_prune_keeps_future_event(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(db, date=FUTURE_DATE, source_vk_post_url=_wall_url(103))

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={103: {"reposts": {"count": 0}, "comments": {"count": 0}}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["candidates"] == 0
    assert stats["deleted"] == 0
    assert deletes == []


@pytest.mark.asyncio
async def test_prune_deletes_cancelled_future_event(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(
        db,
        date=FUTURE_DATE,
        lifecycle_status="cancelled",
        source_vk_post_url=_wall_url(113),
    )

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={113: {"reposts": {"count": 0}, "comments": {"count": 0}}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["candidates"] == 1
    assert stats["deleted"] == 1
    assert deletes == [(OWNER_ID, 113)]


@pytest.mark.asyncio
async def test_prune_deletes_live_post_by_postponed_id_for_cancelled_event(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(
        db,
        date=FUTURE_DATE,
        lifecycle_status="cancelled",
        source_vk_post_url=_wall_url(114),
    )

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={},
        recent_items=[
            {
                "id": 3512,
                "postponed_id": 114,
                "reposts": {"count": 0},
                "comments": {"count": 0},
            }
        ],
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["candidates"] == 1
    assert stats["deleted"] == 1
    assert deletes == [(OWNER_ID, 3512)]


@pytest.mark.asyncio
async def test_prune_keeps_ongoing_event_via_end_date(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    # Started in the past but ends in the future -> must NOT be deleted.
    await _add_event(
        db, date=PAST_DATE, end_date=FUTURE_DATE, source_vk_post_url=_wall_url(104)
    )

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={104: {"reposts": {"count": 0}, "comments": {"count": 0}}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["candidates"] == 0
    assert deletes == []


@pytest.mark.asyncio
async def test_prune_skips_external_owner(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    # VK-imported event: source_vk_post_url points at an external community wall.
    external_url = _wall_url(105, owner_id=-999999)
    await _add_event(db, source_vk_post_url=external_url)

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={105: {"reposts": {"count": 0}, "comments": {"count": 0}}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["candidates"] == 0
    assert deletes == []


@pytest.mark.asyncio
async def test_prune_keeps_pinned_post(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(db, source_vk_post_url=_wall_url(106))

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={106: {"reposts": {"count": 0}, "comments": {"count": 0}, "is_pinned": 1}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["deleted"] == 0
    assert stats["kept_pinned"] == 1
    assert deletes == []


@pytest.mark.asyncio
async def test_prune_missing_post_is_skipped(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(db, source_vk_post_url=_wall_url(107))

    deletes: list = []
    # wall.getById returns nothing -> post already gone/unavailable.
    _patch_vk(monkeypatch, posts_by_id={}, deletes=deletes)

    stats = await main.prune_past_event_vk_posts(db)

    assert stats["candidates"] == 1
    assert stats["deleted"] == 0
    assert stats["missing"] == 1
    assert deletes == []


@pytest.mark.asyncio
async def test_prune_dry_run_does_not_delete(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(db, source_vk_post_url=_wall_url(108))

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={108: {"reposts": {"count": 0}, "comments": {"count": 0}}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db, dry_run=True)

    assert stats["deleted"] == 1
    assert deletes == []


@pytest.mark.asyncio
async def test_prune_no_group_configured(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _add_event(db, source_vk_post_url=_wall_url(109))

    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "", raising=False)
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "", raising=False)

    stats = await main.prune_past_event_vk_posts(db)
    assert stats["candidates"] == 0
    assert stats["deleted"] == 0


@pytest.mark.asyncio
async def test_prune_limit_caps_candidates(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    for pid in (201, 202, 203):
        await _add_event(db, source_vk_post_url=_wall_url(pid))

    deletes: list = []
    posts = {pid: {"reposts": {"count": 0}, "comments": {"count": 0}} for pid in (201, 202, 203)}
    _patch_vk(monkeypatch, posts_by_id=posts, deletes=deletes)

    stats = await main.prune_past_event_vk_posts(db, limit=2)

    assert stats["candidates"] == 3
    assert stats["deleted"] == 2
    assert len(deletes) == 2


@pytest.mark.asyncio
async def test_prune_dry_run_keeps_url(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event_id = await _add_event(db, source_vk_post_url=_wall_url(110))

    deletes: list = []
    _patch_vk(
        monkeypatch,
        posts_by_id={110: {"reposts": {"count": 0}, "comments": {"count": 0}}},
        deletes=deletes,
    )

    stats = await main.prune_past_event_vk_posts(db, dry_run=True)

    assert stats["deleted"] == 1
    assert stats["dry_run"] == 1
    assert deletes == []
    async with db.get_session() as session:
        stored = await session.get(Event, event_id)
    assert stored.source_vk_post_url == _wall_url(110)


@pytest.mark.asyncio
async def test_prune_backlog_drains_across_runs(tmp_path, monkeypatch):
    """Clearing the URL on delete lets a capped run reach the rest next time."""
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    for pid in (301, 302):
        await _add_event(db, source_vk_post_url=_wall_url(pid))

    deletes: list = []
    posts = {pid: {"reposts": {"count": 0}, "comments": {"count": 0}} for pid in (301, 302)}
    _patch_vk(monkeypatch, posts_by_id=posts, deletes=deletes)

    # First run with limit=1 deletes exactly one and clears its URL.
    stats1 = await main.prune_past_event_vk_posts(db, limit=1)
    assert stats1["candidates"] == 2
    assert stats1["deleted"] == 1

    # Simulate VK no longer returning the already-deleted post.
    deleted_pid = deletes[0][1]
    posts.pop(deleted_pid, None)

    # Second run reaches the remaining candidate instead of re-processing the
    # already-deleted one.
    stats2 = await main.prune_past_event_vk_posts(db, limit=1)
    assert stats2["candidates"] == 1
    assert stats2["deleted"] == 1
    assert len(deletes) == 2
    assert {d[1] for d in deletes} == {301, 302}


@pytest.mark.asyncio
async def test_prune_scheduler_uses_plain_database(tmp_path, monkeypatch):
    """Production Database has no ensure_connection method; scheduler must not require it."""

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    called: list[tuple[Database, object | None]] = []

    async def fake_prune(db_obj, bot_obj):
        called.append((db_obj, bot_obj))
        return {
            "candidates": 0,
            "deleted": 0,
            "kept_reposts": 0,
            "kept_comments": 0,
            "kept_pinned": 0,
            "missing": 0,
            "errors": 0,
            "dry_run": 0,
        }

    monkeypatch.setattr(main, "prune_past_event_vk_posts", fake_prune)

    await main.vk_post_prune_scheduler(db, None, run_id="test")

    assert called == [(db, None)]
