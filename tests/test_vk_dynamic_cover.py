from __future__ import annotations

from io import BytesIO
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

import main
from handlers.vk_cover_cmd import _cover_keyboard, _usage
from models import Festival
from vk_dynamic_cover import (
    CoverItem,
    WIDE_SIZE,
    apply_dynamic_cover,
    default_cover_path,
    fetch_current_owner_cover_url,
    load_default_cover_state,
    load_cover_history,
    render_cover_pack,
    restore_saved_default_cover,
    save_current_cover_as_default,
    select_cover_items,
    upload_owner_cover,
)


def test_render_cover_pack_creates_wide_and_mobile_images(tmp_path: Path):
    pack = render_cover_pack(
        [
            CoverItem("80 историй о главном", "июнь", "истории города"),
            CoverItem("Кантата", "лето", "музыка и места"),
            CoverItem("Русская музыка на Балтике", "июнь", "концерты фестиваля"),
        ],
        output_dir=tmp_path,
    )

    assert pack.wide_path.exists()
    assert len(pack.mobile_paths) == 4
    with Image.open(pack.wide_path) as img:
        assert img.size == WIDE_SIZE
        assert img.getbbox() is not None
    with Image.open(pack.mobile_paths[0]) as img:
        assert img.size == (1080, 1920)
        assert img.getbbox() is not None


def test_cover_command_apply_is_proposal_only():
    usage = _usage()
    keyboard = _cover_keyboard()
    button_texts = [
        button.text
        for row in keyboard.inline_keyboard
        for button in row
    ]

    assert "/cover request" in usage
    assert "alias: генерирует предложение без публикации" in usage
    assert "✅ Apply" not in button_texts
    assert "📨 На согласование" in button_texts


@pytest.mark.asyncio
async def test_select_cover_items_prefers_active_festivals(tmp_path: Path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(
            Festival(
                name="Past",
                start_date="2026-05-01",
                end_date="2026-05-10",
            )
        )
        session.add(
            Festival(
                name="Active",
                start_date="2026-06-01",
                end_date="2026-06-30",
                city="Калининград",
            )
        )
        session.add(
            Festival(
                name="Future",
                start_date="2026-07-01",
                end_date="2026-07-10",
            )
        )
        await session.commit()

    items = await select_cover_items(
        db,
        now=datetime(2026, 6, 7, tzinfo=timezone.utc),
    )

    assert [item.title for item in items[:2]] == ["Active", "Future"]


@pytest.mark.asyncio
async def test_upload_owner_cover_uses_vk_owner_cover_flow(tmp_path: Path, monkeypatch):
    image_path = tmp_path / "cover.png"
    Image.new("RGB", WIDE_SIZE, "red").save(image_path)
    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")
    calls = []

    async def fake_vk_api(method, params, db=None, bot=None, token=None, token_kind="group", **_kwargs):
        calls.append((method, dict(params), token, token_kind))
        if method == "photos.getOwnerCoverPhotoUploadServer":
            return {"response": {"upload_url": "https://upload.example/cover"}}
        if method == "photos.saveOwnerCoverPhoto":
            return {"response": {"images": [{"url": "https://vk.example/cover.jpg"}]}}
        raise AssertionError(method)

    def fake_post(url, files=None, timeout=None):
        assert url == "https://upload.example/cover"
        assert "file" in files
        assert timeout == 60
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {"hash": "h", "photo": "p"},
        )

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr("vk_dynamic_cover.requests.post", fake_post)

    result = await upload_owner_cover("231920894", image_path)

    assert result["images"][0]["url"] == "https://vk.example/cover.jpg"
    assert calls[0][0] == "photos.getOwnerCoverPhotoUploadServer"
    assert calls[0][1]["group_id"] == "231920894"
    assert calls[0][2:] == ("user-token", "user")
    assert calls[1][0] == "photos.saveOwnerCoverPhoto"
    assert calls[1][1] == {"hash": "h", "photo": "p"}


@pytest.mark.asyncio
async def test_fetch_current_owner_cover_url_uses_largest_vk_cover(monkeypatch):
    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")

    async def fake_vk_api(method, params, db=None, bot=None, token=None, token_kind="group", **_kwargs):
        assert method == "groups.getById"
        assert params == {"group_id": "231920894", "fields": "cover"}
        assert token == "user-token"
        assert token_kind == "user"
        return {
            "response": {
                "groups": [
                    {
                        "cover": {
                            "enabled": 1,
                            "images": [
                                {"url": "https://vk.example/small.jpg", "width": 400, "height": 160},
                                {"url": "https://vk.example/full.jpg", "width": 1920, "height": 768},
                            ],
                        }
                    }
                ]
            }
        }

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await fetch_current_owner_cover_url("231920894")

    assert url == "https://vk.example/full.jpg"


@pytest.mark.asyncio
async def test_save_current_cover_as_default_persists_server_file(tmp_path: Path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("VK_DYNAMIC_COVER_GROUP_ID", "231920894")
    monkeypatch.setenv("VK_DYNAMIC_COVER_STORAGE_DIR", str(tmp_path / "covers"))
    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")

    async def fake_vk_api(method, params, db=None, bot=None, token=None, token_kind="group", **_kwargs):
        assert method == "groups.getById"
        return {
            "response": {
                "groups": [
                    {
                        "cover": {
                            "images": [
                                {"url": "https://vk.example/current.jpg", "width": 1920, "height": 768},
                            ],
                        }
                    }
                ]
            }
        }

    buf = BytesIO()
    Image.new("RGB", (640, 256), "blue").save(buf, "JPEG")

    def fake_get(url, timeout=None):
        assert url == "https://vk.example/current.jpg"
        assert timeout == 60
        return SimpleNamespace(
            content=buf.getvalue(),
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr("vk_dynamic_cover.requests.get", fake_get)

    state = await save_current_cover_as_default(db, operator_id=42)
    loaded = await load_default_cover_state(db)
    path = default_cover_path("231920894")

    assert state["path"] == str(path)
    assert loaded and loaded["path"] == str(path)
    assert path.exists()
    with Image.open(path) as img:
        assert img.size == WIDE_SIZE
    history = await load_cover_history(db)
    assert history[0]["kind"] == "default_saved"
    assert history[0]["operator_id"] == 42


@pytest.mark.asyncio
async def test_restore_saved_default_cover_uploads_saved_file(tmp_path: Path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("VK_DYNAMIC_COVER_GROUP_ID", "231920894")
    monkeypatch.setenv("VK_DYNAMIC_COVER_STORAGE_DIR", str(tmp_path / "covers"))
    path = default_cover_path("231920894")
    path.parent.mkdir(parents=True)
    Image.new("RGB", WIDE_SIZE, "green").save(path)
    await main.set_setting_value(
        db,
        "vk_dynamic_cover_default_state",
        f'{{"kind":"default_saved","group_id":"231920894","path":"{path}","saved_at":"2026-06-07T00:00:00+00:00"}}',
    )
    calls = []

    async def fake_upload(group_id, image_path, db=None, bot=None):
        calls.append((group_id, image_path))
        return {"ok": True}

    monkeypatch.setattr("vk_dynamic_cover.upload_owner_cover", fake_upload)

    changed = await restore_saved_default_cover(db, force=True)

    assert changed is True
    assert calls == [("231920894", path)]
    history = await load_cover_history(db)
    assert history[0]["kind"] == "default_saved_restore"


@pytest.mark.asyncio
async def test_apply_dynamic_cover_records_history_without_publish(tmp_path: Path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("VK_DYNAMIC_COVER_GROUP_ID", "231920894")

    async with db.get_session() as session:
        session.add(
            Festival(
                name="Кантата",
                start_date="2026-06-01",
                end_date="2026-06-30",
                city="Калининград",
            )
        )
        await session.commit()

    pack = await apply_dynamic_cover(
        db,
        operator_id=123,
        reason="test",
        publish=False,
    )
    history = await load_cover_history(db)

    assert pack.wide_path.exists()
    assert history
    assert history[0]["published"] is False
    assert history[0]["items"][0]["title"] == "Кантата"
    assert history[0]["group_id"] == "231920894"
