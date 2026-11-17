import ast
import asyncio
import base64
import io
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock

from PIL import Image
from telethon.tl import types

from guide_excursions import scanner


def _channel(peer_id, photo_id):
    photo = types.ChatPhoto(photo_id=photo_id, dc_id=2) if photo_id else types.ChatPhotoEmpty()
    return types.Channel(id=peer_id, title="Tours", date=datetime.now(), photo=photo)


def test_downloads_only_for_a_new_photo_of_this_telegram_peer():
    async def download(peer, *, file, download_big):
        assert download_big is False
        # Real small Telegram profile photos can carry >600 KiB of metadata.
        Image.new("RGB", (640, 640), "blue").save(file, format="JPEG", icc_profile=b"i" * 650_000)

    client = AsyncMock()
    client.download_profile_photo.side_effect = download
    source = _channel(12345, 1234567890123456789)
    current = asyncio.run(scanner._telegram_source_avatar(client, source, None))
    assert current["peer_id"] == -1000000012345
    assert current["photo_id"] == "1234567890123456789"
    assert datetime.fromisoformat(current["observed_at"]).utcoffset().total_seconds() == 0
    encoded = base64.b64decode(current["image_base64"])
    assert len(encoded) < 128 * 1024
    with Image.open(io.BytesIO(encoded)) as image:
        assert image.size == (320, 320)
        assert not image.info.get("icc_profile")
    same = asyncio.run(scanner._telegram_source_avatar(client, source, current))
    assert "image_base64" not in same
    assert client.download_profile_photo.await_count == 1

    other = asyncio.run(scanner._telegram_source_avatar(client, _channel(54321, 1234567890123456789), current))
    changed = asyncio.run(scanner._telegram_source_avatar(client, _channel(12345, 2), current))
    assert other["image_base64"] == changed["image_base64"] == current["image_base64"]
    assert client.download_profile_photo.await_count == 3


def test_unavailable_or_large_profile_photo_keeps_only_observed_metadata():
    client = AsyncMock()
    source = _channel(12345, None)
    empty = asyncio.run(scanner._telegram_source_avatar(client, source, {}))
    assert set(empty) == {"peer_id", "observed_at"}
    client.download_profile_photo.assert_not_awaited()

    def too_large(peer, *, file, download_big):
        file.write(b"x" * (2 * 1024 * 1024))
        file.write(b"x")
    client.download_profile_photo.side_effect = too_large
    source = _channel(12345, 7)
    oversize = asyncio.run(scanner._telegram_source_avatar(client, source, {}))
    assert set(oversize) == {"peer_id", "photo_id", "observed_at"}
    client.download_profile_photo.side_effect = OSError("unavailable")
    missing = asyncio.run(scanner._telegram_source_avatar(client, source, {}))
    assert set(missing) == set(oversize)


def test_standalone_kaggle_scanner_uses_the_same_avatar_collection():
    local = ast.parse(Path("guide_excursions/scanner.py").read_text())
    notebook = ast.parse(Path("kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py").read_text())
    for name in ("_TelegramAvatarBuffer", "_telegram_source_avatar"):
        select = lambda tree: next(node for node in tree.body if getattr(node, "name", "") == name)
        assert ast.dump(select(local)) == ast.dump(select(notebook))
