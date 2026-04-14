import asyncio
import logging
import types

import pytest

import main
import media_dedup
import supabase_storage
import yandex_storage


class DummyResp:
    def __init__(self, status: int, text: str):
        self.status = status
        self._text = text

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummySession:
    def __init__(self, responses):
        self._responses = iter(responses)

    def post(self, url, data):
        return next(self._responses)


@pytest.mark.asyncio
async def test_upload_images_catbox_ok(monkeypatch):
    monkeypatch.setenv("UPLOAD_IMAGES_SUPABASE_MODE", "off")
    main.CATBOX_ENABLED = True
    resp = DummyResp(200, "http://cat/1.png")
    monkeypatch.setattr(main, "get_http_session", lambda: DummySession([resp]))
    monkeypatch.setattr(main, "detect_image_type", lambda *a, **k: "jpeg")
    urls, msg = await main.upload_images([(b"1", "a.png")])
    assert urls == ["http://cat/1.png"]
    assert "ok" in msg


@pytest.mark.asyncio
async def test_upload_images_fail(monkeypatch):
    monkeypatch.setenv("UPLOAD_IMAGES_SUPABASE_MODE", "off")
    main.CATBOX_ENABLED = True
    resp = DummyResp(500, "err")
    monkeypatch.setattr(main, "get_http_session", lambda: DummySession([resp, resp, resp]))
    async def dummy_sleep(_):
        return None
    monkeypatch.setattr(asyncio, "sleep", dummy_sleep)
    monkeypatch.setattr(main, "detect_image_type", lambda *a, **k: "jpeg")
    urls, msg = await main.upload_images([(b"1", "a.png")])
    assert urls == []
    assert "failed" in msg


@pytest.mark.asyncio
async def test_upload_images_catbox_disabled(monkeypatch, caplog):
    monkeypatch.setenv("UPLOAD_IMAGES_SUPABASE_MODE", "off")
    main.CATBOX_ENABLED = False
    caplog.set_level(logging.INFO)
    urls, msg = await main.upload_images([(b"1", "a.png")], event_hint="test")
    assert urls == []
    assert msg == "disabled"
    assert any(
        "poster_upload disabled catbox_enabled=False force=False images=1 event_hint=test"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_upload_images_prefers_yandex_storage(monkeypatch):
    monkeypatch.setenv("UPLOAD_IMAGES_SUPABASE_MODE", "only")
    monkeypatch.setenv("YC_SA_BOT_STORAGE", "access")
    monkeypatch.setenv("YC_SA_BOT_STORAGE_KEY", "secret")
    monkeypatch.setenv("YC_STORAGE_BUCKET", "kenigevents")
    monkeypatch.setattr(main, "CATBOX_ENABLED", True, raising=False)

    monkeypatch.setattr(main, "ensure_jpeg", lambda data, name: (data, name))
    monkeypatch.setattr(main, "detect_image_type", lambda *a, **k: "jpeg")
    monkeypatch.setattr(
        media_dedup,
        "prepare_image_for_supabase",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            dhash_hex="abc123",
            webp_bytes=b"webp-bytes",
        ),
    )
    monkeypatch.setattr(
        media_dedup,
        "build_supabase_poster_object_path",
        lambda dhash_hex, *, prefix, dhash_size: f"{prefix}/dh16/ab/{dhash_hex}.webp",
    )
    monkeypatch.setattr(
        supabase_storage,
        "storage_object_exists_http",
        lambda **_kwargs: False,
    )
    monkeypatch.setattr(
        yandex_storage,
        "upload_yandex_public_bytes",
        lambda data, *, object_path, content_type, bucket=None: (
            f"https://storage.yandexcloud.net/{bucket}/{object_path}"
        ),
    )

    urls, msg = await main.upload_images([(b"raw-image", "a.png")])

    assert urls == [
        "https://storage.yandexcloud.net/kenigevents/p/dh16/ab/abc123.webp"
    ]
    assert msg == "storage_primary"
