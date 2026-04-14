from __future__ import annotations

import importlib
import sys
from pathlib import Path


def test_poster_image_path_downloads_remote_candidate(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BLENDER_BIN", "/usr/bin/true")
    monkeypatch.setenv("CHERRYFLASH_ROOT", str(tmp_path))
    monkeypatch.setenv("CHERRYFLASH_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))

    module_name = "scripts.render_mobilefeed_intro_still"
    sys.modules.pop(module_name, None)
    intro = importlib.import_module(module_name)

    cache_dir = tmp_path / "poster-cache"
    monkeypatch.setattr(intro, "POSTER_CACHE_DIR", cache_dir)

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def read(self) -> bytes:
            return b"fake-image-bytes"

    monkeypatch.setattr(intro, "urlopen", lambda req, timeout=20: _Resp())

    poster = intro.Poster(
        event_id=101,
        title="Remote Poster Event",
        date="2026-04-13",
        city="Калининград",
        file_name="7fp9nh.jpg",
        file_candidates=("https://files.catbox.moe/7fp9nh.jpg",),
        time="19:00",
        location_name="Hall One",
    )

    resolved = poster.image_path

    assert resolved == cache_dir / f"{intro.hashlib.sha1(b'https://files.catbox.moe/7fp9nh.jpg').hexdigest()[:16]}.jpg"
    assert resolved.read_bytes() == b"fake-image-bytes"


def test_poster_image_path_downloads_yandex_bucket_candidate(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BLENDER_BIN", "/usr/bin/true")
    monkeypatch.setenv("CHERRYFLASH_ROOT", str(tmp_path))
    monkeypatch.setenv("CHERRYFLASH_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))

    module_name = "scripts.render_mobilefeed_intro_still"
    sys.modules.pop(module_name, None)
    intro = importlib.import_module(module_name)

    cache_dir = tmp_path / "poster-cache"
    monkeypatch.setattr(intro, "POSTER_CACHE_DIR", cache_dir)

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def read(self) -> bytes:
            return b"yandex-image-bytes"

    monkeypatch.setattr(intro, "urlopen", lambda req, timeout=20: _Resp())

    url = (
        "https://storage.yandexcloud.net/cherryflash-backfill/posters/7fp9nh.webp"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=test"
    )
    poster = intro.Poster(
        event_id=202,
        title="Yandex Poster Event",
        date="2026-04-13",
        city="Калининград",
        file_name="7fp9nh.webp",
        file_candidates=(url,),
        time="19:00",
        location_name="Hall Two",
    )

    resolved = poster.image_path

    expected = cache_dir / f"{intro.hashlib.sha1(url.encode('utf-8')).hexdigest()[:16]}.webp"
    assert resolved == expected
    assert resolved.read_bytes() == b"yandex-image-bytes"
