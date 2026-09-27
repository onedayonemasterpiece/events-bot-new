from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.publish_static_site_preview import (
    PreviewPublishError,
    object_metadata,
    publish_preview,
)


class FakeClient:
    def __init__(self):
        self.calls = []

    def upload_file(self, filename, bucket, key, ExtraArgs):
        self.calls.append((filename, bucket, key, ExtraArgs))


def make_preview(tmp_path: Path, build_id: str = "preview-review-test") -> Path:
    root = tmp_path / build_id
    (root / "__preview").mkdir(parents=True)
    (root / "poisk").mkdir()
    (root / "_astro").mkdir()
    (root / "calendar").mkdir()
    (root / "preview-build.json").write_text(
        json.dumps({"buildId": build_id, "basePath": f"/{build_id}"}),
        encoding="utf-8",
    )
    (root / "__preview" / "index.html").write_text("review", encoding="utf-8")
    (root / "poisk" / "index.html").write_text("search", encoding="utf-8")
    (root / "_astro" / "app.js").write_text("x", encoding="utf-8")
    (root / "calendar" / "event.ics").write_text("BEGIN:VCALENDAR", encoding="utf-8")
    return root


def test_publish_is_bounded_to_named_preview_prefix(tmp_path: Path) -> None:
    root = make_preview(tmp_path)
    client = FakeClient()
    result = publish_preview(root, root.name, client=client, bucket="bucket")
    assert result["objects"] == len(client.calls)
    assert result["objects"] >= 5
    assert all(key.startswith(root.name + "/") for _, _, key, _ in client.calls)
    assert {bucket for _, bucket, _, _ in client.calls} == {"bucket"}
    astro = next(call for call in client.calls if "/_astro/" in call[2])
    assert astro[3]["CacheControl"] == "public, max-age=31536000, immutable"
    ics = next(call for call in client.calls if call[2].endswith(".ics"))
    assert ics[3]["ContentType"] == "text/calendar; charset=utf-8"


def test_publish_rejects_manifest_identity_mismatch(tmp_path: Path) -> None:
    root = make_preview(tmp_path)
    (root / "preview-build.json").write_text(
        json.dumps({"buildId": "preview-other", "basePath": f"/{root.name}"}),
        encoding="utf-8",
    )
    with pytest.raises(PreviewPublishError, match="buildId mismatch"):
        publish_preview(root, root.name, client=FakeClient(), bucket="bucket")


def test_metadata_matches_existing_preview_policy() -> None:
    assert object_metadata("_astro/app.js")["CacheControl"].endswith("immutable")
    assert object_metadata("service-share/current/manifest.json")["CacheControl"] == "no-cache, max-age=0"
    assert object_metadata("manifest.webmanifest")["ContentType"].startswith("application/manifest+json")
    assert object_metadata("event.ics")["ContentDisposition"] == 'inline; filename="event.ics"'
