from __future__ import annotations

import asyncio
import hashlib
import stat
from dataclasses import dataclass
from io import BytesIO

import pytest
from PIL import Image

from private_events_mcp_media import (
    MediaExpired,
    MediaIngressRejected,
    MediaIntegrityError,
    MediaOwnershipError,
    SecureMediaAssetStore,
)

OWNER_A = hashlib.sha256(b"owner-A").hexdigest()
OWNER_B = hashlib.sha256(b"owner-B").hexdigest()


@dataclass
class File:
    download_url: str = "https://media.example.test/download"
    file_id: str = "file-1"
    mime_type: str | None = "image/png"
    file_name: str | None = "ignored.png"


class Response:
    def __init__(self, chunks, *, status=200, headers=None):
        self._chunks = list(chunks)
        self.status = status
        self.headers = headers or {"Content-Type": "image/png"}

    async def aiter_bytes(self, _size):
        for chunk in self._chunks:
            yield chunk


class Fetcher:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


async def public_resolver(host, port):
    assert port == 443
    return ["93.184.216.34"]


def image_bytes(fmt="PNG", size=(8, 6), color=(20, 40, 60)):
    output = BytesIO()
    Image.new("RGB", size, color).save(output, format=fmt)
    return output.getvalue()


def make_store(
    tmp_path, *, response=None, resolver=public_resolver, clock=None, **kwargs
):
    fetcher = Fetcher(response or Response([image_bytes()]))
    store = SecureMediaAssetStore(
        tmp_path / "media",
        allowed_hosts=["media.example.test"],
        resolver=resolver,
        http_fetch=fetcher,
        clock=clock or (lambda: 1_000),
        **kwargs,
    )
    return store, fetcher


def ingest(store, file=None, **kwargs):
    return asyncio.run(
        store.ingest(
            file or File(),
            owner_binding=kwargs.pop("owner_binding", OWNER_A),
            max_bytes=kwargs.pop("max_bytes", 1024 * 1024),
            expires_at=kwargs.pop("expires_at", 1_500),
            **kwargs,
        )
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://media.example.test/a",
        "https://user@media.example.test/a",
        "https://media.example.test:444/a",
        "https://media.example.test/a#fragment",
        "https://media.example.test/a\nnext",
        "https://media.example.test\\@evil.test/a",
        "https://127.0.0.1/a",
        "https://127.1/a",
        "https://2130706433/a",
        "https://0177.0.0.1/a",
        "https://0x7f000001/a",
        "https://evil.example.test/a",
    ],
)
def test_ssrf_url_matrix_rejected_before_fetch(tmp_path, url):
    store, fetcher = make_store(tmp_path)
    with pytest.raises(MediaIngressRejected):
        ingest(store, File(download_url=url))
    assert fetcher.calls == []


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "224.0.0.1",
        "192.0.2.1",
        "0.0.0.0",
        "::1",
        "fc00::1",
        "fe80::1",
        "ff02::1",
        "::",
    ],
)
def test_dns_must_resolve_exclusively_to_public_addresses(tmp_path, address):
    async def resolver(_host, _port):
        return [address]

    store, fetcher = make_store(tmp_path, resolver=resolver)
    with pytest.raises(MediaIngressRejected, match="DNS"):
        ingest(store)
    assert fetcher.calls == []


def test_mixed_public_private_dns_answer_is_rejected(tmp_path):
    async def resolver(_host, _port):
        return ["93.184.216.34", "127.0.0.1"]

    store, fetcher = make_store(tmp_path, resolver=resolver)
    with pytest.raises(MediaIngressRejected):
        ingest(store)
    assert fetcher.calls == []


def test_dns_is_resolved_for_every_ingest_and_fetch_is_pinned(tmp_path):
    calls = []

    async def resolver(host, port):
        calls.append((host, port))
        return ["93.184.216.34"]

    store, fetcher = make_store(tmp_path, resolver=resolver)
    first = ingest(store, File(file_id="one"))
    second = ingest(store, File(file_id="two"))
    assert len(calls) == 2
    assert first.storage_ref != second.storage_ref
    assert fetcher.calls[0][1]["allowed_ips"] == ("93.184.216.34",)


def test_explicit_wildcard_allowlist_matches_only_subdomains(tmp_path):
    fetcher = Fetcher(Response([image_bytes()]))
    store = SecureMediaAssetStore(
        tmp_path / "media",
        allowed_hosts=["*.example.test"],
        resolver=public_resolver,
        http_fetch=fetcher,
        clock=lambda: 1_000,
    )
    ingest(store, File(download_url="https://cdn.example.test/x"))
    with pytest.raises(MediaIngressRejected):
        ingest(store, File(download_url="https://example.test/x", file_id="two"))


def test_redirect_is_not_followed_and_sensitive_headers_are_never_sent(tmp_path):
    response = Response(
        [], status=302, headers={"Location": "https://media.example.test/next"}
    )
    store, fetcher = make_store(tmp_path, response=response)
    with pytest.raises(MediaIngressRejected, match="redirect"):
        ingest(store)
    assert len(fetcher.calls) == 1
    sent = {key.lower() for key in fetcher.calls[0][1]["headers"]}
    assert not ({"authorization", "cookie", "referer", "referrer"} & sent)


def test_chunked_body_over_limit_is_removed(tmp_path):
    store, _ = make_store(
        tmp_path,
        response=Response(
            [b"a" * 6, b"b" * 6], headers={"Content-Type": "application/octet-stream"}
        ),
        max_asset_bytes=10,
        max_store_bytes=100,
    )
    with pytest.raises(MediaIngressRejected, match="byte limit"):
        ingest(store, max_bytes=100)
    assert not list((tmp_path / "media").glob(".ingress-*"))
    assert not list((tmp_path / "media").glob("*.asset"))


def test_stream_has_a_hard_wall_clock_timeout(tmp_path):
    class SlowResponse(Response):
        async def aiter_bytes(self, _size):
            await asyncio.sleep(0.1)
            yield image_bytes()

    store, _ = make_store(
        tmp_path,
        response=SlowResponse([]),
        timeout_seconds=0.01,
    )
    with pytest.raises(MediaIngressRejected, match="timed out"):
        ingest(store)
    assert not list((tmp_path / "media").glob(".ingress-*"))


def test_claimed_mime_must_match_sniffed_content(tmp_path):
    jpeg = image_bytes("JPEG")
    store, _ = make_store(
        tmp_path,
        response=Response([jpeg], headers={"Content-Type": "image/jpeg"}),
    )
    with pytest.raises(MediaIngressRejected, match="does not match"):
        ingest(store, File(mime_type="image/png"))


def test_image_is_verified_and_contract_metadata_is_returned(tmp_path):
    payload = image_bytes("WEBP", size=(13, 11))
    store, _ = make_store(
        tmp_path,
        response=Response(
            [payload], headers={"Content-Type": "application/octet-stream"}
        ),
    )
    asset = ingest(store, File(mime_type="image/webp"))
    assert asset.storage_ref.startswith("ing_")
    assert "/" not in asset.storage_ref
    assert asset.owner_binding == OWNER_A
    assert asset.content_digest == "sha256:" + hashlib.sha256(payload).hexdigest()
    assert (asset.mime_type, asset.byte_length, asset.width, asset.height) == (
        "image/webp",
        len(payload),
        13,
        11,
    )


def test_unsupported_video_is_explicitly_rejected(tmp_path):
    mp4 = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64
    store, _ = make_store(
        tmp_path,
        response=Response([mp4], headers={"Content-Type": "video/mp4"}),
    )
    with pytest.raises(MediaIngressRejected, match="image validation"):
        ingest(store, File(mime_type="video/mp4"))


def test_dimensions_and_pixel_bomb_limits_precede_full_decode(tmp_path):
    payload = image_bytes(size=(17, 9))
    store, _ = make_store(
        tmp_path,
        response=Response([payload]),
        max_width=16,
        max_height=16,
        max_pixels=100,
    )
    with pytest.raises(MediaIngressRejected, match="dimensions"):
        ingest(store)


def test_owner_binding_open_and_digest_reverification(tmp_path):
    payload = image_bytes()
    store, _ = make_store(tmp_path, response=Response([payload]))
    asset = ingest(store)
    with pytest.raises(MediaOwnershipError):
        store.verify(asset.storage_ref, OWNER_B)
    with store.open_verified(asset.storage_ref, OWNER_A) as (reader, opened):
        assert reader.read() == payload
        assert opened.storage_ref == asset.storage_ref
        assert not hasattr(opened, "_path")


def test_tampered_file_is_rehashed_and_rejected(tmp_path):
    store, _ = make_store(tmp_path)
    asset = ingest(store)
    stored_path = next((tmp_path / "media").glob("*.asset"))
    stored_path.chmod(0o600)
    stored_path.write_bytes(stored_path.read_bytes() + b"tamper")
    with pytest.raises(MediaIntegrityError, match="digest"):
        store.verify(asset.storage_ref, OWNER_A)


def test_expiry_and_cleanup_remove_manifest_and_asset(tmp_path):
    now = [1_000]
    store, _ = make_store(tmp_path, clock=lambda: now[0], ttl_seconds=600)
    asset = ingest(store, expires_at=1_500)
    now[0] = 1_500
    with pytest.raises(MediaExpired):
        store.verify(asset.storage_ref, OWNER_A)
    assert store.cleanup_expired() == 1
    assert not list((tmp_path / "media").glob("*.asset"))


def test_ttl_cannot_exceed_configured_or_global_day_cap(tmp_path):
    store, _ = make_store(tmp_path, ttl_seconds=300)
    with pytest.raises(MediaIngressRejected, match="expiry"):
        ingest(store, expires_at=1_301)
    with pytest.raises(ValueError):
        SecureMediaAssetStore(
            tmp_path / "bad", allowed_hosts=["x.test"], ttl_seconds=86_401
        )


def test_private_directory_control_files_and_asset_permissions(tmp_path):
    store, _ = make_store(tmp_path)
    asset = ingest(store)
    root = tmp_path / "media"
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / ".owner_hmac.key").stat().st_mode) == 0o600
    assert stat.S_IMODE((root / ".manifest.sqlite3").stat().st_mode) == 0o600
    media_file = next(root.glob("*.asset"))
    assert stat.S_IMODE(media_file.stat().st_mode) == 0o400
    assert media_file.stat().st_nlink == 1
    assert "ing_" not in repr(store.verify(asset.storage_ref, OWNER_A))


def test_root_manifest_and_asset_symlinks_are_not_followed(tmp_path):
    target_dir = tmp_path / "elsewhere"
    target_dir.mkdir()
    root_link = tmp_path / "root-link"
    root_link.symlink_to(target_dir, target_is_directory=True)
    with pytest.raises(MediaIntegrityError):
        SecureMediaAssetStore(root_link, allowed_hosts=["media.example.test"])

    manifest_root = tmp_path / "manifest-root"
    manifest_root.mkdir()
    target = tmp_path / "sentinel-target"
    target.write_text("do not touch")
    (manifest_root / ".manifest.sqlite3").symlink_to(target)
    with pytest.raises(MediaIntegrityError):
        SecureMediaAssetStore(manifest_root, allowed_hosts=["media.example.test"])
    assert target.read_text() == "do not touch"

    store, _ = make_store(tmp_path / "asset-case")
    asset = ingest(store)
    media_file = next((tmp_path / "asset-case" / "media").glob("*.asset"))
    media_file.unlink()
    media_file.symlink_to(target)
    with pytest.raises(MediaIntegrityError):
        store.verify(asset.storage_ref, OWNER_A)
    assert target.read_text() == "do not touch"


def test_url_file_id_file_name_and_owner_sentinels_are_not_persisted(tmp_path):
    sentinel = "SUPER_SECRET_SENTINEL_9f6d83"
    store, _ = make_store(tmp_path)
    asset = ingest(
        store,
        File(
            download_url=f"https://media.example.test/download?token={sentinel}",
            file_id=f"raw-id-{sentinel}",
            file_name=f"raw-name-{sentinel}.png",
        ),
        owner_binding=OWNER_A,
    )
    assert asset.owner_binding == OWNER_A
    for path in (tmp_path / "media").iterdir():
        if path.is_file():
            assert sentinel.encode() not in path.read_bytes()


def test_aggregate_store_quota_is_transactionally_enforced(tmp_path):
    payload = image_bytes()
    per_file = len(payload)
    store, _ = make_store(
        tmp_path,
        response=Response([payload]),
        max_asset_bytes=per_file,
        max_store_bytes=per_file * 2 - 1,
    )
    ingest(store, File(file_id="one"), max_bytes=per_file)
    with pytest.raises(MediaIngressRejected, match="capacity"):
        ingest(store, File(file_id="two"), max_bytes=per_file)
    assert len(list((tmp_path / "media").glob("*.asset"))) == 1


def test_empty_allowlist_and_invalid_limits_fail_closed(tmp_path):
    with pytest.raises(ValueError, match="allowed_hosts"):
        SecureMediaAssetStore(tmp_path / "empty", allowed_hosts=[])
    with pytest.raises(ValueError, match="numeric"):
        SecureMediaAssetStore(tmp_path / "numeric", allowed_hosts=["2130706433"])
    with pytest.raises(ValueError, match="byte limits"):
        SecureMediaAssetStore(
            tmp_path / "limits",
            allowed_hosts=["media.example.test"],
            max_asset_bytes=100,
            max_store_bytes=99,
        )


@pytest.mark.parametrize(
    "owner_binding",
    ["", "owner-A", "A" * 64, "a" * 63, "a" * 65, "g" * 64],
)
def test_owner_binding_is_exact_lowercase_sha256(tmp_path, owner_binding):
    store, fetcher = make_store(tmp_path)
    with pytest.raises(MediaOwnershipError, match="owner binding"):
        ingest(store, owner_binding=owner_binding)
    assert fetcher.calls == []


def test_root_keyword_alias_is_supported(tmp_path):
    store = SecureMediaAssetStore(
        root=tmp_path / "media",
        allowed_hosts=["media.example.test"],
        resolver=public_resolver,
        http_fetch=Fetcher(Response([image_bytes()])),
        clock=lambda: 1_000,
    )
    assert ingest(store).storage_ref.startswith("ing_")
