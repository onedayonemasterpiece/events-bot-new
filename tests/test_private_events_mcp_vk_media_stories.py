from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from private_events_mcp.social_workspace import (
    MediaRole,
    validate_prepare_request,
    validate_read_request,
)
from private_events_mcp_vk_adapter import (
    VK_API_VERSION,
    VK_FIXED_METHOD_ALLOWLIST,
    VK_OPERATION_ACTORS,
    VKActor,
    VKWorkspaceAdapter,
    VKWorkspaceError,
    _validated_vk_https_url,
)
from private_events_mcp_vk_transport import (
    VKMediaTransportError,
)
from private_events_mcp_vk_transport import (
    _validated_url as _validated_vk_transport_url,
)
from private_events_mcp_vk_upload import (
    VKAssetMaterialization,
    VKMultipartUploadResult,
    VKStoryMediaMaterialization,
    VKUploadPurpose,
)

OWNER_BINDING = "a" * 64
IMAGE_BYTES = b"verified-image-bytes"
VIDEO_BYTES = b"verified-video-bytes"


class FakeRefs:
    def __init__(self) -> None:
        self.values: dict[tuple[str, str], dict[str, Any]] = {}

    def mint(self, kind: str, native_value: Mapping[str, Any]) -> str:
        encoded = json.dumps(dict(native_value), sort_keys=True, separators=(",", ":"))
        prefix = {"target": "tgt", "item": "itm", "asset": "ast"}[kind]
        ref = prefix + "_" + hashlib.sha256((kind + encoded).encode()).hexdigest()[:24]
        self.values[(kind, ref)] = dict(native_value)
        return ref

    def resolve(self, kind: str, opaque_ref: str) -> Mapping[str, Any]:
        return dict(self.values[(kind, opaque_ref)])


class FakeGovernor:
    async def before_call(self, _actor: VKActor, _capability: str) -> None:
        return None

    async def after_call(self, _actor: VKActor, _capability: str, _outcome: str) -> None:
        return None


class FakeCooldown:
    async def ensure_available(self, _actor: VKActor) -> None:
        return None

    async def record_captcha(self, _actor: VKActor) -> None:
        return None

    async def record_success(self, _actor: VKActor) -> None:
        return None


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.denied: set[tuple[VKActor, str]] = set()
        self.fail_method: str | None = None

    def permits(self, actor: VKActor, capability: str) -> bool:
        return (actor, capability) not in self.denied

    async def invoke(self, **call: Any) -> Any:
        self.calls.append(call)
        method = call["method"]
        if method == self.fail_method:
            raise RuntimeError("provider secret must not escape")
        if method == "photos.getWallUploadServer":
            return {"upload_url": "https://pu.vk.com/upload.php?sig=signed"}
        if method == "photos.saveWallPhoto":
            return [{"id": 55, "owner_id": -101, "access_key": "safe_key"}]
        if method == "wall.post":
            return {"post_id": 501}
        if method in {"stories.getPhotoUploadServer", "stories.getVideoUploadServer"}:
            return {"upload_url": "https://pu.vk.com/story-upload?sig=signed"}
        if method == "stories.save":
            return {"count": 1, "items": [{"id": 71, "owner_id": -101}]}
        if method == "stories.getById":
            return {
                "count": 1,
                "items": [
                    {
                        "id": 71,
                        "owner_id": -101,
                        "date": 1_800_000_000,
                        "expires_at": 1_900_000_000,
                        "video": {
                            "width": 1080,
                            "height": 1920,
                            "files": {"mp4_720": "https://cdn.userapi.com/story.mp4"},
                        },
                    }
                ],
                "profiles": [],
                "groups": [],
            }
        if method == "stories.get":
            return {
                "count": 1,
                "items": [
                    {
                        "type": "stories",
                        "owner_id": -101,
                        "stories": [
                            {
                                "id": 71,
                                "owner_id": -101,
                                "date": 1_800_000_000,
                                "expires_at": 1_900_000_000,
                                "views": 99,
                                "viewers": [{"user_id": 777}],
                                "photo": {
                                    "sizes": [
                                        {
                                            "url": "https://sun9-1.userapi.com/story.jpg?sig=1",
                                            "width": 1080,
                                            "height": 1920,
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ],
                "profiles": [{"id": 777, "first_name": "Private"}],
                "groups": [],
            }
        if method == "stories.getStats":
            return {
                "views": {"count": 45, "state": "on"},
                "likes": {"count": 3, "state": "on"},
                "replies": {"count": 1, "state": "hidden"},
                "shares": {"count": 2, "state": "off"},
                "subscribers": {"count": 4, "state": "on"},
            }
        if method == "stats.get":
            return [
                {
                    "period_from": 1_800_000_000,
                    "period_to": 1_800_086_399,
                    "visitors": {"views": 20, "visitors": 12, "cities": [{"name": "private"}]},
                    "activity": {"likes": 4, "comments": 2, "copies": 1},
                },
                {
                    "period_from": 1_800_086_400,
                    "period_to": 1_800_172_799,
                    "visitors": {"views": 30, "visitors": 19},
                    "activity": {"likes": 5, "comments": 3, "copies": 2},
                },
            ]
        if method == "stories.delete":
            return 1
        raise AssertionError(f"unexpected fixed call {method}")


class FakeAssetReader:
    def __init__(self, by_storage_ref: Mapping[str, bytes]) -> None:
        self.by_storage_ref = dict(by_storage_ref)
        self.calls: list[tuple[str, str]] = []

    async def open_verified(self, storage_ref: str, owner_binding: str) -> VKAssetMaterialization:
        self.calls.append((storage_ref, owner_binding))
        content = self.by_storage_ref[storage_ref]
        mime = "video/mp4" if storage_ref.endswith("video") else "image/jpeg"
        return VKAssetMaterialization(
            storage_ref=storage_ref,
            owner_binding=owner_binding,
            content_digest="sha256:" + hashlib.sha256(content).hexdigest(),
            mime_type=mime,
            byte_length=len(content),
            content=content,
        )


class FakeMultipart:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def upload(self, **call: Any) -> VKMultipartUploadResult:
        self.calls.append(call)
        if call["purpose"] is VKUploadPurpose.WALL_PHOTO:
            return VKMultipartUploadResult(server=321, photo="opaque-photo-json", upload_hash="upload-hash")
        return VKMultipartUploadResult(story_upload_result='{"upload_result":"opaque"}')


class FakeStoryMediaReader:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def fetch_vk_cdn(self, **call: Any) -> VKStoryMediaMaterialization:
        self.calls.append(call)
        content = b"story-visual"
        return VKStoryMediaMaterialization(
            mime_type="image/jpeg",
            content_digest="sha256:" + hashlib.sha256(content).hexdigest(),
            byte_length=len(content),
            content=content,
        )


def verified_asset(storage_ref: str, content: bytes, mime_type: str) -> SimpleNamespace:
    return SimpleNamespace(
        storage_ref=storage_ref,
        owner_binding=OWNER_BINDING,
        content_digest="sha256:" + hashlib.sha256(content).hexdigest(),
        mime_type=mime_type,
        byte_length=len(content),
        expires_at=int(datetime.now(timezone.utc).timestamp()) + 3600,
        width=1080,
        height=1920,
    )


def build_adapter():
    refs = FakeRefs()
    transport = FakeTransport()
    asset_reader = FakeAssetReader(
        {
            "ing_" + "i" * 24: IMAGE_BYTES,
            "ing_" + "v" * 19 + "video": VIDEO_BYTES,
        }
    )
    multipart = FakeMultipart()
    story_reader = FakeStoryMediaReader()
    adapter = VKWorkspaceAdapter(
        transport=transport,
        refs=refs,
        governor=FakeGovernor(),
        cooldown=FakeCooldown(),
        sanitize_text=lambda value: value,
        asset_reader=asset_reader,
        multipart_transport=multipart,
        story_media_reader=story_reader,
        timeout_seconds=0.1,
    )
    target_ref = refs.mint(
        "target", {"kind": "community", "group_id": 101, "owner_id": -101}
    )
    return adapter, refs, transport, asset_reader, multipart, story_reader, target_ref


@pytest.mark.asyncio
async def test_verified_wall_photo_upload_is_fixed_opaque_and_idempotent() -> None:
    adapter, _refs, transport, reader, multipart, _story_reader, target_ref = build_adapter()
    storage_ref = "ing_" + "i" * 24
    asset_ref = await adapter.stage_asset(
        verified_asset(storage_ref, IMAGE_BYTES, "image/jpeg"), role=MediaRole.IMAGE
    )
    assert asset_ref.startswith("ast_")
    assert reader.calls == [] and transport.calls == []
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "publish",
            "idempotency_key": "wall-media-idempotent-001",
            "target_ref": target_ref,
            "content": {"text": "Photo", "media": [{"asset_ref": asset_ref, "role": "image"}]},
        }
    )
    receipt = await adapter.execute(intent)
    assert receipt["status"] == "succeeded" and receipt["item_ref"].startswith("itm_")
    assert "owner_id" not in json.dumps(receipt)
    assert await adapter.execute(intent) == receipt
    assert reader.calls == [(storage_ref, OWNER_BINDING)]
    assert [call["method"] for call in transport.calls] == [
        "photos.getWallUploadServer",
        "photos.saveWallPhoto",
        "wall.post",
    ]
    assert transport.calls[0]["params"] == {"group_id": 101}
    assert transport.calls[1]["params"] == {
        "group_id": 101,
        "photo": "opaque-photo-json",
        "server": 321,
        "hash": "upload-hash",
    }
    assert transport.calls[2]["params"]["attachments"] == "photo-101_55_safe_key"
    assert len(multipart.calls) == 1
    assert multipart.calls[0]["purpose"] is VKUploadPurpose.WALL_PHOTO
    assert multipart.calls[0]["content"] == IMAGE_BYTES
    assert all(call["version"] == VK_API_VERSION for call in transport.calls)


@pytest.mark.asyncio
async def test_denial_and_integrity_failure_happen_before_provider_attempt() -> None:
    adapter, _refs, transport, reader, _multipart, _story_reader, target_ref = build_adapter()
    storage_ref = "ing_" + "i" * 24
    asset_ref = await adapter.stage_asset(
        verified_asset(storage_ref, IMAGE_BYTES, "image/jpeg"), role=MediaRole.IMAGE
    )
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "publish",
            "idempotency_key": "wall-media-denied-001",
            "target_ref": target_ref,
            "content": {"media": [{"asset_ref": asset_ref, "role": "image"}]},
        }
    )
    transport.denied.add((VKActor.COMMUNITY_EDITOR, "media_upload"))
    denied = await adapter.execute(intent)
    assert denied["status"] == "failed"
    assert denied["error_code"] == "actor_capability_denied"
    assert reader.calls == [] and transport.calls == []

    adapter, _refs, transport, reader, _multipart, _story_reader, target_ref = build_adapter()
    asset_ref = await adapter.stage_asset(
        verified_asset(storage_ref, b"different-declared-bytes", "image/jpeg"),
        role=MediaRole.IMAGE,
    )
    bad = validate_prepare_request(
        {
            "platform": "vk",
            "action": "publish",
            "idempotency_key": "wall-media-integrity-001",
            "target_ref": target_ref,
            "content": {"media": [{"asset_ref": asset_ref, "role": "image"}]},
        }
    )
    receipt = await adapter.execute(bad)
    assert receipt["status"] == "failed" and receipt["error_code"] == "asset_integrity_failed"
    assert transport.calls == [] and len(reader.calls) == 1


@pytest.mark.asyncio
async def test_story_image_upload_save_readback_and_unknown_after_attempt() -> None:
    adapter, _refs, transport, reader, multipart, _story_reader, target_ref = build_adapter()
    storage_ref = "ing_" + "i" * 24
    asset_ref = await adapter.stage_asset(
        verified_asset(storage_ref, IMAGE_BYTES, "image/jpeg"), role=MediaRole.IMAGE
    )
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "story",
            "idempotency_key": "story-video-idempotent-001",
            "target_ref": target_ref,
            "content": {"media": [{"asset_ref": asset_ref, "role": "image"}]},
        }
    )
    receipt = await adapter.execute(intent)
    assert receipt["status"] == "succeeded"
    assert [call["method"] for call in transport.calls] == [
        "stories.getPhotoUploadServer",
        "stories.save",
        "stories.getById",
    ]
    assert transport.calls[0]["params"] == {"group_id": 101, "add_to_news": 1}
    assert transport.calls[1]["params"] == {
        "upload_results": ['{"upload_result":"opaque"}']
    }
    assert transport.calls[2]["params"] == {"stories": ["-101_71"]}
    assert multipart.calls[0]["purpose"] is VKUploadPurpose.STORY_PHOTO
    assert reader.calls == [(storage_ref, OWNER_BINDING)]
    assert await adapter.execute(intent) == receipt
    assert len(transport.calls) == 3

    adapter, _refs, transport, _reader, _multipart, _story_reader, target_ref = build_adapter()
    asset_ref = await adapter.stage_asset(
        verified_asset(storage_ref, IMAGE_BYTES, "image/jpeg"), role=MediaRole.IMAGE
    )
    transport.fail_method = "stories.getPhotoUploadServer"
    uncertain = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "story",
                "idempotency_key": "story-provider-attempt-001",
                "target_ref": target_ref,
                "content": {"media": [{"asset_ref": asset_ref, "role": "image"}]},
            }
        )
    )
    assert uncertain["status"] == "outcome_unknown"
    assert uncertain["error_code"] == "outcome_unknown" and uncertain["retry_safe"] is False


@pytest.mark.asyncio
async def test_official_story_shape_visual_read_and_nested_aggregate_stats() -> None:
    adapter, refs, transport, _reader, _multipart, story_reader, target_ref = build_adapter()
    page = await adapter.read(
        validate_read_request(
            {"platform": "vk", "operation": "list_stories", "target_ref": target_ref, "limit": 5}
        )
    )
    assert len(page["results"]) == 1
    story = page["results"][0]
    assert story["kind"] == "story" and len(story["media"]) == 1
    serialized = json.dumps(page)
    assert "userapi.com" not in serialized
    assert "viewer" not in serialized and "Private" not in serialized and "777" not in serialized
    assert transport.calls[0]["method"] == "stories.get"
    assert transport.calls[0]["params"] == {"owner_id": -101}

    visual = await adapter.read_asset(
        story["media"][0], owner_binding=OWNER_BINDING, max_bytes=1024
    )
    assert visual.content == b"story-visual"
    assert story_reader.calls[0]["source_url"].startswith("https://sun9-1.userapi.com/")
    assert "source_url" not in json.dumps(refs.resolve("item", story["item_ref"]))

    stats = await adapter.read(
        validate_read_request(
            {"platform": "vk", "operation": "get_statistics", "item_ref": story["item_ref"]}
        )
    )
    assert stats["basic_metrics"] == {
        "views": 45,
        "reactions": 3,
        "comments": 1,
        "shares": 2,
    }
    assert set(stats["basic_metrics"]) == {"views", "reactions", "comments", "shares"}


def test_vk_story_media_accepts_provider_observed_okcdn_host_only() -> None:
    observed = "https://vkvd740.okcdn.ru/?sig=opaque"
    assert _validated_vk_https_url(observed) == observed
    assert _validated_vk_transport_url(observed) == ("vkvd740.okcdn.ru", 443)

    for malicious in (
        "https://vkvd740.okcdn.ru.evil.example/story.mp4",
        "https://okcdn.ru@evil.example/story.mp4",
        "http://vkvd740.okcdn.ru/story.mp4",
    ):
        with pytest.raises(VKWorkspaceError):
            _validated_vk_https_url(malicious)
        with pytest.raises(VKMediaTransportError):
            _validated_vk_transport_url(malicious)


@pytest.mark.asyncio
async def test_community_stats_use_5199_timestamps_and_nested_periods() -> None:
    adapter, _refs, transport, _reader, _multipart, _story_reader, target_ref = build_adapter()
    result = await adapter.read(
        validate_read_request(
            {
                "platform": "vk",
                "operation": "get_statistics",
                "target_ref": target_ref,
                "date_from": "2027-01-15",
                "date_to": "2027-01-16",
            }
        )
    )
    assert result["basic_metrics"] == {
        "views": 50,
        "reactions": 9,
        "comments": 5,
        "shares": 3,
    }
    call = transport.calls[-1]
    assert call["method"] == "stats.get"
    assert set(call["params"]) == {"group_id", "interval", "timestamp_from", "timestamp_to"}
    assert call["params"]["interval"] == "day"
    assert "date_from" not in call["params"] and "date_to" not in call["params"]
    assert "cities" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malicious_url",
    [
        "https://vk.com\n.evil.example/story.jpg",
        "https://vk.com\t@evil.example/story.jpg",
        "https://user@vk.com/story.jpg",
        "https://vk.com:444/story.jpg",
        "https://vk.com/story.jpg#fragment",
        "https://vk.com\\@evil.example/story.jpg",
    ],
)
async def test_story_media_rejects_noncanonical_or_ambiguous_urls(malicious_url: str) -> None:
    adapter, _refs, transport, _reader, _multipart, _story_reader, target_ref = build_adapter()

    async def malicious_invoke(**call: Any) -> Any:
        transport.calls.append(call)
        return {
            "count": 1,
            "items": [
                {
                    "type": "stories",
                    "owner_id": -101,
                    "stories": [
                        {
                            "id": 71,
                            "owner_id": -101,
                            "photo": {"sizes": [{"url": malicious_url, "width": 1, "height": 1}]},
                        }
                    ],
                }
            ],
        }

    transport.invoke = malicious_invoke  # type: ignore[method-assign]
    with pytest.raises(VKWorkspaceError, match="provider_media_invalid"):
        await adapter.read(
            validate_read_request(
                {"platform": "vk", "operation": "list_stories", "target_ref": target_ref}
            )
        )


@pytest.mark.asyncio
async def test_stage_rejects_paths_urls_and_story_delete_is_fixed() -> None:
    adapter, refs, transport, _reader, _multipart, _story_reader, _target_ref = build_adapter()
    for storage_ref in ("/tmp/private.jpg", "https://files.example/private.jpg", "med_" + "x" * 24):
        with pytest.raises(VKWorkspaceError, match="verified_asset_invalid"):
            await adapter.stage_asset(
                verified_asset(storage_ref, IMAGE_BYTES, "image/jpeg"), role=MediaRole.IMAGE
            )
    with pytest.raises(VKWorkspaceError, match="asset_role_unsupported"):
        await adapter.stage_asset(
            verified_asset("ing_" + "v" * 24, VIDEO_BYTES, "video/mp4"),
            role=MediaRole.VIDEO,
        )
    capabilities = await adapter.capabilities(None)
    assert "video" not in capabilities["content_features"]
    story_ref = refs.mint("item", {"kind": "story", "owner_id": -101, "story_id": 71})
    receipt = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "delete",
                "idempotency_key": "delete-story-fixed-001",
                "item_ref": story_ref,
            }
        )
    )
    assert receipt["status"] == "succeeded"
    assert transport.calls[-1]["method"] == "stories.delete"
    assert transport.calls[-1]["params"] == {"owner_id": -101, "story_id": 71}
    required = {
        "photos.getWallUploadServer",
        "photos.saveWallPhoto",
        "stories.getPhotoUploadServer",
        "stories.getVideoUploadServer",
        "stories.save",
        "stories.getById",
        "stories.delete",
    }
    assert required.issubset(VK_FIXED_METHOD_ALLOWLIST)
    assert VK_OPERATION_ACTORS["wall_photo_upload_server"] == (
        "community_editor",
        "media_upload",
    )
