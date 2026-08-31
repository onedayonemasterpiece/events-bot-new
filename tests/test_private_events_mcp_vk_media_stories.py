from __future__ import annotations

import hashlib
import gzip
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest

import private_events_mcp_vk_transport as vk_transport_module

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
    SecureVKMultipartTransport,
    VKMediaTransportError,
)
from private_events_mcp_vk_transport import _session_for as _vk_session_for
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
PNG_STORAGE_REFS = tuple("ing_" + character * 24 for character in "pqrs")
PNG_BYTES = tuple(f"verified-png-{index}".encode() for index in range(4))


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
        self.last_wall_post: dict[str, Any] = {}

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
            self.last_wall_post = dict(call["params"])
            return {"post_id": 501}
        if method == "wall.getById":
            return [{
                "id": 501,
                "owner_id": -101,
                "date": 1_800_000_000,
                "text": self.last_wall_post.get("message", ""),
                "attachments": [{"type": "photo", "photo": {"id": 55}}],
            }]
        if method == "wall.get" and call["params"].get("filter") == "postponed":
            return {
                "items": [
                    {
                        "id": 501,
                        "owner_id": -101,
                        "date": self.last_wall_post.get("publish_date"),
                        "text": self.last_wall_post.get("message", ""),
                        "attachments": [
                            {"type": "photo", "photo": {"id": 55}}
                            for attachment in str(
                                self.last_wall_post.get("attachments") or ""
                            ).split(",")
                            if attachment.startswith("photo")
                        ],
                    }
                ]
            }
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
    def __init__(
        self,
        by_storage_ref: Mapping[str, bytes],
        mime_by_storage_ref: Mapping[str, str] | None = None,
    ) -> None:
        self.by_storage_ref = dict(by_storage_ref)
        self.mime_by_storage_ref = dict(mime_by_storage_ref or {})
        self.calls: list[tuple[str, str]] = []

    async def open_verified(self, storage_ref: str, owner_binding: str) -> VKAssetMaterialization:
        self.calls.append((storage_ref, owner_binding))
        content = self.by_storage_ref[storage_ref]
        mime = self.mime_by_storage_ref.get(
            storage_ref,
            "video/mp4" if storage_ref.endswith("video") else "image/jpeg",
        )
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
        self.fail = False
        self.wall_result: VKMultipartUploadResult | None = None

    async def upload(self, **call: Any) -> VKMultipartUploadResult:
        self.calls.append(call)
        if self.fail:
            raise VKMediaTransportError("sanitized multipart failure")
        if call["purpose"] is VKUploadPurpose.WALL_PHOTO:
            if self.wall_result is not None:
                return self.wall_result
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


def build_adapter(*, attempt_recorder=None):
    refs = FakeRefs()
    transport = FakeTransport()
    asset_reader = FakeAssetReader(
        {
            "ing_" + "i" * 24: IMAGE_BYTES,
            "ing_" + "v" * 19 + "video": VIDEO_BYTES,
            **dict(zip(PNG_STORAGE_REFS, PNG_BYTES, strict=True)),
        },
        {storage_ref: "image/png" for storage_ref in PNG_STORAGE_REFS},
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
        attempt_recorder=attempt_recorder,
        timeout_seconds=0.1,
    )
    target_ref = refs.mint(
        "target", {"kind": "community", "group_id": 101, "owner_id": -101}
    )
    return adapter, refs, transport, asset_reader, multipart, story_reader, target_ref


@pytest.mark.asyncio
async def test_verified_wall_photo_upload_is_fixed_opaque_and_idempotent() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.events: list[tuple[str, dict[str, Any]]] = []

        def record(self, operation_ref: str, event: Mapping[str, Any]) -> None:
            self.events.append((operation_ref, dict(event)))

    recorder = Recorder()
    adapter, _refs, transport, reader, multipart, _story_reader, target_ref = build_adapter(
        attempt_recorder=recorder
    )
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
        "wall.getById",
    ]
    assert transport.calls[0]["params"] == {"group_id": 101}
    assert transport.calls[1]["params"] == {
        "group_id": 101,
        "photo": "opaque-photo-json",
        "server": 321,
        "hash": "upload-hash",
    }
    assert transport.calls[2]["params"]["attachments"] == "photo-101_55_safe_key"
    assert receipt["read_after_write"]["verified"] is True
    stages = [
        (event["stage"], event["phase"])
        for _operation_ref, event in recorder.events
    ]
    assert stages == [
        ("wall_photo_upload_server", "started"),
        ("wall_photo_upload_server", "finished"),
        ("wall_photo_multipart", "started"),
        ("wall_photo_multipart", "finished"),
        ("wall_photo_save", "started"),
        ("wall_photo_save", "finished"),
        ("wall_post", "started"),
        ("wall_post", "finished"),
    ]
    saved = recorder.events[5][1]["provider_result"]
    posted = recorder.events[7][1]["provider_result"]
    assert saved == {"photo_id": 55, "photo_owner_id": -101}
    assert posted == {"post_id": 501}
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
    transport.denied.add((VKActor.MEDIA_EDITOR, "media_upload"))
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
async def test_multipart_failure_before_wall_post_is_definite_and_retry_safe() -> None:
    adapter, _refs, transport, _reader, multipart, _story_reader, target_ref = build_adapter()
    storage_ref = "ing_" + "i" * 24
    asset_ref = await adapter.stage_asset(
        verified_asset(storage_ref, IMAGE_BYTES, "image/jpeg"), role=MediaRole.IMAGE
    )
    multipart.fail = True
    result = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "publish",
                "idempotency_key": "wall-multipart-definite-001",
                "target_ref": target_ref,
                "content": {
                    "text": "Photo",
                    "media": [{"asset_ref": asset_ref, "role": "image"}],
                },
            }
        )
    )
    assert [call["method"] for call in transport.calls] == [
        "photos.getWallUploadServer"
    ]
    assert result == {
        "platform": "vk",
        "operation_ref": result["operation_ref"],
        "action": "publish",
        "status": "failed",
        "retry_safe": True,
        "error_code": "media_upload_failed",
        "stage": "wall_photo_multipart",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_result",
    [
        VKMultipartUploadResult(server=321, photo="", upload_hash="upload-hash"),
        VKMultipartUploadResult(server=321, photo="opaque-photo", upload_hash=""),
        VKMultipartUploadResult(server=None, photo="opaque-photo", upload_hash="upload-hash"),
    ],
)
async def test_invalid_wall_receipt_is_exact_safe_prewall_failure(
    invalid_result: VKMultipartUploadResult,
) -> None:
    class Recorder:
        def __init__(self) -> None:
            self.events: list[dict[str, Any]] = []

        def record(self, _operation_ref: str, event: Mapping[str, Any]) -> None:
            self.events.append(dict(event))

    recorder = Recorder()
    adapter, _refs, transport, _reader, multipart, _story_reader, target_ref = (
        build_adapter(attempt_recorder=recorder)
    )
    asset_ref = await adapter.stage_asset(
        verified_asset("ing_" + "i" * 24, IMAGE_BYTES, "image/jpeg"),
        role=MediaRole.IMAGE,
    )
    multipart.wall_result = invalid_result

    result = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "schedule",
                "idempotency_key": "invalid-wall-receipt-" + str(len(recorder.events)),
                "target_ref": target_ref,
                "schedule_at": "2030-01-01T12:00:00Z",
                "content": {
                    "text": "Four photos later",
                    "media": [{"asset_ref": asset_ref, "role": "image"}],
                },
            }
        )
    )

    assert result["status"] == "failed"
    assert result["retry_safe"] is True
    assert result["error_code"] == "media_upload_response_invalid"
    assert result["stage"] == "wall_photo_multipart"
    assert [call["method"] for call in transport.calls] == [
        "photos.getWallUploadServer"
    ]
    finished = next(
        event
        for event in recorder.events
        if event["stage"] == "wall_photo_multipart"
        and event["phase"] == "finished"
    )
    evidence = finished["provider_result"]
    assert evidence["image_ordinal"] == 1
    assert evidence["mutation_boundary_reached"] is False
    assert evidence["photo_field"]["type"] in {"str", "NoneType"}
    assert evidence["hash_field"]["type"] in {"str", "NoneType"}
    assert "opaque-photo" not in json.dumps(evidence)
    assert "upload-hash" not in json.dumps(evidence)


@pytest.mark.asyncio
async def test_four_pngs_upload_and_save_sequentially_before_one_scheduled_wall_post() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.events: list[dict[str, Any]] = []

        def record(self, _operation_ref: str, event: Mapping[str, Any]) -> None:
            self.events.append(dict(event))

    recorder = Recorder()
    adapter, _refs, transport, _reader, multipart, _story_reader, target_ref = (
        build_adapter(attempt_recorder=recorder)
    )
    asset_refs = [
        await adapter.stage_asset(
            verified_asset(storage_ref, content, "image/png"),
            role=MediaRole.IMAGE,
        )
        for storage_ref, content in zip(PNG_STORAGE_REFS, PNG_BYTES, strict=True)
    ]
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "schedule",
            "idempotency_key": "four-png-scheduled-wall-001",
            "target_ref": target_ref,
            "schedule_at": "2030-01-01T12:00:00Z",
            "content": {
                "text": "Four ordered PNGs",
                "media": [
                    {"asset_ref": asset_ref, "role": "image"}
                    for asset_ref in asset_refs
                ],
            },
        }
    )

    receipt = await adapter.execute(intent)

    assert receipt["status"] == "succeeded"
    methods = [call["method"] for call in transport.calls]
    assert methods == [
        "photos.getWallUploadServer",
        "photos.saveWallPhoto",
        "photos.getWallUploadServer",
        "photos.saveWallPhoto",
        "photos.getWallUploadServer",
        "photos.saveWallPhoto",
        "photos.getWallUploadServer",
        "photos.saveWallPhoto",
        "wall.post",
        "wall.get",
    ]
    assert [call["content"] for call in multipart.calls] == list(PNG_BYTES)
    assert transport.last_wall_post["publish_date"] == int(
        datetime.fromisoformat("2030-01-01T12:00:00+00:00").timestamp()
    )
    assert len(transport.last_wall_post["attachments"].split(",")) == 4
    finished = [
        event
        for event in recorder.events
        if event["stage"] == "wall_photo_multipart"
        and event["phase"] == "finished"
    ]
    assert [event["image_ordinal"] for event in finished] == [1, 2, 3, 4]
    assert [event["expected_digest_prefix"] for event in finished] == [
        hashlib.sha256(content).hexdigest()[:12] for content in PNG_BYTES
    ]
    assert all(
        event["provider_result"]["mutation_boundary_reached"] is False
        for event in finished
    )
    wall_started = next(
        event
        for event in recorder.events
        if event["stage"] == "wall_post" and event["phase"] == "started"
    )
    assert wall_started["mutation_boundary_reached"] is True


@pytest.mark.asyncio
async def test_retry_reuses_logical_operation_only_after_safe_prewall_failure() -> None:
    adapter, _refs, transport, _reader, multipart, _story_reader, target_ref = build_adapter()
    asset_ref = await adapter.stage_asset(
        verified_asset("ing_" + "i" * 24, IMAGE_BYTES, "image/jpeg"),
        role=MediaRole.IMAGE,
    )
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "publish",
            "idempotency_key": "retry-safe-same-logical-operation-001",
            "target_ref": target_ref,
            "content": {
                "text": "Retry one safe pre-wall failure",
                "media": [{"asset_ref": asset_ref, "role": "image"}],
            },
        }
    )
    multipart.fail = True
    failed = await adapter.execute(intent)
    assert failed["status"] == "failed" and failed["retry_safe"] is True

    multipart.fail = False
    succeeded = await adapter.retry(
        intent,
        operation_ref=failed["operation_ref"],
        attempt_number=2,
    )

    assert succeeded["operation_ref"] == failed["operation_ref"]
    assert succeeded["attempt_number"] == 2
    assert succeeded["status"] == "succeeded"
    assert len([call for call in transport.calls if call["method"] == "wall.post"]) == 1
    with pytest.raises(VKWorkspaceError, match="retry_not_safe"):
        await adapter.retry(
            intent,
            operation_ref=failed["operation_ref"],
            attempt_number=3,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failed_method", "expected_methods"),
    [
        ("photos.getWallUploadServer", ["photos.getWallUploadServer"]),
        (
            "photos.saveWallPhoto",
            ["photos.getWallUploadServer", "photos.saveWallPhoto"],
        ),
    ],
)
async def test_vk_api_transport_failure_before_wall_post_is_retry_safe(
    failed_method: str, expected_methods: list[str]
) -> None:
    adapter, _refs, transport, _reader, _multipart, _story_reader, target_ref = build_adapter()
    asset_ref = await adapter.stage_asset(
        verified_asset("ing_" + "i" * 24, IMAGE_BYTES, "image/jpeg"),
        role=MediaRole.IMAGE,
    )
    transport.fail_method = failed_method
    result = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "publish",
                "idempotency_key": "prewall-failure-" + failed_method,
                "target_ref": target_ref,
                "content": {
                    "text": "Photo",
                    "media": [{"asset_ref": asset_ref, "role": "image"}],
                },
            }
        )
    )
    assert [call["method"] for call in transport.calls] == expected_methods
    assert result["status"] == "failed"
    assert result["retry_safe"] is True
    assert result["error_code"] == "provider_transport_error"


@pytest.mark.asyncio
async def test_multipart_transport_reads_fragmented_json_until_eof(monkeypatch) -> None:
    class FragmentedContent:
        async def read(self, _limit: int) -> bytes:
            # This models aiohttp StreamReader.read(n): it may return currently
            # available bytes without waiting for EOF.
            return b'{"server":321,'

        async def iter_chunked(self, _size: int):
            yield b'{"server":321,'
            yield b'"photo":"opaque-photo",'
            yield b'"hash":"upload-hash"}'

    class Response:
        status = 200
        headers: dict[str, str] = {}
        content = FragmentedContent()

    class RequestContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Session:
        def post(self, *_args, **_kwargs):
            return RequestContext()

        async def close(self):
            return None

    async def fake_session_for(_url: str, _timeout: float):
        return Session(), object()

    monkeypatch.setattr(vk_transport_module, "_session_for", fake_session_for)
    result = await SecureVKMultipartTransport().upload(
        purpose=VKUploadPurpose.WALL_PHOTO,
        upload_url="https://pu.vk.com/upload.php?sig=signed",
        content=b"image-bytes",
        filename="asset.png",
        mime_type="image/png",
        timeout_seconds=5,
    )
    assert result == VKMultipartUploadResult(
        server=321, photo="opaque-photo", upload_hash="upload-hash"
    )
    assert result.observation["consumed_to_eof"] is True
    assert result.observation["decoded_bytes"] == len(
        b'{"server":321,"photo":"opaque-photo","hash":"upload-hash"}'
    )
    assert result.observation["top_level_key_names"] == [
        "hash",
        "photo",
        "server",
    ]


@pytest.mark.asyncio
async def test_multipart_transport_decodes_actual_fragmented_gzip_nested_receipt(
    monkeypatch,
) -> None:
    decoded = (
        b'{"response":{"server":321,"photo":"opaque-photo",'
        b'"hash":"upload-hash"}}'
    )
    compressed = gzip.compress(decoded)

    class FragmentedContent:
        async def iter_chunked(self, _size: int):
            yield compressed[:7]
            yield compressed[7:19]
            yield compressed[19:]

    class Response:
        status = 200
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Encoding": "gzip",
            "Content-Length": str(len(compressed)),
        }
        content = FragmentedContent()

    class RequestContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Session:
        def post(self, *_args, **_kwargs):
            return RequestContext()

        async def close(self):
            return None

    async def fake_session_for(_url: str, _timeout: float):
        return Session(), object()

    monkeypatch.setattr(vk_transport_module, "_session_for", fake_session_for)
    result = await SecureVKMultipartTransport().upload(
        purpose=VKUploadPurpose.WALL_PHOTO,
        upload_url="https://pu.vk.com/upload.php?sig=signed",
        content=b"image-bytes",
        filename="asset.png",
        mime_type="image/png",
        timeout_seconds=5,
    )

    assert result == VKMultipartUploadResult(
        server=321, photo="opaque-photo", upload_hash="upload-hash"
    )
    assert result.observation == {
        "http_status": 200,
        "content_type": "application/json",
        "content_encoding": "gzip",
        "compressed_bytes": len(compressed),
        "decoded_bytes": len(decoded),
        "consumed_to_eof": True,
        "top_level_key_names": ["response"],
        "top_level_unknown_key_count": 0,
        "nested_key_names": ["hash", "photo", "server"],
        "nested_unknown_key_count": 0,
        "server_field": {"type": "int", "length": None, "length_capped": False},
        "photo_field": {"type": "str", "length": 12, "length_capped": False},
        "hash_field": {"type": "str", "length": 11, "length_capped": False},
    }


@pytest.mark.asyncio
async def test_multipart_transport_uses_explicit_bounded_content_decompression(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class Connector:
        def __init__(self, **kwargs: Any) -> None:
            captured["connector"] = kwargs

    class Session:
        def __init__(self, **kwargs: Any) -> None:
            captured["session"] = kwargs

    async def public_addresses(_host: str, _port: int) -> tuple[str, ...]:
        return ("93.184.216.34",)

    monkeypatch.setattr(vk_transport_module, "_public_addresses", public_addresses)
    monkeypatch.setattr(aiohttp, "TCPConnector", Connector)
    monkeypatch.setattr(aiohttp, "ClientSession", Session)

    session, _connector = await _vk_session_for(
        "https://pu.vk.com/upload.php?sig=signed", 5
    )

    assert isinstance(session, Session)
    assert captured["session"]["auto_decompress"] is False


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
    assert uncertain["status"] == "failed"
    assert uncertain["error_code"] == "provider_transport_error"
    assert uncertain["retry_safe"] is True


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
        "media_editor",
        "media_upload",
    )
