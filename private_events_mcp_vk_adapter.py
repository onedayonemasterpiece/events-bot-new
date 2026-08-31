"""Closed, role-scoped VK adapter for :mod:`private_events_mcp.social_workspace`.

The public surface accepts provider-neutral requests only.  Native VK identifiers
are held by an injected opaque-reference store and fixed API calls are made through
an injected, dedicated actor transport.  This module never imports ``main`` and has
no credential or generic VK API fallback.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import re
import secrets
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol
from urllib.parse import urlsplit

from private_events_mcp.social_workspace import (
    ContentFeature,
    MediaRole,
    SocialAction,
    SocialActionIntent,
    SocialActionStatus,
    SocialItemKind,
    SocialPlatform,
    SocialReadAccess,
    SocialReadOperation,
    SocialReadRequest,
    SocialTargetKind,
    SocialWorkspaceValidationError,
    TargetLocatorKind,
    compute_action_digest,
    validate_opaque_ref,
)
from private_events_mcp_vk_upload import (
    VKAssetMaterialization,
    VKMultipartTransport,
    VKMultipartUploadResult,
    VKStoryMediaMaterialization,
    VKStoryMediaReader,
    VKUploadPurpose,
    VKVerifiedAssetReader,
)

try:
    from private_events_mcp.media_contract import VerifiedAsset
except ModuleNotFoundError as exc:  # Core lane adds this module before integration.
    if exc.name != "private_events_mcp.media_contract":
        raise
    VerifiedAsset = Any  # type: ignore[misc,assignment]

VK_API_VERSION = "5.199"
_TRUST = "untrusted_external_data"
_MAX_RESULT_PAGE = 25
_MAX_PROVIDER_CURSOR_BYTES = 512
_MAX_EDITORIAL_TOTAL = 100
_MAX_TEXT = 4096
_MAX_MEDIA_BYTES = 64 * 1024 * 1024
_SAFE_HANDLE = re.compile(r"^[A-Za-z0-9_.-]{2,128}$")
_NATIVE_HANDLE = re.compile(r"^(?:id|club|public|event)\d+$", re.IGNORECASE)
_STORAGE_REF = re.compile(r"^ing_[A-Za-z0-9_-]{24,160}$")
_OWNER_BINDING = re.compile(r"^[a-f0-9]{64}$")
_CONTENT_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_STORY_UPLOAD_RESULT = re.compile(r"^[^\x00]{8,8192}$", re.DOTALL)
_VK_CDN_SUFFIXES = (
    ".okcdn.ru",
    ".userapi.com",
    ".vk.com",
    ".vk.me",
    ".vkuser.net",
    ".vkuseraudio.net",
    ".vkvideo.ru",
)
_LOG = logging.getLogger(__name__)


class VKWorkspaceError(RuntimeError):
    """Sanitized adapter error.  Provider payloads are never included."""

    def __init__(
        self,
        code: str,
        *,
        outcome_unknown: bool = False,
        retry_safe: bool = False,
        stage: str | None = None,
    ) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", code):
            code = "provider_unavailable"
        self.code = code
        self.outcome_unknown = bool(outcome_unknown)
        self.retry_safe = bool(retry_safe) and not self.outcome_unknown
        self.stage = stage
        super().__init__(code)


class VKActor(str, Enum):
    PUBLIC_READER = "public_reader"
    NOTIFICATION_READER = "notification_reader"
    DIALOG_READER = "dialog_reader"
    USER_MESSENGER = "user_messenger"
    COMMUNITY_EDITOR = "community_editor"
    MEDIA_EDITOR = "media_editor"
    ANALYTICS_READER = "analytics_reader"
    STORY_READER = "story_reader"
    STORY_EDITOR = "story_editor"


class VKActorTransport(Protocol):
    """Dedicated transport.  Implementations bind actors to role-scoped tokens."""

    def permits(self, actor: VKActor, capability: str) -> bool: ...

    async def invoke(
        self,
        *,
        actor: VKActor,
        method: str,
        params: Mapping[str, Any],
        version: str,
        timeout_seconds: float,
    ) -> Mapping[str, Any] | Sequence[Any]: ...


class VKOpaqueRefStore(Protocol):
    """Maps public opaque references to native values without exposing them."""

    def mint(self, kind: str, native_value: Mapping[str, Any]) -> str: ...

    def resolve(self, kind: str, opaque_ref: str) -> Mapping[str, Any]: ...


class VKCallGovernor(Protocol):
    async def before_call(self, actor: VKActor, capability: str) -> None: ...

    async def after_call(self, actor: VKActor, capability: str, outcome: str) -> None: ...


class VKCooldownHook(Protocol):
    async def ensure_available(self, actor: VKActor) -> None: ...

    async def record_captcha(self, actor: VKActor) -> None: ...

    async def record_success(self, actor: VKActor) -> None: ...


class VKAttemptRecorder(Protocol):
    def record(self, operation_ref: str, event: Mapping[str, Any]) -> None: ...


@dataclass(frozen=True, slots=True)
class _CallPolicy:
    actor: VKActor
    capability: str
    method: str
    required: frozenset[str]
    optional: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class _CursorBinding:
    operation: str
    resource_fingerprint: str
    query_fingerprint: str
    sample_ref: str
    read_access: str
    offset: int
    nonce: str


def _policy(
    actor: VKActor,
    capability: str,
    method: str,
    required: Sequence[str],
    optional: Sequence[str] = (),
) -> _CallPolicy:
    return _CallPolicy(actor, capability, method, frozenset(required), frozenset(optional))


# The operation key, provider method, actor and parameter vocabulary are all fixed.
# No public function accepts any of these values.
_CALLS: Mapping[str, _CallPolicy] = {
    "resolve_screen": _policy(VKActor.PUBLIC_READER, "discover", "utils.resolveScreenName", ["screen_name"]),
    "get_users": _policy(VKActor.PUBLIC_READER, "discover", "users.get", [], ["user_ids", "fields"]),
    "get_self": _policy(VKActor.DIALOG_READER, "dialogs", "users.get", [], ["fields"]),
    "get_groups": _policy(VKActor.PUBLIC_READER, "discover", "groups.getById", ["group_ids"], ["fields"]),
    "search_groups": _policy(VKActor.PUBLIC_READER, "discover", "groups.search", ["q", "count"], ["offset", "type"]),
    "wall_feed": _policy(VKActor.PUBLIC_READER, "read_public", "wall.get", ["owner_id", "count", "filter"], ["offset"]),
    "wall_feed_editor": _policy(VKActor.COMMUNITY_EDITOR, "post_publish", "wall.get", ["owner_id", "count", "filter"], ["offset"]),
    "wall_postponed_delete_verify": _policy(VKActor.COMMUNITY_EDITOR, "delete", "wall.get", ["owner_id", "count", "filter"], ["offset"]),
    "wall_item": _policy(VKActor.PUBLIC_READER, "read_public", "wall.getById", ["posts"], ["extended"]),
    "wall_search": _policy(VKActor.PUBLIC_READER, "search_public", "wall.search", ["owner_id", "query", "count"], ["offset", "owners_only"]),
    "newsfeed_search": _policy(VKActor.PUBLIC_READER, "search_public", "newsfeed.search", ["q", "count"], ["start_from"]),
    "notifications": _policy(
        VKActor.NOTIFICATION_READER,
        "notifications_read",
        "notifications.get",
        ["count"],
        ["start_from", "filters", "start_time", "end_time"],
    ),
    "wall_comments": _policy(VKActor.PUBLIC_READER, "read_public", "wall.getComments", ["owner_id", "post_id", "count"], ["offset", "extended", "sort"]),
    "wall_likes": _policy(VKActor.PUBLIC_READER, "read_public", "likes.getList", ["type", "owner_id", "item_id", "count"], ["extended"]),
    "dialog_history": _policy(VKActor.DIALOG_READER, "dialogs", "messages.getHistory", ["peer_id", "count"], ["offset", "start_message_id", "rev"]),
    "message_item": _policy(VKActor.DIALOG_READER, "dialogs", "messages.getById", ["message_ids"]),
    "conversations": _policy(
        VKActor.DIALOG_READER,
        "dialogs",
        "messages.getConversations",
        ["count"],
        ["offset", "filter", "extended", "fields"],
    ),
    "stories": _policy(VKActor.STORY_READER, "story_read", "stories.get", ["owner_id"], ["extended", "fields"]),
    "story_item": _policy(VKActor.STORY_READER, "story_read", "stories.getById", ["stories"], ["extended", "fields"]),
    "story_stats": _policy(VKActor.ANALYTICS_READER, "analytics", "stories.getStats", ["owner_id", "story_id"]),
    "community_stats": _policy(VKActor.ANALYTICS_READER, "analytics", "stats.get", ["group_id", "interval"], ["timestamp_from", "timestamp_to", "intervals_count"]),
    "audience": _policy(VKActor.PUBLIC_READER, "audience", "groups.getMembers", ["group_id", "count"], ["offset"]),
    "send_message": _policy(VKActor.USER_MESSENGER, "dm_send", "messages.send", ["peer_id", "message", "random_id"], ["attachment", "forward"]),
    "wall_post": _policy(VKActor.COMMUNITY_EDITOR, "post_publish", "wall.post", ["owner_id", "from_group", "message", "guid"], ["attachments", "publish_date", "signed"]),
    "wall_edit": _policy(VKActor.COMMUNITY_EDITOR, "edit", "wall.edit", ["owner_id", "post_id", "message"], ["attachments"]),
    "wall_delete": _policy(VKActor.COMMUNITY_EDITOR, "delete", "wall.delete", ["owner_id", "post_id"]),
    "wall_comment": _policy(VKActor.COMMUNITY_EDITOR, "comment", "wall.createComment", ["owner_id", "post_id", "message", "guid"], ["attachments", "from_group"]),
    "like_add": _policy(VKActor.COMMUNITY_EDITOR, "reaction", "likes.add", ["type", "owner_id", "item_id"]),
    "like_delete": _policy(VKActor.COMMUNITY_EDITOR, "reaction", "likes.delete", ["type", "owner_id", "item_id"]),
    "wall_repost": _policy(VKActor.COMMUNITY_EDITOR, "forward", "wall.repost", ["object"], ["group_id", "message"]),
    "message_forward": _policy(VKActor.USER_MESSENGER, "forward", "messages.send", ["peer_id", "message", "random_id", "forward"]),
    "message_edit": _policy(VKActor.USER_MESSENGER, "edit", "messages.edit", ["peer_id", "message", "message_id"], ["attachment"]),
    "message_delete": _policy(VKActor.USER_MESSENGER, "delete", "messages.delete", ["message_ids", "delete_for_all"]),
    "wall_photo_upload_server": _policy(VKActor.MEDIA_EDITOR, "media_upload", "photos.getWallUploadServer", ["group_id"]),
    "wall_photo_save": _policy(VKActor.MEDIA_EDITOR, "media_upload", "photos.saveWallPhoto", ["group_id", "photo", "server", "hash"]),
    "story_photo_upload_server": _policy(VKActor.STORY_EDITOR, "story_write", "stories.getPhotoUploadServer", ["group_id", "add_to_news"]),
    "story_video_upload_server": _policy(VKActor.STORY_EDITOR, "story_write", "stories.getVideoUploadServer", ["group_id", "add_to_news"]),
    "story_save": _policy(VKActor.STORY_EDITOR, "story_write", "stories.save", ["upload_results"]),
    "story_delete": _policy(VKActor.STORY_EDITOR, "story_write", "stories.delete", ["owner_id", "story_id"]),
}

VK_FIXED_METHOD_ALLOWLIST = frozenset(policy.method for policy in _CALLS.values())
VK_OPERATION_ACTORS = {
    name: (policy.actor.value, policy.capability) for name, policy in _CALLS.items()
}


def _utc(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        value = 0
    try:
        return datetime.fromtimestamp(max(0, value), timezone.utc).isoformat().replace("+00:00", "Z")
    except (OSError, OverflowError, ValueError):
        return "1970-01-01T00:00:00Z"


def _int(value: Any) -> int | None:
    return value if type(value) is int else None


def _items(response: Any) -> list[Mapping[str, Any]]:
    if isinstance(response, Mapping):
        raw = response.get("items")
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, Mapping)]
    if isinstance(response, list):
        return [item for item in response if isinstance(item, Mapping)]
    return []


def _story_feed_items(response: Any) -> list[Mapping[str, Any]]:
    """Flatten the official ``stories.get`` ``items[].stories`` shape."""

    stories: list[Mapping[str, Any]] = []
    for feed_item in _items(response):
        raw = feed_item.get("stories")
        if isinstance(raw, list):
            stories.extend(item for item in raw if isinstance(item, Mapping))
    return stories


def _story_stat_count(response: Any, name: str) -> int:
    if not isinstance(response, Mapping):
        return 0
    stat = response.get(name)
    if isinstance(stat, Mapping):
        state = stat.get("state")
        count = _int(stat.get("count"))
        if state in {"on", "off", "hidden"} and count is not None:
            return max(0, count)
    return 0


def _validated_vk_https_url(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 4096
        or "\\" in value
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise VKWorkspaceError("provider_media_invalid")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise VKWorkspaceError("provider_media_invalid") from None
    host = (parsed.hostname or "").lower().rstrip(".")
    expected_netloc = host if port is None else f"{host}:{port}"
    if (
        parsed.scheme != "https"
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.fragment
        or parsed.netloc != expected_netloc
        or not any(host == suffix[1:] or host.endswith(suffix) for suffix in _VK_CDN_SUFFIXES)
    ):
        raise VKWorkspaceError("provider_media_invalid")
    return value


class VKWorkspaceAdapter:
    """Capability-gated VK implementation of the provider-neutral workspace."""

    platform = SocialPlatform.VK

    def __init__(
        self,
        *,
        transport: VKActorTransport,
        refs: VKOpaqueRefStore,
        governor: VKCallGovernor,
        cooldown: VKCooldownHook,
        sanitize_text: Any,
        asset_reader: VKVerifiedAssetReader | None = None,
        multipart_transport: VKMultipartTransport | None = None,
        story_media_reader: VKStoryMediaReader | None = None,
        attempt_recorder: VKAttemptRecorder | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        if not callable(getattr(transport, "invoke", None)) or not callable(getattr(transport, "permits", None)):
            raise TypeError("transport must be a dedicated actor transport")
        if not callable(getattr(refs, "mint", None)) or not callable(getattr(refs, "resolve", None)):
            raise TypeError("refs must be an opaque-reference store")
        if not all(callable(getattr(governor, name, None)) for name in ("before_call", "after_call")):
            raise TypeError("governor hooks are required")
        if not all(callable(getattr(cooldown, name, None)) for name in ("ensure_available", "record_captcha", "record_success")):
            raise TypeError("cooldown hooks are required")
        if not callable(sanitize_text):
            raise TypeError("recursive sanitation hook is required")
        if asset_reader is not None and not callable(getattr(asset_reader, "open_verified", None)):
            raise TypeError("asset_reader must expose open_verified")
        if multipart_transport is not None and not callable(getattr(multipart_transport, "upload", None)):
            raise TypeError("multipart_transport must expose upload")
        if story_media_reader is not None and not callable(getattr(story_media_reader, "fetch_vk_cdn", None)):
            raise TypeError("story_media_reader must expose fetch_vk_cdn")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not 0 < timeout_seconds <= 60:
            raise ValueError("timeout_seconds is invalid")
        self._transport = transport
        self._refs = refs
        self._governor = governor
        self._cooldown = cooldown
        self._sanitize_text = sanitize_text
        self._asset_reader = asset_reader
        self._multipart_transport = multipart_transport
        self._story_media_reader = story_media_reader
        self._attempt_recorder = attempt_recorder
        self._timeout = float(timeout_seconds)
        self._lock = asyncio.Lock()
        self._action_lock = asyncio.Lock()
        self._asset_lock = asyncio.Lock()
        self._operations: dict[str, dict[str, Any]] = {}
        self._idempotency: dict[str, tuple[str, str]] = {}
        self._operation_claims: dict[str, tuple[str, str]] = {}
        self._staged_assets: dict[tuple[str, str, str, str], str] = {}

    def __repr__(self) -> str:
        return "<VKWorkspaceAdapter platform='vk' transport='role_scoped'>"

    async def _await(self, value: Any) -> Any:
        return await value if inspect.isawaitable(value) else value

    def _permitted(self, actor: VKActor, capability: str) -> bool:
        try:
            return self._transport.permits(actor, capability) is True
        except Exception:  # noqa: BLE001 - fail closed across injected transport implementations
            return False

    async def _call(
        self,
        operation: str,
        params: Mapping[str, Any],
        *,
        attempt_hook: Any | None = None,
    ) -> Any:
        policy = _CALLS[operation]
        keys = frozenset(params)
        if not policy.required.issubset(keys) or not keys.issubset(policy.required | policy.optional):
            raise VKWorkspaceError("adapter_contract_error")
        if not self._permitted(policy.actor, policy.capability):
            raise VKWorkspaceError("actor_capability_denied")
        async with self._lock:
            try:
                await self._await(self._cooldown.ensure_available(policy.actor))
            except Exception:  # noqa: BLE001 - normalize injected cooldown failures
                raise VKWorkspaceError("cooldown_active") from None
            try:
                await self._await(self._governor.before_call(policy.actor, policy.capability))
            except Exception:  # noqa: BLE001 - normalize injected governor failures
                raise VKWorkspaceError("rate_limited") from None
            outcome = "failed"
            attempt_finished = False
            def emit(phase: str, **details: Any) -> None:
                if attempt_hook is not None:
                    attempt_hook(
                        {
                            "stage": operation,
                            "method": policy.method,
                            "phase": phase,
                            **details,
                        }
                    )
            try:
                if attempt_hook is not None:
                    emit("started")
                result = await asyncio.wait_for(
                    self._transport.invoke(
                        actor=policy.actor,
                        method=policy.method,
                        params=dict(params),
                        version=VK_API_VERSION,
                        timeout_seconds=self._timeout,
                    ),
                    timeout=self._timeout,
                )
                if isinstance(result, Mapping) and "error" in result:
                    error = result.get("error")
                    code = error.get("error_code") if isinstance(error, Mapping) else None
                    if code == 14:
                        emit(
                            "finished",
                            http_status=200,
                            provider_error_code=14,
                            provider_error_message="vk_api_error_14",
                            error_class="captcha_cooldown",
                            outcome="failed",
                        )
                        attempt_finished = True
                        await self._await(self._cooldown.record_captcha(policy.actor))
                        raise VKWorkspaceError("captcha_cooldown")
                    provider_code = code if type(code) is int and 0 <= code <= 9999 else 0
                    emit(
                        "finished",
                        http_status=200,
                        provider_error_code=provider_code,
                        provider_error_message=f"vk_api_error_{provider_code}",
                        error_class="vk_api_error",
                        outcome="failed",
                    )
                    attempt_finished = True
                    raise VKWorkspaceError(
                        f"vk_api_error_{provider_code}",
                        retry_safe=True,
                        stage=operation,
                    )
                outcome = "succeeded"
                provider_result: dict[str, Any] = {}
                if operation == "wall_post" and isinstance(result, Mapping):
                    if type(result.get("post_id")) is int:
                        provider_result["post_id"] = result["post_id"]
                elif operation == "wall_photo_save":
                    photos = _items(result)
                    if photos and type(photos[0].get("id")) is int and type(photos[0].get("owner_id")) is int:
                        provider_result = {
                            "photo_id": photos[0]["id"],
                            "photo_owner_id": photos[0]["owner_id"],
                        }
                emit(
                    "finished",
                    http_status=200,
                    provider_result=provider_result,
                    outcome="succeeded",
                )
                attempt_finished = True
                await self._await(self._cooldown.record_success(policy.actor))
                return result
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            except asyncio.TimeoutError:
                outcome = "outcome_unknown"
                if not attempt_finished:
                    emit(
                        "finished",
                        error_class="provider_timeout",
                        outcome="outcome_unknown",
                    )
                raise VKWorkspaceError(
                    "provider_timeout", outcome_unknown=True, stage=operation
                ) from None
            except VKWorkspaceError as exc:
                if not attempt_finished:
                    emit(
                        "finished",
                        error_class=exc.code,
                        outcome=("outcome_unknown" if exc.outcome_unknown else "failed"),
                    )
                raise
            except Exception as exc:  # noqa: BLE001 - sanitize arbitrary provider failures
                if getattr(exc, "code", None) == 14 or getattr(exc, "error_code", None) == 14:
                    await self._await(self._cooldown.record_captcha(policy.actor))
                    raise VKWorkspaceError("captcha_cooldown") from None
                if not attempt_finished:
                    emit(
                        "finished",
                        error_class="provider_transport_error",
                        outcome="outcome_unknown",
                    )
                raise VKWorkspaceError(
                    "provider_transport_error", outcome_unknown=True, stage=operation
                ) from None
            finally:
                try:
                    await self._await(self._governor.after_call(policy.actor, policy.capability, outcome))
                except Exception:  # noqa: BLE001,S110 - accounting must not mask provider outcome
                    pass

    def _sanitize(self, value: Any) -> Any:
        """Recursively copy provider data and sanitize every string leaf."""
        if isinstance(value, str):
            clean = self._sanitize_text(value)
            if not isinstance(clean, str):
                raise VKWorkspaceError("sanitation_failed")
            return clean.replace("\x00", "")
        if isinstance(value, Mapping):
            return {str(key): self._sanitize(child) for key, child in value.items()}
        if isinstance(value, list):
            return [self._sanitize(child) for child in value]
        if isinstance(value, tuple):
            return tuple(self._sanitize(child) for child in value)
        return value

    def _mint(self, kind: str, native: Mapping[str, Any]) -> str:
        try:
            ref = self._refs.mint(kind, dict(native))
            return validate_opaque_ref(ref, kind)
        except Exception:  # noqa: BLE001 - opaque-store boundary is intentionally generic
            raise VKWorkspaceError("opaque_reference_failed") from None

    def _resolve_ref(self, kind: str, ref: str) -> Mapping[str, Any]:
        validate_opaque_ref(ref, kind)
        try:
            value = self._refs.resolve(kind, ref)
        except Exception:  # noqa: BLE001 - opaque-store boundary is intentionally generic
            raise VKWorkspaceError("opaque_reference_failed") from None
        if not isinstance(value, Mapping):
            raise VKWorkspaceError("opaque_reference_failed")
        return value

    async def stage_asset(self, asset: VerifiedAsset, *, role: MediaRole) -> str:
        """Store only a verified-asset descriptor and return its inner VK ref.

        The bytes are deliberately not opened or sent to VK here.  The core calls
        this hook before persisting an outer asset binding; provider I/O happens
        only from ``execute`` after the durable approval gate has been consumed.
        """

        if role is not MediaRole.IMAGE:
            raise VKWorkspaceError("asset_role_unsupported")
        try:
            storage_ref = asset.storage_ref
            owner_binding = asset.owner_binding
            content_digest = asset.content_digest
            mime_type = asset.mime_type
            byte_length = asset.byte_length
            expires_at = asset.expires_at
            width = asset.width
            height = asset.height
        except AttributeError:
            raise VKWorkspaceError("verified_asset_invalid") from None
        if not isinstance(storage_ref, str) or not _STORAGE_REF.fullmatch(storage_ref):
            raise VKWorkspaceError("verified_asset_invalid")
        if not isinstance(owner_binding, str) or not _OWNER_BINDING.fullmatch(owner_binding):
            raise VKWorkspaceError("verified_asset_invalid")
        if not isinstance(content_digest, str) or not _CONTENT_DIGEST.fullmatch(content_digest):
            raise VKWorkspaceError("verified_asset_invalid")
        allowed_mimes = {MediaRole.IMAGE: {"image/jpeg", "image/png", "image/webp"}}
        if mime_type not in allowed_mimes[role]:
            raise VKWorkspaceError("asset_mime_unsupported")
        if type(byte_length) is not int or not 1 <= byte_length <= _MAX_MEDIA_BYTES:
            raise VKWorkspaceError("verified_asset_invalid")
        if type(expires_at) is not int or expires_at <= int(datetime.now(timezone.utc).timestamp()):
            raise VKWorkspaceError("asset_expired")
        if (width is None) != (height is None) or any(
            type(value) is not int or not 1 <= value <= 32_768
            for value in (width, height)
            if value is not None
        ):
            raise VKWorkspaceError("asset_geometry_invalid")
        descriptor = {
            "binding_kind": "verified_asset",
            "storage_ref": storage_ref,
            "owner_binding": owner_binding,
            "content_digest": content_digest,
            "mime_type": mime_type,
            "byte_length": byte_length,
            "expires_at": expires_at,
            "width": width,
            "height": height,
            "role": role.value,
        }
        cache_key = (owner_binding, storage_ref, content_digest, role.value)
        async with self._asset_lock:
            existing = self._staged_assets.get(cache_key)
            if existing is not None:
                return existing
            asset_ref = self._mint("asset", descriptor)
            self._staged_assets[cache_key] = asset_ref
            return asset_ref

    async def _materialize_asset(self, binding: Mapping[str, Any]) -> VKAssetMaterialization:
        if self._asset_reader is None:
            raise VKWorkspaceError("asset_reader_unavailable")
        storage_ref = binding.get("storage_ref")
        owner_binding = binding.get("owner_binding")
        content_digest = binding.get("content_digest")
        byte_length = binding.get("byte_length")
        mime_type = binding.get("mime_type")
        if (
            not isinstance(storage_ref, str)
            or not _STORAGE_REF.fullmatch(storage_ref)
            or not isinstance(owner_binding, str)
            or not _OWNER_BINDING.fullmatch(owner_binding)
            or not isinstance(content_digest, str)
            or not _CONTENT_DIGEST.fullmatch(content_digest)
            or type(byte_length) is not int
            or not 1 <= byte_length <= _MAX_MEDIA_BYTES
            or mime_type not in {"image/jpeg", "image/png", "image/webp"}
        ):
            raise VKWorkspaceError("verified_asset_invalid")
        expires_at = _int(binding.get("expires_at"))
        if expires_at is None or expires_at <= int(datetime.now(timezone.utc).timestamp()):
            raise VKWorkspaceError("asset_expired")
        try:
            materialized = await self._await(
                self._asset_reader.open_verified(
                    storage_ref, owner_binding
                )
            )
        except Exception:  # noqa: BLE001 - never expose storage implementation details
            raise VKWorkspaceError("asset_materialization_failed") from None
        if not isinstance(materialized, VKAssetMaterialization):
            raise VKWorkspaceError("asset_materialization_failed")
        expected = (
            binding.get("storage_ref"),
            binding.get("owner_binding"),
            binding.get("content_digest"),
            binding.get("mime_type"),
            binding.get("byte_length"),
        )
        observed = (
            materialized.storage_ref,
            materialized.owner_binding,
            materialized.content_digest,
            materialized.mime_type,
            materialized.byte_length,
        )
        content = materialized.content
        if (
            observed != expected
            or type(content) is not bytes
            or len(content) != materialized.byte_length
            or not 1 <= len(content) <= _MAX_MEDIA_BYTES
            or "sha256:" + hashlib.sha256(content).hexdigest() != materialized.content_digest
        ):
            raise VKWorkspaceError("asset_integrity_failed")
        return materialized

    async def _upload_multipart(
        self,
        *,
        purpose: VKUploadPurpose,
        upload_url: Any,
        materialized: VKAssetMaterialization,
        attempt_hook: Any,
        image_ordinal: int = 1,
    ) -> VKMultipartUploadResult:
        if self._multipart_transport is None:
            raise VKWorkspaceError("multipart_transport_unavailable")
        safe_url = _validated_vk_https_url(upload_url)
        extensions = {
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/webp": "webp",
            "video/mp4": "mp4",
        }
        extension = extensions.get(materialized.mime_type)
        if extension is None:
            raise VKWorkspaceError("asset_mime_unsupported")
        multipart_stage = {
            VKUploadPurpose.WALL_PHOTO: "wall_photo_multipart",
            VKUploadPurpose.STORY_PHOTO: "story_photo_multipart",
            VKUploadPurpose.STORY_VIDEO: "story_video_multipart",
        }[purpose]
        digest_prefix = materialized.content_digest.removeprefix("sha256:")[:12]
        safe_context = {
            "image_ordinal": image_ordinal,
            "expected_digest_prefix": digest_prefix,
        }
        attempt_hook(
            {
                "stage": multipart_stage,
                "method": "multipart.upload",
                "phase": "started",
                **safe_context,
            }
        )
        try:
            result = await asyncio.wait_for(
                self._multipart_transport.upload(
                    purpose=purpose,
                    upload_url=safe_url,
                    content=materialized.content,
                    filename=f"asset.{extension}",
                    mime_type=materialized.mime_type,
                    timeout_seconds=self._timeout,
                ),
                timeout=self._timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - multipart payloads/errors are provider-private
            observation = self._safe_multipart_observation(
                getattr(exc, "observation", None)
            )
            attempt_hook(
                {
                    "stage": multipart_stage,
                    "method": "multipart.upload",
                    "phase": "finished",
                    "error_class": "media_upload_failed",
                    "outcome": "failed",
                    **safe_context,
                    **observation,
                }
            )
            raise VKWorkspaceError(
                "media_upload_failed", retry_safe=True, stage=multipart_stage
            ) from None
        if not isinstance(result, VKMultipartUploadResult):
            attempt_hook(
                {
                    "stage": multipart_stage,
                    "method": "multipart.upload",
                    "phase": "finished",
                    "error_class": "media_upload_response_invalid",
                    "outcome": "failed",
                    **safe_context,
                }
            )
            raise VKWorkspaceError(
                "media_upload_response_invalid",
                retry_safe=purpose is VKUploadPurpose.WALL_PHOTO,
                stage=multipart_stage,
            )
        observation = self._safe_multipart_observation(result.observation)
        observation.setdefault("http_status", 200)
        if purpose is VKUploadPurpose.WALL_PHOTO:
            observation.update(
                self._multipart_field_observation(
                    result.server, result.photo, result.upload_hash
                )
            )
            if (
                result.response_valid is not True
                or type(result.server) is not int
                or not isinstance(result.photo, str)
                or not 1 <= len(result.photo) <= 65536
                or not isinstance(result.upload_hash, str)
                or not 1 <= len(result.upload_hash) <= 8192
            ):
                attempt_hook(
                    {
                        "stage": multipart_stage,
                        "method": "multipart.upload",
                        "phase": "finished",
                        "error_class": "media_upload_response_invalid",
                        "outcome": "failed",
                        **safe_context,
                        **observation,
                    }
                )
                raise VKWorkspaceError(
                    "media_upload_response_invalid",
                    retry_safe=True,
                    stage=multipart_stage,
                )
        attempt_hook(
            {
                "stage": multipart_stage,
                "method": "multipart.upload",
                "phase": "finished",
                "outcome": "succeeded",
                **safe_context,
                **observation,
            }
        )
        return result

    @staticmethod
    def _multipart_field_observation(
        server: Any, photo: Any, upload_hash: Any
    ) -> dict[str, Any]:
        def field(value: Any, cap: int) -> dict[str, Any]:
            length = len(value) if isinstance(value, str) else None
            return {
                "type": type(value).__name__,
                "length": min(length, cap) if length is not None else None,
                "length_capped": bool(length is not None and length > cap),
            }

        return {
            "server_field": field(server, 32),
            "photo_field": field(photo, 65536),
            "hash_field": field(upload_hash, 8192),
        }

    @staticmethod
    def _safe_multipart_observation(value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            return {}
        safe: dict[str, Any] = {}
        status = value.get("http_status")
        if type(status) is int and 100 <= status <= 599:
            safe["http_status"] = status
        for key, allowed in (
            (
                "content_type",
                {"application/json", "text/json", "text/plain", "application/octet-stream", "other"},
            ),
            ("content_encoding", {"identity", "gzip", "deflate", "other"}),
        ):
            observed = value.get(key)
            if observed in allowed:
                safe[key] = observed
        for key in (
            "compressed_bytes",
            "decoded_bytes",
            "top_level_unknown_key_count",
            "nested_unknown_key_count",
        ):
            observed = value.get(key)
            if type(observed) is int and 0 <= observed <= 256 * 1024:
                safe[key] = observed
        if type(value.get("consumed_to_eof")) is bool:
            safe["consumed_to_eof"] = value["consumed_to_eof"]
        allowed_names = {"response", "server", "photo", "hash", "upload_result"}
        for key in ("top_level_key_names", "nested_key_names"):
            observed = value.get(key)
            if isinstance(observed, Sequence) and not isinstance(observed, (str, bytes)):
                safe[key] = sorted(
                    {str(name) for name in observed if str(name) in allowed_names}
                )[:8]
        allowed_types = {"NoneType", "str", "int", "float", "bool", "dict", "list"}
        for key in ("server_field", "photo_field", "hash_field"):
            observed = value.get(key)
            if not isinstance(observed, Mapping):
                continue
            field_type = observed.get("type")
            length = observed.get("length")
            capped = observed.get("length_capped")
            if field_type not in allowed_types:
                continue
            safe[key] = {
                "type": field_type,
                "length": (
                    length
                    if type(length) is int and 0 <= length <= 65536
                    else None
                ),
                "length_capped": bool(capped) if type(capped) is bool else False,
            }
        return safe

    async def read_asset(
        self, asset_ref: str, *, owner_binding: str, max_bytes: int
    ) -> VKStoryMediaMaterialization:
        """Materialize a story visual through a closed, principal-bound CDN hook."""

        validate_opaque_ref(asset_ref, "asset")
        if not isinstance(owner_binding, str) or not _OWNER_BINDING.fullmatch(owner_binding):
            raise SocialWorkspaceValidationError("owner_binding is invalid")
        if type(max_bytes) is not int or not 1 <= max_bytes <= _MAX_MEDIA_BYTES:
            raise SocialWorkspaceValidationError("max_bytes is invalid")
        if self._story_media_reader is None:
            raise VKWorkspaceError("story_media_reader_unavailable")
        binding = self._resolve_ref("asset", asset_ref)
        if binding.get("binding_kind") != "story_media":
            raise VKWorkspaceError("asset_not_readable")
        expires_at = _int(binding.get("expires_at"))
        if expires_at is not None and expires_at <= int(datetime.now(timezone.utc).timestamp()):
            raise VKWorkspaceError("asset_expired")
        source_url = _validated_vk_https_url(binding.get("source_url"))
        try:
            result = await asyncio.wait_for(
                self._story_media_reader.fetch_vk_cdn(
                    source_url=source_url,
                    owner_binding=owner_binding,
                    max_bytes=max_bytes,
                    timeout_seconds=self._timeout,
                ),
                timeout=self._timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - provider URL/errors never cross this boundary
            raise VKWorkspaceError("provider_unavailable") from None
        if (
            not isinstance(result, VKStoryMediaMaterialization)
            or type(result.content) is not bytes
            or result.byte_length != len(result.content)
            or not 1 <= result.byte_length <= max_bytes
            or not _CONTENT_DIGEST.fullmatch(result.content_digest)
            or "sha256:" + hashlib.sha256(result.content).hexdigest() != result.content_digest
            or result.mime_type != binding.get("mime_type")
        ):
            raise VKWorkspaceError("asset_integrity_failed")
        return result

    @staticmethod
    def _cursor_context(request: SocialReadRequest, sample_ref: str | None = None) -> tuple[str, str, str, str, str]:
        resource = request.target_ref or request.item_ref or "none"
        resource_fingerprint = hashlib.sha256(resource.encode()).hexdigest()
        query_binding = request.query or ""
        query_binding += "\0date_from=" + (request.date_from or "")
        query_binding += "\0date_to=" + (request.date_to or "")
        query_binding += "\0item_kinds=" + ",".join(
            sorted(kind.value for kind in request.item_kinds)
        )
        if request.operation is SocialReadOperation.LIST_DIALOGS:
            query_binding += "\0unread=" + ("1" if request.unread_only else "0")
        query_fingerprint = hashlib.sha256(query_binding.encode()).hexdigest()
        read_access = request.read_access.value if request.read_access is not None else "none"
        return request.operation.value, resource_fingerprint, query_fingerprint, sample_ref or request.sample_ref or "none", read_access

    def _cursor(self, request: SocialReadRequest, offset: int, *, sample_ref: str | None = None) -> str:
        operation, resource, query, sample, read_access = self._cursor_context(request, sample_ref)
        nonce = secrets.token_hex(8)
        binding = _CursorBinding(operation, resource, query, sample, read_access, offset, nonce)
        try:
            return self._refs.mint(
                "cursor",
                {
                    "operation": binding.operation,
                    "resource_fingerprint": binding.resource_fingerprint,
                    "query_fingerprint": binding.query_fingerprint,
                    "sample_ref": binding.sample_ref,
                    "read_access": binding.read_access,
                    "offset": binding.offset,
                    "nonce": binding.nonce,
                },
            )
        except Exception:  # noqa: BLE001 - opaque cursor failures are normalized
            raise VKWorkspaceError("cursor_invalid") from None

    def _offset(self, request: SocialReadRequest) -> int:
        if request.cursor is None:
            return 0
        try:
            stored = self._refs.resolve("cursor", request.cursor)
            binding = _CursorBinding(
                str(stored["operation"]),
                str(stored["resource_fingerprint"]),
                str(stored["query_fingerprint"]),
                str(stored["sample_ref"]),
                str(stored["read_access"]),
                int(stored["offset"]),
                str(stored["nonce"]),
            )
        except Exception:  # noqa: BLE001 - normalize opaque cursor failures
            raise VKWorkspaceError("cursor_invalid")
        expected = self._cursor_context(request)
        if (binding.operation, binding.resource_fingerprint, binding.query_fingerprint, binding.sample_ref, binding.read_access) != expected:
            raise VKWorkspaceError("cursor_context_mismatch")
        return binding.offset

    def _target_kind(self, native: Mapping[str, Any]) -> SocialTargetKind:
        try:
            return SocialTargetKind(str(native["kind"]))
        except Exception:  # noqa: BLE001 - normalize malformed provider binding
            raise VKWorkspaceError("opaque_reference_failed") from None

    @staticmethod
    def _valid_user_binding(native: Mapping[str, Any]) -> bool:
        user_id, peer_id = _int(native.get("user_id")), _int(native.get("peer_id"))
        return user_id is not None and user_id > 0 and peer_id == user_id

    @staticmethod
    def _valid_community_binding(native: Mapping[str, Any]) -> bool:
        group_id, owner_id = _int(native.get("group_id")), _int(native.get("owner_id"))
        return native.get("kind") == "community" and group_id is not None and group_id > 0 and owner_id == -group_id

    @staticmethod
    def _valid_community_post(native: Mapping[str, Any]) -> bool:
        group_id, owner_id, post_id = (
            _int(native.get("group_id")), _int(native.get("owner_id")), _int(native.get("post_id"))
        )
        return native.get("kind") == "post" and group_id is not None and group_id > 0 and owner_id == -group_id and post_id is not None and post_id > 0

    @staticmethod
    def _valid_message_binding(native: Mapping[str, Any]) -> bool:
        peer_id, message_id = _int(native.get("peer_id")), _int(native.get("message_id"))
        return peer_id not in {None, 0} and message_id is not None and message_id > 0

    @classmethod
    def _dialog_peer_id(cls, native: Mapping[str, Any]) -> int | None:
        kind = str(native.get("kind") or "")
        peer_id = _int(native.get("peer_id"))
        if kind == SocialTargetKind.USER.value and cls._valid_user_binding(native):
            return peer_id
        if (
            kind == SocialTargetKind.CHAT.value
            and peer_id is not None
            and peer_id > 2_000_000_000
        ):
            return peer_id
        if kind == SocialTargetKind.COMMUNITY.value and cls._valid_community_binding(native):
            owner_id = _int(native.get("owner_id"))
            if peer_id in {None, owner_id}:
                return owner_id
        return None

    @staticmethod
    def _wall_native(owner_id: int, post_id: int) -> dict[str, Any]:
        native: dict[str, Any] = {"kind": "post", "owner_id": owner_id, "post_id": post_id}
        if owner_id < 0:
            native["group_id"] = -owner_id
        return native

    def _target_preview(self, native: Mapping[str, Any], raw: Mapping[str, Any], *, exact: bool = True) -> dict[str, Any]:
        kind = self._target_kind(native)
        ref = self._mint("target", native)
        first = str(raw.get("first_name") or "").strip()
        last = str(raw.get("last_name") or "").strip()
        name = str(raw.get("name") or (first + " " + last).strip() or "VK target")
        output: dict[str, Any] = {
            "platform": "vk",
            "target_ref": ref,
            "kind": kind.value,
            "display_name": self._sanitize(name)[:512] or "VK target",
            "is_exact_match": exact,
            "trust": _TRUST,
        }
        handle = str(raw.get("screen_name") or raw.get("domain") or "").strip()
        if _SAFE_HANDLE.fullmatch(handle) and not _NATIVE_HANDLE.fullmatch(handle):
            output["canonical_handle"] = self._sanitize(handle)[:128]
            output["profile_link"] = f"https://vk.com/{handle}"
        description = raw.get("description") or raw.get("status")
        if isinstance(description, str):
            output["description"] = self._sanitize(description)[:2048]
        return output

    async def capabilities(self, target_ref: str | None) -> Mapping[str, Any]:
        kind: SocialTargetKind | None = None
        if target_ref is not None:
            kind = self._target_kind(self._resolve_ref("target", target_ref))
        reads: set[SocialReadOperation] = set()
        actions: set[SocialAction] = set()
        for operation in ("resolve_screen", "search_groups"):
            p = _CALLS[operation]
            if self._permitted(p.actor, p.capability):
                reads.update({SocialReadOperation.RESOLVE_TARGET, SocialReadOperation.SEARCH_TARGETS})
        if self._permitted(VKActor.PUBLIC_READER, "read_public"):
            reads.update({SocialReadOperation.LIST_ITEMS, SocialReadOperation.GET_ITEM, SocialReadOperation.LIST_COMMENTS, SocialReadOperation.LIST_REACTIONS, SocialReadOperation.EDITORIAL_SAMPLE})
        if self._permitted(VKActor.PUBLIC_READER, "search_public") or self._permitted(VKActor.DIALOG_READER, "search_newsfeed"):
            reads.add(SocialReadOperation.SEARCH_ITEMS)
        if self._permitted(VKActor.DIALOG_READER, "dialogs"):
            reads.update(
                {
                    SocialReadOperation.LIST_DIALOGS,
                    SocialReadOperation.LIST_ITEMS,
                    SocialReadOperation.GET_ITEM,
                }
            )
        if self._permitted(VKActor.STORY_READER, "story_read"):
            reads.add(SocialReadOperation.LIST_STORIES)
        if self._permitted(VKActor.ANALYTICS_READER, "analytics"):
            reads.add(SocialReadOperation.GET_STATISTICS)
        if self._permitted(VKActor.PUBLIC_READER, "audience"):
            reads.add(SocialReadOperation.GET_AUDIENCE)
        if self._permitted(VKActor.NOTIFICATION_READER, "notifications_read"):
            reads.add(SocialReadOperation.LIST_NOTIFICATIONS)
        if self._permitted(VKActor.PUBLIC_READER, "read_public"):
            reads.add(SocialReadOperation.RESOLVE_ITEM)
        checks: Mapping[SocialAction, tuple[tuple[VKActor, str], ...]] = {
            SocialAction.SEND_MESSAGE: ((VKActor.USER_MESSENGER, "dm_send"),),
            SocialAction.PUBLISH: ((VKActor.COMMUNITY_EDITOR, "post_publish"),),
            SocialAction.EDIT: ((VKActor.COMMUNITY_EDITOR, "edit"), (VKActor.USER_MESSENGER, "edit")),
            SocialAction.DELETE: ((VKActor.COMMUNITY_EDITOR, "delete"), (VKActor.USER_MESSENGER, "delete")),
            SocialAction.FORWARD: ((VKActor.COMMUNITY_EDITOR, "forward"), (VKActor.USER_MESSENGER, "forward")),
            SocialAction.REACTION: ((VKActor.COMMUNITY_EDITOR, "reaction"),),
            SocialAction.COMMENT: ((VKActor.COMMUNITY_EDITOR, "comment"),),
            SocialAction.SCHEDULE: ((VKActor.COMMUNITY_EDITOR, "post_publish"),),
            SocialAction.STORY: ((VKActor.STORY_EDITOR, "story_write"),),
        }
        for action, alternatives in checks.items():
            if any(self._permitted(actor, capability) for actor, capability in alternatives):
                actions.add(action)
        if target_ref is not None:
            native = self._resolve_ref("target", target_ref)
            actions.clear()
            if self._dialog_peer_id(native) is not None:
                if self._permitted(VKActor.USER_MESSENGER, "dm_send"):
                    actions.add(SocialAction.SEND_MESSAGE)
                if (
                    kind is not SocialTargetKind.COMMUNITY
                    and self._permitted(VKActor.USER_MESSENGER, "forward")
                ):
                    actions.add(SocialAction.FORWARD)
            if kind is SocialTargetKind.COMMUNITY and self._valid_community_binding(native):
                community_checks = {
                    SocialAction.PUBLISH: "post_publish", SocialAction.SCHEDULE: "post_publish",
                    SocialAction.EDIT: "edit", SocialAction.DELETE: "delete",
                    SocialAction.FORWARD: "forward", SocialAction.REACTION: "reaction",
                    SocialAction.COMMENT: "comment",
                }
                for action, capability in community_checks.items():
                    if self._permitted(VKActor.COMMUNITY_EDITOR, capability):
                        actions.add(action)
                if self._permitted(VKActor.STORY_EDITOR, "story_write"):
                    actions.add(SocialAction.STORY)
        content_features = {ContentFeature.RICH_TEXT, ContentFeature.LINKS}
        if self._permitted(VKActor.MEDIA_EDITOR, "media_upload"):
            content_features.add(ContentFeature.IMAGE)
        return {
            "platform": "vk",
            **({"target_ref": target_ref} if target_ref is not None else {}),
            "target_kinds": [kind.value] if kind else ["self", "user", "group", "community"],
            "read_operations": sorted(operation.value for operation in reads),
            "actions": sorted(action.value for action in actions),
            "content_features": sorted(feature.value for feature in content_features),
            "max_text_length": _MAX_TEXT,
            "max_media_items": 10,
        }

    async def resolve(self, request: SocialReadRequest) -> Mapping[str, Any]:
        if request.platform is not SocialPlatform.VK or request.operation is not SocialReadOperation.RESOLVE_TARGET or request.target_locator is None:
            raise SocialWorkspaceValidationError("VK exact resolution request is required")
        locator = request.target_locator
        expected = set(request.expected_target_kinds)
        if locator.kind is TargetLocatorKind.SELF:
            if expected != {SocialTargetKind.SELF}:
                raise SocialWorkspaceValidationError("self resolution must expect self")
            response = await self._call("get_self", {"fields": "screen_name,status"})
            candidates = _items(response)
            if not candidates:
                raise VKWorkspaceError("target_not_found")
            raw = candidates[0]
            user_id = _int(raw.get("id"))
            if user_id is None:
                raise VKWorkspaceError("target_not_found")
            return self._target_preview({"kind": "self", "user_id": user_id, "peer_id": user_id}, raw)

        value = locator.value or ""
        resolved_type: str | None = None
        native_id: int | None = None
        if locator.kind is TargetLocatorKind.PROVIDER_ID:
            try:
                parsed_id = int(value)
            except ValueError:
                raise SocialWorkspaceValidationError("provider id is invalid") from None
            if SocialTargetKind.USER in expected and parsed_id > 0:
                resolved_type, native_id = "user", parsed_id
            elif expected & {SocialTargetKind.COMMUNITY, SocialTargetKind.GROUP, SocialTargetKind.CHANNEL}:
                resolved_type, native_id = "group", abs(parsed_id)
            else:
                raise SocialWorkspaceValidationError("provider id kind is ambiguous")
        else:
            if locator.kind is TargetLocatorKind.PROFILE_LINK:
                parsed = urlsplit(value)
                if parsed.scheme != "https" or parsed.hostname not in {"vk.com", "www.vk.com"} or parsed.query or parsed.fragment:
                    raise SocialWorkspaceValidationError("profile link is not canonical VK")
                parts = [part for part in parsed.path.split("/") if part]
                if len(parts) != 1:
                    raise SocialWorkspaceValidationError("profile link is not canonical VK")
                value = parts[0]
            value = value.lstrip("@").strip()
            if not _SAFE_HANDLE.fullmatch(value):
                raise SocialWorkspaceValidationError("screen name is invalid")
            response = await self._call("resolve_screen", {"screen_name": value})
            if not isinstance(response, Mapping):
                raise VKWorkspaceError("target_not_found")
            resolved_type = str(response.get("type") or "")
            native_id = _int(response.get("object_id"))
        if native_id is None or native_id <= 0:
            raise VKWorkspaceError("target_not_found")
        if resolved_type in {"group", "page", "event"}:
            if not expected.intersection({SocialTargetKind.COMMUNITY, SocialTargetKind.GROUP, SocialTargetKind.CHANNEL}):
                raise VKWorkspaceError("target_kind_mismatch")
            response = await self._call("get_groups", {"group_ids": str(native_id), "fields": "screen_name,description,activity,site,members_count"})
            candidates = _items(response)
            if not candidates and isinstance(response, Mapping) and isinstance(response.get("groups"), list):
                candidates = [item for item in response["groups"] if isinstance(item, Mapping)]
            if not candidates or _int(candidates[0].get("id")) != native_id:
                raise VKWorkspaceError("target_not_found")
            kind = SocialTargetKind.COMMUNITY
            return self._target_preview({"kind": kind.value, "group_id": native_id, "owner_id": -native_id}, candidates[0])
        if resolved_type not in {"user", "profile"} or SocialTargetKind.USER not in expected:
            raise VKWorkspaceError("target_kind_mismatch")
        response = await self._call("get_users", {"user_ids": str(native_id), "fields": "screen_name,status"})
        candidates = _items(response)
        if not candidates or _int(candidates[0].get("id")) != native_id:
            raise VKWorkspaceError("target_not_found")
        return self._target_preview({"kind": "user", "user_id": native_id, "peer_id": native_id}, candidates[0])

    @staticmethod
    def _parse_post_link(value: str) -> tuple[int, int]:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in {"vk.com", "www.vk.com"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
            or parsed.query
            or parsed.fragment
        ):
            raise SocialWorkspaceValidationError("post link is not canonical VK")
        match = re.fullmatch(r"/wall(-?[1-9][0-9]*)_([1-9][0-9]*)", parsed.path)
        if match is None:
            raise SocialWorkspaceValidationError("post link is not canonical VK")
        owner_id, post_id = int(match.group(1)), int(match.group(2))
        return owner_id, post_id

    async def _resolve_item(self, request: SocialReadRequest) -> Mapping[str, Any]:
        locator = request.target_locator
        if (
            request.read_access is not SocialReadAccess.PUBLIC
            or locator is None
            or locator.kind is not TargetLocatorKind.PROFILE_LINK
            or locator.value is None
        ):
            raise SocialWorkspaceValidationError("VK post-link resolution is required")
        owner_id, post_id = self._parse_post_link(locator.value)
        response = await self._call(
            "wall_item", {"posts": f"{owner_id}_{post_id}", "extended": 0}
        )
        posts = _items(response)
        if not posts:
            raise VKWorkspaceError("item_not_found")
        post = posts[0]
        if _int(post.get("owner_id")) != owner_id or _int(post.get("id")) != post_id:
            raise VKWorkspaceError("item_not_found")

        if owner_id < 0:
            group_id = -owner_id
            source_response = await self._call(
                "get_groups",
                {
                    "group_ids": str(group_id),
                    "fields": "screen_name,description,activity,site,members_count",
                },
            )
            sources = _items(source_response)
            if not sources and isinstance(source_response, Mapping):
                groups = source_response.get("groups")
                if isinstance(groups, list):
                    sources = [item for item in groups if isinstance(item, Mapping)]
            if not sources or _int(sources[0].get("id")) != group_id:
                raise VKWorkspaceError("target_not_found")
            source = sources[0]
            target_ref = self._mint(
                "target",
                {
                    "kind": SocialTargetKind.COMMUNITY.value,
                    "group_id": group_id,
                    "owner_id": owner_id,
                },
            )
            kind = SocialTargetKind.COMMUNITY.value
            title = str(source.get("name") or "VK community")
            about = str(source.get("activity") or source.get("site") or "")
            description = str(source.get("description") or "")
            members = max(0, _int(source.get("members_count")) or 0)
        else:
            source_response = await self._call(
                "get_users", {"user_ids": str(owner_id), "fields": "screen_name,status"}
            )
            sources = _items(source_response)
            if not sources or _int(sources[0].get("id")) != owner_id:
                raise VKWorkspaceError("target_not_found")
            source = sources[0]
            target_ref = self._mint(
                "target",
                {"kind": SocialTargetKind.USER.value, "user_id": owner_id, "peer_id": owner_id},
            )
            kind = SocialTargetKind.USER.value
            title = " ".join(
                part for part in (
                    str(source.get("first_name") or "").strip(),
                    str(source.get("last_name") or "").strip(),
                ) if part
            ) or "VK user"
            about = str(source.get("status") or "")
            description = ""
            members = 0

        handle = str(source.get("screen_name") or "").strip()
        source_target: dict[str, Any] = {
            "target_ref": target_ref,
            "kind": kind,
            "title": self._sanitize(title)[:256] or "VK source",
            "about": self._sanitize(about)[:1024],
            "description": self._sanitize(description)[:1024],
            "basic_metrics": {"members": members},
            "trust": _TRUST,
        }
        if _SAFE_HANDLE.fullmatch(handle):
            source_target["canonical_handle"] = self._sanitize(handle)[:128]
            source_target["profile_link"] = f"https://vk.com/{handle}"
        return {
            "item": self._public_item(
                post,
                native=self._wall_native(owner_id, post_id),
                target_ref=target_ref,
            ),
            "source_target": source_target,
            "trust": _TRUST,
        }

    def _notification_start_from(self, request: SocialReadRequest) -> str | None:
        if request.cursor is None:
            return None
        try:
            state = self._refs.resolve("cursor", request.cursor)
        except Exception:  # noqa: BLE001 - opaque cursor failures are normalized
            raise VKWorkspaceError("cursor_invalid") from None
        expected = self._cursor_context(request)
        actual = (
            str(state.get("operation") or ""),
            str(state.get("resource_fingerprint") or ""),
            str(state.get("query_fingerprint") or ""),
            str(state.get("sample_ref") or ""),
            str(state.get("read_access") or ""),
        )
        start_from = state.get("start_from")
        if (
            actual != expected
            or not isinstance(start_from, str)
            or not start_from
            or len(start_from.encode("utf-8")) > _MAX_PROVIDER_CURSOR_BYTES
            or any(ord(character) < 0x20 for character in start_from)
        ):
            raise VKWorkspaceError("cursor_context_mismatch")
        return start_from

    def _notification_cursor(self, request: SocialReadRequest, start_from: str) -> str:
        if (
            not start_from
            or len(start_from.encode("utf-8")) > _MAX_PROVIDER_CURSOR_BYTES
            or any(ord(character) < 0x20 for character in start_from)
        ):
            raise VKWorkspaceError("cursor_invalid")
        operation, resource, query, sample, read_access = self._cursor_context(request)
        try:
            return self._refs.mint(
                "cursor",
                {
                    "operation": operation,
                    "resource_fingerprint": resource,
                    "query_fingerprint": query,
                    "sample_ref": sample,
                    "read_access": read_access,
                    "start_from": start_from,
                },
            )
        except Exception:  # noqa: BLE001 - opaque cursor failures are normalized
            raise VKWorkspaceError("cursor_invalid") from None

    async def _notifications(self, request: SocialReadRequest) -> Mapping[str, Any]:
        params: dict[str, Any] = {
            "count": min(request.limit, _MAX_RESULT_PAGE),
            "filters": "comments,mentions",
        }
        start_from = self._notification_start_from(request)
        if start_from is not None:
            params["start_from"] = start_from
        if request.date_from is not None:
            params["start_time"] = int(
                datetime.strptime(request.date_from, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
        if request.date_to is not None:
            params["end_time"] = int(
                datetime.strptime(request.date_to, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            ) + 86_399
        response = await self._call("notifications", params)
        results: list[dict[str, Any]] = []
        for raw in _items(response):
            notification_type = str(raw.get("type") or "")
            if notification_type not in {
                "comment_post",
                "reply_comment",
                "mention_comments",
            }:
                continue
            parent = raw.get("parent")
            feedback = raw.get("feedback")
            if not isinstance(parent, Mapping) or not isinstance(feedback, Mapping):
                continue
            owner_id = _int(parent.get("owner_id"))
            post_id = _int(parent.get("id")) or _int(parent.get("post_id"))
            comment_id = _int(feedback.get("id"))
            if (
                owner_id is None
                or owner_id == 0
                or post_id is None
                or post_id <= 0
                or comment_id is None
                or comment_id <= 0
            ):
                continue
            text = self._sanitize(str(feedback.get("text") or ""))[:_MAX_TEXT]
            if not text.strip():
                continue
            results.append(
                {
                    "item_ref": self._mint(
                        "item",
                        {
                            "kind": "comment",
                            "owner_id": owner_id,
                            "post_id": post_id,
                            "comment_id": comment_id,
                        },
                    ),
                    "root_item_ref": self._mint(
                        "item", self._wall_native(owner_id, post_id)
                    ),
                    "kind": SocialItemKind.COMMENT.value,
                    "published_at": _utc(feedback.get("date") or raw.get("date")),
                    "text": text,
                    "source_kind": (
                        "mention" if "mention" in notification_type else "comment"
                    ),
                    "trust": _TRUST,
                }
            )
            if len(results) >= min(request.limit, _MAX_RESULT_PAGE):
                break
        output: dict[str, Any] = {"results": results, "trust": _TRUST}
        if isinstance(response, Mapping):
            next_from = response.get("next_from")
            if isinstance(next_from, str) and next_from:
                output["next_cursor"] = self._notification_cursor(request, next_from)
        return output

    def _metrics(self, item: Mapping[str, Any]) -> dict[str, int]:
        def count(field: str) -> int:
            value = item.get(field)
            if isinstance(value, Mapping):
                value = value.get("count")
            return max(0, value) if type(value) is int else 0
        return {"views": count("views"), "reactions": count("likes"), "comments": count("comments"), "shares": count("reposts")}

    def _story_media_ref(self, raw: Mapping[str, Any]) -> str | None:
        source_url: str | None = None
        role: str | None = None
        mime_type: str | None = None
        width: int | None = None
        height: int | None = None
        photo = raw.get("photo")
        if isinstance(photo, Mapping):
            candidates: list[tuple[int, str, int | None, int | None]] = []
            sizes = photo.get("sizes")
            if isinstance(sizes, list):
                for size in sizes:
                    if not isinstance(size, Mapping):
                        continue
                    url = size.get("url")
                    candidate_width = _int(size.get("width"))
                    candidate_height = _int(size.get("height"))
                    if isinstance(url, str):
                        area = max(0, candidate_width or 0) * max(0, candidate_height or 0)
                        candidates.append((area, url, candidate_width, candidate_height))
            if candidates:
                _area, source_url, width, height = max(candidates, key=lambda value: value[0])
                role, mime_type = "image", "image/jpeg"
        video = raw.get("video")
        if source_url is None and isinstance(video, Mapping):
            files = video.get("files")
            if isinstance(files, Mapping):
                ranked: list[tuple[int, str]] = []
                for key, value in files.items():
                    match = re.fullmatch(r"mp4_(\d{3,4})", str(key))
                    if match and isinstance(value, str):
                        ranked.append((int(match.group(1)), value))
                if ranked:
                    _quality, source_url = max(ranked)
                    width, height = _int(video.get("width")), _int(video.get("height"))
                    role, mime_type = "video", "video/mp4"
        if source_url is None or role is None or mime_type is None:
            return None
        safe_url = _validated_vk_https_url(source_url)
        return self._mint(
            "asset",
            {
                "binding_kind": "story_media",
                "source_url": safe_url,
                "role": role,
                "mime_type": mime_type,
                "expires_at": _int(raw.get("expires_at")),
                "width": width,
                "height": height,
            },
        )

    def _public_item(self, raw: Mapping[str, Any], *, native: Mapping[str, Any], kind: SocialItemKind = SocialItemKind.POST, target_ref: str | None = None, caption: str = "") -> dict[str, Any]:
        item_ref = self._mint("item", native)
        text = self._sanitize(str(raw.get("text") or ""))[:_MAX_TEXT]
        output: dict[str, Any] = {
            "item_ref": item_ref,
            "kind": kind.value,
            "published_at": _utc(raw.get("date")),
            "text": text,
            "caption": self._sanitize(caption)[:1024],
            "basic_metrics": self._metrics(raw),
            "trust": _TRUST,
        }
        if target_ref is not None:
            output["target_ref"] = target_ref
        media: list[str] = []
        details: list[dict[str, Any]] = []
        attachments = raw.get("attachments")
        for attachment in (
            attachments if isinstance(attachments, list) else []
        )[:10]:
            if not isinstance(attachment, Mapping) or attachment.get("type") != "photo":
                continue
            try:
                asset_ref = self._story_media_ref(attachment)
            except VKWorkspaceError:
                continue
            if asset_ref is None:
                continue
            media.append(asset_ref)
            details.append(
                {
                    "asset_ref": asset_ref,
                    "kind": "photo",
                    "mime_type": "image/jpeg",
                    "trust": _TRUST,
                }
            )
        if media:
            output["media"] = media
            output["attachments"] = details
            output["media_details"] = details
        return output

    @staticmethod
    def _within_date_bounds(item: Mapping[str, Any], request: SocialReadRequest) -> bool:
        published = item.get("published_at")
        if not isinstance(published, str) or len(published) < 10:
            return False
        day = published[:10]
        return not (
            (request.date_from is not None and day < request.date_from)
            or (request.date_to is not None and day > request.date_to)
        )

    async def _discover(self, request: SocialReadRequest) -> Mapping[str, Any]:
        tokens = [token for token in re.findall(r"[\w.-]{2,}", request.query or "", re.UNICODE)][:3]
        queries = [request.query or ""] + sorted(set(tokens), key=lambda token: (token.casefold(), token))
        seen: dict[int, Mapping[str, Any]] = {}
        for query in queries[:3]:
            response = await self._call("search_groups", {"q": query[:500], "count": min(_MAX_RESULT_PAGE, request.limit), "offset": 0, "type": "group"})
            for raw in _items(response):
                group_id = _int(raw.get("id"))
                if group_id is not None and group_id > 0:
                    seen.setdefault(group_id, raw)
                if len(seen) >= _MAX_RESULT_PAGE:
                    break
        ranked = sorted(seen.items(), key=lambda pair: (str(pair[1].get("name") or "").casefold(), str(pair[1].get("screen_name") or "").casefold(), pair[0]))[: min(request.limit, _MAX_RESULT_PAGE)]
        results = []
        for group_id, raw in ranked:
            preview = self._target_preview({"kind": "community", "group_id": group_id, "owner_id": -group_id}, raw, exact=False)
            results.append({
                "target_ref": preview["target_ref"], "kind": "community", "title": preview["display_name"],
                **({key: preview[key] for key in ("canonical_handle", "profile_link") if key in preview}),
                "about": self._sanitize(str(raw.get("activity") or ""))[:1024],
                "description": self._sanitize(str(raw.get("description") or ""))[:1024],
                "basic_metrics": {"members": max(0, _int(raw.get("members_count")) or 0)}, "trust": _TRUST,
            })
        return {"results": results, "trust": _TRUST}

    async def _dialogs(self, request: SocialReadRequest) -> Mapping[str, Any]:
        """Return dialog identity metadata without projecting message content."""

        if request.read_access is not SocialReadAccess.DIALOGS:
            raise VKWorkspaceError("access_target_mismatch")
        limit = min(request.limit, _MAX_RESULT_PAGE)
        offset = self._offset(request)
        response = await self._call(
            "conversations",
            {
                "count": limit,
                "offset": offset,
                "filter": "unread" if request.unread_only else "all",
                "extended": 1,
                "fields": "screen_name",
            },
        )
        if not isinstance(response, Mapping):
            raise VKWorkspaceError("provider_response_invalid")
        profiles = {
            _int(raw.get("id")): raw
            for raw in response.get("profiles", [])
            if isinstance(raw, Mapping) and _int(raw.get("id")) is not None
        }
        groups = {
            _int(raw.get("id")): raw
            for raw in response.get("groups", [])
            if isinstance(raw, Mapping) and _int(raw.get("id")) is not None
        }
        provider_items = _items(response)
        results: list[dict[str, Any]] = []
        for entry in provider_items:
            conversation = entry.get("conversation")
            if not isinstance(conversation, Mapping):
                continue
            peer = conversation.get("peer")
            if not isinstance(peer, Mapping):
                continue
            peer_id = _int(peer.get("id"))
            peer_type = str(peer.get("type") or "")
            unread_count = _int(conversation.get("unread_count"))
            if unread_count is None or unread_count < 0:
                unread_count = 0
            if request.unread_only and unread_count == 0:
                continue

            native: dict[str, Any]
            title: str
            kind: SocialTargetKind
            if peer_type == "user" and peer_id is not None and peer_id > 0:
                profile = profiles.get(peer_id, {})
                first = str(profile.get("first_name") or "").strip()
                last = str(profile.get("last_name") or "").strip()
                title = " ".join(part for part in (first, last) if part) or "VK user"
                kind = SocialTargetKind.USER
                native = {"kind": kind.value, "user_id": peer_id, "peer_id": peer_id}
            elif peer_type == "group" and peer_id is not None and peer_id < 0:
                group_id = abs(peer_id)
                group = groups.get(group_id, {})
                title = str(group.get("name") or "VK community").strip()
                kind = SocialTargetKind.COMMUNITY
                native = {
                    "kind": kind.value,
                    "group_id": group_id,
                    "owner_id": -group_id,
                    "peer_id": -group_id,
                }
            elif peer_type == "chat" and peer_id is not None and peer_id > 2_000_000_000:
                settings = conversation.get("chat_settings")
                title = (
                    str(settings.get("title") or "").strip()
                    if isinstance(settings, Mapping)
                    else ""
                ) or "VK group chat"
                kind = SocialTargetKind.CHAT
                native = {"kind": kind.value, "peer_id": peer_id}
            else:
                continue
            results.append(
                {
                    "target_ref": self._mint("target", native),
                    "kind": kind.value,
                    "title": self._sanitize(title)[:256] or "VK dialog",
                    "unread_count": unread_count,
                    "trust": _TRUST,
                }
            )
            if len(results) >= limit:
                break
        output: dict[str, Any] = {"results": results, "trust": _TRUST}
        if len(provider_items) == limit:
            output["next_cursor"] = self._cursor(request, offset + limit)
        return output

    async def _editorial(self, request: SocialReadRequest) -> Mapping[str, Any]:
        if request.read_access is not SocialReadAccess.PUBLIC:
            raise VKWorkspaceError("access_target_mismatch")
        native = self._resolve_ref("target", request.target_ref or "")
        if self._target_kind(native) not in {SocialTargetKind.COMMUNITY, SocialTargetKind.GROUP, SocialTargetKind.CHANNEL}:
            raise VKWorkspaceError("target_kind_mismatch")
        group_id = _int(native.get("group_id"))
        owner_id = _int(native.get("owner_id"))
        if group_id is None or owner_id is None:
            raise VKWorkspaceError("opaque_reference_failed")
        page_size = min(request.page_size, _MAX_RESULT_PAGE)
        offset = self._offset(request)
        sample_ref = request.sample_ref
        if sample_ref is None:
            sample_ref = self._refs.mint(
                "sample", {"target_ref": request.target_ref, "cumulative": 0}
            )
        try:
            sample_state = self._refs.resolve("sample", sample_ref)
        except Exception:  # noqa: BLE001 - initialize only the runtime-bound sample ref
            put_named = getattr(self._refs, "put_named", None)
            if request.sample_ref is None or not callable(put_named):
                raise VKWorkspaceError("sample_state_invalid") from None
            put_named(
                "sample",
                sample_ref,
                {"target_ref": request.target_ref, "cumulative": 0},
            )
            sample_state = self._refs.resolve("sample", sample_ref)
        if sample_state.get("target_ref") != request.target_ref:
            raise VKWorkspaceError("sample_state_invalid")
        cumulative = _int(sample_state.get("cumulative")) or 0
        remaining = min(request.total_limit, _MAX_EDITORIAL_TOTAL) - cumulative
        count = min(page_size, max(0, remaining))
        group_response, wall_response = await asyncio.gather(
            self._call("get_groups", {"group_ids": str(group_id), "fields": "screen_name,description,activity,site,members_count"}),
            self._call("wall_feed", {"owner_id": owner_id, "count": count, "offset": offset, "filter": "owner"}),
        )
        groups = _items(group_response)
        if not groups and isinstance(group_response, Mapping) and isinstance(group_response.get("groups"), list):
            groups = [item for item in group_response["groups"] if isinstance(item, Mapping)]
        if not groups:
            raise VKWorkspaceError("target_not_found")
        metadata = groups[0]
        selected = []
        for raw in _items(wall_response):
            # Provider-neutral schema extension for these flags is integrated separately.
            if raw.get("marked_as_ads") in {1} or raw.get("is_pinned") in {1} or raw.get("copy_history"):
                continue
            published = _utc(raw.get("date"))
            date = published[:10]
            if request.date_from and date < request.date_from:
                continue
            if request.date_to and date > request.date_to:
                continue
            post_id = _int(raw.get("id"))
            if post_id is None:
                continue
            item = self._public_item(raw, native=self._wall_native(owner_id, post_id))
            # Editorial sampling has its own deliberately text-only contract;
            # ordinary item/feed reads retain the safe opaque media projection.
            item.pop("media", None)
            item.pop("attachments", None)
            item.pop("media_details", None)
            item["text"] = item["text"][:768]
            item["caption"] = item["caption"][:256]
            selected.append(item)
        cumulative += len(selected)
        put_named = getattr(self._refs, "put_named", None)
        if not callable(put_named):
            raise VKWorkspaceError("sample_state_invalid")
        put_named(
            "sample",
            sample_ref,
            {"target_ref": request.target_ref, "cumulative": cumulative},
        )
        output: dict[str, Any] = {
            "sample_ref": sample_ref,
            "target": {
                "target_ref": request.target_ref,
                "kind": "community",
                "title": self._sanitize(str(metadata.get("name") or "VK community"))[:256],
                "about": self._sanitize(str(metadata.get("activity") or metadata.get("site") or ""))[:1024],
                "description": self._sanitize(str(metadata.get("description") or ""))[:1024],
                "basic_metrics": {"members": max(0, _int(metadata.get("members_count")) or 0)},
                "trust": _TRUST,
            },
            "items": selected,
            "sampled_count": len(selected),
            "cumulative_count": cumulative,
            "total_limit": request.total_limit,
            "storage_disposition": "ephemeral_no_index",
            "trust": _TRUST,
        }
        if len(_items(wall_response)) == count and cumulative < request.total_limit:
            output["next_cursor"] = self._cursor(request, offset + count, sample_ref=sample_ref)
        return output

    async def read(self, request: SocialReadRequest) -> Mapping[str, Any]:
        if request.platform is not SocialPlatform.VK:
            raise SocialWorkspaceValidationError("VK request is required")
        if request.operation is SocialReadOperation.RESOLVE_TARGET:
            return await self.resolve(request)
        if request.operation is SocialReadOperation.RESOLVE_ITEM:
            return await self._resolve_item(request)
        if request.operation is SocialReadOperation.LIST_NOTIFICATIONS:
            return await self._notifications(request)
        if request.operation is SocialReadOperation.SEARCH_TARGETS:
            return await self._discover(request)
        if request.operation is SocialReadOperation.LIST_DIALOGS:
            return await self._dialogs(request)
        if request.operation is SocialReadOperation.EDITORIAL_SAMPLE:
            return await self._editorial(request)
        limit = min(request.limit, _MAX_RESULT_PAGE)
        offset = self._offset(request)

        if request.operation in {SocialReadOperation.LIST_ITEMS, SocialReadOperation.SEARCH_ITEMS}:
            if request.target_ref:
                native = self._resolve_ref("target", request.target_ref)
                kind = self._target_kind(native)
                if request.read_access is SocialReadAccess.DIALOGS:
                    if kind not in {
                        SocialTargetKind.USER,
                        SocialTargetKind.CHAT,
                        SocialTargetKind.COMMUNITY,
                        SocialTargetKind.SELF,
                    }:
                        raise VKWorkspaceError("access_target_mismatch")
                    if kind is SocialTargetKind.SELF:
                        response = await self._call("conversations", {"count": limit, "offset": offset, "filter": "all"})
                        raws = []
                        for entry in _items(response):
                            message = entry.get("last_message")
                            if isinstance(message, Mapping):
                                raws.append(message)
                        results = []
                        for raw in raws:
                            peer_id, message_id = _int(raw.get("peer_id")), _int(raw.get("id"))
                            if peer_id is not None and message_id is not None:
                                results.append(self._public_item(raw, native={"kind": "message", "peer_id": peer_id, "message_id": message_id}, kind=SocialItemKind.MESSAGE))
                        output = {"results": results[:limit], "trust": _TRUST}
                        if len(_items(response)) == limit:
                            output["next_cursor"] = self._cursor(request, offset + limit)
                        return output
                    peer_id = self._dialog_peer_id(native)
                    if peer_id is None:
                        raise VKWorkspaceError("opaque_reference_failed")
                    response = await self._call("dialog_history", {"peer_id": peer_id, "count": limit, "offset": offset, "rev": 0})
                    raws = _items(response)
                    results = [self._public_item(raw, native={"kind": "message", "peer_id": peer_id, "message_id": _int(raw.get("id"))}, kind=SocialItemKind.MESSAGE, target_ref=request.target_ref) for raw in raws if _int(raw.get("id")) is not None]
                else:
                    if request.read_access is not SocialReadAccess.PUBLIC or kind in {SocialTargetKind.CHAT, SocialTargetKind.SELF}:
                        raise VKWorkspaceError("access_target_mismatch")
                    owner_id = _int(native.get("owner_id")) or _int(native.get("user_id"))
                    if owner_id is None:
                        raise VKWorkspaceError("opaque_reference_failed")
                    if request.operation is SocialReadOperation.SEARCH_ITEMS:
                        response = await self._call("wall_search", {"owner_id": owner_id, "query": request.query or "", "count": limit, "offset": offset, "owners_only": 1})
                    else:
                        response = await self._call("wall_feed", {"owner_id": owner_id, "count": limit, "offset": offset, "filter": "owner"})
                    raws = _items(response)
                    results = [self._public_item(raw, native=self._wall_native(owner_id, _int(raw.get("id"))), target_ref=request.target_ref) for raw in raws if _int(raw.get("id")) is not None]
            else:
                if request.read_access is not SocialReadAccess.PUBLIC:
                    raise VKWorkspaceError("access_target_mismatch")
                response = await self._call("newsfeed_search", {"q": request.query or "", "count": limit, **({"start_from": request.cursor} if request.cursor else {})})
                raws = _items(response)
                results = []
                for raw in raws:
                    owner_id, post_id = _int(raw.get("owner_id")), _int(raw.get("id"))
                    if owner_id is not None and post_id is not None:
                        results.append(self._public_item(raw, native=self._wall_native(owner_id, post_id)))
            results = [
                item for item in results if self._within_date_bounds(item, request)
            ]
            output: dict[str, Any] = {"results": results[:limit], "trust": _TRUST}
            if len(raws) == limit and request.target_ref:
                output["next_cursor"] = self._cursor(request, offset + limit)
            return output

        if request.operation is SocialReadOperation.GET_ITEM:
            native = self._resolve_ref("item", request.item_ref or "")
            if native.get("kind") == "message":
                if request.read_access is not SocialReadAccess.DIALOGS:
                    raise VKWorkspaceError("access_target_mismatch")
                response = await self._call("message_item", {"message_ids": str(native.get("message_id"))})
                kind = SocialItemKind.MESSAGE
            elif native.get("kind") == "post":
                if request.read_access is not SocialReadAccess.PUBLIC:
                    raise VKWorkspaceError("access_target_mismatch")
                response = await self._call("wall_item", {"posts": f"{native.get('owner_id')}_{native.get('post_id')}", "extended": 0})
                kind = SocialItemKind.POST
            else:
                raise VKWorkspaceError("access_target_mismatch")
            raws = _items(response)
            if not raws:
                raise VKWorkspaceError("item_not_found")
            return {"item": self._public_item(raws[0], native=native, kind=kind), "trust": _TRUST}

        if request.operation is SocialReadOperation.LIST_COMMENTS:
            native = self._resolve_ref("item", request.item_ref or "")
            if request.read_access is not SocialReadAccess.PUBLIC or native.get("kind") != "post":
                raise VKWorkspaceError("access_target_mismatch")
            response = await self._call("wall_comments", {"owner_id": native.get("owner_id"), "post_id": native.get("post_id"), "count": limit, "offset": offset, "extended": 0, "sort": "asc"})
            results = [self._public_item(raw, native={"kind": "comment", "owner_id": native.get("owner_id"), "post_id": native.get("post_id"), "comment_id": _int(raw.get("id"))}, kind=SocialItemKind.COMMENT) for raw in _items(response) if _int(raw.get("id")) is not None]
            return {"root_item_ref": request.item_ref, "items": results, "trust": _TRUST}

        if request.operation is SocialReadOperation.LIST_REACTIONS:
            native = self._resolve_ref("item", request.item_ref or "")
            if request.read_access is not SocialReadAccess.PUBLIC or native.get("kind") != "post":
                raise VKWorkspaceError("access_target_mismatch")
            response = await self._call("wall_likes", {"type": "post", "owner_id": native.get("owner_id"), "item_id": native.get("post_id"), "count": 1, "extended": 0})
            count = response.get("count", 0) if isinstance(response, Mapping) else 0
            return {"item_ref": request.item_ref, "reactions": [{"reaction": "like", "count": max(0, count) if type(count) is int else 0}], "trust": _TRUST}

        if request.operation is SocialReadOperation.LIST_STORIES:
            native = self._resolve_ref("target", request.target_ref or "")
            owner = _int(native.get("owner_id")) or _int(native.get("user_id"))
            if owner is None:
                raise VKWorkspaceError("target_binding_invalid")
            response = await self._call("stories", {"owner_id": owner})
            results = []
            for raw in _story_feed_items(response):
                story_id = _int(raw.get("id"))
                story_owner = _int(raw.get("owner_id")) or owner
                if story_id is None or story_owner != owner:
                    continue
                media_ref = self._story_media_ref(raw)
                # A caption-only story is not inspectable by the MCP client.
                if media_ref is None:
                    continue
                item = self._public_item(raw, native={"kind": "story", "owner_id": owner, "story_id": story_id}, kind=SocialItemKind.STORY, target_ref=request.target_ref)
                if media_ref is not None:
                    item["media"] = [media_ref]
                results.append(item)
            return {"results": results[:limit], "trust": _TRUST}

        if request.operation is SocialReadOperation.GET_AUDIENCE:
            native = self._resolve_ref("target", request.target_ref or "")
            response = await self._call("audience", {"group_id": native.get("group_id"), "count": 0})
            count = response.get("count", 0) if isinstance(response, Mapping) else 0
            return {"target_ref": request.target_ref, "audience": {"total": max(0, count) if type(count) is int else 0}, "trust": _TRUST}

        if request.operation is SocialReadOperation.GET_STATISTICS:
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            start = (request.date_from + "T00:00:00Z") if request.date_from else now
            end = (request.date_to + "T23:59:59Z") if request.date_to else now
            if request.item_ref:
                native = self._resolve_ref("item", request.item_ref)
                if native.get("kind") == "story":
                    response = await self._call("story_stats", {"owner_id": native.get("owner_id"), "story_id": native.get("story_id")})
                    metrics = {
                        "views": _story_stat_count(response, "views"),
                        "reactions": _story_stat_count(response, "likes"),
                        "comments": _story_stat_count(response, "replies"),
                        "shares": _story_stat_count(response, "shares"),
                    }
                else:
                    response = await self._call("wall_item", {"posts": f"{native.get('owner_id')}_{native.get('post_id')}", "extended": 0})
                    raws = _items(response)
                    metrics = self._metrics(raws[0] if raws else {})
                return {"item_ref": request.item_ref, "period_from": start, "period_to": end, "basic_metrics": metrics, "trust": _TRUST}
            native = self._resolve_ref("target", request.target_ref or "")
            params: dict[str, Any] = {"group_id": native.get("group_id"), "interval": "day"}
            if request.date_from:
                params["timestamp_from"] = int(
                    datetime.fromisoformat(request.date_from + "T00:00:00+00:00").timestamp()
                )
            if request.date_to:
                params["timestamp_to"] = int(
                    datetime.fromisoformat(request.date_to + "T23:59:59+00:00").timestamp()
                )
            if not request.date_from and not request.date_to:
                params["intervals_count"] = 1
            response = await self._call("community_stats", params)
            metrics = {"views": 0, "reactions": 0, "comments": 0, "shares": 0}
            for row in _items(response):
                visitors = row.get("visitors")
                activity = row.get("activity")
                if isinstance(visitors, Mapping):
                    views = _int(visitors.get("views"))
                    if views is not None and views > 0:
                        metrics["views"] += views
                if isinstance(activity, Mapping):
                    for key, source in (
                        ("reactions", "likes"),
                        ("comments", "comments"),
                        ("shares", "copies"),
                    ):
                        value = _int(activity.get(source))
                        if value is not None and value > 0:
                            metrics[key] += value
            return {"target_ref": request.target_ref, "period_from": start, "period_to": end, "basic_metrics": metrics, "trust": _TRUST}
        raise SocialWorkspaceValidationError("unsupported VK read operation")

    @staticmethod
    def _scheduled_bound(value: str | None, field: str) -> int | None:
        if value is None:
            return None
        if not isinstance(value, str) or len(value) > 64:
            raise SocialWorkspaceValidationError(f"{field} is invalid")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise SocialWorkspaceValidationError(f"{field} is invalid") from None
        if parsed.tzinfo is None:
            raise SocialWorkspaceValidationError(f"{field} is invalid")
        return int(parsed.timestamp())

    @staticmethod
    def _postponed_items(response: Any) -> list[Mapping[str, Any]]:
        if not isinstance(response, Mapping) or not isinstance(
            response.get("items"), list
        ):
            raise VKWorkspaceError("provider_response_invalid")
        return [
            item for item in response["items"] if isinstance(item, Mapping)
        ]

    async def scheduled_items(
        self,
        *,
        target_ref: str,
        scheduled_from: str | None = None,
        scheduled_to: str | None = None,
        text_sha256: str | None = None,
        media_count: int | None = None,
        limit: int = 10,
    ) -> Mapping[str, Any]:
        """Return bounded logical items from one exact VK postponed queue."""

        if type(limit) is not int or not 1 <= limit <= _MAX_RESULT_PAGE:
            raise SocialWorkspaceValidationError("limit is invalid")
        if text_sha256 is not None and (
            not isinstance(text_sha256, str)
            or not re.fullmatch(r"[a-f0-9]{64}", text_sha256)
        ):
            raise SocialWorkspaceValidationError("text_sha256 is invalid")
        if media_count is not None and (
            type(media_count) is not int or not 0 <= media_count <= 10
        ):
            raise SocialWorkspaceValidationError("media_count is invalid")
        start = self._scheduled_bound(scheduled_from, "scheduled_from")
        end = self._scheduled_bound(scheduled_to, "scheduled_to")
        if start is not None and end is not None and start > end:
            raise SocialWorkspaceValidationError("scheduled range is invalid")
        target = self._resolve_ref("target", target_ref)
        if not self._valid_community_binding(target):
            raise VKWorkspaceError("community_binding_invalid")
        response = await self._call(
            "wall_feed_editor",
            {
                "owner_id": target["owner_id"],
                "count": 100,
                "offset": 0,
                "filter": "postponed",
            },
        )
        logical: list[dict[str, Any]] = []
        role_by_type = {
            "photo": MediaRole.IMAGE.value,
            "video": MediaRole.VIDEO.value,
            "doc": MediaRole.DOCUMENT.value,
            "audio": MediaRole.AUDIO.value,
        }
        for raw in self._postponed_items(response)[:100]:
            post_id = _int(raw.get("id"))
            owner_id = _int(raw.get("owner_id"))
            scheduled = _int(raw.get("date"))
            if (
                post_id is None
                or post_id <= 0
                or owner_id != target["owner_id"]
                or scheduled is None
                or (start is not None and scheduled < start)
                or (end is not None and scheduled > end)
            ):
                continue
            digest = hashlib.sha256(
                self._match_text(str(raw.get("text") or "")).encode("utf-8")
            ).hexdigest()
            attachments = raw.get("attachments")
            roles = [
                role_by_type[str(attachment.get("type"))]
                for attachment in (
                    attachments if isinstance(attachments, list) else []
                )[:10]
                if isinstance(attachment, Mapping)
                and str(attachment.get("type")) in role_by_type
            ]
            if text_sha256 is not None and digest != text_sha256:
                continue
            if media_count is not None and len(roles) != media_count:
                continue
            native = self._wall_native(owner_id, post_id)
            native["queue"] = "postponed"
            logical.append(
                {
                    "item_ref": self._mint("item", native),
                    "target_ref": target_ref,
                    "queue": "scheduled",
                    "scheduled_at": _utc(scheduled),
                    "text_sha256": digest,
                    "media_count": len(roles),
                    "media_roles": roles,
                    "trust": _TRUST,
                }
            )
        return {
            "platform": "vk",
            "target_ref": target_ref,
            "queue": "scheduled",
            "items": logical[:limit],
            "exact_match_count": len(logical),
            "has_more": len(logical) > limit,
            "trust": _TRUST,
        }

    def _content(self, intent: SocialActionIntent) -> tuple[str, list[Mapping[str, Any]]]:
        content = intent.content
        if content is None:
            return "", []
        bindings: list[Mapping[str, Any]] = []
        for media in content.media:
            binding = self._resolve_ref("asset", media.asset_ref)
            role = binding.get("role")
            if role != media.role.value:
                raise VKWorkspaceError("asset_role_mismatch")
            if binding.get("binding_kind") != "verified_asset":
                attachment = binding.get("attachment")
                if not isinstance(attachment, str) or not re.fullmatch(r"(?:photo|video|doc|audio|audio_message|graffiti|album)-?\d+_\d+(?:_[A-Za-z0-9]+)?", attachment):
                    raise VKWorkspaceError("asset_not_ready")
            bindings.append(binding)
        return content.text, bindings

    def durable_intent_evidence(
        self, intent: SocialActionIntent
    ) -> tuple[str, list[str]]:
        text, bindings = self._content(intent)
        content_fingerprint = hashlib.sha256(
            self._match_text(text).encode("utf-8")
        ).hexdigest()
        media_digests: list[str] = []
        for binding in bindings:
            digest = binding.get("content_digest")
            if isinstance(digest, str) and _CONTENT_DIGEST.fullmatch(digest):
                media_digests.append(digest)
            else:
                attachment = str(binding.get("attachment") or "")
                media_digests.append(
                    "sha256:" + hashlib.sha256(attachment.encode()).hexdigest()
                )
        return content_fingerprint, media_digests

    @staticmethod
    def _legacy_attachments(bindings: Sequence[Mapping[str, Any]]) -> str | None:
        values: list[str] = []
        for binding in bindings:
            attachment = binding.get("attachment")
            if not isinstance(attachment, str):
                raise VKWorkspaceError("asset_not_supported_for_action")
            values.append(attachment)
        return ",".join(values) if values else None

    def _ensure_operation_permitted(self, operation: str) -> None:
        policy = _CALLS[operation]
        if not self._permitted(policy.actor, policy.capability):
            raise VKWorkspaceError("actor_capability_denied")

    async def _wall_attachments(
        self,
        bindings: Sequence[Mapping[str, Any]],
        *,
        group_id: int,
        attempt_hook: Any,
    ) -> str | None:
        attachments: list[str] = []
        for image_ordinal, binding in enumerate(bindings, start=1):
            if binding.get("binding_kind") != "verified_asset":
                attachment = binding.get("attachment")
                if not isinstance(attachment, str):
                    raise VKWorkspaceError("asset_not_ready")
                attachments.append(attachment)
                continue
            if binding.get("role") != MediaRole.IMAGE.value:
                raise VKWorkspaceError("asset_not_supported_for_action")
            self._ensure_operation_permitted("wall_photo_upload_server")
            self._ensure_operation_permitted("wall_photo_save")
            materialized = await self._materialize_asset(binding)
            server_response = await self._call(
                "wall_photo_upload_server",
                {"group_id": group_id},
                attempt_hook=attempt_hook,
            )
            upload_url = server_response.get("upload_url") if isinstance(server_response, Mapping) else None
            upload = await self._upload_multipart(
                purpose=VKUploadPurpose.WALL_PHOTO,
                upload_url=upload_url,
                materialized=materialized,
                attempt_hook=attempt_hook,
                image_ordinal=image_ordinal,
            )
            if (
                type(upload.server) is not int
                or not isinstance(upload.photo, str)
                or not upload.photo
                or not isinstance(upload.upload_hash, str)
                or not upload.upload_hash
            ):
                raise VKWorkspaceError(
                    "media_upload_response_invalid",
                    retry_safe=True,
                    stage="wall_photo_multipart",
                )
            saved = await self._call(
                "wall_photo_save",
                {
                    "group_id": group_id,
                    "photo": upload.photo,
                    "server": upload.server,
                    "hash": upload.upload_hash,
                },
                attempt_hook=attempt_hook,
            )
            photos = _items(saved)
            if not photos:
                raise VKWorkspaceError(
                    "photo_save_response_invalid",
                    retry_safe=True,
                    stage="wall_photo_save",
                )
            owner_id = _int(photos[0].get("owner_id"))
            photo_id = _int(photos[0].get("id"))
            access_key = photos[0].get("access_key")
            if owner_id is None or photo_id is None:
                raise VKWorkspaceError(
                    "photo_save_response_invalid",
                    retry_safe=True,
                    stage="wall_photo_save",
                )
            attachment = f"photo{owner_id}_{photo_id}"
            if isinstance(access_key, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,256}", access_key):
                attachment += "_" + access_key
            attachments.append(attachment)
        return ",".join(attachments) if attachments else None

    @staticmethod
    def _operation_ref(intent: SocialActionIntent, action_digest: str) -> str:
        digest = hashlib.sha256((intent.idempotency_key + "\0" + action_digest).encode()).hexdigest()
        return "op_" + digest[:32]

    async def execute(
        self, intent: SocialActionIntent, *, operation_ref: str | None = None
    ) -> Mapping[str, Any]:
        if intent.platform is not SocialPlatform.VK:
            raise SocialWorkspaceValidationError("VK action is required")
        if operation_ref is not None and not re.fullmatch(r"op_[A-Za-z0-9_-]{24,160}", operation_ref):
            raise SocialWorkspaceValidationError("operation_ref is invalid")
        action_digest = compute_action_digest(intent)
        async with self._action_lock:
            existing = self._idempotency.get(intent.idempotency_key)
            if existing is not None:
                existing_digest, existing_ref = existing
                if not hmac.compare_digest(existing_digest, action_digest):
                    raise VKWorkspaceError("idempotency_conflict")
                if operation_ref is not None and operation_ref != existing_ref:
                    raise VKWorkspaceError("operation_ref_conflict")
                receipt = self._operations.get(existing_ref)
                if receipt is None:
                    raise VKWorkspaceError("operation_in_progress")
                return dict(receipt)
            claimed_ref = operation_ref or self._operation_ref(intent, action_digest)
            prior_claim = self._operation_claims.get(claimed_ref)
            claim = (intent.idempotency_key, action_digest)
            if prior_claim is not None and prior_claim != claim:
                raise VKWorkspaceError("operation_ref_conflict")
            self._idempotency[intent.idempotency_key] = (action_digest, claimed_ref)
            self._operation_claims[claimed_ref] = claim
            try:
                return await self._execute_once(intent, claimed_ref)
            except VKWorkspaceError as exc:
                status = SocialActionStatus.OUTCOME_UNKNOWN if exc.outcome_unknown else SocialActionStatus.FAILED
                receipt = {"platform": "vk", "operation_ref": claimed_ref, "action": intent.action.value, "status": status.value, "retry_safe": exc.retry_safe, "error_code": exc.code}
                if exc.stage:
                    receipt["stage"] = exc.stage
                self._operations[claimed_ref] = receipt
                return dict(receipt)

    async def retry(
        self,
        intent: SocialActionIntent,
        *,
        operation_ref: str,
        attempt_number: int,
    ) -> Mapping[str, Any]:
        """Retry one proven-safe pre-wall failure under its logical operation.

        The durable runtime owns attempt allocation/CAS. This in-process guard
        independently prevents concurrent adapter retries and rejects any
        replay after the VK wall mutation boundary may have been crossed.
        """

        if intent.platform is not SocialPlatform.VK:
            raise SocialWorkspaceValidationError("VK action is required")
        if not re.fullmatch(r"op_[A-Za-z0-9_-]{24,160}", operation_ref):
            raise SocialWorkspaceValidationError("operation_ref is invalid")
        if type(attempt_number) is not int or not 2 <= attempt_number <= 100:
            raise SocialWorkspaceValidationError("attempt_number is invalid")
        digest = compute_action_digest(intent)
        async with self._action_lock:
            existing = self._idempotency.get(intent.idempotency_key)
            claim = (intent.idempotency_key, digest)
            prior_claim = self._operation_claims.get(operation_ref)
            prior = self._operations.get(operation_ref)
            if existing is None and prior_claim is None and prior is None:
                # A process restart legitimately loses the delegate's memory;
                # the durable wrapper has already proved retry safety and
                # allocated attempt_number with a transactional CAS.
                self._idempotency[intent.idempotency_key] = (
                    digest,
                    operation_ref,
                )
                self._operation_claims[operation_ref] = claim
            elif existing != (digest, operation_ref):
                raise VKWorkspaceError("retry_binding_mismatch")
            if self._operation_claims.get(operation_ref) != claim:
                raise VKWorkspaceError("retry_binding_mismatch")
            if prior is not None and (
                not isinstance(prior, Mapping)
                or prior.get("status") != SocialActionStatus.FAILED.value
                or prior.get("retry_safe") is not True
            ):
                raise VKWorkspaceError("retry_not_safe")
            # Mark in progress before yielding to provider I/O. A concurrent
            # retry cannot enter because _action_lock covers the whole attempt.
            self._operations.pop(operation_ref, None)
            try:
                result = dict(await self._execute_once(intent, operation_ref))
            except BaseException:
                if isinstance(prior, Mapping):
                    self._operations[operation_ref] = dict(prior)
                raise
            result["attempt_number"] = attempt_number
            self._operations[operation_ref] = dict(result)
            return result

    async def _execute_once(self, intent: SocialActionIntent, operation_ref: str) -> Mapping[str, Any]:
        if intent.expected_revision is not None:
            result = {"platform": "vk", "operation_ref": operation_ref, "action": intent.action.value, "status": "failed", "retry_safe": False, "error_code": "expected_revision_unsupported"}
            self._operations[operation_ref] = result
            return dict(result)
        text, bindings = self._content(intent)
        random_id = int(hashlib.sha256(intent.idempotency_key.encode()).hexdigest()[:8], 16) & 0x7FFFFFFF or 1
        guid = hashlib.sha256(intent.idempotency_key.encode()).hexdigest()
        target = self._resolve_ref("target", intent.target_ref) if intent.target_ref else None
        item = self._resolve_ref("item", intent.item_ref) if intent.item_ref else None
        destination = self._resolve_ref("target", intent.destination_target_ref) if intent.destination_target_ref else None
        new_item_native: Mapping[str, Any] | None = None
        write_verified = False
        mutation_may_have_happened = False

        def mark_provider_attempted(event: Mapping[str, Any]) -> None:
            safe_event = dict(event)
            safe_event["mutation_boundary_reached"] = mutation_may_have_happened
            if (
                safe_event.get("stage") == "wall_photo_multipart"
                and safe_event.get("phase") == "finished"
            ):
                diagnostic_keys = {
                    "http_status",
                    "content_type",
                    "content_encoding",
                    "compressed_bytes",
                    "decoded_bytes",
                    "consumed_to_eof",
                    "top_level_key_names",
                    "top_level_unknown_key_count",
                    "nested_key_names",
                    "nested_unknown_key_count",
                    "server_field",
                    "photo_field",
                    "hash_field",
                    "image_ordinal",
                    "expected_digest_prefix",
                }
                safe_event["provider_result"] = {
                    key: safe_event[key]
                    for key in diagnostic_keys
                    if key in safe_event
                }
                safe_event["provider_result"]["mutation_boundary_reached"] = (
                    mutation_may_have_happened
                )
            if self._attempt_recorder is not None:
                self._attempt_recorder.record(operation_ref, safe_event)

        async def action_call(operation: str, params: Mapping[str, Any]) -> Any:
            nonlocal mutation_may_have_happened
            if operation in {
                "send_message", "wall_post", "wall_comment", "like_add",
                "like_delete", "wall_edit", "wall_delete", "wall_repost",
                "message_forward", "message_edit", "message_delete",
                "story_save", "story_delete",
            }:
                mutation_may_have_happened = True
            return await self._call(operation, params, attempt_hook=mark_provider_attempted)

        try:
            if intent.action is SocialAction.SEND_MESSAGE:
                peer_id = self._dialog_peer_id(target or {})
                if target is None or peer_id is None:
                    raise VKWorkspaceError("exact_user_required")
                attachments = self._legacy_attachments(bindings)
                params = {"peer_id": peer_id, "message": text, "random_id": random_id, **({"attachment": attachments} if attachments else {})}
                response = await action_call("send_message", params)
                message_id = response.get("message_id") if isinstance(response, Mapping) else response
                if type(message_id) is not int:
                    raise VKWorkspaceError("provider_unavailable")
                readback = await action_call("message_item", {"message_ids": str(message_id)})
                observed = next((raw for raw in _items(readback) if _int(raw.get("id")) == message_id and _int(raw.get("peer_id")) == peer_id), None)
                if observed is None:
                    raise VKWorkspaceError("read_after_write_failed")
                new_item_native = {"kind": "message", "peer_id": peer_id, "message_id": message_id}
            elif intent.action in {SocialAction.PUBLISH, SocialAction.SCHEDULE}:
                if target is None or self._target_kind(target) is not SocialTargetKind.COMMUNITY:
                    raise VKWorkspaceError("community_required")
                if not self._valid_community_binding(target):
                    raise VKWorkspaceError("community_binding_invalid")
                attachments = await self._wall_attachments(
                    bindings,
                    group_id=target["group_id"],
                    attempt_hook=mark_provider_attempted,
                )
                params = {"owner_id": target.get("owner_id"), "from_group": 1, "message": text, "guid": guid, "signed": 0, **({"attachments": attachments} if attachments else {})}
                if intent.action is SocialAction.SCHEDULE:
                    assert intent.schedule_at is not None
                    params["publish_date"] = int(datetime.fromisoformat(intent.schedule_at.replace("Z", "+00:00")).timestamp())
                response = await action_call("wall_post", params)
                post_id = response.get("post_id") if isinstance(response, Mapping) else None
                if type(post_id) is not int:
                    raise VKWorkspaceError(
                        "wall_post_response_invalid",
                        outcome_unknown=True,
                        stage="wall_post",
                    )
                new_item_native = self._wall_native(target["owner_id"], post_id)
                try:
                    if intent.action is SocialAction.SCHEDULE:
                        readback = await self._call(
                            "wall_feed_editor",
                            {
                                "owner_id": target["owner_id"],
                                "count": 100,
                                "offset": 0,
                                "filter": "postponed",
                            },
                        )
                    else:
                        readback = await self._call(
                            "wall_item",
                            {
                                "posts": f"{target['owner_id']}_{post_id}",
                                "extended": 0,
                            },
                        )
                except VKWorkspaceError:
                    raise VKWorkspaceError(
                        "read_after_write_failed",
                        outcome_unknown=True,
                        stage="wall_item",
                    ) from None
                expected_publish_date = (
                    int(
                        datetime.fromisoformat(
                            (intent.schedule_at or "").replace("Z", "+00:00")
                        ).timestamp()
                    )
                    if intent.action is SocialAction.SCHEDULE
                    else None
                )
                observed = next(
                    (
                        raw
                        for raw in _items(readback)
                        if _int(raw.get("id")) == post_id
                        and _int(raw.get("owner_id")) == target["owner_id"]
                        and (
                            expected_publish_date is None
                            or (
                                _int(raw.get("date")) is not None
                                and abs(
                                    int(_int(raw.get("date")))
                                    - expected_publish_date
                                )
                                <= 60
                            )
                        )
                    ),
                    None,
                )
                expected_photos = sum(
                    1
                    for binding in bindings
                    if binding.get("binding_kind") == "verified_asset"
                    or str(binding.get("attachment") or "").startswith("photo")
                )
                observed_attachments = (
                    observed.get("attachments") if isinstance(observed, Mapping) else None
                )
                observed_photos = sum(
                    1
                    for attachment in (
                        observed_attachments
                        if isinstance(observed_attachments, list)
                        else []
                    )
                    if isinstance(attachment, Mapping)
                    and attachment.get("type") == "photo"
                )
                if (
                    observed is None
                    or self._match_text(str(observed.get("text") or ""))
                    != self._match_text(text)
                    or observed_photos < expected_photos
                ):
                    raise VKWorkspaceError(
                        "read_after_write_failed",
                        outcome_unknown=True,
                        stage="wall_item",
                    )
                write_verified = True
            elif intent.action is SocialAction.COMMENT:
                if item is None or not self._valid_community_post(item):
                    raise VKWorkspaceError("community_post_required")
                group_id = _int(item.get("group_id")) or -int(item["owner_id"])
                attachments = await self._wall_attachments(
                    bindings,
                    group_id=group_id,
                    attempt_hook=mark_provider_attempted,
                )
                params = {"owner_id": item.get("owner_id"), "post_id": item.get("post_id"), "message": text, "guid": guid, "from_group": 1, **({"attachments": attachments} if attachments else {})}
                response = await action_call("wall_comment", params)
                comment_id = response.get("comment_id") if isinstance(response, Mapping) else None
                if type(comment_id) is not int:
                    raise VKWorkspaceError("provider_unavailable")
                new_item_native = {"kind": "comment", "owner_id": item.get("owner_id"), "post_id": item.get("post_id"), "comment_id": comment_id}
            elif intent.action is SocialAction.REACTION:
                if item is None or not self._valid_community_post(item):
                    raise VKWorkspaceError("community_post_required")
                if intent.reaction not in {"like", "unlike"}:
                    raise VKWorkspaceError("reaction_unsupported")
                await action_call("like_add" if intent.reaction == "like" else "like_delete", {"type": "post", "owner_id": item.get("owner_id"), "item_id": item.get("post_id")})
            elif intent.action is SocialAction.EDIT:
                if item is None:
                    raise VKWorkspaceError("item_required")
                if item.get("kind") == "message":
                    if not self._valid_message_binding(item):
                        raise VKWorkspaceError("message_binding_invalid")
                    attachments = self._legacy_attachments(bindings)
                    await action_call("message_edit", {"peer_id": item.get("peer_id"), "message": text, "message_id": item.get("message_id"), **({"attachment": attachments} if attachments else {})})
                else:
                    if not self._valid_community_post(item):
                        raise VKWorkspaceError("community_post_required")
                    group_id = _int(item.get("group_id")) or -int(item["owner_id"])
                    attachments = await self._wall_attachments(
                        bindings,
                        group_id=group_id,
                        attempt_hook=mark_provider_attempted,
                    )
                    await action_call("wall_edit", {"owner_id": item.get("owner_id"), "post_id": item.get("post_id"), "message": text, **({"attachments": attachments} if attachments else {})})
            elif intent.action is SocialAction.DELETE:
                if item is None:
                    raise VKWorkspaceError("item_required")
                if item.get("kind") == "message":
                    if not self._valid_message_binding(item):
                        raise VKWorkspaceError("message_binding_invalid")
                    await action_call("message_delete", {"message_ids": str(item.get("message_id")), "delete_for_all": 1})
                elif item.get("kind") == "story":
                    owner_id, story_id = _int(item.get("owner_id")), _int(item.get("story_id"))
                    if owner_id is None or story_id is None:
                        raise VKWorkspaceError("story_binding_invalid")
                    await action_call("story_delete", {"owner_id": owner_id, "story_id": story_id})
                else:
                    if not self._valid_community_post(item):
                        raise VKWorkspaceError("community_post_required")
                    await action_call("wall_delete", {"owner_id": item.get("owner_id"), "post_id": item.get("post_id")})
                    if item.get("queue") == "postponed":
                        try:
                            readback = await self._call(
                                "wall_postponed_delete_verify",
                                {
                                    "owner_id": item["owner_id"],
                                    "count": 100,
                                    "offset": 0,
                                    "filter": "postponed",
                                },
                            )
                            postponed = self._postponed_items(readback)
                        except VKWorkspaceError:
                            raise VKWorkspaceError(
                                "read_after_write_failed",
                                outcome_unknown=True,
                                stage="wall_delete",
                            ) from None
                        if any(
                            _int(raw.get("owner_id")) == item["owner_id"]
                            and _int(raw.get("id")) == item["post_id"]
                            for raw in postponed
                        ):
                            raise VKWorkspaceError(
                                "read_after_write_failed",
                                outcome_unknown=True,
                                stage="wall_delete",
                            )
                        write_verified = True
            elif intent.action is SocialAction.FORWARD:
                if item is None or destination is None:
                    raise VKWorkspaceError("destination_required")
                if item.get("kind") == "message":
                    destination_peer_id = self._dialog_peer_id(destination)
                    if not self._valid_message_binding(item) or destination_peer_id is None:
                        raise VKWorkspaceError("exact_user_required")
                    forward = json.dumps({"peer_id": item.get("peer_id"), "message_ids": [item.get("message_id")]}, separators=(",", ":"))
                    await action_call("message_forward", {"peer_id": destination_peer_id, "message": "", "random_id": random_id, "forward": forward})
                else:
                    if _int(item.get("owner_id")) is None or _int(item.get("post_id")) is None:
                        raise VKWorkspaceError("wall_post_required")
                    if self._target_kind(destination) is not SocialTargetKind.COMMUNITY:
                        raise VKWorkspaceError("community_required")
                    if not self._valid_community_binding(destination):
                        raise VKWorkspaceError("community_binding_invalid")
                    params = {"object": f"wall{item.get('owner_id')}_{item.get('post_id')}", "group_id": destination["group_id"]}
                    await action_call("wall_repost", params)
            elif intent.action is SocialAction.STORY:
                if target is None or self._target_kind(target) is not SocialTargetKind.COMMUNITY:
                    raise VKWorkspaceError("community_required")
                if not self._valid_community_binding(target):
                    raise VKWorkspaceError("community_binding_invalid")
                if not intent.content or len(bindings) != 1:
                    raise VKWorkspaceError("story_requires_one_asset")
                binding = bindings[0]
                legacy_binding = binding.get("binding_kind") != "verified_asset"
                if legacy_binding:
                    upload_result = binding.get("story_upload_result")
                    if not isinstance(upload_result, str) or not _STORY_UPLOAD_RESULT.fullmatch(upload_result):
                        raise VKWorkspaceError("asset_not_ready")
                else:
                    role = binding.get("role")
                    if role == MediaRole.IMAGE.value:
                        setup_operation = "story_photo_upload_server"
                        purpose = VKUploadPurpose.STORY_PHOTO
                    elif role == MediaRole.VIDEO.value:
                        setup_operation = "story_video_upload_server"
                        purpose = VKUploadPurpose.STORY_VIDEO
                    else:
                        raise VKWorkspaceError("asset_role_unsupported")
                    self._ensure_operation_permitted(setup_operation)
                    self._ensure_operation_permitted("story_save")
                    materialized = await self._materialize_asset(binding)
                    server_response = await action_call(
                        setup_operation,
                        {"group_id": target["group_id"], "add_to_news": 1},
                    )
                    upload_url = server_response.get("upload_url") if isinstance(server_response, Mapping) else None
                    uploaded = await self._upload_multipart(
                        purpose=purpose,
                        upload_url=upload_url,
                        materialized=materialized,
                        attempt_hook=mark_provider_attempted,
                    )
                    upload_result = uploaded.story_upload_result
                    if not isinstance(upload_result, str) or not _STORY_UPLOAD_RESULT.fullmatch(upload_result):
                        raise VKWorkspaceError("provider_unavailable")
                response = await action_call("story_save", {"upload_results": [upload_result]})
                stories = _items(response)
                if not stories:
                    raise VKWorkspaceError("provider_unavailable")
                story_id = _int(stories[0].get("id"))
                story_owner = _int(stories[0].get("owner_id")) or target["owner_id"]
                if story_id is None or story_owner != target["owner_id"]:
                    raise VKWorkspaceError("provider_unavailable")
                if not legacy_binding:
                    readback = await action_call(
                        "story_item",
                        {"stories": [f"{story_owner}_{story_id}"]},
                    )
                    observed = next(
                        (
                            raw
                            for raw in _items(readback)
                            if _int(raw.get("id")) == story_id
                            and _int(raw.get("owner_id")) == story_owner
                        ),
                        None,
                    )
                    if observed is None:
                        raise VKWorkspaceError("read_after_write_failed")
                new_item_native = {"kind": "story", "group_id": target["group_id"], "owner_id": story_owner, "story_id": story_id}
            else:
                raise SocialWorkspaceValidationError("unsupported VK action")
        except VKWorkspaceError as exc:
            unknown = exc.outcome_unknown and mutation_may_have_happened
            status = SocialActionStatus.OUTCOME_UNKNOWN if unknown else SocialActionStatus.FAILED
            error_code = exc.code
            retry_safe = exc.retry_safe or (
                exc.outcome_unknown and not mutation_may_have_happened
            )
            _LOG.warning(
                "private MCP VK action classified operation_ref=%s action=%s stage=%s status=%s error_code=%s retry_safe=%s",
                operation_ref,
                intent.action.value,
                exc.stage or "local_validation",
                status.value,
                error_code,
                retry_safe if not unknown else False,
            )
            result = {"platform": "vk", "operation_ref": operation_ref, "action": intent.action.value, "status": status.value, "retry_safe": (retry_safe if not unknown else False), "error_code": error_code}
            if exc.stage:
                result["stage"] = exc.stage
            self._operations[operation_ref] = result
            return dict(result)
        result: dict[str, Any] = {"platform": "vk", "operation_ref": operation_ref, "action": intent.action.value, "status": "succeeded", "retry_safe": False}
        if intent.target_ref:
            result["target_ref"] = intent.target_ref
        if new_item_native is not None:
            item_ref = self._mint("item", new_item_native)
            result["item_ref"] = item_ref
        elif intent.item_ref:
            result["item_ref"] = intent.item_ref
        if (
            (intent.action is SocialAction.SEND_MESSAGE or write_verified)
            and isinstance(result.get("item_ref"), str)
        ):
            observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            result["read_after_write"] = {
                "verified": True,
                "observed_item_ref": result["item_ref"],
                "observed_at": observed_at,
            }
        self._operations[operation_ref] = result
        return dict(result)

    async def reconcile(self, operation_ref: str) -> Mapping[str, Any]:
        if not isinstance(operation_ref, str) or not re.fullmatch(r"op_[A-Za-z0-9_-]{24,160}", operation_ref):
            raise SocialWorkspaceValidationError("operation_ref is invalid")
        result = self._operations.get(operation_ref)
        if result is None:
            raise VKWorkspaceError("operation_not_found")
        return dict(result)

    @staticmethod
    def _match_text(value: str) -> str:
        return " ".join(unicodedata.normalize("NFKC", value).split())

    async def reconcile_intent(
        self,
        operation_ref: str,
        intent: SocialActionIntent,
        *,
        claimed_at_ms: int,
        provider_post_id: int | None = None,
        provider_photo_refs: Sequence[tuple[int, int]] = (),
    ) -> Mapping[str, Any]:
        """Reconcile an uncertain wall write by bounded provider readback.

        This method never retries ``wall.post``.  It succeeds only when exactly
        one post on the intended wall matches the normalized text, time window,
        and expected photo presence.
        """

        if intent.action not in {SocialAction.PUBLISH, SocialAction.SCHEDULE}:
            return {
                "platform": "vk",
                "operation_ref": operation_ref,
                "action": intent.action.value,
                "status": SocialActionStatus.OUTCOME_UNKNOWN.value,
                "retry_safe": False,
                "error_code": "reconciliation_unsupported",
            }
        target = self._resolve_ref("target", intent.target_ref or "")
        if not self._valid_community_binding(target):
            raise VKWorkspaceError("community_binding_invalid")
        expected_text = self._match_text(intent.content.text if intent.content else "")
        expected_photos = sum(
            1
            for media in (intent.content.media if intent.content else ())
            if media.role is MediaRole.IMAGE
        )
        if provider_post_id is not None and intent.action is SocialAction.PUBLISH:
            response = await self._call(
                "wall_item",
                {
                    "posts": f"{target['owner_id']}_{provider_post_id}",
                    "extended": 0,
                },
            )
            responses = [response]
        elif intent.action is SocialAction.SCHEDULE:
            responses = [
                await self._call(
                    "wall_feed_editor",
                    {
                        "owner_id": target["owner_id"],
                        "count": 100,
                        "offset": 0,
                        "filter": "postponed",
                    },
                )
            ]
            # Once VK has published the item it leaves the postponed queue.
            # A later status reconciliation must inspect the live owner wall
            # without ever replaying wall.post.
            responses.append(
                await self._call(
                    "wall_feed",
                    {
                        "owner_id": target["owner_id"],
                        "count": 100,
                        "offset": 0,
                        "filter": "owner",
                    },
                )
            )
        else:
            responses = [await self._call(
                "wall_feed",
                {
                    "owner_id": target["owner_id"],
                    "count": 100,
                    "offset": 0,
                    "filter": "owner",
                },
            )]
        claimed_seconds = claimed_at_ms // 1000
        scheduled_seconds = (
            int(
                datetime.fromisoformat(
                    intent.schedule_at.replace("Z", "+00:00")
                ).timestamp()
            )
            if intent.action is SocialAction.SCHEDULE and intent.schedule_at
            else None
        )
        matches: list[Mapping[str, Any]] = []
        seen_posts: set[tuple[int, int]] = set()
        for raw in (
            item for response in responses for item in _items(response)
        ):
            post_id = _int(raw.get("id"))
            owner_id = _int(raw.get("owner_id"))
            published = _int(raw.get("date"))
            if post_id is None or owner_id != target["owner_id"]:
                continue
            post_key = (owner_id, post_id)
            if post_key in seen_posts:
                continue
            seen_posts.add(post_key)
            if provider_post_id is not None and post_id != provider_post_id:
                continue
            expected_seconds = (
                scheduled_seconds
                if intent.action is SocialAction.SCHEDULE
                else claimed_seconds
            )
            tolerance_seconds = 60 if intent.action is SocialAction.SCHEDULE else 30 * 60
            if (
                published is None
                or expected_seconds is None
                or abs(published - expected_seconds) > tolerance_seconds
            ):
                continue
            if self._match_text(str(raw.get("text") or "")) != expected_text:
                continue
            attachments = raw.get("attachments")
            photo_count = sum(
                1
                for attachment in (attachments if isinstance(attachments, list) else [])
                if isinstance(attachment, Mapping)
                and attachment.get("type") == "photo"
            )
            if photo_count < expected_photos:
                continue
            # VK re-owns a saved user photo when it becomes a community wall
            # attachment, so its owner/id pair is not stable across
            # photos.saveWallPhoto -> postponed/live post readback.  Cardinality
            # and the exact post/text/time binding remain stable.
            if photo_count < max(expected_photos, len(provider_photo_refs)):
                continue
            matches.append(raw)
        if len(matches) == 1:
            post_id = int(matches[0]["id"])
            item_ref = self._mint(
                "item", self._wall_native(int(target["owner_id"]), post_id)
            )
            result = {
                "platform": "vk",
                "operation_ref": operation_ref,
                "action": intent.action.value,
                "status": SocialActionStatus.SUCCEEDED.value,
                "retry_safe": False,
                "target_ref": intent.target_ref,
                "item_ref": item_ref,
                "read_after_write": {
                    "verified": True,
                    "observed_item_ref": item_ref,
                    "observed_at": datetime.now(timezone.utc).isoformat().replace(
                        "+00:00", "Z"
                    ),
                },
            }
            self._operations[operation_ref] = dict(result)
            return result
        return {
            "platform": "vk",
            "operation_ref": operation_ref,
            "action": intent.action.value,
            "status": SocialActionStatus.OUTCOME_UNKNOWN.value,
            "retry_safe": False,
            "error_code": (
                "reconciliation_not_observed"
                if not matches
                else "reconciliation_ambiguous"
            ),
        }


__all__ = [
    "VK_API_VERSION",
    "VK_FIXED_METHOD_ALLOWLIST",
    "VK_OPERATION_ACTORS",
    "VKActor",
    "VKActorTransport",
    "VKCallGovernor",
    "VKCooldownHook",
    "VKMultipartTransport",
    "VKOpaqueRefStore",
    "VKStoryMediaReader",
    "VKVerifiedAssetReader",
    "VKWorkspaceAdapter",
    "VKWorkspaceError",
]
