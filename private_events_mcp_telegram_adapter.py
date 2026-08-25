"""Closed, lazy-Telethon adapter for the private Social Workspace.

The adapter accepts only the validated provider-neutral dataclasses from
``private_events_mcp.social_workspace``.  Native Telegram peers, identity material,
message identifiers, staged media and cursors live exclusively behind an injected
opaque-reference store.  The integration layer is responsible for durable ref
storage, approvals and audit; this module is the narrow provider translation layer.

There is intentionally no environment parsing, login flow, raw TL request entry
point, URL fetch, filesystem path, or arbitrary provider-kwargs surface here.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import io
import re
import secrets
import unicodedata
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import urlsplit

from private_events_mcp.social_workspace import (
    ContentFeature,
    MediaRole,
    RichContent,
    RichEntityKind,
    SocialAction,
    SocialActionIntent,
    SocialItemKind,
    SocialPlatform,
    SocialReadAccess,
    SocialReadOperation,
    SocialReadRequest,
    SocialTargetKind,
    SocialWorkspaceValidationError,
    TargetLocatorKind,
    compute_action_digest,
    validate_action_status_response,
    validate_opaque_ref,
)

_TRUST = "untrusted_external_data"
_MAX_PAGE = 25
_MAX_SAMPLE = 100
_MAX_GLOBAL_SCAN = 100
_MIN_TELETHON_VERSION = (1, 44)
_MAX_TELETHON_MAJOR = 1
_MAX_UPLOAD_BYTES = 30 * 1024 * 1024
_MAX_DOCUMENT_UPLOAD_BYTES = 64 * 1024 * 1024
_MAX_DOCUMENT_FILENAME_BYTES = 180
_UPLOAD_MIME_TYPES = {
    MediaRole.IMAGE: frozenset({"image/jpeg", "image/png", "image/webp"}),
    MediaRole.VIDEO: frozenset({"video/mp4", "video/quicktime", "video/webm"}),
}
_DOCUMENT_MIME_TYPES = frozenset(
    {
        "application/vnd.android.package-archive",
        "application/pdf",
        "application/zip",
        "application/json",
        "text/plain",
        "text/csv",
        "text/markdown",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }
)
_DOCUMENT_MIME_EXTENSIONS = {
    "application/vnd.android.package-archive": ".apk",
    "application/pdf": ".pdf",
    "application/zip": ".zip",
    "application/json": ".json",
    "text/plain": ".txt",
    "text/csv": ".csv",
    "text/markdown": ".md",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
}
_BIDI_CONTROLS = frozenset(
    "\u061c\u200e\u200f\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069"
)


class TelegramWorkspaceError(RuntimeError):
    """Sanitized provider-boundary error safe to return to orchestration."""

    def __init__(
        self,
        code: str = "provider_error",
        *,
        retry_safe: bool = True,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__("Telegram workspace operation failed")
        self.code = code
        self.retry_safe = retry_safe
        self.retry_after_seconds = retry_after_seconds

    def __repr__(self) -> str:
        return (
            f"TelegramWorkspaceError(code={self.code!r}, retry_safe={self.retry_safe!r}, "
            f"retry_after_seconds={self.retry_after_seconds!r})"
        )


@dataclass(frozen=True, slots=True)
class TelegramTargetBinding:
    """Server-side target binding. ``entity`` must never be serialized."""

    target_ref: str
    kind: SocialTargetKind
    entity: Any = field(repr=False)
    title: str = ""
    canonical_handle: str | None = None
    profile_link: str | None = None
    is_self: bool = False
    allowed_actions: frozenset[SocialAction] | None = None
    story_privacy: tuple[Any, ...] | None = field(default=None, repr=False)
    binding_version: str | None = None


@dataclass(frozen=True, slots=True)
class TelegramItemBinding:
    """Server-side item binding. Native message id is deliberately private."""

    item_ref: str
    target_ref: str
    message_id: int = field(repr=False)
    allowed_actions: frozenset[SocialAction] | None = None
    kind: SocialItemKind = SocialItemKind.MESSAGE


@dataclass(frozen=True, slots=True)
class TelegramAssetBinding:
    """Server-side staged asset binding. Native media is deliberately private."""

    asset_ref: str
    role: MediaRole
    provider_media: Any = field(repr=False)
    target_ref: str | None = None
    story_id: int | None = None
    expires_at: datetime | None = None
    media_kind: str | None = None
    mime_type: str | None = None
    byte_length: int | None = None
    duration_seconds: float | None = None
    identity_fingerprint: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class TelegramVerifiedUpload:
    """Immutable metadata for bytes held by the injected secure asset reader."""

    storage_ref: str = field(repr=False)
    owner_binding: str = field(repr=False)
    content_digest: str
    mime_type: str
    byte_length: int
    width: int | None
    height: int | None
    duration: float | None
    expires_at: datetime
    display_name: str | None
    classification: str | None


@dataclass(frozen=True, slots=True)
class TelegramOperationClaim:
    """Atomic ledger claim; stores must compare-and-set without replacing a digest."""

    operation_ref: str
    action_digest: str
    claimed_now: bool
    result: Mapping[str, Any] | None = None


class TelegramOpaqueRefStore(Protocol):
    """Opaque reference service supplied by the integration/storage lane."""

    def resolve_target(self, target_ref: str) -> TelegramTargetBinding: ...

    def resolve_item(self, item_ref: str) -> TelegramItemBinding: ...

    def resolve_asset(self, asset_ref: str) -> TelegramAssetBinding: ...

    def mint_target(
        self,
        *,
        entity: Any,
        kind: SocialTargetKind,
        title: str,
        canonical_handle: str | None,
        profile_link: str | None,
        is_self: bool,
    ) -> TelegramTargetBinding: ...

    def mint_item(
        self,
        *,
        target_ref: str,
        message_id: int,
        allowed_actions: frozenset[SocialAction] | None = None,
        kind: SocialItemKind = SocialItemKind.MESSAGE,
    ) -> TelegramItemBinding: ...

    def mint_read_asset(
        self,
        *,
        target_ref: str,
        media: Any,
        role: MediaRole,
        story_id: int | None = None,
        expires_at: datetime | None = None,
        item_kind: SocialItemKind | None = None,
    ) -> str: ...

    def mint_upload_asset(
        self, *, role: MediaRole, upload: TelegramVerifiedUpload
    ) -> TelegramAssetBinding | Awaitable[TelegramAssetBinding]: ...

    def mint_cursor(self, *, family: str, state: Mapping[str, Any]) -> str: ...

    def resolve_cursor(self, *, family: str, cursor: str) -> Mapping[str, Any]: ...

    def claim_operation(
        self, *, operation_ref: str, action_digest: str
    ) -> TelegramOperationClaim | Awaitable[TelegramOperationClaim]: ...

    def release_operation(
        self, *, operation_ref: str, action_digest: str
    ) -> bool | Awaitable[bool]: ...

    def complete_operation(
        self,
        *,
        operation_ref: str,
        action_digest: str,
        result: Mapping[str, Any],
    ) -> TelegramOperationClaim | Awaitable[TelegramOperationClaim]: ...

    def resolve_operation(
        self, operation_ref: str
    ) -> TelegramOperationClaim | Awaitable[TelegramOperationClaim]: ...


@dataclass(frozen=True, slots=True)
class TelegramLease:
    fence: str = field(repr=False)


class TelegramGovernor(Protocol):
    """Cross-process lease/fence and persistent cooldown contract."""

    def cooldown_remaining(self) -> int | Awaitable[int]: ...

    def note_flood_wait(self, seconds: int) -> None | Awaitable[None]: ...

    def acquire(self, operation: str) -> TelegramLease | Awaitable[TelegramLease]: ...

    def assert_current(self, lease: TelegramLease) -> bool | Awaitable[bool]: ...

    def release(self, lease: TelegramLease) -> None | Awaitable[None]: ...


class TelegramAssetReader(Protocol):
    """Secure server-side byte source. Storage references are never filesystem paths."""

    def open_verified(
        self, storage_ref: str, owner_binding: str
    ) -> Any | Awaitable[Any]: ...


ClientFactory = Callable[[], Any | Awaitable[Any]]


async def _await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _safe_text(value: Any, maximum: int, *, fallback: str = "") -> str:
    if not isinstance(value, str):
        return fallback
    return value.replace("\x00", "").strip()[:maximum] or fallback


def _utc(value: Any) -> str:
    if not isinstance(value, datetime):
        raise TelegramWorkspaceError("invalid_provider_response")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utf16_units(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def _utf16_range(text: str, offset: int, length: int) -> tuple[int, int]:
    """Convert validated Python-codepoint ranges to Telegram UTF-16 ranges."""

    return _utf16_units(text[:offset]), _utf16_units(text[offset : offset + length])


def _int(value: Any, *, minimum: int = 0) -> int:
    return value if type(value) is int and value >= minimum else minimum


def _reaction_count(message: Any) -> int:
    results = getattr(getattr(message, "reactions", None), "results", None)
    if not isinstance(results, Sequence):
        return 0
    return sum(_int(getattr(result, "count", None)) for result in results[:50])


def _message_metrics(message: Any) -> dict[str, int]:
    replies = getattr(message, "replies", None)
    return {
        "views": _int(getattr(message, "views", None)),
        "reactions": _reaction_count(message),
        "comments": _int(getattr(replies, "replies", None)),
        "shares": _int(getattr(message, "forwards", None)),
    }


def _provider_message_id(message: Any) -> int:
    value = getattr(message, "id", None)
    if type(value) is not int or value <= 0:
        raise TelegramWorkspaceError("invalid_provider_response")
    return value


def _entity_kind(entity: Any) -> SocialTargetKind:
    if getattr(entity, "broadcast", False):
        return SocialTargetKind.CHANNEL
    if getattr(entity, "megagroup", False) or entity.__class__.__name__ in {
        "Chat",
        "ChannelForbidden",
    }:
        return SocialTargetKind.GROUP
    if entity.__class__.__name__ in {"Channel", "InputChannel"}:
        return SocialTargetKind.CHANNEL
    return SocialTargetKind.USER


def _entity_title(entity: Any, kind: SocialTargetKind) -> str:
    if kind in {SocialTargetKind.CHANNEL, SocialTargetKind.GROUP}:
        return _safe_text(getattr(entity, "title", None), 256, fallback="Telegram target")
    parts = [
        _safe_text(getattr(entity, "first_name", None), 128),
        _safe_text(getattr(entity, "last_name", None), 128),
    ]
    title = " ".join(value for value in parts if value).strip()
    return title or _safe_text(getattr(entity, "username", None), 128, fallback="Telegram user")


def _entity_handle(entity: Any) -> str | None:
    value = _safe_text(getattr(entity, "username", None), 128)
    return value or None


def _extract_flood_wait(exc: BaseException) -> int | None:
    name = exc.__class__.__name__.lower()
    seconds = getattr(exc, "seconds", None)
    if ("floodwait" in name or "flood_wait" in name) and type(seconds) is int:
        return max(1, min(seconds, 7 * 24 * 60 * 60))
    return None


def _is_definite_provider_rejection(exc: BaseException) -> bool:
    """Identify closed request/RPC rejection without importing Telethon eagerly."""

    if isinstance(exc, ValueError):
        return True
    names = {base.__name__ for base in type(exc).__mro__}
    if "RPCError" not in names:
        return False
    ambiguous = {
        "FloodError",
        "ServerError",
        "TimedOutError",
        "TimeoutError",
    }
    return not names & ambiguous and not any("FloodWait" in name for name in names)


class _DefaultTelethonTypes:
    """Lazy, closed factory for the exact TL types used by this adapter."""

    def __init__(self) -> None:
        self._functions: Any | None = None
        self._types: Any | None = None
        self._utils: Any | None = None

    def _load(self) -> tuple[Any, Any]:
        if self._functions is not None and self._types is not None:
            return self._functions, self._types
        try:
            import telethon  # type: ignore
            from telethon import TelegramClient, utils  # type: ignore
            from telethon.tl import functions, types  # type: ignore

            version = tuple(int(part) for part in telethon.__version__.split(".")[:2])
            if version < _MIN_TELETHON_VERSION or version[0] > _MAX_TELETHON_MAJOR:
                raise RuntimeError("unsupported Telethon version")
            required = (
                (functions.channels, "GetChannelRecommendationsRequest"),
                (functions.channels, "GetFullChannelRequest"),
                (functions.messages, "SearchGlobalRequest"),
                (functions.messages, "GetRepliesRequest"),
                (functions.messages, "SendReactionRequest"),
                (functions.stories, "GetPeerStoriesRequest"),
                (functions.stories, "GetStoriesByIDRequest"),
                (functions.stories, "GetStoriesViewsRequest"),
                (functions.stories, "CanSendStoryRequest"),
                (functions.stories, "SendStoryRequest"),
                (types, "InputMediaUploadedPhoto"),
                (types, "InputMediaUploadedDocument"),
                (types, "DocumentAttributeFilename"),
                (types, "DocumentAttributeVideo"),
                (types, "InputPrivacyValueAllowAll"),
                (types, "UpdateStoryID"),
                (types, "MessageEntityCustomEmoji"),
                (types, "MessageEntityBlockquote"),
                (types, "InputMessageEntityMentionName"),
                (types, "ReactionEmoji"),
                (types, "InputMessagesFilterEmpty"),
                (types, "InputPeerEmpty"),
                (types, "ChatBannedRights"),
            )
            if any(not hasattr(owner, name) for owner, name in required):
                raise RuntimeError("required Telethon feature missing")
            signatures = {
                TelegramClient.send_message: {
                    "formatting_entities", "schedule", "comment_to"
                },
                TelegramClient.send_file: {
                    "formatting_entities",
                    "schedule",
                    "comment_to",
                    "force_document",
                    "mime_type",
                    "file_size",
                    "attributes",
                    "parse_mode",
                },
                TelegramClient.edit_message: {"formatting_entities"},
            }
            if any(
                not parameters.issubset(inspect.signature(method).parameters)
                for method, parameters in signatures.items()
            ):
                raise RuntimeError("required Telethon client feature missing")
            banned_right_fields = {
                "send_plain",
                "send_media",
                "send_photos",
                "send_videos",
                "send_docs",
                "send_audios",
                "send_gifs",
                "send_reactions",
            }
            if not banned_right_fields.issubset(
                inspect.signature(types.ChatBannedRights).parameters
            ):
                raise RuntimeError("required Telethon rights feature missing")
        except Exception:  # noqa: BLE001 - normalize optional dependency drift
            raise TelegramWorkspaceError("provider_dependency_unavailable") from None
        self._functions, self._types, self._utils = functions, types, utils
        return functions, types

    def ensure(self) -> None:
        self._load()

    def entity(self, kind: RichEntityKind, *, offset: int, length: int, **extra: Any) -> Any:
        _, types = self._load()
        constructors = {
            RichEntityKind.BOLD: lambda: types.MessageEntityBold(offset, length),
            RichEntityKind.ITALIC: lambda: types.MessageEntityItalic(offset, length),
            RichEntityKind.UNDERLINE: lambda: types.MessageEntityUnderline(offset, length),
            RichEntityKind.STRIKETHROUGH: lambda: types.MessageEntityStrike(offset, length),
            RichEntityKind.SPOILER: lambda: types.MessageEntitySpoiler(offset, length),
            RichEntityKind.CODE: lambda: types.MessageEntityCode(offset, length),
            RichEntityKind.PRE: lambda: types.MessageEntityPre(offset, length, ""),
            RichEntityKind.BLOCKQUOTE: lambda: types.MessageEntityBlockquote(offset, length),
            RichEntityKind.LINK: lambda: types.MessageEntityTextUrl(
                offset, length, extra["url"]
            ),
            RichEntityKind.MENTION: lambda: types.InputMessageEntityMentionName(
                offset, length, self._utils.get_input_user(extra["user"])
            ),
            RichEntityKind.CUSTOM_EMOJI: lambda: types.MessageEntityCustomEmoji(
                offset, length, extra["document_id"]
            ),
        }
        try:
            return constructors[kind]()
        except (KeyError, TypeError, ValueError):
            raise TelegramWorkspaceError("unsupported_rich_entity") from None

    def media(self, value: Any, *, spoiler: bool) -> Any:
        if not spoiler:
            return value
        if not hasattr(value, "spoiler"):
            raise TelegramWorkspaceError("unsupported_media_spoiler")
        cloned = copy.copy(value)
        cloned.spoiler = True
        return cloned

    def document_filename(self, file_name: str) -> Any:
        _, types = self._load()
        return types.DocumentAttributeFilename(file_name=file_name)

    def peer_id(self, entity: Any) -> int:
        self._load()
        return self._utils.get_peer_id(entity)

    def request(self, name: str, **values: Any) -> Any:
        functions, types = self._load()
        if name == "full_channel":
            return functions.channels.GetFullChannelRequest(channel=values["channel"])
        if name == "similar_channels":
            return functions.channels.GetChannelRecommendationsRequest(
                channel=values["channel"]
            )
        if name == "global_search":
            return functions.messages.SearchGlobalRequest(
                q=values["query"],
                filter=types.InputMessagesFilterEmpty(),
                min_date=values.get("min_date"),
                max_date=values.get("max_date"),
                offset_rate=values.get("offset_rate", 0),
                offset_peer=values.get("offset_peer") or types.InputPeerEmpty(),
                offset_id=values.get("offset_id", 0),
                limit=values["limit"],
            )
        if name == "comments":
            return functions.messages.GetRepliesRequest(
                peer=values["peer"],
                msg_id=values["message_id"],
                offset_id=values.get("offset_id", 0),
                offset_date=None,
                add_offset=0,
                limit=values["limit"],
                max_id=0,
                min_id=0,
                hash=0,
            )
        if name == "reaction":
            return functions.messages.SendReactionRequest(
                peer=values["peer"],
                msg_id=values["message_id"],
                reaction=[types.ReactionEmoji(emoticon=values["reaction"])],
            )
        if name == "peer_stories":
            return functions.stories.GetPeerStoriesRequest(peer=values["peer"])
        if name == "stories_by_id":
            return functions.stories.GetStoriesByIDRequest(
                peer=values["peer"], id=list(values["story_ids"])
            )
        if name == "stories_views":
            return functions.stories.GetStoriesViewsRequest(
                peer=values["peer"], id=list(values["story_ids"])
            )
        if name == "can_send_story":
            return functions.stories.CanSendStoryRequest(peer=values["peer"])
        if name == "send_story":
            return functions.stories.SendStoryRequest(
                peer=values["peer"],
                media=values["media"],
                privacy_rules=list(values["privacy_rules"]),
                caption=values["caption"],
                entities=values["entities"],
                random_id=secrets.randbits(63),
                media_areas=[],
                pinned=values.get("pinned"),
                noforwards=values.get("noforwards"),
                period=values.get("period"),
            )
        raise TelegramWorkspaceError("unsupported_provider_feature")

    def upload_media(
        self,
        uploaded: Any,
        *,
        role: MediaRole,
        mime_type: str,
        width: int,
        height: int,
        duration: float | None,
        spoiler: bool,
    ) -> Any:
        _, types = self._load()
        if role is MediaRole.IMAGE:
            return types.InputMediaUploadedPhoto(file=uploaded, spoiler=spoiler or None)
        if role is MediaRole.VIDEO and duration is not None:
            return types.InputMediaUploadedDocument(
                file=uploaded,
                mime_type=mime_type,
                attributes=[
                    types.DocumentAttributeVideo(
                        duration=duration,
                        w=width,
                        h=height,
                        supports_streaming=True,
                    )
                ],
                spoiler=spoiler or None,
            )
        raise TelegramWorkspaceError("unsupported_provider_feature")

    def public_story_privacy(self) -> tuple[Any, ...]:
        _, types = self._load()
        return (types.InputPrivacyValueAllowAll(),)

    @staticmethod
    def story_id(response: Any, *, random_id: int) -> int:
        updates = list(getattr(response, "updates", None) or [])
        single = getattr(response, "update", None)
        if single is not None:
            updates.append(single)
        matches = {
            getattr(update, "id", None)
            for update in updates[:100]
            if update.__class__.__name__ == "UpdateStoryID"
            and getattr(update, "random_id", None) == random_id
            and type(getattr(update, "id", None)) is int
            and getattr(update, "id", 0) > 0
        }
        if len(matches) != 1:
            raise TimeoutError("Telegram story id was not confirmed")
        return matches.pop()


@dataclass(slots=True)
class _Attempt:
    provider_mutation_attempted: bool = False


@dataclass(frozen=True, slots=True)
class _PreflightSnapshot:
    target: TelegramTargetBinding
    source: TelegramTargetBinding | None
    item: TelegramItemBinding | None


@dataclass(frozen=True, slots=True)
class _LivePolicy:
    actions: frozenset[SocialAction]
    content_features: frozenset[ContentFeature]


class TelegramWorkspaceAdapter:
    """Fixed high-level Telegram implementation for Social Workspace."""

    platform = "telegram"
    document_send_supported = True

    def __init__(
        self,
        *,
        client_factory: ClientFactory,
        refs: TelegramOpaqueRefStore,
        governor: TelegramGovernor,
        telethon_types: Any | None = None,
        asset_reader: TelegramAssetReader | None = None,
        operation_timeout_seconds: float = 30.0,
    ) -> None:
        if not callable(client_factory):
            raise TypeError("client_factory must be callable")
        for method in (
            "resolve_target",
            "resolve_item",
            "resolve_asset",
            "mint_target",
            "mint_item",
            "mint_read_asset",
            "mint_cursor",
            "resolve_cursor",
            "claim_operation",
            "release_operation",
            "complete_operation",
            "resolve_operation",
        ):
            if not callable(getattr(refs, method, None)):
                raise TypeError("refs must implement the opaque reference contract")
        for method in (
            "cooldown_remaining",
            "note_flood_wait",
            "acquire",
            "assert_current",
            "release",
        ):
            if not callable(getattr(governor, method, None)):
                raise TypeError("governor must implement lease, fencing, and cooldown")
        if operation_timeout_seconds <= 0 or operation_timeout_seconds > 120:
            raise ValueError("operation timeout is invalid")
        self._client_factory = client_factory
        self._refs = refs
        self._governor = governor
        self._types = telethon_types or _DefaultTelethonTypes()
        if asset_reader is not None and not callable(
            getattr(asset_reader, "open_verified", None)
        ):
            raise TypeError("asset_reader must implement open_verified")
        self._asset_reader = asset_reader
        self._timeout = float(operation_timeout_seconds)
        self._lock = asyncio.Lock()

    def __repr__(self) -> str:
        return "<TelegramWorkspaceAdapter platform='telegram'>"

    async def _fenced(self, lease: TelegramLease) -> None:
        if not await _await(self._governor.assert_current(lease)):
            raise TelegramWorkspaceError("lease_lost", retry_safe=False)

    async def _disconnect(self, client: Any) -> None:
        try:
            if callable(getattr(client, "disconnect", None)):
                await _await(client.disconnect())
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001, S110 - best-effort secret-free disconnect
            pass

    async def _session(
        self,
        operation: str,
        body: Callable[[Any, TelegramLease, _Attempt], Awaitable[Any]],
    ) -> Any:
        async with self._lock:
            remaining = await _await(self._governor.cooldown_remaining())
            if type(remaining) is not int or remaining < 0:
                raise TelegramWorkspaceError("governor_invalid", retry_safe=False)
            if remaining:
                raise TelegramWorkspaceError(
                    "provider_cooldown", retry_after_seconds=remaining
                )
            lease = await _await(self._governor.acquire(operation))
            if not isinstance(lease, TelegramLease):
                raise TelegramWorkspaceError("lease_unavailable", retry_safe=False)
            client: Any | None = None
            attempt = _Attempt()
            try:
                await self._fenced(lease)
                client = await _await(self._client_factory())
                if client is None:
                    raise TelegramWorkspaceError("provider_unavailable")
                if callable(getattr(client, "connect", None)):
                    await _await(client.connect())
                if (
                    callable(getattr(client, "is_user_authorized", None))
                    and not await _await(client.is_user_authorized())
                ):
                    raise TelegramWorkspaceError("provider_unauthorized", retry_safe=False)
                await self._fenced(lease)
                return await asyncio.wait_for(body(client, lease, attempt), self._timeout)
            except asyncio.CancelledError:
                raise
            except SocialWorkspaceValidationError:
                if attempt.provider_mutation_attempted:
                    raise TelegramWorkspaceError(
                        "outcome_unknown", retry_safe=False
                    ) from None
                raise
            except TelegramWorkspaceError:
                raise
            except (asyncio.TimeoutError, TimeoutError):
                raise TelegramWorkspaceError(
                    "outcome_unknown" if attempt.provider_mutation_attempted else "provider_timeout",
                    retry_safe=not attempt.provider_mutation_attempted,
                ) from None
            except Exception as exc:  # noqa: BLE001 - provider error secrecy boundary
                wait = _extract_flood_wait(exc)
                if wait is not None:
                    await _await(self._governor.note_flood_wait(wait))
                    raise TelegramWorkspaceError(
                        "provider_cooldown",
                        retry_safe=not attempt.provider_mutation_attempted,
                        retry_after_seconds=wait,
                    ) from None
                raise TelegramWorkspaceError(
                    "provider_error", retry_safe=not attempt.provider_mutation_attempted
                ) from None
            finally:
                if client is not None:
                    await self._disconnect(client)
                try:
                    await _await(self._governor.release(lease))
                except Exception:  # noqa: BLE001, S110 - best-effort lease cleanup
                    pass

    async def _call(self, client: Any, lease: TelegramLease, value: Any) -> Any:
        await self._fenced(lease)
        return await _await(client(value))

    def _target(self, target_ref: str) -> TelegramTargetBinding:
        validate_opaque_ref(target_ref, "target")
        binding = self._refs.resolve_target(target_ref)
        if not isinstance(binding, TelegramTargetBinding) or binding.target_ref != target_ref:
            raise SocialWorkspaceValidationError("opaque target binding mismatch")
        if isinstance(binding.entity, (str, bytes, bytearray, memoryview, int)):
            raise SocialWorkspaceValidationError("target binding must hold a provider entity")
        try:
            detached_entity = copy.deepcopy(binding.entity)
            detached_privacy = copy.deepcopy(binding.story_privacy)
        except Exception:  # noqa: BLE001 - opaque provider entities vary by Telethon layer
            raise SocialWorkspaceValidationError("target binding cannot be snapshotted") from None
        if detached_entity is binding.entity:
            raise SocialWorkspaceValidationError("target binding is not detachable")
        return TelegramTargetBinding(
            target_ref=binding.target_ref,
            kind=binding.kind,
            entity=detached_entity,
            title=binding.title,
            canonical_handle=binding.canonical_handle,
            profile_link=binding.profile_link,
            is_self=binding.is_self,
            allowed_actions=binding.allowed_actions,
            story_privacy=detached_privacy,
            binding_version=binding.binding_version,
        )

    def _item(self, item_ref: str) -> TelegramItemBinding:
        validate_opaque_ref(item_ref, "item")
        binding = self._refs.resolve_item(item_ref)
        if not isinstance(binding, TelegramItemBinding) or binding.item_ref != item_ref:
            raise SocialWorkspaceValidationError("opaque item binding mismatch")
        return binding

    def _mint_item_binding(
        self,
        *,
        target_ref: str,
        message_id: int,
        kind: SocialItemKind,
        allowed_actions: frozenset[SocialAction] | None = None,
    ) -> TelegramItemBinding:
        minter = self._refs.mint_item
        parameters = inspect.signature(minter).parameters
        supports_kind = "kind" in parameters or any(
            value.kind is inspect.Parameter.VAR_KEYWORD
            for value in parameters.values()
        )
        values: dict[str, Any] = {
            "target_ref": target_ref,
            "message_id": message_id,
            "allowed_actions": allowed_actions,
        }
        if supports_kind:
            values["kind"] = kind
        binding = minter(**values)
        if not isinstance(binding, TelegramItemBinding):
            raise SocialWorkspaceValidationError("item minter returned invalid binding")
        if supports_kind and binding.kind is not kind:
            raise SocialWorkspaceValidationError("item minter returned wrong item kind")
        validate_opaque_ref(binding.item_ref, "item")
        return binding

    def _asset(self, asset_ref: str) -> TelegramAssetBinding:
        validate_opaque_ref(asset_ref, "asset")
        binding = self._refs.resolve_asset(asset_ref)
        if not isinstance(binding, TelegramAssetBinding) or binding.asset_ref != asset_ref:
            raise SocialWorkspaceValidationError("opaque asset binding mismatch")
        if isinstance(binding.provider_media, (str, bytes, bytearray, memoryview)):
            raise SocialWorkspaceValidationError("asset is not staged provider media")
        return binding

    @staticmethod
    def _verified_upload(asset: Any, role: MediaRole) -> TelegramVerifiedUpload:
        if role not in {*_UPLOAD_MIME_TYPES, MediaRole.DOCUMENT}:
            raise SocialWorkspaceValidationError(
                "Telegram upload role must be image, video, or document"
            )

        def field_value(primary: str, fallback: str | None = None) -> Any:
            value = getattr(asset, primary, None)
            return getattr(asset, fallback, None) if value is None and fallback else value

        storage_ref = field_value("storage_ref", "storage_path")
        owner_binding = field_value("owner_binding")
        content_digest = field_value("content_digest", "sha256")
        mime_type = field_value("mime_type", "detected_mime")
        byte_length = field_value("byte_length")
        width = field_value("width")
        height = field_value("height")
        duration = field_value("duration")
        expires_at = field_value("expires_at")
        declared_role = field_value("role")
        display_name = field_value("display_name", "safe_file_name")
        classification = field_value("classification")

        # The initial provider contract mentioned storage_path, but callers must
        # never pass a filesystem path. Only an opaque storage_ref is accepted.
        if getattr(asset, "storage_ref", None) is None or not isinstance(storage_ref, str):
            raise SocialWorkspaceValidationError("verified asset storage_ref is required")
        if (
            not re.fullmatch(r"ing_[A-Za-z0-9_-]{24,160}", storage_ref)
        ):
            raise SocialWorkspaceValidationError("verified asset storage_ref is invalid")
        if not isinstance(owner_binding, str) or not re.fullmatch(
            r"[0-9a-f]{64}", owner_binding
        ):
            raise SocialWorkspaceValidationError("verified asset owner binding is invalid")
        if not isinstance(content_digest, str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", content_digest
        ):
            raise SocialWorkspaceValidationError("verified asset digest is invalid")
        if not isinstance(mime_type, str):
            raise SocialWorkspaceValidationError("verified asset MIME is invalid")
        mime_type = mime_type.strip().casefold()
        allowed_mime_types = (
            _DOCUMENT_MIME_TYPES
            if role is MediaRole.DOCUMENT
            else _UPLOAD_MIME_TYPES[role]
        )
        if mime_type not in allowed_mime_types:
            raise SocialWorkspaceValidationError("verified asset MIME does not match role")
        size_limit = (
            _MAX_DOCUMENT_UPLOAD_BYTES
            if role is MediaRole.DOCUMENT
            else _MAX_UPLOAD_BYTES
        )
        if type(byte_length) is not int or not 0 < byte_length <= size_limit:
            raise SocialWorkspaceValidationError("verified asset exceeds Telegram size limit")
        if role is MediaRole.DOCUMENT:
            if width is not None or height is not None or duration is not None:
                raise SocialWorkspaceValidationError(
                    "verified document dimensions and duration must be absent"
                )
            if (
                not isinstance(display_name, str)
                or not display_name
                or display_name != unicodedata.normalize("NFKC", display_name)
                or display_name.strip(" .") != display_name
                or "/" in display_name
                or "\\" in display_name
                or display_name in {".", ".."}
                or len(display_name.encode("utf-8")) > _MAX_DOCUMENT_FILENAME_BYTES
                or any(
                    character in _BIDI_CONTROLS
                    or unicodedata.category(character) in {"Cc", "Cs"}
                    for character in display_name
                )
                or not display_name.casefold().endswith(
                    _DOCUMENT_MIME_EXTENSIONS[mime_type]
                )
            ):
                raise SocialWorkspaceValidationError(
                    "verified document display name is invalid"
                )
            if not isinstance(classification, str) or not re.fullmatch(
                r"[a-z][a-z0-9_]{1,63}", classification
            ):
                raise SocialWorkspaceValidationError(
                    "verified document classification is invalid"
                )
        elif type(width) is not int or type(height) is not int or not (
            0 < width <= 20_000 and 0 < height <= 20_000
        ):
            raise SocialWorkspaceValidationError("verified asset dimensions are invalid")
        if role is MediaRole.VIDEO:
            if type(duration) not in {int, float} or not 0 < float(duration) <= 3600:
                raise SocialWorkspaceValidationError("verified video duration is invalid")
            duration = float(duration)
        elif role is not MediaRole.DOCUMENT and duration is not None:
            raise SocialWorkspaceValidationError("verified image duration must be absent")
        if declared_role is not None:
            try:
                normalized_declared_role = (
                    declared_role
                    if isinstance(declared_role, MediaRole)
                    else MediaRole(declared_role)
                )
            except (TypeError, ValueError):
                raise SocialWorkspaceValidationError("verified asset role is invalid") from None
            if normalized_declared_role is not role:
                raise SocialWorkspaceValidationError("verified asset role mismatch")

        if isinstance(expires_at, datetime):
            expiry = expires_at
        elif type(expires_at) in {int, float}:
            try:
                expiry = datetime.fromtimestamp(float(expires_at), tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                raise SocialWorkspaceValidationError("verified asset expiry is invalid") from None
        elif isinstance(expires_at, str):
            try:
                expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            except ValueError:
                raise SocialWorkspaceValidationError("verified asset expiry is invalid") from None
        else:
            raise SocialWorkspaceValidationError("verified asset expiry is invalid")
        if expiry.tzinfo is None:
            raise SocialWorkspaceValidationError("verified asset expiry needs timezone")
        expiry = expiry.astimezone(timezone.utc)
        if expiry <= datetime.now(timezone.utc):
            raise SocialWorkspaceValidationError("verified asset has expired")
        return TelegramVerifiedUpload(
            storage_ref=storage_ref,
            owner_binding=owner_binding,
            content_digest=content_digest,
            mime_type=mime_type,
            byte_length=byte_length,
            width=width,
            height=height,
            duration=duration,
            expires_at=expiry,
            display_name=display_name,
            classification=classification,
        )

    async def stage_asset(self, asset: Any, *, role: MediaRole) -> str:
        """Bind verified server-owned metadata; provider upload is deferred to commit."""

        if not isinstance(role, MediaRole):
            raise SocialWorkspaceValidationError("Telegram upload role is invalid")
        if role is MediaRole.VIDEO:
            raise SocialWorkspaceValidationError(
                "verified Telegram video ingress is unavailable"
            )
        upload = self._verified_upload(asset, role)
        minter = getattr(self._refs, "mint_upload_asset", None)
        if not callable(minter):
            raise TelegramWorkspaceError("asset_store_unavailable", retry_safe=False)
        binding = await _await(minter(role=role, upload=upload))
        if (
            not isinstance(binding, TelegramAssetBinding)
            or binding.role is not role
            or not isinstance(binding.provider_media, TelegramVerifiedUpload)
            or binding.provider_media != upload
        ):
            raise SocialWorkspaceValidationError("upload asset minter returned invalid binding")
        validate_opaque_ref(binding.asset_ref, "asset")
        return binding.asset_ref

    async def read_asset(
        self, asset_ref: str, *, owner_binding: str, max_bytes: int
    ) -> bytes:
        """Materialize bounded story media without reading or marking the story itself."""

        if not isinstance(owner_binding, str) or not re.fullmatch(
            r"[0-9a-f]{64}", owner_binding
        ):
            raise SocialWorkspaceValidationError("asset owner binding is invalid")
        if type(max_bytes) is not int or not 0 < max_bytes <= _MAX_UPLOAD_BYTES:
            raise SocialWorkspaceValidationError("asset read bound is invalid")
        binding = self._asset(asset_ref)
        if (
            binding.expires_at is not None
            and binding.expires_at <= datetime.now(timezone.utc)
        ):
            raise SocialWorkspaceValidationError("asset has expired")
        if isinstance(binding.provider_media, TelegramVerifiedUpload):
            if binding.provider_media.owner_binding != owner_binding:
                raise SocialWorkspaceValidationError("asset owner binding mismatch")
            data = await self._read_upload_bytes(binding.provider_media, role=binding.role)
            if len(data) > max_bytes:
                raise SocialWorkspaceValidationError("asset exceeds requested read bound")
            return data

        async def run(
            client: Any, lease: TelegramLease, _attempt: _Attempt
        ) -> bytes:
            await self._fenced(lease)
            iterator_factory = getattr(client, "iter_download", None)
            if not callable(iterator_factory):
                raise TelegramWorkspaceError("provider_dependency_unavailable")
            chunks: list[bytes] = []
            total = 0
            iterator = iterator_factory(binding.provider_media, request_size=512 * 1024)
            if not hasattr(iterator, "__aiter__"):
                raise TelegramWorkspaceError("invalid_provider_response")
            async for chunk in iterator:
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise TelegramWorkspaceError("invalid_provider_response")
                total += len(chunk)
                if total > max_bytes:
                    raise SocialWorkspaceValidationError(
                        "asset exceeds requested read bound"
                    )
                chunks.append(bytes(chunk))
            return b"".join(chunks)

        return await self._session("read_asset", run)

    def _mint_read_asset(
        self,
        *,
        target_ref: str,
        media: Any,
        role: MediaRole,
        story_id: int | None = None,
        expires_at: datetime | None = None,
        media_kind: str | None = None,
        mime_type: str | None = None,
        byte_length: int | None = None,
        duration_seconds: float | None = None,
        item_message_id: int | None = None,
    ) -> str:
        minter = self._refs.mint_read_asset
        parameters = inspect.signature(minter).parameters
        supports_extra = any(
            value.kind is inspect.Parameter.VAR_KEYWORD
            for value in parameters.values()
        )
        values: dict[str, Any] = {
            "target_ref": target_ref,
            "media": media,
            "role": role,
        }
        for name, value in (
            ("story_id", story_id),
            ("expires_at", expires_at),
            ("item_kind", SocialItemKind.STORY if story_id is not None else None),
            ("media_kind", media_kind),
            ("mime_type", mime_type),
            ("byte_length", byte_length),
            ("duration_seconds", duration_seconds),
            ("item_message_id", item_message_id),
        ):
            if value is not None and (name in parameters or supports_extra):
                values[name] = value
        media_ref = minter(**values)
        validate_opaque_ref(media_ref, "asset")
        return media_ref

    async def _read_upload_bytes(
        self, upload: TelegramVerifiedUpload, *, role: MediaRole
    ) -> bytes:
        if upload.expires_at <= datetime.now(timezone.utc):
            raise SocialWorkspaceValidationError("verified asset has expired")
        reader = self._asset_reader
        if reader is None:
            raise TelegramWorkspaceError("asset_reader_unavailable", retry_safe=False)
        opened = await _await(
            reader.open_verified(upload.storage_ref, upload.owner_binding)
        )
        close: Callable[[], Any] | None = None
        if isinstance(opened, (bytes, bytearray, memoryview)):
            data = bytes(opened)
        else:
            if isinstance(opened, (str, int)) or not callable(getattr(opened, "read", None)):
                raise TelegramWorkspaceError("asset_reader_invalid", retry_safe=False)
            close = getattr(opened, "close", None)
            try:
                limit = (
                    _MAX_DOCUMENT_UPLOAD_BYTES
                    if role is MediaRole.DOCUMENT
                    else _MAX_UPLOAD_BYTES
                )
                data = await _await(opened.read(limit + 1))
            finally:
                if callable(close):
                    await _await(close())
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TelegramWorkspaceError("asset_reader_invalid", retry_safe=False)
            data = bytes(data)
        if (
            len(data) != upload.byte_length
            or len(data)
            > (
                _MAX_DOCUMENT_UPLOAD_BYTES
                if role is MediaRole.DOCUMENT
                else _MAX_UPLOAD_BYTES
            )
            or hashlib.sha256(data).hexdigest()
            != upload.content_digest.removeprefix("sha256:")
        ):
            raise SocialWorkspaceValidationError("verified asset bytes do not match metadata")
        return data

    async def _provider_media(
        self,
        client: Any,
        binding: TelegramAssetBinding,
        *,
        spoiler: bool,
        attempt: _Attempt,
    ) -> Any:
        upload = binding.provider_media
        if not isinstance(upload, TelegramVerifiedUpload):
            compile_media = getattr(self._types, "media", None)
            if spoiler and not callable(compile_media):
                raise SocialWorkspaceValidationError("media spoiler is unsupported")
            return (
                compile_media(upload, spoiler=spoiler)
                if callable(compile_media)
                else upload
            )
        data = await self._read_upload_bytes(upload, role=binding.role)
        compile_upload = getattr(self._types, "upload_media", None)
        if not callable(compile_upload) or not callable(getattr(client, "upload_file", None)):
            raise TelegramWorkspaceError("provider_dependency_unavailable")
        extension = {
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/webp": "webp",
            "video/mp4": "mp4",
            "video/quicktime": "mov",
            "video/webm": "webm",
        }[upload.mime_type]
        stream = io.BytesIO(data)
        stream.name = f"verified-asset.{extension}"
        attempt.provider_mutation_attempted = True
        uploaded = await _await(
            client.upload_file(
                stream,
                file_size=upload.byte_length,
                file_name=stream.name,
            )
        )
        return compile_upload(
            uploaded,
            role=binding.role,
            mime_type=upload.mime_type,
            width=upload.width,
            height=upload.height,
            duration=upload.duration,
            spoiler=spoiler,
        )

    @staticmethod
    def _cursor_binding(
        request: SocialReadRequest, *, target_ref: str | None = None
    ) -> dict[str, Any]:
        return {
            "platform": request.platform.value,
            "operation": request.operation.value,
            "target_ref": target_ref if target_ref is not None else request.target_ref,
            "item_ref": request.item_ref,
            "sample_ref": request.sample_ref,
            "query": request.query,
            "limit": request.limit,
            "date_from": request.date_from,
            "date_to": request.date_to,
            "read_access": (
                request.read_access.value if request.read_access is not None else None
            ),
            "purpose": request.purpose.value if request.purpose is not None else None,
            "page_size": request.page_size,
            "total_limit": request.total_limit,
            "item_kinds": [value.value for value in request.item_kinds],
            "expected_target_kinds": [
                value.value for value in request.expected_target_kinds
            ],
        }

    def _cursor(
        self,
        family: str,
        value: str | None,
        *,
        binding: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if value is None:
            return {}
        state = self._refs.resolve_cursor(family=family, cursor=value)
        if not isinstance(state, Mapping):
            raise SocialWorkspaceValidationError("cursor binding is invalid")
        if state.get("_binding") != dict(binding):
            raise SocialWorkspaceValidationError("cursor request binding mismatch")
        return state

    def _new_cursor(
        self,
        family: str,
        state: Mapping[str, Any],
        *,
        binding: Mapping[str, Any],
    ) -> str:
        if "_binding" in state:
            raise SocialWorkspaceValidationError("cursor state contains reserved binding")
        bound_state = {**dict(state), "_binding": dict(binding)}
        cursor = self._refs.mint_cursor(family=family, state=bound_state)
        if not isinstance(cursor, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,512}", cursor):
            raise SocialWorkspaceValidationError("cursor minter returned invalid cursor")
        return cursor

    async def _iterate(self, iterator: Any) -> list[Any]:
        values: list[Any] = []
        if hasattr(iterator, "__aiter__"):
            async for value in iterator:
                values.append(value)
        else:
            resolved = await _await(iterator)
            if not isinstance(resolved, Sequence):
                raise TelegramWorkspaceError("invalid_provider_response")
            values.extend(resolved)
        return values

    def _mint_target(self, entity: Any, *, is_self: bool = False) -> TelegramTargetBinding:
        kind = SocialTargetKind.SELF if is_self else _entity_kind(entity)
        handle = _entity_handle(entity)
        binding = self._refs.mint_target(
            entity=entity,
            kind=kind,
            title="Saved Messages" if is_self else _entity_title(entity, kind),
            canonical_handle=handle,
            profile_link=f"https://t.me/{handle}" if handle else None,
            is_self=is_self,
        )
        if not isinstance(binding, TelegramTargetBinding):
            raise SocialWorkspaceValidationError("target minter returned invalid binding")
        validate_opaque_ref(binding.target_ref, "target")
        return binding

    def _target_preview(self, binding: TelegramTargetBinding) -> dict[str, Any]:
        result: dict[str, Any] = {
            "platform": "telegram",
            "target_ref": binding.target_ref,
            "kind": binding.kind.value,
            "display_name": "Saved Messages" if binding.is_self else _safe_text(
                binding.title, 512, fallback="Telegram target"
            ),
            "is_exact_match": True,
            "trust": _TRUST,
        }
        if binding.canonical_handle:
            result["canonical_handle"] = _safe_text(binding.canonical_handle, 128)
        if binding.profile_link:
            result["profile_link"] = _safe_text(binding.profile_link, 512)
        return result

    async def resolve(self, request: SocialReadRequest) -> Mapping[str, Any]:
        if request.platform is not SocialPlatform.TELEGRAM:
            raise SocialWorkspaceValidationError("Telegram adapter requires telegram platform")
        if (
            request.operation is not SocialReadOperation.RESOLVE_TARGET
            or request.target_locator is None
        ):
            raise SocialWorkspaceValidationError("resolve requires resolve_target request")

        async def run(client: Any, lease: TelegramLease, _attempt: _Attempt) -> Mapping[str, Any]:
            locator = request.target_locator
            await self._fenced(lease)
            if locator.kind is TargetLocatorKind.SELF:
                entity = await _await(client.get_me())
                binding = self._mint_target(entity, is_self=True)
            else:
                assert locator.value is not None
                value: str | int = locator.value
                if locator.kind is TargetLocatorKind.PROFILE_LINK:
                    value = urlsplit(locator.value).path.strip("/")
                elif locator.kind is TargetLocatorKind.PROVIDER_ID:
                    try:
                        value = int(locator.value)
                    except ValueError:
                        raise SocialWorkspaceValidationError("provider id is invalid") from None
                entity = await _await(client.get_entity(value))
                binding = self._mint_target(entity)
                handle = _entity_handle(entity)
                if locator.kind in {TargetLocatorKind.USERNAME, TargetLocatorKind.PROFILE_LINK}:
                    expected = str(value).lstrip("@").casefold()
                    if handle is None or handle.casefold() != expected:
                        raise SocialWorkspaceValidationError("target is not an exact match")
                elif locator.kind is TargetLocatorKind.PROVIDER_ID:
                    entity_id = getattr(entity, "id", None)
                    marked_channel_id = (
                        -int(f"100{entity_id}")
                        if type(entity_id) is int and entity_id > 0
                        else None
                    )
                    marked_chat_id = (
                        -entity_id
                        if binding.kind is SocialTargetKind.GROUP
                        and type(entity_id) is int
                        and entity_id > 0
                        else None
                    )
                    if value not in {entity_id, marked_channel_id, marked_chat_id}:
                        raise SocialWorkspaceValidationError("target is not an exact match")
            if request.expected_target_kinds and binding.kind not in request.expected_target_kinds:
                raise SocialWorkspaceValidationError("resolved target kind mismatch")
            return self._target_preview(binding)

        return await self._session("resolve_target", run)

    @staticmethod
    def _parse_message_link(value: str) -> tuple[str | int, int, SocialReadAccess]:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in {"t.me", "telegram.me"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
            or parsed.query
            or parsed.fragment
        ):
            raise SocialWorkspaceValidationError("Telegram message link is not canonical")
        private = re.fullmatch(r"/c/([1-9][0-9]{0,18})/([1-9][0-9]{0,18})/?", parsed.path)
        if private is not None:
            return -int("100" + private.group(1)), int(private.group(2)), SocialReadAccess.PRIVATE
        public = re.fullmatch(
            r"/([A-Za-z][A-Za-z0-9_]{1,127})/([1-9][0-9]{0,18})/?",
            parsed.path,
        )
        if public is None:
            raise SocialWorkspaceValidationError("Telegram message link is not canonical")
        return public.group(1), int(public.group(2)), SocialReadAccess.PUBLIC

    @staticmethod
    def _source_target_payload(target: TelegramTargetBinding) -> dict[str, Any]:
        result: dict[str, Any] = {
            "target_ref": target.target_ref,
            "kind": target.kind.value,
            "title": _safe_text(target.title, 256, fallback="Telegram target"),
            "about": "",
            "description": "",
            "basic_metrics": {
                "members": _int(getattr(target.entity, "participants_count", None))
            },
            "trust": _TRUST,
        }
        if target.canonical_handle:
            result["canonical_handle"] = _safe_text(target.canonical_handle, 128)
        if target.profile_link:
            result["profile_link"] = _safe_text(target.profile_link, 512)
        return result

    async def _resolve_item_link(
        self, client: Any, lease: TelegramLease, request: SocialReadRequest
    ) -> Mapping[str, Any]:
        locator = request.target_locator
        if (
            locator is None
            or locator.kind is not TargetLocatorKind.PROFILE_LINK
            or locator.value is None
        ):
            raise SocialWorkspaceValidationError("Telegram message-link resolution is required")
        entity_locator, message_id, access = self._parse_message_link(locator.value)
        if request.read_access is not access:
            raise SocialWorkspaceValidationError("Telegram message-link access mode mismatch")
        await self._fenced(lease)
        entity = await _await(client.get_entity(entity_locator))
        target = self._mint_target(entity)
        message = await _await(client.get_messages(target.entity, ids=message_id))
        if message is None or _provider_message_id(message) != message_id or not self._message_matches_target(message, target):
            raise TelegramWorkspaceError("item_not_found")
        album = await self._album_messages_near(client, target, message)
        return {
            "item": self._item_payload(message, target, album_messages=album),
            "source_target": self._source_target_payload(target),
            "trust": _TRUST,
        }

    async def _live_policy(
        self,
        client: Any,
        binding: TelegramTargetBinding,
        *,
        content: RichContent | None = None,
    ) -> _LivePolicy:
        configured = (
            set(binding.allowed_actions) if binding.allowed_actions is not None else None
        )
        features = set(ContentFeature)
        # This release has a byte-verifying image ingestor only. Story video
        # reads remain supported, but new video writes stay fail-closed.
        features.discard(ContentFeature.VIDEO)
        if binding.kind is SocialTargetKind.SELF:
            actions = {
                SocialAction.SEND_MESSAGE,
                SocialAction.EDIT,
                SocialAction.DELETE,
                SocialAction.FORWARD,
                SocialAction.REACTION,
                SocialAction.SCHEDULE,
            }
            if self._story_privacy(binding) is not None:
                actions.add(SocialAction.STORY)
            return _LivePolicy(
                frozenset(actions if configured is None else actions & configured),
                frozenset(features),
            )
        if binding.kind is SocialTargetKind.USER:
            actions = {
                SocialAction.SEND_MESSAGE,
                SocialAction.EDIT,
                SocialAction.DELETE,
                SocialAction.FORWARD,
                SocialAction.REACTION,
                SocialAction.SCHEDULE,
            }
            return _LivePolicy(
                frozenset(actions if configured is None else actions & configured),
                frozenset(features),
            )
        permissions = None
        if callable(getattr(client, "get_permissions", None)):
            try:
                permissions_peer = copy.deepcopy(binding.entity)
            except Exception:  # noqa: BLE001 - provider entity snapshot boundary
                raise SocialWorkspaceValidationError(
                    "capability peer cannot be snapshotted"
                ) from None
            if permissions_peer is binding.entity:
                raise SocialWorkspaceValidationError(
                    "capability peer is not detachable"
                )
            permissions = await _await(client.get_permissions(permissions_peer, "me"))
        creator = (
            bool(getattr(permissions, "is_creator", False))
            if permissions is not None
            else bool(getattr(binding.entity, "creator", False))
        )
        entity_rights = getattr(binding.entity, "admin_rights", None)
        participant = getattr(permissions, "participant", None)
        participant_rights = getattr(participant, "admin_rights", None)
        participant_banned_rights = getattr(participant, "banned_rights", None)

        def live_right(name: str) -> bool:
            if permissions is not None:
                return bool(getattr(permissions, name, False))
            return bool(getattr(entity_rights, name, False))

        is_admin = creator or live_right("is_admin")
        default_banned_rights = getattr(binding.entity, "default_banned_rights", None)
        banned_rights = (
            ()
            if is_admin
            else tuple(
                value
                for value in (default_banned_rights, participant_banned_rights)
                if value is not None
            )
        )

        def restricted(name: str) -> bool:
            return any(
                bool(getattr(rights, name, False)) for rights in banned_rights
            )
        has_left = bool(getattr(permissions, "has_left", False)) or bool(
            getattr(participant, "left", False)
        )
        cannot_view = restricted("view_messages")
        is_restricted_member = bool(getattr(permissions, "is_banned", False))
        is_present_member = (
            creator
            or is_admin
            or bool(getattr(permissions, "has_default_permissions", False))
            or (
                is_restricted_member
                and participant_banned_rights is not None
                and not has_left
                and not cannot_view
            )
        )
        can_send_base = (
            binding.kind is SocialTargetKind.GROUP
            and is_present_member
            and not has_left
            and not cannot_view
            and not restricted("send_messages")
        )
        media_bans = {
            ContentFeature.IMAGE: "send_photos",
            ContentFeature.VIDEO: "send_videos",
            ContentFeature.DOCUMENT: "send_docs",
            ContentFeature.AUDIO: "send_audios",
            ContentFeature.ANIMATION: "send_gifs",
        }

        def media_allowed(feature: ContentFeature) -> bool:
            return not restricted("send_media") and not restricted(media_bans[feature])

        content_allowed = True
        if binding.kind is SocialTargetKind.GROUP:
            allowed_media = (
                {feature for feature in media_bans if media_allowed(feature)}
                if can_send_base
                else set()
            )
            plain_allowed = can_send_base and not restricted("send_plain")
            if not (plain_allowed or allowed_media):
                features.clear()
            else:
                features -= set(media_bans) - allowed_media
                if restricted("embed_links"):
                    features.discard(ContentFeature.LINKS)
                if restricted("send_stickers"):
                    features.discard(ContentFeature.CUSTOM_EMOJI)

            content_allowed = plain_allowed or bool(allowed_media)
            if content is not None:
                content_allowed = (
                    can_send_base
                    and (
                        plain_allowed
                        if not content.media
                        else all(
                            ContentFeature(attachment.role.value) in allowed_media
                            for attachment in content.media
                        )
                    )
                    and not (
                        ContentFeature.LINKS in content.features
                        and restricted("embed_links")
                    )
                    and not (
                        ContentFeature.CUSTOM_EMOJI in content.features
                        and restricted("send_stickers")
                    )
                )
        can_post = creator or live_right("post_messages")
        can_edit = creator or live_right("edit_messages")
        can_delete = creator or live_right("delete_messages")
        can_story = creator or bool(
            getattr(participant_rights, "post_stories", False)
        ) or (
            permissions is None and bool(getattr(entity_rights, "post_stories", False))
        )
        actions: set[SocialAction] = set()
        if binding.kind is SocialTargetKind.CHANNEL and can_post:
            actions.update({SocialAction.PUBLISH, SocialAction.SCHEDULE})
        if binding.kind is SocialTargetKind.CHANNEL and can_post:
            actions.update(
                {SocialAction.FORWARD, SocialAction.REACTION, SocialAction.COMMENT}
            )
        if binding.kind is SocialTargetKind.GROUP and content_allowed:
            actions.update(
                {
                    SocialAction.SEND_MESSAGE,
                    SocialAction.PUBLISH,
                    SocialAction.COMMENT,
                    SocialAction.SCHEDULE,
                }
            )
        if binding.kind is SocialTargetKind.GROUP and can_send_base:
            unknown_forward_restrictions = (
                "send_plain",
                "send_media",
                "send_photos",
                "send_videos",
                "send_roundvideos",
                "send_docs",
                "send_audios",
                "send_voices",
                "send_gifs",
                "send_stickers",
                "send_games",
                "send_inline",
                "send_polls",
                "embed_links",
            )
            if not any(restricted(name) for name in unknown_forward_restrictions):
                actions.add(SocialAction.FORWARD)
            if not restricted("send_reactions"):
                actions.add(SocialAction.REACTION)
        if can_edit:
            actions.add(SocialAction.EDIT)
        if can_delete:
            actions.add(SocialAction.DELETE)
        if self._story_privacy(binding) and can_story:
            actions.add(SocialAction.STORY)
        return _LivePolicy(
            frozenset(actions if configured is None else actions & configured),
            frozenset(features),
        )

    def _story_privacy(self, binding: TelegramTargetBinding) -> tuple[Any, ...] | None:
        if binding.story_privacy:
            return binding.story_privacy
        if binding.kind is not SocialTargetKind.CHANNEL:
            return None
        public_privacy = getattr(self._types, "public_story_privacy", None)
        if not callable(public_privacy):
            return None
        rules = public_privacy()
        if not isinstance(rules, tuple) or not rules:
            raise TelegramWorkspaceError("provider_dependency_unavailable")
        return rules

    async def _live_actions(
        self,
        client: Any,
        binding: TelegramTargetBinding,
        *,
        content: RichContent | None = None,
    ) -> set[SocialAction]:
        return set((await self._live_policy(client, binding, content=content)).actions)

    async def _capabilities_with_client(
        self, client: Any, binding: TelegramTargetBinding | None
    ) -> Mapping[str, Any]:
        ensure = getattr(self._types, "ensure", None)
        if callable(ensure):
            ensure()
        reads = set(SocialReadOperation)
        if binding and binding.kind in {SocialTargetKind.SELF, SocialTargetKind.USER}:
            reads.discard(SocialReadOperation.EDITORIAL_SAMPLE)
            reads.discard(SocialReadOperation.GET_AUDIENCE)
        policy = await self._live_policy(client, binding) if binding else None
        actions = set(policy.actions) if policy else set()
        content_features = (
            set(policy.content_features) if policy else set(ContentFeature)
        )
        if binding is not None and SocialAction.SEND_MESSAGE not in actions:
            content_features.discard(ContentFeature.DOCUMENT)
        return {
            "platform": "telegram",
            **({"target_ref": binding.target_ref} if binding else {}),
            "target_kinds": (
                [binding.kind.value]
                if binding
                else [kind.value for kind in (
                    SocialTargetKind.SELF,
                    SocialTargetKind.USER,
                    SocialTargetKind.CHANNEL,
                    SocialTargetKind.GROUP,
                )]
            ),
            "read_operations": sorted(value.value for value in reads),
            "actions": sorted(value.value for value in actions),
            "content_features": sorted(value.value for value in content_features),
            "max_text_length": 4096,
            "max_media_items": (
                10
                if content_features
                & {
                    ContentFeature.IMAGE,
                    ContentFeature.VIDEO,
                    ContentFeature.DOCUMENT,
                    ContentFeature.AUDIO,
                    ContentFeature.ANIMATION,
                }
                else 0
            ),
        }

    async def capabilities(self, target_ref: str | None) -> Mapping[str, Any]:
        binding = self._target(target_ref) if target_ref is not None else None

        async def run(client: Any, _lease: TelegramLease, _attempt: _Attempt) -> Mapping[str, Any]:
            return await self._capabilities_with_client(client, binding)

        return await self._session("capabilities", run)

    def _compile_entities(self, content: RichContent) -> list[Any]:
        compiled: list[Any] = []
        for entity in content.entities:
            offset, length = _utf16_range(content.text, entity.offset, entity.length)
            extra: dict[str, Any] = {}
            if entity.kind is RichEntityKind.LINK:
                extra["url"] = entity.link_target
            elif entity.kind is RichEntityKind.MENTION:
                target = self._target(entity.mention_target_ref or "")
                if target.kind is not SocialTargetKind.USER:
                    raise SocialWorkspaceValidationError("mention target must be a user")
                extra["user"] = target.entity
            elif entity.kind is RichEntityKind.CUSTOM_EMOJI:
                asset = self._asset(entity.custom_emoji_asset_ref or "")
                document_id = getattr(asset.provider_media, "id", None)
                if type(document_id) is not int or document_id <= 0:
                    raise SocialWorkspaceValidationError("custom emoji asset is invalid")
                extra["document_id"] = document_id
            compiled.append(
                self._types.entity(entity.kind, offset=offset, length=length, **extra)
            )
        return compiled

    def _compile_media(self, content: RichContent) -> list[TelegramAssetBinding]:
        assets = [self._asset(media.asset_ref) for media in content.media]
        for supplied, staged in zip(content.media, assets):
            if supplied.role is not staged.role:
                raise SocialWorkspaceValidationError("media role does not match staged asset")
        return assets

    @staticmethod
    def _document_metadata(message: Any) -> tuple[Any | None, str | None, int | None]:
        media = getattr(message, "media", None)
        document = getattr(media, "document", None)
        if document is None:
            document = getattr(message, "document", None)
        if document is None:
            return None, None, None
        file = getattr(message, "file", None)
        file_name = getattr(file, "name", None)
        file_size = getattr(file, "size", None)
        if file_name is None:
            for attribute in list(getattr(document, "attributes", None) or [])[:100]:
                candidate = getattr(attribute, "file_name", None)
                if isinstance(candidate, str):
                    file_name = candidate
                    break
        if file_size is None:
            file_size = getattr(document, "size", None)
        return (
            document,
            file_name if isinstance(file_name, str) else None,
            file_size if type(file_size) is int else None,
        )

    def _message_matches_target(
        self, message: Any, target: TelegramTargetBinding
    ) -> bool:
        marker = getattr(message, "_workspace_entity", None)
        if marker is not None:
            return getattr(marker, "id", None) == getattr(target.entity, "id", None)
        observed_chat_id = getattr(message, "chat_id", None)
        peer_id = getattr(self._types, "peer_id", None)
        if type(observed_chat_id) is int and callable(peer_id):
            try:
                return observed_chat_id == peer_id(target.entity)
            except Exception:  # noqa: BLE001 - normalize provider metadata drift
                return False
        # ``get_messages(entity, ids=...)`` is already scoped to the snapshotted
        # target. Some Telethon test doubles/provider layers omit peer metadata.
        return True

    def _item_payload(
        self,
        message: Any,
        target: TelegramTargetBinding,
        *,
        kind: SocialItemKind | None = None,
        album_messages: Sequence[Any] | None = None,
    ) -> dict[str, Any]:
        members = self._ordered_album_members(message, album_messages)
        representative = next(
            (
                member
                for member in members
                if _safe_text(getattr(member, "message", None), 4096)
            ),
            members[0],
        )
        message_id = _provider_message_id(representative)
        text = _safe_text(getattr(representative, "message", None), 4096)
        selected_kind = kind or (
            SocialItemKind.POST
            if target.kind is SocialTargetKind.CHANNEL
            else SocialItemKind.MESSAGE
        )
        item = self._mint_item_binding(
            target_ref=target.target_ref,
            message_id=message_id,
            kind=selected_kind,
            allowed_actions=None,
        )
        payload: dict[str, Any] = {
            "item_ref": item.item_ref,
            "target_ref": target.target_ref,
            "kind": selected_kind.value,
            "published_at": _utc(getattr(representative, "date", None)),
            "text": text,
            "caption": "",
            "basic_metrics": _message_metrics(representative),
            "trust": _TRUST,
        }
        media_refs: list[str] = []
        attachments: list[dict[str, Any]] = []
        for member in members[:10]:
            media = getattr(member, "media", None)
            detail = self._message_media_detail(member)
            if media is None or detail is None:
                continue
            media_ref = self._mint_read_asset(
                target_ref=target.target_ref,
                media=media,
                role=detail["role"],
                media_kind=detail["kind"],
                mime_type=detail.get("mime_type"),
                byte_length=detail.get("byte_length"),
                duration_seconds=detail.get("duration_seconds"),
                item_message_id=_provider_message_id(member),
            )
            validate_opaque_ref(media_ref, "asset")
            media_refs.append(media_ref)
            binding = self._asset(media_ref)
            attachment: dict[str, Any] = {
                "asset_ref": media_ref,
                "kind": detail["kind"],
                "trust": _TRUST,
            }
            for name in ("mime_type", "byte_length", "duration_seconds"):
                value = detail.get(name)
                if value is not None:
                    attachment[name] = value
            if binding.identity_fingerprint:
                attachment["binding_fingerprint"] = binding.identity_fingerprint
            attachments.append(attachment)
        if media_refs:
            payload["media"] = media_refs
            payload["attachments"] = attachments
        return payload

    @staticmethod
    def _message_grouped_id(message: Any) -> int | None:
        grouped_id = getattr(message, "grouped_id", None)
        return grouped_id if type(grouped_id) is int and grouped_id != 0 else None

    @staticmethod
    def _message_media_detail(message: Any) -> dict[str, Any] | None:
        media = getattr(message, "media", None)
        if media is None:
            return None
        if media.__class__.__name__ == "MessageMediaPhoto" or getattr(
            media, "photo", None
        ) is not None:
            return {"role": MediaRole.IMAGE, "kind": "photo", "mime_type": "image/jpeg"}
        document = getattr(message, "document", None) or getattr(media, "document", None)
        if document is None:
            return None
        mime_type = getattr(document, "mime_type", None)
        mime_type = mime_type.lower() if isinstance(mime_type, str) else ""
        attributes = list(getattr(document, "attributes", None) or [])[:100]
        audio_attr = next(
            (value for value in attributes if value.__class__.__name__ == "DocumentAttributeAudio"),
            None,
        )
        video_attr = next(
            (value for value in attributes if value.__class__.__name__ == "DocumentAttributeVideo"),
            None,
        )
        animated = any(
            value.__class__.__name__ == "DocumentAttributeAnimated" for value in attributes
        )
        duration = getattr(audio_attr or video_attr, "duration", None)
        detail: dict[str, Any] = {
            "mime_type": mime_type or "application/octet-stream",
            "byte_length": max(0, getattr(document, "size", 0))
            if type(getattr(document, "size", None)) is int
            else None,
            "duration_seconds": float(duration)
            if type(duration) in {int, float} and duration >= 0
            else None,
        }
        if audio_attr is not None or mime_type.startswith("audio/") or getattr(message, "voice", None) is not None or getattr(message, "audio", None) is not None:
            detail.update(
                role=MediaRole.AUDIO,
                kind="voice" if bool(getattr(audio_attr, "voice", False)) or getattr(message, "voice", None) is not None else "audio",
            )
        elif bool(getattr(video_attr, "round_message", False)) or getattr(message, "video_note", None) is not None:
            detail.update(role=MediaRole.VIDEO, kind="round_video")
        elif animated or mime_type == "image/gif" or getattr(message, "gif", None) is not None:
            detail.update(role=MediaRole.ANIMATION, kind="animation")
        elif mime_type.startswith("video/") or getattr(message, "video", None) is not None:
            detail.update(role=MediaRole.VIDEO, kind="video")
        elif mime_type.startswith("image/"):
            detail.update(role=MediaRole.IMAGE, kind="photo")
        else:
            detail.update(role=MediaRole.DOCUMENT, kind="document")
        return {name: value for name, value in detail.items() if value is not None}

    @classmethod
    def _ordered_album_members(
        cls, message: Any, album_messages: Sequence[Any] | None
    ) -> list[Any]:
        grouped_id = cls._message_grouped_id(message)
        if grouped_id is None:
            return [message]
        candidates = list(album_messages or ())
        if not any(
            _provider_message_id(candidate) == _provider_message_id(message)
            for candidate in candidates
        ):
            candidates.append(message)
        members = [
            candidate
            for candidate in candidates
            if cls._message_grouped_id(candidate) == grouped_id
        ]
        members.sort(key=_provider_message_id)
        return members[:10] or [message]

    @classmethod
    def _logical_message_groups(cls, messages: Sequence[Any]) -> list[list[Any]]:
        grouped: dict[int, list[Any]] = {}
        for message in messages:
            grouped_id = cls._message_grouped_id(message)
            if grouped_id is not None:
                grouped.setdefault(grouped_id, []).append(message)
        results: list[list[Any]] = []
        seen: set[int] = set()
        for message in messages:
            grouped_id = cls._message_grouped_id(message)
            if grouped_id is None:
                results.append([message])
            elif grouped_id not in seen:
                seen.add(grouped_id)
                results.append(
                    cls._ordered_album_members(message, grouped[grouped_id])
                )
        return results

    async def _album_messages_near(
        self, client: Any, target: TelegramTargetBinding, message: Any
    ) -> list[Any]:
        """Fetch at most one Telegram album around an already resolved member."""

        if self._message_grouped_id(message) is None:
            return [message]
        message_id = _provider_message_id(message)
        iterator = client.iter_messages(
            target.entity,
            limit=21,
            # Telegram media groups contain at most ten items with adjacent
            # message ids.  Starting just above that window includes the
            # selected member and all possible siblings without a broad scan.
            offset_id=message_id + 11,
        )
        nearby = await self._iterate(iterator)
        return self._ordered_album_members(message, nearby)

    @staticmethod
    def _story_metrics(story_or_views: Any) -> dict[str, int]:
        views = getattr(story_or_views, "views", None)
        source = views if views is not None and not isinstance(views, int) else story_or_views
        return {
            "views": _int(getattr(source, "views_count", None)),
            "reactions": _int(getattr(source, "reactions_count", None)),
            "comments": 0,
            "shares": _int(getattr(source, "forwards_count", None)),
        }

    @staticmethod
    def _story_media_role(media: Any) -> MediaRole:
        name = media.__class__.__name__
        if name == "MessageMediaPhoto" or getattr(media, "photo", None) is not None:
            return MediaRole.IMAGE
        document = getattr(media, "document", None)
        mime_type = getattr(document, "mime_type", None)
        if (
            name == "MessageMediaDocument"
            and (
                bool(getattr(media, "video", False))
                or (isinstance(mime_type, str) and mime_type.startswith("video/"))
            )
        ):
            return MediaRole.VIDEO
        raise TelegramWorkspaceError("invalid_provider_response")

    def _story_payload(
        self, story: Any, target: TelegramTargetBinding
    ) -> dict[str, Any]:
        story_id = _provider_message_id(story)
        media = getattr(story, "media", None)
        binding = self._mint_item_binding(
            target_ref=target.target_ref,
            message_id=story_id,
            kind=SocialItemKind.STORY,
            allowed_actions=None,
        )
        result: dict[str, Any] = {
            "item_ref": binding.item_ref,
            "target_ref": target.target_ref,
            "kind": SocialItemKind.STORY.value,
            "published_at": _utc(getattr(story, "date", None)),
            "text": _safe_text(getattr(story, "caption", None), 4096),
            "caption": "",
            "basic_metrics": self._story_metrics(story),
            "trust": _TRUST,
        }
        if media is not None:
            role = self._story_media_role(media)
            expires_at = getattr(story, "expire_date", None)
            if expires_at is not None and not isinstance(expires_at, datetime):
                raise TelegramWorkspaceError("invalid_provider_response")
            media_ref = self._mint_read_asset(
                target_ref=target.target_ref,
                media=media,
                role=role,
                story_id=story_id,
                expires_at=expires_at,
            )
            result["media"] = [media_ref]
        return result

    def _page(
        self,
        results: list[dict[str, Any]],
        *,
        family: str,
        more: bool,
        state: Mapping[str, Any] | None,
        binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"results": results, "trust": _TRUST}
        if more and state:
            payload["next_cursor"] = self._new_cursor(
                family, state, binding=binding
            )
        return payload

    async def _target_from_message(self, client: Any, message: Any) -> TelegramTargetBinding:
        entity = getattr(message, "_workspace_entity", None)
        if entity is None:
            peer = getattr(message, "peer_id", None)
            if peer is None:
                raise TelegramWorkspaceError("invalid_provider_response")
            entity = await _await(client.get_entity(peer))
        return self._mint_target(entity)

    async def _read_targets(
        self, client: Any, lease: TelegramLease, request: SocialReadRequest
    ) -> Mapping[str, Any]:
        if request.target_ref:
            target = self._target(request.target_ref)
            if target.kind is not SocialTargetKind.CHANNEL:
                raise SocialWorkspaceValidationError("similar targets require a channel")
            response = await self._call(
                client,
                lease,
                self._types.request("similar_channels", channel=target.entity),
            )
            entities = list(getattr(response, "chats", []) or [])[:_MAX_PAGE]
            offset = 0
        else:
            # The integration maps the fixed dialog-list tool to the closed "*"
            # sentinel because the provider-neutral enum has no LIST_TARGETS member.
            query = "" if request.query == "*" else (request.query or "").casefold()
            cursor_binding = self._cursor_binding(request)
            cursor = self._cursor(
                "target_search", request.cursor, binding=cursor_binding
            )
            offset = cursor.get("offset", 0)
            if type(offset) is not int or offset < 0 or offset > _MAX_GLOBAL_SCAN:
                raise SocialWorkspaceValidationError("target cursor state is invalid")
            await self._fenced(lease)
            iterator = client.iter_dialogs(limit=_MAX_GLOBAL_SCAN)
            dialogs = await self._iterate(iterator)
            entities = [
                getattr(dialog, "entity", dialog)
                for dialog in dialogs
                if not query
                or query in _entity_title(
                    getattr(dialog, "entity", dialog),
                    _entity_kind(getattr(dialog, "entity", dialog)),
                ).casefold()
            ][offset : offset + _MAX_PAGE + 1]
        results = []
        for entity in entities[:_MAX_PAGE]:
            binding = self._mint_target(entity)
            results.append(
                {
                    "target_ref": binding.target_ref,
                    "kind": binding.kind.value,
                    "title": _safe_text(binding.title, 256, fallback="Telegram target"),
                    **(
                        {"canonical_handle": _safe_text(binding.canonical_handle, 128)}
                        if binding.canonical_handle
                        else {}
                    ),
                    **(
                        {"profile_link": _safe_text(binding.profile_link, 512)}
                        if binding.profile_link
                        else {}
                    ),
                    "about": "",
                    "description": "",
                    "basic_metrics": {"members": _int(getattr(entity, "participants_count", None))},
                    "trust": _TRUST,
                }
            )
        return self._page(
            results,
            family="target_search",
            more=not request.target_ref and len(entities) > _MAX_PAGE,
            state={"offset": offset + _MAX_PAGE, "query": request.query or ""},
            binding=self._cursor_binding(request),
        )

    async def _read_messages(
        self,
        client: Any,
        request: SocialReadRequest,
        *,
        target: TelegramTargetBinding,
    ) -> Mapping[str, Any]:
        family = "target_items"
        cursor_binding = self._cursor_binding(request, target_ref=target.target_ref)
        cursor = self._cursor(
            family, request.cursor, binding=cursor_binding
        )
        offset_id = cursor.get("offset_id", 0)
        if type(offset_id) is not int or offset_id < 0:
            raise SocialWorkspaceValidationError("cursor state is invalid")
        limit = min(request.limit, _MAX_PAGE)
        # ``limit`` is a logical-post limit.  Telegram counts every member of a
        # grouped media album as a message, so fetch a bounded expansion before
        # collapsing.  This prevents a three-photo post from consuming three
        # result slots or being truncated to one photo at a page boundary.
        scan_limit = min(_MAX_GLOBAL_SCAN + 1, limit * 10 + 11)
        iterator = client.iter_messages(
            target.entity,
            limit=scan_limit,
            offset_id=offset_id,
            search=(
                request.query
                if request.operation is SocialReadOperation.SEARCH_ITEMS
                else None
            ),
        )
        messages = await self._iterate(iterator)
        logical = self._logical_message_groups(messages)
        selected = logical[:limit]
        results = [
            self._item_payload(group[0], target, album_messages=group)
            for group in selected
        ]
        state = (
            {
                "offset_id": min(
                    _provider_message_id(message) for message in selected[-1]
                ),
                "target_ref": target.target_ref,
                "query": request.query or "",
            }
            if selected
            else None
        )
        return self._page(
            results,
            family=family,
            more=len(logical) > limit or len(messages) >= scan_limit,
            state=state,
            binding=cursor_binding,
        )

    async def _global_search(
        self,
        client: Any,
        lease: TelegramLease,
        request: SocialReadRequest,
    ) -> Mapping[str, Any]:
        cursor_binding = self._cursor_binding(request)
        cursor = self._cursor(
            "global_search", request.cursor, binding=cursor_binding
        )
        offset_id = cursor.get("offset_id", 0)
        if type(offset_id) is not int or offset_id < 0:
            raise SocialWorkspaceValidationError("cursor state is invalid")
        limit = min(request.limit, _MAX_PAGE)
        await self._fenced(lease)
        response = await self._call(
            client,
            lease,
            self._types.request(
                "global_search",
                query=request.query or "",
                offset_id=offset_id,
                offset_rate=cursor.get("offset_rate", 0),
                offset_peer=cursor.get("offset_peer"),
                limit=limit + 1,
            ),
        )
        messages = list(getattr(response, "messages", []) or [])[: limit + 1]
        results: list[dict[str, Any]] = []
        for message in messages[:limit]:
            target = await self._target_from_message(client, message)
            results.append(self._item_payload(message, target))
        last = messages[min(limit, len(messages)) - 1] if results else None
        state = (
            {
                "offset_id": _provider_message_id(last),
                "offset_rate": _int(getattr(response, "next_rate", None)),
                "offset_peer": getattr(last, "peer_id", None),
                "query": request.query or "",
            }
            if last is not None
            else None
        )
        return self._page(
            results,
            family="global_search",
            more=len(messages) > limit,
            state=state,
            binding=cursor_binding,
        )

    async def _editorial(
        self,
        client: Any,
        lease: TelegramLease,
        request: SocialReadRequest,
    ) -> Mapping[str, Any]:
        target = self._target(request.target_ref or "")
        if target.kind not in {SocialTargetKind.CHANNEL, SocialTargetKind.GROUP}:
            raise SocialWorkspaceValidationError("editorial target must be channel or group")
        if (
            type(request.page_size) is not int
            or not 1 <= request.page_size <= _MAX_PAGE
            or type(request.total_limit) is not int
            or not 1 <= request.total_limit <= _MAX_SAMPLE
        ):
            raise SocialWorkspaceValidationError("editorial bounds are invalid")
        if not isinstance(request.sample_ref, str) or not re.fullmatch(
            r"smp_[A-Za-z0-9_-]{24,160}", request.sample_ref
        ):
            raise SocialWorkspaceValidationError("editorial sample_ref is required")
        if request.date_from is not None and request.date_to is not None and (
            request.date_from > request.date_to
        ):
            raise SocialWorkspaceValidationError("editorial date range is reversed")
        cursor_binding = self._cursor_binding(request, target_ref=target.target_ref)
        cursor = self._cursor(
            "editorial_sample", request.cursor, binding=cursor_binding
        )
        offset_id = cursor.get("offset_id", 0)
        cumulative = cursor.get("cumulative_count", 0)
        if type(offset_id) is not int or type(cumulative) is not int or cumulative < 0:
            raise SocialWorkspaceValidationError("editorial cursor state is invalid")
        if cumulative + request.page_size > min(request.total_limit, _MAX_SAMPLE):
            raise SocialWorkspaceValidationError("editorial sample budget exceeded")
        full = await self._call(
            client, lease, self._types.request("full_channel", channel=target.entity)
        )
        await self._fenced(lease)
        iterator = client.iter_messages(
            target.entity,
            limit=_MAX_GLOBAL_SCAN + 1,
            offset_id=offset_id,
        )
        scanned = await self._iterate(iterator)
        messages = []
        for message in scanned:
            published = _utc(getattr(message, "date", None))[:10]
            if request.date_from is not None and published < request.date_from:
                continue
            if request.date_to is not None and published > request.date_to:
                continue
            messages.append(message)
        page = messages[: request.page_size]
        items = [self._item_payload(message, target) for message in page]
        for item in items:
            item.pop("target_ref", None)
            item.pop("media", None)
            item.pop("entities", None)
            item["text"] = item["text"][:768]
            item["caption"] = item["caption"][:256]
        next_count = cumulative + len(items)
        about = _safe_text(
            getattr(getattr(full, "full_chat", None), "about", None),
            1024,
            fallback=target.title or "Telegram target",
        )
        metrics = {
            "members": _int(
                getattr(getattr(full, "full_chat", None), "participants_count", None)
            )
        }
        payload: dict[str, Any] = {
            "sample_ref": request.sample_ref or "",
            "target": {
                "target_ref": target.target_ref,
                "kind": target.kind.value,
                "title": _safe_text(target.title, 256, fallback="Telegram target"),
                "about": about,
                "description": about,
                "basic_metrics": metrics,
                "trust": _TRUST,
            },
            "items": items,
            "sampled_count": len(items),
            "cumulative_count": next_count,
            "total_limit": request.total_limit,
            "storage_disposition": "ephemeral_no_index",
            "trust": _TRUST,
        }
        more = len(messages) > request.page_size or len(scanned) > _MAX_GLOBAL_SCAN
        if more and next_count < request.total_limit and (page or scanned):
            payload["next_cursor"] = self._new_cursor(
                "editorial_sample",
                {
                    "offset_id": _provider_message_id(page[-1] if page else scanned[-1]),
                    "cumulative_count": next_count,
                },
                binding=cursor_binding,
            )
        return payload

    async def _comments(
        self, client: Any, lease: TelegramLease, request: SocialReadRequest
    ) -> Mapping[str, Any]:
        item = self._item(request.item_ref or "")
        target = self._target(item.target_ref)
        cursor_binding = self._cursor_binding(request, target_ref=target.target_ref)
        cursor = self._cursor("comments", request.cursor, binding=cursor_binding)
        offset_id = cursor.get("offset_id", 0)
        response = await self._call(
            client,
            lease,
            self._types.request(
                "comments",
                peer=target.entity,
                message_id=item.message_id,
                offset_id=offset_id,
                limit=min(request.limit, _MAX_PAGE) + 1,
            ),
        )
        messages = list(getattr(response, "messages", []) or [])
        limit = min(request.limit, _MAX_PAGE)
        payload: dict[str, Any] = {
            "root_item_ref": item.item_ref,
            "items": [
                self._item_payload(message, target, kind=SocialItemKind.COMMENT)
                for message in messages[:limit]
            ],
            "trust": _TRUST,
        }
        if len(messages) > limit and messages[:limit]:
            payload["next_cursor"] = self._new_cursor(
                "comments",
                {
                    "offset_id": _provider_message_id(messages[limit - 1]),
                    "item_ref": item.item_ref,
                },
                binding=cursor_binding,
            )
        return payload

    def _reactions(self, message: Any, item_ref: str) -> Mapping[str, Any]:
        results = getattr(getattr(message, "reactions", None), "results", None) or []
        normalized = []
        for result in list(results)[:50]:
            reaction = getattr(result, "reaction", None)
            value = getattr(reaction, "emoticon", None)
            if not isinstance(value, str):
                value = "custom"
            normalized.append({"reaction": value[:32], "count": _int(getattr(result, "count", None))})
        return {"item_ref": item_ref, "reactions": normalized, "trust": _TRUST}

    async def _stories(
        self, client: Any, lease: TelegramLease, request: SocialReadRequest
    ) -> Mapping[str, Any]:
        target = self._target(request.target_ref or "")
        response = await self._call(
            client, lease, self._types.request("peer_stories", peer=target.entity)
        )
        stories = list(
            getattr(getattr(response, "stories", None), "stories", None) or []
        )
        cursor_binding = self._cursor_binding(request, target_ref=target.target_ref)
        cursor = self._cursor("stories", request.cursor, binding=cursor_binding)
        offset = cursor.get("offset", 0)
        if type(offset) is not int or offset < 0:
            raise SocialWorkspaceValidationError("story cursor state is invalid")
        limit = min(request.limit, _MAX_PAGE)
        page = stories[offset : offset + limit]
        results: list[dict[str, Any]] = []
        for story in page:
            results.append(self._story_payload(story, target))
        return self._page(
            results,
            family="stories",
            more=offset + limit < len(stories),
            state={"offset": offset + limit, "target_ref": target.target_ref},
            binding=cursor_binding,
        )

    async def _story_by_id(
        self,
        client: Any,
        lease: TelegramLease,
        target: TelegramTargetBinding,
        story_id: int,
    ) -> Any:
        response = await self._call(
            client,
            lease,
            self._types.request(
                "stories_by_id", peer=target.entity, story_ids=[story_id]
            ),
        )
        matches = [
            story
            for story in list(getattr(response, "stories", None) or [])[:25]
            if getattr(story, "id", None) == story_id
        ]
        if len(matches) != 1:
            raise TelegramWorkspaceError("invalid_provider_response")
        return matches[0]

    async def _story_views(
        self,
        client: Any,
        lease: TelegramLease,
        target: TelegramTargetBinding,
        story_ids: Sequence[int],
    ) -> list[Any]:
        if not story_ids or len(story_ids) > _MAX_PAGE or any(
            type(story_id) is not int or story_id <= 0 for story_id in story_ids
        ):
            raise SocialWorkspaceValidationError("story statistics bounds are invalid")
        response = await self._call(
            client,
            lease,
            self._types.request(
                "stories_views", peer=target.entity, story_ids=list(story_ids)
            ),
        )
        # Intentionally ignore response.users and StoryViews.recent_viewers. The
        # Social Workspace exposes aggregates only and never viewer identities.
        views = list(getattr(response, "views", None) or [])
        if len(views) != len(story_ids):
            raise TelegramWorkspaceError("invalid_provider_response")
        return views

    async def _story_statistics(
        self,
        client: Any,
        lease: TelegramLease,
        request: SocialReadRequest,
    ) -> Mapping[str, Any]:
        now = datetime.now(timezone.utc)
        if request.item_ref:
            item = self._item(request.item_ref)
            if item.kind is not SocialItemKind.STORY:
                raise SocialWorkspaceValidationError("item is not a story")
            target = self._target(item.target_ref)
            story = await self._story_by_id(client, lease, target, item.message_id)
            metrics = self._story_metrics(
                (await self._story_views(client, lease, target, [item.message_id]))[0]
            )
            ref = {"item_ref": item.item_ref}
            dates = [
                _utc(getattr(story, "date", None)),
                now.isoformat().replace("+00:00", "Z"),
            ]
        else:
            target = self._target(request.target_ref or "")
            response = await self._call(
                client, lease, self._types.request("peer_stories", peer=target.entity)
            )
            stories = list(
                getattr(getattr(response, "stories", None), "stories", None) or []
            )[:_MAX_PAGE]
            ids = [_provider_message_id(story) for story in stories]
            metrics = {name: 0 for name in ("views", "reactions", "comments", "shares")}
            if ids:
                for view in await self._story_views(client, lease, target, ids):
                    for name, value in self._story_metrics(view).items():
                        metrics[name] += value
            ref = {"target_ref": target.target_ref}
            dates = [
                _utc(getattr(stories[-1], "date", None))
                if stories
                else now.isoformat().replace("+00:00", "Z"),
                now.isoformat().replace("+00:00", "Z"),
            ]
        return {
            **ref,
            "period_from": dates[0],
            "period_to": dates[1],
            "basic_metrics": metrics,
            "trust": _TRUST,
        }

    async def _statistics(
        self, client: Any, lease: TelegramLease, request: SocialReadRequest
    ) -> Mapping[str, Any]:
        if request.item_ref and self._item(request.item_ref).kind is SocialItemKind.STORY:
            return await self._story_statistics(client, lease, request)
        if SocialItemKind.STORY in request.item_kinds:
            if request.item_kinds != (SocialItemKind.STORY,):
                raise SocialWorkspaceValidationError("story statistics require story-only kind")
            return await self._story_statistics(client, lease, request)
        now = datetime.now(timezone.utc)
        if request.item_ref:
            item = self._item(request.item_ref)
            target = self._target(item.target_ref)
            message = await _await(client.get_messages(target.entity, ids=item.message_id))
            metrics = _message_metrics(message)
            ref = {"item_ref": item.item_ref}
            dates = [_utc(getattr(message, "date", None)), now.isoformat().replace("+00:00", "Z")]
        else:
            target = self._target(request.target_ref or "")
            messages = await self._iterate(client.iter_messages(target.entity, limit=25))
            metrics = {name: 0 for name in ("views", "reactions", "comments", "shares")}
            for message in messages:
                for name, value in _message_metrics(message).items():
                    metrics[name] += value
            ref = {"target_ref": target.target_ref}
            dates = [
                _utc(getattr(messages[-1], "date", None)) if messages else now.isoformat().replace("+00:00", "Z"),
                now.isoformat().replace("+00:00", "Z"),
            ]
        return {
            **ref,
            "period_from": dates[0],
            "period_to": dates[1],
            "basic_metrics": metrics,
            "trust": _TRUST,
        }

    async def read(self, request: SocialReadRequest) -> Mapping[str, Any]:
        if request.platform is not SocialPlatform.TELEGRAM:
            raise SocialWorkspaceValidationError("Telegram adapter requires telegram platform")
        if request.operation is SocialReadOperation.RESOLVE_TARGET:
            return await self.resolve(request)

        async def run(client: Any, lease: TelegramLease, _attempt: _Attempt) -> Mapping[str, Any]:
            operation = request.operation
            if operation is SocialReadOperation.RESOLVE_ITEM:
                return await self._resolve_item_link(client, lease, request)
            if operation is SocialReadOperation.SEARCH_TARGETS:
                return await self._read_targets(client, lease, request)
            if operation is SocialReadOperation.SEARCH_ITEMS and request.target_ref is None:
                return await self._global_search(client, lease, request)
            if operation in {SocialReadOperation.LIST_ITEMS, SocialReadOperation.SEARCH_ITEMS}:
                return await self._read_messages(
                    client, request, target=self._target(request.target_ref or "")
                )
            if operation is SocialReadOperation.GET_ITEM:
                item = self._item(request.item_ref or "")
                target = self._target(item.target_ref)
                if item.kind is SocialItemKind.STORY:
                    story = await self._story_by_id(
                        client, lease, target, item.message_id
                    )
                    return {"item": self._story_payload(story, target), "trust": _TRUST}
                message = await _await(client.get_messages(target.entity, ids=item.message_id))
                album = await self._album_messages_near(client, target, message)
                return {
                    "item": self._item_payload(
                        message, target, album_messages=album
                    ),
                    "trust": _TRUST,
                }
            if operation is SocialReadOperation.LIST_COMMENTS:
                return await self._comments(client, lease, request)
            if operation is SocialReadOperation.LIST_REACTIONS:
                item = self._item(request.item_ref or "")
                target = self._target(item.target_ref)
                message = await _await(client.get_messages(target.entity, ids=item.message_id))
                return self._reactions(message, item.item_ref)
            if operation is SocialReadOperation.LIST_STORIES:
                return await self._stories(client, lease, request)
            if operation is SocialReadOperation.GET_STATISTICS:
                return await self._statistics(client, lease, request)
            if operation is SocialReadOperation.GET_AUDIENCE:
                target = self._target(request.target_ref or "")
                if target.kind not in {SocialTargetKind.CHANNEL, SocialTargetKind.GROUP}:
                    raise SocialWorkspaceValidationError("audience requires channel or group")
                full = await self._call(
                    client, lease, self._types.request("full_channel", channel=target.entity)
                )
                return {
                    "target_ref": target.target_ref,
                    "audience": {
                        "total": _int(
                            getattr(getattr(full, "full_chat", None), "participants_count", None)
                        )
                    },
                    "trust": _TRUST,
                }
            if operation is SocialReadOperation.EDITORIAL_SAMPLE:
                return await self._editorial(client, lease, request)
            raise SocialWorkspaceValidationError("unsupported Telegram read operation")

        return await self._session(request.operation.value, run)

    async def _preflight(
        self, client: Any, lease: TelegramLease, intent: SocialActionIntent
    ) -> _PreflightSnapshot:
        item = self._item(intent.item_ref) if intent.item_ref else None
        source = self._target(item.target_ref) if item is not None else None
        if intent.action is SocialAction.FORWARD:
            target = self._target(intent.destination_target_ref or "")
        elif source is not None:
            target = source
        else:
            target = self._target(intent.target_ref or "")
        checked_target = source if source is not None else target
        actions = await self._live_actions(
            client, checked_target, content=intent.content
        )
        if intent.action not in actions:
            raise SocialWorkspaceValidationError("capability denied: unsupported_action")
        if intent.action is SocialAction.STORY:
            can_send_request = getattr(self._types, "request", None)
            if self._types.__class__ is _DefaultTelethonTypes or callable(
                getattr(self._types, "upload_media", None)
            ):
                if not callable(can_send_request):
                    raise TelegramWorkspaceError("provider_dependency_unavailable")
                allowed = await self._call(
                    client,
                    lease,
                    can_send_request("can_send_story", peer=target.entity),
                )
                count_remains = getattr(allowed, "count_remains", None)
                if type(count_remains) is not int or count_remains <= 0:
                    raise SocialWorkspaceValidationError(
                        "capability denied: story_quota_or_rights"
                    )
        if intent.action is SocialAction.FORWARD:
            destination_actions = await self._live_actions(client, target)
            if SocialAction.FORWARD not in destination_actions:
                raise SocialWorkspaceValidationError(
                    "capability denied: forward_destination"
                )
        if item and item.allowed_actions is not None and intent.action not in item.allowed_actions:
            raise SocialWorkspaceValidationError("capability denied: item_action")
        if intent.action is SocialAction.SEND_MESSAGE and target.kind not in {
            SocialTargetKind.SELF,
            SocialTargetKind.USER,
            SocialTargetKind.GROUP,
        }:
            raise SocialWorkspaceValidationError(
                "send_message requires Saved Messages, user DM, or writable group"
            )
        if intent.action is SocialAction.PUBLISH and target.kind not in {
            SocialTargetKind.CHANNEL,
            SocialTargetKind.GROUP,
        }:
            raise SocialWorkspaceValidationError("publish requires channel or group")
        if intent.action is SocialAction.STORY:
            if self._story_privacy(target) is None:
                raise SocialWorkspaceValidationError(
                    "story privacy is unavailable"
                )
            if intent.content is None or len(intent.content.media) != 1:
                raise SocialWorkspaceValidationError("story requires exactly one media asset")
            if intent.content.media[0].role is not MediaRole.IMAGE:
                raise SocialWorkspaceValidationError("story media must be a verified image")
        document_media = (
            tuple(
                attachment
                for attachment in intent.content.media
                if attachment.role is MediaRole.DOCUMENT
            )
            if intent.content is not None
            else ()
        )
        if document_media and (
            intent.action is not SocialAction.SEND_MESSAGE
            or intent.content is None
            or len(intent.content.media) != 1
            or len(document_media) != 1
        ):
            raise SocialWorkspaceValidationError(
                "Telegram documents require send_message with exactly one document"
            )
        if intent.content and (
            len(intent.content.text) > 4096 or len(intent.content.media) > 10
        ):
            raise SocialWorkspaceValidationError("capability denied: content limit")
        return _PreflightSnapshot(target=target, source=source, item=item)

    async def _send_content(
        self,
        client: Any,
        target: TelegramTargetBinding,
        content: RichContent,
        *,
        attempt: _Attempt,
        schedule: datetime | None = None,
        comment_to: int | None = None,
        reply_to: int | None = None,
    ) -> Any:
        entities = self._compile_entities(content)
        assets = self._compile_media(content)
        if len(assets) == 1 and assets[0].role is MediaRole.DOCUMENT:
            upload = assets[0].provider_media
            if not isinstance(upload, TelegramVerifiedUpload):
                raise SocialWorkspaceValidationError(
                    "document asset is not a verified immutable upload"
                )
            data = await self._read_upload_bytes(upload, role=MediaRole.DOCUMENT)
            compile_filename = getattr(self._types, "document_filename", None)
            if not callable(compile_filename) or upload.display_name is None:
                raise TelegramWorkspaceError("provider_dependency_unavailable")
            stream = io.BytesIO(data)
            stream.name = upload.display_name
            attributes = [compile_filename(upload.display_name)]
            attempt.provider_mutation_attempted = True
            try:
                return await _await(
                    client.send_file(
                        target.entity,
                        stream,
                        caption=content.text,
                        formatting_entities=entities,
                        parse_mode=None,
                        force_document=True,
                        mime_type=upload.mime_type,
                        file_size=upload.byte_length,
                        attributes=attributes,
                    )
                )
            except (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError):
                raise
            except Exception as exc:
                if _is_definite_provider_rejection(exc):
                    raise TelegramWorkspaceError(
                        "provider_rejected", retry_safe=False
                    ) from None
                raise
        if assets:
            media_values = []
            for attachment, asset in zip(content.media, assets):
                media_values.append(
                    await self._provider_media(
                        client,
                        asset,
                        spoiler=attachment.spoiler,
                        attempt=attempt,
                    )
                )
            attempt.provider_mutation_attempted = True
            return await _await(
                client.send_file(
                    target.entity,
                    media_values,
                    caption=content.text,
                    formatting_entities=entities,
                    schedule=schedule,
                    comment_to=comment_to,
                    reply_to=reply_to,
                )
            )
        attempt.provider_mutation_attempted = True
        return await _await(
            client.send_message(
                target.entity,
                content.text,
                formatting_entities=entities,
                parse_mode=None,
                link_preview=False,
                schedule=schedule,
                comment_to=comment_to,
                reply_to=reply_to,
            )
        )

    async def _claim_operation(
        self, operation_ref: str, action_digest: str
    ) -> Mapping[str, Any] | None:
        try:
            claim = await _await(
                self._refs.claim_operation(
                    operation_ref=operation_ref, action_digest=action_digest
                )
            )
        except Exception:  # noqa: BLE001 - opaque durable ledger boundary
            raise TelegramWorkspaceError(
                "operation_ledger_failed", retry_safe=False
            ) from None
        if (
            not isinstance(claim, TelegramOperationClaim)
            or claim.operation_ref != operation_ref
        ):
            raise SocialWorkspaceValidationError("operation claim binding mismatch")
        if claim.action_digest != action_digest:
            raise SocialWorkspaceValidationError("operation_ref intent conflict")
        if claim.result is not None:
            return dict(
                self._recordless_operation_validation(operation_ref, claim.result)
            )
        if claim.claimed_now is not True:
            raise TelegramWorkspaceError("operation_in_progress", retry_safe=False)
        return None

    async def _release_operation(self, operation_ref: str, action_digest: str) -> None:
        try:
            released = await _await(
                self._refs.release_operation(
                    operation_ref=operation_ref, action_digest=action_digest
                )
            )
        except Exception:  # noqa: BLE001 - opaque durable ledger boundary
            raise TelegramWorkspaceError(
                "operation_ledger_failed", retry_safe=False
            ) from None
        if released is not True:
            raise TelegramWorkspaceError("operation_ledger_failed", retry_safe=False)

    async def _complete_operation(
        self,
        operation_ref: str,
        action_digest: str,
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        self._recordless_operation_validation(operation_ref, result)
        try:
            claim = await _await(
                self._refs.complete_operation(
                    operation_ref=operation_ref,
                    action_digest=action_digest,
                    result=dict(result),
                )
            )
        except Exception:  # noqa: BLE001 - opaque durable ledger boundary
            raise TelegramWorkspaceError(
                "operation_ledger_failed", retry_safe=False
            ) from None
        if (
            not isinstance(claim, TelegramOperationClaim)
            or claim.operation_ref != operation_ref
            or claim.action_digest != action_digest
            or claim.result is None
        ):
            raise SocialWorkspaceValidationError("operation completion binding mismatch")
        return dict(self._recordless_operation_validation(operation_ref, claim.result))

    async def reconcile(self, operation_ref: str) -> Mapping[str, Any]:
        if not isinstance(operation_ref, str) or not re.fullmatch(
            r"op_[A-Za-z0-9_-]{24,160}", operation_ref
        ):
            raise SocialWorkspaceValidationError("operation_ref is invalid")
        try:
            claim = await _await(self._refs.resolve_operation(operation_ref))
        except Exception:  # noqa: BLE001 - normalize opaque ledger failures
            raise TelegramWorkspaceError("operation_not_found", retry_safe=False) from None
        if (
            not isinstance(claim, TelegramOperationClaim)
            or claim.operation_ref != operation_ref
        ):
            raise SocialWorkspaceValidationError("operation ledger binding mismatch")
        if claim.result is None:
            raise TelegramWorkspaceError("operation_in_progress", retry_safe=False)
        return dict(self._recordless_operation_validation(operation_ref, claim.result))

    def _recordless_operation_validation(
        self, operation_ref: str, result: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        if result.get("operation_ref") != operation_ref or result.get("platform") != "telegram":
            raise SocialWorkspaceValidationError("operation ledger binding mismatch")
        if result.get("target_ref") is not None:
            validate_opaque_ref(result.get("target_ref"), "target")
        if result.get("item_ref") is not None:
            validate_opaque_ref(result.get("item_ref"), "item")
        error_code = result.get("error_code")
        if error_code is not None and (
            not isinstance(error_code, str)
            or not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", error_code)
        ):
            raise SocialWorkspaceValidationError("operation error_code is invalid")
        validate_action_status_response(result)
        return result

    def _operation_ledger_unknown(
        self, operation_ref: str, action: SocialAction
    ) -> Mapping[str, Any]:
        return dict(
            self._recordless_operation_validation(
                operation_ref,
                {
                    "platform": "telegram",
                    "operation_ref": operation_ref,
                    "action": action.value,
                    "status": "outcome_unknown",
                    "retry_safe": False,
                    "error_code": "operation_ledger_failed",
                },
            )
        )

    def _provider_rejected_result(
        self, operation_ref: str, action: SocialAction
    ) -> Mapping[str, Any]:
        return dict(
            self._recordless_operation_validation(
                operation_ref,
                {
                    "platform": "telegram",
                    "operation_ref": operation_ref,
                    "action": action.value,
                    "status": "failed",
                    "retry_safe": False,
                    "error_code": "provider_rejected",
                },
            )
        )

    async def execute(
        self,
        intent: SocialActionIntent,
        *,
        operation_ref: str,
    ) -> Mapping[str, Any]:
        if intent.platform is not SocialPlatform.TELEGRAM:
            raise SocialWorkspaceValidationError("Telegram adapter requires telegram platform")
        if not isinstance(operation_ref, str) or not re.fullmatch(
            r"op_[A-Za-z0-9_-]{24,160}", operation_ref
        ):
            raise SocialWorkspaceValidationError("operation_ref is invalid")
        action_digest = compute_action_digest(intent)
        replay = await self._claim_operation(operation_ref, action_digest)
        if replay is not None:
            return replay

        async def run(client: Any, lease: TelegramLease, attempt: _Attempt) -> Mapping[str, Any]:
            snapshot = await self._preflight(client, lease, intent)
            target, source, item = snapshot.target, snapshot.source, snapshot.item
            await self._fenced(lease)
            result: Any = None
            if intent.action in {SocialAction.SEND_MESSAGE, SocialAction.PUBLISH}:
                assert intent.content is not None
                result = await self._send_content(
                    client, target, intent.content, attempt=attempt
                )
            elif intent.action is SocialAction.SCHEDULE:
                assert intent.content is not None and intent.schedule_at is not None
                schedule = datetime.fromisoformat(intent.schedule_at.replace("Z", "+00:00"))
                result = await self._send_content(
                    client, target, intent.content, attempt=attempt, schedule=schedule
                )
            elif intent.action is SocialAction.COMMENT:
                assert intent.content is not None and item is not None and source is not None
                result = await self._send_content(
                    client,
                    source,
                    intent.content,
                    attempt=attempt,
                    comment_to=(
                        item.message_id
                        if source.kind is SocialTargetKind.CHANNEL
                        else None
                    ),
                    reply_to=(
                        item.message_id
                        if source.kind is not SocialTargetKind.CHANNEL
                        else None
                    ),
                )
                target = source
            elif intent.action is SocialAction.EDIT:
                assert intent.content is not None and item is not None and source is not None
                if intent.content.media:
                    raise SocialWorkspaceValidationError("media replacement is unsupported")
                compiled_entities = self._compile_entities(intent.content)
                attempt.provider_mutation_attempted = True
                result = await _await(
                    client.edit_message(
                        source.entity,
                        item.message_id,
                        intent.content.text,
                        formatting_entities=compiled_entities,
                        parse_mode=None,
                    )
                )
                target = source
            elif intent.action is SocialAction.DELETE:
                assert item is not None and source is not None
                attempt.provider_mutation_attempted = True
                await _await(client.delete_messages(source.entity, [item.message_id], revoke=True))
                target = source
            elif intent.action is SocialAction.FORWARD:
                assert item is not None and source is not None
                attempt.provider_mutation_attempted = True
                result = await _await(
                    client.forward_messages(
                        target.entity, [item.message_id], from_peer=source.entity
                    )
                )
            elif intent.action is SocialAction.REACTION:
                assert item is not None and source is not None and intent.reaction is not None
                reaction_request = self._types.request(
                    "reaction",
                    peer=source.entity,
                    message_id=item.message_id,
                    reaction=intent.reaction,
                )
                attempt.provider_mutation_attempted = True
                result = await self._call(
                    client,
                    lease,
                    reaction_request,
                )
                target = source
            elif intent.action is SocialAction.STORY:
                assert intent.content is not None
                assets = self._compile_media(intent.content)
                story_privacy = self._story_privacy(target)
                if len(assets) != 1 or story_privacy is None:
                    raise SocialWorkspaceValidationError(
                        "story requires one staged asset and closed privacy"
                    )
                provider_media = await self._provider_media(
                    client,
                    assets[0],
                    spoiler=intent.content.media[0].spoiler,
                    attempt=attempt,
                )
                story_request = self._types.request(
                    "send_story",
                    peer=target.entity,
                    media=provider_media,
                    privacy_rules=story_privacy,
                    caption=intent.content.text,
                    entities=self._compile_entities(intent.content),
                )
                attempt.provider_mutation_attempted = True
                result = await self._call(
                    client,
                    lease,
                    story_request,
                )
                story_id_parser = getattr(self._types, "story_id", None)
                if callable(story_id_parser):
                    story_id = story_id_parser(
                        result, random_id=getattr(story_request, "random_id", None)
                    )
                    observed_story = await self._story_by_id(
                        client, lease, target, story_id
                    )
                    if _provider_message_id(observed_story) != story_id:
                        raise TimeoutError("story read-back mismatch")
                    result = observed_story
            else:
                raise SocialWorkspaceValidationError("unsupported Telegram action")

            await self._fenced(lease)

            receipt: dict[str, Any] = {
                "platform": "telegram",
                "operation_ref": operation_ref,
                "action": intent.action.value,
                "status": "succeeded",
                "retry_safe": False,
                "target_ref": target.target_ref,
            }
            if intent.action is SocialAction.DELETE:
                return receipt
            if intent.action is SocialAction.REACTION:
                assert item is not None
                receipt["item_ref"] = item.item_ref
                return receipt
            message = result[0] if isinstance(result, Sequence) and result else result
            message_id = getattr(message, "id", None)
            document_send = (
                intent.action is SocialAction.SEND_MESSAGE
                and intent.content is not None
                and any(
                    attachment.role is MediaRole.DOCUMENT
                    for attachment in intent.content.media
                )
            )
            if document_send and (type(message_id) is not int or message_id <= 0):
                raise TimeoutError("document message id was not confirmed")
            if type(message_id) is int and message_id > 0:
                minted = self._mint_item_binding(
                    target_ref=target.target_ref,
                    message_id=message_id,
                    kind=(
                        SocialItemKind.STORY
                        if intent.action is SocialAction.STORY
                        else (
                            SocialItemKind.POST
                            if target.kind is SocialTargetKind.CHANNEL
                            else SocialItemKind.MESSAGE
                        )
                    ),
                    allowed_actions=None,
                )
                receipt["item_ref"] = minted.item_ref
                if intent.action is SocialAction.SEND_MESSAGE:
                    await self._fenced(lease)
                    observed = await _await(client.get_messages(target.entity, ids=message_id))
                    if (
                        type(getattr(observed, "id", None)) is not int
                        or getattr(observed, "id", None) != message_id
                        or not self._message_matches_target(observed, target)
                    ):
                        raise TimeoutError("read-back mismatch")
                    if document_send:
                        assets = self._compile_media(intent.content)
                        upload = assets[0].provider_media if len(assets) == 1 else None
                        if not isinstance(upload, TelegramVerifiedUpload):
                            raise TimeoutError("document read-back binding mismatch")
                        document, file_name, file_size = self._document_metadata(observed)
                        if (
                            document is None
                            or (file_name is not None and file_name != upload.display_name)
                            or (file_size is not None and file_size != upload.byte_length)
                        ):
                            raise TimeoutError("document read-back mismatch")
                    receipt["read_after_write"] = {
                        "verified": True,
                        "observed_item_ref": minted.item_ref,
                        "observed_at": datetime.now(timezone.utc).isoformat().replace(
                            "+00:00", "Z"
                        ),
                    }
            return receipt

        provider_completed = False
        try:
            result = await self._session(intent.action.value, run)
            provider_completed = True
            return await self._complete_operation(
                operation_ref, action_digest, result
            )
        except SocialWorkspaceValidationError:
            if provider_completed:
                return self._operation_ledger_unknown(operation_ref, intent.action)
            await self._release_operation(operation_ref, action_digest)
            raise
        except TelegramWorkspaceError as exc:
            if provider_completed and exc.code == "operation_ledger_failed":
                return self._operation_ledger_unknown(operation_ref, intent.action)
            if exc.code == "provider_rejected":
                rejected = self._provider_rejected_result(
                    operation_ref, intent.action
                )
                try:
                    return await self._complete_operation(
                        operation_ref, action_digest, rejected
                    )
                except (SocialWorkspaceValidationError, TelegramWorkspaceError):
                    return self._operation_ledger_unknown(
                        operation_ref, intent.action
                    )
            if exc.retry_safe:
                await self._release_operation(operation_ref, action_digest)
                raise
            if exc.code in {
                "outcome_unknown", "provider_cooldown", "provider_error", "lease_lost"
            } and not exc.retry_safe:
                result = {
                    "platform": "telegram",
                    "operation_ref": operation_ref,
                    "action": intent.action.value,
                    "status": "outcome_unknown",
                    "retry_safe": False,
                    "error_code": exc.code,
                }
                return await self._complete_operation(
                    operation_ref, action_digest, result
                )
            raise


__all__ = [
    "TelegramAssetBinding",
    "TelegramAssetReader",
    "TelegramGovernor",
    "TelegramItemBinding",
    "TelegramLease",
    "TelegramOpaqueRefStore",
    "TelegramOperationClaim",
    "TelegramTargetBinding",
    "TelegramVerifiedUpload",
    "TelegramWorkspaceAdapter",
    "TelegramWorkspaceError",
]
