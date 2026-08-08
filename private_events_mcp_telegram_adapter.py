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
import inspect
import re
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
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
    SocialReadOperation,
    SocialReadRequest,
    SocialTargetKind,
    SocialWorkspaceValidationError,
    TargetLocatorKind,
    validate_action_status_response,
    validate_opaque_ref,
)


_TRUST = "untrusted_external_data"
_MAX_PAGE = 25
_MAX_SAMPLE = 100
_MAX_GLOBAL_SCAN = 100
_MIN_TELETHON_VERSION = (1, 34)
_MAX_TELETHON_MAJOR = 1


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
            "TelegramWorkspaceError(code={!r}, retry_safe={!r}, "
            "retry_after_seconds={!r})"
        ).format(self.code, self.retry_safe, self.retry_after_seconds)


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


@dataclass(frozen=True, slots=True)
class TelegramAssetBinding:
    """Server-side staged asset binding. Native media is deliberately private."""

    asset_ref: str
    role: MediaRole
    provider_media: Any = field(repr=False)


@dataclass(frozen=True, slots=True)
class TelegramOperationBinding:
    operation_ref: str
    result: Mapping[str, Any]


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
    ) -> TelegramItemBinding: ...

    def mint_read_asset(self, *, target_ref: str, media: Any, role: MediaRole) -> str: ...

    def mint_cursor(self, *, family: str, state: Mapping[str, Any]) -> str: ...

    def resolve_cursor(self, *, family: str, cursor: str) -> Mapping[str, Any]: ...

    def mint_operation(self, *, action: SocialAction, idempotency_key: str) -> str: ...

    def record_operation(
        self, *, operation_ref: str, result: Mapping[str, Any]
    ) -> None | Awaitable[None]: ...

    def resolve_operation(
        self, operation_ref: str
    ) -> TelegramOperationBinding | Awaitable[TelegramOperationBinding]: ...


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
                (functions.stories, "SendStoryRequest"),
                (types, "MessageEntityCustomEmoji"),
                (types, "MessageEntityBlockquote"),
                (types, "InputMessageEntityMentionName"),
                (types, "ReactionEmoji"),
                (types, "InputMessagesFilterEmpty"),
                (types, "InputPeerEmpty"),
            )
            if any(not hasattr(owner, name) for owner, name in required):
                raise RuntimeError("required Telethon feature missing")
            signatures = {
                TelegramClient.send_message: {
                    "formatting_entities", "schedule", "comment_to"
                },
                TelegramClient.send_file: {
                    "formatting_entities", "schedule", "comment_to"
                },
                TelegramClient.edit_message: {"formatting_entities"},
            }
            if any(
                not parameters.issubset(inspect.signature(method).parameters)
                for method, parameters in signatures.items()
            ):
                raise RuntimeError("required Telethon client feature missing")
        except Exception:
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
        if name == "send_story":
            return functions.stories.SendStoryRequest(
                peer=values["peer"],
                media=values["media"],
                privacy_rules=list(values["privacy_rules"]),
                caption=values["caption"],
                entities=values["entities"],
                random_id=secrets.randbits(63),
                media_areas=[],
            )
        raise TelegramWorkspaceError("unsupported_provider_feature")


@dataclass(slots=True)
class _Attempt:
    provider_mutation_attempted: bool = False


@dataclass(frozen=True, slots=True)
class _PreflightSnapshot:
    target: TelegramTargetBinding
    source: TelegramTargetBinding | None
    item: TelegramItemBinding | None


class TelegramWorkspaceAdapter:
    """Fixed high-level Telegram implementation for Social Workspace."""

    platform = "telegram"

    def __init__(
        self,
        *,
        client_factory: ClientFactory,
        refs: TelegramOpaqueRefStore,
        governor: TelegramGovernor,
        telethon_types: Any | None = None,
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
            "mint_operation",
            "record_operation",
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
        except Exception:
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
                if callable(getattr(client, "is_user_authorized", None)):
                    if not await _await(client.is_user_authorized()):
                        raise TelegramWorkspaceError("provider_unauthorized", retry_safe=False)
                await self._fenced(lease)
                return await asyncio.wait_for(body(client, lease, attempt), self._timeout)
            except asyncio.CancelledError:
                raise
            except SocialWorkspaceValidationError:
                raise
            except TelegramWorkspaceError:
                raise
            except (asyncio.TimeoutError, TimeoutError):
                raise TelegramWorkspaceError(
                    "outcome_unknown" if attempt.provider_mutation_attempted else "provider_timeout",
                    retry_safe=not attempt.provider_mutation_attempted,
                ) from None
            except Exception as exc:
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
                except Exception:
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
        return binding

    def _item(self, item_ref: str) -> TelegramItemBinding:
        validate_opaque_ref(item_ref, "item")
        binding = self._refs.resolve_item(item_ref)
        if not isinstance(binding, TelegramItemBinding) or binding.item_ref != item_ref:
            raise SocialWorkspaceValidationError("opaque item binding mismatch")
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
                    if value not in {entity_id, marked_channel_id}:
                        raise SocialWorkspaceValidationError("target is not an exact match")
            if request.expected_target_kinds and binding.kind not in request.expected_target_kinds:
                raise SocialWorkspaceValidationError("resolved target kind mismatch")
            return self._target_preview(binding)

        return await self._session("resolve_target", run)

    async def _live_actions(self, client: Any, binding: TelegramTargetBinding) -> set[SocialAction]:
        configured = (
            set(binding.allowed_actions) if binding.allowed_actions is not None else None
        )
        if binding.kind is SocialTargetKind.SELF:
            actions = {
                SocialAction.SEND_MESSAGE,
                SocialAction.EDIT,
                SocialAction.DELETE,
                SocialAction.FORWARD,
                SocialAction.REACTION,
                SocialAction.SCHEDULE,
            }
            if binding.story_privacy is not None:
                actions.add(SocialAction.STORY)
            return actions if configured is None else actions & configured
        if binding.kind is SocialTargetKind.USER:
            actions = {
                SocialAction.SEND_MESSAGE,
                SocialAction.EDIT,
                SocialAction.DELETE,
                SocialAction.FORWARD,
                SocialAction.REACTION,
                SocialAction.SCHEDULE,
            }
            return actions if configured is None else actions & configured
        permissions = None
        if callable(getattr(client, "get_permissions", None)):
            permissions = await _await(client.get_permissions(binding.entity, "me"))
        creator = (
            bool(getattr(permissions, "is_creator", False))
            if permissions is not None
            else bool(getattr(binding.entity, "creator", False))
        )
        rights = getattr(binding.entity, "admin_rights", None)
        def live_right(name: str) -> bool:
            if permissions is not None:
                return bool(getattr(permissions, name, False))
            return bool(getattr(rights, name, False))

        is_admin = creator or live_right("is_admin")
        can_send = creator or live_right("send_messages")
        can_post = creator or live_right("post_messages")
        can_edit = creator or live_right("edit_messages")
        can_delete = creator or live_right("delete_messages")
        can_story = creator or live_right("post_stories")
        managed = is_admin or can_post or can_edit or can_delete or can_story
        actions: set[SocialAction] = set()
        if binding.kind is SocialTargetKind.CHANNEL and can_post:
            actions.update({SocialAction.PUBLISH, SocialAction.SCHEDULE})
        if binding.kind is SocialTargetKind.CHANNEL and managed:
            actions.update(
                {SocialAction.FORWARD, SocialAction.REACTION, SocialAction.COMMENT}
            )
        if binding.kind is SocialTargetKind.GROUP and can_send:
            actions.update(
                {
                    SocialAction.PUBLISH,
                    SocialAction.COMMENT,
                    SocialAction.SCHEDULE,
                    SocialAction.FORWARD,
                    SocialAction.REACTION,
                }
            )
        if can_edit:
            actions.add(SocialAction.EDIT)
        if can_delete:
            actions.add(SocialAction.DELETE)
        if binding.story_privacy and can_story:
            actions.add(SocialAction.STORY)
        return actions if configured is None else actions & configured

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
        actions = await self._live_actions(client, binding) if binding else set()
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
            "content_features": sorted(value.value for value in ContentFeature),
            "max_text_length": 4096,
            "max_media_items": 10,
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

    def _item_payload(
        self,
        message: Any,
        target: TelegramTargetBinding,
        *,
        kind: SocialItemKind | None = None,
    ) -> dict[str, Any]:
        message_id = _provider_message_id(message)
        item = self._refs.mint_item(
            target_ref=target.target_ref,
            message_id=message_id,
            allowed_actions=None,
        )
        if not isinstance(item, TelegramItemBinding):
            raise SocialWorkspaceValidationError("item minter returned invalid binding")
        validate_opaque_ref(item.item_ref, "item")
        text = _safe_text(getattr(message, "message", None), 4096)
        selected_kind = kind or (
            SocialItemKind.POST
            if target.kind is SocialTargetKind.CHANNEL
            else SocialItemKind.MESSAGE
        )
        payload: dict[str, Any] = {
            "item_ref": item.item_ref,
            "target_ref": target.target_ref,
            "kind": selected_kind.value,
            "published_at": _utc(getattr(message, "date", None)),
            "text": text,
            "caption": "",
            "basic_metrics": _message_metrics(message),
            "trust": _TRUST,
        }
        media = getattr(message, "media", None)
        if media is not None:
            media_ref = self._refs.mint_read_asset(
                target_ref=target.target_ref,
                media=media,
                role=MediaRole.DOCUMENT,
            )
            validate_opaque_ref(media_ref, "asset")
            payload["media"] = [media_ref]
        return payload

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
        iterator = client.iter_messages(
            target.entity,
            limit=limit + 1,
            offset_id=offset_id,
            search=request.query if request.operation is SocialReadOperation.SEARCH_ITEMS else None,
        )
        messages = await self._iterate(iterator)
        selected = messages[:limit]
        results = [self._item_payload(message, target) for message in selected]
        state = (
            {
                "offset_id": _provider_message_id(selected[-1]),
                "target_ref": target.target_ref,
                "query": request.query or "",
            }
            if selected
            else None
        )
        return self._page(
            results,
            family=family,
            more=len(messages) > limit,
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
            story_id = _provider_message_id(story)
            binding = self._refs.mint_item(
                target_ref=target.target_ref, message_id=story_id, allowed_actions=None
            )
            results.append(
                {
                    "item_ref": binding.item_ref,
                    "target_ref": target.target_ref,
                    "kind": "story",
                    "published_at": _utc(getattr(story, "date", None)),
                    "text": _safe_text(getattr(story, "caption", None), 4096),
                    "caption": "",
                    "basic_metrics": {"views": _int(getattr(story, "views", None))},
                    "trust": _TRUST,
                }
            )
        return self._page(
            results,
            family="stories",
            more=offset + limit < len(stories),
            state={"offset": offset + limit, "target_ref": target.target_ref},
            binding=cursor_binding,
        )

    async def _statistics(self, client: Any, request: SocialReadRequest) -> Mapping[str, Any]:
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
                message = await _await(client.get_messages(target.entity, ids=item.message_id))
                return {"item": self._item_payload(message, target), "trust": _TRUST}
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
                return await self._statistics(client, request)
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
        self, client: Any, intent: SocialActionIntent
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
        actions = await self._live_actions(client, checked_target)
        if intent.action not in actions:
            raise SocialWorkspaceValidationError("capability denied: unsupported_action")
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
        }:
            raise SocialWorkspaceValidationError("send_message requires Saved or user DM")
        if intent.action is SocialAction.PUBLISH and target.kind not in {
            SocialTargetKind.CHANNEL,
            SocialTargetKind.GROUP,
        }:
            raise SocialWorkspaceValidationError("publish requires channel or group")
        if intent.content:
            if len(intent.content.text) > 4096 or len(intent.content.media) > 10:
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
        if assets:
            media_values = []
            for attachment, asset in zip(content.media, assets):
                compile_media = getattr(self._types, "media", None)
                if attachment.spoiler and not callable(compile_media):
                    raise SocialWorkspaceValidationError("media spoiler is unsupported")
                media_values.append(
                    compile_media(asset.provider_media, spoiler=attachment.spoiler)
                    if callable(compile_media)
                    else asset.provider_media
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

    async def _record_operation(
        self, operation_ref: str, result: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        self._recordless_operation_validation(operation_ref, result)
        try:
            await _await(
                self._refs.record_operation(
                    operation_ref=operation_ref, result=dict(result)
                )
            )
        except Exception:
            raise TelegramWorkspaceError(
                "operation_ledger_failed", retry_safe=False
            ) from None
        return result

    async def reconcile(self, operation_ref: str) -> Mapping[str, Any]:
        if not isinstance(operation_ref, str) or not re.fullmatch(
            r"op_[A-Za-z0-9_-]{24,160}", operation_ref
        ):
            raise SocialWorkspaceValidationError("operation_ref is invalid")
        try:
            binding = await _await(self._refs.resolve_operation(operation_ref))
        except Exception:
            raise TelegramWorkspaceError("operation_not_found", retry_safe=False) from None
        if (
            not isinstance(binding, TelegramOperationBinding)
            or binding.operation_ref != operation_ref
            or binding.result.get("operation_ref") != operation_ref
        ):
            raise SocialWorkspaceValidationError("operation ledger binding mismatch")
        return dict(self._recordless_operation_validation(operation_ref, binding.result))

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

    async def execute(
        self,
        intent: SocialActionIntent,
        *,
        operation_ref: str | None = None,
    ) -> Mapping[str, Any]:
        if intent.platform is not SocialPlatform.TELEGRAM:
            raise SocialWorkspaceValidationError("Telegram adapter requires telegram platform")
        operation_ref = operation_ref or self._refs.mint_operation(
            action=intent.action, idempotency_key=intent.idempotency_key
        )
        if not isinstance(operation_ref, str) or not re.fullmatch(
            r"op_[A-Za-z0-9_-]{24,160}", operation_ref
        ):
            raise SocialWorkspaceValidationError("operation minter returned invalid ref")

        async def run(client: Any, lease: TelegramLease, attempt: _Attempt) -> Mapping[str, Any]:
            snapshot = await self._preflight(client, intent)
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
                if len(assets) != 1 or target.story_privacy is None:
                    raise SocialWorkspaceValidationError(
                        "story requires one staged asset and explicit stored privacy"
                    )
                compile_story_media = getattr(self._types, "media", None)
                if intent.content.media[0].spoiler and not callable(compile_story_media):
                    raise SocialWorkspaceValidationError("story media spoiler is unsupported")
                story_request = self._types.request(
                    "send_story",
                    peer=target.entity,
                    media=(
                        compile_story_media(
                            assets[0].provider_media,
                            spoiler=intent.content.media[0].spoiler,
                        )
                        if callable(compile_story_media)
                        else assets[0].provider_media
                    ),
                    privacy_rules=target.story_privacy,
                    caption=intent.content.text,
                    entities=self._compile_entities(intent.content),
                )
                attempt.provider_mutation_attempted = True
                result = await self._call(
                    client,
                    lease,
                    story_request,
                )
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
            if type(message_id) is int and message_id > 0:
                minted = self._refs.mint_item(
                    target_ref=target.target_ref, message_id=message_id, allowed_actions=None
                )
                receipt["item_ref"] = minted.item_ref
                if intent.action is SocialAction.SEND_MESSAGE:
                    await self._fenced(lease)
                    observed = await _await(client.get_messages(target.entity, ids=message_id))
                    if _provider_message_id(observed) != message_id:
                        raise TimeoutError("read-back mismatch")
                    receipt["read_after_write"] = {
                        "verified": True,
                        "observed_item_ref": minted.item_ref,
                        "observed_at": datetime.now(timezone.utc).isoformat().replace(
                            "+00:00", "Z"
                        ),
                    }
            return receipt

        try:
            result = await self._session(intent.action.value, run)
            return await self._record_operation(operation_ref, result)
        except TelegramWorkspaceError as exc:
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
                return await self._record_operation(operation_ref, result)
            raise


__all__ = [
    "TelegramAssetBinding",
    "TelegramGovernor",
    "TelegramLease",
    "TelegramOpaqueRefStore",
    "TelegramOperationBinding",
    "TelegramTargetBinding",
    "TelegramItemBinding",
    "TelegramWorkspaceAdapter",
    "TelegramWorkspaceError",
]
