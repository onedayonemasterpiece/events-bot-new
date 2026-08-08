"""Closed, role-scoped VK adapter for :mod:`private_events_mcp.social_workspace`.

The public surface accepts provider-neutral requests only.  Native VK identifiers
are held by an injected opaque-reference store and fixed API calls are made through
an injected, dedicated actor transport.  This module never imports ``main`` and has
no credential or generic VK API fallback.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence
from urllib.parse import urlsplit

from private_events_mcp.social_workspace import (
    ContentFeature,
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
    validate_opaque_ref,
)


VK_API_VERSION = "5.199"
_TRUST = "untrusted_external_data"
_MAX_RESULT_PAGE = 25
_MAX_EDITORIAL_TOTAL = 100
_MAX_TEXT = 4096
_SAFE_HANDLE = re.compile(r"^[A-Za-z0-9_.-]{2,128}$")
_NATIVE_HANDLE = re.compile(r"^(?:id|club|public|event)\d+$", re.IGNORECASE)


class VKWorkspaceError(RuntimeError):
    """Sanitized adapter error.  Provider payloads are never included."""

    def __init__(self, code: str) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", code):
            code = "provider_unavailable"
        self.code = code
        super().__init__(code)


class VKActor(str, Enum):
    PUBLIC_READER = "public_reader"
    DIALOG_READER = "dialog_reader"
    USER_MESSENGER = "user_messenger"
    COMMUNITY_EDITOR = "community_editor"
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


@dataclass(frozen=True, slots=True)
class _CallPolicy:
    actor: VKActor
    capability: str
    method: str
    required: frozenset[str]
    optional: frozenset[str] = frozenset()


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
    "wall_item": _policy(VKActor.PUBLIC_READER, "read_public", "wall.getById", ["posts"], ["extended"]),
    "wall_search": _policy(VKActor.PUBLIC_READER, "search_public", "wall.search", ["owner_id", "query", "count"], ["offset", "owners_only"]),
    "newsfeed_search": _policy(VKActor.DIALOG_READER, "search_newsfeed", "newsfeed.search", ["q", "count"], ["start_from"]),
    "wall_comments": _policy(VKActor.PUBLIC_READER, "read_public", "wall.getComments", ["owner_id", "post_id", "count"], ["offset", "extended", "sort"]),
    "wall_likes": _policy(VKActor.PUBLIC_READER, "read_public", "likes.getList", ["type", "owner_id", "item_id", "count"], ["extended"]),
    "dialog_history": _policy(VKActor.DIALOG_READER, "dialogs", "messages.getHistory", ["peer_id", "count"], ["offset", "start_message_id", "rev"]),
    "message_item": _policy(VKActor.DIALOG_READER, "dialogs", "messages.getById", ["message_ids"]),
    "conversations": _policy(VKActor.DIALOG_READER, "dialogs", "messages.getConversations", ["count"], ["offset", "filter"]),
    "stories": _policy(VKActor.STORY_READER, "story_read", "stories.get", ["owners"]),
    "story_stats": _policy(VKActor.ANALYTICS_READER, "analytics", "stories.getStats", ["owner_id", "story_id"]),
    "community_stats": _policy(VKActor.ANALYTICS_READER, "analytics", "stats.get", ["group_id"], ["date_from", "date_to", "intervals_count"]),
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
    "story_save": _policy(VKActor.STORY_EDITOR, "story_write", "stories.save", ["upload_results"], ["extended"]),
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
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not 0 < timeout_seconds <= 60:
            raise ValueError("timeout_seconds is invalid")
        self._transport = transport
        self._refs = refs
        self._governor = governor
        self._cooldown = cooldown
        self._sanitize_text = sanitize_text
        self._timeout = float(timeout_seconds)
        self._lock = asyncio.Lock()
        self._cursors: dict[str, int] = {}
        self._samples: dict[str, int] = {}
        self._operations: dict[str, dict[str, Any]] = {}

    def __repr__(self) -> str:
        return "<VKWorkspaceAdapter platform='vk' transport='role_scoped'>"

    async def _await(self, value: Any) -> Any:
        return await value if inspect.isawaitable(value) else value

    def _permitted(self, actor: VKActor, capability: str) -> bool:
        try:
            return self._transport.permits(actor, capability) is True
        except Exception:
            return False

    async def _call(self, operation: str, params: Mapping[str, Any]) -> Any:
        policy = _CALLS[operation]
        keys = frozenset(params)
        if not policy.required.issubset(keys) or not keys.issubset(policy.required | policy.optional):
            raise VKWorkspaceError("adapter_contract_error")
        if not self._permitted(policy.actor, policy.capability):
            raise VKWorkspaceError("actor_capability_denied")
        try:
            await self._await(self._cooldown.ensure_available(policy.actor))
        except Exception:
            raise VKWorkspaceError("cooldown_active") from None
        try:
            await self._await(self._governor.before_call(policy.actor, policy.capability))
        except Exception:
            raise VKWorkspaceError("rate_limited") from None
        outcome = "failed"
        try:
            async with self._lock:
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
                    await self._await(self._cooldown.record_captcha(policy.actor))
                    raise VKWorkspaceError("captcha_cooldown")
                raise VKWorkspaceError("provider_unavailable")
            outcome = "succeeded"
            await self._await(self._cooldown.record_success(policy.actor))
            return result
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except asyncio.TimeoutError:
            outcome = "outcome_unknown"
            raise VKWorkspaceError("outcome_unknown") from None
        except VKWorkspaceError:
            raise
        except Exception as exc:
            if getattr(exc, "code", None) == 14 or getattr(exc, "error_code", None) == 14:
                await self._await(self._cooldown.record_captcha(policy.actor))
                raise VKWorkspaceError("captcha_cooldown") from None
            raise VKWorkspaceError("provider_unavailable") from None
        finally:
            try:
                await self._await(self._governor.after_call(policy.actor, policy.capability, outcome))
            except Exception:
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
        except Exception:
            raise VKWorkspaceError("opaque_reference_failed") from None

    def _resolve_ref(self, kind: str, ref: str) -> Mapping[str, Any]:
        validate_opaque_ref(ref, kind)
        try:
            value = self._refs.resolve(kind, ref)
        except Exception:
            raise VKWorkspaceError("opaque_reference_failed") from None
        if not isinstance(value, Mapping):
            raise VKWorkspaceError("opaque_reference_failed")
        return value

    def _cursor(self, offset: int) -> str:
        token = "cur_" + hashlib.sha256(f"vk:{offset}:{len(self._cursors)}".encode()).hexdigest()[:28]
        self._cursors[token] = offset
        return token

    def _offset(self, cursor: str | None) -> int:
        if cursor is None:
            return 0
        if cursor not in self._cursors:
            raise VKWorkspaceError("cursor_invalid")
        return self._cursors[cursor]

    def _target_kind(self, native: Mapping[str, Any]) -> SocialTargetKind:
        try:
            return SocialTargetKind(str(native["kind"]))
        except Exception:
            raise VKWorkspaceError("opaque_reference_failed") from None

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
            reads.update({SocialReadOperation.LIST_ITEMS, SocialReadOperation.GET_ITEM})
        if self._permitted(VKActor.STORY_READER, "story_read"):
            reads.add(SocialReadOperation.LIST_STORIES)
        if self._permitted(VKActor.ANALYTICS_READER, "analytics"):
            reads.add(SocialReadOperation.GET_STATISTICS)
        if self._permitted(VKActor.PUBLIC_READER, "audience"):
            reads.add(SocialReadOperation.GET_AUDIENCE)
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
        if kind is SocialTargetKind.USER:
            actions &= {SocialAction.SEND_MESSAGE, SocialAction.FORWARD}
        elif kind in {SocialTargetKind.COMMUNITY, SocialTargetKind.GROUP, SocialTargetKind.CHANNEL}:
            actions.discard(SocialAction.SEND_MESSAGE)
        elif kind is SocialTargetKind.SELF:
            actions.clear()
        return {
            "platform": "vk",
            **({"target_ref": target_ref} if target_ref is not None else {}),
            "target_kinds": [kind.value] if kind else ["self", "user", "group", "community"],
            "read_operations": sorted(operation.value for operation in reads),
            "actions": sorted(action.value for action in actions),
            "content_features": sorted(feature.value for feature in {ContentFeature.RICH_TEXT, ContentFeature.LINKS, ContentFeature.IMAGE, ContentFeature.VIDEO, ContentFeature.DOCUMENT, ContentFeature.AUDIO, ContentFeature.ANIMATION}),
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

    def _metrics(self, item: Mapping[str, Any]) -> dict[str, int]:
        def count(field: str) -> int:
            value = item.get(field)
            if isinstance(value, Mapping):
                value = value.get("count")
            return max(0, value) if type(value) is int else 0
        return {"views": count("views"), "reactions": count("likes"), "comments": count("comments"), "shares": count("reposts")}

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
        return output

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

    async def _editorial(self, request: SocialReadRequest) -> Mapping[str, Any]:
        native = self._resolve_ref("target", request.target_ref or "")
        if self._target_kind(native) not in {SocialTargetKind.COMMUNITY, SocialTargetKind.GROUP, SocialTargetKind.CHANNEL}:
            raise VKWorkspaceError("target_kind_mismatch")
        group_id = _int(native.get("group_id"))
        owner_id = _int(native.get("owner_id"))
        if group_id is None or owner_id is None:
            raise VKWorkspaceError("opaque_reference_failed")
        page_size = min(request.page_size, _MAX_RESULT_PAGE)
        offset = self._offset(request.cursor)
        sample_ref = request.sample_ref
        if sample_ref is None:
            sample_ref = "smp_" + hashlib.sha256(f"vk:{request.target_ref}:{len(self._samples)}".encode()).hexdigest()[:32]
            self._samples[sample_ref] = 0
        if sample_ref not in self._samples:
            raise VKWorkspaceError("sample_state_invalid")
        cumulative = self._samples[sample_ref]
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
            if raw.get("marked_as_ads") in {1, True} or raw.get("is_pinned") in {1, True} or raw.get("copy_history"):
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
            item = self._public_item(raw, native={"kind": "post", "owner_id": owner_id, "post_id": post_id})
            item["text"] = item["text"][:768]
            item["caption"] = item["caption"][:256]
            selected.append(item)
        cumulative += len(selected)
        self._samples[sample_ref] = cumulative
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
            output["next_cursor"] = self._cursor(offset + count)
        return output

    async def read(self, request: SocialReadRequest) -> Mapping[str, Any]:
        if request.platform is not SocialPlatform.VK:
            raise SocialWorkspaceValidationError("VK request is required")
        if request.operation is SocialReadOperation.RESOLVE_TARGET:
            return await self.resolve(request)
        if request.operation is SocialReadOperation.SEARCH_TARGETS:
            return await self._discover(request)
        if request.operation is SocialReadOperation.EDITORIAL_SAMPLE:
            return await self._editorial(request)
        limit = min(request.limit, _MAX_RESULT_PAGE)
        offset = self._offset(request.cursor)

        if request.operation in {SocialReadOperation.LIST_ITEMS, SocialReadOperation.SEARCH_ITEMS}:
            if request.target_ref:
                native = self._resolve_ref("target", request.target_ref)
                kind = self._target_kind(native)
                if request.read_access is SocialReadAccess.DIALOGS or kind in {SocialTargetKind.CHAT, SocialTargetKind.SELF}:
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
                            output["next_cursor"] = self._cursor(offset + limit)
                        return output
                    peer_id = _int(native.get("peer_id"))
                    if peer_id is None:
                        raise VKWorkspaceError("opaque_reference_failed")
                    response = await self._call("dialog_history", {"peer_id": peer_id, "count": limit, "offset": offset, "rev": 0})
                    raws = _items(response)
                    results = [self._public_item(raw, native={"kind": "message", "peer_id": peer_id, "message_id": _int(raw.get("id"))}, kind=SocialItemKind.MESSAGE, target_ref=request.target_ref) for raw in raws if _int(raw.get("id")) is not None]
                else:
                    owner_id = _int(native.get("owner_id")) or _int(native.get("user_id"))
                    if owner_id is None:
                        raise VKWorkspaceError("opaque_reference_failed")
                    if request.operation is SocialReadOperation.SEARCH_ITEMS:
                        response = await self._call("wall_search", {"owner_id": owner_id, "query": request.query or "", "count": limit, "offset": offset, "owners_only": 1})
                    else:
                        response = await self._call("wall_feed", {"owner_id": owner_id, "count": limit, "offset": offset, "filter": "owner"})
                    raws = _items(response)
                    results = [self._public_item(raw, native={"kind": "post", "owner_id": owner_id, "post_id": _int(raw.get("id"))}, target_ref=request.target_ref) for raw in raws if _int(raw.get("id")) is not None]
            else:
                response = await self._call("newsfeed_search", {"q": request.query or "", "count": limit, **({"start_from": request.cursor} if request.cursor else {})})
                raws = _items(response)
                results = []
                for raw in raws:
                    owner_id, post_id = _int(raw.get("owner_id")), _int(raw.get("id"))
                    if owner_id is not None and post_id is not None:
                        results.append(self._public_item(raw, native={"kind": "post", "owner_id": owner_id, "post_id": post_id}))
            output: dict[str, Any] = {"results": results[:limit], "trust": _TRUST}
            if len(raws) == limit and request.target_ref:
                output["next_cursor"] = self._cursor(offset + limit)
            return output

        if request.operation is SocialReadOperation.GET_ITEM:
            native = self._resolve_ref("item", request.item_ref or "")
            if native.get("kind") == "message":
                response = await self._call("message_item", {"message_ids": str(native.get("message_id"))})
                kind = SocialItemKind.MESSAGE
            else:
                response = await self._call("wall_item", {"posts": f"{native.get('owner_id')}_{native.get('post_id')}", "extended": 0})
                kind = SocialItemKind.POST
            raws = _items(response)
            if not raws:
                raise VKWorkspaceError("item_not_found")
            return {"item": self._public_item(raws[0], native=native, kind=kind), "trust": _TRUST}

        if request.operation is SocialReadOperation.LIST_COMMENTS:
            native = self._resolve_ref("item", request.item_ref or "")
            response = await self._call("wall_comments", {"owner_id": native.get("owner_id"), "post_id": native.get("post_id"), "count": limit, "offset": offset, "extended": 0, "sort": "asc"})
            results = [self._public_item(raw, native={"kind": "comment", "owner_id": native.get("owner_id"), "post_id": native.get("post_id"), "comment_id": _int(raw.get("id"))}, kind=SocialItemKind.COMMENT) for raw in _items(response) if _int(raw.get("id")) is not None]
            return {"root_item_ref": request.item_ref, "items": results, "trust": _TRUST}

        if request.operation is SocialReadOperation.LIST_REACTIONS:
            native = self._resolve_ref("item", request.item_ref or "")
            response = await self._call("wall_likes", {"type": "post", "owner_id": native.get("owner_id"), "item_id": native.get("post_id"), "count": 1, "extended": 0})
            count = response.get("count", 0) if isinstance(response, Mapping) else 0
            return {"item_ref": request.item_ref, "reactions": [{"reaction": "like", "count": max(0, count) if type(count) is int else 0}], "trust": _TRUST}

        if request.operation is SocialReadOperation.LIST_STORIES:
            native = self._resolve_ref("target", request.target_ref or "")
            owner = _int(native.get("owner_id")) or _int(native.get("user_id"))
            response = await self._call("stories", {"owners": str(owner)})
            results = []
            for raw in _items(response):
                story_id = _int(raw.get("id"))
                if story_id is not None:
                    results.append(self._public_item(raw, native={"kind": "story", "owner_id": owner, "story_id": story_id}, kind=SocialItemKind.STORY, target_ref=request.target_ref))
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
                    raw_stats = response if isinstance(response, Mapping) else {}
                    likes = raw_stats.get("likes")
                    if isinstance(likes, Mapping):
                        likes = likes.get("count")
                    metrics = {
                        "views": max(0, _int(raw_stats.get("views")) or 0),
                        "reactions": max(0, _int(likes) or 0),
                        "comments": max(0, _int(raw_stats.get("replies")) or 0),
                        "shares": max(0, _int(raw_stats.get("shares")) or 0),
                    }
                else:
                    response = await self._call("wall_item", {"posts": f"{native.get('owner_id')}_{native.get('post_id')}", "extended": 0})
                    raws = _items(response)
                    metrics = self._metrics(raws[0] if raws else {})
                return {"item_ref": request.item_ref, "period_from": start, "period_to": end, "basic_metrics": metrics, "trust": _TRUST}
            native = self._resolve_ref("target", request.target_ref or "")
            params: dict[str, Any] = {"group_id": native.get("group_id")}
            if request.date_from: params["date_from"] = request.date_from
            if request.date_to: params["date_to"] = request.date_to
            response = await self._call("community_stats", params)
            metrics = {"views": 0, "reactions": 0, "comments": 0, "shares": 0}
            for row in _items(response):
                for key, source in (("views", "views"), ("reactions", "likes"), ("comments", "comments"), ("shares", "reposts")):
                    value = row.get(source)
                    if isinstance(value, Mapping): value = value.get("count")
                    if type(value) is int and value > 0: metrics[key] += value
            return {"target_ref": request.target_ref, "period_from": start, "period_to": end, "basic_metrics": metrics, "trust": _TRUST}
        raise SocialWorkspaceValidationError("unsupported VK read operation")

    def _content(self, intent: SocialActionIntent) -> tuple[str, str | None]:
        content = intent.content
        if content is None:
            return "", None
        attachments = []
        for media in content.media:
            binding = self._resolve_ref("asset", media.asset_ref)
            attachment = binding.get("attachment")
            role = binding.get("role")
            if not isinstance(attachment, str) or not re.fullmatch(r"(?:photo|video|doc|audio|audio_message|graffiti|album)-?\d+_\d+(?:_[A-Za-z0-9]+)?", attachment):
                raise VKWorkspaceError("asset_not_ready")
            if role != media.role.value:
                raise VKWorkspaceError("asset_role_mismatch")
            attachments.append(attachment)
        return content.text, ",".join(attachments) if attachments else None

    def _operation_ref(self, intent: SocialActionIntent) -> str:
        digest = hashlib.sha256((intent.idempotency_key + "\0" + intent.action.value).encode()).hexdigest()
        return "op_" + digest[:32]

    async def execute(self, intent: SocialActionIntent) -> Mapping[str, Any]:
        if intent.platform is not SocialPlatform.VK:
            raise SocialWorkspaceValidationError("VK action is required")
        operation_ref = self._operation_ref(intent)
        if operation_ref in self._operations:
            return dict(self._operations[operation_ref])
        if intent.expected_revision is not None:
            result = {"platform": "vk", "operation_ref": operation_ref, "action": intent.action.value, "status": "failed", "retry_safe": False, "error_code": "expected_revision_unsupported"}
            self._operations[operation_ref] = result
            return dict(result)
        text, attachments = self._content(intent)
        random_id = int(hashlib.sha256(intent.idempotency_key.encode()).hexdigest()[:8], 16) & 0x7FFFFFFF or 1
        guid = hashlib.sha256(intent.idempotency_key.encode()).hexdigest()
        target = self._resolve_ref("target", intent.target_ref) if intent.target_ref else None
        item = self._resolve_ref("item", intent.item_ref) if intent.item_ref else None
        destination = self._resolve_ref("target", intent.destination_target_ref) if intent.destination_target_ref else None
        new_item_native: Mapping[str, Any] | None = None
        try:
            if intent.action is SocialAction.SEND_MESSAGE:
                if target is None or self._target_kind(target) is not SocialTargetKind.USER:
                    raise VKWorkspaceError("exact_user_required")
                peer_id = target.get("peer_id")
                params = {"peer_id": peer_id, "message": text, "random_id": random_id, **({"attachment": attachments} if attachments else {})}
                response = await self._call("send_message", params)
                message_id = response.get("message_id") if isinstance(response, Mapping) else response
                if type(message_id) is not int:
                    raise VKWorkspaceError("provider_unavailable")
                readback = await self._call("message_item", {"message_ids": str(message_id)})
                observed = next((raw for raw in _items(readback) if _int(raw.get("id")) == message_id and _int(raw.get("peer_id")) == peer_id), None)
                if observed is None:
                    raise VKWorkspaceError("read_after_write_failed")
                new_item_native = {"kind": "message", "peer_id": peer_id, "message_id": message_id}
            elif intent.action in {SocialAction.PUBLISH, SocialAction.SCHEDULE}:
                if target is None or self._target_kind(target) not in {SocialTargetKind.COMMUNITY, SocialTargetKind.GROUP, SocialTargetKind.CHANNEL}:
                    raise VKWorkspaceError("community_required")
                params = {"owner_id": target.get("owner_id"), "from_group": 1, "message": text, "guid": guid, "signed": 0, **({"attachments": attachments} if attachments else {})}
                if intent.action is SocialAction.SCHEDULE:
                    assert intent.schedule_at is not None
                    params["publish_date"] = int(datetime.fromisoformat(intent.schedule_at.replace("Z", "+00:00")).timestamp())
                response = await self._call("wall_post", params)
                post_id = response.get("post_id") if isinstance(response, Mapping) else None
                if type(post_id) is not int: raise VKWorkspaceError("provider_unavailable")
                new_item_native = {"kind": "post", "owner_id": target.get("owner_id"), "post_id": post_id}
            elif intent.action is SocialAction.COMMENT:
                if item is None: raise VKWorkspaceError("item_required")
                params = {"owner_id": item.get("owner_id"), "post_id": item.get("post_id"), "message": text, "guid": guid, "from_group": 1, **({"attachments": attachments} if attachments else {})}
                response = await self._call("wall_comment", params)
                comment_id = response.get("comment_id") if isinstance(response, Mapping) else None
                if type(comment_id) is not int: raise VKWorkspaceError("provider_unavailable")
                new_item_native = {"kind": "comment", "owner_id": item.get("owner_id"), "post_id": item.get("post_id"), "comment_id": comment_id}
            elif intent.action is SocialAction.REACTION:
                if item is None or intent.reaction not in {"like", "unlike"}: raise VKWorkspaceError("reaction_unsupported")
                await self._call("like_add" if intent.reaction == "like" else "like_delete", {"type": "post", "owner_id": item.get("owner_id"), "item_id": item.get("post_id")})
            elif intent.action is SocialAction.EDIT:
                if item is None: raise VKWorkspaceError("item_required")
                if item.get("kind") == "message":
                    await self._call("message_edit", {"peer_id": item.get("peer_id"), "message": text, "message_id": item.get("message_id"), **({"attachment": attachments} if attachments else {})})
                else:
                    await self._call("wall_edit", {"owner_id": item.get("owner_id"), "post_id": item.get("post_id"), "message": text, **({"attachments": attachments} if attachments else {})})
            elif intent.action is SocialAction.DELETE:
                if item is None: raise VKWorkspaceError("item_required")
                if item.get("kind") == "message":
                    await self._call("message_delete", {"message_ids": str(item.get("message_id")), "delete_for_all": 1})
                else:
                    await self._call("wall_delete", {"owner_id": item.get("owner_id"), "post_id": item.get("post_id")})
            elif intent.action is SocialAction.FORWARD:
                if item is None or destination is None: raise VKWorkspaceError("destination_required")
                if item.get("kind") == "message":
                    forward = json.dumps({"peer_id": item.get("peer_id"), "message_ids": [item.get("message_id")]}, separators=(",", ":"))
                    await self._call("message_forward", {"peer_id": destination.get("peer_id"), "message": "", "random_id": random_id, "forward": forward})
                else:
                    params = {"object": f"wall{item.get('owner_id')}_{item.get('post_id')}"}
                    if destination.get("group_id") is not None: params["group_id"] = destination.get("group_id")
                    await self._call("wall_repost", params)
            elif intent.action is SocialAction.STORY:
                if not intent.content or len(intent.content.media) != 1:
                    raise VKWorkspaceError("story_requires_one_asset")
                binding = self._resolve_ref("asset", intent.content.media[0].asset_ref)
                upload_result = binding.get("story_upload_result")
                if not isinstance(upload_result, str) or not re.fullmatch(r"[A-Za-z0-9._~-]{8,2048}", upload_result):
                    raise VKWorkspaceError("asset_not_ready")
                response = await self._call("story_save", {"upload_results": upload_result, "extended": 0})
                stories = _items(response)
                if stories and _int(stories[0].get("id")) is not None:
                    new_item_native = {"kind": "story", "owner_id": target.get("owner_id") if target else None, "story_id": stories[0]["id"]}
            else:
                raise SocialWorkspaceValidationError("unsupported VK action")
        except VKWorkspaceError as exc:
            status = SocialActionStatus.OUTCOME_UNKNOWN if exc.code == "outcome_unknown" else SocialActionStatus.FAILED
            result = {"platform": "vk", "operation_ref": operation_ref, "action": intent.action.value, "status": status.value, "retry_safe": False, "error_code": exc.code}
            self._operations[operation_ref] = result
            return dict(result)
        result: dict[str, Any] = {"platform": "vk", "operation_ref": operation_ref, "action": intent.action.value, "status": "succeeded", "retry_safe": False}
        if intent.target_ref: result["target_ref"] = intent.target_ref
        if new_item_native is not None:
            item_ref = self._mint("item", new_item_native)
            result["item_ref"] = item_ref
            if intent.action is SocialAction.SEND_MESSAGE:
                observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                result["read_after_write"] = {"verified": True, "observed_item_ref": item_ref, "observed_at": observed_at}
        elif intent.item_ref:
            result["item_ref"] = intent.item_ref
        self._operations[operation_ref] = result
        return dict(result)

    async def reconcile(self, operation_ref: str) -> Mapping[str, Any]:
        if not isinstance(operation_ref, str) or not re.fullmatch(r"op_[A-Za-z0-9_-]{24,160}", operation_ref):
            raise SocialWorkspaceValidationError("operation_ref is invalid")
        result = self._operations.get(operation_ref)
        if result is None:
            raise VKWorkspaceError("operation_not_found")
        return dict(result)


__all__ = [
    "VKActor",
    "VKActorTransport",
    "VKCallGovernor",
    "VKCooldownHook",
    "VK_FIXED_METHOD_ALLOWLIST",
    "VK_API_VERSION",
    "VK_OPERATION_ACTORS",
    "VKOpaqueRefStore",
    "VKWorkspaceAdapter",
    "VKWorkspaceError",
]
