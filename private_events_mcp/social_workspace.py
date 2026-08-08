"""Provider-neutral contract for a broad, capability-gated social workspace.

This module deliberately contains no Telegram/VK transport code.  Provider adapters
exchange opaque references with this boundary and translate the validated operations
to their native APIs behind explicit consent, policy, and capability gates.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit


class SocialWorkspaceValidationError(ValueError):
    """A request does not conform to the public Social Workspace contract."""


class _StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class SocialPlatform(_StringEnum):
    TELEGRAM = "telegram"
    VK = "vk"


class SocialReadOperation(_StringEnum):
    RESOLVE_TARGET = "resolve_target"
    SEARCH_TARGETS = "search_targets"
    LIST_ITEMS = "list_items"
    SEARCH_ITEMS = "search_items"
    GET_ITEM = "get_item"
    LIST_COMMENTS = "list_comments"
    LIST_REACTIONS = "list_reactions"
    LIST_STORIES = "list_stories"
    GET_STATISTICS = "get_statistics"
    EDITORIAL_SAMPLE = "editorial_sample"


class SocialAction(_StringEnum):
    """Closed action set.  Native provider method names are not part of the API."""

    SEND_MESSAGE = "send_message"
    PUBLISH = "publish"
    EDIT = "edit"
    DELETE = "delete"
    FORWARD = "forward"
    REACTION = "reaction"
    COMMENT = "comment"
    SCHEDULE = "schedule"
    STORY = "story"


class SocialTargetKind(_StringEnum):
    SELF = "self"
    USER = "user"
    CHAT = "chat"
    CHANNEL = "channel"
    GROUP = "group"
    COMMUNITY = "community"


class TargetLocatorKind(_StringEnum):
    USERNAME = "username"
    PROFILE_LINK = "profile_link"
    PROVIDER_ID = "provider_id"


class SocialReadPurpose(_StringEnum):
    EDITORIAL_ANALYSIS = "editorial_analysis"


class SocialItemKind(_StringEnum):
    MESSAGE = "message"
    POST = "post"
    COMMENT = "comment"
    STORY = "story"


class ContentFeature(_StringEnum):
    RICH_TEXT = "rich_text"
    LINKS = "links"
    CUSTOM_EMOJI = "custom_emoji"
    IMAGE = "image"
    VIDEO = "video"
    DOCUMENT = "document"
    AUDIO = "audio"
    ANIMATION = "animation"


class RichEntityKind(_StringEnum):
    BOLD = "bold"
    ITALIC = "italic"
    UNDERLINE = "underline"
    STRIKETHROUGH = "strikethrough"
    SPOILER = "spoiler"
    CODE = "code"
    PRE = "pre"
    BLOCKQUOTE = "blockquote"
    LINK = "link"
    MENTION = "mention"
    CUSTOM_EMOJI = "custom_emoji"


class MediaRole(_StringEnum):
    IMAGE = "image"
    VIDEO = "video"
    DOCUMENT = "document"
    AUDIO = "audio"
    ANIMATION = "animation"


class SocialActionStatus(_StringEnum):
    PREPARED = "prepared"
    COMMITTED = "committed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


_REF_PREFIXES = {"target": "tgt", "item": "itm", "asset": "ast"}
_OPAQUE_REF_RE = re.compile(r"^(tgt|itm|ast)_[A-Za-z0-9_-]{16,160}$")
_PREPARATION_REF_RE = re.compile(r"^prep_[A-Za-z0-9_-]{24,160}$")
_OPERATION_REF_RE = re.compile(r"^op_[A-Za-z0-9_-]{24,160}$")
_SAMPLE_REF_RE = re.compile(r"^smp_[A-Za-z0-9_-]{24,160}$")
_CURSOR_RE = re.compile(r"^[A-Za-z0-9_-]{1,512}$")
_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9._~-]{8,128}$")
_REACTION_RE = re.compile(r"^\S(?:.{0,30}\S)?$", re.DOTALL)

_SCOPE_SUFFIXES = frozenset({"read", "publish", "manage", "stories", "analytics"})
SOCIAL_WORKSPACE_SCOPES = frozenset(
    f"{platform.value}:{suffix}"
    for platform in SocialPlatform
    for suffix in _SCOPE_SUFFIXES
)

_ACTION_SCOPE_SUFFIX: Mapping[SocialAction, str] = {
    SocialAction.SEND_MESSAGE: "publish",
    SocialAction.PUBLISH: "publish",
    SocialAction.EDIT: "manage",
    SocialAction.DELETE: "manage",
    SocialAction.FORWARD: "publish",
    SocialAction.REACTION: "publish",
    SocialAction.COMMENT: "publish",
    SocialAction.SCHEDULE: "publish",
    SocialAction.STORY: "stories",
}
_READ_SCOPE_SUFFIX: Mapping[SocialReadOperation, str] = {
    operation: "analytics" if operation is SocialReadOperation.GET_STATISTICS else "read"
    for operation in SocialReadOperation
}


def _enum(value: Any, enum_type: type[_StringEnum], field: str) -> _StringEnum:
    if not isinstance(value, str):
        raise SocialWorkspaceValidationError(f"{field} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise SocialWorkspaceValidationError(f"unsupported {field}") from exc


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SocialWorkspaceValidationError(f"{field} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise SocialWorkspaceValidationError(f"{field} keys must be strings")
    return value


def _only_fields(value: Mapping[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise SocialWorkspaceValidationError(
            f"unsupported {field} field(s): {', '.join(unknown)}"
        )


def _optional_text(
    value: Any,
    field: str,
    *,
    maximum: int,
    required: bool = False,
    preserve_space: bool = False,
) -> str | None:
    if value is None:
        if required:
            raise SocialWorkspaceValidationError(f"{field} is required")
        return None
    if not isinstance(value, str):
        raise SocialWorkspaceValidationError(f"{field} must be a string")
    clean = value if preserve_space else value.strip()
    if required and not clean:
        raise SocialWorkspaceValidationError(f"{field} is required")
    if "\x00" in clean or len(clean) > maximum:
        raise SocialWorkspaceValidationError(f"{field} is invalid")
    return clean


def validate_opaque_ref(value: Any, kind: str) -> str:
    """Validate a non-provider-native reference and return its normalized token."""

    prefix = _REF_PREFIXES.get(kind)
    if prefix is None:
        raise ValueError("kind must be target, item, or asset")
    if not isinstance(value, str) or not _OPAQUE_REF_RE.fullmatch(value):
        raise SocialWorkspaceValidationError(f"{kind}_ref is invalid")
    if not value.startswith(prefix + "_"):
        raise SocialWorkspaceValidationError(f"{kind}_ref has the wrong kind")
    return value


def _optional_ref(value: Any, kind: str) -> str | None:
    return None if value is None else validate_opaque_ref(value, kind)


def _validate_rfc3339(value: Any, field: str) -> str:
    clean = _optional_text(value, field, maximum=40, required=True)
    assert clean is not None
    try:
        parsed = datetime.fromisoformat(clean[:-1] + "+00:00" if clean.endswith("Z") else clean)
    except ValueError as exc:
        raise SocialWorkspaceValidationError(f"{field} must be RFC 3339") from exc
    if parsed.tzinfo is None:
        raise SocialWorkspaceValidationError(f"{field} must include a timezone")
    return clean


def required_scope_for_read(
    platform: SocialPlatform | str, operation: SocialReadOperation | str
) -> frozenset[str]:
    normalized_platform = (
        platform
        if isinstance(platform, SocialPlatform)
        else _enum(platform, SocialPlatform, "platform")
    )
    normalized_operation = (
        operation
        if isinstance(operation, SocialReadOperation)
        else _enum(operation, SocialReadOperation, "operation")
    )
    return frozenset(
        {f"{normalized_platform.value}:{_READ_SCOPE_SUFFIX[normalized_operation]}"}
    )


def required_scope_for_action(
    platform: SocialPlatform | str, action: SocialAction | str
) -> frozenset[str]:
    normalized_platform = (
        platform
        if isinstance(platform, SocialPlatform)
        else _enum(platform, SocialPlatform, "platform")
    )
    normalized_action = (
        action if isinstance(action, SocialAction) else _enum(action, SocialAction, "action")
    )
    return frozenset(
        {f"{normalized_platform.value}:{_ACTION_SCOPE_SUFFIX[normalized_action]}"}
    )


@dataclass(frozen=True, slots=True)
class TargetLocator:
    kind: TargetLocatorKind
    value: str


@dataclass(frozen=True, slots=True)
class SocialReadRequest:
    platform: SocialPlatform
    operation: SocialReadOperation
    target_ref: str | None
    item_ref: str | None
    query: str | None
    cursor: str | None
    limit: int
    item_kinds: tuple[SocialItemKind, ...]
    target_locator: TargetLocator | None
    purpose: SocialReadPurpose | None
    sample_ref: str | None
    date_from: str | None
    date_to: str | None
    page_size: int
    total_limit: int

    @property
    def required_scopes(self) -> frozenset[str]:
        return required_scope_for_read(self.platform, self.operation)


@dataclass(frozen=True, slots=True)
class RichEntity:
    kind: RichEntityKind
    offset: int
    length: int
    link_target: str | None = None
    mention_target_ref: str | None = None
    custom_emoji_asset_ref: str | None = None


@dataclass(frozen=True, slots=True)
class MediaAttachment:
    asset_ref: str
    role: MediaRole
    alt_text: str | None = None
    spoiler: bool = False


@dataclass(frozen=True, slots=True)
class RichContent:
    text: str
    entities: tuple[RichEntity, ...]
    media: tuple[MediaAttachment, ...]

    @property
    def features(self) -> frozenset[ContentFeature]:
        features: set[ContentFeature] = set()
        if self.entities:
            features.add(ContentFeature.RICH_TEXT)
        if any(entity.kind is RichEntityKind.LINK for entity in self.entities):
            features.add(ContentFeature.LINKS)
        if any(entity.kind is RichEntityKind.CUSTOM_EMOJI for entity in self.entities):
            features.add(ContentFeature.CUSTOM_EMOJI)
        for attachment in self.media:
            features.add(ContentFeature(attachment.role.value))
        return frozenset(features)


@dataclass(frozen=True, slots=True)
class SocialActionIntent:
    platform: SocialPlatform
    action: SocialAction
    idempotency_key: str
    target_ref: str | None
    item_ref: str | None
    destination_target_ref: str | None
    content: RichContent | None
    reaction: str | None
    schedule_at: str | None
    expected_revision: str | None

    @property
    def required_scopes(self) -> frozenset[str]:
        return required_scope_for_action(self.platform, self.action)


@dataclass(frozen=True, slots=True)
class SocialCapabilities:
    platform: SocialPlatform
    target_ref: str | None
    target_kinds: frozenset[SocialTargetKind]
    read_operations: frozenset[SocialReadOperation]
    actions: frozenset[SocialAction]
    content_features: frozenset[ContentFeature]
    max_text_length: int
    max_media_items: int


@dataclass(frozen=True, slots=True)
class GateDecision:
    allowed: bool
    code: str
    reason: str = ""

    def __post_init__(self) -> None:
        if type(self.allowed) is not bool:
            raise TypeError("allowed must be boolean")
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", self.code):
            raise ValueError("gate decision code is invalid")
        if len(self.reason) > 300:
            raise ValueError("gate decision reason is too long")


ActionGateHook = Callable[[SocialActionIntent], GateDecision]
CapabilityHook = Callable[[SocialActionIntent], SocialCapabilities]
ReadGateHook = Callable[[SocialReadRequest], GateDecision]


def enforce_action_gates(
    intent: SocialActionIntent,
    *,
    consent_hook: ActionGateHook,
    policy_hook: ActionGateHook,
    capability_hook: CapabilityHook,
) -> None:
    """Fail closed unless consent, policy, and provider capabilities all allow intent."""

    for gate_name, hook in (("consent", consent_hook), ("policy", policy_hook)):
        decision = hook(intent)
        if not isinstance(decision, GateDecision):
            raise SocialWorkspaceValidationError(f"{gate_name} gate returned no decision")
        if not decision.allowed:
            raise SocialWorkspaceValidationError(f"{gate_name} denied: {decision.code}")
    capabilities = capability_hook(intent)
    if not isinstance(capabilities, SocialCapabilities):
        raise SocialWorkspaceValidationError("capability gate returned no capabilities")
    if capabilities.platform is not intent.platform or intent.action not in capabilities.actions:
        raise SocialWorkspaceValidationError("capability denied: unsupported_action")
    if capabilities.target_ref is not None:
        relevant_target = intent.destination_target_ref or intent.target_ref
        if relevant_target != capabilities.target_ref:
            raise SocialWorkspaceValidationError("capability denied: target_mismatch")
    if intent.content is not None:
        if len(intent.content.text) > capabilities.max_text_length:
            raise SocialWorkspaceValidationError("capability denied: text_limit")
        if len(intent.content.media) > capabilities.max_media_items:
            raise SocialWorkspaceValidationError("capability denied: media_limit")
        unsupported = intent.content.features - capabilities.content_features
        if unsupported:
            raise SocialWorkspaceValidationError("capability denied: content_feature")


def enforce_editorial_sample_gates(
    request: SocialReadRequest,
    *,
    consent_hook: ReadGateHook,
    purpose_hook: ReadGateHook,
) -> None:
    """Require explicit consent and purpose approval for bounded editorial analysis."""

    if request.operation is not SocialReadOperation.EDITORIAL_SAMPLE:
        raise SocialWorkspaceValidationError("editorial sample gate requires editorial_sample")
    for gate_name, hook in (("consent", consent_hook), ("purpose", purpose_hook)):
        decision = hook(request)
        if not isinstance(decision, GateDecision):
            raise SocialWorkspaceValidationError(f"{gate_name} gate returned no decision")
        if not decision.allowed:
            raise SocialWorkspaceValidationError(f"{gate_name} denied: {decision.code}")


def _validate_target_locator(value: Any, platform: SocialPlatform) -> TargetLocator:
    data = _object(value, "target_locator")
    _only_fields(data, {"kind", "value"}, "target_locator")
    kind = _enum(data.get("kind"), TargetLocatorKind, "target locator kind")
    raw = _optional_text(data.get("value"), "target locator value", maximum=512, required=True)
    assert raw is not None
    if kind is TargetLocatorKind.USERNAME:
        normalized = raw[1:] if raw.startswith("@") else raw
        if not re.fullmatch(r"[A-Za-z0-9_.-]{2,128}", normalized):
            raise SocialWorkspaceValidationError("target username is invalid")
        raw = normalized
    elif kind is TargetLocatorKind.PROVIDER_ID:
        if not re.fullmatch(r"-?[A-Za-z0-9_.]{1,128}", raw):
            raise SocialWorkspaceValidationError("provider target id is invalid")
    else:
        parsed = urlsplit(raw)
        allowed_hosts = {
            SocialPlatform.TELEGRAM: {"t.me", "telegram.me"},
            SocialPlatform.VK: {"vk.com", "www.vk.com"},
        }[platform]
        if (
            parsed.scheme != "https"
            or parsed.hostname not in allowed_hosts
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
            or not parsed.path.strip("/")
            or parsed.query
            or parsed.fragment
        ):
            raise SocialWorkspaceValidationError("profile link is not canonical for platform")
    return TargetLocator(kind=kind, value=raw)


def _validate_iso_date(value: Any, field: str) -> str:
    clean = _optional_text(value, field, maximum=10, required=True)
    assert clean is not None
    try:
        datetime.strptime(clean, "%Y-%m-%d")
    except ValueError as exc:
        raise SocialWorkspaceValidationError(f"{field} must be YYYY-MM-DD") from exc
    return clean


def validate_read_request(payload: Mapping[str, Any]) -> SocialReadRequest:
    data = _object(payload, "request")
    _only_fields(
        data,
        {
            "platform", "operation", "target_ref", "item_ref", "query", "cursor", "limit",
            "item_kinds", "target_locator", "purpose", "sample_ref", "date_from", "date_to",
            "page_size", "total_limit",
        },
        "request",
    )
    platform = _enum(data.get("platform"), SocialPlatform, "platform")
    operation = _enum(data.get("operation"), SocialReadOperation, "operation")
    target_ref = _optional_ref(data.get("target_ref"), "target")
    item_ref = _optional_ref(data.get("item_ref"), "item")
    query = _optional_text(data.get("query"), "query", maximum=500)
    target_locator = (
        _validate_target_locator(data["target_locator"], platform)
        if "target_locator" in data
        else None
    )
    cursor = _optional_text(data.get("cursor"), "cursor", maximum=512)
    if cursor is not None and not _CURSOR_RE.fullmatch(cursor):
        raise SocialWorkspaceValidationError("cursor is invalid")
    raw_limit = data.get("limit", 25)
    if type(raw_limit) is not int or not 1 <= raw_limit <= 100:
        raise SocialWorkspaceValidationError("limit must be an integer from 1 to 100")
    raw_kinds = data.get("item_kinds", [])
    if not isinstance(raw_kinds, Sequence) or isinstance(raw_kinds, (str, bytes)):
        raise SocialWorkspaceValidationError("item_kinds must be an array")
    kinds: list[SocialItemKind] = []
    for value in raw_kinds:
        kind = _enum(value, SocialItemKind, "item_kinds")
        if kind not in kinds:
            kinds.append(kind)

    purpose = (
        _enum(data.get("purpose"), SocialReadPurpose, "purpose")
        if "purpose" in data
        else None
    )
    sample_ref = data.get("sample_ref")
    if sample_ref is not None and (
        not isinstance(sample_ref, str) or not _SAMPLE_REF_RE.fullmatch(sample_ref)
    ):
        raise SocialWorkspaceValidationError("sample_ref is invalid")
    date_from = _validate_iso_date(data["date_from"], "date_from") if "date_from" in data else None
    date_to = _validate_iso_date(data["date_to"], "date_to") if "date_to" in data else None
    if date_from is not None and date_to is not None and date_from > date_to:
        raise SocialWorkspaceValidationError("date range is reversed")
    page_size = data.get("page_size", 25)
    total_limit = data.get("total_limit", 100)
    if type(page_size) is not int or not 1 <= page_size <= 25:
        raise SocialWorkspaceValidationError("page_size must be an integer from 1 to 25")
    if type(total_limit) is not int or not 1 <= total_limit <= 100:
        raise SocialWorkspaceValidationError("total_limit must be an integer from 1 to 100")

    if operation is SocialReadOperation.RESOLVE_TARGET:
        if target_locator is None:
            raise SocialWorkspaceValidationError("target_locator is required for exact resolution")
    elif operation is SocialReadOperation.SEARCH_TARGETS:
        if not query:
            raise SocialWorkspaceValidationError("query is required for target discovery")
    elif operation in {SocialReadOperation.LIST_ITEMS, SocialReadOperation.LIST_STORIES}:
        if target_ref is None:
            raise SocialWorkspaceValidationError("target_ref is required")
    elif operation in {
        SocialReadOperation.GET_ITEM,
        SocialReadOperation.LIST_COMMENTS,
        SocialReadOperation.LIST_REACTIONS,
    }:
        if item_ref is None:
            raise SocialWorkspaceValidationError("item_ref is required")
    elif operation is SocialReadOperation.SEARCH_ITEMS and not (query or target_ref):
        raise SocialWorkspaceValidationError("query or target_ref is required")
    elif operation is SocialReadOperation.GET_STATISTICS and (target_ref is None) == (item_ref is None):
        raise SocialWorkspaceValidationError("statistics requires exactly one target_ref or item_ref")
    elif operation is SocialReadOperation.EDITORIAL_SAMPLE:
        if target_ref is None:
            raise SocialWorkspaceValidationError("target_ref is required for editorial sample")
        if purpose is not SocialReadPurpose.EDITORIAL_ANALYSIS:
            raise SocialWorkspaceValidationError("editorial_analysis purpose is required")
        forbidden = {"item_ref", "query", "limit", "item_kinds", "target_locator"} & set(data)
        if forbidden:
            raise SocialWorkspaceValidationError(
                "editorial sample does not allow: " + ", ".join(sorted(forbidden))
            )

    return SocialReadRequest(
        platform=platform,
        operation=operation,
        target_ref=target_ref,
        item_ref=item_ref,
        query=query,
        cursor=cursor,
        limit=raw_limit,
        item_kinds=tuple(kinds),
        target_locator=target_locator,
        purpose=purpose,
        sample_ref=sample_ref,
        date_from=date_from,
        date_to=date_to,
        page_size=page_size,
        total_limit=total_limit,
    )


def _validate_link_target(value: Any) -> str:
    clean = _optional_text(value, "link_target", maximum=2048, required=True)
    assert clean is not None
    parsed = urlsplit(clean)
    if parsed.scheme not in {"http", "https", "mailto"}:
        raise SocialWorkspaceValidationError("link_target scheme is not allowed")
    if parsed.scheme in {"http", "https"} and (
        not parsed.hostname or parsed.username is not None or parsed.password is not None
    ):
        raise SocialWorkspaceValidationError("link_target is invalid")
    return clean


def _validate_content(value: Any) -> RichContent:
    data = _object(value, "content")
    _only_fields(data, {"text", "entities", "media"}, "content")
    text = _optional_text(data.get("text", ""), "text", maximum=32768, preserve_space=True)
    assert text is not None
    raw_entities = data.get("entities", [])
    raw_media = data.get("media", [])
    if not isinstance(raw_entities, list) or len(raw_entities) > 256:
        raise SocialWorkspaceValidationError("entities must contain at most 256 items")
    if not isinstance(raw_media, list) or len(raw_media) > 10:
        raise SocialWorkspaceValidationError("media must contain at most 10 items")
    entities: list[RichEntity] = []
    for index, raw in enumerate(raw_entities):
        entity = _object(raw, f"entities[{index}]")
        _only_fields(
            entity,
            {"kind", "offset", "length", "link_target", "mention_target_ref", "custom_emoji_asset_ref"},
            f"entities[{index}]",
        )
        kind = _enum(entity.get("kind"), RichEntityKind, "entity kind")
        offset = entity.get("offset")
        length = entity.get("length")
        if type(offset) is not int or type(length) is not int or offset < 0 or length < 1:
            raise SocialWorkspaceValidationError("entity range is invalid")
        if offset + length > len(text):
            raise SocialWorkspaceValidationError("entity range exceeds text")
        link_target = (
            _validate_link_target(entity.get("link_target"))
            if entity.get("link_target") is not None
            else None
        )
        mention_ref = _optional_ref(entity.get("mention_target_ref"), "target")
        emoji_ref = _optional_ref(entity.get("custom_emoji_asset_ref"), "asset")
        if kind is RichEntityKind.LINK:
            if link_target is None or mention_ref is not None or emoji_ref is not None:
                raise SocialWorkspaceValidationError("link entity fields are invalid")
        elif kind is RichEntityKind.MENTION:
            if mention_ref is None or link_target is not None or emoji_ref is not None:
                raise SocialWorkspaceValidationError("mention entity fields are invalid")
        elif kind is RichEntityKind.CUSTOM_EMOJI:
            if emoji_ref is None or link_target is not None or mention_ref is not None:
                raise SocialWorkspaceValidationError("custom emoji entity fields are invalid")
        elif any(value is not None for value in (link_target, mention_ref, emoji_ref)):
            raise SocialWorkspaceValidationError("formatting entity has unsupported metadata")
        entities.append(RichEntity(kind, offset, length, link_target, mention_ref, emoji_ref))
    media: list[MediaAttachment] = []
    for index, raw in enumerate(raw_media):
        attachment = _object(raw, f"media[{index}]")
        _only_fields(attachment, {"asset_ref", "role", "alt_text", "spoiler"}, f"media[{index}]")
        asset_ref = validate_opaque_ref(attachment.get("asset_ref"), "asset")
        role = _enum(attachment.get("role"), MediaRole, "media role")
        alt_text = _optional_text(attachment.get("alt_text"), "alt_text", maximum=1000)
        spoiler = attachment.get("spoiler", False)
        if type(spoiler) is not bool:
            raise SocialWorkspaceValidationError("spoiler must be boolean")
        media.append(MediaAttachment(asset_ref, role, alt_text, spoiler))
    if not text.strip() and not media:
        raise SocialWorkspaceValidationError("content must include text or media")
    return RichContent(text=text, entities=tuple(entities), media=tuple(media))


def validate_prepare_request(payload: Mapping[str, Any]) -> SocialActionIntent:
    data = _object(payload, "request")
    _only_fields(
        data,
        {
            "platform", "action", "idempotency_key", "target_ref", "item_ref",
            "destination_target_ref", "content", "reaction", "schedule_at", "expected_revision",
        },
        "request",
    )
    platform = _enum(data.get("platform"), SocialPlatform, "platform")
    action = _enum(data.get("action"), SocialAction, "action")
    idempotency_key = _optional_text(
        data.get("idempotency_key"), "idempotency_key", maximum=128, required=True
    )
    assert idempotency_key is not None
    if not _IDEMPOTENCY_RE.fullmatch(idempotency_key):
        raise SocialWorkspaceValidationError("idempotency_key is invalid")
    target_ref = _optional_ref(data.get("target_ref"), "target")
    item_ref = _optional_ref(data.get("item_ref"), "item")
    destination_ref = _optional_ref(data.get("destination_target_ref"), "target")
    content = _validate_content(data["content"]) if "content" in data else None
    reaction = _optional_text(data.get("reaction"), "reaction", maximum=32)
    if reaction is not None and not _REACTION_RE.fullmatch(reaction):
        raise SocialWorkspaceValidationError("reaction is invalid")
    schedule_at = (
        _validate_rfc3339(data.get("schedule_at"), "schedule_at")
        if data.get("schedule_at") is not None
        else None
    )
    expected_revision = _optional_text(
        data.get("expected_revision"), "expected_revision", maximum=160
    )

    required_content = {
        SocialAction.SEND_MESSAGE, SocialAction.PUBLISH, SocialAction.EDIT, SocialAction.COMMENT,
        SocialAction.SCHEDULE, SocialAction.STORY,
    }
    if action in required_content and content is None:
        raise SocialWorkspaceValidationError("content is required for this action")
    if action in {SocialAction.SEND_MESSAGE, SocialAction.PUBLISH, SocialAction.SCHEDULE, SocialAction.STORY}:
        if target_ref is None:
            raise SocialWorkspaceValidationError("target_ref is required for this action")
    if action in {SocialAction.EDIT, SocialAction.DELETE, SocialAction.REACTION, SocialAction.COMMENT, SocialAction.FORWARD}:
        if item_ref is None:
            raise SocialWorkspaceValidationError("item_ref is required for this action")
    if action is SocialAction.FORWARD and destination_ref is None:
        raise SocialWorkspaceValidationError("destination_target_ref is required for forward")
    if action is SocialAction.REACTION and reaction is None:
        raise SocialWorkspaceValidationError("reaction is required for reaction")
    if action is SocialAction.SCHEDULE and schedule_at is None:
        raise SocialWorkspaceValidationError("schedule_at is required for schedule")
    if action is SocialAction.STORY and content is not None and not content.media:
        raise SocialWorkspaceValidationError("story content requires media")

    allowed_presence: Mapping[SocialAction, set[str]] = {
        SocialAction.SEND_MESSAGE: {"target_ref", "content"},
        SocialAction.PUBLISH: {"target_ref", "content"},
        SocialAction.EDIT: {"item_ref", "content", "expected_revision"},
        SocialAction.DELETE: {"item_ref", "expected_revision"},
        SocialAction.FORWARD: {"item_ref", "destination_target_ref"},
        SocialAction.REACTION: {"item_ref", "reaction"},
        SocialAction.COMMENT: {"item_ref", "content"},
        SocialAction.SCHEDULE: {"target_ref", "content", "schedule_at"},
        SocialAction.STORY: {"target_ref", "content"},
    }
    populated = {
        name
        for name, value in {
            "target_ref": target_ref,
            "item_ref": item_ref,
            "destination_target_ref": destination_ref,
            "content": content,
            "reaction": reaction,
            "schedule_at": schedule_at,
            "expected_revision": expected_revision,
        }.items()
        if value is not None
    }
    unsupported = populated - allowed_presence[action]
    if unsupported:
        raise SocialWorkspaceValidationError(
            f"field(s) not valid for {action.value}: {', '.join(sorted(unsupported))}"
        )
    return SocialActionIntent(
        platform=platform,
        action=action,
        idempotency_key=idempotency_key,
        target_ref=target_ref,
        item_ref=item_ref,
        destination_target_ref=destination_ref,
        content=content,
        reaction=reaction,
        schedule_at=schedule_at,
        expected_revision=expected_revision,
    )


def validate_capabilities(payload: Mapping[str, Any]) -> SocialCapabilities:
    data = _object(payload, "capabilities")
    _only_fields(
        data,
        {
            "platform", "target_ref", "target_kinds", "read_operations", "actions",
            "content_features", "max_text_length", "max_media_items",
        },
        "capabilities",
    )
    required = {
        "platform", "target_kinds", "read_operations", "actions", "content_features",
        "max_text_length", "max_media_items",
    }
    if missing := sorted(required - set(data)):
        raise SocialWorkspaceValidationError("missing capability field(s): " + ", ".join(missing))

    def enum_set(field: str, enum_type: type[_StringEnum]) -> frozenset[Any]:
        raw = data[field]
        if not isinstance(raw, list) or len(raw) != len(set(raw)):
            raise SocialWorkspaceValidationError(f"{field} must be a unique array")
        return frozenset(_enum(value, enum_type, field) for value in raw)

    max_text = data["max_text_length"]
    max_media = data["max_media_items"]
    if type(max_text) is not int or not 0 <= max_text <= 100000:
        raise SocialWorkspaceValidationError("max_text_length is invalid")
    if type(max_media) is not int or not 0 <= max_media <= 100:
        raise SocialWorkspaceValidationError("max_media_items is invalid")
    return SocialCapabilities(
        platform=_enum(data["platform"], SocialPlatform, "platform"),
        target_ref=_optional_ref(data.get("target_ref"), "target"),
        target_kinds=enum_set("target_kinds", SocialTargetKind),
        read_operations=enum_set("read_operations", SocialReadOperation),
        actions=enum_set("actions", SocialAction),
        content_features=enum_set("content_features", ContentFeature),
        max_text_length=max_text,
        max_media_items=max_media,
    )


def validate_commit_request(payload: Mapping[str, Any]) -> tuple[str, bool]:
    data = _object(payload, "request")
    _only_fields(data, {"preparation_ref", "confirm"}, "request")
    preparation_ref = data.get("preparation_ref")
    if not isinstance(preparation_ref, str) or not _PREPARATION_REF_RE.fullmatch(preparation_ref):
        raise SocialWorkspaceValidationError("preparation_ref is invalid")
    if data.get("confirm") is not True:
        raise SocialWorkspaceValidationError("confirm must be true")
    return preparation_ref, True


def validate_status_request(payload: Mapping[str, Any]) -> tuple[str, str]:
    data = _object(payload, "request")
    _only_fields(data, {"preparation_ref", "operation_ref"}, "request")
    preparation_ref = data.get("preparation_ref")
    operation_ref = data.get("operation_ref")
    if (preparation_ref is None) == (operation_ref is None):
        raise SocialWorkspaceValidationError("provide exactly one status reference")
    if preparation_ref is not None:
        if not isinstance(preparation_ref, str) or not _PREPARATION_REF_RE.fullmatch(preparation_ref):
            raise SocialWorkspaceValidationError("preparation_ref is invalid")
        return "preparation", preparation_ref
    if not isinstance(operation_ref, str) or not _OPERATION_REF_RE.fullmatch(operation_ref):
        raise SocialWorkspaceValidationError("operation_ref is invalid")
    return "operation", operation_ref


def validate_send_message_receipt(payload: Mapping[str, Any]) -> tuple[str, str]:
    """Validate the mandatory read-after-write proof for an exact-person DM."""

    data = _object(payload, "receipt")
    _only_fields(
        data,
        {"platform", "action", "status", "operation_ref", "target_ref", "item_ref", "read_after_write"},
        "receipt",
    )
    if _enum(data.get("action"), SocialAction, "action") is not SocialAction.SEND_MESSAGE:
        raise SocialWorkspaceValidationError("receipt is not for send_message")
    if _enum(data.get("status"), SocialActionStatus, "status") is not SocialActionStatus.SUCCEEDED:
        raise SocialWorkspaceValidationError("send_message has not succeeded")
    _enum(data.get("platform"), SocialPlatform, "platform")
    operation_ref = data.get("operation_ref")
    if not isinstance(operation_ref, str) or not _OPERATION_REF_RE.fullmatch(operation_ref):
        raise SocialWorkspaceValidationError("operation_ref is invalid")
    validate_opaque_ref(data.get("target_ref"), "target")
    item_ref = validate_opaque_ref(data.get("item_ref"), "item")
    proof = _object(data.get("read_after_write"), "read_after_write")
    _only_fields(proof, {"verified", "observed_item_ref", "observed_at"}, "read_after_write")
    if proof.get("verified") is not True:
        raise SocialWorkspaceValidationError("read-after-write is not verified")
    observed_item_ref = validate_opaque_ref(proof.get("observed_item_ref"), "item")
    if observed_item_ref != item_ref:
        raise SocialWorkspaceValidationError("read-after-write item mismatch")
    _validate_rfc3339(proof.get("observed_at"), "observed_at")
    return operation_ref, item_ref


def _enum_values(enum_type: type[_StringEnum]) -> list[str]:
    return [item.value for item in enum_type]


_DEFS: dict[str, Any] = {
    "target_ref": {"type": "string", "pattern": r"^tgt_[A-Za-z0-9_-]{16,160}$"},
    "item_ref": {"type": "string", "pattern": r"^itm_[A-Za-z0-9_-]{16,160}$"},
    "asset_ref": {"type": "string", "pattern": r"^ast_[A-Za-z0-9_-]{16,160}$"},
    "target_locator": {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "value"],
        "properties": {
            "kind": {"type": "string", "enum": _enum_values(TargetLocatorKind)},
            "value": {"type": "string", "minLength": 1, "maxLength": 512},
        },
    },
    "basic_metrics": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "followers": {"type": "integer", "minimum": 0},
            "members": {"type": "integer", "minimum": 0},
            "views": {"type": "integer", "minimum": 0},
            "reactions": {"type": "integer", "minimum": 0},
            "comments": {"type": "integer", "minimum": 0},
            "shares": {"type": "integer", "minimum": 0},
        },
    },
    "entity": {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "offset", "length"],
        "properties": {
            "kind": {"type": "string", "enum": _enum_values(RichEntityKind)},
            "offset": {"type": "integer", "minimum": 0},
            "length": {"type": "integer", "minimum": 1},
            "link_target": {"type": "string", "minLength": 1, "maxLength": 2048},
            "mention_target_ref": {"$ref": "#/$defs/target_ref"},
            "custom_emoji_asset_ref": {"$ref": "#/$defs/asset_ref"},
        },
    },
    "media": {
        "type": "object",
        "additionalProperties": False,
        "required": ["asset_ref", "role"],
        "properties": {
            "asset_ref": {"$ref": "#/$defs/asset_ref"},
            "role": {"type": "string", "enum": _enum_values(MediaRole)},
            "alt_text": {"type": "string", "maxLength": 1000},
            "spoiler": {"type": "boolean"},
        },
    },
    "content": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "text": {"type": "string", "maxLength": 32768},
            "entities": {"type": "array", "maxItems": 256, "items": {"$ref": "#/$defs/entity"}},
            "media": {"type": "array", "maxItems": 10, "items": {"$ref": "#/$defs/media"}},
        },
        "anyOf": [
            {"required": ["text"]},
            {"required": ["media"], "properties": {"media": {"minItems": 1}}},
        ],
    },
}


SOCIAL_WORKSPACE_READ_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["platform", "operation"],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "operation": {"type": "string", "enum": _enum_values(SocialReadOperation)},
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "query": {"type": "string", "minLength": 1, "maxLength": 500},
        "cursor": {"type": "string", "pattern": r"^[A-Za-z0-9_-]{1,512}$"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        "item_kinds": {
            "type": "array", "uniqueItems": True,
            "items": {"type": "string", "enum": _enum_values(SocialItemKind)},
        },
        "target_locator": {"$ref": "#/$defs/target_locator"},
        "purpose": {"type": "string", "enum": _enum_values(SocialReadPurpose)},
        "sample_ref": {"type": "string", "pattern": r"^smp_[A-Za-z0-9_-]{24,160}$"},
        "date_from": {"type": "string", "format": "date"},
        "date_to": {"type": "string", "format": "date"},
        "page_size": {"type": "integer", "minimum": 1, "maximum": 25},
        "total_limit": {"type": "integer", "minimum": 1, "maximum": 100},
    },
}

SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["platform", "target_ref", "kind", "display_name", "is_exact_match", "trust"],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "kind": {"type": "string", "enum": _enum_values(SocialTargetKind)},
        "display_name": {"type": "string", "minLength": 1, "maxLength": 512},
        "canonical_handle": {"type": "string", "minLength": 1, "maxLength": 128},
        "profile_link": {"type": "string", "minLength": 1, "maxLength": 512},
        "description": {"type": "string", "maxLength": 2048},
        "is_exact_match": {"const": True},
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": [
        "sample_ref", "target", "items", "sampled_count", "total_limit",
        "storage_disposition", "trust",
    ],
    "properties": {
        "sample_ref": {"type": "string", "pattern": r"^smp_[A-Za-z0-9_-]{24,160}$"},
        "target": {
            "type": "object",
            "additionalProperties": False,
            "required": ["target_ref", "kind", "title", "basic_metrics"],
            "properties": {
                "target_ref": {"$ref": "#/$defs/target_ref"},
                "kind": {"type": "string", "enum": _enum_values(SocialTargetKind)},
                "title": {"type": "string", "minLength": 1, "maxLength": 256},
                "about": {"type": "string", "maxLength": 1024},
                "description": {"type": "string", "maxLength": 1024},
                "basic_metrics": {"$ref": "#/$defs/basic_metrics"},
            },
        },
        "items": {
            "type": "array",
            "maxItems": 25,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["item_ref", "published_at", "text", "caption", "basic_metrics"],
                "properties": {
                    "item_ref": {"$ref": "#/$defs/item_ref"},
                    "published_at": {"type": "string", "format": "date-time"},
                    "text": {"type": "string", "maxLength": 768},
                    "caption": {"type": "string", "maxLength": 256},
                    "basic_metrics": {"$ref": "#/$defs/basic_metrics"},
                },
            },
        },
        "sampled_count": {"type": "integer", "minimum": 0, "maximum": 100},
        "total_limit": {"type": "integer", "minimum": 1, "maximum": 100},
        "next_cursor": {"type": "string", "pattern": r"^[A-Za-z0-9_-]{1,512}$"},
        "storage_disposition": {"const": "ephemeral_no_index"},
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_PREPARE_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["platform", "action", "idempotency_key"],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "action": {"type": "string", "enum": _enum_values(SocialAction)},
        "idempotency_key": {
            "type": "string", "pattern": r"^[A-Za-z0-9._~-]{8,128}$"
        },
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "destination_target_ref": {"$ref": "#/$defs/target_ref"},
        "content": {"$ref": "#/$defs/content"},
        "reaction": {"type": "string", "minLength": 1, "maxLength": 32},
        "schedule_at": {"type": "string", "format": "date-time"},
        "expected_revision": {"type": "string", "minLength": 1, "maxLength": 160},
    },
}

SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["preparation_ref", "action", "summary", "expires_at", "required_scopes"],
    "properties": {
        "preparation_ref": {
            "type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"
        },
        "action": {"type": "string", "enum": _enum_values(SocialAction)},
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "summary": {"type": "string", "minLength": 1, "maxLength": 1000},
        "expires_at": {"type": "string", "format": "date-time"},
        "required_scopes": {
            "type": "array", "minItems": 1, "maxItems": 1, "uniqueItems": True,
            "items": {"type": "string", "enum": sorted(SOCIAL_WORKSPACE_SCOPES)},
        },
    },
}

SOCIAL_WORKSPACE_COMMIT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["preparation_ref", "confirm"],
    "properties": {
        "preparation_ref": {
            "type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"
        },
        "confirm": {"const": True},
    },
}

SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["operation_ref", "status"],
    "properties": {
        "operation_ref": {"type": "string", "pattern": r"^op_[A-Za-z0-9_-]{24,160}$"},
        "status": {"type": "string", "enum": ["committed", "running", "succeeded"]},
    },
}

SOCIAL_WORKSPACE_STATUS_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "oneOf": [
        {
            "required": ["preparation_ref"],
            "properties": {
                "preparation_ref": {
                    "type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"
                }
            },
        },
        {
            "required": ["operation_ref"],
            "properties": {
                "operation_ref": {
                    "type": "string", "pattern": r"^op_[A-Za-z0-9_-]{24,160}$"
                }
            },
        },
    ],
    "properties": {
        "preparation_ref": {"type": "string"},
        "operation_ref": {"type": "string"},
    },
}

SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["operation_ref", "action", "status"],
    "properties": {
        "operation_ref": {"type": "string", "pattern": r"^op_[A-Za-z0-9_-]{24,160}$"},
        "action": {"type": "string", "enum": _enum_values(SocialAction)},
        "status": {"type": "string", "enum": _enum_values(SocialActionStatus)},
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "error_code": {"type": "string", "pattern": r"^[a-z][a-z0-9_]{1,63}$"},
        "read_after_write": {
            "type": "object",
            "additionalProperties": False,
            "required": ["verified", "observed_item_ref", "observed_at"],
            "properties": {
                "verified": {"type": "boolean"},
                "observed_item_ref": {"$ref": "#/$defs/item_ref"},
                "observed_at": {"type": "string", "format": "date-time"},
            },
        },
    },
}

SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": [
        "platform", "action", "status", "operation_ref", "target_ref", "item_ref",
        "read_after_write",
    ],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "action": {"const": SocialAction.SEND_MESSAGE.value},
        "status": {"const": SocialActionStatus.SUCCEEDED.value},
        "operation_ref": {"type": "string", "pattern": r"^op_[A-Za-z0-9_-]{24,160}$"},
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "read_after_write": {
            "type": "object",
            "additionalProperties": False,
            "required": ["verified", "observed_item_ref", "observed_at"],
            "properties": {
                "verified": {"const": True},
                "observed_item_ref": {"$ref": "#/$defs/item_ref"},
                "observed_at": {"type": "string", "format": "date-time"},
            },
        },
    },
}

SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": [
        "platform", "target_kinds", "read_operations", "actions", "content_features",
        "max_text_length", "max_media_items",
    ],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "target_kinds": {
            "type": "array", "uniqueItems": True,
            "items": {"type": "string", "enum": _enum_values(SocialTargetKind)},
        },
        "read_operations": {
            "type": "array", "uniqueItems": True,
            "items": {"type": "string", "enum": _enum_values(SocialReadOperation)},
        },
        "actions": {
            "type": "array", "uniqueItems": True,
            "items": {"type": "string", "enum": _enum_values(SocialAction)},
        },
        "content_features": {
            "type": "array", "uniqueItems": True,
            "items": {"type": "string", "enum": _enum_values(ContentFeature)},
        },
        "max_text_length": {"type": "integer", "minimum": 0, "maximum": 100000},
        "max_media_items": {"type": "integer", "minimum": 0, "maximum": 100},
    },
}


__all__ = [
    "ActionGateHook",
    "CapabilityHook",
    "ContentFeature",
    "GateDecision",
    "MediaAttachment",
    "MediaRole",
    "ReadGateHook",
    "RichContent",
    "RichEntity",
    "RichEntityKind",
    "SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA",
    "SOCIAL_WORKSPACE_COMMIT_SCHEMA",
    "SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA",
    "SOCIAL_WORKSPACE_PREPARE_SCHEMA",
    "SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_READ_SCHEMA",
    "SOCIAL_WORKSPACE_SCOPES",
    "SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA",
    "SOCIAL_WORKSPACE_STATUS_SCHEMA",
    "SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA",
    "SocialAction",
    "SocialActionIntent",
    "SocialActionStatus",
    "SocialCapabilities",
    "SocialItemKind",
    "SocialPlatform",
    "SocialReadOperation",
    "SocialReadPurpose",
    "SocialReadRequest",
    "SocialTargetKind",
    "TargetLocator",
    "TargetLocatorKind",
    "SocialWorkspaceValidationError",
    "enforce_action_gates",
    "enforce_editorial_sample_gates",
    "required_scope_for_action",
    "required_scope_for_read",
    "validate_capabilities",
    "validate_commit_request",
    "validate_opaque_ref",
    "validate_prepare_request",
    "validate_read_request",
    "validate_send_message_receipt",
    "validate_status_request",
]
