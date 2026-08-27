"""Provider-neutral contract for a broad, capability-gated social workspace.

This module deliberately contains no Telegram/VK transport code.  Provider adapters
exchange opaque references with this boundary and translate the validated operations
to their native APIs behind explicit consent, policy, and capability gates.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import urlsplit

from .media_contract import ChatGPTFile


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
    RESOLVE_ITEM = "resolve_item"
    SEARCH_TARGETS = "search_targets"
    LIST_DIALOGS = "list_dialogs"
    LIST_ITEMS = "list_items"
    SEARCH_ITEMS = "search_items"
    GET_ITEM = "get_item"
    LIST_COMMENTS = "list_comments"
    LIST_REACTIONS = "list_reactions"
    LIST_STORIES = "list_stories"
    GET_STATISTICS = "get_statistics"
    GET_AUDIENCE = "get_audience"
    LIST_NOTIFICATIONS = "list_notifications"
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


class SocialReactionPreset(_StringEnum):
    """Closed semantic reactions whose provider IDs stay server-side."""

    GITHUB_ADDED = "github_added"


# Invoking one of these tools is the ChatGPT connector's typed assertion that
# the current user explicitly requested the exact outbound action.  The
# durable prepare/commit split still binds payload, target and idempotency, but
# no second browser confirmation is required.  Mutating existing content
# (edit/delete) intentionally remains outside this set.
DIRECT_USER_AUTHORIZED_ACTIONS = frozenset(
    {
        SocialAction.SEND_MESSAGE,
        SocialAction.PUBLISH,
        SocialAction.FORWARD,
        SocialAction.REACTION,
        SocialAction.COMMENT,
        SocialAction.SCHEDULE,
        SocialAction.STORY,
    }
)


class SocialTargetKind(_StringEnum):
    SELF = "self"
    USER = "user"
    CHAT = "chat"
    CHANNEL = "channel"
    GROUP = "group"
    COMMUNITY = "community"


class TargetLocatorKind(_StringEnum):
    SELF = "self"
    USERNAME = "username"
    PROFILE_LINK = "profile_link"
    PROVIDER_ID = "provider_id"


class SocialReadPurpose(_StringEnum):
    EDITORIAL_ANALYSIS = "editorial_analysis"


class EditorialAuthorizationBasis(_StringEnum):
    SELF_OWNED = "self_owned"
    OPERATOR_AUTHORIZED = "operator_authorized"
    PROVIDER_APPROVED = "provider_approved"


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


class AssetLifecycleStatus(_StringEnum):
    STAGING = "staging"
    READY = "ready"
    FAILED = "failed"
    EXPIRED = "expired"


class SocialActionStatus(_StringEnum):
    PREPARED = "prepared"
    AWAITING_HUMAN_APPROVAL = "awaiting_human_approval"
    APPROVED = "approved"
    COMMITTED = "committed"
    RUNNING = "running"
    PROVIDER_ATTEMPTED = "provider_attempted"
    SUCCEEDED = "succeeded"
    OUTCOME_UNKNOWN = "outcome_unknown"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


_REF_PREFIXES = {"target": "tgt", "item": "itm", "asset": "ast"}
_OPAQUE_REF_RE = re.compile(r"^(tgt|itm|ast)_[A-Za-z0-9_-]{16,160}$")
_PREPARATION_REF_RE = re.compile(r"^prep_[A-Za-z0-9_-]{24,160}$")
_OPERATION_REF_RE = re.compile(r"^op_[A-Za-z0-9_-]{24,160}$")
_SAMPLE_REF_RE = re.compile(r"^smp_[A-Za-z0-9_-]{24,160}$")
_APPROVAL_REF_RE = re.compile(r"^apr_[A-Za-z0-9_-]{24,160}$")
_APPROVAL_RECEIPT_RE = re.compile(r"^arc_[A-Za-z0-9_-]{24,160}$")
_CURSOR_RE = re.compile(r"^[A-Za-z0-9_-]{1,512}$")
_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9._~-]{8,128}$")
_REACTION_RE = re.compile(r"^\S(?:.{0,30}\S)?$", re.DOTALL)

_SCOPE_SUFFIXES = frozenset(
    {
        "discover",
        "read:public",
        "read:private",
        "read:dialogs",
        "dm:send",
        "post:publish",
        "edit",
        "delete",
        "forward",
        "reaction",
        "comment",
        "schedule",
        "story:read",
        "story:write",
        "analytics",
        "audience",
    }
)
SOCIAL_WORKSPACE_SCOPES = frozenset(
    f"{platform.value}:{suffix}"
    for platform in SocialPlatform
    for suffix in _SCOPE_SUFFIXES
) | frozenset({"vk:notifications:read"})

_ACTION_SCOPE_SUFFIX: Mapping[SocialAction, str] = {
    SocialAction.SEND_MESSAGE: "dm:send",
    SocialAction.PUBLISH: "post:publish",
    SocialAction.EDIT: "edit",
    SocialAction.DELETE: "delete",
    SocialAction.FORWARD: "forward",
    SocialAction.REACTION: "reaction",
    SocialAction.COMMENT: "comment",
    SocialAction.SCHEDULE: "schedule",
    SocialAction.STORY: "story:write",
}


class SocialReadAccess(_StringEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    DIALOGS = "dialogs"


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
    platform: SocialPlatform | str,
    operation: SocialReadOperation | str,
    read_access: SocialReadAccess | str | None = None,
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
    if normalized_operation in {
        SocialReadOperation.RESOLVE_TARGET,
        SocialReadOperation.SEARCH_TARGETS,
    }:
        suffix = "discover"
    elif normalized_operation is SocialReadOperation.LIST_NOTIFICATIONS:
        if normalized_platform is not SocialPlatform.VK:
            raise SocialWorkspaceValidationError(
                "notifications are supported only for VK"
            )
        suffix = "notifications:read"
    elif normalized_operation is SocialReadOperation.LIST_STORIES:
        suffix = "story:read"
    elif normalized_operation is SocialReadOperation.GET_STATISTICS:
        suffix = "analytics"
    elif normalized_operation is SocialReadOperation.GET_AUDIENCE:
        suffix = "audience"
    else:
        normalized_access = (
            read_access
            if isinstance(read_access, SocialReadAccess)
            else _enum(read_access, SocialReadAccess, "read_access")
        )
        suffix = f"read:{normalized_access.value}"
    return frozenset({f"{normalized_platform.value}:{suffix}"})


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
    value: str | None


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
    read_access: SocialReadAccess | None
    expected_target_kinds: tuple[SocialTargetKind, ...]
    authorization_basis: EditorialAuthorizationBasis | None = None
    unread_only: bool = False
    transcribe_audio: bool = True
    transcription_wait_seconds: int = 0

    @property
    def required_scopes(self) -> frozenset[str]:
        return required_scope_for_read(self.platform, self.operation, self.read_access)


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
    reaction_preset: SocialReactionPreset | None
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
class EditorialSampleState:
    sample_ref: str
    target_ref: str
    target_kinds: frozenset[SocialTargetKind]
    purpose: SocialReadPurpose
    date_from: str | None
    date_to: str | None
    total_limit: int
    cumulative_count: int
    server_minted: bool
    continuation_cursor: str | None
    cursor_server_minted: bool
    ephemeral: bool
    durable_index: bool


EditorialSampleStateHook = Callable[[SocialReadRequest], EditorialSampleState]


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


@dataclass(frozen=True, slots=True)
class ApprovalContext:
    client_id: str
    subject: str
    resource: str
    action_digest: str
    preparation_ref: str
    preparation_expires_at: str
    preparation_durable: bool


@dataclass(frozen=True, slots=True)
class ApprovalGrant:
    approval_ref: str
    approval_receipt: str
    client_id: str
    subject: str
    resource: str
    action_digest: str
    preparation_ref: str
    preparation_expires_at: str
    durable_state: bool
    expires_at: str
    one_time: bool
    prior_uses: int
    consumed_now: bool


ApprovalConsumeHook = Callable[[str, str, ApprovalContext], ApprovalGrant]


@dataclass(frozen=True, slots=True)
class ValidatedCommit:
    preparation_ref: str
    approval_ref: str
    approval_receipt: str
    action_digest: str


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
    ephemeral_policy_hook: ReadGateHook,
    state_hook: EditorialSampleStateHook,
) -> EditorialSampleState:
    """Require explicit consent and purpose approval for bounded editorial analysis."""

    if request.operation is not SocialReadOperation.EDITORIAL_SAMPLE:
        raise SocialWorkspaceValidationError("editorial sample gate requires editorial_sample")
    for gate_name, hook in (
        ("consent", consent_hook),
        ("purpose", purpose_hook),
        ("ephemeral_policy", ephemeral_policy_hook),
    ):
        decision = hook(request)
        if not isinstance(decision, GateDecision):
            raise SocialWorkspaceValidationError(f"{gate_name} gate returned no decision")
        if not decision.allowed:
            raise SocialWorkspaceValidationError(f"{gate_name} denied: {decision.code}")
    state = state_hook(request)
    if not isinstance(state, EditorialSampleState):
        raise SocialWorkspaceValidationError("sample state hook returned no state")
    if (
        state.server_minted is not True
        or state.ephemeral is not True
        or state.durable_index is not False
        or not _SAMPLE_REF_RE.fullmatch(state.sample_ref)
    ):
        raise SocialWorkspaceValidationError("sample state is not server-minted ephemeral state")
    if (
        state.target_ref != request.target_ref
        or state.target_kinds != frozenset(request.expected_target_kinds)
        or state.purpose is not request.purpose
        or state.date_from != request.date_from
        or state.date_to != request.date_to
        or state.total_limit != request.total_limit
    ):
        raise SocialWorkspaceValidationError("sample continuation binding mismatch")
    if request.sample_ref is not None and request.sample_ref != state.sample_ref:
        raise SocialWorkspaceValidationError("sample continuation reference mismatch")
    if request.sample_ref is None:
        if state.cumulative_count != 0 or state.continuation_cursor is not None:
            raise SocialWorkspaceValidationError("new sample must start at zero without cursor")
    elif (
        state.cursor_server_minted is not True
        or state.continuation_cursor != request.cursor
        or not state.continuation_cursor
    ):
        raise SocialWorkspaceValidationError("sample continuation cursor is not server-minted")
    if (
        type(state.cumulative_count) is not int
        or type(state.total_limit) is not int
        or not 0 <= state.cumulative_count <= state.total_limit <= 100
        or state.cumulative_count + request.page_size > state.total_limit
    ):
        raise SocialWorkspaceValidationError("sample cumulative limit exceeded")
    return state


def _validate_basic_metrics(value: Any, field: str) -> None:
    metrics = _object(value, field)
    allowed = {"followers", "members", "views", "reactions", "comments", "shares"}
    _only_fields(metrics, allowed, field)
    if not metrics:
        raise SocialWorkspaceValidationError(f"{field} must not be empty")
    if any(type(metric) is not int or metric < 0 for metric in metrics.values()):
        raise SocialWorkspaceValidationError(f"{field} values must be non-negative integers")


def validate_editorial_sample_response(
    request: SocialReadRequest,
    state: EditorialSampleState,
    payload: Mapping[str, Any],
) -> int:
    """Bind an editorial page to its server state and return the new cumulative count."""

    if request.operation is not SocialReadOperation.EDITORIAL_SAMPLE:
        raise SocialWorkspaceValidationError("response is not for editorial_sample")
    if (
        state.server_minted is not True
        or state.ephemeral is not True
        or state.durable_index is not False
        or state.target_ref != request.target_ref
        or state.target_kinds != frozenset(request.expected_target_kinds)
        or state.purpose is not request.purpose
        or state.date_from != request.date_from
        or state.date_to != request.date_to
        or state.total_limit != request.total_limit
        or (request.sample_ref is not None and request.sample_ref != state.sample_ref)
    ):
        raise SocialWorkspaceValidationError("editorial response state binding mismatch")
    data = _object(payload, "editorial response")
    _only_fields(
        data,
        {
            "sample_ref", "target", "items", "sampled_count", "cumulative_count",
            "total_limit", "next_cursor", "storage_disposition", "trust",
        },
        "editorial response",
    )
    if data.get("sample_ref") != state.sample_ref:
        raise SocialWorkspaceValidationError("editorial response sample_ref mismatch")
    if data.get("total_limit") != request.total_limit or request.total_limit != state.total_limit:
        raise SocialWorkspaceValidationError("editorial response total_limit mismatch")
    if data.get("storage_disposition") != "ephemeral_no_index":
        raise SocialWorkspaceValidationError("editorial response is not ephemeral/no-index")
    if data.get("trust") != "untrusted_external_data":
        raise SocialWorkspaceValidationError("editorial response trust marker is missing")
    target = _object(data.get("target"), "editorial target")
    _only_fields(
        target,
        {"target_ref", "kind", "title", "about", "description", "basic_metrics", "trust"},
        "editorial target",
    )
    if target.get("target_ref") != request.target_ref or request.target_ref != state.target_ref:
        raise SocialWorkspaceValidationError("editorial response target_ref mismatch")
    kind = _enum(target.get("kind"), SocialTargetKind, "editorial target kind")
    allowed_kinds = {
        SocialTargetKind.CHANNEL,
        SocialTargetKind.GROUP,
        SocialTargetKind.COMMUNITY,
    }
    if (
        kind not in allowed_kinds
        or kind not in request.expected_target_kinds
        or kind not in state.target_kinds
    ):
        raise SocialWorkspaceValidationError("editorial response target kind mismatch")
    for field, maximum in (("title", 256), ("about", 1024), ("description", 1024)):
        _optional_text(target.get(field), field, maximum=maximum, required=True)
    _validate_basic_metrics(target.get("basic_metrics"), "editorial target basic_metrics")
    if target.get("trust") != "untrusted_external_data":
        raise SocialWorkspaceValidationError("editorial target trust marker is missing")

    items = data.get("items")
    if not isinstance(items, list) or len(items) > request.page_size:
        raise SocialWorkspaceValidationError("editorial items exceed requested page_size")
    for index, raw_item in enumerate(items):
        item = _object(raw_item, f"editorial items[{index}]")
        _only_fields(
            item,
            {"item_ref", "kind", "published_at", "text", "caption", "basic_metrics", "trust"},
            f"editorial items[{index}]",
        )
        validate_opaque_ref(item.get("item_ref"), "item")
        item_kind = _enum(item.get("kind"), SocialItemKind, "editorial item kind")
        if item_kind not in {SocialItemKind.MESSAGE, SocialItemKind.POST}:
            raise SocialWorkspaceValidationError("editorial item must be message or post")
        published_at = _validate_rfc3339(item.get("published_at"), "published_at")
        published = datetime.fromisoformat(
            published_at[:-1] + "+00:00" if published_at.endswith("Z") else published_at
        ).date().isoformat()
        if request.date_from is not None and published < request.date_from:
            raise SocialWorkspaceValidationError("editorial item predates requested range")
        if request.date_to is not None and published > request.date_to:
            raise SocialWorkspaceValidationError("editorial item exceeds requested range")
        if "text" not in item or "caption" not in item:
            raise SocialWorkspaceValidationError("editorial item text and caption are required")
        text = _optional_text(item.get("text"), "text", maximum=768, preserve_space=True)
        caption = _optional_text(item.get("caption"), "caption", maximum=256, preserve_space=True)
        assert text is not None and caption is not None
        if not text.strip() and not caption.strip():
            raise SocialWorkspaceValidationError("editorial item text or caption is required")
        _validate_basic_metrics(item.get("basic_metrics"), f"editorial items[{index}] basic_metrics")
        if item.get("trust") != "untrusted_external_data":
            raise SocialWorkspaceValidationError("editorial item trust marker is missing")
    if data.get("sampled_count") != len(items):
        raise SocialWorkspaceValidationError("sampled_count does not match items")
    cumulative = state.cumulative_count + len(items)
    if (
        data.get("cumulative_count") != cumulative
        or cumulative > request.total_limit
        or request.total_limit > 100
    ):
        raise SocialWorkspaceValidationError("editorial cumulative_count is incoherent")
    remaining = request.total_limit - cumulative
    next_cursor = data.get("next_cursor")
    if next_cursor is not None and (
        remaining <= 0
        or not isinstance(next_cursor, str)
        or not _CURSOR_RE.fullmatch(next_cursor)
    ):
        raise SocialWorkspaceValidationError(
            "next_cursor exists without remaining sample budget"
        )
    return cumulative


def _validate_target_locator(value: Any, platform: SocialPlatform) -> TargetLocator:
    data = _object(value, "target_locator")
    _only_fields(data, {"kind", "value"}, "target_locator")
    kind = _enum(data.get("kind"), TargetLocatorKind, "target locator kind")
    if kind is TargetLocatorKind.SELF:
        if "value" in data:
            raise SocialWorkspaceValidationError("self locator must not contain value")
        return TargetLocator(kind=kind, value=None)
    raw = _optional_text(data.get("value"), "target locator value", maximum=512, required=True)
    assert raw is not None
    if kind is TargetLocatorKind.USERNAME:
        normalized = raw.removeprefix("@")
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
        datetime.strptime(clean, "%Y-%m-%d").replace(tzinfo=timezone.utc)
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
            "page_size", "total_limit", "read_access", "expected_target_kinds",
            "authorization_basis",
            "unread_only",
            "transcribe_audio",
            "transcription_wait_seconds",
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
    read_access = (
        _enum(data.get("read_access"), SocialReadAccess, "read_access")
        if "read_access" in data
        else None
    )
    # Compatibility for ChatGPT connectors that cached the former generic
    # resolve_item schema, where read_access was advertised as optional.  The
    # canonical /c/<internal-chat>/<message> Telegram shape is unambiguously a
    # private read; all other exact item links use the public lane.
    if (
        operation is SocialReadOperation.RESOLVE_ITEM
        and read_access is None
        and target_locator is not None
        and target_locator.kind is TargetLocatorKind.PROFILE_LINK
    ):
        is_private_telegram_item = False
        if platform is SocialPlatform.TELEGRAM and target_locator.value is not None:
            path = urlsplit(target_locator.value).path
            is_private_telegram_item = bool(
                re.fullmatch(r"/c/[1-9][0-9]*/[1-9][0-9]*/?", path)
            )
        read_access = (
            SocialReadAccess.PRIVATE
            if is_private_telegram_item
            else SocialReadAccess.PUBLIC
        )
    raw_expected_kinds = data.get("expected_target_kinds", [])
    if (
        not isinstance(raw_expected_kinds, list)
        or any(not isinstance(value, str) for value in raw_expected_kinds)
        or len(raw_expected_kinds) != len(set(raw_expected_kinds))
    ):
        raise SocialWorkspaceValidationError("expected_target_kinds must be a unique array")
    expected_target_kinds = tuple(
        _enum(value, SocialTargetKind, "expected_target_kinds")
        for value in raw_expected_kinds
    )

    purpose = (
        _enum(data.get("purpose"), SocialReadPurpose, "purpose")
        if "purpose" in data
        else None
    )
    authorization_basis = (
        _enum(
            data.get("authorization_basis"),
            EditorialAuthorizationBasis,
            "authorization_basis",
        )
        if "authorization_basis" in data
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
    unread_only = data.get("unread_only", False)
    if type(unread_only) is not bool:
        raise SocialWorkspaceValidationError("unread_only must be a boolean")
    transcribe_audio = data.get("transcribe_audio", True)
    if type(transcribe_audio) is not bool:
        raise SocialWorkspaceValidationError("transcribe_audio must be a boolean")
    transcription_wait_seconds = data.get("transcription_wait_seconds", 0)
    if (
        type(transcription_wait_seconds) is not int
        or not 0 <= transcription_wait_seconds <= 30
    ):
        raise SocialWorkspaceValidationError(
            "transcription_wait_seconds must be an integer from 0 to 30"
        )
    if (
        "transcription_wait_seconds" in data
        and data.get("transcribe_audio") is not True
    ):
        raise SocialWorkspaceValidationError(
            "transcription_wait_seconds requires transcribe_audio=true"
        )
    if (
        "transcription_wait_seconds" in data
        and platform is not SocialPlatform.TELEGRAM
    ):
        raise SocialWorkspaceValidationError(
            "transcription_wait_seconds is supported only for Telegram"
        )
    if (
        "transcription_wait_seconds" in data
        and operation
        not in {
            SocialReadOperation.RESOLVE_ITEM,
            SocialReadOperation.LIST_ITEMS,
            SocialReadOperation.GET_ITEM,
            SocialReadOperation.LIST_COMMENTS,
        }
    ):
        raise SocialWorkspaceValidationError(
            "transcription_wait_seconds is unsupported for this read operation"
        )

    if operation is SocialReadOperation.RESOLVE_TARGET:
        if target_locator is None:
            raise SocialWorkspaceValidationError("target_locator is required for exact resolution")
        expected = set(expected_target_kinds)
        if target_locator.kind is TargetLocatorKind.SELF:
            if expected != {SocialTargetKind.SELF}:
                raise SocialWorkspaceValidationError("self resolution must expect only self")
        elif len(expected) != 1 or SocialTargetKind.SELF in expected:
            raise SocialWorkspaceValidationError(
                "exact target resolution must expect one non-self target kind"
            )
    elif operation is SocialReadOperation.RESOLVE_ITEM:
        if target_locator is None or target_locator.kind is not TargetLocatorKind.PROFILE_LINK:
            raise SocialWorkspaceValidationError(
                "exact item resolution requires one canonical item link"
            )
        if platform is SocialPlatform.VK and read_access is not SocialReadAccess.PUBLIC:
            raise SocialWorkspaceValidationError(
                "exact item resolution requires public read access"
            )
        if platform is SocialPlatform.TELEGRAM and read_access not in {
            SocialReadAccess.PUBLIC,
            SocialReadAccess.PRIVATE,
        }:
            raise SocialWorkspaceValidationError(
                "Telegram item resolution requires public or private read access"
            )
        if (
            len(expected_target_kinds) > 1
            or SocialTargetKind.SELF in expected_target_kinds
        ):
            raise SocialWorkspaceValidationError(
                "exact item resolution accepts at most one non-self target kind"
            )
    elif operation is SocialReadOperation.SEARCH_TARGETS:
        if not query:
            raise SocialWorkspaceValidationError("query is required for target discovery")
    elif operation is SocialReadOperation.LIST_DIALOGS:
        if platform is not SocialPlatform.VK:
            raise SocialWorkspaceValidationError("dialog listing is supported only for VK")
        if read_access is not SocialReadAccess.DIALOGS:
            raise SocialWorkspaceValidationError("dialog listing requires dialogs access")
        if raw_limit > 25:
            raise SocialWorkspaceValidationError(
                "dialog limit must be an integer from 1 to 25"
            )
        forbidden = {
            "target_ref",
            "item_ref",
            "query",
            "item_kinds",
            "target_locator",
            "purpose",
            "sample_ref",
            "date_from",
            "date_to",
            "page_size",
            "total_limit",
            "expected_target_kinds",
            "authorization_basis",
        } & set(data)
        if forbidden:
            raise SocialWorkspaceValidationError(
                "dialog listing does not allow: " + ", ".join(sorted(forbidden))
            )
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
    elif operation in {SocialReadOperation.GET_STATISTICS, SocialReadOperation.GET_AUDIENCE} and (
        target_ref is None
    ) == (item_ref is None):
        raise SocialWorkspaceValidationError("statistics requires exactly one target_ref or item_ref")
    elif operation is SocialReadOperation.EDITORIAL_SAMPLE:
        if target_ref is None:
            raise SocialWorkspaceValidationError("target_ref is required for editorial sample")
        if purpose is not SocialReadPurpose.EDITORIAL_ANALYSIS:
            raise SocialWorkspaceValidationError("editorial_analysis purpose is required")
        if authorization_basis is None:
            raise SocialWorkspaceValidationError(
                "editorial sample authorization_basis is required"
            )
        if read_access not in {SocialReadAccess.PUBLIC, SocialReadAccess.PRIVATE}:
            raise SocialWorkspaceValidationError("editorial sample requires public or private access")
        allowed_editorial_targets = {
            SocialTargetKind.CHANNEL,
            SocialTargetKind.GROUP,
            SocialTargetKind.COMMUNITY,
        }
        if not expected_target_kinds or not set(expected_target_kinds).issubset(
            allowed_editorial_targets
        ):
            raise SocialWorkspaceValidationError(
                "editorial sample target must be channel, group, or community"
            )
        if (sample_ref is None) != (cursor is None):
            raise SocialWorkspaceValidationError(
                "continuation requires server-minted sample_ref and cursor together"
            )
        forbidden = {"item_ref", "query", "limit", "item_kinds", "target_locator"} & set(data)
        if forbidden:
            raise SocialWorkspaceValidationError(
                "editorial sample does not allow: " + ", ".join(sorted(forbidden))
            )
    elif operation is SocialReadOperation.LIST_NOTIFICATIONS:
        if platform is not SocialPlatform.VK:
            raise SocialWorkspaceValidationError(
                "notifications are supported only for VK"
            )
        if read_access is not None:
            raise SocialWorkspaceValidationError(
                "notifications use their dedicated access scope"
            )
        if raw_limit > 25:
            raise SocialWorkspaceValidationError(
                "notification limit must be an integer from 1 to 25"
            )
        if date_from is not None and date_to is not None:
            start = datetime.strptime(date_from, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            end = datetime.strptime(date_to, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            if (end - start).days > 2:
                raise SocialWorkspaceValidationError(
                    "notification window must not exceed 48 hours"
                )
        forbidden = {
            "target_ref",
            "item_ref",
            "query",
            "item_kinds",
            "target_locator",
            "purpose",
            "sample_ref",
            "page_size",
            "total_limit",
            "expected_target_kinds",
            "authorization_basis",
        } & set(data)
        if forbidden:
            raise SocialWorkspaceValidationError(
                "notifications do not allow: " + ", ".join(sorted(forbidden))
            )
    if operation in {
        SocialReadOperation.LIST_ITEMS,
        SocialReadOperation.SEARCH_ITEMS,
        SocialReadOperation.GET_ITEM,
        SocialReadOperation.LIST_COMMENTS,
        SocialReadOperation.LIST_REACTIONS,
    } and read_access is None:
        raise SocialWorkspaceValidationError("read_access is required for this operation")
    if operation is not SocialReadOperation.LIST_DIALOGS and "unread_only" in data:
        raise SocialWorkspaceValidationError(
            "unread_only is supported only for dialog listing"
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
        read_access=read_access,
        expected_target_kinds=expected_target_kinds,
        authorization_basis=authorization_basis,
        unread_only=unread_only,
        transcribe_audio=transcribe_audio,
        transcription_wait_seconds=transcription_wait_seconds,
    )


def validate_resolved_target_preview(
    request: SocialReadRequest, payload: Mapping[str, Any]
) -> str:
    if request.operation is not SocialReadOperation.RESOLVE_TARGET or request.target_locator is None:
        raise SocialWorkspaceValidationError("target preview requires resolve_target request")
    data = _object(payload, "target preview")
    _only_fields(
        data,
        {
            "platform", "target_ref", "kind", "display_name", "canonical_handle",
            "profile_link", "description", "is_exact_match", "trust",
        },
        "target preview",
    )
    if _enum(data.get("platform"), SocialPlatform, "platform") is not request.platform:
        raise SocialWorkspaceValidationError("target preview platform mismatch")
    target_ref = validate_opaque_ref(data.get("target_ref"), "target")
    kind = _enum(data.get("kind"), SocialTargetKind, "target kind")
    if kind not in request.expected_target_kinds:
        raise SocialWorkspaceValidationError("target preview kind mismatch")
    if request.target_locator.kind is TargetLocatorKind.SELF:
        if kind is not SocialTargetKind.SELF:
            raise SocialWorkspaceValidationError("self locator did not resolve self")
    elif kind is SocialTargetKind.SELF:
        raise SocialWorkspaceValidationError("non-self locator resolved self")
    _optional_text(data.get("display_name"), "display_name", maximum=512, required=True)
    if data.get("is_exact_match") is not True:
        raise SocialWorkspaceValidationError("target resolution is not an exact match")
    if data.get("trust") != "untrusted_external_data":
        raise SocialWorkspaceValidationError("target preview trust marker is missing")
    return target_ref


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
            "destination_target_ref", "content", "reaction", "reaction_preset",
            "schedule_at", "expected_revision",
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
    reaction_preset = (
        _enum(data.get("reaction_preset"), SocialReactionPreset, "reaction preset")
        if data.get("reaction_preset") is not None
        else None
    )
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
    if (
        action
        in {
            SocialAction.SEND_MESSAGE,
            SocialAction.PUBLISH,
            SocialAction.SCHEDULE,
            SocialAction.STORY,
        }
        and target_ref is None
    ):
        raise SocialWorkspaceValidationError("target_ref is required for this action")
    if (
        action
        in {
            SocialAction.EDIT,
            SocialAction.DELETE,
            SocialAction.REACTION,
            SocialAction.COMMENT,
            SocialAction.FORWARD,
        }
        and item_ref is None
    ):
        raise SocialWorkspaceValidationError("item_ref is required for this action")
    if action is SocialAction.FORWARD and destination_ref is None:
        raise SocialWorkspaceValidationError("destination_target_ref is required for forward")
    if action is SocialAction.REACTION and (reaction is None) == (reaction_preset is None):
        raise SocialWorkspaceValidationError(
            "exactly one of reaction or reaction_preset is required for reaction"
        )
    if reaction_preset is not None and platform is not SocialPlatform.TELEGRAM:
        raise SocialWorkspaceValidationError("reaction preset is Telegram-only")
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
        SocialAction.REACTION: {"item_ref", "reaction", "reaction_preset"},
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
            "reaction_preset": reaction_preset,
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
    intent = SocialActionIntent(
        platform=platform,
        action=action,
        idempotency_key=idempotency_key,
        target_ref=target_ref,
        item_ref=item_ref,
        destination_target_ref=destination_ref,
        content=content,
        reaction=reaction,
        reaction_preset=reaction_preset,
        schedule_at=schedule_at,
        expected_revision=expected_revision,
    )
    validate_document_attachment_policy(intent)
    return intent


def validate_document_attachment_policy(intent: SocialActionIntent) -> None:
    """Keep Telegram document v1 narrow at every contract boundary."""

    media = intent.content.media if intent.content is not None else ()
    if not any(item.role is MediaRole.DOCUMENT for item in media):
        return
    if (
        intent.platform is not SocialPlatform.TELEGRAM
        or intent.action is not SocialAction.SEND_MESSAGE
        or len(media) != 1
        or media[0].role is not MediaRole.DOCUMENT
    ):
        raise SocialWorkspaceValidationError(
            "document content requires one Telegram send_message attachment"
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
        if (
            not isinstance(raw, list)
            or any(not isinstance(value, str) for value in raw)
            or len(raw) != len(set(raw))
        ):
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


def validate_commit_request(
    payload: Mapping[str, Any],
    *,
    context: ApprovalContext | None = None,
    approval_hook: ApprovalConsumeHook | None = None,
    now: datetime | None = None,
) -> ValidatedCommit:
    data = _object(payload, "request")
    _only_fields(
        data,
        {"preparation_ref", "approval_ref", "approval_receipt", "action_digest"},
        "request",
    )
    preparation_ref = data.get("preparation_ref")
    if not isinstance(preparation_ref, str) or not _PREPARATION_REF_RE.fullmatch(preparation_ref):
        raise SocialWorkspaceValidationError("preparation_ref is invalid")
    approval_ref = data.get("approval_ref")
    approval_receipt = data.get("approval_receipt")
    action_digest = data.get("action_digest")
    if not isinstance(approval_ref, str) or not _APPROVAL_REF_RE.fullmatch(approval_ref):
        raise SocialWorkspaceValidationError("approval_ref is invalid")
    if not isinstance(approval_receipt, str) or not _APPROVAL_RECEIPT_RE.fullmatch(
        approval_receipt
    ):
        raise SocialWorkspaceValidationError("approval_receipt is invalid")
    if not isinstance(action_digest, str) or not re.fullmatch(r"[a-f0-9]{64}", action_digest):
        raise SocialWorkspaceValidationError("action_digest is invalid")
    if context is None or approval_hook is None:
        raise SocialWorkspaceValidationError("durable approval validation hook is required")
    for name, value in (
        ("client_id", context.client_id),
        ("subject", context.subject),
        ("resource", context.resource),
    ):
        if not isinstance(value, str) or not value:
            raise SocialWorkspaceValidationError(f"approval {name} binding is invalid")
    if action_digest != context.action_digest:
        raise SocialWorkspaceValidationError("approval action digest mismatch")
    if (
        context.preparation_ref != preparation_ref
        or not _PREPARATION_REF_RE.fullmatch(context.preparation_ref)
        or context.preparation_durable is not True
    ):
        raise SocialWorkspaceValidationError("preparation is not durable or bound")
    preparation_expires_at = _validate_rfc3339(
        context.preparation_expires_at, "preparation expires_at"
    )
    grant = approval_hook(approval_ref, approval_receipt, context)
    if not isinstance(grant, ApprovalGrant):
        raise SocialWorkspaceValidationError("approval hook returned no grant")
    if (
        grant.approval_ref != approval_ref
        or grant.approval_receipt != approval_receipt
        or grant.client_id != context.client_id
        or grant.subject != context.subject
        or grant.resource != context.resource
        or grant.action_digest != action_digest
        or grant.preparation_ref != preparation_ref
        or grant.preparation_expires_at != preparation_expires_at
        or grant.durable_state is not True
    ):
        raise SocialWorkspaceValidationError("approval binding mismatch")
    expires_at = _validate_rfc3339(grant.expires_at, "approval expires_at")
    expires = datetime.fromisoformat(
        expires_at[:-1] + "+00:00" if expires_at.endswith("Z") else expires_at
    )
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    preparation_expires = datetime.fromisoformat(
        preparation_expires_at[:-1] + "+00:00"
        if preparation_expires_at.endswith("Z")
        else preparation_expires_at
    )
    if expires <= current or preparation_expires <= current:
        raise SocialWorkspaceValidationError("approval is expired")
    if (
        grant.one_time is not True
        or type(grant.prior_uses) is not int
        or grant.prior_uses != 0
        or grant.consumed_now is not True
    ):
        raise SocialWorkspaceValidationError("approval was not atomically consumed once")
    return ValidatedCommit(preparation_ref, approval_ref, approval_receipt, action_digest)


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
        {
            "platform", "action", "status", "operation_ref", "target_ref", "item_ref",
            "retry_safe", "read_after_write",
        },
        "receipt",
    )
    if _enum(data.get("action"), SocialAction, "action") is not SocialAction.SEND_MESSAGE:
        raise SocialWorkspaceValidationError("receipt is not for send_message")
    if _enum(data.get("status"), SocialActionStatus, "status") is not SocialActionStatus.SUCCEEDED:
        raise SocialWorkspaceValidationError("send_message has not succeeded")
    if data.get("retry_safe") is not False:
        raise SocialWorkspaceValidationError("successful send_message must not be retried")
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


def validate_action_status_response(payload: Mapping[str, Any]) -> SocialActionStatus:
    data = _object(payload, "status response")
    _only_fields(
        data,
        {
            "platform", "operation_ref", "action", "status", "retry_safe", "target_ref",
            "item_ref", "error_code", "read_after_write",
        },
        "status response",
    )
    _enum(data.get("platform"), SocialPlatform, "platform")
    operation_ref = data.get("operation_ref")
    if not isinstance(operation_ref, str) or not _OPERATION_REF_RE.fullmatch(operation_ref):
        raise SocialWorkspaceValidationError("operation_ref is invalid")
    action = _enum(data.get("action"), SocialAction, "action")
    status = _enum(data.get("status"), SocialActionStatus, "status")
    if type(data.get("retry_safe")) is not bool:
        raise SocialWorkspaceValidationError("retry_safe must be boolean")
    if status is SocialActionStatus.OUTCOME_UNKNOWN and data.get("retry_safe") is not False:
        raise SocialWorkspaceValidationError("outcome_unknown must not be retried")
    if action is SocialAction.SEND_MESSAGE and status is SocialActionStatus.SUCCEEDED:
        validate_send_message_receipt(data)
    return status


@dataclass(frozen=True, slots=True)
class AssetStageRequest:
    platform: SocialPlatform
    file: ChatGPTFile
    role: MediaRole


def validate_asset_stage_request(payload: Mapping[str, Any]) -> AssetStageRequest:
    data = _object(payload, "request")
    _only_fields(data, {"platform", "file", "role"}, "request")
    file_data = _object(data.get("file"), "file")
    _only_fields(
        file_data,
        {"download_url", "file_id", "mime_type", "file_name"},
        "file",
    )
    download_url = file_data.get("download_url")
    if (
        not isinstance(download_url, str)
        or download_url != download_url.strip()
        or not 1 <= len(download_url) <= 4096
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in download_url)
    ):
        raise SocialWorkspaceValidationError("file download_url is invalid")
    parsed_url = urlsplit(download_url)
    if (
        parsed_url.scheme != "https"
        or not parsed_url.netloc
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.fragment
    ):
        raise SocialWorkspaceValidationError("file download_url is invalid")
    file_id = file_data.get("file_id")
    if (
        not isinstance(file_id, str)
        or not 1 <= len(file_id) <= 256
        or file_id != file_id.strip()
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in file_id)
    ):
        raise SocialWorkspaceValidationError("file file_id is invalid")
    role = _enum(data.get("role"), MediaRole, "role")
    platform = _enum(data.get("platform"), SocialPlatform, "platform")
    mime_type = _optional_text(file_data.get("mime_type"), "mime_type", maximum=100)
    if mime_type is not None and not re.fullmatch(
        r"(?:image|video|audio|application|text)/[A-Za-z0-9.+-]{1,64}", mime_type
    ):
        raise SocialWorkspaceValidationError("file mime_type is invalid")
    raw_file_name = file_data.get("file_name")
    if role is MediaRole.DOCUMENT:
        # The document-policy boundary, not this transport parser, owns NFKC,
        # basename, control/bidi stripping and extension enforcement. Preserve
        # the bounded untrusted hint so that sanitizer is actually exercised.
        if raw_file_name is not None and (
            not isinstance(raw_file_name, str) or len(raw_file_name) > 255
        ):
            raise SocialWorkspaceValidationError("file file_name is invalid")
        file_name = raw_file_name
    else:
        file_name = _optional_text(raw_file_name, "file_name", maximum=255)
        if file_name is not None and any(
            character in file_name for character in ("/", "\\", "\x00")
        ):
            raise SocialWorkspaceValidationError("file file_name is invalid")
    if role not in {MediaRole.IMAGE, MediaRole.DOCUMENT}:
        raise SocialWorkspaceValidationError(
            "only image or document asset staging is enabled"
        )
    if role is MediaRole.DOCUMENT and platform is not SocialPlatform.TELEGRAM:
        raise SocialWorkspaceValidationError(
            "document asset staging is supported only for Telegram"
        )
    return AssetStageRequest(
        platform=platform,
        file=ChatGPTFile(
            download_url=download_url,
            file_id=file_id,
            mime_type=mime_type,
            file_name=file_name,
        ),
        role=role,
    )


def validate_asset_status_request(payload: Mapping[str, Any]) -> str:
    data = _object(payload, "request")
    _only_fields(data, {"asset_ref"}, "request")
    return validate_opaque_ref(data.get("asset_ref"), "asset")


def validate_asset_preview_request(payload: Mapping[str, Any]) -> tuple[SocialPlatform, str]:
    data = _object(payload, "request")
    _only_fields(data, {"platform", "asset_ref"}, "request")
    return (
        _enum(data.get("platform"), SocialPlatform, "platform"),
        validate_opaque_ref(data.get("asset_ref"), "asset"),
    )


def compute_action_digest(
    intent: SocialActionIntent,
    *,
    verified_assets: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    value: dict[str, Any] = {"intent": asdict(intent)}
    if verified_assets is not None:
        value["verified_assets"] = sorted(
            (dict(asset) for asset in verified_assets),
            key=lambda asset: str(asset.get("asset_ref", "")),
        )
    encoded = json.dumps(
        value if verified_assets is not None else asdict(intent),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class RecursiveRedactionResult:
    value: Any
    recursive: bool


@dataclass(frozen=True, slots=True)
class DurableIdempotencyReservation:
    key: str
    action_digest: str
    durable: bool
    accepted: bool


@dataclass(frozen=True, slots=True)
class SafetyExecutionContext:
    client_id: str
    subject: str
    resource: str
    platform: SocialPlatform
    operation: str
    action_digest: str | None
    encoded_response_bytes: int
    media_items: int


@dataclass(frozen=True, slots=True)
class AuditAppendResult:
    appended: bool
    durable: bool


@dataclass(frozen=True, slots=True)
class SafetyAuditEvent:
    principal_hash: str
    platform: SocialPlatform
    operation: str
    action_digest: str | None
    encoded_response_bytes: int
    media_items: int
    outcome: str
    reason_code: str


RecursiveRedactionHook = Callable[[Any], RecursiveRedactionResult]
ResponseCapHook = Callable[[SafetyExecutionContext], GateDecision]
BudgetHook = Callable[[SafetyExecutionContext], GateDecision]
IdempotencyHook = Callable[[SocialActionIntent, str], DurableIdempotencyReservation]
AuditAppendHook = Callable[[SafetyAuditEvent], AuditAppendResult]


@dataclass(frozen=True, slots=True)
class ExecutionSafetyHooks:
    recursive_redaction: RecursiveRedactionHook
    response_cap: ResponseCapHook
    append_audit: AuditAppendHook
    durable_idempotency: IdempotencyHook
    rate_budget: BudgetHook
    egress_budget: BudgetHook
    media_budget: BudgetHook


def enforce_execution_safety(
    request: SocialActionIntent | SocialReadRequest,
    response: Any,
    *,
    client_id: str,
    subject: str,
    resource: str,
    hooks: ExecutionSafetyHooks | None,
    encoded_response_cap: int = 128 * 1024,
) -> Any:
    """Apply every guard; a durable sanitized audit is mandatory before any denial."""

    if hooks is None:
        raise SocialWorkspaceValidationError("execution safety hooks are required")
    if not callable(hooks.append_audit):
        raise SocialWorkspaceValidationError("append_audit hook is required")
    action_digest = (
        compute_action_digest(request) if isinstance(request, SocialActionIntent) else None
    )
    media_items = (
        len(request.content.media)
        if isinstance(request, SocialActionIntent) and request.content is not None
        else 0
    )
    context = SafetyExecutionContext(
        client_id=client_id,
        subject=subject,
        resource=resource,
        platform=request.platform,
        operation=(
            request.action.value
            if isinstance(request, SocialActionIntent)
            else request.operation.value
        ),
        action_digest=action_digest,
        encoded_response_bytes=0,
        media_items=media_items,
    )
    principal_hash = hashlib.sha256(
        (client_id + "\0" + subject + "\0" + resource).encode("utf-8")
    ).hexdigest()

    def append_audit(outcome: str, reason_code: str) -> None:
        event = SafetyAuditEvent(
            principal_hash=principal_hash,
            platform=context.platform,
            operation=context.operation,
            action_digest=context.action_digest,
            encoded_response_bytes=context.encoded_response_bytes,
            media_items=context.media_items,
            outcome=outcome,
            reason_code=reason_code,
        )
        try:
            result = hooks.append_audit(event)
        except Exception as exc:
            raise SocialWorkspaceValidationError("append-only durable audit failed") from exc
        if (
            not isinstance(result, AuditAppendResult)
            or result.appended is not True
            or result.durable is not True
        ):
            raise SocialWorkspaceValidationError("append-only durable audit failed")

    def deny(reason_code: str, message: str) -> None:
        append_audit("denied", reason_code)
        raise SocialWorkspaceValidationError(message)

    for name, hook in (
        ("recursive_redaction", hooks.recursive_redaction),
        ("response_cap", hooks.response_cap),
        ("durable_idempotency", hooks.durable_idempotency),
        ("rate_budget", hooks.rate_budget),
        ("egress_budget", hooks.egress_budget),
        ("media_budget", hooks.media_budget),
    ):
        if not callable(hook):
            deny(f"missing_{name}", f"{name} hook is required")
    if isinstance(request, SocialActionIntent):
        assert action_digest is not None
        try:
            reservation = hooks.durable_idempotency(request, action_digest)
        except Exception:  # noqa: BLE001 - fail closed on caller-supplied hook errors
            deny("idempotency_hook_failed", "durable idempotency reservation denied")
        if (
            not isinstance(reservation, DurableIdempotencyReservation)
            or reservation.key != request.idempotency_key
            or reservation.action_digest != action_digest
            or reservation.durable is not True
            or reservation.accepted is not True
        ):
            deny("idempotency_denied", "durable idempotency reservation denied")
    try:
        redaction = hooks.recursive_redaction(response)
    except Exception:  # noqa: BLE001 - fail closed on caller-supplied hook errors
        deny("redaction_hook_failed", "recursive redaction did not complete")
    if not isinstance(redaction, RecursiveRedactionResult) or redaction.recursive is not True:
        deny("redaction_incomplete", "recursive redaction did not complete")
    try:
        encoded = json.dumps(
            redaction.value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    except (TypeError, ValueError):
        deny("response_not_json", "redacted response is not JSON encodable")
    context = SafetyExecutionContext(
        client_id=context.client_id,
        subject=context.subject,
        resource=context.resource,
        platform=context.platform,
        operation=context.operation,
        action_digest=context.action_digest,
        encoded_response_bytes=len(encoded),
        media_items=context.media_items,
    )
    if type(encoded_response_cap) is not int or not 1 <= encoded_response_cap <= 128 * 1024:
        deny("invalid_response_cap", "encoded response cap is invalid")
    if len(encoded) > encoded_response_cap:
        deny("response_cap_exceeded", "encoded response cap exceeded")
    for name, hook in (
        ("response_cap", hooks.response_cap),
        ("rate_budget", hooks.rate_budget),
        ("egress_budget", hooks.egress_budget),
        ("media_budget", hooks.media_budget),
    ):
        try:
            decision = hook(context)
        except Exception:  # noqa: BLE001 - fail closed on caller-supplied hook errors
            deny(f"{name}_hook_failed", f"{name} denied: hook_failed")
        if not isinstance(decision, GateDecision) or not decision.allowed:
            code = decision.code if isinstance(decision, GateDecision) else "invalid_decision"
            deny(f"{name}_denied", f"{name} denied: {code}")
    append_audit("succeeded", "all_safety_gates_passed")
    return redaction.value


def _enum_values(enum_type: type[_StringEnum]) -> list[str]:
    return [item.value for item in enum_type]


_DEFS: dict[str, Any] = {
    "target_ref": {"type": "string", "pattern": r"^tgt_[A-Za-z0-9_-]{16,160}$"},
    "item_ref": {"type": "string", "pattern": r"^itm_[A-Za-z0-9_-]{16,160}$"},
    "asset_ref": {"type": "string", "pattern": r"^ast_[A-Za-z0-9_-]{16,160}$"},
    "target_locator": {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind"],
        "properties": {
            "kind": {"type": "string", "enum": _enum_values(TargetLocatorKind)},
            "value": {"type": "string", "minLength": 1, "maxLength": 512},
        },
        "allOf": [
            {
                "if": {"properties": {"kind": {"const": "self"}}, "required": ["kind"]},
                "then": {"not": {"required": ["value"]}},
                "else": {"required": ["value"]},
            }
        ],
    },
    "basic_metrics": {
        "type": "object",
        "additionalProperties": False,
        "minProperties": 1,
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
        "authorization_basis": {
            "type": "string",
            "enum": _enum_values(EditorialAuthorizationBasis),
        },
        "unread_only": {"type": "boolean"},
        "transcribe_audio": {
            "type": "boolean",
            "default": True,
            "description": "Enrich Telegram voice/audio media through the existing transcription pipeline.",
        },
        "transcription_wait_seconds": {
            "type": "integer",
            "minimum": 0,
            "maximum": 30,
            "default": 0,
            "description": (
                "Telegram-only bounded wait for the whole voice/audio batch. "
                "Zero enqueues or snapshots without actively waiting."
            ),
        },
        "sample_ref": {"type": "string", "pattern": r"^smp_[A-Za-z0-9_-]{24,160}$"},
        "date_from": {"type": "string", "format": "date"},
        "date_to": {"type": "string", "format": "date"},
        "page_size": {"type": "integer", "minimum": 1, "maximum": 25},
        "total_limit": {"type": "integer", "minimum": 1, "maximum": 100},
        "read_access": {"type": "string", "enum": _enum_values(SocialReadAccess)},
        "expected_target_kinds": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {"type": "string", "enum": _enum_values(SocialTargetKind)},
        },
    },
    "allOf": [
        {
            "if": {"required": ["transcription_wait_seconds"]},
            "then": {
                "required": ["transcribe_audio"],
                "properties": {
                    "platform": {"const": "telegram"},
                    "transcribe_audio": {"const": True},
                },
            },
        }
    ],
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
        "sample_ref", "target", "items", "sampled_count", "cumulative_count", "total_limit",
        "storage_disposition", "trust",
    ],
    "properties": {
        "sample_ref": {"type": "string", "pattern": r"^smp_[A-Za-z0-9_-]{24,160}$"},
        "target": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "target_ref", "kind", "title", "about", "description", "basic_metrics", "trust"
            ],
            "properties": {
                "target_ref": {"$ref": "#/$defs/target_ref"},
                "kind": {
                    "type": "string",
                    "enum": ["channel", "group", "community"],
                },
                "title": {"type": "string", "minLength": 1, "maxLength": 256},
                "about": {"type": "string", "maxLength": 1024},
                "description": {"type": "string", "maxLength": 1024},
                "basic_metrics": {"$ref": "#/$defs/basic_metrics"},
                "trust": {"const": "untrusted_external_data"},
            },
        },
        "items": {
            "type": "array",
            "maxItems": 25,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "item_ref", "kind", "published_at", "text", "caption", "basic_metrics", "trust"
                ],
                "properties": {
                    "item_ref": {"$ref": "#/$defs/item_ref"},
                    "kind": {"type": "string", "enum": ["message", "post"]},
                    "published_at": {"type": "string", "format": "date-time"},
                    "text": {"type": "string", "maxLength": 768},
                    "caption": {"type": "string", "maxLength": 256},
                    "basic_metrics": {"$ref": "#/$defs/basic_metrics"},
                    "trust": {"const": "untrusted_external_data"},
                },
            },
        },
        "sampled_count": {"type": "integer", "minimum": 0, "maximum": 100},
        "cumulative_count": {"type": "integer", "minimum": 0, "maximum": 100},
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
        "reaction_preset": {
            "type": "string",
            "enum": _enum_values(SocialReactionPreset),
            "description": (
                "Telegram-only semantic marker. github_added uses the "
                "server-configured GitHub custom emoji without exposing its "
                "provider document ID."
            ),
        },
        "schedule_at": {"type": "string", "format": "date-time"},
        "expected_revision": {"type": "string", "minLength": 1, "maxLength": 160},
    },
    "allOf": [
        {
            "if": {
                "properties": {"action": {"const": "reaction"}},
                "required": ["action"],
            },
            "then": {
                "oneOf": [
                    {
                        "required": ["reaction"],
                        "not": {"required": ["reaction_preset"]},
                    },
                    {
                        "required": ["reaction_preset"],
                        "not": {"required": ["reaction"]},
                        "properties": {"platform": {"const": "telegram"}},
                    },
                ]
            },
        },
        {
            "if": {
                "properties": {
                    "content": {
                        "properties": {
                            "media": {
                                "contains": {
                                    "properties": {"role": {"const": "document"}},
                                    "required": ["role"],
                                }
                            }
                        },
                        "required": ["media"],
                    }
                },
                "required": ["content"],
            },
            "then": {
                "properties": {
                    "platform": {"const": "telegram"},
                    "action": {"const": "send_message"},
                    "content": {
                        "properties": {
                            "media": {
                                "minItems": 1,
                                "maxItems": 1,
                                "items": {
                                    "properties": {"role": {"const": "document"}},
                                    "required": ["role"],
                                },
                            }
                        }
                    },
                }
            },
        }
    ],
}

SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": [
        "preparation_ref", "action", "status", "action_digest", "summary", "expires_at",
        "required_scopes",
    ],
    "properties": {
        "preparation_ref": {
            "type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"
        },
        "action": {"type": "string", "enum": _enum_values(SocialAction)},
        "status": {
            "type": "string",
            "enum": [
                SocialActionStatus.AWAITING_HUMAN_APPROVAL.value,
                SocialActionStatus.APPROVED.value,
            ],
        },
        "action_digest": {"type": "string", "pattern": r"^[a-f0-9]{64}$"},
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "summary": {"type": "string", "minLength": 1, "maxLength": 1000},
        "expires_at": {"type": "string", "format": "date-time"},
        "required_scopes": {
            "type": "array", "minItems": 1, "maxItems": 1, "uniqueItems": True,
            "items": {"type": "string", "enum": sorted(SOCIAL_WORKSPACE_SCOPES)},
        },
        "approval_url": {"type": "string", "format": "uri", "maxLength": 1000},
    },
}

SOCIAL_WORKSPACE_COMMIT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["preparation_ref", "approval_ref", "approval_receipt", "action_digest"],
    "properties": {
        "preparation_ref": {
            "type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"
        },
        "approval_ref": {
            "type": "string", "pattern": r"^apr_[A-Za-z0-9_-]{24,160}$"
        },
        "approval_receipt": {
            "type": "string", "pattern": r"^arc_[A-Za-z0-9_-]{24,160}$"
        },
        "action_digest": {"type": "string", "pattern": r"^[a-f0-9]{64}$"},
    },
}

# Public MCP commit consumes approval state created by the operator-authenticated
# browser page. Approval credentials never enter the model context.
SOCIAL_WORKSPACE_MCP_COMMIT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["preparation_ref", "action_digest"],
    "properties": {
        "preparation_ref": {
            "type": "string",
            "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$",
        },
        "action_digest": {"type": "string", "pattern": r"^[a-f0-9]{64}$"},
    },
}

SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["operation_ref", "status"],
    "properties": {
        "operation_ref": {"type": "string", "pattern": r"^op_[A-Za-z0-9_-]{24,160}$"},
        "status": {
            "type": "string",
            "enum": ["approved", "committed", "running", "provider_attempted", "succeeded"],
        },
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
    "required": ["platform", "operation_ref", "action", "status", "retry_safe"],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "operation_ref": {"type": "string", "pattern": r"^op_[A-Za-z0-9_-]{24,160}$"},
        "action": {"type": "string", "enum": _enum_values(SocialAction)},
        "status": {"type": "string", "enum": _enum_values(SocialActionStatus)},
        "retry_safe": {"type": "boolean"},
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
    "allOf": [
        {
            "if": {"properties": {"status": {"const": "outcome_unknown"}}, "required": ["status"]},
            "then": {"properties": {"retry_safe": {"const": False}}},
        },
        {
            "if": {
                "properties": {
                    "action": {"const": "send_message"},
                    "status": {"const": "succeeded"},
                },
                "required": ["action", "status"],
            },
            "then": {
                "required": ["target_ref", "item_ref", "read_after_write"],
                "properties": {
                    "retry_safe": {"const": False},
                    "read_after_write": {
                        "properties": {"verified": {"const": True}}
                    },
                },
            },
        },
    ],
}

SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": [
        "platform", "action", "status", "operation_ref", "target_ref", "item_ref",
        "retry_safe", "read_after_write",
    ],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "action": {"const": SocialAction.SEND_MESSAGE.value},
        "status": {"const": SocialActionStatus.SUCCEEDED.value},
        "retry_safe": {"const": False},
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

_EXTERNAL_TARGET: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "target_ref", "kind", "title", "about", "description", "basic_metrics", "trust"
    ],
    "properties": {
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "kind": {"type": "string", "enum": _enum_values(SocialTargetKind)},
        "title": {"type": "string", "minLength": 1, "maxLength": 256},
        "canonical_handle": {"type": "string", "maxLength": 128},
        "profile_link": {"type": "string", "maxLength": 512},
        "about": {"type": "string", "maxLength": 1024},
        "description": {"type": "string", "maxLength": 1024},
        "basic_metrics": {"$ref": "#/$defs/basic_metrics"},
        "trust": {"const": "untrusted_external_data"},
    },
}

_EXTERNAL_TRANSCRIPTION: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "status",
        "cache_hit",
        "created",
        "text_included",
        "truncated",
        "next_offset",
        "next_poll_after_seconds",
        "trust",
    ],
    "properties": {
        "status": {
            "type": "string",
            "enum": ["ready", "queued", "running", "failed"],
        },
        "transcription_ref": {
            "type": "string",
            "pattern": r"^atr_[A-Za-z0-9_-]{24,160}$",
        },
        "text": {"type": "string", "maxLength": 60000},
        "cache_hit": {"type": "boolean"},
        "created": {"type": "boolean"},
        "text_included": {"type": "boolean"},
        "truncated": {"type": "boolean"},
        "next_offset": {
            "type": ["integer", "null"],
            "minimum": 0,
            "maximum": 2147483647,
        },
        "next_poll_after_seconds": {
            "type": "integer",
            "minimum": 0,
            "maximum": 86400,
        },
        "error_code": {"type": "string", "pattern": r"^[A-Z0-9_]{3,64}$"},
        "trust": {"const": "untrusted_external_data"},
    },
}

_EXTERNAL_TRANSCRIPTION_SUMMARY: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "total",
        "ready",
        "queued",
        "running",
        "failed",
        "cache_hits",
        "created",
        "wait_expired",
        "next_poll_after_seconds",
    ],
    "properties": {
        "total": {"type": "integer", "minimum": 0, "maximum": 250},
        "ready": {"type": "integer", "minimum": 0, "maximum": 250},
        "queued": {"type": "integer", "minimum": 0, "maximum": 250},
        "running": {"type": "integer", "minimum": 0, "maximum": 250},
        "failed": {"type": "integer", "minimum": 0, "maximum": 250},
        "cache_hits": {"type": "integer", "minimum": 0, "maximum": 250},
        "created": {"type": "integer", "minimum": 0, "maximum": 250},
        "wait_expired": {"type": "boolean"},
        "next_poll_after_seconds": {
            "type": "integer",
            "minimum": 0,
            "maximum": 86400,
        },
    },
}

_EXTERNAL_MEDIA_DETAIL: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["asset_ref", "kind", "trust"],
    "properties": {
        "asset_ref": {"$ref": "#/$defs/asset_ref"},
        "kind": {
            "type": "string",
            "enum": [
                "voice", "audio", "photo", "video", "round_video",
                "animation", "document",
            ],
        },
        "mime_type": {"type": "string", "maxLength": 128},
        "byte_length": {"type": "integer", "minimum": 0},
        "duration_seconds": {"type": "number", "minimum": 0},
        "transcription": _EXTERNAL_TRANSCRIPTION,
        "trust": {"const": "untrusted_external_data"},
    },
}

_EXTERNAL_ITEM: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "item_ref", "kind", "published_at", "text", "caption", "basic_metrics", "trust"
    ],
    "properties": {
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "kind": {"type": "string", "enum": _enum_values(SocialItemKind)},
        "published_at": {"type": "string", "format": "date-time"},
        "text": {"type": "string", "maxLength": 4096},
        "caption": {"type": "string", "maxLength": 1024},
        "basic_metrics": {"$ref": "#/$defs/basic_metrics"},
        "media": {
            "type": "array", "maxItems": 10,
            "items": {"$ref": "#/$defs/asset_ref"},
        },
        "attachments": {
            "type": "array", "maxItems": 10,
            "items": _EXTERNAL_MEDIA_DETAIL,
        },
        "media_details": {
            "type": "array", "maxItems": 10,
            "items": _EXTERNAL_MEDIA_DETAIL,
        },
        "entities": {
            "type": "array",
            "maxItems": 256,
            "items": {"$ref": "#/$defs/entity"},
        },
        "trust": {"const": "untrusted_external_data"},
    },
}


def _external_page_schema(
    item_schema: Mapping[str, Any], *, include_transcription_summary: bool = False
) -> Mapping[str, Any]:
    properties: dict[str, Any] = {
        "results": {"type": "array", "maxItems": 25, "items": item_schema},
        "next_cursor": {"type": "string", "pattern": r"^[A-Za-z0-9_-]{1,512}$"},
        "trust": {"const": "untrusted_external_data"},
    }
    if include_transcription_summary:
        properties["transcription_summary"] = _EXTERNAL_TRANSCRIPTION_SUMMARY
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": _DEFS,
        "type": "object",
        "additionalProperties": False,
        "required": ["results", "trust"],
        "properties": properties,
    }


SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA = _external_page_schema(_EXTERNAL_TARGET)
SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA = _external_page_schema(_EXTERNAL_TARGET)
_EXTERNAL_DIALOG: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["target_ref", "kind", "title", "unread_count", "trust"],
    "properties": {
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "kind": {
            "type": "string",
            "enum": ["user", "chat", "community"],
        },
        "title": {"type": "string", "minLength": 1, "maxLength": 256},
        "unread_count": {"type": "integer", "minimum": 0, "maximum": 1000000},
        "trust": {"const": "untrusted_external_data"},
    },
}
SOCIAL_WORKSPACE_DIALOG_LIST_OUTPUT_SCHEMA = _external_page_schema(_EXTERNAL_DIALOG)
SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA = _external_page_schema(
    _EXTERNAL_ITEM, include_transcription_summary=True
)
SOCIAL_WORKSPACE_ITEM_SEARCH_OUTPUT_SCHEMA = _external_page_schema(_EXTERNAL_ITEM)
_EXTERNAL_STORY_ITEM: Mapping[str, Any] = {
    **_EXTERNAL_ITEM,
    "properties": {**_EXTERNAL_ITEM["properties"], "kind": {"const": "story"}},
}
_EXTERNAL_COMMENT_ITEM: Mapping[str, Any] = {
    **_EXTERNAL_ITEM,
    "properties": {**_EXTERNAL_ITEM["properties"], "kind": {"const": "comment"}},
}
SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA = _external_page_schema(_EXTERNAL_STORY_ITEM)

SOCIAL_WORKSPACE_TARGET_GET_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["target", "trust"],
    "properties": {
        "target": _EXTERNAL_TARGET,
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["item", "trust"],
    "properties": {
        "item": _EXTERNAL_ITEM,
        "transcription_summary": _EXTERNAL_TRANSCRIPTION_SUMMARY,
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_ITEM_RESOLVE_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["item", "source_target", "trust"],
    "properties": {
        "item": _EXTERNAL_ITEM,
        "source_target": _EXTERNAL_TARGET,
        "transcription_summary": _EXTERNAL_TRANSCRIPTION_SUMMARY,
        "trust": {"const": "untrusted_external_data"},
    },
}

_EXTERNAL_NOTIFICATION: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "item_ref", "root_item_ref", "kind", "published_at", "text", "trust"
    ],
    "properties": {
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "root_item_ref": {"$ref": "#/$defs/item_ref"},
        "kind": {"const": "comment"},
        "published_at": {"type": "string", "format": "date-time"},
        "text": {"type": "string", "maxLength": 4096},
        "source_kind": {
            "type": "string",
            "enum": ["comment", "mention"],
        },
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_NOTIFICATIONS_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["results", "trust"],
    "properties": {
        "results": {
            "type": "array",
            "maxItems": 25,
            "items": _EXTERNAL_NOTIFICATION,
        },
        "next_cursor": {
            "type": "string",
            "pattern": r"^[A-Za-z0-9_-]{1,512}$",
        },
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["root_item_ref", "items", "trust"],
    "properties": {
        "root_item_ref": {"$ref": "#/$defs/item_ref"},
        "items": {"type": "array", "maxItems": 25, "items": _EXTERNAL_COMMENT_ITEM},
        "next_cursor": {"type": "string", "pattern": r"^[A-Za-z0-9_-]{1,512}$"},
        "transcription_summary": _EXTERNAL_TRANSCRIPTION_SUMMARY,
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["item_ref", "reactions", "trust"],
    "properties": {
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "reactions": {
            "type": "array", "maxItems": 50,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["reaction", "count"],
                "properties": {
                    "reaction": {"type": "string", "minLength": 1, "maxLength": 32},
                    "count": {"type": "integer", "minimum": 0},
                    "actor_target_ref": {"$ref": "#/$defs/target_ref"},
                },
            },
        },
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["period_from", "period_to", "basic_metrics", "trust"],
    "properties": {
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "item_ref": {"$ref": "#/$defs/item_ref"},
        "period_from": {"type": "string", "format": "date-time"},
        "period_to": {"type": "string", "format": "date-time"},
        "basic_metrics": {"$ref": "#/$defs/basic_metrics"},
        "trust": {"const": "untrusted_external_data"},
    },
    "oneOf": [{"required": ["target_ref"]}, {"required": ["item_ref"]}],
}

SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["target_ref", "audience", "trust"],
    "properties": {
        "target_ref": {"$ref": "#/$defs/target_ref"},
        "audience": {
            "type": "object",
            "additionalProperties": False,
            "required": ["total"],
            "properties": {
                "total": {"type": "integer", "minimum": 0},
                "online": {"type": "integer", "minimum": 0},
            },
        },
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "OpenAIFile": {
            "type": "object",
            "properties": {
                "download_url": {"type": "string"},
                "file_id": {"type": "string"},
                "mime_type": {"type": "string"},
                "file_name": {"type": "string"},
            },
            "required": ["download_url", "file_id"],
            "additionalProperties": False,
        }
    },
    "type": "object",
    "additionalProperties": False,
    "required": ["platform", "file", "role"],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "file": {"$ref": "#/$defs/OpenAIFile"},
        "role": {
            "type": "string",
            "enum": [MediaRole.IMAGE.value, MediaRole.DOCUMENT.value],
        },
    },
    "allOf": [
        {
            "if": {
                "properties": {"role": {"const": "document"}},
                "required": ["role"],
            },
            "then": {"properties": {"platform": {"const": "telegram"}}},
        }
    ],
}

SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["asset_ref", "status"],
    "properties": {
        "asset_ref": {"$ref": "#/$defs/asset_ref"},
        "status": {"type": "string", "enum": ["staging", "ready"]},
    },
}

SOCIAL_WORKSPACE_ASSET_STATUS_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["asset_ref"],
    "properties": {"asset_ref": {"$ref": "#/$defs/asset_ref"}},
}

SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": [
        "asset_ref",
        "status",
        "mime_type",
        "byte_length",
        "content_digest",
        "expires_at",
        "trust",
    ],
    "properties": {
        "asset_ref": {"$ref": "#/$defs/asset_ref"},
        "status": {"type": "string", "enum": _enum_values(AssetLifecycleStatus)},
        "mime_type": {"type": "string", "maxLength": 100},
        "byte_length": {"type": "integer", "minimum": 0, "maximum": 67108864},
        "content_digest": {"type": "string", "pattern": r"^sha256:[a-f0-9]{64}$"},
        "width": {"type": "integer", "minimum": 1, "maximum": 8192},
        "height": {"type": "integer", "minimum": 1, "maximum": 8192},
        "display_name": {"type": "string", "minLength": 1, "maxLength": 255},
        "classification": {"type": "string", "minLength": 1, "maxLength": 64},
        "expires_at": {"type": "string", "format": "date-time"},
        "error_code": {"type": "string", "pattern": r"^[a-z][a-z0-9_]{1,63}$"},
        "trust": {"const": "untrusted_external_data"},
    },
}

SOCIAL_WORKSPACE_ASSET_PREVIEW_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": ["platform", "asset_ref"],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "asset_ref": {"$ref": "#/$defs/asset_ref"},
    },
}

SOCIAL_WORKSPACE_ASSET_PREVIEW_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": _DEFS,
    "type": "object",
    "additionalProperties": False,
    "required": [
        "platform", "asset_ref", "mime_type", "byte_length", "width", "height", "trust"
    ],
    "properties": {
        "platform": {"type": "string", "enum": _enum_values(SocialPlatform)},
        "asset_ref": {"$ref": "#/$defs/asset_ref"},
        "mime_type": {"const": "image/jpeg"},
        "byte_length": {"type": "integer", "minimum": 1, "maximum": 65536},
        "width": {"type": "integer", "minimum": 1, "maximum": 768},
        "height": {"type": "integer", "minimum": 1, "maximum": 768},
        "trust": {"const": "untrusted_external_data"},
    },
}


__all__ = [
    "DIRECT_USER_AUTHORIZED_ACTIONS",
    "SOCIAL_WORKSPACE_ASSET_PREVIEW_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_ASSET_PREVIEW_SCHEMA",
    "SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA",
    "SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_ASSET_STATUS_SCHEMA",
    "SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA",
    "SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_COMMIT_SCHEMA",
    "SOCIAL_WORKSPACE_DIALOG_LIST_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA",
    "SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_ITEM_RESOLVE_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_ITEM_SEARCH_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_MCP_COMMIT_SCHEMA",
    "SOCIAL_WORKSPACE_NOTIFICATIONS_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_PREPARE_SCHEMA",
    "SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_READ_SCHEMA",
    "SOCIAL_WORKSPACE_SCOPES",
    "SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA",
    "SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_STATUS_SCHEMA",
    "SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_TARGET_GET_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA",
    "SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA",
    "SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA",
    "ActionGateHook",
    "ApprovalConsumeHook",
    "ApprovalContext",
    "ApprovalGrant",
    "AssetLifecycleStatus",
    "AssetStageRequest",
    "AuditAppendResult",
    "BudgetHook",
    "CapabilityHook",
    "ContentFeature",
    "DurableIdempotencyReservation",
    "EditorialAuthorizationBasis",
    "EditorialSampleState",
    "EditorialSampleStateHook",
    "ExecutionSafetyHooks",
    "GateDecision",
    "MediaAttachment",
    "MediaRole",
    "ReadGateHook",
    "RecursiveRedactionResult",
    "RichContent",
    "RichEntity",
    "RichEntityKind",
    "SafetyAuditEvent",
    "SafetyExecutionContext",
    "SocialAction",
    "SocialActionIntent",
    "SocialActionStatus",
    "SocialCapabilities",
    "SocialItemKind",
    "SocialPlatform",
    "SocialReactionPreset",
    "SocialReadAccess",
    "SocialReadOperation",
    "SocialReadPurpose",
    "SocialReadRequest",
    "SocialTargetKind",
    "SocialWorkspaceValidationError",
    "TargetLocator",
    "TargetLocatorKind",
    "ValidatedCommit",
    "compute_action_digest",
    "enforce_action_gates",
    "enforce_editorial_sample_gates",
    "enforce_execution_safety",
    "required_scope_for_action",
    "required_scope_for_read",
    "validate_action_status_response",
    "validate_asset_preview_request",
    "validate_asset_stage_request",
    "validate_asset_status_request",
    "validate_capabilities",
    "validate_commit_request",
    "validate_document_attachment_policy",
    "validate_editorial_sample_response",
    "validate_opaque_ref",
    "validate_prepare_request",
    "validate_read_request",
    "validate_resolved_target_preview",
    "validate_send_message_receipt",
    "validate_status_request",
]
