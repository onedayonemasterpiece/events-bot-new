"""Provider-neutral native poll contract for the private Social Workspace.

This module is deliberately provider-method free. It validates a closed poll
vocabulary, keeps provider extensions typed, and preserves the existing
``social_action_prepare`` contract for non-poll actions.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .social_workspace import (
    MediaAttachment,
    RichContent,
    RichEntity,
    SocialPlatform,
    SocialWorkspaceValidationError,
    _validate_content,
    validate_opaque_ref,
)


class _StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class PollAction(_StringEnum):
    PUBLISH = "publish"
    SCHEDULE = "schedule"
    EDIT = "poll_edit"
    RESCHEDULE = "poll_reschedule"
    CANCEL = "poll_cancel"
    CLOSE = "poll_close"
    DELETE_CONTAINER = "poll_delete_container"


class PollKind(_StringEnum):
    REGULAR = "regular"
    QUIZ = "quiz"


class CompatibilityPolicy(_StringEnum):
    STRICT = "strict"
    EXPLICIT_BEST_EFFORT = "explicit_best_effort"


class PollLifecycle(_StringEnum):
    PREPARED = "prepared"
    COMMITTED = "committed"
    PROVIDER_OBJECT_CREATING = "provider_object_creating"
    PROVIDER_OBJECT_CREATED = "provider_object_created"
    QUEUED = "queued"
    PROVIDER_SCHEDULED = "provider_scheduled"
    DISPATCHING = "dispatching"
    PUBLISHED = "published"
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"
    CANCELING = "canceling"
    CANCELED = "canceled"
    FAILED = "failed"
    UNKNOWN = "unknown"
    ORPHANED = "orphaned"
    COMPENSATION_PENDING = "compensation_pending"
    CONTAINER_DELETED = "container_deleted"


class PollErrorCode(_StringEnum):
    POLL_UNSUPPORTED = "POLL_UNSUPPORTED"
    POLL_FIELD_UNSUPPORTED = "POLL_FIELD_UNSUPPORTED"
    POLL_FIELD_CONFLICT = "POLL_FIELD_CONFLICT"
    POLL_LIMIT_EXCEEDED = "POLL_LIMIT_EXCEEDED"
    POLL_AUTHORIZATION_MISSING = "POLL_AUTHORIZATION_MISSING"
    POLL_TARGET_UNSUPPORTED = "POLL_TARGET_UNSUPPORTED"
    POLL_PRINCIPAL_UNSUPPORTED = "POLL_PRINCIPAL_UNSUPPORTED"
    POLL_NOT_EDITABLE = "POLL_NOT_EDITABLE"
    POLL_ALREADY_CLOSED = "POLL_ALREADY_CLOSED"
    POLL_RESULTS_PRIVATE = "POLL_RESULTS_PRIVATE"
    POLL_VOTERS_UNAVAILABLE = "POLL_VOTERS_UNAVAILABLE"
    POLL_OPTION_NOT_FOUND = "POLL_OPTION_NOT_FOUND"
    POLL_PROVIDER_OBJECT_ORPHANED = "POLL_PROVIDER_OBJECT_ORPHANED"
    SCHEDULE_WINDOW_INVALID = "SCHEDULE_WINDOW_INVALID"
    SCHEDULE_ALREADY_DISPATCHED = "SCHEDULE_ALREADY_DISPATCHED"
    PROVIDER_OUTCOME_UNKNOWN = "PROVIDER_OUTCOME_UNKNOWN"
    PROVIDER_RECONCILIATION_REQUIRED = "PROVIDER_RECONCILIATION_REQUIRED"
    POLL_REVISION_CONFLICT = "POLL_REVISION_CONFLICT"
    POLL_PRIVACY_DENIED = "POLL_PRIVACY_DENIED"
    POLL_REFERENCE_INVALID = "POLL_REFERENCE_INVALID"


class PollValidationError(SocialWorkspaceValidationError):
    """Stable, structured and provider-safe poll contract error."""

    def __init__(
        self,
        code: PollErrorCode | str,
        message: str,
        *,
        field_path: str | None = None,
        platform: str | None = None,
        transport: str | None = None,
        provider_method: str | None = None,
        retryable: bool = False,
        safe_to_retry: bool = False,
        capability_requirement: str | None = None,
    ) -> None:
        normalized = code.value if isinstance(code, PollErrorCode) else str(code)
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", normalized):
            normalized = PollErrorCode.POLL_FIELD_CONFLICT.value
        self.error_code = normalized
        self.field_path = field_path
        self.platform = platform
        self.transport = transport
        self.provider_method = provider_method
        self.retryable = bool(retryable)
        self.safe_to_retry = bool(safe_to_retry)
        self.capability_requirement = capability_requirement
        super().__init__(message)

    def public_details(self) -> dict[str, Any]:
        return {
            "code": self.error_code,
            **({"field_path": self.field_path} if self.field_path else {}),
            **({"platform": self.platform} if self.platform else {}),
            **({"transport": self.transport} if self.transport else {}),
            **({"provider_method": self.provider_method} if self.provider_method else {}),
            "retryable": self.retryable,
            "safe_to_retry": self.safe_to_retry,
            **(
                {"capability_requirement": self.capability_requirement}
                if self.capability_requirement
                else {}
            ),
        }


_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9._~-]{8,128}$")
_POLL_REF_RE = re.compile(r"^pol_[A-Za-z0-9_-]{24,160}$")
_POLL_OPTION_REF_RE = re.compile(r"^popt_[A-Za-z0-9_-]{24,160}$")
_SCHEDULE_REF_RE = re.compile(r"^sch_[A-Za-z0-9_-]{24,160}$")
_BACKGROUND_REF_RE = re.compile(r"^pbg_[A-Za-z0-9_-]{24,160}$")
_CLIENT_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._~-]{0,63}$")
_COUNTRY_RE = re.compile(r"^(?:[A-Z]{2}|FT)$")
_REVISION_RE = re.compile(r"^prv_[a-f0-9]{32}$")


def _error(
    code: PollErrorCode,
    message: str,
    *,
    field: str | None = None,
    platform: SocialPlatform | str | None = None,
    capability: str | None = None,
) -> PollValidationError:
    return PollValidationError(
        code,
        message,
        field_path=field,
        platform=(platform.value if isinstance(platform, SocialPlatform) else platform),
        capability_requirement=capability,
    )


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            f"{field} must be an object",
            field=field,
        )
    return value


def _only(value: Mapping[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise _error(
            PollErrorCode.POLL_FIELD_UNSUPPORTED,
            f"unsupported {field} field(s): {', '.join(unknown)}",
            field=f"{field}.{unknown[0]}",
        )


def _text(
    value: Any,
    field: str,
    *,
    minimum: int = 0,
    maximum: int,
    preserve_space: bool = False,
) -> str:
    if not isinstance(value, str):
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            f"{field} must be a string",
            field=field,
        )
    clean = value if preserve_space else value.strip()
    if "\x00" in clean or not minimum <= len(clean) <= maximum:
        raise _error(
            PollErrorCode.POLL_LIMIT_EXCEEDED,
            f"{field} length is outside the supported range",
            field=field,
        )
    return clean


def _boolean(value: Any, field: str, *, default: bool | None = None) -> bool:
    if value is None and default is not None:
        return default
    if type(value) is not bool:
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            f"{field} must be boolean",
            field=field,
        )
    return value


def _integer(
    value: Any,
    field: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise _error(
            PollErrorCode.POLL_LIMIT_EXCEEDED,
            f"{field} is outside the supported range",
            field=field,
        )
    return value


def _enum(value: Any, enum_type: type[_StringEnum], field: str) -> Any:
    if not isinstance(value, str):
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            f"{field} must be a string",
            field=field,
        )
    try:
        return enum_type(value)
    except ValueError:
        raise _error(
            PollErrorCode.POLL_FIELD_UNSUPPORTED,
            f"unsupported {field}",
            field=field,
        ) from None


def validate_poll_ref(value: Any) -> str:
    if not isinstance(value, str) or not _POLL_REF_RE.fullmatch(value):
        raise _error(
            PollErrorCode.POLL_REFERENCE_INVALID,
            "poll_ref is invalid",
            field="poll_ref",
        )
    return value


def validate_poll_option_ref(value: Any) -> str:
    if not isinstance(value, str) or not _POLL_OPTION_REF_RE.fullmatch(value):
        raise _error(
            PollErrorCode.POLL_REFERENCE_INVALID,
            "poll_option_ref is invalid",
            field="poll_option_ref",
        )
    return value


def validate_background_ref(value: Any) -> str:
    if not isinstance(value, str) or not _BACKGROUND_REF_RE.fullmatch(value):
        raise _error(
            PollErrorCode.POLL_REFERENCE_INVALID,
            "background_ref is invalid",
            field="content.poll.provider_options.vk.background_ref",
        )
    return value


def _parse_rfc3339(value: Any, field: str) -> tuple[str, datetime]:
    raw = _text(value, field, minimum=1, maximum=64)
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    except ValueError:
        raise _error(
            PollErrorCode.SCHEDULE_WINDOW_INVALID,
            f"{field} must be RFC 3339",
            field=field,
        ) from None
    if parsed.tzinfo is None:
        raise _error(
            PollErrorCode.SCHEDULE_WINDOW_INVALID,
            f"{field} must include an explicit UTC offset",
            field=field,
        )
    return raw, parsed


def _timezone(value: Any) -> str:
    raw = _text(value, "timezone", minimum=1, maximum=64)
    try:
        ZoneInfo(raw)
    except ZoneInfoNotFoundError:
        raise _error(
            PollErrorCode.SCHEDULE_WINDOW_INVALID,
            "timezone must be an IANA timezone",
            field="timezone",
        ) from None
    return raw


def _rich_text(value: Any, field: str, *, minimum: int, maximum: int) -> "PollRichText":
    data = _object(value, field)
    _only(data, {"text", "entities"}, field)
    text = _text(data.get("text"), f"{field}.text", minimum=minimum, maximum=maximum, preserve_space=True)
    entities_raw = data.get("entities", [])
    try:
        parsed = _validate_content({"text": text, "entities": entities_raw, "media": []})
    except SocialWorkspaceValidationError as exc:
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            f"{field} rich text is invalid",
            field=f"{field}.entities",
        ) from exc
    return PollRichText(text=text, entities=parsed.entities)


def _optional_rich_text(
    value: Any,
    field: str,
    *,
    maximum: int,
) -> "PollRichText | None":
    if value is None:
        return None
    return _rich_text(value, field, minimum=0, maximum=maximum)


def _core_content(value: Mapping[str, Any]) -> RichContent:
    core = {key: value[key] for key in ("text", "entities", "media") if key in value}
    text_value = core.get("text", "")
    raw_media = core.get("media", [])
    raw_entities = core.get("entities", [])
    if not isinstance(text_value, str):
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "content.text must be a string",
            field="content.text",
        )
    if text_value.strip() or raw_media:
        try:
            return _validate_content(core)
        except SocialWorkspaceValidationError as exc:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "accompanying content is invalid",
                field="content",
            ) from exc
    if raw_entities not in (None, []):
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "content.entities require content.text",
            field="content.entities",
        )
    return RichContent(text=text_value, entities=(), media=())


@dataclass(frozen=True, slots=True)
class PollRichText:
    text: str
    entities: tuple[RichEntity, ...] = ()


@dataclass(frozen=True, slots=True)
class PollOptionSpec:
    client_key: str
    text: PollRichText
    media_asset_ref: str | None = None


@dataclass(frozen=True, slots=True)
class PollCloseSpec:
    at: str | None = None
    at_utc: str | None = None
    open_period_seconds: int | None = None
    initially_closed: bool = False


@dataclass(frozen=True, slots=True)
class TelegramPollOptions:
    open_answers: bool = False
    revoting_disabled: bool = False
    shuffle_answers: bool = False
    hide_results_until_close: bool = False
    subscribers_only: bool = False
    countries_iso2: tuple[str, ...] = ()
    attached_media_asset_ref: str | None = None
    solution_media_asset_ref: str | None = None


@dataclass(frozen=True, slots=True)
class VKPollOptions:
    disable_unvote: bool = False
    background_ref: str | None = None
    photo_asset_ref: str | None = None


@dataclass(frozen=True, slots=True)
class PollSpec:
    question: PollRichText
    options: tuple[PollOptionSpec, ...]
    kind: PollKind
    anonymous: bool
    multiple_answers: bool
    correct_option_client_keys: tuple[str, ...]
    explanation: PollRichText | None
    close: PollCloseSpec
    telegram: TelegramPollOptions
    vk: VKPollOptions
    compatibility_policy: CompatibilityPolicy


@dataclass(frozen=True, slots=True)
class PollActionIntent:
    platform: SocialPlatform
    action: PollAction
    idempotency_key: str
    target_ref: str | None
    poll_ref: str | None
    content: RichContent | None
    poll: PollSpec | None
    schedule_at: str | None
    schedule_at_utc: str | None
    timezone: str | None
    original_offset: str | None
    expected_revision: str | None

    @property
    def required_scopes(self) -> frozenset[str]:
        suffix = {
            PollAction.PUBLISH: "post:publish",
            PollAction.SCHEDULE: "schedule",
            PollAction.EDIT: "edit",
            PollAction.RESCHEDULE: "schedule",
            PollAction.CANCEL: "schedule",
            PollAction.CLOSE: "edit",
            PollAction.DELETE_CONTAINER: "delete",
        }[self.action]
        return frozenset({f"{self.platform.value}:{suffix}"})


def _telegram_options(value: Any) -> TelegramPollOptions:
    if value is None:
        return TelegramPollOptions()
    data = _object(value, "content.poll.provider_options.telegram")
    allowed = {
        "open_answers",
        "revoting_disabled",
        "shuffle_answers",
        "hide_results_until_close",
        "subscribers_only",
        "countries_iso2",
        "attached_media_asset_ref",
        "solution_media_asset_ref",
    }
    _only(data, allowed, "content.poll.provider_options.telegram")
    countries_raw = data.get("countries_iso2", [])
    if not isinstance(countries_raw, list) or len(countries_raw) > 12:
        raise _error(
            PollErrorCode.POLL_LIMIT_EXCEEDED,
            "Telegram country restriction supports at most 12 countries",
            field="content.poll.provider_options.telegram.countries_iso2",
        )
    countries: list[str] = []
    for index, raw in enumerate(countries_raw):
        if not isinstance(raw, str) or not _COUNTRY_RE.fullmatch(raw.upper()):
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "country code must be ISO 3166-1 alpha-2 or FT",
                field=f"content.poll.provider_options.telegram.countries_iso2[{index}]",
            )
        normalized = raw.upper()
        if normalized not in countries:
            countries.append(normalized)
    attached = data.get("attached_media_asset_ref")
    solution = data.get("solution_media_asset_ref")
    return TelegramPollOptions(
        open_answers=_boolean(data.get("open_answers"), "content.poll.provider_options.telegram.open_answers", default=False),
        revoting_disabled=_boolean(data.get("revoting_disabled"), "content.poll.provider_options.telegram.revoting_disabled", default=False),
        shuffle_answers=_boolean(data.get("shuffle_answers"), "content.poll.provider_options.telegram.shuffle_answers", default=False),
        hide_results_until_close=_boolean(data.get("hide_results_until_close"), "content.poll.provider_options.telegram.hide_results_until_close", default=False),
        subscribers_only=_boolean(data.get("subscribers_only"), "content.poll.provider_options.telegram.subscribers_only", default=False),
        countries_iso2=tuple(countries),
        attached_media_asset_ref=(validate_opaque_ref(attached, "asset") if attached is not None else None),
        solution_media_asset_ref=(validate_opaque_ref(solution, "asset") if solution is not None else None),
    )


def _vk_options(value: Any) -> VKPollOptions:
    if value is None:
        return VKPollOptions()
    data = _object(value, "content.poll.provider_options.vk")
    _only(
        data,
        {"disable_unvote", "background_ref", "photo_asset_ref"},
        "content.poll.provider_options.vk",
    )
    background = data.get("background_ref")
    photo = data.get("photo_asset_ref")
    if background is not None and photo is not None:
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "VK poll background and photo are mutually exclusive",
            field="content.poll.provider_options.vk",
        )
    return VKPollOptions(
        disable_unvote=_boolean(data.get("disable_unvote"), "content.poll.provider_options.vk.disable_unvote", default=False),
        background_ref=(validate_background_ref(background) if background is not None else None),
        photo_asset_ref=(validate_opaque_ref(photo, "asset") if photo is not None else None),
    )


def _poll_spec(value: Any, platform: SocialPlatform) -> PollSpec:
    data = _object(value, "content.poll")
    _only(
        data,
        {
            "question",
            "options",
            "kind",
            "anonymous",
            "multiple_answers",
            "correct_option_client_keys",
            "explanation",
            "close",
            "provider_options",
            "compatibility_policy",
        },
        "content.poll",
    )
    question = _rich_text(data.get("question"), "content.poll.question", minimum=1, maximum=300)
    raw_options = data.get("options")
    if not isinstance(raw_options, list) or not 1 <= len(raw_options) <= 12:
        raise _error(
            PollErrorCode.POLL_LIMIT_EXCEEDED,
            "poll must contain 1 to 12 options",
            field="content.poll.options",
        )
    options: list[PollOptionSpec] = []
    client_keys: set[str] = set()
    normalized_texts: set[str] = set()
    for index, raw in enumerate(raw_options):
        option = _object(raw, f"content.poll.options[{index}]")
        _only(option, {"client_key", "text", "media_asset_ref"}, f"content.poll.options[{index}]")
        client_key = _text(
            option.get("client_key"),
            f"content.poll.options[{index}].client_key",
            minimum=1,
            maximum=64,
        )
        if not _CLIENT_KEY_RE.fullmatch(client_key) or client_key in client_keys:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "poll option client_key must be unique and stable",
                field=f"content.poll.options[{index}].client_key",
            )
        text_value = _rich_text(
            option.get("text"),
            f"content.poll.options[{index}].text",
            minimum=1,
            maximum=100,
        )
        normalized = " ".join(text_value.text.casefold().split())
        if normalized in normalized_texts:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "duplicate poll option text is not allowed",
                field=f"content.poll.options[{index}].text.text",
            )
        media = option.get("media_asset_ref")
        options.append(
            PollOptionSpec(
                client_key=client_key,
                text=text_value,
                media_asset_ref=(validate_opaque_ref(media, "asset") if media is not None else None),
            )
        )
        client_keys.add(client_key)
        normalized_texts.add(normalized)

    kind = _enum(data.get("kind", PollKind.REGULAR.value), PollKind, "content.poll.kind")
    anonymous = _boolean(data.get("anonymous"), "content.poll.anonymous", default=True)
    multiple = _boolean(data.get("multiple_answers"), "content.poll.multiple_answers", default=False)
    correct_raw = data.get("correct_option_client_keys", [])
    if not isinstance(correct_raw, list) or any(not isinstance(item, str) for item in correct_raw):
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "correct_option_client_keys must be an array of client keys",
            field="content.poll.correct_option_client_keys",
        )
    correct = tuple(dict.fromkeys(correct_raw))
    if any(key not in client_keys for key in correct):
        raise _error(
            PollErrorCode.POLL_OPTION_NOT_FOUND,
            "correct answer references an unknown client_key",
            field="content.poll.correct_option_client_keys",
        )
    explanation = _optional_rich_text(data.get("explanation"), "content.poll.explanation", maximum=200)
    if kind is PollKind.QUIZ:
        if not correct:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "quiz requires at least one correct answer",
                field="content.poll.correct_option_client_keys",
            )
        if multiple:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "quiz cannot enable multiple_answers",
                field="content.poll.multiple_answers",
            )
    elif correct or explanation is not None:
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "regular poll cannot contain quiz-only fields",
            field="content.poll.kind",
        )

    close_raw = data.get("close", {})
    close_data = _object(close_raw, "content.poll.close")
    _only(close_data, {"at", "open_period_seconds", "initially_closed"}, "content.poll.close")
    close_at = None
    close_at_utc = None
    if close_data.get("at") is not None:
        close_at, close_dt = _parse_rfc3339(close_data.get("at"), "content.poll.close.at")
        close_at_utc = close_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    open_period = None
    if close_data.get("open_period_seconds") is not None:
        open_period = _integer(
            close_data.get("open_period_seconds"),
            "content.poll.close.open_period_seconds",
            minimum=5,
            maximum=2_628_000,
        )
    if close_at is not None and open_period is not None:
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "close.at and open_period_seconds are mutually exclusive",
            field="content.poll.close",
        )
    close = PollCloseSpec(
        at=close_at,
        at_utc=close_at_utc,
        open_period_seconds=open_period,
        initially_closed=_boolean(close_data.get("initially_closed"), "content.poll.close.initially_closed", default=False),
    )

    provider_raw = data.get("provider_options", {})
    provider = _object(provider_raw, "content.poll.provider_options")
    _only(provider, {"telegram", "vk"}, "content.poll.provider_options")
    telegram = _telegram_options(provider.get("telegram"))
    vk = _vk_options(provider.get("vk"))
    if platform is SocialPlatform.TELEGRAM and provider.get("vk") not in (None, {}):
        raise _error(
            PollErrorCode.POLL_FIELD_UNSUPPORTED,
            "VK poll options cannot be supplied for Telegram",
            field="content.poll.provider_options.vk",
            platform=platform,
        )
    if platform is SocialPlatform.VK and provider.get("telegram") not in (None, {}):
        raise _error(
            PollErrorCode.POLL_FIELD_UNSUPPORTED,
            "Telegram poll options cannot be supplied for VK",
            field="content.poll.provider_options.telegram",
            platform=platform,
        )
    if telegram.open_answers and (anonymous or kind is PollKind.QUIZ):
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "open answers require a public regular Telegram poll",
            field="content.poll.provider_options.telegram.open_answers",
            platform=platform,
        )
    if telegram.solution_media_asset_ref is not None and kind is not PollKind.QUIZ:
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "solution media is quiz-only",
            field="content.poll.provider_options.telegram.solution_media_asset_ref",
            platform=platform,
        )
    compatibility = _enum(
        data.get("compatibility_policy", CompatibilityPolicy.STRICT.value),
        CompatibilityPolicy,
        "content.poll.compatibility_policy",
    )
    return PollSpec(
        question=question,
        options=tuple(options),
        kind=kind,
        anonymous=anonymous,
        multiple_answers=multiple,
        correct_option_client_keys=correct,
        explanation=explanation,
        close=close,
        telegram=telegram,
        vk=vk,
        compatibility_policy=compatibility,
    )


def validate_poll_prepare_request(
    payload: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> PollActionIntent:
    data = _object(payload, "request")
    allowed = {
        "platform",
        "action",
        "idempotency_key",
        "target_ref",
        "poll_ref",
        "content",
        "schedule_at",
        "timezone",
        "expected_revision",
    }
    _only(data, allowed, "request")
    platform = _enum(data.get("platform"), SocialPlatform, "platform")
    action = _enum(data.get("action"), PollAction, "action")
    idempotency = _text(data.get("idempotency_key"), "idempotency_key", minimum=8, maximum=128)
    if not _IDEMPOTENCY_RE.fullmatch(idempotency):
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "idempotency_key is invalid",
            field="idempotency_key",
        )
    target_ref = validate_opaque_ref(data.get("target_ref"), "target") if data.get("target_ref") is not None else None
    poll_ref = validate_poll_ref(data.get("poll_ref")) if data.get("poll_ref") is not None else None
    expected_revision = data.get("expected_revision")
    if expected_revision is not None:
        expected_revision = _text(expected_revision, "expected_revision", minimum=1, maximum=160)
        if not _REVISION_RE.fullmatch(expected_revision):
            raise _error(
                PollErrorCode.POLL_REVISION_CONFLICT,
                "expected_revision is invalid",
                field="expected_revision",
            )

    content = None
    poll = None
    if data.get("content") is not None:
        raw_content = _object(data.get("content"), "content")
        _only(raw_content, {"text", "entities", "media", "poll"}, "content")
        content = _core_content(raw_content)
        if raw_content.get("poll") is not None:
            poll = _poll_spec(raw_content.get("poll"), platform)
    create_actions = {PollAction.PUBLISH, PollAction.SCHEDULE}
    if action in create_actions:
        if target_ref is None or poll is None:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "target_ref and content.poll are required",
                field="content.poll" if poll is None else "target_ref",
            )
        if poll_ref is not None:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "poll_ref is server-minted for poll creation",
                field="poll_ref",
            )
    else:
        if poll_ref is None:
            raise _error(
                PollErrorCode.POLL_REFERENCE_INVALID,
                "poll_ref is required for poll mutation",
                field="poll_ref",
            )
        if target_ref is not None:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "target_ref is derived from poll_ref for mutation",
                field="target_ref",
            )
        if action is PollAction.EDIT and poll is None:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "poll_edit requires content.poll",
                field="content.poll",
            )
        if action is not PollAction.EDIT and content is not None:
            raise _error(
                PollErrorCode.POLL_FIELD_CONFLICT,
                f"content is not valid for {action.value}",
                field="content",
            )

    schedule_at = None
    schedule_utc = None
    timezone_name = None
    original_offset = None
    if action in {PollAction.SCHEDULE, PollAction.RESCHEDULE}:
        if data.get("schedule_at") is None or data.get("timezone") is None:
            raise _error(
                PollErrorCode.SCHEDULE_WINDOW_INVALID,
                "schedule_at and timezone are required",
                field="schedule_at",
            )
        schedule_at, schedule_dt = _parse_rfc3339(data.get("schedule_at"), "schedule_at")
        timezone_name = _timezone(data.get("timezone"))
        zone = ZoneInfo(timezone_name)
        zoned = schedule_dt.astimezone(zone)
        if zoned.utcoffset() != schedule_dt.utcoffset():
            raise _error(
                PollErrorCode.SCHEDULE_WINDOW_INVALID,
                "schedule_at offset does not match timezone at that instant",
                field="timezone",
            )
        raw_offset = schedule_dt.strftime("%z")
        original_offset = raw_offset[:3] + ":" + raw_offset[3:]
        schedule_utc_dt = schedule_dt.astimezone(timezone.utc)
        schedule_utc = schedule_utc_dt.isoformat().replace("+00:00", "Z")
        reference_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if schedule_utc_dt <= reference_now:
            raise _error(
                PollErrorCode.SCHEDULE_WINDOW_INVALID,
                "schedule_at must be in the future",
                field="schedule_at",
            )
    elif data.get("schedule_at") is not None or data.get("timezone") is not None:
        raise _error(
            PollErrorCode.POLL_FIELD_CONFLICT,
            "schedule fields are valid only for schedule/reschedule",
            field="schedule_at",
        )

    if poll is not None and poll.close.at_utc is not None:
        close_dt = datetime.fromisoformat(poll.close.at_utc.replace("Z", "+00:00"))
        publish_dt = (
            datetime.fromisoformat(schedule_utc.replace("Z", "+00:00"))
            if schedule_utc is not None
            else (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        )
        if close_dt <= publish_dt:
            raise _error(
                PollErrorCode.SCHEDULE_WINDOW_INVALID,
                "poll close time must be after publication",
                field="content.poll.close.at",
            )

    return PollActionIntent(
        platform=platform,
        action=action,
        idempotency_key=idempotency,
        target_ref=target_ref,
        poll_ref=poll_ref,
        content=content,
        poll=poll,
        schedule_at=schedule_at,
        schedule_at_utc=schedule_utc,
        timezone=timezone_name,
        original_offset=original_offset,
        expected_revision=expected_revision,
    )


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable(asdict(value))
    return value


def poll_intent_payload(intent: PollActionIntent) -> dict[str, Any]:
    return _jsonable(intent)


def poll_intent_request(intent: PollActionIntent) -> dict[str, Any]:
    """Reconstruct the public prepare shape for encrypted durable storage."""

    request: dict[str, Any] = {
        "platform": intent.platform.value,
        "action": intent.action.value,
        "idempotency_key": intent.idempotency_key,
    }
    if intent.target_ref is not None:
        request["target_ref"] = intent.target_ref
    if intent.poll_ref is not None:
        request["poll_ref"] = intent.poll_ref
    if intent.expected_revision is not None:
        request["expected_revision"] = intent.expected_revision
    if intent.schedule_at is not None:
        request["schedule_at"] = intent.schedule_at
    if intent.timezone is not None:
        request["timezone"] = intent.timezone
    if intent.content is not None or intent.poll is not None:
        content = _jsonable(intent.content) if intent.content is not None else {
            "text": "", "entities": [], "media": []
        }
        if intent.poll is not None:
            poll = _jsonable(intent.poll)
            telegram = poll.pop("telegram")
            vk = poll.pop("vk")
            close = poll.get("close") or {}
            close.pop("at_utc", None)
            poll["provider_options"] = (
                {"telegram": telegram}
                if intent.platform is SocialPlatform.TELEGRAM
                else {"vk": vk}
            )
            content["poll"] = poll
        request["content"] = content
    return request


def poll_action_digest(intent: PollActionIntent) -> str:
    encoded = json.dumps(
        poll_intent_payload(intent),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def poll_revision(payload: Mapping[str, Any] | PollSpec) -> str:
    value = _jsonable(payload)
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "prv_" + hashlib.sha256(encoded).hexdigest()[:32]


_RICH_ENTITY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["kind", "offset", "length"],
    "properties": {
        "kind": {
            "type": "string",
            "enum": [
                "bold",
                "italic",
                "underline",
                "strikethrough",
                "spoiler",
                "code",
                "pre",
                "blockquote",
                "link",
                "mention",
                "custom_emoji",
            ],
        },
        "offset": {"type": "integer", "minimum": 0},
        "length": {"type": "integer", "minimum": 1},
        "link_target": {"type": "string", "maxLength": 2048},
        "mention_target_ref": {"type": "string", "pattern": r"^tgt_[A-Za-z0-9_-]{16,160}$"},
        "custom_emoji_asset_ref": {"type": "string", "pattern": r"^ast_[A-Za-z0-9_-]{16,160}$"},
    },
}

_RICH_TEXT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["text"],
    "properties": {
        "text": {"type": "string"},
        "entities": {"type": "array", "maxItems": 256, "items": _RICH_ENTITY_SCHEMA},
    },
}

_POLL_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["question", "options"],
    "properties": {
        "question": {**_RICH_TEXT_SCHEMA, "properties": {**_RICH_TEXT_SCHEMA["properties"], "text": {"type": "string", "minLength": 1, "maxLength": 300}}},
        "options": {
            "type": "array",
            "minItems": 1,
            "maxItems": 12,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["client_key", "text"],
                "properties": {
                    "client_key": {"type": "string", "pattern": r"^[A-Za-z0-9][A-Za-z0-9._~-]{0,63}$"},
                    "text": {**_RICH_TEXT_SCHEMA, "properties": {**_RICH_TEXT_SCHEMA["properties"], "text": {"type": "string", "minLength": 1, "maxLength": 100}}},
                    "media_asset_ref": {"type": "string", "pattern": r"^ast_[A-Za-z0-9_-]{16,160}$"},
                },
            },
        },
        "kind": {"type": "string", "enum": ["regular", "quiz"], "default": "regular"},
        "anonymous": {"type": "boolean", "default": True},
        "multiple_answers": {"type": "boolean", "default": False},
        "correct_option_client_keys": {"type": "array", "uniqueItems": True, "items": {"type": "string", "maxLength": 64}},
        "explanation": _RICH_TEXT_SCHEMA,
        "close": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "at": {"type": "string", "format": "date-time"},
                "open_period_seconds": {"type": "integer", "minimum": 5, "maximum": 2628000},
                "initially_closed": {"type": "boolean", "default": False},
            },
        },
        "provider_options": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "telegram": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "open_answers": {"type": "boolean"},
                        "revoting_disabled": {"type": "boolean"},
                        "shuffle_answers": {"type": "boolean"},
                        "hide_results_until_close": {"type": "boolean"},
                        "subscribers_only": {"type": "boolean"},
                        "countries_iso2": {"type": "array", "maxItems": 12, "items": {"type": "string", "pattern": r"^(?:[A-Z]{2}|FT)$"}},
                        "attached_media_asset_ref": {"type": "string", "pattern": r"^ast_[A-Za-z0-9_-]{16,160}$"},
                        "solution_media_asset_ref": {"type": "string", "pattern": r"^ast_[A-Za-z0-9_-]{16,160}$"},
                    },
                },
                "vk": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "disable_unvote": {"type": "boolean"},
                        "background_ref": {"type": "string", "pattern": r"^pbg_[A-Za-z0-9_-]{24,160}$"},
                        "photo_asset_ref": {"type": "string", "pattern": r"^ast_[A-Za-z0-9_-]{16,160}$"},
                    },
                },
            },
        },
        "compatibility_policy": {"type": "string", "enum": ["strict", "explicit_best_effort"], "default": "strict"},
    },
}

POLL_PREPARE_INPUT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["platform", "action", "idempotency_key"],
    "properties": {
        "platform": {"type": "string", "enum": ["telegram", "vk"]},
        "action": {"type": "string", "enum": [action.value for action in PollAction]},
        "idempotency_key": {"type": "string", "pattern": r"^[A-Za-z0-9._~-]{8,128}$"},
        "target_ref": {"type": "string", "pattern": r"^tgt_[A-Za-z0-9_-]{16,160}$"},
        "poll_ref": {"type": "string", "pattern": r"^pol_[A-Za-z0-9_-]{24,160}$"},
        "content": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "text": {"type": "string", "maxLength": 32768},
                "entities": {"type": "array", "maxItems": 256, "items": _RICH_ENTITY_SCHEMA},
                "media": {"type": "array", "maxItems": 10, "items": {"type": "object"}},
                "poll": _POLL_SCHEMA,
            },
        },
        "schedule_at": {"type": "string", "format": "date-time"},
        "timezone": {"type": "string", "minLength": 1, "maxLength": 64},
        "expected_revision": {"type": "string", "pattern": r"^prv_[a-f0-9]{32}$"},
    },
}

POLL_PREPARE_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["preparation_ref", "action", "status", "action_digest", "poll_ref", "summary", "expires_at", "required_scopes", "preview"],
    "properties": {
        "preparation_ref": {"type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"},
        "action": {"type": "string", "enum": [action.value for action in PollAction]},
        "status": {"type": "string"},
        "action_digest": {"type": "string", "pattern": r"^[a-f0-9]{64}$"},
        "poll_ref": {"type": "string", "pattern": r"^pol_[A-Za-z0-9_-]{24,160}$"},
        "schedule_ref": {"type": "string", "pattern": r"^sch_[A-Za-z0-9_-]{24,160}$"},
        "summary": {"type": "string"},
        "expires_at": {"type": "string", "format": "date-time"},
        "required_scopes": {"type": "array", "items": {"type": "string"}},
        "preview": {"type": "object"},
        "compatibility_transformations": {"type": "array", "items": {"type": "object"}},
    },
}

POLL_COMMIT_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["preparation_ref", "action_digest"],
    "properties": {
        "preparation_ref": {"type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"},
        "action_digest": {"type": "string", "pattern": r"^[a-f0-9]{64}$"},
        "approval_ref": {"type": "string"},
        "approval_receipt": {"type": "string"},
    },
}

POLL_STATUS_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "preparation_ref": {"type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"},
        "operation_ref": {"type": "string", "pattern": r"^op_[A-Za-z0-9_-]{24,160}$"},
    },
    "oneOf": [
        {"required": ["preparation_ref"]},
        {"required": ["operation_ref"]},
    ],
}

POLL_MUTATION_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
    "required": ["platform", "action", "status", "poll_ref", "retry_safe"],
    "properties": {
        "platform": {"type": "string", "enum": ["telegram", "vk"]},
        "operation_ref": {"type": "string", "pattern": r"^op_[A-Za-z0-9_-]{24,160}$"},
        "preparation_ref": {"type": "string", "pattern": r"^prep_[A-Za-z0-9_-]{24,160}$"},
        "action": {"type": "string", "enum": [action.value for action in PollAction]},
        "status": {"type": "string"},
        "poll_ref": {"type": "string", "pattern": r"^pol_[A-Za-z0-9_-]{24,160}$"},
        "item_ref": {"type": "string", "pattern": r"^itm_[A-Za-z0-9_-]{16,160}$"},
        "schedule_ref": {"type": "string", "pattern": r"^sch_[A-Za-z0-9_-]{24,160}$"},
        "revision": {"type": "string", "pattern": r"^prv_[a-f0-9]{32}$"},
        "lifecycle_state": {"type": "string"},
        "retry_safe": {"type": "boolean"},
        "error_code": {"type": ["string", "null"]},
    },
}

POLL_GET_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["poll_ref"],
    "properties": {
        "poll_ref": {"type": "string", "pattern": r"^pol_[A-Za-z0-9_-]{24,160}$"},
        "refresh": {"type": "boolean", "default": True},
    },
}

POLL_RESULTS_INPUT_SCHEMA: dict[str, Any] = POLL_GET_INPUT_SCHEMA

POLL_VOTERS_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["poll_ref"],
    "properties": {
        "poll_ref": {"type": "string", "pattern": r"^pol_[A-Za-z0-9_-]{24,160}$"},
        "poll_option_ref": {"type": "string", "pattern": r"^popt_[A-Za-z0-9_-]{24,160}$"},
        "cursor": {"type": "string", "maxLength": 512},
        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
    },
}

POLL_GET_OUTPUT_SCHEMA: dict[str, Any] = {"type": "object", "additionalProperties": True}
POLL_RESULTS_OUTPUT_SCHEMA: dict[str, Any] = {"type": "object", "additionalProperties": True}
POLL_VOTERS_OUTPUT_SCHEMA: dict[str, Any] = {"type": "object", "additionalProperties": True}


__all__ = [
    "CompatibilityPolicy",
    "POLL_COMMIT_INPUT_SCHEMA",
    "POLL_GET_INPUT_SCHEMA",
    "POLL_GET_OUTPUT_SCHEMA",
    "POLL_MUTATION_OUTPUT_SCHEMA",
    "POLL_PREPARE_INPUT_SCHEMA",
    "POLL_PREPARE_OUTPUT_SCHEMA",
    "POLL_RESULTS_INPUT_SCHEMA",
    "POLL_RESULTS_OUTPUT_SCHEMA",
    "POLL_STATUS_INPUT_SCHEMA",
    "POLL_VOTERS_INPUT_SCHEMA",
    "POLL_VOTERS_OUTPUT_SCHEMA",
    "PollAction",
    "PollActionIntent",
    "PollCloseSpec",
    "PollErrorCode",
    "PollKind",
    "PollLifecycle",
    "PollOptionSpec",
    "PollRichText",
    "PollSpec",
    "PollValidationError",
    "TelegramPollOptions",
    "VKPollOptions",
    "poll_action_digest",
    "poll_intent_payload",
    "poll_intent_request",
    "poll_revision",
    "validate_background_ref",
    "validate_poll_option_ref",
    "validate_poll_prepare_request",
    "validate_poll_ref",
]
