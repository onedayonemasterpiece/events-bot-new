from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

from .auth_store import (
    OAuthStateStore,
    SocialPublishBudgetError,
    SocialTicketError,
)
from .crypto import random_token
from .repository import InvalidArgumentsError, redact_and_clip_untrusted
from .tool_catalog import ToolCallContext, ToolSpec


SOCIAL_PLATFORMS = frozenset({"telegram", "vk"})
_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_-]{1,63}$")
_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9._~-]{8,128}$")
_TELEGRAM_TARGET_RE = re.compile(r"^-100[0-9]{5,20}$")
_VK_TARGET_RE = re.compile(r"^[1-9][0-9]{0,19}$")


class SocialAdapterError(RuntimeError):
    """A provider adapter failed without exposing provider details to MCP."""


@dataclass(frozen=True, slots=True)
class ResolvedTarget:
    platform: str
    alias: str
    provider_target: str


@dataclass(frozen=True, slots=True)
class SocialPost:
    post_id: str
    text: str
    published_at: str | None = None


@dataclass(frozen=True, slots=True)
class SocialReadResult:
    posts: Sequence[SocialPost]


@dataclass(frozen=True, slots=True)
class SocialPublishReceipt:
    reference: str


class SocialAdapter(Protocol):
    """Injected low-level transport; core imports no Telegram/VK libraries."""

    platform: str

    async def read_text(
        self,
        *,
        target: ResolvedTarget,
        limit: int,
    ) -> SocialReadResult: ...

    async def publish_text(
        self,
        *,
        target: ResolvedTarget,
        text: str,
        idempotency_key: str,
    ) -> SocialPublishReceipt: ...


@dataclass(frozen=True, slots=True)
class _TargetRule:
    provider_target: str
    allow_read: bool
    allow_publish: bool


class TargetAliasPolicy:
    """Explicit alias allowlist. Empty configuration denies every target."""

    def __init__(self, rules: Mapping[tuple[str, str], _TargetRule] | None = None) -> None:
        self._rules = dict(rules or {})
        serializable = {
            f"{platform}:{alias}": {
                "provider_target_hash": hashlib.sha256(
                    rule.provider_target.encode("utf-8")
                ).hexdigest(),
                "allow_read": rule.allow_read,
                "allow_publish": rule.allow_publish,
            }
            for (platform, alias), rule in sorted(self._rules.items())
        }
        self.fingerprint = hashlib.sha256(
            json.dumps(serializable, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:20]

    @classmethod
    def from_json(cls, raw: str | None) -> "TargetAliasPolicy":
        if not raw or not raw.strip():
            return cls()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("PRIVATE_EVENTS_MCP_SOCIAL_TARGETS_JSON must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("PRIVATE_EVENTS_MCP_SOCIAL_TARGETS_JSON must be an object")
        unknown_platforms = set(payload) - SOCIAL_PLATFORMS
        if unknown_platforms:
            raise ValueError("Social target policy contains an unsupported platform")
        rules: dict[tuple[str, str], _TargetRule] = {}
        for platform, aliases in payload.items():
            if not isinstance(aliases, dict):
                raise ValueError("Each social platform policy must be an object")
            for alias, raw_rule in aliases.items():
                if not isinstance(alias, str) or not _ALIAS_RE.fullmatch(alias):
                    raise ValueError("Social target alias is invalid")
                if not isinstance(raw_rule, dict) or set(raw_rule) != {
                    "provider_target",
                    "allow_read",
                    "allow_publish",
                }:
                    raise ValueError("Social target rule fields are invalid")
                provider_target = raw_rule["provider_target"]
                if not isinstance(provider_target, str):
                    raise ValueError("Social provider target must be a string")
                target_re = _TELEGRAM_TARGET_RE if platform == "telegram" else _VK_TARGET_RE
                if not target_re.fullmatch(provider_target):
                    raise ValueError("Social provider target is invalid")
                allow_read = raw_rule["allow_read"]
                allow_publish = raw_rule["allow_publish"]
                if type(allow_read) is not bool or type(allow_publish) is not bool:
                    raise ValueError("Social target permissions must be booleans")
                rules[(platform, alias)] = _TargetRule(
                    provider_target=provider_target,
                    allow_read=allow_read,
                    allow_publish=allow_publish,
                )
        return cls(rules)

    @property
    def is_empty(self) -> bool:
        return not self._rules

    def resolve(self, platform: str, alias: str, *, action: str) -> ResolvedTarget:
        if platform not in SOCIAL_PLATFORMS or not _ALIAS_RE.fullmatch(alias):
            raise InvalidArgumentsError("Target alias is not allowed")
        rule = self._rules.get((platform, alias))
        allowed = rule and (
            rule.allow_read if action == "read" else rule.allow_publish
        )
        if not allowed or rule is None:
            raise InvalidArgumentsError("Target alias is not allowed")
        return ResolvedTarget(platform, alias, rule.provider_target)


def _alias(arguments: Mapping[str, Any]) -> str:
    value = arguments.get("target_alias")
    if not isinstance(value, str) or not _ALIAS_RE.fullmatch(value):
        raise InvalidArgumentsError("target_alias must be an allowed alias")
    return value


def _platform(arguments: Mapping[str, Any]) -> str:
    value = arguments.get("platform")
    if not isinstance(value, str) or value not in SOCIAL_PLATFORMS:
        raise InvalidArgumentsError("platform must be telegram or vk")
    return value


def _text(arguments: Mapping[str, Any]) -> str:
    value = arguments.get("text")
    if not isinstance(value, str) or not value.strip():
        raise InvalidArgumentsError("text is required")
    if len(value) > 4000 or "\x00" in value:
        raise InvalidArgumentsError("text must contain at most 4000 characters")
    return value


def _idempotency_key(arguments: Mapping[str, Any]) -> str:
    value = arguments.get("idempotency_key")
    if not isinstance(value, str) or not _IDEMPOTENCY_RE.fullmatch(value):
        raise InvalidArgumentsError("idempotency_key must be 8-128 URL-safe characters")
    return value


def _ticket(arguments: Mapping[str, Any]) -> str:
    value = arguments.get("preparation_ticket")
    if not isinstance(value, str) or len(value) < 32 or len(value) > 160:
        raise InvalidArgumentsError("preparation_ticket is invalid")
    return value


def _publish_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
    return frozenset({_platform(arguments) + ":publish"})


def build_social_tools(
    *,
    store: OAuthStateStore,
    policy: TargetAliasPolicy,
    adapters: Mapping[str, SocialAdapter],
    ticket_ttl_seconds: int,
    provider_timeout_seconds: float,
    publish_attempts_per_day: int,
) -> tuple[ToolSpec, ...]:
    normalized_adapters = dict(adapters)
    if set(normalized_adapters) - SOCIAL_PLATFORMS:
        raise ValueError("Unsupported social adapter platform")
    for platform, adapter in normalized_adapters.items():
        if adapter.platform != platform:
            raise ValueError("Social adapter platform registration mismatch")

    def adapter_for(platform: str) -> SocialAdapter:
        adapter = normalized_adapters.get(platform)
        if adapter is None:
            raise InvalidArgumentsError("Social platform transport is unavailable")
        return adapter

    async def read_platform(
        platform: str,
        arguments: Mapping[str, Any],
        context: ToolCallContext,
    ) -> dict[str, Any]:
        alias = _alias(arguments)
        try:
            target = policy.resolve(platform, alias, action="read")
            adapter = adapter_for(platform)
        except (InvalidArgumentsError, ValueError):
            await asyncio.to_thread(
                store.audit_social_action,
                action="read_text",
                outcome="denied",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
            )
            raise
        raw_limit = arguments.get("limit", 10)
        if isinstance(raw_limit, bool):
            raise InvalidArgumentsError("limit must be an integer")
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError) as exc:
            raise InvalidArgumentsError("limit must be an integer") from exc
        limit = max(1, min(limit, 20))
        try:
            result = await adapter.read_text(target=target, limit=limit)
            posts = []
            for item in tuple(result.posts)[:limit]:
                if (
                    not isinstance(item, SocialPost)
                    or not isinstance(item.post_id, str)
                    or not item.post_id
                    or len(item.post_id) > 200
                    or not isinstance(item.text, str)
                ):
                    raise SocialAdapterError("Invalid social adapter response")
                if not item.text.strip():
                    continue
                safe = redact_and_clip_untrusted(
                    {
                        "post_id": item.post_id,
                        "text": item.text,
                        "published_at": item.published_at,
                    },
                    limit=6000,
                )
                posts.append(
                    {
                        **safe,
                        "trust": "untrusted_external_data",
                    }
                )
        except asyncio.CancelledError:
            await asyncio.to_thread(
                store.audit_social_action,
                action="read_text",
                outcome="cancelled",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
            )
            raise
        except Exception as exc:
            await asyncio.to_thread(
                store.audit_social_action,
                action="read_text",
                outcome="failed",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
            )
            if isinstance(exc, SocialAdapterError):
                raise
            raise SocialAdapterError("Social provider operation failed") from exc
        await asyncio.to_thread(
            store.audit_social_action,
            action="read_text",
            outcome="succeeded",
            client_id=context.identity.client_id,
            subject=context.identity.subject,
            resource=context.resource,
            platform=platform,
            target_alias=alias,
        )
        return {
            "platform": platform,
            "target_alias": alias,
            "trust": "untrusted_external_data",
            "posts": posts,
        }

    async def telegram_read(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        return await read_platform("telegram", arguments, context)

    async def vk_read(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        return await read_platform("vk", arguments, context)

    async def prepare(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        platform = _platform(arguments)
        alias = _alias(arguments)
        text = _text(arguments)
        idempotency_key = _idempotency_key(arguments)
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        try:
            policy.resolve(platform, alias, action="publish")
            adapter_for(platform)
        except (InvalidArgumentsError, ValueError):
            await asyncio.to_thread(
                store.audit_social_action,
                action="prepare_text_publication",
                outcome="denied",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
                text_hash=text_hash,
                idempotency_key=idempotency_key,
            )
            raise
        ticket = random_token(48)
        now = int(time.time())
        expires_at = now + max(60, min(int(ticket_ttl_seconds), 900))
        try:
            await asyncio.to_thread(
                store.create_preparation_ticket,
                ticket=ticket,
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
                text_hash=text_hash,
                idempotency_key=idempotency_key,
                expires_at=expires_at,
                daily_limit=publish_attempts_per_day,
                now=now,
            )
        except (SocialTicketError, SocialPublishBudgetError) as exc:
            await asyncio.to_thread(
                store.audit_social_action,
                action="prepare_text_publication",
                outcome="denied",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
                text_hash=text_hash,
                idempotency_key=idempotency_key,
                now=now,
            )
            raise InvalidArgumentsError(str(exc)) from exc
        await asyncio.to_thread(
            store.audit_social_action,
            action="prepare_text_publication",
            outcome="prepared",
            client_id=context.identity.client_id,
            subject=context.identity.subject,
            resource=context.resource,
            platform=platform,
            target_alias=alias,
            text_hash=text_hash,
            ticket=ticket,
            idempotency_key=idempotency_key,
            now=now,
        )
        return {
            "platform": platform,
            "target_alias": alias,
            "text_sha256": text_hash,
            "preparation_ticket": ticket,
            "expires_at": expires_at,
        }

    async def publish(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        platform = _platform(arguments)
        alias = _alias(arguments)
        text = _text(arguments)
        idempotency_key = _idempotency_key(arguments)
        ticket = _ticket(arguments)
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        try:
            target = policy.resolve(platform, alias, action="publish")
            adapter = adapter_for(platform)
        except (InvalidArgumentsError, ValueError):
            await asyncio.to_thread(
                store.audit_social_action,
                action="publish_prepared_text",
                outcome="denied",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
                text_hash=text_hash,
                ticket=ticket,
                idempotency_key=idempotency_key,
            )
            raise
        try:
            await asyncio.to_thread(
                store.consume_preparation_ticket,
                ticket=ticket,
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
                text_hash=text_hash,
                idempotency_key=idempotency_key,
            )
        except SocialTicketError as exc:
            await asyncio.to_thread(
                store.audit_social_action,
                action="publish_prepared_text",
                outcome="denied",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
                text_hash=text_hash,
                ticket=ticket,
                idempotency_key=idempotency_key,
            )
            raise InvalidArgumentsError(str(exc)) from exc
        try:
            receipt = await adapter.publish_text(
                target=target,
                text=text,
                idempotency_key=idempotency_key,
            )
            if not isinstance(receipt, SocialPublishReceipt) or not receipt.reference:
                raise SocialAdapterError("Invalid social adapter response")
        except asyncio.CancelledError:
            # The provider outcome is unknowable after cancellation. The ticket
            # was consumed before the call and remains non-replayable.
            await asyncio.to_thread(
                store.audit_social_action,
                action="publish_prepared_text",
                outcome="outcome_unknown",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
                text_hash=text_hash,
                ticket=ticket,
                idempotency_key=idempotency_key,
            )
            raise
        except Exception as exc:
            await asyncio.to_thread(
                store.audit_social_action,
                action="publish_prepared_text",
                outcome="failed",
                client_id=context.identity.client_id,
                subject=context.identity.subject,
                resource=context.resource,
                platform=platform,
                target_alias=alias,
                text_hash=text_hash,
                ticket=ticket,
                idempotency_key=idempotency_key,
            )
            if isinstance(exc, SocialAdapterError):
                raise
            raise SocialAdapterError("Social provider operation failed") from exc
        receipt_fingerprint = hashlib.sha256(receipt.reference.encode("utf-8")).hexdigest()[:16]
        await asyncio.to_thread(
            store.audit_social_action,
            action="publish_prepared_text",
            outcome="succeeded",
            client_id=context.identity.client_id,
            subject=context.identity.subject,
            resource=context.resource,
            platform=platform,
            target_alias=alias,
            text_hash=text_hash,
            ticket=ticket,
            idempotency_key=idempotency_key,
            receipt_reference=receipt.reference,
        )
        return {
            "published": True,
            "platform": platform,
            "target_alias": alias,
            "text_sha256": text_hash,
            "receipt_fingerprint": receipt_fingerprint,
        }

    read_input = {
        "type": "object",
        "additionalProperties": False,
        "required": ["target_alias"],
        "properties": {
            "target_alias": {"type": "string", "pattern": _ALIAS_RE.pattern},
            "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 10},
        },
    }
    publication_properties = {
        "platform": {"type": "string", "enum": sorted(SOCIAL_PLATFORMS)},
        "target_alias": {"type": "string", "pattern": _ALIAS_RE.pattern},
        "text": {"type": "string", "minLength": 1, "maxLength": 4000},
        "idempotency_key": {"type": "string", "pattern": _IDEMPOTENCY_RE.pattern},
    }
    generic_output = {"type": "object", "additionalProperties": True}
    read_output = {
        "type": "object",
        "additionalProperties": False,
        "required": ["platform", "target_alias", "trust", "posts"],
        "properties": {
            "platform": {"type": "string", "enum": sorted(SOCIAL_PLATFORMS)},
            "target_alias": {"type": "string"},
            "trust": {"type": "string", "const": "untrusted_external_data"},
            "posts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["post_id", "text", "published_at", "trust"],
                    "properties": {
                        "post_id": {"type": "string"},
                        "text": {"type": "string"},
                        "published_at": {"type": ["string", "null"]},
                        "trust": {"type": "string", "const": "untrusted_external_data"},
                    },
                },
            },
        },
    }
    publish_options = (
        frozenset({"telegram:publish"}),
        frozenset({"vk:publish"}),
    )
    return (
        ToolSpec(
            name="telegram_read",
            title="Read recent Telegram text",
            description="Read recent text from one explicitly allowlisted Telegram alias.",
            input_schema=read_input,
            output_schema=read_output,
            scopes=frozenset({"telegram:read"}),
            handler=telegram_read,
            open_world=True,
            publicly_discoverable=False,
            timeout_seconds=provider_timeout_seconds,
        ),
        ToolSpec(
            name="vk_read",
            title="Read recent VK text",
            description="Read recent text from one explicitly allowlisted VK alias.",
            input_schema=read_input,
            output_schema=read_output,
            scopes=frozenset({"vk:read"}),
            handler=vk_read,
            open_world=True,
            publicly_discoverable=False,
            timeout_seconds=provider_timeout_seconds,
        ),
        ToolSpec(
            name="prepare_text_publication",
            title="Prepare a text publication",
            description=(
                "Create a short-lived, one-use ticket bound to the exact client, resource, "
                "subject, platform, target alias, text and idempotency key. Does not publish."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "required": list(publication_properties),
                "properties": publication_properties,
            },
            output_schema=generic_output,
            scopes=frozenset(),
            scope_options=publish_options,
            scope_selector=_publish_scope,
            handler=prepare,
            read_only=False,
            idempotent=False,
            cacheable=False,
            publicly_discoverable=False,
            timeout_seconds=provider_timeout_seconds,
        ),
        ToolSpec(
            name="publish_prepared_text",
            title="Publish prepared text",
            description=(
                "Consume a matching one-use preparation ticket and publish its exact text to "
                "the allowlisted alias. A provider attempt cannot be replayed."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "required": [*publication_properties, "preparation_ticket"],
                "properties": {
                    **publication_properties,
                    "preparation_ticket": {"type": "string", "minLength": 32, "maxLength": 160},
                },
            },
            output_schema=generic_output,
            scopes=frozenset(),
            scope_options=publish_options,
            scope_selector=_publish_scope,
            handler=publish,
            read_only=False,
            destructive=True,
            idempotent=False,
            open_world=True,
            cacheable=False,
            publicly_discoverable=False,
            timeout_seconds=provider_timeout_seconds,
        ),
    )
