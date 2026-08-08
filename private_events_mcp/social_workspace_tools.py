"""Non-public ChatGPT ToolSpecs for :mod:`social_workspace_runtime`."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping
from typing import Any

from .repository import InvalidArgumentsError
from .social_workspace import (
    SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_SCHEMA,
    SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_SCHEMA,
    SOCIAL_WORKSPACE_READ_SCHEMA,
    SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
    SocialReadOperation,
    required_scope_for_action,
    validate_asset_stage_request,
    validate_asset_status_request,
    validate_prepare_request,
    validate_read_request,
    validate_status_request,
)
from .social_workspace_runtime import SocialWorkspaceRuntime
from .tool_catalog import ToolCallContext, ToolSpec

_PLATFORMS = ("telegram", "vk")


def _read_schema(operation: SocialReadOperation) -> dict[str, Any]:
    schema = copy.deepcopy(dict(SOCIAL_WORKSPACE_READ_SCHEMA))
    schema["properties"]["operation"] = {"const": operation.value}
    required = list(schema.get("required", []))
    if "operation" not in required:
        required.append("operation")
    schema["required"] = required
    return schema


def _thread_schema() -> dict[str, Any]:
    schema = copy.deepcopy(dict(SOCIAL_WORKSPACE_READ_SCHEMA))
    schema["properties"]["operation"] = {"const": "list_comments"}
    required = list(schema.get("required", []))
    if "operation" not in required:
        required.append("operation")
    schema["required"] = required
    return schema


def _combined_output(*schemas: Mapping[str, Any]) -> dict[str, Any]:
    """Combine closed contract schemas while hoisting their shared definitions."""
    definitions: dict[str, Any] = {}
    alternatives = []
    for schema in schemas:
        value = copy.deepcopy(dict(schema))
        definitions.update(value.pop("$defs", {}))
        value.pop("$schema", None)
        alternatives.append(value)
    result: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "oneOf": alternatives,
    }
    if definitions:
        result["$defs"] = definitions
    return result


def _analytics_schema() -> dict[str, Any]:
    schema = copy.deepcopy(dict(SOCIAL_WORKSPACE_READ_SCHEMA))
    schema["properties"]["operation"] = {
        "type": "string", "enum": ["get_statistics", "get_audience"]
    }
    required = list(schema.get("required", []))
    if "operation" not in required:
        required.append("operation")
    schema["required"] = required
    return schema


def build_social_workspace_tools(
    runtime: SocialWorkspaceRuntime,
    *,
    feature_policy: Mapping[str, bool] | Callable[[str], bool] | None = None,
    capability_policy: Mapping[str, bool] | Callable[[str], bool] | None = None,
) -> tuple[ToolSpec, ...]:
    """Build a deterministic private catalog, omitting disabled provider features."""

    def policy_allows(policy: Mapping[str, bool] | Callable[[str], bool] | None,
                      name: str) -> bool:
        if policy is None:
            return True
        return bool(policy(name) if callable(policy) else policy.get(name, True))

    enabled_platforms = tuple(
        p for p in _PLATFORMS
        if p in runtime.adapters and policy_allows(capability_policy, p)
    )
    if not enabled_platforms:
        return ()

    def enabled(name: str) -> bool:
        return policy_allows(feature_policy, name) and policy_allows(capability_policy, name)

    all_scope_options = tuple(
        frozenset({f"{platform}:{suffix}"})
        for platform in enabled_platforms
        for suffix in (
            "discover", "read:public", "read:private", "read:dialogs", "dm:send",
            "post:publish", "edit", "delete", "forward", "reaction", "comment",
            "schedule", "story:read", "story:write", "analytics", "audience",
        )
    )

    def read_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        try:
            return validate_read_request(arguments).required_scopes
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    def action_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        try:
            return validate_prepare_request(arguments).required_scopes
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    def capabilities_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        platform = arguments.get("platform")
        if platform not in enabled_platforms:
            raise InvalidArgumentsError("platform is unavailable")
        return frozenset({f"{platform}:discover"})

    def status_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        try:
            kind, ref = validate_status_request(arguments)
            table = "social_workspace_operation" if kind == "operation" else "social_workspace_preparation"
            column = "operation_hash" if kind == "operation" else "preparation_hash"
            with runtime.store._lock, runtime.store._connect() as conn:
                row = conn.execute(
                    f"SELECT platform,action FROM {table} WHERE {column}=?",  # fixed identifiers
                    (runtime._hash(ref),),
                ).fetchone()
            if row is None:
                raise InvalidArgumentsError("status reference is unknown")
            return required_scope_for_action(row["platform"], row["action"])
        except InvalidArgumentsError:
            raise
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    def commit_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        ref = arguments.get("preparation_ref")
        if not isinstance(ref, str):
            raise InvalidArgumentsError("preparation_ref is required")
        with runtime.store._lock, runtime.store._connect() as conn:
            row = conn.execute(
                "SELECT platform,action FROM social_workspace_preparation WHERE preparation_hash=?",
                (runtime._hash(ref),),
            ).fetchone()
        if row is None:
            raise InvalidArgumentsError("preparation reference is unknown")
        return required_scope_for_action(row["platform"], row["action"])

    def asset_status_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        ref = arguments.get("asset_ref")
        if not isinstance(ref, str):
            raise InvalidArgumentsError("asset_ref is required")
        with runtime.store._lock, runtime.store._connect() as conn:
            row = conn.execute(
                "SELECT platform FROM social_workspace_ref WHERE ref_hash=? AND ref_kind='asset'",
                (runtime._hash(ref),),
            ).fetchone()
        if row is None:
            raise InvalidArgumentsError("asset reference is unknown")
        return frozenset({f"{row['platform']}:post:publish"})

    async def denial(arguments: Mapping[str, Any], context: ToolCallContext, reason: str) -> None:
        runtime.audit_denial(
            context,
            platform=arguments.get("platform") if isinstance(arguments.get("platform"), str) else None,
            operation=str(arguments.get("operation") or "social_tool"),
            reason=reason,
            target_ref=arguments.get("target_ref") if isinstance(arguments.get("target_ref"), str) else None,
        )

    async def capabilities(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        platform = arguments.get("platform")
        if platform not in enabled_platforms:
            raise InvalidArgumentsError("platform is unavailable")
        target = arguments.get("target_ref")
        return await runtime.capabilities(target if isinstance(target, str) else None,
                                          context, platform=str(platform))

    async def read(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            request = validate_read_request(arguments)
            return await (runtime.resolve(request, context)
                          if request.operation is SocialReadOperation.RESOLVE_TARGET
                          else runtime.read(request, context))
        except InvalidArgumentsError:
            raise
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    async def targets_list(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        payload = dict(arguments)
        payload.setdefault("query", "*")
        payload["operation"] = SocialReadOperation.SEARCH_TARGETS.value
        return await read(payload, context)

    async def prepare(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            return await runtime.prepare(validate_prepare_request(arguments), context)
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    async def commit(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            return await runtime.commit(arguments, context)
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    async def status(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            kind, ref = validate_status_request(arguments)
            if kind == "operation":
                return await runtime.reconcile(ref, context)
            return await runtime.status(kind, ref, context)
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    async def asset_stage(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            return await runtime.stage_asset(validate_asset_stage_request(arguments), context)
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    async def asset_status(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            ref = validate_asset_status_request(arguments)
            return await runtime.asset_status(ref, context)
        except Exception as exc:
            raise InvalidArgumentsError(str(exc)) from exc

    capability_schema = {
        "type": "object", "additionalProperties": False,
        "required": ["platform"],
        "properties": {
            "platform": {"type": "string", "enum": list(enabled_platforms)},
            "target_ref": {"type": "string", "pattern": r"^tgt_[A-Za-z0-9_-]{16,160}$"},
        },
    }
    common = {
        "scopes": frozenset(),
        "scope_options": all_scope_options,
        "denial_handler": denial,
        "publicly_discoverable": False,
        "cacheable": False,
        "open_world": True,
        "timeout_seconds": runtime.provider_timeout_seconds,
    }
    specs = [
        ToolSpec("social_capabilities", "Social capabilities",
                 "Inspect the capability-gated provider-neutral social surface.",
                 capability_schema, SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA,
                 handler=capabilities, scope_selector=capabilities_scope, **common),
        ToolSpec("social_target_resolve", "Resolve exact social target",
                 "Resolve self or one exact person to a bound opaque target reference.",
                 _read_schema(SocialReadOperation.RESOLVE_TARGET),
                 SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_targets_search", "Search social targets",
                 "Search provider targets without exposing provider-native identifiers.",
                 _read_schema(SocialReadOperation.SEARCH_TARGETS),
                 SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_targets_list", "List social targets",
                 "List bounded accessible targets as opaque references.",
                 _read_schema(SocialReadOperation.SEARCH_TARGETS),
                 SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA, handler=targets_list,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_search", "Search social content",
                 "Search bounded content with mandatory untrusted-data marking.",
                 _read_schema(SocialReadOperation.SEARCH_ITEMS),
                 SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_feed", "Read social feed",
                 "Read a bounded feed through an opaque target reference.",
                 _read_schema(SocialReadOperation.LIST_ITEMS),
                 SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_item", "Read social item",
                 "Read one item through a bound opaque item reference.",
                 _read_schema(SocialReadOperation.GET_ITEM),
                 SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_thread", "Read social thread",
                 "Read comments or reaction summaries without native identifiers.",
                 _thread_schema(), SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
                 handler=read, scope_selector=read_scope, **common),
        ToolSpec("social_content_stories", "Read social stories",
                 "Read a bounded stories page through provider-neutral contracts.",
                 _read_schema(SocialReadOperation.LIST_STORIES),
                 SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_editorial_sample", "Sample editorial content",
                 "Read an ephemeral server-cursored editorial sample, cumulatively capped at 100.",
                 _read_schema(SocialReadOperation.EDITORIAL_SAMPLE),
                 SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_analytics", "Read social analytics",
                 "Read bounded provider-neutral statistics or audience counts.",
                 _analytics_schema(), _combined_output(
                     SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA,
                     SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA,
                 ),
                 handler=read, scope_selector=read_scope, **common),
        ToolSpec("social_asset_stage", "Stage social asset",
                 "Bind an accepted upload handle to an opaque asset reference.",
                 SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA,
                 SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA, handler=asset_stage,
                 scope_selector=lambda a: frozenset({f"{a.get('platform')}:post:publish"}),
                 read_only=False, idempotent=True, **common),
        ToolSpec("social_asset_status", "Get social asset status",
                 "Read lifecycle state for a bound opaque asset reference.",
                 {"type": "object", "additionalProperties": False,
                  "required": ["asset_ref"],
                  "properties": {"asset_ref": {"type": "string", "pattern": r"^ast_[A-Za-z0-9_-]{16,160}$"}}},
                 SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA, handler=asset_status,
                 scope_selector=asset_status_scope, **common),
        ToolSpec("social_action_prepare", "Prepare social action",
                 "Persist an exact action digest for external human approval; this never executes it.",
                 SOCIAL_WORKSPACE_PREPARE_SCHEMA, SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA,
                 handler=prepare, scope_selector=action_scope,
                 read_only=False, idempotent=True, **common),
        ToolSpec("social_action_commit", "Commit approved social action",
                 "Atomically consume a server approval receipt before the sole provider attempt.",
                 SOCIAL_WORKSPACE_COMMIT_SCHEMA, _combined_output(
                     SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA,
                     SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
                 ),
                 handler=commit, scope_selector=commit_scope,
                 read_only=False, destructive=True, idempotent=False, **common),
        ToolSpec("social_action_status", "Get social action status",
                 "Read or reconcile durable action state; unknown outcomes are never retry-safe.",
                 SOCIAL_WORKSPACE_STATUS_SCHEMA, SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
                 handler=status, scope_selector=status_scope, **common),
    ]
    return tuple(spec for spec in specs if enabled(spec.name))


__all__ = ["build_social_workspace_tools"]
