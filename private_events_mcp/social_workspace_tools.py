"""Non-public ChatGPT ToolSpecs for :mod:`social_workspace_runtime`."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from .repository import InvalidArgumentsError
from .social_workspace import (
    SOCIAL_WORKSPACE_ASSET_PREVIEW_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_PREVIEW_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_RESOLVE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_MCP_COMMIT_SCHEMA,
    SOCIAL_WORKSPACE_NOTIFICATIONS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_SCHEMA,
    SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_READ_SCHEMA,
    SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
    SocialAction,
    SocialReadAccess,
    SocialReadOperation,
    required_scope_for_action,
    validate_asset_preview_request,
    validate_asset_stage_request,
    validate_asset_status_request,
    validate_prepare_request,
    validate_read_request,
    validate_status_request,
)
from .social_workspace_runtime import SocialWorkspaceRuntime
from .tool_catalog import ToolCallContext, ToolSpec

_PLATFORMS = ("telegram", "vk")


def _read_schema(
    operation: SocialReadOperation,
    *,
    read_access_values: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    schema = copy.deepcopy(dict(SOCIAL_WORKSPACE_READ_SCHEMA))
    schema["properties"]["operation"] = {"const": operation.value}
    required = list(schema.get("required", []))
    if "operation" not in required:
        required.append("operation")
    schema["required"] = required
    if read_access_values is not None:
        schema["properties"]["read_access"] = {
            "type": "string",
            "enum": list(read_access_values),
        }
    return schema


def _thread_schema() -> dict[str, Any]:
    schema = copy.deepcopy(dict(SOCIAL_WORKSPACE_READ_SCHEMA))
    schema["properties"]["operation"] = {
        "type": "string",
        "enum": ["list_comments", "list_reactions"],
    }
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

    action_features = {
        SocialAction.SEND_MESSAGE: "dm",
        SocialAction.PUBLISH: "post",
        SocialAction.COMMENT: "post",
        SocialAction.REACTION: "post",
        SocialAction.FORWARD: "post",
        SocialAction.SCHEDULE: "post",
        SocialAction.EDIT: "edit_delete",
        SocialAction.DELETE: "edit_delete",
        SocialAction.STORY: "media_story",
    }
    enabled_actions = tuple(
        action for action, feature in action_features.items() if enabled(feature)
    )
    read_access_values = (
        ("public", "private", "dialogs")
        if enabled("private_read")
        else ("public",)
    )

    def require_platform(platform: Any) -> str:
        if not isinstance(platform, str) or platform not in enabled_platforms:
            raise InvalidArgumentsError("platform is unavailable")
        return platform

    def rejected(exc: Exception) -> InvalidArgumentsError:
        if isinstance(exc, InvalidArgumentsError):
            return exc
        return InvalidArgumentsError("social workspace request rejected")

    read_suffixes = ["discover", "read:public", "analytics", "audience"]
    if enabled("private_read"):
        read_suffixes.extend(("read:private", "read:dialogs"))
    if enabled("media_story"):
        read_suffixes.append("story:read")
    allowed_scopes = {
        f"{platform}:{suffix}"
        for platform in enabled_platforms
        for suffix in read_suffixes
    }
    if "vk" in enabled_platforms:
        allowed_scopes.add("vk:notifications:read")
    # The original ChatGPT connector uses stable provider-level scope
    # families.  Exact operation authorization is still repeated at call time
    # by MCPProtocol and never crosses read/write or provider boundaries.
    allowed_scopes.update(
        f"{platform}:{family}"
        for platform in enabled_platforms
        for family in ("read", "publish")
    )
    allowed_scopes.update(
        scope
        for platform in enabled_platforms
        for action in enabled_actions
        for scope in required_scope_for_action(platform, action)
    )
    all_scope_options = tuple(
        frozenset({scope}) for scope in sorted(allowed_scopes)
    )

    def read_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        try:
            request = validate_read_request(arguments)
            require_platform(request.platform.value)
            return request.required_scopes
        except Exception as exc:  # noqa: BLE001 - normalize untrusted request errors
            raise rejected(exc) from None

    def require_read_feature(request: Any) -> None:
        if request.read_access in {
            SocialReadAccess.PRIVATE,
            SocialReadAccess.DIALOGS,
        } and not enabled("private_read"):
            raise InvalidArgumentsError("private social reads are disabled")
        if request.operation is SocialReadOperation.LIST_STORIES and not enabled("media_story"):
            raise InvalidArgumentsError("social stories are disabled")

    def require_action_feature(request: Any) -> None:
        feature = action_features[request.action]
        if not enabled(feature):
            raise InvalidArgumentsError("social action class is disabled")
        if request.content is not None and request.content.media and not enabled("media_story"):
            raise InvalidArgumentsError("social media actions are disabled")

    def require_stored_action_feature(action_value: Any) -> SocialAction:
        try:
            action = SocialAction(str(action_value))
        except ValueError:
            raise InvalidArgumentsError("stored social action is invalid") from None
        if not enabled(action_features[action]):
            raise InvalidArgumentsError("social action class is disabled")
        return action

    def action_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        try:
            request = validate_prepare_request(arguments)
            require_platform(request.platform.value)
            return request.required_scopes
        except Exception as exc:  # noqa: BLE001 - normalize untrusted request errors
            raise rejected(exc) from None

    def capabilities_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        platform = arguments.get("platform")
        platform = require_platform(platform)
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
            require_platform(row["platform"])
            return required_scope_for_action(row["platform"], row["action"])
        except InvalidArgumentsError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalize state/validation errors
            raise rejected(exc) from None

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
        require_platform(row["platform"])
        require_stored_action_feature(row["action"])
        return required_scope_for_action(row["platform"], row["action"])

    async def denial(arguments: Mapping[str, Any], context: ToolCallContext, reason: str) -> None:
        runtime.audit_denial(
            context,
            platform=arguments.get("platform") if isinstance(arguments.get("platform"), str) else None,
            operation=str(arguments.get("operation") or "social_tool"),
            reason=reason,
            target_ref=arguments.get("target_ref") if isinstance(arguments.get("target_ref"), str) else None,
        )

    async def capabilities(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        platform = require_platform(arguments.get("platform"))
        target = arguments.get("target_ref")
        return await runtime.capabilities(target if isinstance(target, str) else None,
                                          context, platform=str(platform))

    async def read(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            request = validate_read_request(arguments)
            require_platform(request.platform.value)
            require_read_feature(request)
            return await (runtime.resolve(request, context)
                          if request.operation is SocialReadOperation.RESOLVE_TARGET
                          else runtime.read(request, context))
        except InvalidArgumentsError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalize adapter/runtime errors
            raise rejected(exc) from None

    async def targets_list(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        payload = dict(arguments)
        payload.setdefault("query", "*")
        payload["operation"] = SocialReadOperation.SEARCH_TARGETS.value
        return await read(payload, context)

    async def prepare(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            request = validate_prepare_request(arguments)
            require_platform(request.platform.value)
            require_action_feature(request)
            return await runtime.prepare(request, context)
        except Exception as exc:  # noqa: BLE001 - normalize adapter/runtime errors
            raise rejected(exc) from None

    async def commit(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            commit_scope(arguments)
            return await runtime.commit(arguments, context)
        except Exception as exc:  # noqa: BLE001 - normalize adapter/runtime errors
            raise rejected(exc) from None

    async def status(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            kind, ref = validate_status_request(arguments)
            status_scope(arguments)
            if kind == "operation":
                return await runtime.reconcile(ref, context)
            return await runtime.status(kind, ref, context)
        except Exception as exc:  # noqa: BLE001 - normalize adapter/runtime errors
            raise rejected(exc) from None

    async def asset_stage(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            if not enabled("media_story"):
                raise InvalidArgumentsError("social media actions are disabled")
            request = validate_asset_stage_request(arguments)
            require_platform(request.platform.value)
            return await runtime.stage_asset(request, context)
        except Exception as exc:  # noqa: BLE001 - normalize adapter/runtime errors
            raise rejected(exc) from None

    async def asset_status(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            ref = validate_asset_status_request(arguments)
            return await runtime.asset_status(ref, context)
        except Exception as exc:  # noqa: BLE001 - normalize adapter/runtime errors
            raise rejected(exc) from None

    async def asset_preview(arguments: Mapping[str, Any], context: ToolCallContext) -> Any:
        try:
            if not enabled("media_story"):
                raise InvalidArgumentsError("social story reads are disabled")
            platform, ref = validate_asset_preview_request(arguments)
            require_platform(platform.value)
            return await runtime.asset_preview(platform.value, ref, context)
        except Exception as exc:  # noqa: BLE001 - normalize adapter/runtime errors
            raise rejected(exc) from None

    def read_schema(operation: SocialReadOperation) -> dict[str, Any]:
        return _read_schema(
            operation,
            read_access_values=read_access_values,
        )

    thread_schema = _thread_schema()
    thread_schema["properties"]["read_access"] = {
        "type": "string",
        "enum": list(read_access_values),
    }
    analytics_schema = _analytics_schema()
    analytics_schema["properties"]["read_access"] = {
        "type": "string",
        "enum": list(read_access_values),
    }
    prepare_schema = copy.deepcopy(dict(SOCIAL_WORKSPACE_PREPARE_SCHEMA))
    prepare_schema["properties"]["platform"] = {
        "type": "string",
        "enum": list(enabled_platforms),
    }
    prepare_schema["properties"]["action"] = {
        "type": "string",
        "enum": [action.value for action in enabled_actions],
    }
    prepare_output_schema = copy.deepcopy(
        dict(SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA)
    )
    prepare_output_schema["properties"]["action"] = {
        "type": "string",
        "enum": [action.value for action in enabled_actions],
    }

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
                 read_schema(SocialReadOperation.RESOLVE_TARGET),
                 SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_item_resolve", "Resolve exact social item",
                 "Resolve one canonical VK post URL to bound item and source-target references.",
                 read_schema(SocialReadOperation.RESOLVE_ITEM),
                 SOCIAL_WORKSPACE_ITEM_RESOLVE_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_targets_search", "Search social targets",
                 "Search provider targets without exposing provider-native identifiers.",
                 read_schema(SocialReadOperation.SEARCH_TARGETS),
                 SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_targets_list", "List social targets",
                 "List bounded accessible targets as opaque references.",
                 read_schema(SocialReadOperation.SEARCH_TARGETS),
                 SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA, handler=targets_list,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_search", "Search social content",
                 "Search bounded content with mandatory untrusted-data marking.",
                 read_schema(SocialReadOperation.SEARCH_ITEMS),
                 SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_feed", "Read social feed",
                 "Read a bounded feed through an opaque target reference.",
                 read_schema(SocialReadOperation.LIST_ITEMS),
                 SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_item", "Read social item",
                 "Read one item through a bound opaque item reference.",
                 read_schema(SocialReadOperation.GET_ITEM),
                 SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_thread", "Read social thread",
                 "Read comments or reaction summaries without native identifiers.",
                 thread_schema, _combined_output(
                     SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
                     SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA,
                 ),
                 handler=read, scope_selector=read_scope, **common),
        ToolSpec("social_comment_hints_list", "List VK comment hints",
                 "Read a bounded recent VK comment/mention notification page as untrusted investigation hints.",
                 read_schema(SocialReadOperation.LIST_NOTIFICATIONS),
                 SOCIAL_WORKSPACE_NOTIFICATIONS_OUTPUT_SCHEMA,
                 handler=read, scope_selector=read_scope, **common),
        ToolSpec("social_content_stories", "Read social stories",
                 "Read a bounded stories page through provider-neutral contracts.",
                 read_schema(SocialReadOperation.LIST_STORIES),
                 SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_editorial_sample", "Sample editorial content",
                 "Read an ephemeral server-cursored editorial sample, cumulatively capped at 100.",
                 read_schema(SocialReadOperation.EDITORIAL_SAMPLE),
                 SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA, handler=read,
                 scope_selector=read_scope, **common),
        ToolSpec("social_content_analytics", "Read social analytics",
                 "Read bounded provider-neutral statistics or audience counts.",
                 analytics_schema, _combined_output(
                     SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA,
                     SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA,
                 ),
                 handler=read, scope_selector=read_scope, **common),
        ToolSpec("social_asset_stage", "Stage social asset",
                 "Ingest one authenticated ChatGPT file into a verified opaque social asset.",
                 SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA,
                 SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA, handler=asset_stage,
                 scope_selector=lambda _a: frozenset(), file_params=("file",),
                 read_only=False, idempotent=False,
                 **{
                     **common,
                     "timeout_seconds": (
                         runtime.asset_ingest_timeout_seconds
                         + runtime.provider_timeout_seconds
                         + 2.0
                     ),
                 }),
        ToolSpec("social_asset_status", "Get social asset status",
                 "Read lifecycle state for a bound opaque asset reference.",
                 SOCIAL_WORKSPACE_ASSET_STATUS_SCHEMA,
                 SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA, handler=asset_status,
                 scope_selector=lambda _a: frozenset(), **common),
        ToolSpec("social_asset_preview", "Preview social story image",
                 "Return one bounded image preview for a principal-bound story asset.",
                 SOCIAL_WORKSPACE_ASSET_PREVIEW_SCHEMA,
                 SOCIAL_WORKSPACE_ASSET_PREVIEW_OUTPUT_SCHEMA, handler=asset_preview,
                 scope_selector=lambda arguments: frozenset({
                     f"{require_platform(arguments.get('platform'))}:story:read"
                 }), **{
                     **common,
                     "timeout_seconds": max(
                         25.0, runtime.provider_timeout_seconds + 5.0
                     ),
                 }),
        ToolSpec("social_action_prepare", "Prepare social action",
                 "Persist an exact action digest for external human approval; this never executes it.",
                 prepare_schema, prepare_output_schema,
                 handler=prepare, scope_selector=action_scope,
                 read_only=False, idempotent=True, **common),
        ToolSpec("social_action_commit", "Commit approved social action",
                 "Atomically consume a server approval receipt before the sole provider attempt.",
                 SOCIAL_WORKSPACE_MCP_COMMIT_SCHEMA, _combined_output(
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
    action_tool_names = {
        "social_action_prepare",
        "social_action_commit",
        "social_action_status",
    }
    feature_tools = {
        "social_content_stories": "media_story",
        "social_asset_stage": "media_story",
        "social_asset_status": "media_story",
        "social_asset_preview": "media_story",
    }
    provider_tools = {
        "social_item_resolve": "vk",
        "social_comment_hints_list": "vk",
    }

    def options_for(
        suffixes: tuple[str, ...], *, legacy_family: str
    ) -> tuple[frozenset[str], ...]:
        values = {
            frozenset({f"{platform}:{suffix}"})
            for platform in enabled_platforms
            for suffix in suffixes
            if f"{platform}:{suffix}" in allowed_scopes
        }
        values.update(
            frozenset({f"{platform}:{legacy_family}"})
            for platform in enabled_platforms
        )
        return tuple(sorted(values, key=lambda option: sorted(option)))

    read_options = options_for(
        tuple(read_suffixes), legacy_family="read"
    )
    discovery_options = options_for(("discover",), legacy_family="read")
    notification_options = (
        frozenset({"vk:notifications:read"}),
        frozenset({"vk:read"}),
    )
    mutation_suffixes = tuple(
        sorted(
            {
                next(iter(required_scope_for_action("telegram", action))).split(
                    ":", 1
                )[1]
                for action in enabled_actions
            }
        )
    )
    mutation_options = options_for(mutation_suffixes, legacy_family="publish")
    asset_options = options_for(
        ("post:publish", "story:write"), legacy_family="publish"
    )
    scoped_options = {
        "social_capabilities": discovery_options,
        "social_target_resolve": discovery_options,
        "social_item_resolve": (
            frozenset({"vk:read:public"}),
            frozenset({"vk:read"}),
        ),
        "social_targets_search": discovery_options,
        "social_targets_list": discovery_options,
        "social_content_search": read_options,
        "social_content_feed": read_options,
        "social_content_item": read_options,
        "social_content_thread": read_options,
        "social_comment_hints_list": notification_options,
        "social_content_stories": options_for(("story:read",), legacy_family="read"),
        "social_content_editorial_sample": read_options,
        "social_content_analytics": options_for(
            ("analytics", "audience"), legacy_family="read"
        ),
        "social_asset_stage": asset_options,
        "social_asset_status": asset_options,
        "social_asset_preview": options_for(("story:read",), legacy_family="read"),
        "social_action_prepare": mutation_options,
        "social_action_commit": mutation_options,
        "social_action_status": mutation_options,
    }
    return tuple(
        replace(spec, scope_options=scoped_options[spec.name])
        for spec in specs
        if enabled(spec.name)
        and (enabled_actions or spec.name not in action_tool_names)
        and enabled(feature_tools.get(spec.name, spec.name))
        and (
            spec.name not in provider_tools
            or provider_tools[spec.name] in enabled_platforms
        )
    )


__all__ = ["build_social_workspace_tools"]
