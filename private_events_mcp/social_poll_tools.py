"""Install the native-poll MCP extension without widening provider raw access."""

from __future__ import annotations

import copy
import re
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from .repository import InvalidArgumentsError
from .social_poll_contract import (
    POLL_COMMIT_INPUT_SCHEMA,
    POLL_GET_INPUT_SCHEMA,
    POLL_GET_OUTPUT_SCHEMA,
    POLL_MUTATION_OUTPUT_SCHEMA,
    POLL_PREPARE_INPUT_SCHEMA,
    POLL_PREPARE_OUTPUT_SCHEMA,
    POLL_RESULTS_INPUT_SCHEMA,
    POLL_RESULTS_OUTPUT_SCHEMA,
    POLL_STATUS_INPUT_SCHEMA,
    POLL_VOTERS_INPUT_SCHEMA,
    POLL_VOTERS_OUTPUT_SCHEMA,
    PollAction,
    PollErrorCode,
    PollValidationError,
    validate_poll_prepare_request,
)
from .social_poll_runtime import PollWorkspaceRuntime
from .tool_catalog import ToolCallContext, ToolExecutionError, ToolSpec

_INSTALLED = False


def _is_poll_prepare(arguments: Mapping[str, Any]) -> bool:
    action = arguments.get("action")
    if isinstance(action, str) and action in {value.value for value in PollAction}:
        content = arguments.get("content")
        return action.startswith("poll_") or (
            isinstance(content, Mapping) and content.get("poll") is not None
        )
    return False


def _scope_for_poll_action(platform: str, action: str) -> frozenset[str]:
    suffix = {
        PollAction.PUBLISH.value: "post:publish",
        PollAction.SCHEDULE.value: "schedule",
        PollAction.EDIT.value: "edit",
        PollAction.RESCHEDULE.value: "schedule",
        PollAction.CANCEL.value: "schedule",
        PollAction.CLOSE.value: "edit",
        PollAction.DELETE_CONTAINER.value: "delete",
    }.get(action)
    if suffix is None or platform not in {"telegram", "vk"}:
        raise InvalidArgumentsError("poll action is unavailable")
    return frozenset({f"{platform}:{suffix}"})


def _merge_any_of(first: Mapping[str, Any], second: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "anyOf": [copy.deepcopy(dict(first)), copy.deepcopy(dict(second))],
    }


def _poll_error(exc: Exception) -> Exception:
    if isinstance(exc, ToolExecutionError):
        return exc
    if isinstance(exc, PollValidationError):
        field = f" Field: {exc.field_path}." if exc.field_path else ""
        return ToolExecutionError(
            exc.error_code,
            "Poll request rejected." + field,
            retry_safe=exc.safe_to_retry,
        )
    if isinstance(exc, InvalidArgumentsError):
        return exc
    return ToolExecutionError(
        PollErrorCode.POLL_UNSUPPORTED.value,
        "Poll operation failed behind the provider boundary.",
        retry_safe=False,
    )


def _capability_output_schema(base: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(dict(base))
    properties = schema.setdefault("properties", {})
    features = properties.get("content_features")
    if isinstance(features, dict):
        items = features.get("items")
        if isinstance(items, dict) and isinstance(items.get("enum"), list):
            if "poll" not in items["enum"]:
                items["enum"].append("poll")
    properties["polls"] = {
        "type": "object",
        "additionalProperties": True,
        "required": [
            "support",
            "platform",
            "transport",
            "principal_type",
            "authorization",
            "create",
            "publish",
            "schedule",
            "lifecycle",
            "reads",
            "fields",
        ],
        "properties": {
            "support": {"type": "string", "enum": ["supported", "conditional", "unsupported"]},
            "platform": {"type": "string", "enum": ["telegram", "vk"]},
            "transport": {"type": "string"},
            "principal_type": {"type": "string"},
            "provider_api_version": {"type": ["string", "null"]},
            "authorization": {"type": "object"},
            "target": {"type": "object"},
            "create": {"type": "object"},
            "publish": {"type": "object"},
            "schedule": {"type": "object"},
            "lifecycle": {"type": "object"},
            "reads": {"type": "object"},
            "fields": {"type": "object"},
            "limits": {"type": "object"},
            "implementation": {"type": "object"},
        },
    }
    required = list(schema.get("required", []))
    if "polls" not in required:
        required.append("polls")
    schema["required"] = required
    return schema


def _runtime_for(runtime: Any) -> PollWorkspaceRuntime:
    value = getattr(runtime, "_native_poll_runtime", None)
    if isinstance(value, PollWorkspaceRuntime):
        return value
    value = PollWorkspaceRuntime(runtime)
    setattr(runtime, "_native_poll_runtime", value)
    return value


def extend_social_workspace_tools(
    runtime: Any,
    specs: tuple[ToolSpec, ...],
) -> tuple[ToolSpec, ...]:
    if not specs:
        return specs
    by_name = {spec.name: spec for spec in specs}
    required = {
        "social_capabilities",
        "social_action_prepare",
        "social_action_commit",
        "social_action_status",
    }
    if not required.issubset(by_name):
        return specs
    polls = _runtime_for(runtime)
    capability_spec = by_name["social_capabilities"]
    prepare_spec = by_name["social_action_prepare"]
    commit_spec = by_name["social_action_commit"]
    status_spec = by_name["social_action_status"]

    original_capabilities = capability_spec.handler
    original_prepare = prepare_spec.handler
    original_commit = commit_spec.handler
    original_status = status_spec.handler
    original_prepare_scope = prepare_spec.scope_selector
    original_commit_scope = commit_spec.scope_selector
    original_status_scope = status_spec.scope_selector

    def scope_options(
        suffix: str,
    ) -> tuple[frozenset[str], ...]:
        values: list[frozenset[str]] = []
        for platform in sorted(runtime.adapters):
            if platform not in {"telegram", "vk"}:
                continue
            values.append(frozenset({f"{platform}:{suffix}"}))
            family = "read" if suffix in {"read:public", "analytics", "audience", "discover"} else "publish"
            values.append(frozenset({f"{platform}:{family}"}))
        return tuple(dict.fromkeys(values))

    def poll_scope_from_preparation(ref: Any) -> frozenset[str]:
        if not isinstance(ref, str):
            raise InvalidArgumentsError("preparation_ref is required")
        with runtime.store._lock, runtime.store._connect() as conn:
            row = conn.execute(
                "SELECT platform,action FROM social_poll_preparation WHERE preparation_hash=?",
                (runtime._hash(ref),),
            ).fetchone()
        if row is None:
            raise InvalidArgumentsError("poll preparation is unknown")
        return _scope_for_poll_action(str(row["platform"]), str(row["action"]))

    def poll_scope_from_operation(ref: Any) -> frozenset[str]:
        if not isinstance(ref, str):
            raise InvalidArgumentsError("operation_ref is required")
        with runtime.store._lock, runtime.store._connect() as conn:
            row = conn.execute(
                "SELECT platform,action FROM social_poll_operation WHERE operation_hash=?",
                (runtime._hash(ref),),
            ).fetchone()
        if row is None:
            raise InvalidArgumentsError("poll operation is unknown")
        return _scope_for_poll_action(str(row["platform"]), str(row["action"]))

    def poll_scope_from_ref(arguments: Mapping[str, Any], suffix: str) -> frozenset[str]:
        ref = arguments.get("poll_ref")
        if not isinstance(ref, str):
            raise InvalidArgumentsError("poll_ref is required")
        with runtime.store._lock, runtime.store._connect() as conn:
            row = conn.execute(
                "SELECT platform FROM social_poll WHERE poll_hash=?",
                (runtime._hash(ref),),
            ).fetchone()
        if row is None:
            raise InvalidArgumentsError("poll_ref is unknown")
        return frozenset({f"{row['platform']}:{suffix}"})

    def prepare_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        if not _is_poll_prepare(arguments):
            if original_prepare_scope is None:
                return prepare_spec.scopes
            return original_prepare_scope(arguments)
        try:
            return validate_poll_prepare_request(arguments).required_scopes
        except Exception as exc:
            raise _poll_error(exc) from None

    def commit_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        if polls.is_poll_preparation(arguments.get("preparation_ref")):
            return poll_scope_from_preparation(arguments.get("preparation_ref"))
        if original_commit_scope is None:
            return commit_spec.scopes
        return original_commit_scope(arguments)

    def status_scope(arguments: Mapping[str, Any]) -> frozenset[str]:
        if polls.is_poll_preparation(arguments.get("preparation_ref")):
            return poll_scope_from_preparation(arguments.get("preparation_ref"))
        if polls.is_poll_operation(arguments.get("operation_ref")):
            return poll_scope_from_operation(arguments.get("operation_ref"))
        if original_status_scope is None:
            return status_spec.scopes
        return original_status_scope(arguments)

    async def capabilities(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        response = dict(await original_capabilities(arguments, context))
        platform = arguments.get("platform")
        if not isinstance(platform, str):
            raise InvalidArgumentsError("platform is required")
        try:
            response["polls"] = await polls.capabilities(
                platform=platform,
                target_ref=(
                    arguments.get("target_ref")
                    if isinstance(arguments.get("target_ref"), str)
                    else None
                ),
                context=context,
            )
            features = response.get("content_features")
            if isinstance(features, list) and "poll" not in features:
                features.append("poll")
                features.sort()
            return response
        except Exception as exc:
            raise _poll_error(exc) from None

    async def prepare(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        if not _is_poll_prepare(arguments):
            return await original_prepare(arguments, context)
        try:
            return await polls.prepare(arguments, context)
        except Exception as exc:
            raise _poll_error(exc) from None

    async def commit(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        if not polls.is_poll_preparation(arguments.get("preparation_ref")):
            return await original_commit(arguments, context)
        try:
            return await polls.commit(arguments, context)
        except Exception as exc:
            raise _poll_error(exc) from None

    async def status(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        if not (
            polls.is_poll_preparation(arguments.get("preparation_ref"))
            or polls.is_poll_operation(arguments.get("operation_ref"))
        ):
            return await original_status(arguments, context)
        try:
            return await polls.status(arguments, context)
        except Exception as exc:
            raise _poll_error(exc) from None

    async def poll_get(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        try:
            return await polls.get(arguments, context)
        except Exception as exc:
            raise _poll_error(exc) from None

    async def poll_results(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        try:
            return await polls.results(arguments, context)
        except Exception as exc:
            raise _poll_error(exc) from None

    async def poll_voters(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        try:
            return await polls.voters(arguments, context)
        except Exception as exc:
            raise _poll_error(exc) from None

    prepare_input = _merge_any_of(prepare_spec.input_schema, POLL_PREPARE_INPUT_SCHEMA)
    prepare_output = _merge_any_of(prepare_spec.output_schema, POLL_PREPARE_OUTPUT_SCHEMA)
    commit_input = _merge_any_of(commit_spec.input_schema, POLL_COMMIT_INPUT_SCHEMA)
    commit_output = _merge_any_of(commit_spec.output_schema, POLL_MUTATION_OUTPUT_SCHEMA)
    status_input = _merge_any_of(status_spec.input_schema, POLL_STATUS_INPUT_SCHEMA)
    status_output = _merge_any_of(status_spec.output_schema, POLL_MUTATION_OUTPUT_SCHEMA)

    replacements = {
        "social_capabilities": replace(
            capability_spec,
            output_schema=_capability_output_schema(capability_spec.output_schema),
            handler=capabilities,
        ),
        "social_action_prepare": replace(
            prepare_spec,
            description=(
                prepare_spec.description
                + " Supports native regular/quiz polls, typed Telegram/VK extensions, "
                "absolute scheduling and strict compatibility previews."
            ),
            input_schema=prepare_input,
            output_schema=prepare_output,
            handler=prepare,
            scope_selector=prepare_scope,
        ),
        "social_action_commit": replace(
            commit_spec,
            input_schema=commit_input,
            output_schema=commit_output,
            handler=commit,
            scope_selector=commit_scope,
        ),
        "social_action_status": replace(
            status_spec,
            input_schema=status_input,
            output_schema=status_output,
            handler=status,
            scope_selector=status_scope,
        ),
    }
    result = [replacements.get(spec.name, spec) for spec in specs]
    denial = capability_spec.denial_handler
    common_timeout = max(
        capability_spec.timeout_seconds or 5.0,
        runtime.provider_timeout_seconds + 2.0,
    )
    result.extend(
        [
            ToolSpec(
                "social_poll_get",
                "Get social poll",
                "Read normalized poll lifecycle and provider-synchronized state through an opaque poll_ref.",
                POLL_GET_INPUT_SCHEMA,
                POLL_GET_OUTPUT_SCHEMA,
                scopes=frozenset(),
                handler=poll_get,
                denial_handler=denial,
                scope_options=scope_options("read:public"),
                scope_selector=lambda arguments: poll_scope_from_ref(arguments, "read:public"),
                read_only=True,
                destructive=False,
                idempotent=True,
                open_world=True,
                cacheable=False,
                publicly_discoverable=False,
                timeout_seconds=common_timeout,
            ),
            ToolSpec(
                "social_poll_results",
                "Read social poll results",
                "Read normalized aggregate results; unavailable provider values remain null and incomplete.",
                POLL_RESULTS_INPUT_SCHEMA,
                POLL_RESULTS_OUTPUT_SCHEMA,
                scopes=frozenset(),
                handler=poll_results,
                denial_handler=denial,
                scope_options=scope_options("analytics"),
                scope_selector=lambda arguments: poll_scope_from_ref(arguments, "analytics"),
                read_only=True,
                destructive=False,
                idempotent=True,
                open_world=True,
                cacheable=False,
                publicly_discoverable=False,
                timeout_seconds=common_timeout,
            ),
            ToolSpec(
                "social_poll_voters",
                "Read social poll voters",
                "Read one bounded, capability-gated voter page for a public poll with privacy audit logging.",
                POLL_VOTERS_INPUT_SCHEMA,
                POLL_VOTERS_OUTPUT_SCHEMA,
                scopes=frozenset(),
                handler=poll_voters,
                denial_handler=denial,
                scope_options=scope_options("audience"),
                scope_selector=lambda arguments: poll_scope_from_ref(arguments, "audience"),
                read_only=True,
                destructive=False,
                idempotent=True,
                open_world=True,
                cacheable=False,
                publicly_discoverable=False,
                timeout_seconds=common_timeout,
            ),
        ]
    )
    return tuple(result)


def install_social_poll_extension() -> None:
    """Patch the social tool builder before the MCP server imports it."""

    global _INSTALLED
    if _INSTALLED:
        return
    from . import social_workspace_tools

    original = social_workspace_tools.build_social_workspace_tools
    if getattr(original, "__native_poll_extension__", False):
        _INSTALLED = True
        return

    def wrapped(
        runtime: Any,
        *,
        feature_policy: Mapping[str, bool] | Callable[[str], bool] | None = None,
        capability_policy: Mapping[str, bool] | Callable[[str], bool] | None = None,
    ) -> tuple[ToolSpec, ...]:
        specs = original(
            runtime,
            feature_policy=feature_policy,
            capability_policy=capability_policy,
        )
        return extend_social_workspace_tools(runtime, specs)

    setattr(wrapped, "__native_poll_extension__", True)
    social_workspace_tools.build_social_workspace_tools = wrapped
    _INSTALLED = True


__all__ = ["extend_social_workspace_tools", "install_social_poll_extension"]
