"""Private event image ingress: no social provider or public upload capability."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from .event_assets import EventAssetService
from .media_contract import ChatGPTFile
from .tool_catalog import ToolCallContext, ToolExecutionError, ToolSpec


def parse_event_file(value: Any) -> ChatGPTFile:
    def invalid() -> ToolExecutionError:
        return ToolExecutionError("EVENT_ASSET_INVALID", "Invalid event image descriptor.")

    if not isinstance(value, Mapping) or set(value) - {"download_url", "file_id", "mime_type", "file_name"}:
        raise invalid()
    for key, maximum in (("download_url", 4096), ("file_id", 256), ("mime_type", 100), ("file_name", 255)):
        item = value.get(key)
        if item is None and key in {"mime_type", "file_name"}:
            continue
        if (not isinstance(item, str) or not 1 <= len(item) <= maximum
                or item != item.strip() or any(ord(c) < 32 or ord(c) == 127 for c in item)):
            raise invalid()
    try:
        parsed = urlsplit(value["download_url"])
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.fragment):
            raise invalid()
        parsed.port  # Reject malformed ports before the network boundary.
    except (ValueError, KeyError):
        raise invalid() from None
    if value.get("mime_type") not in {None, "image/jpeg", "image/png", "image/webp"}:
        raise invalid()
    if any(c in (value.get("file_name") or "") for c in ("/", "\\")):
        raise invalid()
    return ChatGPTFile(**dict(value))


def build_event_asset_tools(service: EventAssetService, *, timeout_seconds: float) -> tuple[ToolSpec, ...]:
    async def stage(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        return await service.stage(parse_event_file(arguments.get("file")), context)

    async def get(arguments: Mapping[str, Any], context: ToolCallContext) -> dict[str, Any]:
        return await service.read(arguments.get("asset_ref"), context)

    output = {"type": "object", "additionalProperties": True}
    return (
        ToolSpec(
            name="event_asset_stage", title="Stage private event image",
            description="Stage an authenticated attachment privately for event creation. No event, provider upload or publication is created. References expire.",
            input_schema={"type": "object", "additionalProperties": False, "required": ["file"],
                          "properties": {"file": {"type": "object", "additionalProperties": False,
                            "required": ["download_url", "file_id"], "properties": {
                                "download_url": {"type": "string", "maxLength": 4096},
                                "file_id": {"type": "string", "maxLength": 256},
                                "mime_type": {"type": "string", "enum": ["image/jpeg", "image/png", "image/webp"]},
                                "file_name": {"type": "string", "maxLength": 255}}}}},
            output_schema=output, scopes=frozenset({"events:write"}), handler=stage,
            read_only=False, idempotent=False, open_world=True, cacheable=False,
            timeout_seconds=timeout_seconds, file_params=("file",),
        ),
        ToolSpec(
            name="event_asset_get", title="Read private event image metadata",
            description="Reverify an unexpired event image belonging to the current OAuth actor. Returns metadata, never download credentials or local paths.",
            input_schema={"type": "object", "additionalProperties": False, "required": ["asset_ref"],
                          "properties": {"asset_ref": {"type": "string", "maxLength": 164}}},
            output_schema=output, scopes=frozenset({"events:write"}), handler=get,
            cacheable=False,
        ),
    )
