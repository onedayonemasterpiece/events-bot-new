from __future__ import annotations

import json
import time
from typing import Any, Mapping

from . import __version__
from .repository import RepositoryError
from .runtime_evidence import RuntimeEvidenceError
from .security import AuthContext, TTLResultCache, cache_key, redact
from .tool_catalog import ToolDefinition

LATEST_PROTOCOL = "2025-11-25"


class MCPProtocol:
    def __init__(self, tools: dict[str, ToolDefinition], cache: TTLResultCache) -> None:
        self.tools, self.cache = tools, cache

    def visible_tools(self, auth: AuthContext) -> list[dict[str, Any]]:
        return [tool.public() for tool in self.tools.values() if auth.permits(tool.name)]

    async def dispatch(self, payload: Mapping[str, Any], auth: AuthContext):
        request_id, method = payload.get("id"), payload.get("method")
        if payload.get("jsonrpc") != "2.0" or not isinstance(method, str):
            return 400, self.error(request_id, -32600, "Invalid Request")
        params = payload.get("params") or {}
        if not isinstance(params, Mapping):
            return 200, self.error(request_id, -32602, "Invalid params")
        if request_id is None:
            return 202, None
        if method == "initialize":
            requested = str(params.get("protocolVersion") or LATEST_PROTOCOL)
            selected = requested if requested in {LATEST_PROTOCOL, "2025-06-18"} else LATEST_PROTOCOL
            return 200, self.result(request_id, {
                "protocolVersion": selected, "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "events-prod-ops", "version": __version__},
                "instructions": "Read-only bounded evidence gateway; no raw SQL/live social calls/publication.",
            })
        if method == "ping":
            return 200, self.result(request_id, {})
        if method == "tools/list":
            return 200, self.result(request_id, {"tools": self.visible_tools(auth)})
        if method != "tools/call":
            return 200, self.error(request_id, -32601, "Method not found")
        name, arguments = params.get("name"), params.get("arguments") or {}
        if not isinstance(name, str) or not isinstance(arguments, Mapping):
            return 200, self.error(request_id, -32602, "Invalid tool call")
        if not auth.permits(name):
            return 200, self.tool_result(request_id,
                {"error": "tool_not_permitted", "auth_mode": auth.mode}, True)
        tool = self.tools.get(name)
        if tool is None:
            return 200, self.error(request_id, -32602, "Unknown tool")
        key = cache_key(name, arguments)
        cached = await self.cache.get(key)
        if cached is not None:
            data = dict(cached); data["cache"] = "hit"
            return 200, self.tool_result(request_id, data)
        try:
            data = await tool.handler(arguments)
        except (RepositoryError, RuntimeEvidenceError, ValueError, TypeError) as exc:
            return 200, self.tool_result(request_id,
                {"error": "bounded_read_rejected", "message": str(exc)}, True)
        envelope = {"schema_version": "events_prod_ops_mcp_v1",
            "observed_at_epoch": int(time.time()), "source_of_truth": "local_read_only",
            "provider_network_calls": 0, "cache": "miss", "data": redact(data)}
        await self.cache.put(key, envelope)
        return 200, self.tool_result(request_id, envelope)

    @staticmethod
    def result(request_id: Any, result: Any):
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    @staticmethod
    def error(request_id: Any, code: int, message: str):
        return {"jsonrpc": "2.0", "id": request_id,
            "error": {"code": code, "message": message}}

    @classmethod
    def tool_result(cls, request_id: Any, data: Any, is_error: bool = False):
        text = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return cls.result(request_id, {"content": [{"type": "text", "text": text}],
            "structuredContent": data, "isError": is_error})
