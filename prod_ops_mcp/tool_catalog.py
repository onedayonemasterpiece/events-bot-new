from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from .repository import ReadOnlyOperationsRepository
from .runtime_evidence import RuntimeEvidenceReader
from .social_gate import SocialCapabilityGate


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[[Mapping[str, Any]], Awaitable[dict[str, Any]]]

    def public(self) -> dict[str, Any]:
        return {
            "name": self.name, "title": self.name.replace("_", " ").title(),
            "description": self.description, "inputSchema": self.input_schema,
            "annotations": {"readOnlyHint": True, "destructiveHint": False,
                "idempotentHint": True, "openWorldHint": False},
        }


def build_tools(repository: ReadOnlyOperationsRepository, runtime: RuntimeEvidenceReader,
                social: SocialCapabilityGate) -> dict[str, ToolDefinition]:
    async def health(_: Mapping[str, Any]): return await repository.health_snapshot()
    async def find(args: Mapping[str, Any]): return await repository.events_find(args)
    async def explain(args: Mapping[str, Any]): return await repository.event_explain(int(args.get("event_id") or 0))
    async def trace(args: Mapping[str, Any]): return await repository.source_trace(args)
    async def jobs(args: Mapping[str, Any]): return await repository.jobs_inspect(args)
    async def runs(args: Mapping[str, Any]): return await repository.ops_runs_inspect(args)
    async def logs(args: Mapping[str, Any]): return await runtime.trace(args)
    async def capabilities(_: Mapping[str, Any]): return social.describe()
    obj = {"type": "object", "additionalProperties": False}
    limit = {"type": "integer", "minimum": 1, "maximum": 20, "default": 10}
    return {
        "prod_health_snapshot": ToolDefinition("prod_health_snapshot",
            "Tiny local database and recent-job snapshot; no provider calls.", {**obj, "properties": {}}, health),
        "events_find": ToolDefinition("events_find",
            "Find at most 20 events with bounded filters; no full descriptions/source text.",
            {**obj, "properties": {
                "event_id": {"type": "integer", "minimum": 1},
                "query": {"type": "string", "maxLength": 120},
                "date_from": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                "date_to": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                "city": {"type": "string", "maxLength": 80},
                "source_url": {"type": "string", "maxLength": 500}, "limit": limit}}, find),
        "event_explain": ToolDefinition("event_explain",
            "Explain one event through safe fields, provenance, decisions, facts and jobs.",
            {**obj, "properties": {"event_id": {"type": "integer", "minimum": 1}},
                "required": ["event_id"]}, explain),
        "source_trace": ToolDefinition("source_trace",
            "Trace local source provenance without fetching Telegram/VK.",
            {**obj, "properties": {"event_id": {"type": "integer", "minimum": 1},
                "source_url": {"type": "string", "maxLength": 500}}}, trace),
        "jobs_inspect": ToolDefinition("jobs_inspect", "Inspect a bounded durable-outbox slice.",
            {**obj, "properties": {"event_id": {"type": "integer", "minimum": 1},
                "status": {"type": "string", "maxLength": 32}, "limit": limit}}, jobs),
        "ops_runs_inspect": ToolDefinition("ops_runs_inspect", "Inspect bounded operational runs.",
            {**obj, "properties": {"kind": {"type": "string", "maxLength": 80},
                "status": {"type": "string", "maxLength": 32}, "limit": limit}}, runs),
        "runtime_trace": ToolDefinition("runtime_trace",
            "Literal search in only the bounded tail of the active runtime log.",
            {**obj, "properties": {"needle": {"type": "string", "minLength": 3, "maxLength": 128},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
                "max_scan_bytes": {"type": "integer", "minimum": 16384,
                    "maximum": 1048576, "default": 262144}}, "required": ["needle"]}, logs),
        "social_capabilities": ToolDefinition("social_capabilities",
            "Describe fail-closed Telegram/VK/MAX policy; no provider call/publication.",
            {**obj, "properties": {}}, capabilities),
    }
