#!/usr/bin/env python3
"""Small in-memory relay and control UI for the Autopresenter vertical slice."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any
from uuid import uuid4

from aiohttp import web

ALLOWED_ACTIONS = frozenset({"run", "stop", "reset"})
ALLOWED_STATUSES = frozenset(
    {"idle", "running", "stopping", "completed", "error"}
)
MAX_LONG_POLL_MS = 25_000
CONTROL_FILE = Path(__file__).with_name("control") / "index.html"


def utc_iso(epoch_seconds: float | None = None) -> str:
    value = time.time() if epoch_seconds is None else epoch_seconds
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def ApiError(status: int, code: str, message: str) -> web.HTTPException:
    """Build a JSON HTTP exception while retaining aiohttp's exact status class."""

    exception_type = {
        400: web.HTTPBadRequest,
        404: web.HTTPNotFound,
        409: web.HTTPConflict,
    }.get(status, web.HTTPInternalServerError)
    return exception_type(
        content_type="application/json",
        text=json.dumps({"error": code, "message": message}, ensure_ascii=False),
    )


@dataclass(slots=True)
class Command:
    id: str
    sequence: int
    action: str
    issued_epoch: float
    expires_epoch: float
    acknowledgments: list[dict[str, Any]] = field(default_factory=list)

    def public(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "sequence": self.sequence,
            "action": self.action,
            "issued_at": utc_iso(self.issued_epoch),
            "expires_at": utc_iso(self.expires_epoch),
            "ttl_ms": max(0, round((self.expires_epoch - time.time()) * 1000)),
        }


class Relay:
    """One-process, one-session command relay with a single active agent."""

    def __init__(self, *, command_ttl_ms: int = 30_000, agent_timeout_ms: int = 30_000):
        self.command_ttl_ms = command_ttl_ms
        self.agent_timeout_ms = agent_timeout_ms
        self._condition = asyncio.Condition()
        self._next_sequence = 1
        self._commands: list[Command] = []
        self._by_id: dict[str, Command] = {}
        self._agent_id: str | None = None
        self._agent_last_seen_monotonic: float | None = None
        self._agent_last_seen_epoch: float | None = None
        self._presentation_status = "idle"
        self._detail = "Waiting for a command"
        self._current_command_id: str | None = None

    def _agent_connected(self) -> bool:
        if self._agent_id is None or self._agent_last_seen_monotonic is None:
            return False
        elapsed_ms = (time.monotonic() - self._agent_last_seen_monotonic) * 1000
        return elapsed_ms <= self.agent_timeout_ms

    def public_state(self) -> dict[str, Any]:
        connected = self._agent_connected()
        current = self._by_id.get(self._current_command_id or "")
        return {
            "status": self._presentation_status if connected else "disconnected",
            "detail": self._detail if connected else "Presenter agent is not connected",
            "agent": {
                "connected": connected,
                "id": self._agent_id,
                "last_seen_at": (
                    utc_iso(self._agent_last_seen_epoch)
                    if self._agent_last_seen_epoch is not None
                    else None
                ),
            },
            "current_command": current.public() if current else None,
            "last_sequence": self._next_sequence - 1,
        }

    async def issue(self, action: str, requested_id: str | None) -> Command:
        if action not in ALLOWED_ACTIONS:
            raise ApiError(400, "invalid_action", "action must be run, stop, or reset")
        command_id = requested_id or str(uuid4())
        if not isinstance(command_id, str) or not command_id.strip() or len(command_id) > 128:
            raise ApiError(400, "invalid_command_id", "command_id must be a non-empty string up to 128 characters")
        command_id = command_id.strip()

        async with self._condition:
            existing = self._by_id.get(command_id)
            if existing is not None:
                if existing.action != action:
                    raise ApiError(409, "idempotency_conflict", "command_id was already used for another action")
                return existing

            now = time.time()
            command = Command(
                id=command_id,
                sequence=self._next_sequence,
                action=action,
                issued_epoch=now,
                expires_epoch=now + self.command_ttl_ms / 1000,
            )
            self._next_sequence += 1
            self._commands.append(command)
            self._by_id[command.id] = command
            self._current_command_id = command.id
            if action == "run":
                self._presentation_status = "running"
                self._detail = "Scenario tomorrow-mobile queued"
            elif action == "stop":
                self._presentation_status = "stopping"
                self._detail = "Stop requested"
            else:
                self._presentation_status = "idle"
                self._detail = "Reset requested"
            self._condition.notify_all()
            return command

    async def touch_agent(self, agent_id: Any) -> None:
        if not isinstance(agent_id, str) or not agent_id.strip() or len(agent_id) > 128:
            raise ApiError(400, "invalid_agent_id", "agent_id is required and must be a short string")
        agent_id = agent_id.strip()
        async with self._condition:
            if self._agent_id not in (None, agent_id) and self._agent_connected():
                raise ApiError(409, "agent_conflict", "another presenter agent is already connected")
            self._agent_id = agent_id
            self._agent_last_seen_monotonic = time.monotonic()
            self._agent_last_seen_epoch = time.time()

    def _next_deliverable(self, after_sequence: int) -> Command | None:
        now = time.time()
        return next(
            (
                command
                for command in self._commands
                if command.sequence > after_sequence and command.expires_epoch > now
            ),
            None,
        )

    async def poll(self, agent_id: str, after_sequence: int, wait_ms: int) -> Command | None:
        await self.touch_agent(agent_id)
        deadline = time.monotonic() + wait_ms / 1000
        async with self._condition:
            while True:
                command = self._next_deliverable(after_sequence)
                if command is not None or wait_ms == 0:
                    return command
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                except TimeoutError:
                    return None
                # A long poll is also a heartbeat while the request is alive.
                self._agent_last_seen_monotonic = time.monotonic()
                self._agent_last_seen_epoch = time.time()

    async def update_agent_state(self, agent_id: str, status: Any, detail: Any = None) -> None:
        if status not in ALLOWED_STATUSES:
            raise ApiError(400, "invalid_status", "unsupported presenter status")
        await self.touch_agent(agent_id)
        async with self._condition:
            self._presentation_status = status
            self._detail = str(detail)[:500] if detail else status
            self._condition.notify_all()

    async def acknowledge(
        self,
        command_id: str,
        *,
        agent_id: str,
        sequence: Any,
        status: Any,
        detail: Any = None,
    ) -> Command:
        if status not in ALLOWED_STATUSES:
            raise ApiError(400, "invalid_status", "unsupported acknowledgment status")
        await self.touch_agent(agent_id)
        async with self._condition:
            command = self._by_id.get(command_id)
            if command is None:
                raise ApiError(404, "command_not_found", "unknown command id")
            if not isinstance(sequence, int) or sequence != command.sequence:
                raise ApiError(409, "sequence_mismatch", "ack sequence does not match command")
            acknowledgment = {
                "agent_id": agent_id,
                "sequence": sequence,
                "status": status,
                "detail": str(detail)[:500] if detail else None,
            }
            if acknowledgment not in command.acknowledgments:
                command.acknowledgments.append(acknowledgment)
            self._current_command_id = command.id
            self._presentation_status = status
            self._detail = acknowledgment["detail"] or status
            self._condition.notify_all()
            return command


async def json_body(request: web.Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except (json.JSONDecodeError, TypeError):
        raise ApiError(400, "invalid_json", "request body must be a JSON object")
    if not isinstance(body, dict):
        raise ApiError(400, "invalid_json", "request body must be a JSON object")
    return body


def relay_for(request: web.Request) -> Relay:
    return request.app[RELAY_KEY]


async def control_page(_: web.Request) -> web.FileResponse:
    return web.FileResponse(CONTROL_FILE)


async def get_state(request: web.Request) -> web.Response:
    return web.json_response({"state": relay_for(request).public_state()})


async def post_command(request: web.Request) -> web.Response:
    body = await json_body(request)
    command = await relay_for(request).issue(body.get("action"), body.get("command_id"))
    return web.json_response(
        {"command": command.public(), "state": relay_for(request).public_state()},
        status=202,
    )


async def next_command(request: web.Request) -> web.Response:
    try:
        after_sequence = int(request.query.get("after_seq", "0"))
        wait_ms = int(request.query.get("wait_ms", "20000"))
    except ValueError:
        raise ApiError(400, "invalid_poll", "after_seq and wait_ms must be integers")
    if after_sequence < 0 or not 0 <= wait_ms <= MAX_LONG_POLL_MS:
        raise ApiError(400, "invalid_poll", f"wait_ms must be 0..{MAX_LONG_POLL_MS} and after_seq non-negative")
    command = await relay_for(request).poll(
        request.query.get("agent_id", ""), after_sequence, wait_ms
    )
    return web.json_response(
        {
            "command": command.public() if command else None,
            "state": relay_for(request).public_state(),
        }
    )


async def acknowledge(request: web.Request) -> web.Response:
    body = await json_body(request)
    command = await relay_for(request).acknowledge(
        request.match_info["command_id"],
        agent_id=body.get("agent_id"),
        sequence=body.get("sequence"),
        status=body.get("status"),
        detail=body.get("detail"),
    )
    return web.json_response({"command": command.public(), "state": relay_for(request).public_state()})


async def agent_state(request: web.Request) -> web.Response:
    body = await json_body(request)
    await relay_for(request).update_agent_state(
        body.get("agent_id"), body.get("status"), body.get("detail")
    )
    return web.json_response({"state": relay_for(request).public_state()})


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "autopresenter-relay"})


async def redirect_to_control(_: web.Request) -> web.StreamResponse:
    raise web.HTTPFound("/control/")


async def options_response(_: web.Request) -> web.Response:
    return web.Response(status=204)


@web.middleware
async def cors_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
    if request.method == "OPTIONS":
        response: web.StreamResponse = web.Response(status=204)
    else:
        try:
            response = await handler(request)
        except web.HTTPException as exception:
            exception.headers["Access-Control-Allow-Origin"] = "*"
            exception.headers["Access-Control-Allow-Headers"] = "Content-Type"
            exception.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
            exception.headers["Cache-Control"] = "no-store"
            raise
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Cache-Control"] = "no-store"
    return response


RELAY_KEY: web.AppKey[Relay] = web.AppKey("relay", Relay)


def create_app(*, command_ttl_ms: int = 30_000, agent_timeout_ms: int = 30_000) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app[RELAY_KEY] = Relay(command_ttl_ms=command_ttl_ms, agent_timeout_ms=agent_timeout_ms)
    app.add_routes(
        [
            web.get("/", redirect_to_control),
            web.get("/control", redirect_to_control),
            web.get("/control/", control_page),
            web.get("/healthz", health),
            web.get("/api/state", get_state),
            web.post("/api/commands", post_command),
            web.get("/api/commands/next", next_command),
            web.post("/api/commands/{command_id}/ack", acknowledge),
            web.post("/api/state/agent", agent_state),
            web.options("/{tail:.*}", options_response),
        ]
    )
    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Autopresenter in-memory relay and control UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--command-ttl-ms", type=int, default=30_000)
    parser.add_argument("--agent-timeout-ms", type=int, default=30_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command_ttl_ms <= 0 or args.agent_timeout_ms <= 0:
        raise SystemExit("TTL and agent timeout must be positive")
    web.run_app(
        create_app(
            command_ttl_ms=args.command_ttl_ms,
            agent_timeout_ms=args.agent_timeout_ms,
        ),
        host=args.host,
        port=args.port,
        print=lambda line: print(line, flush=True),
    )


if __name__ == "__main__":
    main()
