#!/usr/bin/env python3
"""Small in-memory relay and control UI for the Autopresenter vertical slice."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import time
from typing import Any
from uuid import uuid4
import zipfile

from aiohttp import web

ALLOWED_ACTIONS = frozenset({"run", "scroll", "stop", "reset", "shutdown"})
SCROLL_DIRECTIONS = frozenset({"up", "down"})
DEFAULT_SCROLL_AMOUNT = 420
MIN_SCROLL_AMOUNT = 120
MAX_SCROLL_AMOUNT = 1200
ALLOWED_SCENARIOS = frozenset(
    {
        "intro-loop",
        "lecture-01",
        "lecture-02",
        "lecture-03",
        "lecture-04",
        "lecture-05",
        "lecture-06",
        "lecture-07",
        "lecture-convenience-emergence",
        "lecture-usability-measurement",
        "market-01-primary",
        "market-02-substitutes",
        "market-03-dynamics",
        "market-04-position",
        "tomorrow-mobile",
        "tomorrow-rail-like",
        "weekend-amber-artifact",
        "service-wordmark",
        "service-needs",
        "service-medallions",
        "service-medallions-desktop",
        "service-medallions-mobile",
        "service-joke",
        "service-search-concept",
        "service-search-auth-setup",
        "service-search-live",
        "service-personalization",
        "service-disruption",
        "service-taste",
        "service-feedback",
        "service-focus-group",
        "service-nps",
        "service-future-celebrity",
        "service-transport-rail",
        "service-transport-bus",
        "service-navigation-map",
        "service-social-proof",
        "service-artifacts-explained",
        "service-artifact-desktop",
        "service-laws",
        "service-keyboard-concept",
        "service-keyboard-day",
        "service-keyboard-event",
        "service-fast-find",
        "service-share-friends",
        "service-calendar-memory",
        "service-community-curator",
        "service-location-artifact",
        "service-friends-club",
        "weekend-desktop",
        "outro-qr",
    }
)
ALLOWED_STATUSES = frozenset(
    {"idle", "running", "stopping", "completed", "error", "closed"}
)
MAX_LONG_POLL_MS = 25_000
CONTROL_DIR = Path(__file__).with_name("control")
CONTROL_FILE = CONTROL_DIR / "index.html"
CONTROL_AUTH_STORAGE_FILE = CONTROL_DIR / "auth-storage.js"
CONTROL_MANIFEST_FILE = CONTROL_DIR / "manifest.webmanifest"
CONTROL_SERVICE_WORKER_FILE = CONTROL_DIR / "service-worker.js"
CONTROL_ICON_FILES = {
    "icon-192.png": CONTROL_DIR / "icons" / "icon-192.png",
    "icon-512.png": CONTROL_DIR / "icons" / "icon-512.png",
    "icon-maskable-512.png": CONTROL_DIR / "icons" / "icon-maskable-512.png",
}
DEMONSTRATOR_FILE = CONTROL_DIR / "demonstrator.html"
FIRST_TEST_DIR = Path(__file__).parents[1] / "prototype" / "first-test"
AGENT_DIR = Path(__file__).parents[1] / "agent"


def utc_iso(epoch_seconds: float | None = None) -> str:
    value = time.time() if epoch_seconds is None else epoch_seconds
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def ApiError(status: int, code: str, message: str) -> web.HTTPException:
    """Build a JSON HTTP exception while retaining aiohttp's exact status class."""

    exception_type = {
        400: web.HTTPBadRequest,
        401: web.HTTPUnauthorized,
        404: web.HTTPNotFound,
        409: web.HTTPConflict,
        503: web.HTTPServiceUnavailable,
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
    scenario: str | None
    options: dict[str, Any]
    issued_epoch: float
    expires_epoch: float
    acknowledgments: list[dict[str, Any]] = field(default_factory=list)

    def public(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "sequence": self.sequence,
            "action": self.action,
            "scenario": self.scenario,
            "options": self.options,
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
        terminally_closed = self._presentation_status == "closed"
        current = self._by_id.get(self._current_command_id or "")
        return {
            "status": (
                self._presentation_status
                if connected or terminally_closed
                else "disconnected"
            ),
            "detail": (
                self._detail
                if connected or terminally_closed
                else "Presenter agent is not connected"
            ),
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

    async def issue(
        self,
        action: str,
        requested_id: str | None,
        scenario: Any = None,
        options: Any = None,
    ) -> Command:
        if action not in ALLOWED_ACTIONS:
            raise ApiError(
                400,
                "invalid_action",
                "action must be run, scroll, stop, reset, or shutdown",
            )
        normalized_scenario: str | None = None
        normalized_options: dict[str, Any] = {}
        if action == "run":
            normalized_scenario = str(scenario or "tomorrow-mobile").strip()
            if normalized_scenario not in ALLOWED_SCENARIOS:
                raise ApiError(
                    400,
                    "invalid_scenario",
                    "unsupported explicit presenter scenario",
                )
            if options not in (None, ""):
                if not isinstance(options, dict):
                    raise ApiError(400, "invalid_options", "options must be an object")
                if normalized_scenario == "service-search-live":
                    query = str(options.get("query") or "").strip()
                    if not 2 <= len(query) <= 180:
                        raise ApiError(400, "invalid_query", "query must be 2..180 characters")
                    normalized_options["query"] = query
                elif normalized_scenario == "intro-loop":
                    start_at = str(options.get("start_at") or "").strip()
                    if start_at:
                        try:
                            datetime.fromisoformat(start_at.replace("Z", "+00:00"))
                        except ValueError:
                            raise ApiError(400, "invalid_start_at", "start_at must be ISO-8601")
                        normalized_options["start_at"] = start_at
                elif options:
                    raise ApiError(400, "unexpected_options", "this scenario accepts no options")
        elif action == "scroll":
            if scenario not in (None, ""):
                raise ApiError(
                    400,
                    "unexpected_scenario",
                    "scroll commands do not accept a scenario",
                )
            if not isinstance(options, dict):
                raise ApiError(
                    400,
                    "invalid_options",
                    "scroll options must be an object",
                )
            direction = str(options.get("direction") or "").strip().lower()
            if direction not in SCROLL_DIRECTIONS:
                raise ApiError(
                    400,
                    "invalid_scroll_direction",
                    "scroll direction must be up or down",
                )
            amount = options.get("amount", DEFAULT_SCROLL_AMOUNT)
            if (
                isinstance(amount, bool)
                or not isinstance(amount, int)
                or not MIN_SCROLL_AMOUNT <= amount <= MAX_SCROLL_AMOUNT
            ):
                raise ApiError(
                    400,
                    "invalid_scroll_amount",
                    (
                        "scroll amount must be an integer from "
                        f"{MIN_SCROLL_AMOUNT} to {MAX_SCROLL_AMOUNT}"
                    ),
                )
            extra_keys = set(options) - {"direction", "amount"}
            if extra_keys:
                raise ApiError(
                    400,
                    "unexpected_options",
                    "scroll accepts only direction and amount",
                )
            normalized_options = {"direction": direction, "amount": amount}
        elif scenario not in (None, "") or options not in (None, "", {}):
            raise ApiError(
                400,
                "unexpected_scenario",
                "scenario/options are only accepted for run commands",
            )
        command_id = requested_id or str(uuid4())
        if not isinstance(command_id, str) or not command_id.strip() or len(command_id) > 128:
            raise ApiError(400, "invalid_command_id", "command_id must be a non-empty string up to 128 characters")
        command_id = command_id.strip()

        async with self._condition:
            existing = self._by_id.get(command_id)
            if existing is not None:
                if (
                    existing.action != action
                    or existing.scenario != normalized_scenario
                    or existing.options != normalized_options
                ):
                    raise ApiError(
                        409,
                        "idempotency_conflict",
                        "command_id was already used for another command",
                    )
                return existing

            now = time.time()
            command = Command(
                id=command_id,
                sequence=self._next_sequence,
                action=action,
                scenario=normalized_scenario,
                options=normalized_options,
                issued_epoch=now,
                expires_epoch=now + self.command_ttl_ms / 1000,
            )
            self._next_sequence += 1
            self._commands.append(command)
            self._by_id[command.id] = command
            if action == "run":
                self._current_command_id = command.id
                self._presentation_status = "running"
                self._detail = f"Scenario {normalized_scenario} queued"
            elif action == "scroll":
                # A manual viewport nudge is an overlay control. Keep the active
                # scene and its status visible in the PWA while the command is
                # delivered and acknowledged.
                pass
            elif action in {"stop", "shutdown"}:
                self._current_command_id = command.id
                self._presentation_status = "stopping"
                self._detail = (
                    "Presentation shutdown requested"
                    if action == "shutdown"
                    else "Stop requested"
                )
            else:
                self._current_command_id = command.id
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
            if command.action != "scroll":
                self._current_command_id = command.id
                self._presentation_status = status
                self._detail = acknowledgment["detail"] or status
            self._condition.notify_all()
            return command


@dataclass(frozen=True, slots=True)
class SecurityConfig:
    control_token: str = ""
    agent_token: str = ""
    public_base_url: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.control_token and self.agent_token)


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


def security_for(request: web.Request) -> SecurityConfig:
    return request.app[SECURITY_KEY]


async def control_page(_: web.Request) -> web.FileResponse:
    return web.FileResponse(
        CONTROL_FILE,
        headers={"Cache-Control": "no-cache"},
    )


async def control_manifest(_: web.Request) -> web.FileResponse:
    return web.FileResponse(
        CONTROL_MANIFEST_FILE,
        headers={
            "Cache-Control": "no-cache",
            "Content-Type": "application/manifest+json",
            "X-Content-Type-Options": "nosniff",
        },
    )


async def control_auth_storage(_: web.Request) -> web.FileResponse:
    return web.FileResponse(
        CONTROL_AUTH_STORAGE_FILE,
        headers={
            "Cache-Control": "no-cache",
            "Content-Type": "text/javascript; charset=utf-8",
            "X-Content-Type-Options": "nosniff",
        },
    )


async def control_service_worker(_: web.Request) -> web.FileResponse:
    return web.FileResponse(
        CONTROL_SERVICE_WORKER_FILE,
        headers={
            "Cache-Control": "no-cache",
            "Content-Type": "text/javascript; charset=utf-8",
            "Service-Worker-Allowed": "/control/",
            "X-Content-Type-Options": "nosniff",
        },
    )


async def control_icon(request: web.Request) -> web.FileResponse:
    icon = CONTROL_ICON_FILES.get(request.match_info["name"])
    if icon is None:
        raise web.HTTPNotFound()
    return web.FileResponse(
        icon,
        headers={
            "Cache-Control": "public, max-age=31536000, immutable",
            "Content-Type": "image/png",
            "X-Content-Type-Options": "nosniff",
        },
    )


async def demonstrator_page(_: web.Request) -> web.FileResponse:
    return web.FileResponse(DEMONSTRATOR_FILE)


async def get_state(request: web.Request) -> web.Response:
    return web.json_response({"state": relay_for(request).public_state()})


async def post_command(request: web.Request) -> web.Response:
    body = await json_body(request)
    command = await relay_for(request).issue(
        body.get("action"),
        body.get("command_id"),
        body.get("scenario"),
        body.get("options"),
    )
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
    return web.json_response(
        {"ok": True, "service": "autopresenter-relay", "auth": "required"}
    )


async def redirect_to_control(_: web.Request) -> web.StreamResponse:
    raise web.HTTPFound("/control/")


async def redirect_to_demonstrator(_: web.Request) -> web.StreamResponse:
    raise web.HTTPFound("/demonstrator/")


async def options_response(_: web.Request) -> web.Response:
    return web.Response(status=204)


def request_token(request: web.Request) -> str:
    authorization = request.headers.get("Authorization", "")
    scheme, _, value = authorization.partition(" ")
    if scheme.lower() == "bearer":
        return value.strip()
    return ""


def token_matches(received: str, expected: str) -> bool:
    return bool(expected and received and hmac.compare_digest(received, expected))


def required_role(request: web.Request) -> str | None:
    path = request.path
    if path in {"/api/state", "/api/commands", "/api/download/windows-test.zip"}:
        return "control"
    if path == "/api/state/agent" or path == "/api/commands/next":
        return "agent"
    if path.startswith("/api/commands/") and path.endswith("/ack"):
        return "agent"
    return None


@web.middleware
async def auth_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
    if request.method == "OPTIONS":
        return await handler(request)

    role = required_role(request)
    security = security_for(request)
    if role and security.enabled:
        expected = (
            security.control_token if role == "control" else security.agent_token
        )
        if not token_matches(request_token(request), expected):
            raise ApiError(401, "unauthorized", f"{role} bearer token required")
    return await handler(request)


@web.middleware
async def cors_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
    if request.method == "OPTIONS":
        response: web.StreamResponse = web.Response(status=204)
    else:
        try:
            response = await handler(request)
        except web.HTTPException as exception:
            exception.headers["Access-Control-Allow-Headers"] = (
                "Content-Type, Authorization"
            )
            exception.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
            exception.headers["Cache-Control"] = "no-store"
            raise
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers.setdefault("Cache-Control", "no-store")
    return response


RELAY_KEY: web.AppKey[Relay] = web.AppKey("relay", Relay)
SECURITY_KEY: web.AppKey[SecurityConfig] = web.AppKey("security", SecurityConfig)
STATIC_SITE_KEY: web.AppKey[Path | None] = web.AppKey("static_site", Path | None)


def public_base_url(request: web.Request) -> str:
    configured = security_for(request).public_base_url.rstrip("/")
    if configured:
        return configured
    return f"{request.scheme}://{request.host}"


def windows_test_archive(request: web.Request) -> bytes:
    security = security_for(request)
    if not security.agent_token:
        raise ApiError(503, "package_unavailable", "agent token is not configured")

    config = {
        "relay_url": public_base_url(request),
        "stage_url": f"{public_base_url(request)}/internal/presenter-stage/",
        "agent_token": security.agent_token,
        "agent_id": (
            "first-test-"
            + hashlib.sha256(security.agent_token.encode("utf-8")).hexdigest()[:12]
        ),
        "release_kind": "FIRST_TEST_NOT_M3",
    }
    required_files = {
        FIRST_TEST_DIR / "START-DEMONSTRATOR.cmd": "START-DEMONSTRATOR.cmd",
        FIRST_TEST_DIR / "bootstrap.ps1": "bootstrap.ps1",
        FIRST_TEST_DIR / "SELF-TEST.cmd": "SELF-TEST.cmd",
        FIRST_TEST_DIR / "self-test.ps1": "self-test.ps1",
        FIRST_TEST_DIR / "README-FIRST-TEST.txt": "README-FIRST-TEST.txt",
        AGENT_DIR / "agent.mjs": "agent/agent.mjs",
        AGENT_DIR / "abort-utils.mjs": "agent/abort-utils.mjs",
        AGENT_DIR / "pacing.mjs": "agent/pacing.mjs",
        AGENT_DIR / "scenario-contract.mjs": "agent/scenario-contract.mjs",
        AGENT_DIR / "presentation-contract.mjs": "agent/presentation-contract.mjs",
        AGENT_DIR / "outro-contract.mjs": "agent/outro-contract.mjs",
        AGENT_DIR / "package.json": "agent/package.json",
        AGENT_DIR / "package-lock.json": "agent/package-lock.json",
    }

    def add_file(archive: zipfile.ZipFile, target: str, content: bytes) -> None:
        entry = zipfile.ZipInfo(target, date_time=(2025, 1, 1, 0, 0, 0))
        entry.compress_type = zipfile.ZIP_DEFLATED
        entry.create_system = 3
        entry.external_attr = 0o100644 << 16
        archive.writestr(entry, content)

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for source, target in required_files.items():
            add_file(archive, target, source.read_bytes())
        add_file(
            archive,
            "test-config.json",
            (json.dumps(config, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        )
    return output.getvalue()


async def download_windows_test(request: web.Request) -> web.Response:
    return web.Response(
        body=windows_test_archive(request),
        content_type="application/zip",
        headers={
            "Content-Disposition": (
                'attachment; filename="Autopresenter-First-Test-Win10-x64.zip"'
            ),
            "X-Content-Type-Options": "nosniff",
        },
    )


async def static_site(request: web.Request) -> web.StreamResponse:
    root = request.app[STATIC_SITE_KEY]
    if root is None:
        raise web.HTTPNotFound()

    requested = request.match_info.get("path", "").lstrip("/")
    candidate = (root / requested).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        raise web.HTTPNotFound()

    candidates = [candidate]
    if candidate.is_dir() or not candidate.suffix:
        candidates.insert(0, candidate / "index.html")
    for resolved in candidates:
        if resolved.is_file():
            return web.FileResponse(resolved)
    raise web.HTTPNotFound()


def create_app(
    *,
    command_ttl_ms: int = 30_000,
    agent_timeout_ms: int = 30_000,
    control_token: str = "",
    agent_token: str = "",
    public_base_url_value: str = "",
    static_site_dir: str | Path | None = None,
) -> web.Application:
    security = SecurityConfig(
        control_token=control_token.strip(),
        agent_token=agent_token.strip(),
        public_base_url=public_base_url_value.strip(),
    )
    if bool(security.control_token) != bool(security.agent_token):
        raise ValueError("control_token and agent_token must be configured together")

    static_root = Path(static_site_dir).resolve() if static_site_dir else None
    if static_root is not None and not static_root.is_dir():
        raise ValueError(f"static site directory does not exist: {static_root}")

    app = web.Application(middlewares=[cors_middleware, auth_middleware])
    app[RELAY_KEY] = Relay(command_ttl_ms=command_ttl_ms, agent_timeout_ms=agent_timeout_ms)
    app[SECURITY_KEY] = security
    app[STATIC_SITE_KEY] = static_root
    app.add_routes(
        [
            web.get("/control", redirect_to_control),
            web.get("/control/", control_page),
            web.get("/control/auth-storage.js", control_auth_storage),
            web.get("/control/manifest.webmanifest", control_manifest),
            web.get("/control/service-worker.js", control_service_worker),
            web.get("/control/icons/{name}", control_icon),
            web.get("/demonstrator", redirect_to_demonstrator),
            web.get("/demonstrator/", demonstrator_page),
            web.get("/healthz", health),
            web.get("/api/state", get_state),
            web.post("/api/commands", post_command),
            web.get("/api/commands/next", next_command),
            web.post("/api/commands/{command_id}/ack", acknowledge),
            web.post("/api/state/agent", agent_state),
            web.get("/api/download/windows-test.zip", download_windows_test),
            web.options("/{tail:.*}", options_response),
            web.get("/{path:.*}", static_site),
        ]
    )
    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Autopresenter in-memory relay and control UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--command-ttl-ms", type=int, default=30_000)
    parser.add_argument("--agent-timeout-ms", type=int, default=30_000)
    parser.add_argument(
        "--allow-unauthenticated",
        action="store_true",
        help="Development-only: allow API access without bearer tokens",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command_ttl_ms <= 0 or args.agent_timeout_ms <= 0:
        raise SystemExit("TTL and agent timeout must be positive")
    control_token = os.environ.get("AUTOPRESENTER_CONTROL_TOKEN", "")
    agent_token = os.environ.get("AUTOPRESENTER_AGENT_TOKEN", "")
    if args.host not in {"127.0.0.1", "::1", "localhost"} and not (
        control_token and agent_token
    ):
        raise SystemExit(
            "Refusing non-loopback startup without AUTOPRESENTER_CONTROL_TOKEN "
            "and AUTOPRESENTER_AGENT_TOKEN"
        )
    if args.allow_unauthenticated:
        control_token = ""
        agent_token = ""
    web.run_app(
        create_app(
            command_ttl_ms=args.command_ttl_ms,
            agent_timeout_ms=args.agent_timeout_ms,
            control_token=control_token,
            agent_token=agent_token,
            public_base_url_value=os.environ.get("AUTOPRESENTER_PUBLIC_BASE_URL", ""),
            static_site_dir=os.environ.get("AUTOPRESENTER_STATIC_SITE_DIR") or None,
        ),
        host=args.host,
        port=args.port,
        print=lambda line: print(line, flush=True),
    )


if __name__ == "__main__":
    main()
