from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from typing import Mapping

from aiohttp import web

from .config import OpsMCPConfig
from .mcp_protocol import LATEST_PROTOCOL, MCPProtocol
from .repository import ReadOnlyOperationsRepository
from .runtime_evidence import RuntimeEvidenceReader
from .security import AdmissionController, AdmissionError, AuthError, TTLResultCache, authenticate
from .social_gate import SocialCapabilityGate
from .tool_catalog import build_tools

logger = logging.getLogger(__name__)


class OperationsMCPServer:
    def __init__(self, config: OpsMCPConfig, repository: ReadOnlyOperationsRepository,
                 social: SocialCapabilityGate | None = None,
                 runtime: RuntimeEvidenceReader | None = None) -> None:
        self.config = config
        self.admission = AdmissionController(config)
        self.protocol = MCPProtocol(
            build_tools(
                repository,
                runtime or RuntimeEvidenceReader(),
                social or SocialCapabilityGate.from_env(),
            ),
            TTLResultCache(config.cache_ttl_seconds),
        )

    async def handle_post(self, request: web.Request) -> web.StreamResponse:
        started, correlation = time.monotonic(), uuid.uuid4().hex
        auth_mode, tool_name = "rejected", ""
        try:
            await self.admission.admit_ingress()
            auth = authenticate(request.headers, self.config); auth_mode = auth.mode
            if request.content_length is not None and request.content_length > self.config.max_request_bytes:
                return self.plain_error(413, "request_too_large")
            if (request.content_type or "").lower() != "application/json":
                return self.plain_error(415, "application_json_required")
            accept = request.headers.get("Accept", "application/json")
            if "application/json" not in accept and "*/*" not in accept:
                return self.plain_error(406, "application_json_not_accepted")
            async with self.admission.request_slot(auth.mode):
                body = await request.read()
                if len(body) > self.config.max_request_bytes:
                    return self.plain_error(413, "request_too_large")
                try:
                    payload = json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    return self.plain_error(400, "invalid_json")
                if not isinstance(payload, Mapping):
                    return self.plain_error(400, "json_object_required")
                params = payload.get("params")
                if isinstance(params, Mapping): tool_name = str(params.get("name") or "")
                status, response_payload = await self.protocol.dispatch(payload, auth)
                if response_payload is None:
                    return web.Response(status=status, headers=self.headers(correlation))
                encoded = json.dumps(response_payload, ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")).encode()
                if len(encoded) > self.config.max_response_bytes:
                    encoded = json.dumps(self.protocol.error(payload.get("id"), -32001,
                        "Bounded response exceeded; narrow the query"), separators=(",", ":")).encode()
                if not await self.admission.reserve_egress(auth.mode, len(encoded)):
                    return self.plain_error(429, "hourly_egress_budget_exhausted")
                response = web.Response(status=status, body=encoded, content_type="application/json",
                    headers=self.headers(correlation))
                if len(encoded) >= 2048: response.enable_compression()
                return response
        except web.HTTPRequestEntityTooLarge:
            return self.plain_error(413, "request_too_large")
        except AuthError as exc:
            response = self.plain_error(%xc.status, exc.code)
            if exc.status == 401: response.headers["WWW-Authenticate"] = 'Bearer realm="events-prod-ops"'
            return response
        except AdmissionError as exc:
            status = 429 if exc.code in {"rate_limited", "ingress_rate_limited"} else 503
            response = self.plain_error("status, exc.code)
            if status == 429:
                response.headers["Retry-After"] = "15"
            return response
        except Exception:
            logger.exception("prod_ops_mcp request failed correlation_id=%s", correlation)
            return self.plain_error(500, "internal_error")
        finally:
            log = logger.info if auth_mode != "rejected" else logger.debug
            log("prod_ops_mcp request correlation_id=%s auth=%s tool=%s duration_ms=%s",
                correlation, auth_mode, tool_name[:80], int((time.monotonic() - started) * 1000))

    async def handle_get(self, request: web.Request) -> web.Response:
        try:
            authenticate(request.headers, self.config)
        except web.HTTPRequestEntityTooLarge:
            return self.plain_error(413, "request_too_lare")
        except AuthError as exc:
            response = self.plain_error(%xc.status, exc.code)
            if exc.status == 401: response.headers["WWW-Authenticate"] = 'Bearer realm="events-prod-ops"'
            return response
        response = self.plain_error(405, "sse_not_supported_stateless_mvp")
        response.headers["Allow"] = "POST"
        return response

    @staticmethod
    def headers(correlation: str) -> dict[str, str]:
        return {"Cache-Control": "no-store", "Pragma": "no-cache",
            "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff",
            "MCP-Protocol-Version": LATEST_PROTOCOL, "X-Correlation-ID": correlation}

    def plain_error(self, status: int, code: str) -> web.Response:
        return web.Response(status=status, body=json.dumps({"error": code}, separators=(",", ":")).encode(),
            content_type="application/json", headers=self.headers(uuid.uuid4().hex))


SERVER_APP_KEY = web.AppKey("prod_ops_mcp_server", OperationsMCPServer)
ENDPOINT_HASH_APP_KEY = web.AppKey("prod_ops_mcp_endpoint_hash", str)


def create_app(config: OpsMCPConfig, repository: ReadOnlyOperationsRepository | None = None,
               social_gate: SocialCapabilityGate | None = None,
               runtime_reader: RuntimeEvidenceReader | None = None) -> web.Application:
    if not config.enabled: raise ValueError("production operations MCP is disabled")
    repository = repository or ReadOnlyOperationsRepository(
        config.database_path, query_timeout_ms=config.db_timeout_ms)
    server = OperationsMCPServer(config, repository, social_gate, runtime_reader)
    app = web.Application(client_max_size=config.max_request_bytes)
    endpoint = f"/{config.path_secret}/mcp"
    app.router.add_post(endpoint, server.handle_post); app.router.add_get(endpoint, server.handle_get)
    app[SERVER_APP_KEY] = server
    app[ENDPOINT_HASH_APP_KEY] = hashlib.sha256(endpoint.encode()).hexdigest()[:12]
    return app
